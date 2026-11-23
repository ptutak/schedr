//! The chaos scenarios (M4.8z): each worker on a link of its own to the master, cut silently
//! (both ways or one), slowed, or the worker frozen, around the worker lease; then a seeded
//! random schedule of all of them under load. Each ends with the suite's invariants and the
//! cleanups' own (see `check::cleanup_problems`), and leaves a bundle in
//! `target/stress/<scenario>/`: the timeline always, and on a failure the roles' logs, the
//! tasks' marks and the database's rows.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::MINUTE;
use crate::chaos::{Links, ProcessWatch, Timeline, Trouble, Way};
use crate::check::{Report, cleanup_problems, overlapping_tries_but};
use crate::load::{Lcg, Stress, chaos_dag};
use crate::support::postgres_database;

/// The worker lease the stress roles run with (`load::ROLE_ENV`).
const LEASE: Duration = Duration::from_secs(20);
/// Past the lease and the cleanup's move (decision 256: the cancel grace, 30 s, and the move
/// wait, 10 s, after the lease): long enough for a cut worker's cleanups to move.
const PAST_THE_MOVE: Duration = Duration::from_secs(75);

/// A chaos scenario under way: the cluster, the workers' links, the processes watched.
struct Chaos {
    stress: Stress,
    report: Report,
    links: Links,
    watch: ProcessWatch,
    workers: Vec<String>,
    /// How many runs to wait for.
    runs: usize,
    /// Its tasks may not exit on `SIGTERM` (21b's plugins): plugins are less strict, by design
    /// (DN38), and the checks allow for it; the other scenarios' tasks do, checked strictly.
    plugins: bool,
}

impl Chaos {
    /// `dags` chaos DAGs (quick, times the scale) of 8 tasks on `workers` workers of 2 slots,
    /// each on its own link; parsed, and `runs` runs of each triggered (one at a time: the
    /// load lasts).
    fn start(scenario: &'static str, sizes: (usize, usize), workers: usize) -> Self {
        Self::start_with(scenario, sizes, workers, "", &mut |stress, id| {
            chaos_dag(id, 8, "sleep 0.5", 20, &stress.marks)
        })
    }

    /// As [`Chaos::start`], with `config` added to the roles' configuration and each DAG's
    /// source from `dag(stress, its ID)`.
    fn start_with(
        scenario: &'static str,
        (dags, runs): (usize, usize),
        workers: usize,
        config: &str,
        dag: &mut dyn FnMut(&Stress, &str) -> String,
    ) -> Self {
        let mut stress = Stress::new(Some(postgres_database()));
        let report = Report::new(scenario);
        let size = stress.size;
        if !config.is_empty() {
            let path = stress.cluster.dir.path().join("config.toml");
            let mut text = std::fs::read_to_string(&path).unwrap();
            text.push_str(config);
            std::fs::write(&path, text).unwrap();
        }
        let ids: Vec<String> = (1..=size.n(dags, dags * 4))
            .map(|i| {
                let id = format!("chaos{i:04}");
                let source = dag(&stress, &id);
                stress.write_dag(&id, &source);
                id
            })
            .collect();
        stress.start_core("master", &[]);
        let master = SocketAddr::from(([127, 0, 0, 1], stress.cluster.grpc_port_of("master")));
        let mut links = Links::new(master, Timeline::new());
        let names: Vec<String> = (1..=size.n(workers, workers * 2))
            .map(|i| format!("w{i:02}"))
            .collect();
        for worker in &names {
            start_linked(&mut stress, &mut links, worker);
        }
        stress.wait_for_dags(ids.len(), 10 * MINUTE);
        let watch = ProcessWatch::new(stress.marks.clone());
        stress.trigger(&ids, runs);
        links.timeline.note(&format!(
            "{runs} runs of {} DAGs triggered on {} workers",
            ids.len(),
            names.len()
        ));
        Self {
            stress,
            report,
            links,
            watch,
            workers: names,
            runs: ids.len() * runs,
            plugins: false,
        }
    }

    /// Waits until a long task (25 s: `c0000`, `c0004`, ...) has run on `worker` for 10 s at
    /// most, so the trouble begun next finds it under way.
    fn under_way(&self, worker: &str) {
        let db = &self.stress.db;
        let long = "('c0000', 'c0004')";
        self.stress
            .cluster
            .wait(&format!("a long task running on {worker}"), || {
                db.count(&format!(
                    "SELECT COUNT(*) FROM attempts a JOIN task_instances ti \
                     ON ti.id = a.task_instance_id WHERE a.status IS NULL \
                     AND a.worker_id = '{worker}' AND ti.task_id IN {long} \
                     AND a.started_at > now() - interval '10 seconds'"
                )) > 0
            });
    }

    /// The trouble must have cost attempts (else it hit nothing, and the scenario proves
    /// nothing): at least one `lost`.
    fn expect_lost(&mut self) {
        #[allow(clippy::cast_precision_loss)]
        let lost = self
            .stress
            .db
            .count("SELECT COUNT(*) FROM attempts WHERE status = 'lost'") as f64;
        self.report.at_least("lost_attempts", lost, 1.0);
    }

    /// Waits for every run, then the checks, the bundle and the report.
    fn finish(self, limit: Duration) {
        self.finish_with(limit, &|_| Vec::new());
    }

    /// As [`Chaos::finish`], with the scenario's own checks, `more(stress)` once every run ended.
    fn finish_with(mut self, limit: Duration, more: &dyn Fn(&Stress) -> Vec<String>) {
        let count = self.runs;
        // Runs left unfinished fail the scenario with its bundle kept, to see why.
        let unfinished = self.stress.runs_finish(count, limit, |_| {}).err();
        let seen = self.watch.finish();
        let troubles = self.links.troubles();
        let stress = &self.stress;
        let mut report = self.report;
        // Ends of tries without an `end` mark: the harness's kills, else when last seen.
        let mut ends = seen.clone();
        ends.extend(stress.killed.iter().map(|(pid, at)| (*pid, *at)));
        report.broken(unfinished.into_iter().collect());
        report.broken(stress.db.invariants(&["success"]));
        report.broken(more(stress));
        // Whatever its tasks and links went through, every worker stays.
        report.broken(
            self.workers
                .iter()
                .filter(|worker| !alive(stress.cluster.pid(worker)))
                .map(|worker| format!("worker {worker} is gone"))
                .collect(),
        );
        let (overlaps, twice) =
            overlapping_tries_but(&stress.db, &stress.marks, &ends, &|task, worker, at| {
                by_design(&troubles, (task, worker, at), self.plugins)
            });
        report.broken(overlaps);
        #[allow(clippy::cast_precision_loss)]
        report.set("tries_run_twice", twice.len() as f64);
        let (cleanups, beside) = cleanup_problems(
            &stress.db,
            &stress.marks,
            (&seen, &stress.killed),
            &troubles,
            self.plugins,
        );
        report.broken(cleanups);
        // DN38: a frozen worker's try outliving its moved cleanup's start, by design.
        #[allow(clippy::cast_precision_loss)]
        report.set("cleanups_beside_their_try", beside.len() as f64);
        for (name, sql) in [
            (
                "lost_attempts",
                "SELECT COUNT(*) FROM attempts WHERE status = 'lost'",
            ),
            (
                "moved_cleanups",
                "SELECT COUNT(*) FROM attempts WHERE cleanup_moves > 0",
            ),
        ] {
            #[allow(clippy::cast_precision_loss)]
            report.set(name, stress.db.count(sql) as f64);
        }
        report.diagnose(stress.db.diagnosis());
        let dir = report.bundle_dir();
        std::fs::write(dir.join("timeline.txt"), self.links.timeline.text()).unwrap();
        // Decision 280: by design, but listed, to see how often and where.
        std::fs::write(dir.join("tries_run_twice.txt"), twice.join("\n")).unwrap();
        std::fs::write(dir.join("cleanups_beside_their_try.txt"), beside.join("\n")).unwrap();
        if report.failed() {
            keep_for_investigation(stress, &dir);
        }
        report.finish(|| stress.cluster.logs());
    }
}

/// A failed scenario's bundle: each role's log (restarts appended), the tasks' marks, and the
/// database's rows.
fn keep_for_investigation(stress: &Stress, dir: &std::path::Path) {
    for entry in std::fs::read_dir(stress.cluster.dir.path())
        .unwrap()
        .flatten()
    {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "log") {
            let _ = std::fs::copy(&path, dir.join(entry.file_name()));
        }
    }
    let _ = std::fs::copy(&stress.marks, dir.join("marks.txt"));
    let snapshot = serde_json::to_string_pretty(&stress.db.snapshot()).unwrap();
    std::fs::write(dir.join("database.json"), snapshot).unwrap();
}

/// Whether process `pid` is still running (not gone, nor a zombie).
fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        // The state follows the command's closing parenthesis.
        stat.rsplit_once(')')
            .is_some_and(|(_, rest)| !rest.trim_start().starts_with(['Z', 'X']))
    })
}

/// Whether an overlap of a task's tries is by design (decision 280): its earlier try's worker
/// was in trouble when the later one started (or shortly after it healed), and that worker
/// couldn't stop the task: it was frozen, or the task has no cleanup that may move (decision
/// 252: a cut worker stops only tasks with `cleanup_anywhere`; here the `c` tasks). A frozen
/// worker stops them once thawed; with `plugins` (21b's, which may ignore SIGTERM: DN38) one
/// lives on for the grace to SIGKILL (30 s), which the slack after a freeze then holds.
fn by_design(troubles: &[Trouble], (task, worker, at): (&str, &str, u128), plugins: bool) -> bool {
    let slack = 5_000_000_000;
    let grace = 30_000_000_000;
    troubles.iter().any(|t| {
        let after = if t.frozen && plugins {
            slack + grace
        } else {
            slack
        };
        t.worker == worker
            && t.from <= at
            && t.to.is_none_or(|to| at <= to + after)
            && (t.frozen || !task.starts_with('c'))
    })
}

/// Starts `worker` on its own link, its tasks told its name (`STRESS_WORKER`, for the marks).
fn start_linked(stress: &mut Stress, links: &mut Links, worker: &str) {
    let url = links.url(worker);
    stress.start_worker(
        worker,
        2,
        &[
            ("SCHEDR_CONFIG__WORKER__MASTER_URL", &url),
            ("SCHEDR_CONFIG__WORKER__PASS_ENV", r#"["STRESS_WORKER"]"#),
            ("STRESS_WORKER", worker),
        ],
    );
}

/// Scenario 17: A silent cut (both ways, nothing closed): one worker shorter than the lease
/// (it goes on), another past the lease and the cleanups' move (its `cleanup_anywhere` tasks
/// stopped, their cleanups moved, then it comes back).
#[test]
fn stress_17_silent_cut() {
    let mut chaos = Chaos::start("17_silent_cut", (6, 3), 4);
    chaos.under_way("w01");
    chaos.links.cut("w01", &[Way::Up, Way::Down]);
    std::thread::sleep(LEASE / 2);
    chaos.links.heal("w01");
    chaos.under_way("w02");
    chaos.links.cut("w02", &[Way::Up, Way::Down]);
    std::thread::sleep(PAST_THE_MOVE);
    chaos.links.heal("w02");
    chaos.expect_lost();
    chaos.finish(20 * MINUTE);
}

/// Scenario 18: A one-way cut past the lease: first the master hears nothing from one worker
/// (its messages still arrive), then another worker hears nothing from the master.
#[test]
fn stress_18_one_way_cut() {
    let mut chaos = Chaos::start("18_one_way_cut", (6, 4), 4);
    chaos.under_way("w01");
    chaos.links.cut("w01", &[Way::Up]);
    std::thread::sleep(PAST_THE_MOVE);
    chaos.links.heal("w01");
    chaos.under_way("w02");
    chaos.links.cut("w02", &[Way::Down]);
    std::thread::sleep(PAST_THE_MOVE);
    chaos.links.heal("w02");
    chaos.expect_lost();
    chaos.finish(20 * MINUTE);
}

/// Scenario 19: A slow link: every worker's link delayed a little for a minute, then one
/// worker's a lot (1.5 s a chunk) for half a minute; nothing may fail.
#[test]
fn stress_19_slow_link() {
    let mut chaos = Chaos::start("19_slow_link", (6, 2), 4);
    chaos.under_way("w01");
    for worker in &chaos.workers {
        chaos.links.slow(worker, Duration::from_millis(200));
    }
    std::thread::sleep(MINUTE);
    let workers = chaos.workers.clone();
    for worker in &workers {
        chaos.links.heal(worker);
    }
    chaos.links.slow("w01", Duration::from_millis(1500));
    std::thread::sleep(MINUTE / 2);
    chaos.links.heal("w01");
    chaos.finish(20 * MINUTE);
}

/// Scenario 20: A worker frozen (SIGSTOP; its tasks run on) past the lease and the cleanups'
/// move, then thawed.
#[test]
fn stress_20_frozen_worker() {
    let mut chaos = Chaos::start("20_frozen_worker", (6, 3), 4);
    chaos.under_way("w01");
    chaos.links.freeze_worker(&chaos.stress.cluster, "w01");
    std::thread::sleep(PAST_THE_MOVE);
    chaos.links.thaw_worker(&chaos.stress.cluster, "w01");
    chaos.expect_lost();
    chaos.finish(20 * MINUTE);
}

/// What a random step does to one worker.
#[derive(Debug, Clone, Copy)]
enum Fault {
    Cut,
    CutUp,
    CutDown,
    Slow,
    Freeze,
}

/// Scenario 21: A seeded random schedule of every fault above, under load: every few seconds a
/// worker not in trouble (at most half of them at once) gets one, for 5 to 90 s. The seed is
/// `SCHEDR_STRESS_SEED`, else the clock's (a fresh one each night); it is in the report and
/// the timeline, to run the same schedule again.
#[test]
fn stress_21_random_chaos() {
    let seed = seed();
    let mut chaos = Chaos::start("21_random_chaos", (10, 6), 6);
    random_chaos(&mut chaos, seed);
    // More runs while it went on would be a load of its own: one each, triggered at the start.
    chaos.finish(30 * MINUTE);
}

/// Scenario 21b (M5.2i13): scenario 21's seeded schedule with every task an extension task of
/// the suite's chaos plugin (`schedr-ext-chaos`), whose first tries misbehave as the seed says:
/// exiting mid-request, killed, hung past their grace, frozen, writing a frame that isn't JSON or
/// one past the size limit, crashing the extension's service (the worker starts it again). Each
/// recovers as designed: the attempt fails or times out and is retried, its cleanup runs once,
/// no task runs on two workers but as decision 280 allows, and every worker stays.
#[test]
fn stress_21b_random_chaos_extensions() {
    let seed = seed();
    // The tries' faults, from the seed too: the same seed, the same faults on the same tries.
    let mut faults = Lcg::new(seed ^ 0x5eed_0fc4_a05e_4700);
    let config = chaos_extension_config();
    // Each DAG's tasks' planned faults, by try: `(DAG, task)` to its first two tries'.
    let mut planned: HashMap<(String, String), [&'static str; 2]> = HashMap::new();
    let mut chaos = Chaos::start_with(
        "21b_random_chaos_extensions",
        (10, 6),
        6,
        &config,
        &mut |stress, id| {
            let (source, plans) = chaos_extension_dag(id, 8, &stress.marks, &mut faults);
            for (k, plan) in plans.into_iter().enumerate() {
                planned.insert((id.to_owned(), format!("c{k:04}")), plan);
            }
            source
        },
    );
    chaos.plugins = true;
    random_chaos(&mut chaos, seed);
    chaos.finish_with(30 * MINUTE, &|stress| {
        faults_that_succeeded(stress, &planned)
    });
}

/// The tries planned to misbehave that succeeded all the same: a fault the worker let through
/// (a frame that isn't JSON read as a message, one past the limit taken) would show only here,
/// the plugin going on to do its work after it.
fn faults_that_succeeded(
    stress: &Stress,
    planned: &HashMap<(String, String), [&'static str; 2]>,
) -> Vec<String> {
    stress
        .db
        .strings(
            "SELECT r.dag_id || ' ' || ti.task_id || ' ' || a.try_number FROM attempts a \
             JOIN task_instances ti ON ti.id = a.task_instance_id \
             JOIN runs r ON r.id = ti.run_id \
             WHERE a.status = 'success' AND a.try_number <= 2",
        )
        .into_iter()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let (dag, task, number) = (fields.next()?, fields.next()?, fields.next()?);
            let number: usize = number.parse().ok()?;
            let fault = planned.get(&(dag.to_owned(), task.to_owned()))?[number - 1];
            (fault != "ok").then(|| format!("{dag}/{task} try {number}: `{fault}` succeeded"))
        })
        .collect()
}

/// The random scenarios' seed: `SCHEDR_STRESS_SEED`, else the clock's.
fn seed() -> u64 {
    std::env::var("SCHEDR_STRESS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            u64::try_from(crate::check::now_ns() % u128::from(u64::MAX)).unwrap_or(1)
        })
}

/// Scenario 21's schedule from `seed`: every few seconds a worker not in trouble (at most half
/// of them at once) gets a fault for 5 to 90 s, for 5 minutes (quick) or 20 (full).
fn random_chaos(chaos: &mut Chaos, seed: u64) {
    chaos.report.seed(seed);
    chaos.links.timeline.note(&format!("seed {seed}"));
    let mut random = Lcg::new(seed);
    let length = chaos.stress.size.time(5 * MINUTE, 20 * MINUTE);
    let started = Instant::now();
    // Each trouble's worker, and when it ends.
    let mut ending: Vec<(String, Instant, Fault)> = Vec::new();
    let mut next = Instant::now();
    while started.elapsed() < length {
        let now = Instant::now();
        for (worker, _, fault) in ending.iter().filter(|(_, at, _)| *at <= now) {
            end(chaos, worker, *fault);
        }
        ending.retain(|(_, at, _)| *at > now);
        if now >= next && ending.len() < chaos.workers.len() / 2 {
            let worker = chaos.workers[random.below(chaos.workers.len())].clone();
            if !ending.iter().any(|(w, _, _)| *w == worker) {
                let fault = [
                    Fault::Cut,
                    Fault::CutUp,
                    Fault::CutDown,
                    Fault::Slow,
                    Fault::Freeze,
                ][random.below(5)];
                let lasts = Duration::from_secs(5 + random.below(86) as u64);
                begin(chaos, &worker, fault, &mut random);
                ending.push((worker, now + lasts, fault));
            }
            next = now + Duration::from_secs(2 + random.below(7) as u64);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    for (worker, _, fault) in std::mem::take(&mut ending) {
        end(chaos, &worker, fault);
    }
}

/// The chaos plugin's faults, as its payload names them, and how many in a hundred tries of
/// the first two get each: hangs and freezes fewer, each holding a slot past the timeout and
/// the grace.
const PLUGIN_FAULTS: [(&str, usize); 8] = [
    ("ok", 48),
    ("exit", 8),
    ("kill", 8),
    ("garbage", 8),
    ("oversize", 8),
    ("service_crash", 8),
    ("hang", 6),
    ("freeze", 6),
];

/// A chaos DAG of `tasks` extension tasks of the chaos plugin, as [`chaos_dag`]'s: 25 s or 2 s
/// of work, 20 retries, a cleanup that may move (`c` tasks: decision 252); the faults of each
/// one's first two tries drawn from `faults`.
fn chaos_extension_dag(
    id: &str,
    tasks: usize,
    marks: &std::path::Path,
    faults: &mut Lcg,
) -> (String, Vec<[&'static str; 2]>) {
    let mut draw = || {
        let mut at = faults.below(100);
        PLUGIN_FAULTS
            .iter()
            .find(|(_, share)| {
                let hit = at < *share;
                at = at.saturating_sub(*share);
                hit
            })
            .map_or("ok", |(name, _)| *name)
    };
    let plans: Vec<[&'static str; 2]> = (0..tasks).map(|_| [draw(), draw()]).collect();
    let listed: Vec<String> = plans
        .iter()
        .map(|[first, second]| format!("[\"{first}\", \"{second}\"]"))
        .collect();
    let marks = marks.display();
    let source = format!(
        r#"from datetime import timedelta

from schedr import Dag, ExtensionTask

FAULTS = [{plans}]

with Dag("{id}"):
    for k, faults in enumerate(FAULTS):
        ExtensionTask(
            f"c{{k:04}}",
            name="chaos",
            version="1.0",
            payload={{"marks": "{marks}", "secs": 25 if k % 4 == 0 else 2, "faults": faults}},
            retries=20,
            retry_delay=timedelta(0),
            timeout=timedelta(seconds=40),
            cleanup=True,
            cleanup_retries=20,
            cleanup_retry_delay=timedelta(0),
            cleanup_anywhere=True,
        )
"#,
        plans = listed.join(", ")
    );
    (source, plans)
}

/// The chaos extension on every worker: the suite's plugin, and its service, its restarts
/// counted over 2 minutes (`service_settle`, 24 h by default): past them it is turned off on the
/// worker, then tried again within the run (DN29), each worker's turn-off and return tested
/// (the user, 2026-11-23).
fn chaos_extension_config() -> String {
    let python = crate::support::python();
    let plugin = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/stress/schedr-ext-chaos")
        .canonicalize()
        .unwrap();
    let plugin = plugin.display();
    format!(
        "\n[extensions.chaos]\ncommand = [\"{python}\", \"{plugin}\"]\n\
         service = [\"{python}\", \"{plugin}\", \"--service\"]\nversions = [\"1.0\"]\n\
         service_settle = \"2m\"\n"
    )
}

fn begin(chaos: &mut Chaos, worker: &str, fault: Fault, random: &mut Lcg) {
    match fault {
        Fault::Cut => chaos.links.cut(worker, &[Way::Up, Way::Down]),
        Fault::CutUp => chaos.links.cut(worker, &[Way::Up]),
        Fault::CutDown => chaos.links.cut(worker, &[Way::Down]),
        Fault::Slow => chaos.links.slow(
            worker,
            Duration::from_millis(100 + random.below(1900) as u64),
        ),
        Fault::Freeze => chaos.links.freeze_worker(&chaos.stress.cluster, worker),
    }
}

fn end(chaos: &mut Chaos, worker: &str, fault: Fault) {
    match fault {
        Fault::Freeze => chaos.links.thaw_worker(&chaos.stress.cluster, worker),
        _ => chaos.links.heal(worker),
    }
}
