//! The chaos scenarios (M4.8z): each worker on a link of its own to the master, cut silently
//! (both ways or one), slowed, or the worker frozen, around the worker lease; then a seeded
//! random schedule of all of them under load. Each ends with the suite's invariants and the
//! cleanups' own (see `check::cleanup_problems`), and leaves a bundle in
//! `target/stress/<scenario>/`: the timeline always, and on a failure the roles' logs, the
//! tasks' marks and the database's rows.

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
}

impl Chaos {
    /// `dags` chaos DAGs (quick, times the scale) of 8 tasks on `workers` workers of 2 slots,
    /// each on its own link; parsed, and `runs` runs of each triggered (one at a time: the
    /// load lasts).
    fn start(scenario: &'static str, (dags, runs): (usize, usize), workers: usize) -> Self {
        let mut stress = Stress::new(Some(postgres_database()));
        let report = Report::new(scenario);
        let size = stress.size;
        let ids: Vec<String> = (1..=size.n(dags, dags * 4))
            .map(|i| {
                let id = format!("chaos{i:04}");
                stress.write_dag(&id, &chaos_dag(&id, 8, "sleep 0.5", 20, &stress.marks));
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
    fn finish(mut self, limit: Duration) {
        let count = self.runs;
        self.stress.wait_for_runs(count, limit, |_| {});
        let seen = self.watch.finish();
        let troubles = self.links.troubles();
        let stress = &self.stress;
        let mut report = self.report;
        // Ends of tries without an `end` mark: the harness's kills, else when last seen.
        let mut ends = seen.clone();
        ends.extend(stress.killed.iter().map(|(pid, at)| (*pid, *at)));
        report.broken(stress.db.invariants(&["success"]));
        let (overlaps, twice) =
            overlapping_tries_but(&stress.db, &stress.marks, &ends, &|task, worker, at| {
                by_design(&troubles, task, worker, at)
            });
        report.broken(overlaps);
        #[allow(clippy::cast_precision_loss)]
        report.set("tries_run_twice", twice.len() as f64);
        report.broken(cleanup_problems(
            &stress.db,
            &stress.marks,
            (&seen, &stress.killed),
            &troubles,
        ));
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

/// Whether an overlap of a task's tries is by design (decision 280): its earlier try's worker
/// was in trouble when the later one started (or shortly after it healed), and that worker
/// couldn't stop the task: it was frozen, or the task has no cleanup that may move (decision
/// 252: a cut worker stops only tasks with `cleanup_anywhere`; here the `c` tasks).
fn by_design(troubles: &[Trouble], task: &str, worker: &str, at: u128) -> bool {
    let slack = 5_000_000_000;
    troubles.iter().any(|t| {
        t.worker == worker
            && t.from <= at
            && t.to.is_none_or(|to| at <= to + slack)
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
    let seed = std::env::var("SCHEDR_STRESS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            u64::try_from(crate::check::now_ns() % u128::from(u64::MAX)).unwrap_or(1)
        });
    let mut chaos = Chaos::start("21_random_chaos", (10, 6), 6);
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
            end(&mut chaos, worker, *fault);
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
                begin(&mut chaos, &worker, fault, &mut random);
                ending.push((worker, now + lasts, fault));
            }
            next = now + Duration::from_secs(2 + random.below(7) as u64);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    for (worker, _, fault) in std::mem::take(&mut ending) {
        end(&mut chaos, &worker, fault);
    }
    // More runs while it went on would be a load of its own: one each, triggered at the start.
    chaos.finish(30 * MINUTE);
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
