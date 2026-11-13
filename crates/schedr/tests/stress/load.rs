//! The load: how big (the profile), the cluster it runs on, the DAGs it runs, and the CLI calls
//! that drive it, many at once.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::check::{Db, Report, histogram_quantile, labelled_quantile, quantile};
use crate::support::{BINARY, Cluster};

/// The suite's size: `SCHEDR_STRESS_PROFILE` = `quick` (the default; sized for GitHub's
/// smallest standard runner, 2 CPUs and 7 GB, and scaled up by this machine's CPUs and memory,
/// at most 4 times) or `full` (fixed, larger; locally or by hand).
#[derive(Debug, Clone, Copy)]
pub struct Size {
    pub full: bool,
    pub scale: usize,
}

impl Size {
    pub fn get() -> Self {
        let full = match std::env::var("SCHEDR_STRESS_PROFILE").as_deref() {
            Ok("full") => true,
            Ok("quick") | Err(_) => false,
            Ok(other) => panic!("SCHEDR_STRESS_PROFILE: `quick` or `full`, not `{other}`"),
        };
        let cpus = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        let memory_gib = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|text| {
                let line = text.lines().find(|l| l.starts_with("MemTotal:"))?;
                let kib: usize = line.split_whitespace().nth(1)?.parse().ok()?;
                Some(kib >> 20)
            })
            .unwrap_or(7);
        let scale = (cpus / 2).min(memory_gib / 7).clamp(1, 4);
        Self { full, scale }
    }

    /// `quick` (times the scale) or `full`.
    pub fn n(self, quick: usize, full: usize) -> usize {
        if self.full { full } else { quick * self.scale }
    }

    /// The same for durations.
    pub fn time(self, quick: Duration, full: Duration) -> Duration {
        if self.full { full } else { quick }
    }

    pub fn describe(self) -> String {
        if self.full {
            "full".to_owned()
        } else {
            format!("quick x{}", self.scale)
        }
    }
}

/// Settings every stress role gets: worker leases long enough for a loaded machine (a lost
/// worker found within 20 s), the master's rounds every 200 ms.
pub const ROLE_ENV: &[(&str, &str)] = &[
    ("SCHEDR_HIDDEN__MASTER__LEASE_TIMEOUT", "20s"),
    ("SCHEDR_HIDDEN__MASTER__TICK_INTERVAL", "200ms"),
];

/// A cluster under load: the roles, its database read directly, and the tasks' marks.
pub struct Stress {
    pub cluster: Cluster,
    pub db: Db,
    pub size: Size,
    pub marks: PathBuf,
    /// Processes the harness killed, and when (see [`crate::check::overlapping_tries`]).
    pub killed: HashMap<u32, u128>,
}

impl Stress {
    /// A cluster on `database_url` (a SQLite file of its own if none), migrated, no roles yet.
    pub fn new(database_url: Option<String>) -> Self {
        let mut cluster = Cluster::new(database_url, "200ms");
        cluster.keep_logs = true;
        std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
        let (code, err) = cluster.schedr(&["db", "migrate"]);
        assert_eq!(code, 0, "{err}");
        let db = Db::connect(&cluster.database_url);
        let marks = cluster.dir.path().join("marks");
        Self {
            cluster,
            db,
            size: Size::get(),
            marks,
            killed: HashMap::new(),
        }
    }

    /// The master (as `name`, with more settings) and the webserver.
    pub fn start_core(&mut self, master: &str, env: &[(&str, &str)]) {
        let mut all = ROLE_ENV.to_vec();
        all.extend_from_slice(env);
        self.cluster.start_as(master, "master", &all);
        self.cluster.start_as("webserver", "webserver", ROLE_ENV);
    }

    /// Starts worker `id` (its own data directory) with `slots`, and more settings.
    pub fn start_worker(&mut self, id: &str, slots: usize, env: &[(&str, &str)]) {
        let data = self.cluster.dir.path().join(format!("data-{id}"));
        let (data, slots) = (data.display().to_string(), slots.to_string());
        let mut all = ROLE_ENV.to_vec();
        all.extend_from_slice(&[
            ("SCHEDR_CONFIG__WORKER__ID", id),
            ("SCHEDR_CONFIG__WORKER__DATA_DIR", &data),
            ("SCHEDR_CONFIG__WORKER__SLOTS", &slots),
        ]);
        all.extend_from_slice(env);
        self.cluster.start_as(id, "worker", &all);
    }

    /// `count` workers `w01`, `w02`, … with `slots` each.
    pub fn start_workers(&mut self, count: usize, slots: usize, env: &[(&str, &str)]) {
        for i in 1..=count {
            self.start_worker(&format!("w{i:02}"), slots, env);
        }
    }

    /// Writes `source` as the DAG file `name.py` in the root.
    pub fn write_dag(&self, name: &str, source: &str) {
        std::fs::write(self.cluster.root().join(format!("{name}.py")), source).unwrap();
    }

    /// Waits until `count` DAGs are listed (parsed), up to `limit`; how long it took.
    pub fn wait_for_dags(&self, count: usize, limit: Duration) -> Duration {
        let started = Instant::now();
        let want = i64::try_from(count).unwrap();
        self.cluster
            .wait_for(&format!("{count} DAGs parsed"), limit, || {
                // A DAG file that doesn't import never will: say why at once.
                let errors = self.db.strings(&format!(
                    "SELECT path || ': ' || {} FROM parse_files WHERE error IS NOT NULL LIMIT 3",
                    self.db.text("error")
                ));
                assert!(
                    errors.is_empty(),
                    "DAG files that don't import:\n{}",
                    errors.join("\n")
                );
                self.db.count(
                    "SELECT COUNT(*) FROM dags WHERE removed_at IS NULL \
                     AND current_version_id IS NOT NULL",
                ) >= want
            });
        started.elapsed()
    }

    /// Unpauses those of `dags` whose trigger is paused (decision 459: one without a trigger has
    /// no switch, and runs by hand either way) and triggers each `runs` times, many CLI calls at
    /// once.
    pub fn trigger(&self, dags: &[String], runs: usize) {
        let call = ["dags", "list", "--json"].map(str::to_owned);
        let (code, out, err) = cli(&self.cluster.api, &call);
        assert_eq!(code, 0, "{out}{err}");
        let listed: serde_json::Value = serde_json::from_str(&out).unwrap();
        let paused: Vec<&str> = listed["dags"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|d| d["trigger_state"] == "paused")
            .filter_map(|d| d["dag_id"].as_str())
            .collect();
        let unpause = dags
            .iter()
            .filter(|d| paused.contains(&d.as_str()))
            .map(|d| vec!["dags".to_owned(), "unpause".to_owned(), d.clone()])
            .collect();
        self.expect_ok(&cli_many(&self.cluster.api, unpause, 16));
        let triggers = dags
            .iter()
            .flat_map(|d| std::iter::repeat_n(d, runs))
            .map(|d| vec!["runs".to_owned(), "trigger".to_owned(), d.clone()])
            .collect();
        self.expect_ok(&cli_many(&self.cluster.api, triggers, 16));
    }

    /// Fails on the first call that didn't succeed, with the roles' logs.
    pub fn expect_ok(&self, results: &[(i32, String, String)]) {
        if let Some((code, out, err)) = results.iter().find(|(code, _, _)| *code != 0) {
            panic!(
                "a CLI call failed with {code}: {out}{err}\n{}",
                self.cluster.logs()
            );
        }
    }

    /// Waits until `count` runs exist and none is queued or running, up to `limit`, calling
    /// `meanwhile` every second (the chaos, while it lasts).
    pub fn wait_for_runs(
        &mut self,
        count: usize,
        limit: Duration,
        mut meanwhile: impl FnMut(&mut Self),
    ) {
        let want = i64::try_from(count).unwrap();
        let deadline = Instant::now() + limit;
        loop {
            if self.db.runs() >= want && self.db.unfinished_runs() == 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "waited too long for {count} runs to finish: {} exist, {} unfinished\n{}",
                self.db.runs(),
                self.db.unfinished_runs(),
                self.cluster.logs()
            );
            meanwhile(self);
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    /// Throughput and latencies of what ran, into `report`, with their floors: at least
    /// `min_tasks_per_s` successful tasks a second.
    pub fn measure(&self, report: &mut Report, master: &str, min_tasks_per_s: f64) {
        let done = self.throughput();
        report.set("tasks", done.tasks);
        report.at_least("tasks_per_s", done.tasks_per_s, min_tasks_per_s);
        report.set("assigned_to_started_p50_s", done.start_p50_s);
        report.at_most("assigned_to_started_p99_s", done.start_p99_s, 30.0);
        let runs = self.db.floats(&format!(
            "SELECT ({}) - ({}) FROM runs WHERE ended_at IS NOT NULL",
            self.db.epoch("ended_at"),
            self.db.epoch("triggered_at")
        ));
        report.set("run_p50_s", quantile(&runs, 0.5));
        report.set("run_p99_s", quantile(&runs, 0.99));
        if self.cluster.ops.iter().any(|(name, _)| name == master) {
            let (_, metrics) = self.cluster.ops_get(master, "/metrics");
            report.at_most(
                "round_p99_s",
                histogram_quantile(&metrics, "schedr_master_tick_duration_seconds", 0.99),
                5.0,
            );
        }
        self.stages(report, master);
    }

    /// Where a start's time goes (M5.1a), p50 and p99 of each stage, recorded without limits:
    /// on the master, the wait for its core lock (by a worker's turn, by the round), each part
    /// of a turn and each part of a round (M5.1c); on the first worker (or the standalone),
    /// from an `AssignTask` taken to its `Started` sent, and each kind of journal record.
    pub fn stages(&self, report: &mut Report, master: &str) {
        let has = |name: &str| self.cluster.ops.iter().any(|(ops, _)| ops == name);
        let mut put = |metrics: &str, name: &str, labels: &str, key: &str| {
            for (q, at) in [(0.5, "p50"), (0.99, "p99")] {
                let value = if labels.is_empty() {
                    histogram_quantile(metrics, name, q)
                } else {
                    labelled_quantile(metrics, name, labels, q)
                };
                report.set(&format!("{key}_{at}_s"), value);
            }
        };
        let master_metrics = has(master).then(|| self.cluster.ops_get(master, "/metrics").1);
        if let Some(metrics) = &master_metrics {
            for by in ["turn", "round"] {
                let labels = format!("by=\"{by}\"");
                let key = format!("lock_wait_{by}");
                put(
                    metrics,
                    "schedr_master_core_lock_wait_seconds",
                    &labels,
                    &key,
                );
            }
            for part in ["started", "finished", "rest"] {
                let labels = format!("part=\"{part}\"");
                let key = format!("turn_{part}");
                put(metrics, "schedr_master_turn_part_seconds", &labels, &key);
            }
            // Each part of a round (M5.1c): what a round holding the lock spends its time on.
            for part in [
                "commands",
                "scheduler",
                "triggers",
                "prepare",
                "expire",
                "assign",
                "rest",
            ] {
                let labels = format!("part=\"{part}\"");
                let key = format!("round_{part}");
                put(metrics, "schedr_master_round_part_seconds", &labels, &key);
            }
        }
        let worker = if has("w01") {
            Some(self.cluster.ops_get("w01", "/metrics").1)
        } else {
            master_metrics
        };
        if let Some(metrics) = &worker {
            put(
                metrics,
                "schedr_worker_assign_to_started_seconds",
                "",
                "worker_assign",
            );
            for record in ["start", "process", "results"] {
                let labels = format!("record=\"{record}\"");
                let key = format!("journal_{record}");
                put(
                    metrics,
                    "schedr_worker_journal_write_seconds",
                    &labels,
                    &key,
                );
            }
        }
    }
}

/// What the tasks did, from the database: how many succeeded, how many a second (from the
/// first assignment to the last end), and the p99 from assigned to started.
#[derive(Debug, Clone, Copy)]
pub struct Throughput {
    pub tasks: f64,
    pub tasks_per_s: f64,
    pub start_p50_s: f64,
    pub start_p99_s: f64,
}

impl Stress {
    pub fn throughput(&self) -> Throughput {
        let successes = self
            .db
            .count("SELECT COUNT(*) FROM attempts WHERE status = 'success'");
        let span = self.db.floats(&format!(
            "SELECT MAX({}) - MIN({}) FROM attempts",
            self.db.epoch("ended_at"),
            self.db.epoch("assigned_at")
        ));
        let span = span.first().copied().unwrap_or(0.0).max(0.001);
        let tasks = f64::from(u32::try_from(successes).unwrap());
        let starts = self.db.floats(&format!(
            "SELECT ({}) - ({}) FROM attempts WHERE started_at IS NOT NULL",
            self.db.epoch("started_at"),
            self.db.epoch("assigned_at")
        ));
        Throughput {
            tasks,
            tasks_per_s: tasks / span,
            start_p50_s: quantile(&starts, 0.5),
            start_p99_s: quantile(&starts, 0.99),
        }
    }
}

/// A DAG `id` of a Python task and `tasks` command tasks after it, each running `work` between
/// its marks, with `retries` (no delay).
pub fn command_dag(
    id: &str,
    tasks: usize,
    work: &str,
    retries: u32,
    marks: &std::path::Path,
) -> String {
    let command = crate::check::marked(marks, work);
    format!(
        r#"from datetime import timedelta

from schedr import CommandOperator, Dag, task

@task(retries={retries}, retry_delay=timedelta(0))
def plan() -> int:
    return 1

with Dag("{id}"):
    n = plan()
    for k in range({tasks}):
        CommandOperator(
            f"c{{k:04}}",
            "{command}",
            env={{"PLANNED": n}},
            retries={retries},
            retry_delay=timedelta(0),
        )
"#
    )
}

/// Writes `count` DAGs `<prefix>0001`, … of `tasks` tasks each; their IDs.
/// A DAG for the chaos scenarios (M4.8z): a resource `env` (a command setup and cleanup), and
/// `tasks` command tasks using it, each sleeping 2 s (every fourth 25 s, to be under way when a
/// link is cut) with a cleanup `cleanup_work` that may run on another worker when its own is
/// lost (`cleanup_anywhere`); all marked, with `retries` for the tasks and the cleanups alike.
pub fn chaos_dag(
    id: &str,
    tasks: usize,
    cleanup_work: &str,
    retries: u32,
    marks: &std::path::Path,
) -> String {
    let command = crate::check::marked(marks, "sleep {secs}");
    let cleanup = crate::check::marked_cleanup(marks, cleanup_work);
    let setup = crate::check::marked(marks, "sleep 0.2");
    format!(
        r#"from datetime import timedelta

from schedr import CommandOperator, CommandResource, Dag

with Dag("{id}"):
    env = CommandResource("env", "{setup}", retries={retries}, retry_delay=timedelta(0))
    env.cleanup_command("{setup}", retries={retries}, retry_delay=timedelta(0))
    for k in range({tasks}):
        secs = 25 if k % 4 == 0 else 2
        CommandOperator(
            f"c{{k:04}}",
            f"{command}",
            retries={retries},
            retry_delay=timedelta(0),
            cleanup="{cleanup}",
            cleanup_retries={retries},
            cleanup_retry_delay=timedelta(0),
            cleanup_anywhere=True,
            uses=[env],
        )
"#
    )
}

pub fn command_dags(
    stress: &Stress,
    prefix: &str,
    count: usize,
    tasks: usize,
    work: &str,
    retries: u32,
) -> Vec<String> {
    (1..=count)
        .map(|i| {
            let id = format!("{prefix}{i:04}");
            stress.write_dag(&id, &command_dag(&id, tasks, work, retries, &stress.marks));
            id
        })
        .collect()
}

/// Runs `schedr <call>` against `api` for each call, `threads` at a time: exit code, stdout and
/// stderr of each, in order.
pub fn cli_many(api: &str, calls: Vec<Vec<String>>, threads: usize) -> Vec<(i32, String, String)> {
    let calls: Vec<(usize, Vec<String>)> = calls.into_iter().enumerate().collect();
    let chunk = calls.len().div_ceil(threads.max(1)).max(1);
    let mut results: Vec<(usize, (i32, String, String))> = std::thread::scope(|scope| {
        calls
            .chunks(chunk)
            .map(|part| {
                scope.spawn(move || {
                    part.iter()
                        .map(|(i, call)| (*i, cli(api, call)))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect()
    });
    results.sort_by_key(|(i, _)| *i);
    results.into_iter().map(|(_, result)| result).collect()
}

/// `schedr <group> --server <api> <rest>`: exit code, stdout, stderr.
pub fn cli(api: &str, call: &[String]) -> (i32, String, String) {
    let (group, rest) = call.split_first().unwrap();
    let output = Command::new(BINARY)
        .arg(group)
        .args(["--server", api])
        .args(rest)
        .env("TZ", "UTC")
        .output()
        .unwrap();
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// A small deterministic random sequence (the scenarios' chaos is the same each run).
pub struct Lcg(u64);

impl Lcg {
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// A number below `bound`.
    pub fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let high = usize::try_from(self.0 >> 33).unwrap();
        high % bound.max(1)
    }
}
