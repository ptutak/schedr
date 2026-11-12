//! What every scenario checks at its end, and what it reports.
//!
//! **Invariants** (always a failure, see archive/PROJECT.md, stress suite): nothing still running,
//! no task succeeding twice, no command undecided, no run in a state the scenario doesn't expect,
//! no try of a task starting before the previous one ended, by the database and by the tasks' own
//! marks (each task appends a `start` and an `end` line to a shared file). **Performance** only
//! fails on a gross regression (a generous floor per scenario); the numbers are written to
//! `target/stress/<scenario>.json`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

use sqlx::AssertSqlSafe;

/// Nanoseconds since the Unix epoch, as the tasks' `date +%s%N`.
pub fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

enum Pool {
    Postgres(sqlx::PgPool),
    Sqlite(sqlx::SqlitePool),
}

/// Seconds since the Unix epoch.
pub fn now_s() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// The scenario's database, read directly (the API pages and rounds would hide too much).
pub struct Db {
    runtime: tokio::runtime::Runtime,
    pool: Pool,
}

impl Db {
    pub fn connect(url: &str) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pool = runtime.block_on(async {
            if url.starts_with("sqlite:") {
                Pool::Sqlite(sqlx::SqlitePool::connect(url).await.unwrap())
            } else {
                Pool::Postgres(sqlx::PgPool::connect(url).await.unwrap())
            }
        });
        Self { runtime, pool }
    }

    fn postgres(&self) -> bool {
        matches!(self.pool, Pool::Postgres(_))
    }

    /// A time column as seconds since the epoch, for this backend.
    pub fn epoch(&self, column: &str) -> String {
        if self.postgres() {
            format!("EXTRACT(EPOCH FROM {column})::float8")
        } else {
            format!("((julianday({column}) - 2440587.5) * 86400.0)")
        }
    }

    /// A UUID or JSON column as text, for this backend.
    pub fn text(&self, column: &str) -> String {
        if self.postgres() {
            format!("{column}::text")
        } else {
            column.to_owned()
        }
    }

    pub fn count(&self, sql: &str) -> i64 {
        let sql = AssertSqlSafe(sql.to_owned());
        self.runtime.block_on(async {
            match &self.pool {
                Pool::Postgres(pool) => sqlx::query_scalar(sql).fetch_one(pool).await,
                Pool::Sqlite(pool) => sqlx::query_scalar(sql).fetch_one(pool).await,
            }
            .unwrap()
        })
    }

    /// A statement without a result (e.g. `ALTER TABLE`).
    pub fn execute(&self, sql: &str) {
        let sql = AssertSqlSafe(sql.to_owned());
        self.runtime.block_on(async {
            match &self.pool {
                Pool::Postgres(pool) => sqlx::query(sql).execute(pool).await.map(|_| ()),
                Pool::Sqlite(pool) => sqlx::query(sql).execute(pool).await.map(|_| ()),
            }
            .unwrap();
        });
    }

    /// One text column.
    pub fn strings(&self, sql: &str) -> Vec<String> {
        let sql = AssertSqlSafe(sql.to_owned());
        self.runtime.block_on(async {
            match &self.pool {
                Pool::Postgres(pool) => sqlx::query_scalar(sql).fetch_all(pool).await,
                Pool::Sqlite(pool) => sqlx::query_scalar(sql).fetch_all(pool).await,
            }
            .unwrap()
        })
    }

    /// The IDs of `dag`'s runs.
    pub fn run_ids(&self, dag: &str) -> Vec<String> {
        self.strings(&format!(
            "SELECT {} FROM runs WHERE dag_id = '{dag}' ORDER BY triggered_at",
            self.text("id")
        ))
    }

    /// One `float8` / `REAL` column (NULLs left out).
    pub fn floats(&self, sql: &str) -> Vec<f64> {
        let sql = AssertSqlSafe(sql.to_owned());
        let values: Vec<Option<f64>> = self.runtime.block_on(async {
            match &self.pool {
                Pool::Postgres(pool) => sqlx::query_scalar(sql).fetch_all(pool).await,
                Pool::Sqlite(pool) => sqlx::query_scalar(sql).fetch_all(pool).await,
            }
            .unwrap()
        });
        values.into_iter().flatten().collect()
    }

    /// When each try of each task ended, by the database: `(run, task, try)` to nanoseconds.
    fn ended(&self) -> HashMap<(String, String, u32), u128> {
        let sql = AssertSqlSafe(format!(
            "SELECT {}, ti.task_id, a.try_number, {} FROM attempts a \
             JOIN task_instances ti ON ti.id = a.task_instance_id WHERE a.ended_at IS NOT NULL",
            self.text("ti.run_id"),
            self.epoch("a.ended_at")
        ));
        let rows: Vec<(String, String, i32, f64)> = self.runtime.block_on(async {
            match &self.pool {
                Pool::Postgres(pool) => sqlx::query_as(sql).fetch_all(pool).await,
                Pool::Sqlite(pool) => sqlx::query_as(sql).fetch_all(pool).await,
            }
            .unwrap()
        });
        rows.into_iter()
            .map(|(run, task, try_number, seconds)| {
                let try_number = u32::try_from(try_number).unwrap();
                // Whole microseconds, as the database keeps them.
                let micros = (seconds * 1e6).round();
                let ns = u128::from(micros_u64(micros)) * 1000;
                ((run, task, try_number), ns)
            })
            .collect()
    }

    /// The tries whose cleanup ended `ok`, of tasks whose cleanup is a command (a task's
    /// `cleanup`): `(run, task, try)`.
    fn cleaned(&self) -> Vec<(String, String, u32)> {
        let sql = AssertSqlSafe(format!(
            "SELECT {}, ti.task_id, a.try_number FROM attempts a \
             JOIN task_instances ti ON ti.id = a.task_instance_id \
             WHERE a.cleanup_result = 'ok'",
            self.text("ti.run_id"),
        ));
        let rows: Vec<(String, String, i32)> = self.runtime.block_on(async {
            match &self.pool {
                Pool::Postgres(pool) => sqlx::query_as(sql).fetch_all(pool).await,
                Pool::Sqlite(pool) => sqlx::query_as(sql).fetch_all(pool).await,
            }
            .unwrap()
        });
        rows.into_iter()
            .map(|(run, task, number)| (run, task, u32::try_from(number).unwrap()))
            .collect()
    }

    /// The scenario's state as JSON, table by table, for a failure's bundle (Postgres only:
    /// each row through `row_to_json`).
    pub fn snapshot(&self) -> serde_json::Value {
        if !self.postgres() {
            return serde_json::Value::Null;
        }
        let tables = ["runs", "task_instances", "attempts", "workers", "commands"];
        let mut all = serde_json::Map::new();
        for table in tables {
            let rows: Vec<serde_json::Value> = self
                .strings(&format!("SELECT row_to_json(t)::text FROM {table} t"))
                .iter()
                .filter_map(|row| serde_json::from_str(row).ok())
                .collect();
            all.insert(table.to_owned(), serde_json::Value::Array(rows));
        }
        serde_json::Value::Object(all)
    }

    /// Runs in `queued` or `running`.
    pub fn unfinished_runs(&self) -> i64 {
        self.count("SELECT COUNT(*) FROM runs WHERE state IN ('queued', 'running')")
    }

    pub fn runs(&self) -> i64 {
        self.count("SELECT COUNT(*) FROM runs")
    }

    /// Attempts by status (`open` for those still running), and the error of each attempt of
    /// a run that didn't succeed: what a failed scenario's report says about how it failed.
    pub fn diagnosis(&self) -> serde_json::Value {
        let statuses = self.strings(
            "SELECT COALESCE(status, 'open') || ': ' || COUNT(*) FROM attempts \
             GROUP BY status ORDER BY 1",
        );
        let runs = self.strings(&format!(
            "SELECT r.state || ' ' || r.dag_id || ' ' || ti.task_id || ' try ' || a.try_number \
             || ' ' || COALESCE(a.status, 'open') || ' on ' || COALESCE(a.worker_id, '-') || ': ' \
             || COALESCE({}, '') FROM runs r JOIN task_instances ti ON ti.run_id = r.id \
             JOIN attempts a ON a.task_instance_id = ti.id \
             WHERE r.state <> 'success' AND COALESCE(a.status, 'open') <> 'success' \
             ORDER BY r.id, ti.task_id, a.try_number LIMIT 40",
            self.text("a.error_summary")
        ));
        serde_json::json!({ "attempts": statuses, "not_successful": runs })
    }

    /// The invariants every scenario ends with: what is wrong, if anything.
    pub fn invariants(&self, final_states: &[&str]) -> Vec<String> {
        let states = final_states
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let checks = [
            (
                "attempts still running".to_owned(),
                "SELECT COUNT(*) FROM attempts WHERE status IS NULL".to_owned(),
            ),
            (
                "tasks with more than one successful attempt".to_owned(),
                "SELECT COUNT(*) FROM (SELECT task_instance_id FROM attempts \
                 WHERE status = 'success' GROUP BY task_instance_id HAVING COUNT(*) > 1) x"
                    .to_owned(),
            ),
            (
                "commands never decided".to_owned(),
                "SELECT COUNT(*) FROM commands WHERE status = 'pending'".to_owned(),
            ),
            (
                "successful tasks without a successful attempt".to_owned(),
                // A mapped task as a whole (`map_sizes` set once expanded) succeeds through
                // its instances' attempts, never one of its own.
                "SELECT COUNT(*) FROM task_instances ti WHERE ti.state = 'success' \
                 AND ti.map_sizes IS NULL AND NOT \
                 EXISTS (SELECT 1 FROM attempts a WHERE a.task_instance_id = ti.id \
                 AND a.status = 'success')"
                    .to_owned(),
            ),
            (
                "tasks of successful runs that didn't succeed".to_owned(),
                "SELECT COUNT(*) FROM task_instances ti JOIN runs r ON r.id = ti.run_id \
                 WHERE r.state = 'success' AND ti.state <> 'success'"
                    .to_owned(),
            ),
            (
                "tries started (by the database) before the previous one ended".to_owned(),
                "SELECT COUNT(*) FROM attempts a JOIN attempts b \
                 ON b.task_instance_id = a.task_instance_id AND b.try_number = a.try_number + 1 \
                 WHERE b.started_at < a.ended_at"
                    .to_owned(),
            ),
            (
                format!("runs not in {states}"),
                format!("SELECT COUNT(*) FROM runs WHERE state NOT IN ({states})"),
            ),
        ];
        checks
            .into_iter()
            .filter_map(|(what, sql)| {
                let found = self.count(&sql);
                (found != 0).then(|| format!("{found} {what}"))
            })
            .collect()
    }
}

// Times after 1970, in microseconds: positive, and far within u64.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
const fn micros_u64(micros: f64) -> u64 {
    micros as u64
}

/// The shell command a stress task runs: a `start` mark, `work`, an `end` mark, each mark one
/// line appended to `marks` (shorter than `PIPE_BUF`, so lines from tasks running at once never
/// mix), with the worker (`STRESS_WORKER`, set only in the chaos scenarios) as a seventh field.
pub fn marked(marks: &Path, work: &str) -> String {
    let mark = |what: &str| {
        format!(
            "echo {what} $SCHEDR_RUN_ID $SCHEDR_TASK_ID $SCHEDR_TRY_NUMBER $$ $(date +%s%N) \
             $STRESS_WORKER >> {}",
            marks.display()
        )
    };
    format!("{}; {work}; {}", mark("start"), mark("end"))
}

/// One try of a task as its marks tell: when it started and when it ended (its `end` mark, the
/// moment the harness killed it, or when the database recorded its end).
struct Try {
    task: (String, String),
    number: u32,
    start: u128,
    end: Option<u128>,
    worker: String,
}

/// Tries of the same task that ran at the same time, by the tasks' own marks; `killed` holds
/// the processes the harness killed and when.
pub fn overlapping_tries(db: &Db, marks: &Path, killed: &HashMap<u32, u128>) -> Vec<String> {
    overlapping_tries_but(db, marks, killed, &|_, _, _| false).0
}

/// As [`overlapping_tries`], setting apart the overlaps `by_design(task, earlier's worker, the
/// later try's start)` says are by design (decision 280: a task its cut-off worker doesn't
/// stop runs twice, at least once): what is wrong, and those.
pub fn overlapping_tries_but(
    db: &Db,
    marks: &Path,
    killed: &HashMap<u32, u128>,
    by_design: &dyn Fn(&str, &str, u128) -> bool,
) -> (Vec<String>, Vec<String>) {
    let text = std::fs::read_to_string(marks).unwrap_or_default();
    let ended = db.ended();
    // By process too: two runs of one try (an attempt started twice) are two, not one.
    let mut tries: BTreeMap<(String, String, u32, u32), Try> = BTreeMap::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [what @ ("start" | "end"), run, task, number, pid, ns, ..] = fields[..] else {
            continue;
        };
        let worker = fields.get(6).copied().unwrap_or("-");
        let (Ok(number), Ok(pid), Ok(ns)) = (
            number.parse::<u32>(),
            pid.parse::<u32>(),
            ns.parse::<u128>(),
        ) else {
            continue;
        };
        let key = (run.to_owned(), task.to_owned(), number);
        let entry = tries
            .entry((run.to_owned(), task.to_owned(), number, pid))
            .or_insert_with(|| Try {
                task: (run.to_owned(), task.to_owned()),
                number,
                start: ns,
                end: killed.get(&pid).copied(),
                worker: worker.to_owned(),
            });
        match what {
            "start" => entry.start = ns,
            _ => entry.end = Some(ns),
        }
        if entry.end.is_none() {
            entry.end = ended.get(&key).copied();
        }
    }
    let mut by_task: BTreeMap<(String, String), Vec<&Try>> = BTreeMap::new();
    for one in tries.values() {
        by_task.entry(one.task.clone()).or_default().push(one);
    }
    let (mut overlaps, mut twice) = (Vec::new(), Vec::new());
    for (task, mut list) in by_task {
        list.sort_by_key(|t| t.start);
        for pair in list.windows(2) {
            let (earlier, later) = (pair[0], pair[1]);
            match earlier.end {
                Some(end) if end <= later.start => {}
                end => {
                    let what = format!(
                        "{}/{}: try {} started while try {} still ran on {} (ended {end:?}, \
                         started {})",
                        task.0, task.1, later.number, earlier.number, earlier.worker, later.start
                    );
                    if by_design(&task.1, &earlier.worker, later.start) {
                        twice.push(what);
                    } else {
                        overlaps.push(what);
                    }
                }
            }
        }
    }
    (overlaps, twice)
}

/// A cleanup command for a stress task: a `cstart` mark, `work`, a `cend` mark, each with the
/// worker it ran on (`STRESS_WORKER`, passed to tasks with `pass_env`) as a seventh field.
pub fn marked_cleanup(marks: &Path, work: &str) -> String {
    let mark = |what: &str| {
        format!(
            "echo {what} $SCHEDR_RUN_ID $SCHEDR_TASK_ID $SCHEDR_TRY_NUMBER $$ $(date +%s%N) \
             $STRESS_WORKER >> {}",
            marks.display()
        )
    };
    format!("{}; {work}; {}", mark("cstart"), mark("cend"))
}

/// One run of a try's cleanup, by its marks.
struct CleanupRun {
    worker: String,
    start: u128,
    end: Option<u128>,
}

/// What the tasks' and their cleanups' marks say is wrong (PLAN.md M4.8z): a cleanup that
/// started while its try still ran (a try with no mark never ran: its work comes after its
/// `start` mark, decision 460); a try's cleanup run to its end more than once, unless the
/// earlier run's worker was in trouble then (decision 255: a cleanup under way when the master
/// goes is left running, and the master moves it); and a try whose cleanup the database says
/// ran (`ok`) without a mark of it ending. `seen` holds when each task process was last seen
/// alive, `killed` what the harness killed and when.
pub fn cleanup_problems(
    db: &Db,
    marks: &Path,
    (seen, killed): (&HashMap<u32, u128>, &HashMap<u32, u128>),
    troubles: &[crate::chaos::Trouble],
) -> Vec<String> {
    type Key = (String, String, u32);
    let text = std::fs::read_to_string(marks).unwrap_or_default();
    let ended = db.ended();
    let mut tasks: BTreeMap<Key, (Option<u128>, Option<u128>)> = BTreeMap::new();
    let mut cleanups: BTreeMap<Key, BTreeMap<u32, CleanupRun>> = BTreeMap::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [what, run, task, number, pid, ns, ..] = fields[..] else {
            continue;
        };
        let (Ok(number), Ok(pid), Ok(ns)) = (
            number.parse::<u32>(),
            pid.parse::<u32>(),
            ns.parse::<u128>(),
        ) else {
            continue;
        };
        let key = (run.to_owned(), task.to_owned(), number);
        match what {
            "start" => {
                // Its end, if no `end` mark comes: the harness's kill, else the last moment
                // its process was seen.
                let gone = killed.get(&pid).or_else(|| seen.get(&pid)).copied();
                let entry = tasks.entry(key).or_insert((None, gone));
                entry.0 = Some(ns);
            }
            "end" => tasks.entry(key).or_insert((None, None)).1 = Some(ns),
            "cstart" | "cend" => {
                let worker = fields.get(6).copied().unwrap_or("-").to_owned();
                let run = cleanups
                    .entry(key)
                    .or_default()
                    .entry(pid)
                    .or_insert(CleanupRun {
                        worker,
                        start: ns,
                        end: None,
                    });
                if what == "cstart" {
                    run.start = ns;
                } else {
                    run.end = Some(ns);
                }
            }
            _ => {}
        }
    }
    let in_trouble = |worker: &str, from: u128, to: u128| {
        troubles
            .iter()
            .any(|t| t.worker == worker && t.from <= to && t.to.is_none_or(|end| from <= end))
    };
    let mut problems = Vec::new();
    for (key, runs) in &cleanups {
        let name = format!("{}/{} try {}", key.0, key.1, key.2);
        // A try with no mark never ran (sent to a worker cut off, which the master lost
        // not knowing, so it moved the cleanup as for one that may have run, decision 460):
        // nothing for its cleanup to overlap. The database's end is no help there: a moved
        // cleanup's attempt ends with the cleanup.
        let task_end = tasks
            .get(key)
            .map(|(_, end)| end.or_else(|| ended.get(key).copied()));
        for run in runs.values() {
            match task_end {
                None => {}
                Some(Some(end)) if end <= run.start => {}
                Some(end) => problems.push(format!(
                    "{name}: its cleanup started on {} at {} while the try still ran (ended \
                     {end:?})",
                    run.worker, run.start
                )),
            }
        }
        let mut done: Vec<&CleanupRun> = runs.values().filter(|r| r.end.is_some()).collect();
        done.sort_by_key(|r| r.start);
        if let Some((last, earlier)) = done.split_last() {
            for run in earlier {
                // Decision 255: a run under way when its worker lost the master (in trouble
                // at any moment of it) may be moved and run again.
                let end = run.end.unwrap_or(run.start);
                if !in_trouble(&run.worker, run.start, end) {
                    problems.push(format!(
                        "{name}: its cleanup ran to its end on {} (started {}) and again on {} \
                         (started {}), its first worker not cut off while it ran",
                        run.worker, run.start, last.worker, last.start
                    ));
                }
            }
        }
    }
    for (run, task, number) in db.cleaned() {
        let key = (run, task, number);
        let ran = cleanups
            .get(&key)
            .is_some_and(|runs| runs.values().any(|r| r.end.is_some()));
        if !ran {
            problems.push(format!(
                "{}/{} try {}: its cleanup is `ok` in the database, but no mark of it ending",
                key.0, key.1, key.2
            ));
        }
    }
    problems
}

/// The `q` quantile (0..=1) of `values`, the nearest rank; 0 for none.
pub fn quantile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = (q * f64::from(u32::try_from(sorted.len()).unwrap())).ceil();
    // A rank between 0 and the length: positive and small.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let index = (rank as usize).max(1) - 1;
    sorted[index.min(sorted.len() - 1)]
}

/// The `q` quantile of the series of histogram `name` with `labels` besides `le` (e.g.
/// `method="fork",phase="runner"`), as [`histogram_quantile`] reads a plain one.
pub fn labelled_quantile(metrics: &str, name: &str, labels: &str, q: f64) -> f64 {
    // The bucket lines of the series with `labels` (e.g. `method="fork",phase="runner"`),
    // as the plain histogram's: `le` first, then the labels.
    let wanted = format!(",{labels}}}");
    let bucket = format!("{name}_bucket{{");
    let plain = metrics
        .lines()
        .filter(|line| line.starts_with(&bucket) && line.contains(&wanted))
        .map(|line| line.replace(&wanted, "}"))
        .collect::<Vec<_>>()
        .join("\n");
    histogram_quantile(&plain, name, q)
}

/// The `q` quantile of a Prometheus histogram in a `/metrics` text (`<name>_bucket{le="…"}`):
/// the upper bound of the first bucket holding it.
pub fn histogram_quantile(metrics: &str, name: &str, q: f64) -> f64 {
    let prefix = format!("{name}_bucket{{le=\"");
    let buckets: Vec<(f64, f64)> = metrics
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(&prefix)?;
            let (le, count) = rest.split_once("\"}")?;
            let le = if le == "+Inf" {
                f64::INFINITY
            } else {
                le.parse().ok()?
            };
            Some((le, count.trim().parse().ok()?))
        })
        .collect();
    let Some(&(_, total)) = buckets.last() else {
        return 0.0;
    };
    buckets
        .iter()
        .find(|(_, count)| *count >= q * total)
        .map_or(0.0, |(le, _)| *le)
}

/// A scenario's numbers, written to `target/stress/<scenario>.json`, and its gross-regression
/// floors.
pub struct Report {
    scenario: &'static str,
    started: Instant,
    metrics: BTreeMap<String, f64>,
    failures: Vec<String>,
    diagnosis: serde_json::Value,
    seed: Option<u64>,
}

impl Report {
    pub fn new(scenario: &'static str) -> Self {
        eprintln!("stress {scenario}: starting");
        Self {
            scenario,
            started: Instant::now(),
            metrics: BTreeMap::new(),
            failures: Vec::new(),
            diagnosis: serde_json::Value::Null,
            seed: None,
        }
    }

    pub fn set(&mut self, name: &str, value: f64) {
        self.metrics.insert(name.to_owned(), value);
    }

    /// `name` must be at most `limit` (a generous floor: a gross regression only).
    pub fn at_most(&mut self, name: &str, value: f64, limit: f64) {
        self.set(name, value);
        if value > limit {
            self.failures
                .push(format!("{name} = {value:.3}, over its limit {limit}"));
        }
    }

    /// `name` must be at least `floor`.
    pub fn at_least(&mut self, name: &str, value: f64, floor: f64) {
        self.set(name, value);
        if value < floor {
            self.failures
                .push(format!("{name} = {value:.3}, under its floor {floor}"));
        }
    }

    /// Adds what is wrong (invariants broken), each a failure.
    pub fn broken(&mut self, what: Vec<String>) {
        self.failures.extend(what);
    }

    /// What the database says of how the scenario went (see `Db::diagnosis`), for the report.
    pub fn diagnose(&mut self, diagnosis: serde_json::Value) {
        self.diagnosis = diagnosis;
    }

    /// Whether anything failed so far.
    pub fn failed(&self) -> bool {
        !self.failures.is_empty()
    }

    /// The scenario's own directory in `target/stress/` (`<scenario>/`), made empty: its
    /// bundle, kept by CI with the report.
    pub fn bundle_dir(&self) -> PathBuf {
        let dir = report_dir().join(self.scenario);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Notes `seed` in the report: the random schedule's, to run it again.
    pub fn seed(&mut self, seed: u64) {
        self.seed = Some(seed);
    }

    /// Writes the report and fails the scenario on any failure, with `logs` for context.
    pub fn finish(mut self, logs: impl FnOnce() -> String) {
        self.set("wall_s", self.started.elapsed().as_secs_f64());
        let dir = report_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let report = serde_json::json!({
            "scenario": self.scenario,
            "profile": crate::load::Size::get().describe(),
            "metrics": self.metrics,
            "failures": self.failures,
            "diagnosis": self.diagnosis,
            "seed": self.seed,
        });
        std::fs::write(
            dir.join(format!("{}.json", self.scenario)),
            serde_json::to_string_pretty(&report).unwrap(),
        )
        .unwrap();
        eprintln!(
            "stress {}: {}",
            self.scenario,
            serde_json::to_string(&report).unwrap()
        );
        assert!(
            self.failures.is_empty(),
            "stress {} failed:\n  {}\n{}",
            self.scenario,
            self.failures.join("\n  "),
            logs()
        );
    }
}

/// `target/stress/` of the workspace.
fn report_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/stress")
}
