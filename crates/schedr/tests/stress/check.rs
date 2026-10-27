//! What every scenario checks at its end, and what it reports.
//!
//! **Invariants** (always a failure, see PROJECT.md, stress suite): nothing still running, no
//! task succeeding twice, no command undecided, no run in a state the scenario doesn't
//! expect, no try of a task starting before the previous one ended, by the database and by
//! the tasks' own marks (each task appends a `start` and an `end` line to a shared file).
//! **Performance** only fails on a gross regression (a generous floor per scenario); the
//! numbers are written to `target/stress/<scenario>.json`.

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
                "SELECT COUNT(*) FROM task_instances ti WHERE ti.state = 'success' AND NOT \
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
/// mix).
pub fn marked(marks: &Path, work: &str) -> String {
    let mark = |what: &str| {
        format!(
            "echo {what} $SCHEDR_RUN_ID $SCHEDR_TASK_ID $SCHEDR_TRY_NUMBER $$ $(date +%s%N) >> {}",
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
}

/// Tries of the same task that ran at the same time, by the tasks' own marks; `killed` holds
/// the processes the harness killed and when.
pub fn overlapping_tries(db: &Db, marks: &Path, killed: &HashMap<u32, u128>) -> Vec<String> {
    let text = std::fs::read_to_string(marks).unwrap_or_default();
    let ended = db.ended();
    let mut tries: BTreeMap<(String, String, u32), Try> = BTreeMap::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [what, run, task, number, pid, ns] = fields[..] else {
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
        let entry = tries.entry(key.clone()).or_insert_with(|| Try {
            task: (run.to_owned(), task.to_owned()),
            number,
            start: ns,
            end: killed.get(&pid).copied(),
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
    let mut overlaps = Vec::new();
    for (task, mut list) in by_task {
        list.sort_by_key(|t| t.start);
        for pair in list.windows(2) {
            let (earlier, later) = (pair[0], pair[1]);
            match earlier.end {
                Some(end) if end <= later.start => {}
                end => overlaps.push(format!(
                    "{}/{}: try {} started while try {} still ran (ended {end:?}, started {})",
                    task.0, task.1, later.number, earlier.number, later.start
                )),
            }
        }
    }
    overlaps
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
