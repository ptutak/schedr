//! Samples of each schedr process (decision 464): one a minute (step 60), and 15-minute
//! averages and peaks of them (step 900), for the Metrics page's charts.

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_model::usage::{NodeUsage, WorkerParts, WorkerUsage};

use crate::backend::Backend;
use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, parse_time, sql, sql_text, to_json};

#[cfg(test)]
mod tests;

/// A minute sample's step, in seconds.
pub const MINUTE: u32 = 60;

/// A rollup's step, in seconds (15 minutes).
pub const ROLLUP: u32 = 900;

/// The rows one insert writes at most: 22 parameters each, under both databases' limits.
const ROWS_PER_INSERT: usize = 500;

/// A process's sample, as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// The process.
    pub process_id: String,
    /// [`MINUTE`] or [`ROLLUP`].
    pub step_s: u32,
    /// Its minute, or its window's start.
    pub at: Timestamp,
    /// What it runs (`master`, `worker`, `webserver`), each with its node's ID.
    pub roles: BTreeMap<String, String>,
    /// Resident memory, in bytes: the minute's, or the window's average.
    pub resident_avg: u64,
    /// The highest (a minute's: the same).
    pub resident_peak: u64,
    /// Its whole memory, in bytes: itself and, for a worker, its fork servers, wait step hosts
    /// and running tasks; the minute's, or the window's average.
    pub memory_avg: u64,
    /// The highest.
    pub memory_peak: u64,
    /// The minutes it covers: 1, or a rollup's minute samples (its averages' weight).
    pub minutes: u32,
    /// Of those, the ones with a CPU figure (the CPU average's weight).
    pub cpu_minutes: u32,
    /// Of those, the ones with a container figure (the container average's weight).
    pub container_minutes: u32,
    /// The minutes of its window in which any process was sampled: 1 for a minute.
    pub covered: u32,
    /// CPU time since the process started, in seconds (minute samples).
    pub cpu_seconds: Option<f64>,
    /// Cores used since the sample before, or the window's average; none for its first.
    pub cpu_avg: Option<f64>,
    /// The highest minute's.
    pub cpu_peak: Option<f64>,
    /// Its container's memory, average; none outside a cgroup v2.
    pub container_avg: Option<u64>,
    /// The highest.
    pub container_peak: Option<u64>,
    /// Its container's limit; none when unlimited.
    pub container_limit: Option<u64>,
    /// A worker's memory by part and its largest tasks (a rollup: the parts' averages only).
    pub parts: Option<WorkerParts>,
}

/// What the leader samples a process from: a master's or a worker's stored usage.
#[derive(Debug, Clone, PartialEq)]
pub struct Source {
    /// Its process.
    pub process_id: String,
    /// `master` or `worker`.
    pub role: &'static str,
    /// The master's or the worker's ID.
    pub node_id: String,
    /// Its process's figures.
    pub usage: NodeUsage,
    /// A worker's parts.
    pub parts: Option<WorkerParts>,
    /// When the figures were stored: a master's row heard, a worker's usage written.
    pub at: Timestamp,
}

/// The largest byte count stored (4 PiB): a report beyond it is capped, so that the weighted
/// sums in SQL stay in range (Codex's review of M5.1j).
const MOST_BYTES: u64 = 1 << 52;

fn int(value: u64) -> i64 {
    i64::try_from(value.min(MOST_BYTES)).unwrap_or(i64::MAX)
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[derive(sqlx::FromRow)]
struct Row {
    process_id: String,
    step_s: i64,
    at: String,
    roles: String,
    resident_avg: i64,
    resident_peak: i64,
    memory_avg: i64,
    memory_peak: i64,
    minutes: i64,
    cpu_minutes: i64,
    container_minutes: i64,
    covered: i64,
    cpu_seconds: Option<f64>,
    cpu_avg: Option<f64>,
    cpu_peak: Option<f64>,
    container_avg: Option<i64>,
    container_peak: Option<i64>,
    container_limit: Option<i64>,
    parts: Option<String>,
}

fn sample(row: Row) -> Result<Sample, DbError> {
    let count = |n: i64| u32::try_from(n).unwrap_or(0);
    Ok(Sample {
        process_id: row.process_id,
        step_s: count(row.step_s),
        at: parse_time("samples.at", &row.at)?,
        roles: from_json("samples.roles", &row.roles)?,
        resident_avg: unsigned(row.resident_avg),
        resident_peak: unsigned(row.resident_peak),
        memory_avg: unsigned(row.memory_avg),
        memory_peak: unsigned(row.memory_peak),
        minutes: count(row.minutes),
        cpu_minutes: count(row.cpu_minutes),
        container_minutes: count(row.container_minutes),
        covered: count(row.covered),
        cpu_seconds: row.cpu_seconds,
        cpu_avg: row.cpu_avg,
        cpu_peak: row.cpu_peak,
        container_avg: row.container_avg.map(unsigned),
        container_peak: row.container_peak.map(unsigned),
        container_limit: row.container_limit.map(unsigned),
        parts: row
            .parts
            .map(|parts| from_json("samples.parts", &parts))
            .transpose()?,
    })
}

impl Database {
    /// What the leader samples (decision 464): the masters heard from since `masters_since`
    /// and the active or draining workers heard from since `workers_since`, each with the
    /// usage it last stored; none without one.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or stored data doesn't read.
    pub async fn sample_sources(
        &self,
        masters_since: Timestamp,
        workers_since: Timestamp,
    ) -> Result<Vec<Source>, DbError> {
        const MASTERS: &str = "SELECT process_id, master_id, usage{text}, \
            {ts<}heard_at{>ts} FROM masters \
            WHERE heard_at >= $1{ts} AND usage IS NOT NULL ORDER BY master_id";
        const WORKERS: &str = "SELECT process_id, worker_id, usage{text}, \
            {ts<}usage_at{>ts} FROM workers \
            WHERE state IN ('active', 'draining') AND last_heartbeat_at >= $1{ts} \
            AND usage IS NOT NULL AND usage_at IS NOT NULL AND process_id IS NOT NULL \
            ORDER BY worker_id";
        type Row = (String, String, String, String);
        let masters: Vec<Row> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, MASTERS))
                .bind(db_time(masters_since))
                .fetch_all(pool)
                .await?
        });
        let workers: Vec<Row> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, WORKERS))
                .bind(db_time(workers_since))
                .fetch_all(pool)
                .await?
        });
        let mut sources = Vec::with_capacity(masters.len() + workers.len());
        for (process_id, node_id, usage, at) in masters {
            sources.push(Source {
                process_id,
                role: "master",
                node_id,
                usage: from_json("masters.usage", &usage)?,
                parts: None,
                at: parse_time("masters.heard_at", &at)?,
            });
        }
        for (process_id, node_id, usage, at) in workers {
            let usage: WorkerUsage = from_json("workers.usage", &usage)?;
            sources.push(Source {
                process_id,
                role: "worker",
                node_id,
                parts: Some(usage.parts()),
                usage: usage.process,
                at: parse_time("workers.usage_at", &at)?,
            });
        }
        Ok(sources)
    }

    /// Stores `samples`; one already stored for its process, step and time is kept.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn insert_samples(&self, samples: &[Sample]) -> Result<(), DbError> {
        if samples.is_empty() {
            return Ok(());
        }
        // Every chunk in one transaction: a rollup is written whole or not at all (Codex's
        // review of M5.1j: a chunk failing after another committed moved the rollups' end past
        // the processes it left out).
        let queries: Vec<(String, &[Sample])> = samples
            .chunks(ROWS_PER_INSERT)
            .map(|chunk| (insert_query(chunk.len()), chunk))
            .collect();
        in_tx!(self, |tx, backend| {
            for (query, chunk) in &queries {
                let text = sql_text(backend, query);
                let mut insert = crate::query::query(text);
                for sample in *chunk {
                    insert = insert
                        .bind(sample.process_id.clone())
                        .bind(i64::from(sample.step_s))
                        .bind(db_time(sample.at))
                        .bind(to_json(&sample.roles))
                        .bind(int(sample.resident_avg))
                        .bind(int(sample.resident_peak))
                        .bind(int(sample.memory_avg))
                        .bind(int(sample.memory_peak))
                        .bind(i64::from(sample.minutes))
                        .bind(i64::from(sample.cpu_minutes))
                        .bind(i64::from(sample.container_minutes))
                        .bind(i64::from(sample.covered))
                        .bind(sample.cpu_seconds)
                        .bind(sample.cpu_avg)
                        .bind(sample.cpu_peak)
                        .bind(sample.container_avg.map(int))
                        .bind(sample.container_peak.map(int))
                        .bind(sample.container_limit.map(int))
                        .bind(sample.parts.as_ref().map(to_json))
                        .bind(sample.parts.as_ref().map(|p| int(p.fork_servers_bytes)))
                        .bind(sample.parts.as_ref().map(|p| int(p.watcher_hosts_bytes)))
                        .bind(sample.parts.as_ref().map(|p| int(p.tasks_bytes)));
                }
                insert.execute(&mut *tx).await?;
            }
        });
        Ok(())
    }

    /// The oldest minute sample's time; none without one.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the time doesn't read.
    pub async fn oldest_minute_sample(&self) -> Result<Option<Timestamp>, DbError> {
        const OLDEST: &str = "SELECT {ts<}MIN(at){>ts} FROM samples WHERE step_s = $1";
        let oldest: Option<String> = on_pool!(self, |pool, backend| {
            crate::query::query_scalar(sql(backend, OLDEST))
                .bind(i64::from(MINUTE))
                .fetch_one(pool)
                .await?
        });
        oldest.map(|at| parse_time("samples.at", &at)).transpose()
    }

    /// Deletes the samples of step `step_s` older than `before`; how many.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn delete_old_samples(&self, step_s: u32, before: Timestamp) -> Result<u64, DbError> {
        const DELETE: &str = "DELETE FROM samples WHERE step_s = $1 AND at < $2{ts}";
        let deleted = in_tx!(self, |tx, backend| {
            crate::query::query(sql(backend, DELETE))
                .bind(i64::from(step_s))
                .bind(db_time(before))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(deleted)
    }

    /// The samples of step `step_s` in `[from, to)`, by process, then time.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn samples(
        &self,
        step_s: u32,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<Sample>, DbError> {
        self.samples_of(step_s, from, to, None).await
    }

    /// As [`Self::samples`], of one process only when `process` names it.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn samples_of(
        &self,
        step_s: u32,
        from: Timestamp,
        to: Timestamp,
        process: Option<&str>,
    ) -> Result<Vec<Sample>, DbError> {
        const SAMPLES: &str = "SELECT process_id, CAST(step_s AS BIGINT) AS step_s, \
            {ts<}at{>ts} AS at, roles, resident_avg, resident_peak, memory_avg, memory_peak, \
            CAST(minutes AS BIGINT) AS minutes, CAST(cpu_minutes AS BIGINT) AS cpu_minutes, \
            CAST(container_minutes AS BIGINT) AS container_minutes, \
            CAST(covered AS BIGINT) AS covered, cpu_seconds, cpu_avg, cpu_peak, container_avg, \
            container_peak, container_limit, parts{text} AS parts FROM samples \
            WHERE step_s = $1 AND at >= $2{ts} AND at < $3{ts} \
            AND ($4{text} IS NULL OR process_id = $4{text}) ORDER BY process_id, at";
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql(backend, SAMPLES))
                .bind(i64::from(step_s))
                .bind(db_time(from))
                .bind(db_time(to))
                .bind(process)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(sample).collect()
    }

    /// Each process's samples in `[from, to)` in buckets of `bucket_s` seconds (M5.1j): the
    /// rollups before `cut`, the minutes from it (those not rolled up yet; `cut` = `from` for
    /// minutes only), of one process when `process` names it. Averages are weighted by the
    /// minutes each row covers; peaks are the highest; the limit the highest in the bucket.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn sample_buckets(
        &self,
        (from, cut, to): (Timestamp, Timestamp, Timestamp),
        bucket_s: u32,
        process: Option<&str>,
    ) -> Result<Vec<Bucket>, DbError> {
        type BucketRow = (
            String,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<f64>,
            Option<f64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            i64,
        );
        let rows: Vec<BucketRow> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql_text(backend, &bucket_query(backend)))
                .bind(db_time(from))
                .bind(db_time(within(from, cut, to)))
                .bind(db_time(to))
                .bind(i64::from(bucket_s.max(1)))
                .bind(process)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|row| {
                Ok(Bucket {
                    process_id: row.0,
                    at: from_epoch(row.1)?,
                    minutes: u32::try_from(row.2).unwrap_or(0),
                    resident_avg: unsigned(row.3),
                    resident_peak: unsigned(row.4),
                    memory_avg: unsigned(row.5),
                    memory_peak: unsigned(row.6),
                    cpu_avg: row.7,
                    cpu_peak: row.8,
                    container_avg: row.9.map(unsigned),
                    container_peak: row.10.map(unsigned),
                    container_limit: row.11.map(unsigned),
                    parts: row
                        .12
                        .zip(row.13)
                        .zip(row.14)
                        .map(|((forks, hosts), tasks)| {
                            (unsigned(forks), unsigned(hosts), unsigned(tasks))
                        }),
                    covered: u32::try_from(row.15).unwrap_or(0),
                })
            })
            .collect()
    }

    /// Each process sampled in `[from, to)` as its latest sample there says: its roles and its
    /// container's limit (none: unlimited).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn latest_of(
        &self,
        from: Timestamp,
        to: Timestamp,
        process: Option<&str>,
    ) -> Result<BTreeMap<String, Latest>, DbError> {
        // The steps named, so that each side reads an index: the range by `samples_by_time`,
        // each latest row by the primary key; `CROSS JOIN` keeps that order on SQLite (its
        // planner otherwise read every row of each process: 150 ms for a few minutes of 100).
        const LATEST: &str = "SELECT s.process_id, s.roles, s.container_limit, \
            CAST(s.step_s AS BIGINT), {ts<}s.at{>ts} FROM \
            (SELECT process_id, MAX(at) AS last FROM samples \
             WHERE step_s IN (60, 900) AND at >= $1{ts} AND at < $2{ts} \
             AND ($3{text} IS NULL OR process_id = $3{text}) GROUP BY process_id) l \
            CROSS JOIN samples s \
            WHERE s.process_id = l.process_id AND s.step_s IN (60, 900) AND s.at = l.last";
        let rows: Vec<(String, String, Option<i64>, i64, String)> =
            on_pool!(self, |pool, backend| {
                crate::query::query_as(sql(backend, LATEST))
                    .bind(db_time(from))
                    .bind(db_time(to))
                    .bind(process)
                    .fetch_all(pool)
                    .await?
            });
        let mut latest = BTreeMap::new();
        // A minute and a rollup at the same time: the minute is the later.
        for (process_id, roles, limit, step, at) in rows {
            if step == i64::from(ROLLUP) && latest.contains_key(&process_id) {
                continue;
            }
            latest.insert(
                process_id,
                Latest {
                    at: parse_time("samples.at", &at)?,
                    roles: from_json("samples.roles", &roles)?,
                    container_limit: limit.map(unsigned),
                },
            );
        }
        Ok(latest)
    }

    /// The end of the newest rollup's window; none without one.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the time doesn't read.
    pub async fn rolled_up_to(&self) -> Result<Option<Timestamp>, DbError> {
        const NEWEST: &str = "SELECT {ts<}MAX(at){>ts} FROM samples WHERE step_s = $1";
        let newest: Option<String> = on_pool!(self, |pool, backend| {
            crate::query::query_scalar(sql(backend, NEWEST))
                .bind(i64::from(ROLLUP))
                .fetch_one(pool)
                .await?
        });
        newest
            .map(|at| {
                let at = parse_time("samples.at", &at)?;
                Ok(at
                    .checked_add(jiff::SignedDuration::from_secs(i64::from(ROLLUP)))
                    .unwrap_or(at))
            })
            .transpose()
    }

    /// Every process's whole memory together in buckets of `bucket_s` seconds over `[from,
    /// to)`, rollups before `cut` and minutes from it (the Deployments page's sparkline,
    /// M5.1j), oldest first: each process's memory weighted by the minutes it was sampled, over
    /// the minutes the deployment was sampled in the bucket, so a process replaced within a
    /// bucket isn't counted twice.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a time doesn't read.
    pub async fn memory_totals(
        &self,
        (from, cut, to): (Timestamp, Timestamp, Timestamp),
        bucket_s: u32,
    ) -> Result<Vec<(Timestamp, u64)>, DbError> {
        let rows: Vec<(i64, i64)> = on_pool!(self, |pool, backend| {
            crate::query::query_as(sql_text(backend, &totals_query(backend)))
                .bind(db_time(from))
                .bind(db_time(within(from, cut, to)))
                .bind(db_time(to))
                .bind(i64::from(bucket_s.max(1)))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(at, total)| Ok((from_epoch(at)?, unsigned(total))))
            .collect()
    }
}

/// A process's samples in a bucket (see [`Database::sample_buckets`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    /// The process.
    pub process_id: String,
    /// The bucket's start.
    pub at: Timestamp,
    /// The minutes sampled in it.
    pub minutes: u32,
    /// Resident memory, the minutes' average.
    pub resident_avg: u64,
    /// The highest.
    pub resident_peak: u64,
    /// Whole memory, the minutes' average.
    pub memory_avg: u64,
    /// The highest.
    pub memory_peak: u64,
    /// Cores, the average over the minutes that have one.
    pub cpu_avg: Option<f64>,
    /// The highest.
    pub cpu_peak: Option<f64>,
    /// Its container's memory, the average over the minutes that have one.
    pub container_avg: Option<u64>,
    /// The highest.
    pub container_peak: Option<u64>,
    /// Its container's highest limit in the bucket.
    pub container_limit: Option<u64>,
    /// A worker's fork servers', wait step hosts' and tasks' averages over all the bucket's
    /// minutes (0 where it ran no worker); none when it ran none in the bucket.
    pub parts: Option<(u64, u64, u64)>,
    /// The minutes the deployment (any process) was sampled in the bucket, read with it: a
    /// total's denominator.
    pub covered: u32,
}

/// An insert of `rows` samples, the ones already stored kept.
fn insert_query(rows: usize) -> String {
    let mut query = String::from(
        "INSERT INTO samples (process_id, step_s, at, roles, resident_avg, resident_peak, \
         memory_avg, memory_peak, minutes, cpu_minutes, container_minutes, covered, cpu_seconds, \
         cpu_avg, cpu_peak, container_avg, container_peak, container_limit, parts, forks_avg, \
         hosts_avg, tasks_avg) VALUES ",
    );
    for row in 0..rows {
        let n = row * 22;
        if row > 0 {
            query.push_str(", ");
        }
        // Each row's 22 parameters, the time and the parts cast for Postgres.
        let cell = |k: usize| match k {
            3 => format!("${}{{ts}}", n + k),
            19 => format!("${}{{json}}", n + k),
            _ => format!("${}", n + k),
        };
        query.push('(');
        query.push_str(&(1..=22).map(cell).collect::<Vec<_>>().join(", "));
        query.push(')');
    }
    query.push_str(" ON CONFLICT (process_id, step_s, at) DO NOTHING");
    query
}

/// A process as its latest sample says (see [`Database::latest_of`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Latest {
    /// The sample's time.
    pub at: Timestamp,
    /// What it runs, each role with its node's ID.
    pub roles: BTreeMap<String, String>,
    /// Its container's limit; none when unlimited.
    pub container_limit: Option<u64>,
}

/// A bucket's time read back.
fn from_epoch(seconds: i64) -> Result<Timestamp, DbError> {
    Timestamp::from_second(seconds).map_err(|e| DbError::InvalidData {
        what: "samples.at",
        reason: e.to_string(),
    })
}

/// A row's time in seconds since the epoch, on `backend`.
const fn epoch(backend: Backend) -> &'static str {
    match backend {
        Backend::Sqlite => "CAST(strftime('%s', at) AS INTEGER)",
        Backend::Postgres => "CAST(EXTRACT(EPOCH FROM at) AS BIGINT)",
    }
}

/// The rows of `[from, to)` (`$1`, `$3`): rollups before `$2`, minutes from it; `$2` within
/// them ([`within`]), so that each side reads only its own rows of `samples_by_time` (a cut a
/// day before the start had SQLite read a day of minutes for a few).
const RANGE: &str = "((step_s = 900 AND at >= $1{ts} AND at < $2{ts}) \
    OR (step_s = 60 AND at >= $2{ts} AND at < $3{ts}))";

/// `cut` within `[from, to]`.
fn within(from: Timestamp, cut: Timestamp, to: Timestamp) -> Timestamp {
    cut.max(from).min(to.max(from))
}

/// `expr` rounded half away from zero on both backends (Postgres rounds a `double precision`
/// half to even; through `NUMERIC` it doesn't).
fn round(backend: Backend, expr: &str) -> String {
    match backend {
        Backend::Sqlite => format!("ROUND({expr})"),
        Backend::Postgres => format!("ROUND(CAST({expr} AS NUMERIC))"),
    }
}

/// `x` weighted by `weight`, summed in floating point (no overflow).
fn weighted(x: &str, weight: &str) -> String {
    format!("SUM(CAST({x} AS DOUBLE PRECISION) * {weight})")
}

/// [`Database::sample_buckets`]'s query on `backend`.
fn bucket_query(backend: Backend) -> String {
    // An average of `x` weighted by `weight`, rounded; none without a weight.
    let avg = |x: &str, weight: &str| {
        format!(
            "CAST({} AS BIGINT)",
            round(
                backend,
                &format!("{} / NULLIF(SUM({weight}), 0)", weighted(x, weight))
            )
        )
    };
    // A part's average over all the minutes, 0 where the process ran no worker.
    let part = |x: &str| {
        format!(
            "CASE WHEN COUNT({x}) = 0 THEN NULL ELSE CAST({} AS BIGINT) END",
            round(
                backend,
                &format!(
                    "{} / SUM(minutes)",
                    weighted(&format!("COALESCE({x}, 0)"), "minutes")
                )
            )
        )
    };
    // With the deployment's coverage of each bucket, in the same statement: one snapshot, so
    // that a sample stored meanwhile doesn't count in one and not the other (Codex's review of
    // M5.1j).
    format!(
        "SELECT p.*, CAST(c.sampled AS BIGINT) FROM \
         (SELECT process_id, ({e} / $4) * $4 AS bucket, CAST(SUM(minutes) AS BIGINT) AS m, \
          {res} AS r, MAX(resident_peak) AS rp, {mem} AS mm, MAX(memory_peak) AS mp, \
          {cpu} / NULLIF(SUM(cpu_minutes), 0) AS ca, MAX(cpu_peak) AS cp, {cont} AS ka, \
          MAX(container_peak) AS kp, MAX(container_limit) AS kl, {forks} AS fa, {hosts} AS ha, \
          {tasks} AS ta FROM samples WHERE {RANGE} \
          AND ($5{{text}} IS NULL OR process_id = $5{{text}}) GROUP BY process_id, bucket) p \
         JOIN ({c}) c ON c.bucket = p.bucket ORDER BY p.process_id, p.bucket",
        c = coverage_rows(backend),
        e = epoch(backend),
        res = avg("resident_avg", "minutes"),
        mem = avg("memory_avg", "minutes"),
        cpu = weighted("cpu_avg", "cpu_minutes"),
        cont = avg("container_avg", "container_minutes"),
        forks = part("forks_avg"),
        hosts = part("hosts_avg"),
        tasks = part("tasks_avg"),
    )
}

/// The minutes sampled in each bucket: each window's (or minute's) coverage once, summed.
fn coverage_rows(backend: Backend) -> String {
    format!(
        "SELECT bucket, SUM(covered) AS sampled FROM \
         (SELECT ({e} / $4) * $4 AS bucket, at, MAX(covered) AS covered FROM samples \
          WHERE {RANGE} GROUP BY bucket, at) w GROUP BY bucket",
        e = epoch(backend),
    )
}

/// [`Database::memory_totals`]'s query on `backend`.
fn totals_query(backend: Backend) -> String {
    format!(
        "SELECT m.bucket, CAST({r} AS BIGINT) FROM \
         (SELECT ({e} / $4) * $4 AS bucket, {w} AS weighted FROM samples WHERE {RANGE} \
          GROUP BY bucket) m JOIN ({c}) c ON c.bucket = m.bucket ORDER BY m.bucket",
        e = epoch(backend),
        w = weighted("memory_avg", "minutes"),
        c = coverage_rows(backend),
        r = round(backend, "m.weighted / NULLIF(c.sampled, 0)"),
    )
}
