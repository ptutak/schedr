//! Samples of each schedr process (decision 464): one a minute (step 60), and 15-minute
//! averages and peaks of them (step 900), for the Metrics page's charts.

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_model::usage::{NodeUsage, WorkerParts, WorkerUsage};

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, parse_time, sql, sql_text, to_json};

#[cfg(test)]
mod tests;

/// A minute sample's step, in seconds.
pub const MINUTE: u32 = 60;

/// A rollup's step, in seconds (15 minutes).
pub const ROLLUP: u32 = 900;

/// The rows one insert writes at most: 13 parameters each, under both databases' limits.
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
    /// A worker's memory by part and its largest tasks (minute samples).
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

fn int(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

type Row = (
    String,
    i64,
    String,
    String,
    i64,
    i64,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

fn sample(row: Row) -> Result<Sample, DbError> {
    let (process_id, step, at, roles, resident_avg, resident_peak, cpu_seconds) =
        (row.0, row.1, row.2, row.3, row.4, row.5, row.6);
    Ok(Sample {
        process_id,
        step_s: u32::try_from(step).unwrap_or(0),
        at: parse_time("samples.at", &at)?,
        roles: from_json("samples.roles", &roles)?,
        resident_avg: unsigned(resident_avg),
        resident_peak: unsigned(resident_peak),
        cpu_seconds,
        cpu_avg: row.7,
        cpu_peak: row.8,
        container_avg: row.9.map(unsigned),
        container_peak: row.10.map(unsigned),
        container_limit: row.11.map(unsigned),
        parts: row
            .12
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
            sqlx::query_as(sql(backend, MASTERS))
                .bind(db_time(masters_since))
                .fetch_all(pool)
                .await?
        });
        let workers: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, WORKERS))
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
        for chunk in samples.chunks(ROWS_PER_INSERT) {
            let mut query = String::from(
                "INSERT INTO samples (process_id, step_s, at, roles, resident_avg, \
                 resident_peak, cpu_seconds, cpu_avg, cpu_peak, container_avg, container_peak, \
                 container_limit, parts) VALUES ",
            );
            for row in 0..chunk.len() {
                let n = row * 13;
                if row > 0 {
                    query.push_str(", ");
                }
                // Each row's 13 parameters, the time and the parts cast for Postgres.
                let cell = |k: usize| match k {
                    3 => format!("${}{{ts}}", n + k),
                    13 => format!("${}{{json}}", n + k),
                    _ => format!("${}", n + k),
                };
                query.push('(');
                query.push_str(&(1..=13).map(cell).collect::<Vec<_>>().join(", "));
                query.push(')');
            }
            query.push_str(" ON CONFLICT (process_id, step_s, at) DO NOTHING");
            in_tx!(self, |tx, backend| {
                let text = sql_text(backend, &query);
                let mut insert = sqlx::query(text);
                for sample in chunk {
                    insert = insert
                        .bind(sample.process_id.clone())
                        .bind(i64::from(sample.step_s))
                        .bind(db_time(sample.at))
                        .bind(to_json(&sample.roles))
                        .bind(int(sample.resident_avg))
                        .bind(int(sample.resident_peak))
                        .bind(sample.cpu_seconds)
                        .bind(sample.cpu_avg)
                        .bind(sample.cpu_peak)
                        .bind(sample.container_avg.map(int))
                        .bind(sample.container_peak.map(int))
                        .bind(sample.container_limit.map(int))
                        .bind(sample.parts.as_ref().map(to_json));
                }
                insert.execute(&mut *tx).await?;
            });
        }
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
            sqlx::query_scalar(sql(backend, OLDEST))
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
            sqlx::query(sql(backend, DELETE))
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
        const SAMPLES: &str = "SELECT process_id, CAST(step_s AS BIGINT), {ts<}at{>ts}, roles, \
            resident_avg, \
            resident_peak, cpu_seconds, cpu_avg, cpu_peak, container_avg, container_peak, \
            container_limit, parts{text} FROM samples \
            WHERE step_s = $1 AND at >= $2{ts} AND at < $3{ts} ORDER BY process_id, at";
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, SAMPLES))
                .bind(i64::from(step_s))
                .bind(db_time(from))
                .bind(db_time(to))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(sample).collect()
    }
}
