//! The Metrics page's throughput and start latency (decision 494), read from the attempts
//! table: how many attempts ended in each bucket, by how they ended, and how long the ones
//! started in it took from their assignment to their start (`started_at`, the worker's start
//! in master time, decision 490), as p50 and p95. Each read is one statement over an index on
//! the time it buckets by.

use jiff::Timestamp;

use crate::backend::Backend;
use crate::database::{Database, DbError};
use crate::query::{db_time, on_pool, sql_text};

#[cfg(test)]
mod tests;

/// Attempts that ended in a bucket, by their status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedCount {
    /// The bucket's start.
    pub at: Timestamp,
    /// How the attempts ended (`success`, `failed`, `timed_out`, ...).
    pub status: String,
    /// How many.
    pub count: u64,
}

/// The start latency of the attempts started in a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartLatency {
    /// The bucket's start.
    pub at: Timestamp,
    /// How many started.
    pub started: u64,
    /// The median, in milliseconds.
    pub p50_ms: u64,
    /// The 95th percentile, in milliseconds.
    pub p95_ms: u64,
}

/// A row's `column` in whole seconds since the epoch, rounded down, on `backend`: a time in a
/// bucket's last half-second stays in it (Grok's review of M5.1l2: Postgres's cast of the
/// `numeric` epoch rounded it into the next one, and SQLite's `strftime` rounds at the
/// millisecond). SQLite reads the stored text's whole seconds, `YYYY-MM-DDTHH:MM:SS`.
fn epoch(backend: Backend, column: &str) -> String {
    match backend {
        Backend::Sqlite => format!("CAST(strftime('%s', substr({column}, 1, 19)) AS INTEGER)"),
        Backend::Postgres => format!("CAST(FLOOR(EXTRACT(EPOCH FROM {column})) AS BIGINT)"),
    }
}

/// How long from `assigned_at` to `started_at`, in whole milliseconds, never below zero (a
/// worker's master time is read from its clock's samples, decision 447).
fn latency_ms(backend: Backend) -> &'static str {
    match backend {
        Backend::Sqlite => {
            "MAX(0, CAST(ROUND((julianday(started_at) - julianday(assigned_at)) * 86400000) \
             AS INTEGER))"
        }
        Backend::Postgres => {
            "GREATEST(0, CAST(ROUND(EXTRACT(EPOCH FROM (started_at - assigned_at)) * 1000) \
             AS BIGINT))"
        }
    }
}

/// `seconds` since the epoch as a timestamp.
fn from_epoch(seconds: i64) -> Result<Timestamp, DbError> {
    Timestamp::from_second(seconds).map_err(|e| DbError::InvalidData {
        what: "attempts' bucket",
        reason: e.to_string(),
    })
}

/// The attempts ended in `[$1, $2)` by bucket of `$3` seconds and status.
fn ended_query(backend: Backend) -> String {
    format!(
        "SELECT ({e} / $3) * $3 AS bucket, status, COUNT(*) FROM attempts \
         WHERE ended_at >= $1{{ts}} AND ended_at < $2{{ts}} \
         GROUP BY bucket, status ORDER BY bucket, status",
        e = epoch(backend, "ended_at"),
    )
}

/// The start latency's p50 and p95 of the attempts started in `[$1, $2)`, by bucket of `$3`
/// seconds: the nearest rank, the `ceil(n * q)`th of a bucket's `n`, in whole numbers.
fn latency_query(backend: Backend) -> String {
    format!(
        "SELECT bucket, n, \
         MAX(CASE WHEN k = (n * 50 + 99) / 100 THEN ms END), \
         MAX(CASE WHEN k = (n * 95 + 99) / 100 THEN ms END) FROM \
         (SELECT bucket, ms, ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY ms) AS k, \
          COUNT(*) OVER (PARTITION BY bucket) AS n FROM \
          (SELECT ({e} / $3) * $3 AS bucket, {ms} AS ms FROM attempts \
           WHERE started_at >= $1{{ts}} AND started_at < $2{{ts}}) started) ranked \
         WHERE k = (n * 50 + 99) / 100 OR k = (n * 95 + 99) / 100 \
         GROUP BY bucket, n ORDER BY bucket",
        e = epoch(backend, "started_at"),
        ms = latency_ms(backend),
    )
}

impl Database {
    /// The attempts that ended in `[from, to)`, counted by bucket of `bucket_s` seconds and by
    /// status, oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn attempts_ended(
        &self,
        (from, to): (Timestamp, Timestamp),
        bucket_s: u32,
    ) -> Result<Vec<EndedCount>, DbError> {
        let rows: Vec<(i64, String, i64)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &ended_query(backend)))
                .bind(db_time(from))
                .bind(db_time(to))
                .bind(i64::from(bucket_s.max(1)))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(bucket, status, count)| {
                Ok(EndedCount {
                    at: from_epoch(bucket)?,
                    status,
                    count: u64::try_from(count).unwrap_or(0),
                })
            })
            .collect()
    }

    /// The start latency of the attempts started in `[from, to)`, by bucket of `bucket_s`
    /// seconds, oldest first: each bucket's p50 and p95 (the nearest rank), exact.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn start_latency(
        &self,
        (from, to): (Timestamp, Timestamp),
        bucket_s: u32,
    ) -> Result<Vec<StartLatency>, DbError> {
        let rows: Vec<(i64, i64, Option<i64>, Option<i64>)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &latency_query(backend)))
                .bind(db_time(from))
                .bind(db_time(to))
                .bind(i64::from(bucket_s.max(1)))
                .fetch_all(pool)
                .await?
        });
        let ms = |value: Option<i64>| value.and_then(|v| u64::try_from(v).ok()).unwrap_or(0);
        rows.into_iter()
            .map(|(bucket, started, p50, p95)| {
                Ok(StartLatency {
                    at: from_epoch(bucket)?,
                    started: u64::try_from(started).unwrap_or(0),
                    p50_ms: ms(p50),
                    p95_ms: ms(p95),
                })
            })
            .collect()
    }
}
