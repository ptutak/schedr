//! The worker registry: every worker the master has seen, with its state, kept after it is
//! gone (offline or lost) so its logs can still be found (see PROJECT.md, Worker lifecycle).
//!
//! The master is its only writer: on `Register`, at most once per heartbeat interval while the
//! worker is connected, and on every change of state. At start it reads the registry back and
//! counts every worker's lease from then (master downtime doesn't count against workers).

use jiff::Timestamp;
use schedr_model::WorkerState;

use crate::database::{Database, DbError};
use crate::query::{
    db_time, from_json, from_name, on_pool, optional_time, parse_time, sql, to_json,
};

#[cfg(test)]
mod tests;

/// What a worker reports when it registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRegistration<'a> {
    /// Its ID.
    pub id: &'a str,
    /// The incarnation of its agent process.
    pub incarnation: &'a str,
    /// Its protocol version (`major.minor`).
    pub protocol_version: &'a str,
    /// Its schedr version.
    pub schedr_version: &'a str,
    /// The queues it serves (none: `default`).
    pub queues: &'a [String],
    /// Its task slots.
    pub slots: u32,
    /// Where its log endpoint listens (empty until it has one).
    pub log_endpoint: &'a str,
}

/// A worker in the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRecord {
    /// Its ID.
    pub id: String,
    /// The incarnation it last registered with.
    pub incarnation: String,
    /// Its state.
    pub state: WorkerState,
    /// Its protocol version.
    pub protocol_version: String,
    /// Its schedr version.
    pub schedr_version: String,
    /// The queues it serves.
    pub queues: Vec<String>,
    /// Its task slots.
    pub slots: u32,
    /// Where its log endpoint listens.
    pub log_endpoint: String,
    /// When it last registered.
    pub registered_at: Timestamp,
    /// When the master last recorded a sign of life.
    pub last_heartbeat_at: Timestamp,
    /// When it went offline or was lost.
    pub offline_at: Option<Timestamp>,
    /// Its attempts still running (no end recorded).
    pub running: u32,
    /// Whether it is drained for maintenance (no new tasks until resumed).
    pub drained: bool,
}

type WorkerRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    i32,
    String,
    String,
    String,
    Option<String>,
    i64,
    bool,
);

impl Database {
    /// Records a registration: the worker is active, with this incarnation and settings.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn register_worker(
        &self,
        worker: &WorkerRegistration<'_>,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const UPSERT: &str = "INSERT INTO workers (worker_id, incarnation_id, state, \
            protocol_version, schedr_version, queues, slots, log_endpoint, registered_at, \
            last_heartbeat_at, offline_at) \
            VALUES ($1, $2{uuid}, 'active', $3, $4, $5{json}, $6, $7, $8{ts}, $8{ts}, NULL) \
            ON CONFLICT (worker_id) DO UPDATE SET incarnation_id = excluded.incarnation_id, \
            state = 'active', protocol_version = excluded.protocol_version, \
            schedr_version = excluded.schedr_version, queues = excluded.queues, \
            slots = excluded.slots, log_endpoint = excluded.log_endpoint, \
            registered_at = excluded.registered_at, \
            last_heartbeat_at = excluded.last_heartbeat_at, offline_at = NULL";
        on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, UPSERT))
                .bind(worker.id)
                .bind(worker.incarnation)
                .bind(worker.protocol_version)
                .bind(worker.schedr_version)
                .bind(to_json(&worker.queues))
                .bind(i32::try_from(worker.slots).unwrap_or(i32::MAX))
                .bind(worker.log_endpoint)
                .bind(db_time(now))
                .execute(pool)
                .await?;
        });
        Ok(())
    }

    /// Records a sign of life from a worker.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn worker_seen(&self, worker_id: &str, now: Timestamp) -> Result<(), DbError> {
        const SEEN: &str = "UPDATE workers SET last_heartbeat_at = $1{ts} WHERE worker_id = $2";
        on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, SEEN))
                .bind(db_time(now))
                .bind(worker_id)
                .execute(pool)
                .await?;
        });
        Ok(())
    }

    /// Records a worker's new state; going offline or lost also records when.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_worker_state(
        &self,
        worker_id: &str,
        state: WorkerState,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const STATE: &str = "UPDATE workers SET state = $1, \
            offline_at = CASE WHEN $2 THEN $3{ts} ELSE offline_at END WHERE worker_id = $4";
        on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, STATE))
                .bind(state.to_string())
                .bind(state.is_gone())
                .bind(db_time(now))
                .bind(worker_id)
                .execute(pool)
                .await?;
        });
        Ok(())
    }

    /// Drains a worker for maintenance, or resumes it; whether it is in the registry.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_worker_drained(
        &self,
        worker_id: &str,
        drained: bool,
    ) -> Result<bool, DbError> {
        const DRAINED: &str = "UPDATE workers SET drained = $1 WHERE worker_id = $2";
        let updated = on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, DRAINED))
                .bind(drained)
                .bind(worker_id)
                .execute(pool)
                .await?
                .rows_affected()
        });
        Ok(updated > 0)
    }

    /// Resumes a drained worker; whether it was drained (`None` for a worker not in the
    /// registry).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn resume_worker(&self, worker_id: &str) -> Result<Option<bool>, DbError> {
        const RESUME: &str = "UPDATE workers SET drained = $1 WHERE worker_id = $2 AND drained";
        const KNOWN: &str = "SELECT 1 FROM workers WHERE worker_id = $1";
        let (resumed, known) = on_pool!(self, |pool, backend| {
            let resumed = sqlx::query(sql(backend, RESUME))
                .bind(false)
                .bind(worker_id)
                .execute(pool)
                .await?
                .rows_affected();
            let known: Option<i32> = sqlx::query_scalar(sql(backend, KNOWN))
                .bind(worker_id)
                .fetch_optional(pool)
                .await?;
            (resumed, known)
        });
        Ok(known.map(|_| resumed > 0))
    }

    /// Whether a worker is drained for maintenance (`false` for one not in the registry).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn worker_drained(&self, worker_id: &str) -> Result<bool, DbError> {
        const DRAINED: &str = "SELECT drained FROM workers WHERE worker_id = $1";
        let drained: Option<bool> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, DRAINED))
                .bind(worker_id)
                .fetch_optional(pool)
                .await?
        });
        Ok(drained.unwrap_or(false))
    }

    /// Where one worker's log endpoint is, and its state (what the webserver needs to read a
    /// log); `None` for a worker never registered.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the state doesn't read.
    pub async fn worker_log_endpoint(
        &self,
        worker_id: &str,
    ) -> Result<Option<(WorkerState, String)>, DbError> {
        const ENDPOINT: &str = "SELECT state, log_endpoint FROM workers WHERE worker_id = $1";
        let row: Option<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ENDPOINT))
                .bind(worker_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(state, endpoint)| Ok((from_name("workers.state", state)?, endpoint)))
            .transpose()
    }

    /// Every worker in the registry, by ID, with its running attempts counted.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn workers(&self) -> Result<Vec<WorkerRecord>, DbError> {
        const WORKERS: &str = "SELECT w.worker_id, w.incarnation_id{text}, w.state, \
            w.protocol_version, w.schedr_version, w.queues{text}, w.slots, w.log_endpoint, \
            {ts<}w.registered_at{>ts}, {ts<}w.last_heartbeat_at{>ts}, {ts<}w.offline_at{>ts}, \
            (SELECT COUNT(*) FROM attempts a WHERE a.worker_id = w.worker_id \
             AND a.status IS NULL), w.drained \
            FROM workers w ORDER BY w.worker_id";
        let rows: Vec<WorkerRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, WORKERS))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(worker_record).collect()
    }
}

fn worker_record(row: WorkerRow) -> Result<WorkerRecord, DbError> {
    let (
        id,
        incarnation,
        state,
        protocol_version,
        schedr_version,
        queues,
        slots,
        log_endpoint,
        registered,
        heartbeat,
        offline,
        running,
        drained,
    ) = row;
    Ok(WorkerRecord {
        id,
        incarnation,
        state: from_name("workers.state", state)?,
        protocol_version,
        schedr_version,
        queues: from_json("workers.queues", &queues)?,
        slots: u32::try_from(slots).unwrap_or(0),
        log_endpoint,
        registered_at: parse_time("workers.registered_at", &registered)?,
        last_heartbeat_at: parse_time("workers.last_heartbeat_at", &heartbeat)?,
        offline_at: optional_time("workers.offline_at", offline)?,
        running: u32::try_from(running).unwrap_or(0),
        drained,
    })
}
