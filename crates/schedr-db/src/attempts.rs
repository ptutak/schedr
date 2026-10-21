//! Attempts: one try of a task instance on a worker.
//!
//! An assignment is stored **before** `AssignTask` is sent: the task instance becomes
//! `running` and its try number goes up, in one transaction that only succeeds while it is
//! `queued` (two passes can't assign it twice). An attempt's first end wins: delivery is at
//! least once, so a repeated `Finished` changes nothing (see PROJECT.md, Task assignment).

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_model::{AttemptStatus, Dag, RunKind, TaskId, TaskInstanceState};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, from_name, on_pool, parse_time, sql};

#[cfg(test)]
mod tests;

/// A queued task instance of a running run, ready to be assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyInstance {
    /// The task instance's ID.
    pub instance_id: String,
    /// The run's ID.
    pub run_id: String,
    /// The task.
    pub task_id: TaskId,
    /// Its try number so far (the next attempt is this plus one).
    pub try_number: u32,
    /// The run's kind.
    pub run_kind: RunKind,
    /// The moment the run is for.
    pub planned_for: Timestamp,
    /// The start of the run's window.
    pub since: Timestamp,
    /// The DAG version the run runs.
    pub dag: Dag,
}

/// A stored attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAttempt {
    /// Its ID.
    pub id: String,
    /// Its task instance.
    pub instance_id: String,
    /// The run.
    pub run_id: String,
    /// The task.
    pub task_id: TaskId,
    /// Its try number.
    pub try_number: u32,
    /// The worker it was assigned to.
    pub worker_id: String,
    /// How it ended; `None` while it runs.
    pub status: Option<AttemptStatus>,
    /// Whether the worker reported `Started`.
    pub started: bool,
}

/// How an attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptEnd {
    /// Its status.
    pub status: AttemptStatus,
    /// The process's exit code, if it exited.
    pub exit_code: Option<i32>,
    /// The signal that killed it, if one did.
    pub signal: Option<i32>,
    /// The peak memory of its process tree.
    pub peak_memory_bytes: Option<u64>,
    /// The error kind schedr recognised.
    pub error_kind: Option<String>,
    /// The structured error summary, as JSON text.
    pub error_summary: Option<String>,
    /// The content hash the code ran with.
    pub code_hash: Option<String>,
}

impl AttemptEnd {
    /// An end decided by the master (`lost`, `assignment_expired`): only a status.
    #[must_use]
    pub const fn by_master(status: AttemptStatus) -> Self {
        Self {
            status,
            exit_code: None,
            signal: None,
            peak_memory_bytes: None,
            error_kind: None,
            error_summary: None,
            code_hash: None,
        }
    }
}

type ReadyRow = (String, String, String, i64, String, String, String, String);
type AttemptRow = (
    String,
    String,
    String,
    String,
    i64,
    Option<String>,
    Option<String>,
    bool,
);

/// The attempt queries (sqlx needs `'static` text, so the shared columns are written out).
const ATTEMPT_BY_ID: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL \
    FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.id = $1{uuid}";
const OPEN_ATTEMPTS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL \
    FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.status IS NULL AND ($1 IS NULL OR a.worker_id = $1) \
    ORDER BY a.assigned_at, a.id";
const UNSTARTED_ATTEMPTS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL \
    FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.status IS NULL AND a.started_at IS NULL AND a.assigned_at < $1{ts} \
    ORDER BY a.assigned_at, a.id";

impl Database {
    /// Queued task instances of running runs not being cancelled, ready at `now`, oldest
    /// ready first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn ready_instances(&self, now: Timestamp) -> Result<Vec<ReadyInstance>, DbError> {
        const READY: &str = "SELECT t.id{text}, r.id{text}, t.task_id, t.try_number, r.kind, \
            {ts<}r.planned_for{>ts}, {ts<}r.since{>ts}, v.dag_json{text} \
            FROM task_instances t JOIN runs r ON r.id = t.run_id \
            JOIN dag_versions v ON v.id = r.dag_version_id \
            WHERE t.state = 'queued' AND t.ready_at <= $1{ts} AND r.state = 'running' \
            AND r.cancel_requested_at IS NULL ORDER BY t.ready_at, t.id";
        let rows: Vec<ReadyRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, READY))
                .bind(db_time(now))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(ready).collect()
    }

    /// Assigns a queued task instance to a worker: it becomes `running` with a new attempt.
    /// Returns the attempt's ID and try number; `None` if the instance isn't queued any more.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails (nothing is stored then).
    pub async fn assign_attempt(
        &self,
        instance_id: &str,
        worker_id: &str,
        now: Timestamp,
    ) -> Result<Option<(String, u32)>, DbError> {
        const CLAIM: &str = "UPDATE task_instances SET state = 'running', \
            try_number = try_number + 1, ready_at = NULL, queued_reason = NULL, updated_at = $1{ts} \
            WHERE id = $2{uuid} AND state = 'queued'";
        const TRY: &str = "SELECT try_number FROM task_instances WHERE id = $1{uuid}";
        const INSERT: &str = "INSERT INTO attempts \
            (id, task_instance_id, try_number, worker_id, assigned_at) \
            VALUES ($1{uuid}, $2{uuid}, $3, $4, $5{ts})";
        let id = Uuid::now_v7().to_string();
        let now = db_time(now);
        let try_number: Option<i64> = on_pool!(self, |pool, backend| {
            let mut tx = pool.begin().await?;
            let claimed = sqlx::query(sql(backend, CLAIM))
                .bind(&now)
                .bind(instance_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if claimed == 1 {
                let try_number: i64 = sqlx::query_scalar(sql(backend, TRY))
                    .bind(instance_id)
                    .fetch_one(&mut *tx)
                    .await?;
                sqlx::query(sql(backend, INSERT))
                    .bind(&id)
                    .bind(instance_id)
                    .bind(try_number)
                    .bind(worker_id)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                Some(try_number)
            } else {
                None
            }
        });
        Ok(try_number.map(|n| (id, u32::try_from(n).unwrap_or(0))))
    }

    /// Takes back an assignment the worker rejected: the attempt is deleted (a rejection is
    /// not a try), the try number goes back, and the instance gets `state` (`queued`, ready at
    /// `ready_at`, or `unschedulable`) with `reason` to show.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails (nothing is stored then).
    pub async fn undo_assignment(
        &self,
        attempt_id: &str,
        state: TaskInstanceState,
        ready_at: Option<Timestamp>,
        reason: Option<&str>,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const RESET: &str = "UPDATE task_instances SET state = $1, ready_at = $2{ts}, \
            queued_reason = $3, try_number = try_number - 1, updated_at = $4{ts} \
            WHERE id = (SELECT task_instance_id FROM attempts WHERE id = $5{uuid}) \
            AND state = 'running'";
        const DELETE: &str = "DELETE FROM attempts WHERE id = $1{uuid} AND status IS NULL";
        on_pool!(self, |pool, backend| {
            let mut tx = pool.begin().await?;
            sqlx::query(sql(backend, RESET))
                .bind(state.to_string())
                .bind(ready_at.map(db_time))
                .bind(reason)
                .bind(db_time(now))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(sql(backend, DELETE))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        });
        Ok(())
    }

    /// An attempt by ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn attempt(&self, attempt_id: &str) -> Result<Option<StoredAttempt>, DbError> {
        let query = ATTEMPT_BY_ID;
        let row: Option<AttemptRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, query))
                .bind(attempt_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(stored_attempt).transpose()
    }

    /// Attempts still running (no end recorded), of one worker or of every worker.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn open_attempts(
        &self,
        worker_id: Option<&str>,
    ) -> Result<Vec<StoredAttempt>, DbError> {
        let query = OPEN_ATTEMPTS;
        let rows: Vec<AttemptRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, query))
                .bind(worker_id)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(stored_attempt).collect()
    }

    /// Attempts assigned before `cutoff` that never reported `Started`.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn unstarted_attempts(
        &self,
        cutoff: Timestamp,
    ) -> Result<Vec<StoredAttempt>, DbError> {
        let query = UNSTARTED_ATTEMPTS;
        let rows: Vec<AttemptRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, query))
                .bind(db_time(cutoff))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(stored_attempt).collect()
    }

    /// Records `Started` for a running attempt. Returns whether it is still running.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn mark_started(
        &self,
        attempt_id: &str,
        start_method: &str,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        const START: &str = "UPDATE attempts SET started_at = COALESCE(started_at, $1{ts}), \
            start_method = $2 WHERE id = $3{uuid} AND status IS NULL";
        let updated = on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, START))
                .bind(db_time(now))
                .bind(start_method)
                .bind(attempt_id)
                .execute(pool)
                .await?
                .rows_affected()
        });
        Ok(updated == 1)
    }

    /// Records how an attempt ended, unless an end is already recorded. Returns whether this
    /// call recorded it.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn end_attempt(
        &self,
        attempt_id: &str,
        end: &AttemptEnd,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        const END: &str = "UPDATE attempts SET status = $1, exit_code = $2, signal = $3, \
            peak_memory_bytes = $4, error_kind = $5, error_summary = $6, code_hash = $7, \
            ended_at = $8{ts} WHERE id = $9{uuid} AND status IS NULL";
        let updated = on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, END))
                .bind(end.status.to_string())
                .bind(end.exit_code)
                .bind(end.signal)
                .bind(
                    end.peak_memory_bytes
                        .map(|b| i64::try_from(b).unwrap_or(i64::MAX)),
                )
                .bind(end.error_kind.as_deref())
                .bind(end.error_summary.as_deref())
                .bind(end.code_hash.as_deref())
                .bind(db_time(now))
                .bind(attempt_id)
                .execute(pool)
                .await?
                .rows_affected()
        });
        Ok(updated == 1)
    }

    /// The outputs stored for a run's tasks, by task ID (what downstream tasks get as inputs).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn run_outputs(
        &self,
        run_id: &str,
    ) -> Result<BTreeMap<TaskId, serde_json::Value>, DbError> {
        const OUTPUTS: &str = "SELECT t.task_id, o.value{text} FROM outputs o \
            JOIN task_instances t ON t.id = o.task_instance_id WHERE t.run_id = $1{uuid}";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, OUTPUTS))
                .bind(run_id)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(task_id, value)| Ok((task_id_of(task_id)?, from_json("outputs.value", &value)?)))
            .collect()
    }

    /// Queues every `unschedulable` task instance again (the set of workers changed).
    /// Returns how many.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn requeue_unschedulable(&self, now: Timestamp) -> Result<u64, DbError> {
        const REQUEUE: &str = "UPDATE task_instances SET state = 'queued', ready_at = $1{ts}, \
            queued_reason = NULL, updated_at = $1{ts} WHERE state = 'unschedulable'";
        let updated = on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, REQUEUE))
                .bind(db_time(now))
                .execute(pool)
                .await?
                .rows_affected()
        });
        Ok(updated)
    }
}

fn task_id_of(text: String) -> Result<TaskId, DbError> {
    TaskId::new(text).map_err(|e| DbError::InvalidData {
        what: "task_instances.task_id",
        reason: e.to_string(),
    })
}

fn ready(row: ReadyRow) -> Result<ReadyInstance, DbError> {
    let (instance_id, run_id, task_id, try_number, kind, planned_for, since, json) = row;
    Ok(ReadyInstance {
        instance_id,
        run_id,
        task_id: task_id_of(task_id)?,
        try_number: u32::try_from(try_number).unwrap_or(0),
        run_kind: from_name("runs.kind", kind)?,
        planned_for: parse_time("runs.planned_for", &planned_for)?,
        since: parse_time("runs.since", &since)?,
        dag: from_json("dag_versions.dag_json", &json)?,
    })
}

fn stored_attempt(row: AttemptRow) -> Result<StoredAttempt, DbError> {
    let (id, instance_id, run_id, task_id, try_number, worker_id, status, started) = row;
    Ok(StoredAttempt {
        id,
        instance_id,
        run_id,
        task_id: task_id_of(task_id)?,
        try_number: u32::try_from(try_number).unwrap_or(0),
        worker_id: worker_id.unwrap_or_default(),
        status: status
            .map(|s| from_name("attempts.status", s))
            .transpose()?,
        started,
    })
}
