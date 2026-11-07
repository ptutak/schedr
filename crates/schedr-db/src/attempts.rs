//! Attempts: one try of a task instance on a worker.
//!
//! An assignment is stored **before** `AssignTask` is sent: the task instance becomes
//! `running` and its try number goes up, in one transaction that only succeeds while it is
//! `queued` (two passes can't assign it twice). An attempt's first end wins: delivery is at
//! least once, so a repeated `Finished` changes nothing (see PROJECT.md, Task assignment).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use jiff::Timestamp;
use schedr_model::{AttemptStatus, CleanupResult, Dag, DagId, RunKind, TaskId, TaskInstanceState};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, from_name, in_tx, on_pool, parse_time, sql, sql_text};

/// The most tasks one query reads the outputs of.
const OUTPUTS_PER_QUERY: usize = 500;

#[cfg(test)]
mod tests;

/// A task instance of a running run waiting for a worker (`unschedulable`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unschedulable {
    /// The run's ID.
    pub run_id: String,
    /// The task.
    pub task_id: TaskId,
    /// Its place among a mapped task's instances; -1 for an unmapped task.
    pub map_index: i32,
    /// The DAG version the run runs.
    pub dag: Arc<Dag>,
    /// Since when it is `unschedulable`.
    pub since: Timestamp,
}

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
    /// The DAG version the run runs (shared: the cache's, see [`Database::dag_version`]).
    pub dag: Arc<Dag>,
    /// That version's ID.
    pub version_id: String,
    /// The content hash of the code that version was parsed from (the worker reports what it
    /// actually ran).
    pub content_hash: String,
    /// Why it waits, as shown.
    pub queued_reason: Option<String>,
    /// The schedr version of the parse its DAG version came from: workers older than this
    /// don't get it.
    pub parse_version: String,
    /// Its place among a mapped task's instances; -1 for an unmapped task.
    pub map_index: i32,
}

/// An open attempt, as the concurrency limits count it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveAttempt {
    /// The attempt's ID.
    pub attempt_id: String,
    /// Its run's DAG.
    pub dag_id: DagId,
    /// Its run.
    pub run_id: String,
    /// Its task.
    pub task_id: TaskId,
}

/// One instance of a mapped task, as its downstream tasks see it.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceResult {
    /// Its place, from 0.
    pub map_index: i32,
    /// Its state.
    pub state: TaskInstanceState,
    /// Its outcome, once it succeeded.
    pub outcome: Option<String>,
    /// Its output (stored JSON), once it succeeded.
    pub output: Option<serde_json::Value>,
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
    /// Its task instance's place among a mapped task's instances; -1 for an unmapped task.
    pub map_index: i32,
    /// Whether the task's result is kept while its cleanup runs.
    pub result_kept: bool,
    /// How many times its cleanup moved to another worker.
    pub cleanup_moves: u32,
    /// Whether its moved cleanup was assigned, and hasn't been started yet.
    pub moving: bool,
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
    /// What its process used (its parent's view at its exit), if the worker read it.
    pub usage: Option<schedr_model::runner::ProcessUsage>,
    /// Whether its process dumped core.
    pub core_dumped: bool,
    /// How long it ran, measured by the worker with a monotonic clock (the start and end
    /// timestamps come from the master's wall clock, which can step).
    pub duration_ms: Option<u64>,
    /// The error kind schedr recognised.
    pub error_kind: Option<String>,
    /// The structured error summary, as JSON text.
    pub error_summary: Option<String>,
    /// The content hash the code ran with.
    pub code_hash: Option<String>,
    /// How its cleanup ended; `None` for a task without one.
    pub cleanup: Option<CleanupEnd>,
}

/// How an attempt's cleanup ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupEnd {
    /// Its result.
    pub result: CleanupResult,
    /// How many tries it took.
    pub tries: u32,
    /// How long its tries ran, in milliseconds.
    pub duration_ms: u64,
    /// The error kind of its last try.
    pub error_kind: Option<String>,
    /// The error summary of its last try, as JSON text.
    pub error_summary: Option<String>,
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
            usage: None,
            core_dumped: false,
            duration_ms: None,
            error_kind: None,
            error_summary: None,
            code_hash: None,
            cleanup: None,
        }
    }
}

type ReadyRow = (
    String,
    String,
    String,
    i32,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    i32,
    bool,
);
type AttemptRow = (
    String,
    String,
    String,
    String,
    i32,
    Option<String>,
    Option<String>,
    bool,
    i32,
    bool,
    i32,
    bool,
);

/// The attempt queries (sqlx needs `'static` text, so the shared columns are written out).
const ATTEMPT_BY_ID: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index, a.task_result IS NOT NULL, a.cleanup_moves, \
    a.moved_at IS NOT NULL FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.id = $1{uuid}";
const OPEN_ATTEMPTS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index, a.task_result IS NOT NULL, a.cleanup_moves, \
    a.moved_at IS NOT NULL FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.status IS NULL AND ($1 IS NULL OR a.worker_id = $1) \
    ORDER BY a.assigned_at, a.id";
const UNSTARTED_CLEANUPS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index, a.task_result IS NOT NULL, a.cleanup_moves, \
    a.moved_at IS NOT NULL FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.status IS NULL AND a.moved_at < $1{ts} ORDER BY a.moved_at LIMIT $2";
const ACTIVE_ATTEMPTS: &str = "SELECT a.id{text}, r.dag_id, t.run_id{text}, t.task_id \
    FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    JOIN runs r ON r.id = t.run_id WHERE a.status IS NULL ORDER BY a.id";
const UNSTARTED_ATTEMPTS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index, a.task_result IS NOT NULL, a.cleanup_moves, \
    a.moved_at IS NOT NULL FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.status IS NULL AND a.started_at IS NULL AND a.assigned_at < $1{ts} \
    ORDER BY a.assigned_at, a.id LIMIT $2";

impl Database {
    /// Queued task instances of running runs not being cancelled, ready at `now`: resources'
    /// cleanups first (a cancelled run's too, decision 277), then oldest ready first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn ready_instances(&self, now: Timestamp) -> Result<Vec<ReadyInstance>, DbError> {
        const READY: &str = "SELECT t.id{text}, r.id{text}, t.task_id, t.try_number, r.kind, \
            {ts<}r.planned_for{>ts}, {ts<}r.since{>ts}, r.dag_version_id{text}, p.content_hash, \
            t.queued_reason, p.schedr_version, t.map_index, r.cancel_requested_at IS NOT NULL \
            FROM task_instances t \
            JOIN runs r ON r.id = t.run_id \
            JOIN dag_versions v ON v.id = r.dag_version_id \
            JOIN parse_results p ON p.id = v.parse_result_id \
            WHERE t.state = 'queued' AND t.ready_at <= $1{ts} AND r.state = 'running' \
            AND t.cancel_requested_at IS NULL \
            ORDER BY t.ready_at, t.id";
        let rows: Vec<ReadyRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, READY))
                .bind(db_time(now))
                .fetch_all(pool)
                .await?
        });
        // Each run's DAG from the cache, by version: a row no longer brings the whole
        // definition along, once per ready task (B13).
        let mut ready_list = Vec::with_capacity(rows.len());
        for row in rows {
            let run_cancelling = row.12;
            let dag = self.dag_version(&row.7).await?;
            let cleanup = dag
                .resource_of_cleanup(&task_id_of(row.2.clone())?)
                .is_some();
            // A cancelled run's tasks don't start, but its resources' cleanups do (decision 277).
            if run_cancelling && !cleanup {
                continue;
            }
            ready_list.push((cleanup, ready(row, dag)?));
        }
        // Cleanups first, a paid cluster going before queued work (decision 225); the sort is
        // stable, so the oldest first among each.
        ready_list.sort_by_key(|(cleanup, _)| !*cleanup);
        Ok(ready_list.into_iter().map(|(_, r)| r).collect())
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
        let try_number: Option<i32> = in_tx!(self, |tx, backend| {
            let claimed = sqlx::query(sql(backend, CLAIM))
                .bind(&now)
                .bind(instance_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if claimed == 1 {
                let try_number: i32 = sqlx::query_scalar(sql(backend, TRY))
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
        in_tx!(self, |tx, backend| {
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
        });
        Ok(())
    }

    /// An attempt by ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    /// When an instance's first attempt started: a wait step's `max_wait` runs from then
    /// (decision 321).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails or a time doesn't read.
    pub async fn first_start(&self, instance_id: &str) -> Result<Option<Timestamp>, DbError> {
        const FIRST: &str = "SELECT {ts<}started_at{>ts} FROM attempts \
            WHERE task_instance_id = $1{uuid} AND started_at IS NOT NULL \
            ORDER BY started_at LIMIT 1";
        let first: Option<String> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, FIRST))
                .bind(instance_id)
                .fetch_optional(pool)
                .await?
        });
        first
            .map(|at| parse_time("attempts.started_at", &at))
            .transpose()
    }

    /// An attempt by its ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails or a row doesn't read.
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

    /// Keeps the task's result of an open attempt while its cleanup runs (decision 229).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the write fails.
    pub async fn set_task_result(&self, attempt_id: &str, result: &[u8]) -> Result<(), DbError> {
        // The first stands: a cleanup that moved reports no result of its own.
        const SET: &str = "UPDATE attempts SET task_result = $1 \
            WHERE id = $2{uuid} AND status IS NULL AND task_result IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(result)
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The task's result kept for an attempt, if its process's end was reported.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn task_result(&self, attempt_id: &str) -> Result<Option<Vec<u8>>, DbError> {
        const GET: &str = "SELECT task_result FROM attempts WHERE id = $1{uuid}";
        let row: Option<(Option<Vec<u8>>,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, GET))
                .bind(attempt_id)
                .fetch_optional(pool)
                .await?
        });
        Ok(row.and_then(|(result,)| result))
    }

    /// Adds handles an attempt registered, from place `first` on, keeping what is there (they
    /// are sent again after a reconnect), up to `limit` bytes in all. Returns whether all
    /// fitted.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn add_cleanup_handles(
        &self,
        attempt_id: &str,
        first: u32,
        handles: &[String],
        limit: usize,
    ) -> Result<bool, DbError> {
        const STORED: &str = "SELECT position, CAST(OCTET_LENGTH(handle) AS BIGINT) \
            FROM cleanup_handles WHERE attempt_id = $1{uuid}";
        const ADD: &str = "INSERT INTO cleanup_handles (attempt_id, position, handle) \
            VALUES ($1{uuid}, $2, $3) ON CONFLICT (attempt_id, position) DO NOTHING";
        let fitted = in_tx!(self, |tx, backend| {
            let stored: Vec<(i32, i64)> = sqlx::query_as(sql(backend, STORED))
                .bind(attempt_id)
                .fetch_all(&mut *tx)
                .await?;
            let mut size = stored.iter().fold(0_usize, |size, (_, bytes)| {
                size.saturating_add(usize::try_from(*bytes).unwrap_or(usize::MAX))
            });
            let taken: BTreeSet<i32> = stored.into_iter().map(|(position, _)| position).collect();
            let mut fitted = true;
            for (index, handle) in handles.iter().enumerate() {
                // From a worker: `first` may be anything.
                let index = u32::try_from(index).unwrap_or(u32::MAX);
                let position = i32::try_from(first.saturating_add(index)).unwrap_or(i32::MAX);
                // Sent again: what is kept counts once, and what follows it still fits.
                if taken.contains(&position) {
                    continue;
                }
                size = size.saturating_add(handle.len());
                if size > limit {
                    fitted = false;
                    break;
                }
                sqlx::query(sql(backend, ADD))
                    .bind(attempt_id)
                    .bind(position)
                    .bind(handle)
                    .execute(&mut *tx)
                    .await?;
            }
            fitted
        });
        Ok(fitted)
    }

    /// How many of an attempt's handles the master has from the first on without a gap: a
    /// worker holding more cleans those after them (B169). One stored past a gap is cleaned
    /// there again rather than one in the gap left behind (Codex's review of B169).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn cleanup_handles_unbroken(&self, attempt_id: &str) -> Result<u32, DbError> {
        const POSITIONS: &str = "SELECT position FROM cleanup_handles \
            WHERE attempt_id = $1{uuid} ORDER BY position";
        let rows: Vec<(i32,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, POSITIONS))
                .bind(attempt_id)
                .fetch_all(pool)
                .await?
        });
        let unbroken = rows
            .iter()
            .zip(0_i32..)
            .take_while(|((position,), expected)| position == expected)
            .count();
        Ok(u32::try_from(unbroken).unwrap_or(u32::MAX))
    }

    /// An attempt's handles, each as JSON text, in registration order.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn cleanup_handles(&self, attempt_id: &str) -> Result<Vec<String>, DbError> {
        const HANDLES: &str = "SELECT handle FROM cleanup_handles WHERE attempt_id = $1{uuid} \
            ORDER BY position";
        let rows: Vec<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, HANDLES))
                .bind(attempt_id)
                .fetch_all(pool)
                .await?
        });
        Ok(rows.into_iter().map(|(handle,)| handle).collect())
    }

    /// Takes an attempt's cleanup off its worker, to wait for another (decision 243), with
    /// the task's result if none is kept yet, counting a move (decision 247).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the write fails.
    pub async fn move_cleanup(
        &self,
        attempt_id: &str,
        result: &[u8],
        not_before: Timestamp,
    ) -> Result<(), DbError> {
        const MOVE: &str = "UPDATE attempts SET moved_from = worker_id, worker_id = NULL, \
            cleanup_origin = COALESCE(cleanup_origin, worker_id), \
            moved_at = NULL, cleanup_moves = cleanup_moves + 1, \
            task_result = COALESCE(task_result, $1), \
            cleanup_not_before = $2{ts} WHERE id = $3{uuid} AND status IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, MOVE))
                .bind(result)
                .bind(db_time(not_before))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Gives a cleanup waiting to move back to the worker it moved from, which came back
    /// holding handles the master never got (B169): the move is undone, and with
    /// `drop_result` the task's result the move kept goes too (the worker reports its own).
    /// `false` if it no longer waits for a worker, or moved from another.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn take_back_cleanup(
        &self,
        attempt_id: &str,
        worker_id: &str,
        drop_result: bool,
    ) -> Result<bool, DbError> {
        const BACK: &str = "UPDATE attempts SET worker_id = $1, moved_from = NULL, \
            moved_at = NULL, cleanup_not_before = NULL, cleanup_moves = cleanup_moves - 1, \
            cleanup_origin = CASE WHEN cleanup_moves = 1 THEN NULL ELSE cleanup_origin END, \
            task_result = CASE WHEN $2 THEN NULL ELSE task_result END \
            WHERE id = $3{uuid} AND status IS NULL AND worker_id IS NULL AND moved_from = $1 \
            AND cleanup_moves > 0";
        let updated = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, BACK))
                .bind(worker_id)
                .bind(drop_result)
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(updated == 1)
    }

    /// The worker an attempt's cleanup first moved from (the one that ran its task), while no
    /// rest of its cleanup was added (B169): handles it registered that the master never got
    /// are cleaned there. Moves after the first don't change it (Codex's review of B169).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the read fails.
    pub async fn rest_owed_by(&self, attempt_id: &str) -> Result<Option<String>, DbError> {
        const FROM: &str = "SELECT cleanup_origin FROM attempts \
            WHERE id = $1{uuid} AND NOT rest_cleaned";
        let row: Option<(Option<String>,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, FROM))
                .bind(attempt_id)
                .fetch_optional(pool)
                .await?
        });
        Ok(row.and_then(|(from,)| from))
    }

    /// The worker an open attempt's cleanup moved from, while it waits for another (no worker
    /// yet): that worker's late result still counts (decision 282).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the read fails.
    pub async fn cleanup_moved_from(&self, attempt_id: &str) -> Result<Option<String>, DbError> {
        const FROM: &str = "SELECT moved_from FROM attempts \
            WHERE id = $1{uuid} AND status IS NULL AND worker_id IS NULL";
        let row: Option<(Option<String>,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, FROM))
                .bind(attempt_id)
                .fetch_optional(pool)
                .await?
        });
        Ok(row.and_then(|(from,)| from))
    }

    /// Gives a moved cleanup to `worker_id`, if it still waits. Returns whether it did.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the write fails.
    pub async fn assign_cleanup(
        &self,
        attempt_id: &str,
        worker_id: &str,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        const ASSIGN: &str = "UPDATE attempts SET worker_id = $1, moved_at = $2{ts} \
            WHERE id = $3{uuid} AND status IS NULL AND worker_id IS NULL";
        const REASON: &str = "UPDATE task_instances SET queued_reason = NULL \
            WHERE id = (SELECT task_instance_id FROM attempts WHERE id = $1{uuid})";
        let updated = in_tx!(self, |tx, backend| {
            let updated = sqlx::query(sql(backend, ASSIGN))
                .bind(worker_id)
                .bind(db_time(now))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            sqlx::query(sql(backend, REASON))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
            updated
        });
        Ok(updated == 1)
    }

    /// Takes back a moved cleanup its worker refused or never started: it waits again, no
    /// move counted.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the write fails.
    pub async fn unassign_cleanup(&self, attempt_id: &str) -> Result<(), DbError> {
        const BACK: &str = "UPDATE attempts SET worker_id = NULL, moved_at = NULL \
            WHERE id = $1{uuid} AND status IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, BACK))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The moved cleanups assigned before `cutoff` and not started.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn unstarted_cleanups(
        &self,
        cutoff: Timestamp,
        limit: u32,
    ) -> Result<Vec<StoredAttempt>, DbError> {
        let query = UNSTARTED_CLEANUPS;
        let rows: Vec<AttemptRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, query))
                .bind(db_time(cutoff))
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(stored_attempt).collect()
    }

    /// The moved cleanups waiting for a worker, oldest first: each attempt's ID, and what an
    /// assignment needs.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn waiting_cleanups(
        &self,
        now: Timestamp,
    ) -> Result<Vec<(String, ReadyInstance)>, DbError> {
        const WAITING: &str = "SELECT t.id{text}, r.id{text}, t.task_id, t.try_number, r.kind, \
            {ts<}r.planned_for{>ts}, {ts<}r.since{>ts}, r.dag_version_id{text}, p.content_hash, \
            t.queued_reason, p.schedr_version, t.map_index, r.cancel_requested_at IS NOT NULL \
            FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
            JOIN runs r ON r.id = t.run_id \
            JOIN dag_versions v ON v.id = r.dag_version_id \
            JOIN parse_results p ON p.id = v.parse_result_id \
            WHERE a.status IS NULL AND a.worker_id IS NULL AND a.cleanup_not_before <= $1{ts} \
            ORDER BY a.assigned_at, a.id";
        const IDS: &str = "SELECT a.id{text}, a.task_instance_id{text} FROM attempts a \
            WHERE a.status IS NULL AND a.worker_id IS NULL";
        let rows: Vec<ReadyRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, WAITING))
                .bind(db_time(now))
                .fetch_all(pool)
                .await?
        });
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, IDS)).fetch_all(pool).await?
        });
        let mut waiting = Vec::with_capacity(rows.len());
        for row in rows {
            let dag = self.dag_version(&row.7).await?;
            let instance = ready(row, dag)?;
            if let Some((attempt, _)) = ids.iter().find(|(_, i)| *i == instance.instance_id) {
                waiting.push((attempt.clone(), instance));
            }
        }
        Ok(waiting)
    }

    /// Every open attempt (no end recorded) with its DAG, run and task: what the concurrency
    /// limits count (decision 88A), read when the master starts dispatching.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn active_attempts(&self) -> Result<Vec<ActiveAttempt>, DbError> {
        let query = ACTIVE_ATTEMPTS;
        let rows: Vec<(String, String, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, query)).fetch_all(pool).await?
        });
        rows.into_iter()
            .map(|(attempt_id, dag_id, run_id, task_id)| {
                Ok(ActiveAttempt {
                    attempt_id,
                    dag_id: DagId::new(dag_id).map_err(|e| DbError::InvalidData {
                        what: "runs.dag_id",
                        reason: e.to_string(),
                    })?,
                    run_id,
                    task_id: task_id_of(task_id)?,
                })
            })
            .collect()
    }

    /// The open attempts of `dag_id`'s runs, counted by run and task: `(run, task, count)`
    /// (the DAG page's limits with their counts, decision 96).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn active_counts(
        &self,
        dag_id: &DagId,
    ) -> Result<Vec<(String, TaskId, u32)>, DbError> {
        const COUNTS: &str = "SELECT t.run_id{text}, t.task_id, COUNT(*) \
            FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
            JOIN runs r ON r.id = t.run_id WHERE a.status IS NULL AND r.dag_id = $1 \
            GROUP BY t.run_id, t.task_id ORDER BY t.run_id, t.task_id";
        let rows: Vec<(String, String, i64)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, COUNTS))
                .bind(dag_id.as_str())
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(run, task, count)| {
                Ok((
                    run,
                    task_id_of(task)?,
                    u32::try_from(count).unwrap_or(u32::MAX),
                ))
            })
            .collect()
    }

    /// The oldest `limit` attempts assigned before `cutoff` that never reported `Started`,
    /// oldest assigned first (a backlog read a round's worth at a time, B52).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn unstarted_attempts(
        &self,
        cutoff: Timestamp,
        limit: u32,
    ) -> Result<Vec<StoredAttempt>, DbError> {
        let query = UNSTARTED_ATTEMPTS;
        let rows: Vec<AttemptRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, query))
                .bind(db_time(cutoff))
                .bind(i64::from(limit))
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
            start_method = $2, moved_at = NULL WHERE id = $3{uuid} AND status IS NULL";
        let updated = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, START))
                .bind(db_time(now))
                .bind(start_method)
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(updated == 1)
    }

    /// Records that a running attempt has started, with no start method: its worker
    /// reconnected with it in its journal, its `Started` perhaps lost (B50). A start already
    /// recorded is kept.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn mark_adopted(&self, attempt_id: &str, now: Timestamp) -> Result<(), DbError> {
        const START: &str = "UPDATE attempts SET started_at = COALESCE(started_at, $1{ts}), \
            moved_at = NULL WHERE id = $2{uuid} AND status IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, START))
                .bind(db_time(now))
                .bind(attempt_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(())
    }

    /// Records how an attempt ended, unless an end is already recorded. Returns whether this
    /// call recorded it. A rest cleanup added before (B169, [`Self::add_rest_cleanup`]) is
    /// kept: the two cleanups' results combine as there.
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
            peak_memory_bytes = $4, error_kind = $5, error_summary = $6{json}, code_hash = $7, \
            ended_at = $8{ts}, duration_ms = $9, usage = $10{json}, core_dumped = $11, \
            cleanup_result = CASE WHEN NOT rest_cleaned THEN $12 \
                WHEN cleanup_result IN ('failed', 'worker_lost') THEN cleanup_result \
                WHEN $12 IN ('failed', 'worker_lost') THEN $12 \
                WHEN cleanup_result = 'ok' OR $12 = 'ok' THEN 'ok' \
                ELSE COALESCE($12, cleanup_result) END, \
            cleanup_tries = CASE WHEN NOT rest_cleaned THEN $13 \
                ELSE COALESCE(cleanup_tries, 0) + COALESCE($13, 0) END, \
            cleanup_duration_ms = CASE WHEN NOT rest_cleaned THEN $14 \
                ELSE COALESCE(cleanup_duration_ms, 0) + COALESCE($14, 0) END, \
            cleanup_error_kind = CASE WHEN NOT rest_cleaned THEN $15 \
                ELSE COALESCE(cleanup_error_kind, $15) END, \
            cleanup_error_summary = CASE WHEN NOT rest_cleaned THEN $16{json} \
                ELSE COALESCE(cleanup_error_summary, $16{json}) END, \
            task_result = NULL \
            WHERE id = $17{uuid} AND status IS NULL";
        let cleanup = end.cleanup.as_ref();
        let updated = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, END))
                .bind(end.status.to_string())
                .bind(end.exit_code)
                .bind(end.signal)
                .bind(
                    end.peak_memory_bytes
                        .map(|b| i64::try_from(b).unwrap_or(i64::MAX)),
                )
                .bind(end.error_kind.as_deref())
                .bind(end.error_summary.clone().map(crate::query::storable_json))
                .bind(end.code_hash.as_deref())
                .bind(db_time(now))
                .bind(
                    end.duration_ms
                        .map(|ms| i64::try_from(ms).unwrap_or(i64::MAX)),
                )
                .bind(end.usage.as_ref().map(crate::query::to_json))
                .bind(end.core_dumped)
                .bind(cleanup.map(|c| c.result.to_string()))
                .bind(cleanup.map(|c| i64::from(c.tries)))
                .bind(cleanup.map(|c| i64::try_from(c.duration_ms).unwrap_or(i64::MAX)))
                .bind(cleanup.and_then(|c| c.error_kind.as_deref()))
                .bind(
                    cleanup
                        .and_then(|c| c.error_summary.clone())
                        .map(crate::query::storable_json),
                )
                .bind(attempt_id)
                .bind(true)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(updated == 1)
    }

    /// Adds the cleanup of the handles a moved cleanup didn't have (B169: registered while
    /// the master couldn't hear the worker, which ran it on its return) to the attempt's: a
    /// failure of either (`failed`, `worker_lost`) is the result, else either's `ok`; tries and time add up, the rest's
    /// error is kept. Before the attempt ends, its end combines with it. Once only: `false`
    /// if a rest was added already.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn add_rest_cleanup(
        &self,
        attempt_id: &str,
        rest: &CleanupEnd,
    ) -> Result<bool, DbError> {
        const ADD: &str = "UPDATE attempts SET \
            cleanup_result = CASE WHEN cleanup_result IN ('failed', 'worker_lost') \
                THEN cleanup_result \
                WHEN $1 IN ('failed', 'worker_lost') THEN $1 \
                WHEN cleanup_result = 'ok' OR $1 = 'ok' THEN 'ok' ELSE $1 END, \
            cleanup_tries = COALESCE(cleanup_tries, 0) + $2, \
            cleanup_duration_ms = COALESCE(cleanup_duration_ms, 0) + $3, \
            cleanup_error_kind = COALESCE($4, cleanup_error_kind), \
            cleanup_error_summary = COALESCE($5{json}, cleanup_error_summary), \
            rest_cleaned = $7 \
            WHERE id = $6{uuid} AND NOT rest_cleaned";
        let updated = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, ADD))
                .bind(rest.result.to_string())
                .bind(i64::from(rest.tries))
                .bind(i64::try_from(rest.duration_ms).unwrap_or(i64::MAX))
                .bind(rest.error_kind.as_deref())
                .bind(rest.error_summary.clone().map(crate::query::storable_json))
                .bind(attempt_id)
                .bind(true)
                .execute(&mut *tx)
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
            JOIN task_instances t ON t.id = o.task_instance_id \
            WHERE t.run_id = $1{uuid} AND t.map_index = -1";
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

    /// What `tasks` of a run ended with, for a task that uses them: by task ID, the outcome
    /// of one that succeeded, `None` for one that was skipped (each of its edges counts as
    /// not taken). Tasks in any other state are left out.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn upstream_outcomes(
        &self,
        run_id: &str,
        tasks: &[&TaskId],
    ) -> Result<BTreeMap<TaskId, Option<String>>, DbError> {
        let mut outcomes = BTreeMap::new();
        for chunk in tasks.chunks(OUTPUTS_PER_QUERY) {
            let wanted: Vec<String> = (2..chunk.len() + 2).map(|n| format!("${n}")).collect();
            let text = format!(
                "SELECT task_id, state, outcome FROM task_instances \
                 WHERE run_id = $1{{uuid}} AND state IN ('success', 'skipped') \
                 AND map_index = -1 AND task_id IN ({})",
                wanted.join(", ")
            );
            let rows: Vec<(String, String, Option<String>)> = on_pool!(self, |pool, backend| {
                let mut query = sqlx::query_as(sql_text(backend, &text)).bind(run_id);
                for task in chunk {
                    query = query.bind(task.as_str());
                }
                query.fetch_all(pool).await?
            });
            for (task_id, state, outcome) in rows {
                let outcome = (state == "success")
                    .then(|| outcome.unwrap_or_else(|| schedr_model::DEFAULT_OUTCOME.to_owned()));
                outcomes.insert(task_id_of(task_id)?, outcome);
            }
        }
        Ok(outcomes)
    }

    /// The outputs stored for `tasks` of a run, by task ID: what one task gets as inputs,
    /// without reading every other task's (a wide DAG's assignment read the whole run's, B18).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn outputs_of(
        &self,
        run_id: &str,
        tasks: &[&TaskId],
    ) -> Result<BTreeMap<TaskId, serde_json::Value>, DbError> {
        let mut outputs = BTreeMap::new();
        for chunk in tasks.chunks(OUTPUTS_PER_QUERY) {
            let wanted: Vec<String> = (2..chunk.len() + 2).map(|n| format!("${n}")).collect();
            let text = format!(
                "SELECT t.task_id, o.value{{text}} FROM outputs o \
                 JOIN task_instances t ON t.id = o.task_instance_id \
                 WHERE t.run_id = $1{{uuid}} AND t.map_index = -1 AND t.task_id IN ({})",
                wanted.join(", ")
            );
            let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
                let mut query = sqlx::query_as(sql_text(backend, &text)).bind(run_id);
                for task in chunk {
                    query = query.bind(task.as_str());
                }
                query.fetch_all(pool).await?
            });
            for (task_id, value) in rows {
                outputs.insert(task_id_of(task_id)?, from_json("outputs.value", &value)?);
            }
        }
        Ok(outputs)
    }

    /// `tasks` of a run whose success is stored, in the order they ended (one succeeding now
    /// isn't among them yet: it ended last).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored ID doesn't read.
    pub async fn ended_in_order(
        &self,
        run_id: &str,
        tasks: &[&TaskId],
    ) -> Result<Vec<TaskId>, DbError> {
        let mut ended: Vec<TaskId> = Vec::new();
        // Small: the tasks one `first_of` chooses from. One query, ordered by the database (a
        // time as text would follow Postgres's `DateStyle`).
        let wanted: Vec<String> = (2..tasks.len() + 2).map(|n| format!("${n}")).collect();
        let text = format!(
            "SELECT task_id FROM task_instances \
             WHERE run_id = $1{{uuid}} AND map_index = -1 AND state = 'success' \
             AND task_id IN ({}) ORDER BY updated_at, task_id",
            wanted.join(", ")
        );
        if tasks.is_empty() {
            return Ok(ended);
        }
        let rows: Vec<(String,)> = on_pool!(self, |pool, backend| {
            let mut query = sqlx::query_as(sql_text(backend, &text)).bind(run_id);
            for task in tasks {
                query = query.bind(task.as_str());
            }
            query.fetch_all(pool).await?
        });
        for (task_id,) in rows {
            ended.push(task_id_of(task_id)?);
        }
        Ok(ended)
    }

    /// A mapped task's instances in a run, in `map_index` order: each one's state, and its
    /// outcome and output once it succeeded (what a mapped task's whole output and its lanes
    /// are made of, decisions 70 and 77).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn instance_results(
        &self,
        run_id: &str,
        task: &TaskId,
    ) -> Result<Vec<InstanceResult>, DbError> {
        const RESULTS: &str = "SELECT t.map_index, t.state, t.outcome, o.value{text} \
            FROM task_instances t LEFT JOIN outputs o ON o.task_instance_id = t.id \
            WHERE t.run_id = $1{uuid} AND t.task_id = $2 AND t.map_index >= 0 \
            ORDER BY t.map_index";
        let rows: Vec<(i32, String, Option<String>, Option<String>)> =
            on_pool!(self, |pool, backend| {
                sqlx::query_as(sql(backend, RESULTS))
                    .bind(run_id)
                    .bind(task.as_str())
                    .fetch_all(pool)
                    .await?
            });
        rows.into_iter()
            .map(|(map_index, state, outcome, value)| {
                Ok(InstanceResult {
                    map_index,
                    state: from_name("task_instances.state", state)?,
                    outcome,
                    output: value.map(|v| from_json("outputs.value", &v)).transpose()?,
                })
            })
            .collect()
    }

    /// One instance of a mapped task in a run: its state, and its outcome and output once it
    /// succeeded (what a lane gives its downstream instance).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn instance_result(
        &self,
        run_id: &str,
        task: &TaskId,
        map_index: i32,
    ) -> Result<Option<InstanceResult>, DbError> {
        const RESULT: &str = "SELECT t.state, t.outcome, o.value{text} \
            FROM task_instances t LEFT JOIN outputs o ON o.task_instance_id = t.id \
            WHERE t.run_id = $1{uuid} AND t.task_id = $2 AND t.map_index = $3";
        let row: Option<(String, Option<String>, Option<String>)> =
            on_pool!(self, |pool, backend| {
                sqlx::query_as(sql(backend, RESULT))
                    .bind(run_id)
                    .bind(task.as_str())
                    .bind(map_index)
                    .fetch_optional(pool)
                    .await?
            });
        row.map(|(state, outcome, value)| {
            Ok(InstanceResult {
                map_index,
                state: from_name("task_instances.state", state)?,
                outcome,
                output: value.map(|v| from_json("outputs.value", &v)).transpose()?,
            })
        })
        .transpose()
    }

    /// The `unschedulable` task instances of running runs: each one's run, task, `map_index`,
    /// DAG version and since when it waits (decision 273).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn unschedulable_instances(&self) -> Result<Vec<Unschedulable>, DbError> {
        const WAITING: &str = "SELECT r.id{text}, t.task_id, t.map_index, \
            r.dag_version_id{text}, {ts<}t.updated_at{>ts} FROM task_instances t \
            JOIN runs r ON r.id = t.run_id \
            WHERE t.state = 'unschedulable' AND r.state = 'running' ORDER BY t.updated_at";
        let rows: Vec<(String, String, i32, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, WAITING))
                .fetch_all(pool)
                .await?
        });
        let mut waiting = Vec::with_capacity(rows.len());
        for (run_id, task_id, map_index, version_id, since) in rows {
            waiting.push(Unschedulable {
                run_id,
                task_id: task_id_of(task_id)?,
                map_index,
                dag: self.dag_version(&version_id).await?,
                since: parse_time("task_instances.updated_at", &since)?,
            });
        }
        Ok(waiting)
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
        let updated = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, REQUEUE))
                .bind(db_time(now))
                .execute(&mut *tx)
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

fn ready(row: ReadyRow, dag: Arc<Dag>) -> Result<ReadyInstance, DbError> {
    let (
        instance_id,
        run_id,
        task_id,
        try_number,
        kind,
        planned_for,
        since,
        version_id,
        content_hash,
        queued_reason,
        parse_version,
        map_index,
        _,
    ) = row;
    Ok(ReadyInstance {
        instance_id,
        run_id,
        task_id: task_id_of(task_id)?,
        try_number: u32::try_from(try_number).unwrap_or(0),
        run_kind: from_name("runs.kind", kind)?,
        planned_for: parse_time("runs.planned_for", &planned_for)?,
        since: parse_time("runs.since", &since)?,
        dag,
        version_id,
        content_hash,
        queued_reason,
        parse_version,
        map_index,
    })
}

fn stored_attempt(row: AttemptRow) -> Result<StoredAttempt, DbError> {
    let (
        id,
        instance_id,
        run_id,
        task_id,
        try_number,
        worker_id,
        status,
        started,
        map_index,
        kept,
        moves,
        moving,
    ) = row;
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
        map_index,
        result_kept: kept,
        cleanup_moves: u32::try_from(moves).unwrap_or(0),
        moving,
    })
}
