//! Attempts: one try of a task instance on a worker.
//!
//! An assignment is stored **before** `AssignTask` is sent: the task instance becomes
//! `running` and its try number goes up, in one transaction that only succeeds while it is
//! `queued` (two passes can't assign it twice). An attempt's first end wins: delivery is at
//! least once, so a repeated `Finished` changes nothing (see PROJECT.md, Task assignment).

use std::collections::BTreeMap;
use std::sync::Arc;

use jiff::Timestamp;
use schedr_model::{AttemptStatus, Dag, RunKind, TaskId, TaskInstanceState};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, from_name, in_tx, on_pool, parse_time, sql, sql_text};

/// The most tasks one query reads the outputs of.
const OUTPUTS_PER_QUERY: usize = 500;

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
    /// The DAG version the run runs (shared: the cache's, see [`Database::dag_version`]).
    pub dag: Arc<Dag>,
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
);

/// The attempt queries (sqlx needs `'static` text, so the shared columns are written out).
const ATTEMPT_BY_ID: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.id = $1{uuid}";
const OPEN_ATTEMPTS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
    WHERE a.status IS NULL AND ($1 IS NULL OR a.worker_id = $1) \
    ORDER BY a.assigned_at, a.id";
const UNSTARTED_ATTEMPTS: &str = "SELECT a.id{text}, a.task_instance_id{text}, t.run_id{text}, \
    t.task_id, a.try_number, a.worker_id, a.status, a.started_at IS NOT NULL, \
    t.map_index FROM attempts a JOIN task_instances t ON t.id = a.task_instance_id \
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
            {ts<}r.planned_for{>ts}, {ts<}r.since{>ts}, r.dag_version_id{text}, p.content_hash, \
            t.queued_reason, p.schedr_version, t.map_index FROM task_instances t \
            JOIN runs r ON r.id = t.run_id \
            JOIN dag_versions v ON v.id = r.dag_version_id \
            JOIN parse_results p ON p.id = v.parse_result_id \
            WHERE t.state = 'queued' AND t.ready_at <= $1{ts} AND r.state = 'running' \
            AND r.cancel_requested_at IS NULL AND t.cancel_requested_at IS NULL \
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
            let dag = self.dag_version(&row.7).await?;
            ready_list.push(ready(row, dag)?);
        }
        Ok(ready_list)
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
        const START: &str = "UPDATE attempts SET started_at = COALESCE(started_at, $1{ts}) \
            WHERE id = $2{uuid} AND status IS NULL";
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
            peak_memory_bytes = $4, error_kind = $5, error_summary = $6{json}, code_hash = $7, \
            ended_at = $8{ts}, duration_ms = $9, usage = $10{json}, core_dumped = $11 \
            WHERE id = $12{uuid} AND status IS NULL";
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
                .bind(end.error_summary.as_deref())
                .bind(end.code_hash.as_deref())
                .bind(db_time(now))
                .bind(
                    end.duration_ms
                        .map(|ms| i64::try_from(ms).unwrap_or(i64::MAX)),
                )
                .bind(end.usage.as_ref().map(crate::query::to_json))
                .bind(end.core_dumped)
                .bind(attempt_id)
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
        _version_id,
        content_hash,
        queued_reason,
        parse_version,
        map_index,
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
        content_hash,
        queued_reason,
        parse_version,
        map_index,
    })
}

fn stored_attempt(row: AttemptRow) -> Result<StoredAttempt, DbError> {
    let (id, instance_id, run_id, task_id, try_number, worker_id, status, started, map_index) = row;
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
    })
}
