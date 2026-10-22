//! What the webserver reads: runs, their task instances and attempts, as shown to people.
//!
//! The webserver reads the database directly (with a read-only role in production) and never
//! writes scheduling state; actions are command rows (see PROJECT.md, Web UI and CLI).

use jiff::Timestamp;
use schedr_model::{AttemptStatus, DagId, RunKind, RunState, TaskId, TaskInstanceState};

use crate::database::{Database, DbError};
use crate::query::{db_time, from_name, on_pool, parse_time, sql};

#[cfg(test)]
mod tests;

/// The most runs one list returns.
pub const MAX_RUNS: u32 = 500;

/// A run, as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    /// Its ID.
    pub id: String,
    /// The DAG.
    pub dag_id: DagId,
    /// Who or what created it.
    pub kind: RunKind,
    /// Where it stands.
    pub state: RunState,
    /// The moment it is for.
    pub planned_for: Timestamp,
    /// The start of its window.
    pub since: Timestamp,
    /// When it was created.
    pub triggered_at: Timestamp,
    /// `scheduler`, a user name, the parent run, or `trigger`.
    pub triggered_by: String,
    /// When it started running.
    pub started_at: Option<Timestamp>,
    /// When it finished.
    pub ended_at: Option<Timestamp>,
}

/// A task instance, as shown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceRecord {
    /// The task.
    pub task_id: TaskId,
    /// Its state.
    pub state: TaskInstanceState,
    /// Its outcome, on success.
    pub outcome: Option<String>,
    /// Attempts made.
    pub try_number: u32,
    /// Why it is waiting or unschedulable.
    pub queued_reason: Option<String>,
    /// When it last changed.
    pub updated_at: Timestamp,
}

/// An attempt, as shown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    /// Its ID.
    pub id: String,
    /// Its try number.
    pub try_number: u32,
    /// The worker it ran on.
    pub worker_id: Option<String>,
    /// How it ended; `None` while it runs.
    pub status: Option<AttemptStatus>,
    /// The process's exit code.
    pub exit_code: Option<i32>,
    /// The error kind schedr recognised.
    pub error_kind: Option<String>,
    /// The structured error summary (JSON text).
    pub error_summary: Option<String>,
    /// When it was assigned.
    pub assigned_at: Timestamp,
    /// When the worker started it.
    pub started_at: Option<Timestamp>,
    /// When it ended.
    pub ended_at: Option<Timestamp>,
    /// How long it ran, measured by the worker (see [`crate::attempts::AttemptEnd`]).
    pub duration_ms: Option<u64>,
}

type RunRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

type InstanceRow = (String, String, Option<String>, i32, Option<String>, String);

type AttemptRow = (
    String,
    i32,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
);

const RUN_COLUMNS_ONE: &str = "SELECT id{text}, dag_id, kind, state, {ts<}planned_for{>ts}, \
    {ts<}since{>ts}, {ts<}triggered_at{>ts}, triggered_by, {ts<}started_at{>ts}, \
    {ts<}ended_at{>ts} FROM runs WHERE id = $1{uuid}";

const RUN_COLUMNS_OF_DAG: &str = "SELECT id{text}, dag_id, kind, state, {ts<}planned_for{>ts}, \
    {ts<}since{>ts}, {ts<}triggered_at{>ts}, triggered_by, {ts<}started_at{>ts}, \
    {ts<}ended_at{>ts} FROM runs WHERE dag_id = $1 AND ($2{ts} IS NULL OR planned_for = $2{ts}) \
    ORDER BY planned_for DESC, triggered_at DESC, id DESC LIMIT $3";

impl Database {
    /// A DAG's runs, newest `planned_for` first, at most `limit` (capped at [`MAX_RUNS`]);
    /// only those for `planned_for` when given.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn dag_runs(
        &self,
        dag_id: &DagId,
        planned_for: Option<Timestamp>,
        limit: u32,
    ) -> Result<Vec<RunRecord>, DbError> {
        let rows: Vec<RunRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, RUN_COLUMNS_OF_DAG))
                .bind(dag_id.as_str())
                .bind(planned_for.map(db_time))
                .bind(i64::from(limit.min(MAX_RUNS)))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(run_record).collect()
    }

    /// A run by ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn run_record(&self, run_id: &str) -> Result<Option<RunRecord>, DbError> {
        let row: Option<RunRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, RUN_COLUMNS_ONE))
                .bind(run_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(run_record).transpose()
    }

    /// A run's task instances, in task ID order.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn run_instances(&self, run_id: &str) -> Result<Vec<InstanceRecord>, DbError> {
        const INSTANCES: &str = "SELECT task_id, state, outcome, try_number, queued_reason, \
            {ts<}updated_at{>ts} FROM task_instances WHERE run_id = $1{uuid} ORDER BY task_id";
        let rows: Vec<InstanceRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, INSTANCES))
                .bind(run_id)
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(
                |(task_id, state, outcome, try_number, queued_reason, updated_at)| {
                    Ok(InstanceRecord {
                        task_id: task_id_of(task_id)?,
                        state: from_name("task_instances.state", state)?,
                        outcome,
                        try_number: u32::try_from(try_number).unwrap_or(0),
                        queued_reason,
                        updated_at: parse_time("task_instances.updated_at", &updated_at)?,
                    })
                },
            )
            .collect()
    }

    /// The attempts of a run's task, oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn task_attempts(
        &self,
        run_id: &str,
        task_id: &TaskId,
    ) -> Result<Vec<AttemptRecord>, DbError> {
        const ATTEMPTS: &str = "SELECT a.id{text}, a.try_number, a.worker_id, a.status, \
            a.exit_code, a.error_kind, a.error_summary{text}, {ts<}a.assigned_at{>ts}, \
            {ts<}a.started_at{>ts}, {ts<}a.ended_at{>ts}, a.duration_ms FROM attempts a \
            JOIN task_instances t ON t.id = a.task_instance_id \
            WHERE t.run_id = $1{uuid} AND t.task_id = $2 ORDER BY a.try_number";
        let rows: Vec<AttemptRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ATTEMPTS))
                .bind(run_id)
                .bind(task_id.as_str())
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(attempt_record).collect()
    }
}

fn task_id_of(text: String) -> Result<TaskId, DbError> {
    TaskId::new(text).map_err(|e| DbError::InvalidData {
        what: "task_instances.task_id",
        reason: e.to_string(),
    })
}

fn optional_time(what: &'static str, text: Option<String>) -> Result<Option<Timestamp>, DbError> {
    text.map(|t| parse_time(what, &t)).transpose()
}

fn run_record(row: RunRow) -> Result<RunRecord, DbError> {
    let (id, dag_id, kind, state, planned_for, since, triggered_at, triggered_by, started, ended) =
        row;
    Ok(RunRecord {
        id,
        dag_id: DagId::new(dag_id).map_err(|e| DbError::InvalidData {
            what: "runs.dag_id",
            reason: e.to_string(),
        })?,
        kind: from_name("runs.kind", kind)?,
        state: from_name("runs.state", state)?,
        planned_for: parse_time("runs.planned_for", &planned_for)?,
        since: parse_time("runs.since", &since)?,
        triggered_at: parse_time("runs.triggered_at", &triggered_at)?,
        triggered_by,
        started_at: optional_time("runs.started_at", started)?,
        ended_at: optional_time("runs.ended_at", ended)?,
    })
}

fn attempt_record(row: AttemptRow) -> Result<AttemptRecord, DbError> {
    let (
        id,
        try_number,
        worker_id,
        status,
        exit_code,
        error_kind,
        error_summary,
        assigned,
        started,
        ended,
        duration_ms,
    ) = row;
    Ok(AttemptRecord {
        id,
        try_number: u32::try_from(try_number).unwrap_or(0),
        worker_id,
        status: status
            .map(|s| from_name("attempts.status", s))
            .transpose()?,
        exit_code,
        error_kind,
        error_summary,
        assigned_at: parse_time("attempts.assigned_at", &assigned)?,
        started_at: optional_time("attempts.started_at", started)?,
        ended_at: optional_time("attempts.ended_at", ended)?,
        duration_ms: duration_ms.and_then(|ms| u64::try_from(ms).ok()),
    })
}
