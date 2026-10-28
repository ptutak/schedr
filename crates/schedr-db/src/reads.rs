//! What the webserver reads: runs, their task instances and attempts, as shown to people.
//!
//! The webserver reads the database directly (with a read-only role in production) and never
//! writes scheduling state; actions are command rows (see PROJECT.md, Web UI and CLI).

use jiff::Timestamp;
use schedr_model::{AttemptStatus, DagId, RunKind, RunState, TaskId, TaskInstanceState};

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, from_name, on_pool, optional_time, parse_time, sql};

#[cfg(test)]
mod tests;

/// How many runs, tasks and workers are in each state (the master's gauges).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateCounts {
    /// Runs by state and kind: `(state, kind, count)`.
    pub runs: Vec<(String, String, i64)>,
    /// Task instances of unfinished runs, by state.
    pub tasks: Vec<(String, i64)>,
    /// Workers in the registry, by state.
    pub workers: Vec<(String, i64)>,
}

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
    /// The peak memory of its process group, sampled by the worker's watchdog.
    pub peak_memory_bytes: Option<u64>,
    /// How the worker started its process (`spawn` or `fork`); `None` until it started.
    pub start_method: Option<String>,
    /// What its process used (its parent's view at its exit), if the worker read it.
    pub usage: Option<schedr_model::runner::ProcessUsage>,
    /// Whether its process dumped core.
    pub core_dumped: bool,
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
    Option<i64>,
    Option<String>,
    Option<String>,
    bool,
);

const RUN_COLUMNS_ONE: &str = "SELECT id{text}, dag_id, kind, state, {ts<}planned_for{>ts}, \
    {ts<}since{>ts}, {ts<}triggered_at{>ts}, triggered_by, {ts<}started_at{>ts}, \
    {ts<}ended_at{>ts} FROM runs WHERE id = $1{uuid}";

const RUN_COLUMNS_OF_DAG: &str = "SELECT id{text}, dag_id, kind, state, {ts<}planned_for{>ts}, \
    {ts<}since{>ts}, {ts<}triggered_at{>ts}, triggered_by, {ts<}started_at{>ts}, \
    {ts<}ended_at{>ts} FROM runs WHERE dag_id = $1 AND ($2{ts} IS NULL OR planned_for = $2{ts}) \
    ORDER BY planned_for DESC, triggered_at DESC, id DESC LIMIT $3";

const RUNS_FILTERED: &str = "SELECT id{text}, dag_id, kind, state, {ts<}planned_for{>ts}, \
    {ts<}since{>ts}, {ts<}triggered_at{>ts}, triggered_by, {ts<}started_at{>ts}, \
    {ts<}ended_at{>ts} FROM runs WHERE ($1 IS NULL OR dag_id = $1) \
    AND ($2 IS NULL OR state = $2) AND ($3{ts} IS NULL OR triggered_at < $3{ts} \
    OR ($5{uuid} IS NOT NULL AND triggered_at = $3{ts} AND id < $5{uuid})) \
    ORDER BY triggered_at DESC, runs.id DESC LIMIT $4";

/// Which runs [`Database::runs`] lists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunFilter {
    /// Only this DAG's.
    pub dag_id: Option<DagId>,
    /// Only runs in this state.
    pub state: Option<RunState>,
    /// Only runs listed after this point: triggered before this moment, or at it with a
    /// lower ID than `before_run` (the next page: the last run shown's `triggered_at` and ID;
    /// a scheduler round triggers its runs at the same moment).
    pub before: Option<Timestamp>,
    /// The last run shown, with `before` (ignored without it).
    pub before_run: Option<String>,
    /// At most this many (capped at [`MAX_RUNS`]).
    pub limit: u32,
}

impl Database {
    /// Runs of every DAG (or one), newest triggered first, filtered by `filter`.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn runs(&self, filter: &RunFilter) -> Result<Vec<RunRecord>, DbError> {
        let rows: Vec<RunRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, RUNS_FILTERED))
                .bind(filter.dag_id.as_ref().map(DagId::as_str))
                .bind(filter.state.map(|s| s.to_string()))
                .bind(filter.before.map(db_time))
                .bind(i64::from(filter.limit.min(MAX_RUNS)))
                .bind(filter.before_run.as_deref())
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(run_record).collect()
    }

    /// The DAG JSON of the version a run ran with (its graph); `None` for an unknown run.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored JSON doesn't read.
    pub async fn run_dag_json(&self, run_id: &str) -> Result<Option<serde_json::Value>, DbError> {
        const DAG: &str = "SELECT v.dag_json{text} FROM runs r \
            JOIN dag_versions v ON v.id = r.dag_version_id WHERE r.id = $1{uuid}";
        let row: Option<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DAG))
                .bind(run_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(json,)| from_json("dag_versions.dag_json", &json))
            .transpose()
    }

    /// A task's stored output in a run, with its outcome; `None` when it has none (yet).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored value doesn't read.
    pub async fn run_task_output(
        &self,
        run_id: &str,
        task_id: &TaskId,
    ) -> Result<Option<(String, serde_json::Value)>, DbError> {
        const OUTPUT: &str = "SELECT o.outcome, o.value{text} FROM outputs o \
            JOIN task_instances t ON t.id = o.task_instance_id \
            WHERE t.run_id = $1{uuid} AND t.task_id = $2";
        let row: Option<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, OUTPUT))
                .bind(run_id)
                .bind(task_id.as_str())
                .fetch_optional(pool)
                .await?
        });
        row.map(|(outcome, value)| Ok((outcome, from_json("outputs.value", &value)?)))
            .transpose()
    }

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

    /// The code hashes a run's attempts ran with, in the order first used (more than one:
    /// its tasks ran on different code).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn run_code_hashes(&self, run_id: &str) -> Result<Vec<String>, DbError> {
        const HASHES: &str = "SELECT a.code_hash FROM attempts a \
            JOIN task_instances t ON t.id = a.task_instance_id \
            WHERE t.run_id = $1{uuid} AND a.code_hash IS NOT NULL AND a.code_hash <> '' \
            GROUP BY a.code_hash ORDER BY MIN(COALESCE(a.started_at, a.assigned_at)), a.code_hash";
        let hashes = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, HASHES))
                .bind(run_id)
                .fetch_all(pool)
                .await?
        });
        Ok(hashes)
    }

    /// How many runs (by state and kind), task instances of unfinished runs and workers (by
    /// state) there are.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a query fails.
    pub async fn state_counts(&self) -> Result<StateCounts, DbError> {
        const RUNS: &str =
            "SELECT state, kind, COUNT(*) FROM runs GROUP BY state, kind ORDER BY state, kind";
        const TASKS: &str = "SELECT t.state, COUNT(*) FROM task_instances t \
            JOIN runs r ON r.id = t.run_id WHERE r.state IN ('queued', 'running') \
            GROUP BY t.state ORDER BY t.state";
        const WORKERS: &str = "SELECT state, COUNT(*) FROM workers GROUP BY state ORDER BY state";
        let (runs, tasks, workers) = on_pool!(self, |pool, backend| {
            (
                sqlx::query_as(sql(backend, RUNS))
                    .fetch_all(&mut *pool)
                    .await?,
                sqlx::query_as(sql(backend, TASKS))
                    .fetch_all(&mut *pool)
                    .await?,
                sqlx::query_as(sql(backend, WORKERS))
                    .fetch_all(&mut *pool)
                    .await?,
            )
        });
        Ok(StateCounts {
            runs,
            tasks,
            workers,
        })
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
            {ts<}a.started_at{>ts}, {ts<}a.ended_at{>ts}, a.duration_ms, a.peak_memory_bytes, \
            a.start_method, a.usage{text}, a.core_dumped FROM attempts a \
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
        peak_memory,
        start_method,
        usage,
        core_dumped,
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
        peak_memory_bytes: peak_memory.and_then(|bytes| u64::try_from(bytes).ok()),
        start_method,
        usage: usage
            .map(|json| from_json("attempts.usage", &json))
            .transpose()?,
        core_dumped,
    })
}
