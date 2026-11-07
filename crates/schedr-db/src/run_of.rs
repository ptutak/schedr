//! What a `wait_for(RunOf(...))` step reads of another DAG's runs (decision 353).

use jiff::Timestamp;

use crate::database::{Database, DbError};
use crate::query::{db_time, on_pool, parse_time, sql};

/// A run of the DAG waited for, as far as the step needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtherRun {
    /// The run's ID.
    pub run_id: String,
    /// The run's state, or its task's when the step waits for a task (`queued` for a run whose
    /// tasks aren't made yet, `missing` for a run that ended without that task).
    pub state: String,
    /// When it ended, if it did.
    pub ended_at: Option<Timestamp>,
}

/// Which runs of the other DAG count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Matching {
    /// The runs planned for this moment (`lag` given).
    PlannedFor(Timestamp),
    /// The first to end at or after this moment (no `lag`).
    EndedAfter(Timestamp),
}

impl Database {
    /// The runs of `dag` that `matching` picks, with their state, or `task`'s state in each.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn runs_of(
        &self,
        dag: &str,
        task: Option<&str>,
        matching: Matching,
    ) -> Result<Vec<OtherRun>, DbError> {
        const RUNS_FOR: &str = "SELECT id{text}, state, {ts<}ended_at{>ts} FROM runs \
            WHERE dag_id = $1 AND planned_for = $2{ts} ORDER BY triggered_at, id";
        // A run whose tasks aren't made yet (queued) still counts: a rerun on its way.
        const TASKS_FOR: &str = "SELECT r.id{text}, COALESCE(t.state, CASE WHEN r.state IN \
            ('queued', 'running') THEN 'queued' ELSE 'missing' END), {ts<}t.ended_at{>ts} \
            FROM runs r LEFT JOIN task_instances t \
            ON t.run_id = r.id AND t.task_id = $3 AND t.map_index = -1 \
            WHERE r.dag_id = $1 AND r.planned_for = $2{ts} ORDER BY r.triggered_at, r.id";
        const RUN_ENDED: &str = "SELECT id{text}, state, {ts<}ended_at{>ts} FROM runs \
            WHERE dag_id = $1 AND ended_at >= $2{ts} ORDER BY ended_at, id LIMIT 1";
        const TASK_ENDED: &str = "SELECT r.id{text}, t.state, {ts<}t.ended_at{>ts} \
            FROM task_instances t JOIN runs r ON r.id = t.run_id \
            WHERE t.task_id = $3 AND t.map_index = -1 AND t.ended_at >= $2{ts} AND r.dag_id = $1 \
            ORDER BY t.ended_at, r.id LIMIT 1";
        let (query, at) = match (matching, task.is_some()) {
            (Matching::PlannedFor(at), false) => (RUNS_FOR, at),
            (Matching::PlannedFor(at), true) => (TASKS_FOR, at),
            (Matching::EndedAfter(at), false) => (RUN_ENDED, at),
            (Matching::EndedAfter(at), true) => (TASK_ENDED, at),
        };
        let rows: Vec<(String, String, Option<String>)> = on_pool!(self, |pool, backend| {
            let mut statement = sqlx::query_as(sql(backend, query))
                .bind(dag)
                .bind(db_time(at));
            if let Some(task) = task {
                statement = statement.bind(task);
            }
            statement.fetch_all(pool).await?
        });
        rows.into_iter()
            .map(|(run_id, state, ended_at)| {
                Ok(OtherRun {
                    run_id,
                    state,
                    ended_at: ended_at.map(|at| parse_time("ended_at", &at)).transpose()?,
                })
            })
            .collect()
    }

    /// Since when a queued task instance is ready, if it is.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn ready_since(&self, instance_id: &str) -> Result<Option<Timestamp>, DbError> {
        const SINCE: &str = "SELECT {ts<}ready_at{>ts} FROM task_instances WHERE id = $1{uuid} AND ready_at IS NOT NULL";
        let row: Option<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, SINCE))
                .bind(instance_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(at,)| parse_time("task_instances.ready_at", &at))
            .transpose()
    }
}
