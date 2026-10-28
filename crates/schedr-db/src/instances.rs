//! Task instances of runs: created when a run starts, moved forward by the master, and the
//! run's result when they have all finished.

use std::sync::Arc;

use jiff::Timestamp;
use schedr_model::{Dag, RunState, TaskId, TaskInstanceState};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{
    db_time, from_json, from_name, in_tx, on_pool, parse_time, sql, sql_text, to_json,
};

#[cfg(test)]
mod tests;

/// A running run whose task instances aren't created yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewlyRunning {
    /// The run's ID.
    pub run_id: String,
    /// The DAG version it runs.
    pub dag: Arc<Dag>,
}

/// A stored task instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredInstance {
    /// Its ID.
    pub id: String,
    /// The task.
    pub task_id: TaskId,
    /// Its state.
    pub state: TaskInstanceState,
    /// Attempts made so far.
    pub try_number: u32,
    /// Attempts that used a retry.
    pub retries_used: u32,
    /// When it became (or becomes) ready to run.
    pub ready_at: Option<Timestamp>,
    /// Whether a user asked to cancel it (`schedr tasks cancel`).
    pub cancel_requested: bool,
    /// Its outcome, once it succeeded.
    pub outcome: Option<String>,
}

/// A run with its task instances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTasks {
    /// The run's state.
    pub state: RunState,
    /// Whether a user asked to cancel it.
    pub cancel_requested: bool,
    /// The DAG version it runs.
    pub dag: Arc<Dag>,
    /// The schedr version of the parse that DAG version came from.
    pub parse_version: String,
    /// Its task instances, in task ID order.
    pub instances: Vec<StoredInstance>,
}

/// A change to a task instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceUpdate {
    /// The task.
    pub task_id: TaskId,
    /// Its new state.
    pub state: TaskInstanceState,
    /// When it is ready to run (`queued`, `up_for_retry`); cleared otherwise.
    pub ready_at: Option<Timestamp>,
    /// Its outcome, on success.
    pub outcome: Option<String>,
    /// Whether the attempt that led here used a retry.
    pub used_retry: bool,
}

type InstanceRow = (
    String,
    String,
    String,
    i32,
    i32,
    Option<String>,
    bool,
    Option<String>,
);
/// A run's state, whether it is being cancelled, its DAG version and its parse's version.
type RunRow = (String, bool, String, String);

/// `updates` with one entry per task, in the order each task first appears: a task given
/// twice ends as its last update says, its used retries added up, as one statement each in
/// turn did (one statement for many rows would apply one of them, any).
fn merged(updates: &[InstanceUpdate]) -> Vec<(&InstanceUpdate, i32)> {
    let mut merged: Vec<(&InstanceUpdate, i32)> = Vec::with_capacity(updates.len());
    let mut at: std::collections::HashMap<&TaskId, usize> = std::collections::HashMap::new();
    for update in updates {
        let retries = i32::from(update.used_retry);
        if let Some(&i) = at.get(&update.task_id) {
            let used = merged[i].1 + retries;
            merged[i] = (update, used);
        } else {
            at.insert(&update.task_id, merged.len());
            merged.push((update, retries));
        }
    }
    merged
}

/// The most task instances one `UPDATE` changes (five parameters each).
const UPDATES_PER_STATEMENT: usize = 500;

/// The most task instances one `INSERT` creates (six parameters each: well under SQLite's
/// 32,766 and Postgres's 65,535).
const INSTANCES_PER_STATEMENT: usize = 500;

impl Database {
    /// Running runs with no task instances yet, oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored DAG doesn't read.
    pub async fn newly_running(&self) -> Result<Vec<NewlyRunning>, DbError> {
        const NEW: &str = "SELECT r.id{text}, r.dag_version_id{text} FROM runs r \
            WHERE r.state = 'running' \
            AND NOT EXISTS (SELECT 1 FROM task_instances t WHERE t.run_id = r.id) \
            ORDER BY r.started_at, r.id";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, NEW)).fetch_all(pool).await?
        });
        let mut runs = Vec::with_capacity(rows.len());
        for (run_id, version_id) in rows {
            let dag = self.dag_version(&version_id).await?;
            runs.push(NewlyRunning { run_id, dag });
        }
        Ok(runs)
    }

    /// Creates a run's task instances in their first states.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails (nothing is stored then).
    pub async fn create_instances(
        &self,
        run_id: &str,
        states: &[(TaskId, TaskInstanceState)],
        now: Timestamp,
    ) -> Result<(), DbError> {
        let now_text = db_time(now);
        // Many rows a statement (a run of a wide DAG was one statement a task, B18), in chunks
        // that stay under SQLite's and Postgres's limits on parameters.
        in_tx!(self, |tx, backend| {
            for chunk in states.chunks(INSTANCES_PER_STATEMENT) {
                let rows: Vec<String> = (0..chunk.len())
                    .map(|i| {
                        let n = i * 6;
                        format!(
                            "(${}{{uuid}}, ${}{{uuid}}, ${}, ${}, ${}{{ts}}, ${}{{ts}})",
                            n + 1,
                            n + 2,
                            n + 3,
                            n + 4,
                            n + 5,
                            n + 6
                        )
                    })
                    .collect();
                let text = format!(
                    "INSERT INTO task_instances \
                     (id, run_id, task_id, state, ready_at, updated_at) VALUES {}",
                    rows.join(", ")
                );
                let text = sql_text(backend, &text);
                let mut insert = sqlx::query(text);
                for (task_id, state) in chunk {
                    let ready = (*state == TaskInstanceState::Queued).then(|| now_text.clone());
                    insert = insert
                        .bind(Uuid::now_v7().to_string())
                        .bind(run_id)
                        .bind(task_id.as_str())
                        .bind(state.to_string())
                        .bind(ready)
                        .bind(&now_text);
                }
                insert.execute(&mut *tx).await?;
            }
        });
        Ok(())
    }

    /// A run with its DAG version and task instances; `None` for an unknown run.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or stored data doesn't read.
    pub async fn run_tasks(&self, run_id: &str) -> Result<Option<RunTasks>, DbError> {
        const RUN: &str = "SELECT r.state, r.cancel_requested_at IS NOT NULL, v.id{text}, \
            p.schedr_version FROM runs r JOIN dag_versions v ON v.id = r.dag_version_id \
            JOIN parse_results p ON p.id = v.parse_result_id WHERE r.id = $1{uuid}";
        const INSTANCES: &str = "SELECT id{text}, task_id, state, try_number, retries_used, \
            {ts<}ready_at{>ts}, cancel_requested_at IS NOT NULL, outcome FROM task_instances \
            WHERE run_id = $1{uuid} ORDER BY task_id";
        let (run, rows): (Option<RunRow>, Vec<InstanceRow>) = on_pool!(self, |pool, backend| {
            let run = sqlx::query_as(sql(backend, RUN))
                .bind(run_id)
                .fetch_optional(&mut *pool)
                .await?;
            let rows = sqlx::query_as(sql(backend, INSTANCES))
                .bind(run_id)
                .fetch_all(&mut *pool)
                .await?;
            (run, rows)
        });
        let Some((state, cancel_requested, version_id, parse_version)) = run else {
            return Ok(None);
        };
        let dag = self.dag_version(&version_id).await?;
        Ok(Some(RunTasks {
            state: from_name("runs.state", state)?,
            cancel_requested,
            dag,
            parse_version,
            instances: rows.into_iter().map(instance).collect::<Result<_, _>>()?,
        }))
    }

    /// Stores changes to a run's task instances, all or none.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn update_instances(
        &self,
        run_id: &str,
        updates: &[InstanceUpdate],
        now: Timestamp,
    ) -> Result<(), DbError> {
        let now = db_time(now);
        let updates = merged(updates);
        // Many rows a statement, joined to a list of values (a finished task in a wide DAG
        // changes many at once, B18); `columnN` names the list's columns on both backends.
        in_tx!(self, |tx, backend| {
            for chunk in updates.chunks(UPDATES_PER_STATEMENT) {
                let rows: Vec<String> = (0..chunk.len())
                    .map(|i| {
                        let n = 2 + i * 5;
                        format!(
                            "(${}, ${}, ${}{{ts}}, ${}, ${})",
                            n + 1,
                            n + 2,
                            n + 3,
                            n + 4,
                            n + 5
                        )
                    })
                    .collect();
                let text = format!(
                    "UPDATE task_instances AS t SET state = v.column2, ready_at = v.column3, \
                     outcome = v.column4, retries_used = t.retries_used + v.column5, \
                     updated_at = $1{{ts}} FROM (VALUES {}) AS v \
                     WHERE t.run_id = $2{{uuid}} AND t.task_id = v.column1",
                    rows.join(", ")
                );
                let mut update = sqlx::query(sql_text(backend, &text))
                    .bind(&now)
                    .bind(run_id);
                for (one, retries) in chunk {
                    update = update
                        .bind(one.task_id.as_str())
                        .bind(one.state.to_string())
                        .bind(one.ready_at.map(db_time))
                        .bind(one.outcome.as_deref())
                        .bind(*retries);
                }
                update.execute(&mut *tx).await?;
            }
        });
        Ok(())
    }

    /// Sets the waiting reasons of queued task instances (`(instance ID, reason)`): one
    /// statement per distinct reason and 500 instances, where it was one
    /// per instance (B18); most waiting tasks share a few reasons.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn set_queued_reasons(
        &self,
        reasons: &[(String, String)],
        now: Timestamp,
    ) -> Result<(), DbError> {
        let mut by_reason: std::collections::BTreeMap<&str, Vec<&str>> =
            std::collections::BTreeMap::new();
        for (instance_id, reason) in reasons {
            by_reason.entry(reason).or_default().push(instance_id);
        }
        let now = db_time(now);
        in_tx!(self, |tx, backend| {
            for (reason, instances) in &by_reason {
                for chunk in instances.chunks(UPDATES_PER_STATEMENT) {
                    let ids: Vec<String> = (3..chunk.len() + 3)
                        .map(|n| format!("${n}{{uuid}}"))
                        .collect();
                    let text = format!(
                        "UPDATE task_instances SET queued_reason = $1, updated_at = $2{{ts}} \
                         WHERE state = 'queued' AND id IN ({})",
                        ids.join(", ")
                    );
                    let mut update = sqlx::query(sql_text(backend, &text))
                        .bind(*reason)
                        .bind(&now);
                    for id in chunk {
                        update = update.bind(*id);
                    }
                    update.execute(&mut *tx).await?;
                }
            }
        });
        Ok(())
    }

    /// Records a run's result.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn finish_run(
        &self,
        run_id: &str,
        state: RunState,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const FINISH: &str = "UPDATE runs SET state = $1, ended_at = $2{ts} WHERE id = $3{uuid}";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, FINISH))
                .bind(state.to_string())
                .bind(db_time(now))
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Marks a run as being cancelled (the first request counts).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn request_cancel(&self, run_id: &str, now: Timestamp) -> Result<(), DbError> {
        const CANCEL: &str = "UPDATE runs SET cancel_requested_at = $1{ts} \
            WHERE id = $2{uuid} AND cancel_requested_at IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, CANCEL))
                .bind(db_time(now))
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Marks a running task as being cancelled (the first request counts).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn request_task_cancel(
        &self,
        run_id: &str,
        task_id: &TaskId,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const CANCEL: &str = "UPDATE task_instances SET cancel_requested_at = $1{ts} \
            WHERE run_id = $2{uuid} AND task_id = $3 AND cancel_requested_at IS NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, CANCEL))
                .bind(db_time(now))
                .bind(run_id)
                .bind(task_id.as_str())
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Queues an `unschedulable` task again (`schedr tasks retry`); whether it was one.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn retry_unschedulable(
        &self,
        run_id: &str,
        task_id: &TaskId,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        const RETRY: &str = "UPDATE task_instances SET state = 'queued', ready_at = $1{ts}, \
            queued_reason = NULL, updated_at = $1{ts} \
            WHERE run_id = $2{uuid} AND task_id = $3 AND state = 'unschedulable'";
        let updated = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, RETRY))
                .bind(db_time(now))
                .bind(run_id)
                .bind(task_id.as_str())
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(updated > 0)
    }

    /// The task instances whose retry delay has passed: (run ID, task ID), oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored ID doesn't read.
    pub async fn due_retries(&self, now: Timestamp) -> Result<Vec<(String, TaskId)>, DbError> {
        const DUE: &str = "SELECT run_id{text}, task_id FROM task_instances \
            WHERE state = 'up_for_retry' AND ready_at <= $1{ts} ORDER BY ready_at, id";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DUE))
                .bind(db_time(now))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(run_id, task_id)| Ok((run_id, task_id_of(task_id)?)))
            .collect()
    }

    /// Stores a task instance's output (stored JSON) with its outcome.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn store_output(
        &self,
        instance_id: &str,
        outcome: &str,
        value: &serde_json::Value,
    ) -> Result<(), DbError> {
        const OUTPUT: &str = "INSERT INTO outputs (task_instance_id, outcome, value, size_bytes) \
            VALUES ($1{uuid}, $2, $3{json}, $4) ON CONFLICT (task_instance_id) DO UPDATE \
            SET outcome = excluded.outcome, value = excluded.value, size_bytes = excluded.size_bytes";
        let text = to_json(value);
        let size = i64::try_from(text.len()).unwrap_or(i64::MAX);
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, OUTPUT))
                .bind(instance_id)
                .bind(outcome)
                .bind(&text)
                .bind(size)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// A task instance's output and outcome, if stored.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored value doesn't read.
    pub async fn output(
        &self,
        instance_id: &str,
    ) -> Result<Option<(String, serde_json::Value)>, DbError> {
        const OUTPUT: &str =
            "SELECT outcome, value{text} FROM outputs WHERE task_instance_id = $1{uuid}";
        let row: Option<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, OUTPUT))
                .bind(instance_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(|(outcome, value)| Ok((outcome, from_json("outputs.value", &value)?)))
            .transpose()
    }
}

fn task_id_of(text: String) -> Result<TaskId, DbError> {
    TaskId::new(text).map_err(|e| DbError::InvalidData {
        what: "task_instances.task_id",
        reason: e.to_string(),
    })
}

fn instance(row: InstanceRow) -> Result<StoredInstance, DbError> {
    let (id, task_id, state, try_number, retries_used, ready_at, cancel_requested, outcome) = row;
    Ok(StoredInstance {
        id,
        task_id: task_id_of(task_id)?,
        state: from_name("task_instances.state", state)?,
        try_number: u32::try_from(try_number).unwrap_or(0),
        retries_used: u32::try_from(retries_used).unwrap_or(0),
        ready_at: ready_at
            .map(|t| parse_time("task_instances.ready_at", &t))
            .transpose()?,
        cancel_requested,
        outcome,
    })
}
