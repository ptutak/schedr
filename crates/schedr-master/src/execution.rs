//! Moving runs forward: task instances for new runs, results of attempts, retries whose delay
//! passed, cancelling, and the run result (the rules are in [`crate::progress`]).
//!
//! Every change is stored as it is decided. Assigning tasks to workers and the messages of the
//! protocol use these operations (see PROJECT.md, Task assignment).

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_db::instances::{InstanceUpdate, RunTasks};
use schedr_db::{Database, DbError};
use schedr_model::{AttemptStatus, RunState, TaskId, TaskInstanceState};

use crate::progress::{AfterAttempt, RunProgress, after_attempt};

#[cfg(test)]
mod tests;

/// The outcome of every successful task until outcome types arrive (milestone M4).
pub const SUCCESS: &str = "SUCCESS";

/// Why an execution operation failed.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// No such run.
    #[error("no run `{0}`")]
    UnknownRun(String),
    /// No such task in the run.
    #[error("run `{run_id}` has no task `{task_id}`")]
    UnknownTask {
        /// The run.
        run_id: String,
        /// The task.
        task_id: TaskId,
    },
}

/// How an attempt ended, as the master needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptResult {
    /// The attempt's status.
    pub status: AttemptStatus,
    /// The error kind schedr recognised, for failures.
    pub error_kind: Option<String>,
    /// The output (stored JSON) on success.
    pub output: Option<serde_json::Value>,
}

/// What recording an attempt's result did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    /// The task's new state; `None` when the task wasn't running (a repeated result).
    pub state: Option<TaskInstanceState>,
    /// The run's result, if it finished now.
    pub run_result: Option<RunState>,
}

/// What cancelling a run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cancelled {
    /// Tasks still running: their attempts are to be cancelled.
    pub running: Vec<TaskId>,
    /// The run's result, if it finished now.
    pub run_result: Option<RunState>,
}

/// What cancelling one task did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCancel {
    /// It hadn't started: it is `cancelled` now, with the tasks downstream of it; the run's
    /// result, if it finished now.
    Cancelled(Option<RunState>),
    /// It runs: its attempt is to be stopped, and it ends `cancelled` when it reports.
    Stopping,
    /// It had already finished, in this state: nothing to do.
    AlreadyFinished(TaskInstanceState),
    /// The run had already finished, in this state.
    RunFinished(RunState),
    /// The run is queued: its tasks don't exist yet (cancel the run instead).
    RunQueued,
}

/// What retrying an `unschedulable` task did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskRetry {
    /// It is queued again (this task instance).
    Queued(String),
    /// It isn't `unschedulable` (it is in this state).
    NotUnschedulable(TaskInstanceState),
    /// The run had already finished, in this state.
    RunFinished(RunState),
    /// The run's tasks don't exist yet.
    RunQueued,
}

/// Runs moved forward in a database.
#[derive(Debug, Clone)]
pub struct Execution {
    db: Database,
}

impl Execution {
    /// Execution over `db`.
    #[must_use]
    pub const fn new(db: Database) -> Self {
        Self { db }
    }

    /// Creates the task instances of runs that started: empty tasks are completed, tasks with
    /// no upstream tasks are queued. Returns the runs prepared.
    ///
    /// # Errors
    ///
    /// [`ExecutionError::Db`] if the database fails.
    pub async fn prepare_runs(&self, now: Timestamp) -> Result<Vec<String>, ExecutionError> {
        let mut prepared = Vec::new();
        for run in self.db.newly_running().await? {
            let mut progress = RunProgress::new(&run.dag, BTreeMap::new());
            progress.settle();
            let states: Vec<(TaskId, TaskInstanceState)> = progress
                .states()
                .iter()
                .map(|(id, state)| (id.clone(), *state))
                .collect();
            self.db.create_instances(&run.run_id, &states, now).await?;
            let outcomes: Vec<InstanceUpdate> = states
                .iter()
                .filter(|(_, s)| *s == TaskInstanceState::Success)
                .map(|(id, s)| update(id, *s))
                .collect();
            self.db
                .update_instances(&run.run_id, &outcomes, now)
                .await?;
            if let Some(result) = progress.result() {
                self.db.finish_run(&run.run_id, result, now).await?;
            }
            prepared.push(run.run_id);
        }
        Ok(prepared)
    }

    /// Records how an attempt of a running task ended, moves the run forward and finishes it
    /// when every task has. A result for a task that isn't running is ignored (delivery is at
    /// least once).
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or task, or a database failure.
    pub async fn attempt_finished(
        &self,
        run_id: &str,
        task_id: &TaskId,
        result: &AttemptResult,
        now: Timestamp,
    ) -> Result<Recorded, ExecutionError> {
        let run = self.run(run_id).await?;
        let instance = run
            .instances
            .iter()
            .find(|i| &i.task_id == task_id)
            .ok_or_else(|| ExecutionError::UnknownTask {
                run_id: run_id.to_owned(),
                task_id: task_id.clone(),
            })?;
        if instance.state != TaskInstanceState::Running {
            return Ok(Recorded {
                state: None,
                run_result: None,
            });
        }
        let task = run
            .dag
            .task(task_id)
            .ok_or_else(|| ExecutionError::UnknownTask {
                run_id: run_id.to_owned(),
                task_id: task_id.clone(),
            })?;
        let next = after_attempt(
            result.status,
            result.error_kind.as_deref(),
            instance.retries_used,
            &task.settings,
            run.cancel_requested || instance.cancel_requested,
            now,
        );
        let (state, ready_at) = match next {
            AfterAttempt::Succeeded => (TaskInstanceState::Success, None),
            AfterAttempt::RetryAt(at) => (TaskInstanceState::UpForRetry, Some(at)),
            AfterAttempt::Failed => (TaskInstanceState::Failed, None),
            AfterAttempt::Requeue => (TaskInstanceState::Queued, Some(now)),
            AfterAttempt::Cancelled => (TaskInstanceState::Cancelled, None),
        };
        if state == TaskInstanceState::Success {
            let value = result.output.clone().unwrap_or(serde_json::Value::Null);
            self.db.store_output(&instance.id, SUCCESS, &value).await?;
        }
        let first = InstanceUpdate {
            task_id: task_id.clone(),
            state,
            ready_at,
            outcome: (state == TaskInstanceState::Success).then(|| SUCCESS.to_owned()),
            used_retry: next.uses_retry(),
        };
        let run_result = self.advance(run_id, &run, vec![first], now).await?;
        Ok(Recorded {
            state: Some(state),
            run_result,
        })
    }

    /// Queues the tasks whose retry delay has passed. Returns how many.
    ///
    /// # Errors
    ///
    /// [`ExecutionError::Db`] if the database fails.
    pub async fn queue_due_retries(&self, now: Timestamp) -> Result<usize, ExecutionError> {
        let due = self.db.due_retries(now).await?;
        for (run_id, task_id) in &due {
            let queued = InstanceUpdate {
                ready_at: Some(now),
                ..update(task_id, TaskInstanceState::Queued)
            };
            self.db.update_instances(run_id, &[queued], now).await?;
        }
        Ok(due.len())
    }

    /// Cancels a run: a queued run ends `cancelled` at once; in a running one every task that
    /// hasn't started is cancelled, and the running ones are returned to be stopped (their
    /// results still count, and a failure among them isn't retried).
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or a database failure.
    pub async fn cancel_run(
        &self,
        run_id: &str,
        now: Timestamp,
    ) -> Result<Cancelled, ExecutionError> {
        let run = self.run(run_id).await?;
        if run.state.is_final() {
            return Ok(Cancelled {
                running: Vec::new(),
                run_result: None,
            });
        }
        self.db.request_cancel(run_id, now).await?;
        if run.state == RunState::Queued {
            self.db.finish_run(run_id, RunState::Cancelled, now).await?;
            return Ok(Cancelled {
                running: Vec::new(),
                run_result: Some(RunState::Cancelled),
            });
        }
        let mut progress = RunProgress::new(&run.dag, states_of(&run));
        let (changes, running) = progress.cancel();
        let updates: Vec<InstanceUpdate> = changes.iter().map(|(id, s)| update(id, *s)).collect();
        self.db.update_instances(run_id, &updates, now).await?;
        let run_result = progress.result();
        if let Some(result) = run_result {
            self.db.finish_run(run_id, result, now).await?;
        }
        Ok(Cancelled {
            running,
            run_result,
        })
    }

    /// Cancels one task of a run: one that hasn't started becomes `cancelled` at once, one
    /// that runs is marked so that whatever its attempt reports (or a rejection, an expired
    /// assignment, a lost worker) ends it `cancelled` without a retry, a failure staying a
    /// failure. The tasks downstream of it are cancelled when it is.
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or task, or a database failure.
    pub async fn cancel_task(
        &self,
        run_id: &str,
        task_id: &TaskId,
        now: Timestamp,
    ) -> Result<TaskCancel, ExecutionError> {
        use TaskInstanceState as S;
        let run = self.run(run_id).await?;
        if run.state.is_final() {
            return Ok(TaskCancel::RunFinished(run.state));
        }
        if run.dag.task(task_id).is_none() {
            return Err(ExecutionError::UnknownTask {
                run_id: run_id.to_owned(),
                task_id: task_id.clone(),
            });
        }
        // A run that just started may not have its instances yet either.
        let Some(state) = run
            .instances
            .iter()
            .find(|i| &i.task_id == task_id)
            .map(|i| i.state)
        else {
            return Ok(TaskCancel::RunQueued);
        };
        match state {
            S::Running | S::CleaningUp => {
                self.db.request_task_cancel(run_id, task_id, now).await?;
                Ok(TaskCancel::Stopping)
            }
            S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                let first = vec![update(task_id, S::Cancelled)];
                Ok(TaskCancel::Cancelled(
                    self.advance(run_id, &run, first, now).await?,
                ))
            }
            S::Success | S::Failed | S::Cancelled | S::Skipped | S::UpstreamFailed => {
                Ok(TaskCancel::AlreadyFinished(state))
            }
        }
    }

    /// Queues an `unschedulable` task of an unfinished run again (`schedr tasks retry`).
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or task, or a database failure.
    pub async fn retry_task(
        &self,
        run_id: &str,
        task_id: &TaskId,
        now: Timestamp,
    ) -> Result<TaskRetry, ExecutionError> {
        let run = self.run(run_id).await?;
        if run.state.is_final() {
            return Ok(TaskRetry::RunFinished(run.state));
        }
        if run.dag.task(task_id).is_none() {
            return Err(ExecutionError::UnknownTask {
                run_id: run_id.to_owned(),
                task_id: task_id.clone(),
            });
        }
        let Some(instance) = run.instances.iter().find(|i| &i.task_id == task_id) else {
            return Ok(TaskRetry::RunQueued);
        };
        if instance.state == TaskInstanceState::Unschedulable
            && self.db.retry_unschedulable(run_id, task_id, now).await?
        {
            return Ok(TaskRetry::Queued(instance.id.clone()));
        }
        Ok(TaskRetry::NotUnschedulable(instance.state))
    }

    /// Moves a run forward after a task's state was changed directly (an assignment of a
    /// task being cancelled taken back as `cancelled`). Returns the run's result, if it
    /// finished now.
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or a database failure.
    pub async fn settle(
        &self,
        run_id: &str,
        now: Timestamp,
    ) -> Result<Option<RunState>, ExecutionError> {
        let run = self.run(run_id).await?;
        if run.state.is_final() {
            return Ok(None);
        }
        self.advance(run_id, &run, Vec::new(), now).await
    }

    async fn run(&self, run_id: &str) -> Result<RunTasks, ExecutionError> {
        self.db
            .run_tasks(run_id)
            .await?
            .ok_or_else(|| ExecutionError::UnknownRun(run_id.to_owned()))
    }

    /// Applies `first`, settles the run's other tasks, stores everything and finishes the run
    /// if it is done.
    async fn advance(
        &self,
        run_id: &str,
        run: &RunTasks,
        first: Vec<InstanceUpdate>,
        now: Timestamp,
    ) -> Result<Option<RunState>, ExecutionError> {
        let mut progress = RunProgress::new(&run.dag, states_of(run));
        for change in &first {
            progress.set(&change.task_id, change.state);
        }
        let mut updates = first;
        for (task_id, state) in progress.settle() {
            let mut next = update(&task_id, state);
            if state == TaskInstanceState::Queued {
                next.ready_at = Some(now);
            }
            updates.push(next);
        }
        self.db.update_instances(run_id, &updates, now).await?;
        let result = progress.result();
        if let Some(result) = result {
            self.db.finish_run(run_id, result, now).await?;
        }
        Ok(result)
    }
}

fn states_of(run: &RunTasks) -> BTreeMap<TaskId, TaskInstanceState> {
    run.instances
        .iter()
        .map(|i| (i.task_id.clone(), i.state))
        .collect()
}

/// A plain state change; empty tasks that succeed get the success outcome.
fn update(task_id: &TaskId, state: TaskInstanceState) -> InstanceUpdate {
    InstanceUpdate {
        task_id: task_id.clone(),
        state,
        ready_at: None,
        outcome: (state == TaskInstanceState::Success).then(|| SUCCESS.to_owned()),
        used_retry: false,
    }
}
