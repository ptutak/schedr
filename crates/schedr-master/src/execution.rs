//! Moving runs forward: task instances for new runs, results of attempts, retries whose delay
//! passed, cancelling, and the run result (the rules are in [`crate::progress`]).
//!
//! Every change is stored as it is decided. Assigning tasks to workers and the messages of the
//! protocol use these operations (see PROJECT.md, Task assignment).

use std::collections::{BTreeMap, BTreeSet};

use jiff::Timestamp;
use schedr_db::instances::{InstanceUpdate, RunTasks};
use schedr_db::{Database, DbError};
use schedr_model::{
    AttemptStatus, Dag, DagId, Reference, RunState, TaskId, TaskInstanceState, TaskKind,
};

use crate::mapping::{self, Dim};
use crate::metrics::MasterMetrics;
use crate::progress::{AfterAttempt, Expansion, Instances, RunProgress, Snapshot, after_attempt};
use crate::quiet::{WakeReason, Wakes};

/// The most task instances a round creates for runs that just started (whole runs only, at
/// least one): many runs starting at once go over a few rounds (B18).
pub const INSTANCES_PER_ROUND: usize = 2000;

/// The most failed tasks a round queues again once their retry delay passed (B18).
pub const RETRIES_PER_ROUND: usize = 100;

/// `[mapping] max_instances` by default.
pub const DEFAULT_MAX_INSTANCES: usize = 1024;
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
    /// No such instance of the task in the run (not mapped, not expanded, out of range).
    #[error("task `{task_id}` of run `{run_id}` has no instance {map_index}")]
    UnknownInstance {
        /// The run.
        run_id: String,
        /// The task.
        task_id: TaskId,
        /// The instance asked for.
        map_index: i32,
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
    /// The outcome on success, as the worker reported it (`SUCCESS` when it named none).
    pub outcome: Option<String>,
}

/// What recording an attempt's result did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    /// The task's new state; `None` when the task wasn't running (a repeated result).
    pub state: Option<TaskInstanceState>,
    /// The run's result, if it finished now.
    pub run_result: Option<RunState>,
    /// Running instances to stop, by `map_index`: their mapped task's step failed (75a).
    pub stops: Vec<(TaskId, i32)>,
}

/// What cancelling a run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cancelled {
    /// Tasks and instances still running, by `map_index`: their attempts are to be cancelled.
    pub running: Vec<(TaskId, i32)>,
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
    metrics: MasterMetrics,
    wakes: Wakes,
    /// `[mapping] max_instances`.
    max_instances: usize,
}

impl Execution {
    /// Execution over `db`.
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            metrics: MasterMetrics::default(),
            wakes: Wakes::default(),
            max_instances: DEFAULT_MAX_INSTANCES,
        }
    }

    /// Wakes a finished run's DAG through `wakes` (a queued run may take its place).
    pub fn set_wakes(&mut self, wakes: Wakes) {
        self.wakes = wakes;
    }

    /// Counts finished runs in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.metrics = metrics;
    }

    /// The most instances a mapped task may have (`[mapping] max_instances`).
    pub const fn set_max_instances(&mut self, max_instances: usize) {
        self.max_instances = max_instances;
    }

    /// Finishes run `run_id` of `dag` with `result`, and counts it.
    async fn finish(
        &self,
        run_id: &str,
        dag: &DagId,
        result: RunState,
        now: Timestamp,
    ) -> Result<(), ExecutionError> {
        self.db.finish_run(run_id, result, now).await?;
        self.metrics.run_finished(dag.as_str(), &result);
        self.wakes.wake(dag, WakeReason::RunEnded);
        Ok(())
    }

    /// Creates the task instances of runs that started: empty tasks are completed, tasks with
    /// no upstream tasks are queued, mapped tasks with nothing to wait for expanded. Returns
    /// the runs prepared.
    ///
    /// # Errors
    ///
    /// [`ExecutionError::Db`] if the database fails.
    pub async fn prepare_runs(&self, now: Timestamp) -> Result<Vec<String>, ExecutionError> {
        let mut prepared = Vec::new();
        let mut created = 0;
        for run in self.db.newly_running().await? {
            // At most so many instances a round (a run is never split, and one is always
            // made): many runs starting at once go over a few rounds (B18).
            let tasks = run.dag.tasks.len();
            if !prepared.is_empty() && created + tasks > INSTANCES_PER_ROUND {
                break;
            }
            created += tasks;
            let first: Vec<(TaskId, i32, TaskInstanceState)> = run
                .dag
                .tasks
                .iter()
                .map(|t| (t.id.clone(), -1, TaskInstanceState::Scheduled))
                .collect();
            self.db.create_instances(&run.run_id, &first, now).await?;
            let mut progress = RunProgress::new(&run.dag, BTreeMap::new());
            let before = progress.snapshot();
            self.settle_all(&run.run_id, &run.dag, &mut progress)
                .await?;
            self.store(&run.run_id, &mut progress, &before, Vec::new(), now)
                .await?;
            if let Some(result) = progress.result() {
                self.finish(&run.run_id, &run.dag.id, result, now).await?;
            }
            prepared.push(run.run_id);
        }
        Ok(prepared)
    }

    /// Records how an attempt of a running task instance ended, moves the run forward and
    /// finishes it when every task has. A result for an instance that isn't running is
    /// ignored (delivery is at least once).
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or task, or a database failure.
    pub async fn attempt_finished(
        &self,
        run_id: &str,
        task_id: &TaskId,
        map_index: i32,
        result: &AttemptResult,
        now: Timestamp,
    ) -> Result<Recorded, ExecutionError> {
        let run = self.run(run_id).await?;
        let unknown = || ExecutionError::UnknownTask {
            run_id: run_id.to_owned(),
            task_id: task_id.clone(),
        };
        let instance = run
            .instances
            .iter()
            .find(|i| &i.task_id == task_id && i.map_index == map_index)
            .ok_or_else(unknown)?;
        if instance.state != TaskInstanceState::Running {
            return Ok(Recorded {
                state: None,
                run_result: None,
                stops: Vec::new(),
            });
        }
        let task = run.dag.task(task_id).ok_or_else(unknown)?;
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
        let outcome = result
            .outcome
            .as_deref()
            .filter(|o| !o.is_empty())
            .unwrap_or(SUCCESS);
        if state == TaskInstanceState::Success {
            let value = result.output.clone().unwrap_or(serde_json::Value::Null);
            self.db.store_output(&instance.id, outcome, &value).await?;
        }
        let first = InstanceUpdate {
            ready_at,
            outcome: (state == TaskInstanceState::Success).then(|| outcome.to_owned()),
            used_retry: next.uses_retry(),
            ..update(task_id, map_index, state)
        };
        let (run_result, stops) = self.advance(run_id, &run, vec![first], now).await?;
        Ok(Recorded {
            state: Some(state),
            run_result,
            stops,
        })
    }

    /// Queues the task instances whose retry delay has passed. Returns how many.
    ///
    /// # Errors
    ///
    /// [`ExecutionError::Db`] if the database fails.
    pub async fn queue_due_retries(&self, now: Timestamp) -> Result<usize, ExecutionError> {
        let mut due = self.db.due_retries(now).await?;
        // At most so many a round, the earliest due first; the rest in the next ones (B18).
        due.truncate(RETRIES_PER_ROUND);
        for (run_id, task_id, map_index) in &due {
            let queued = InstanceUpdate {
                ready_at: Some(now),
                ..update(task_id, *map_index, TaskInstanceState::Queued)
            };
            self.db.update_instances(run_id, &[queued], now).await?;
        }
        Ok(due.len())
    }

    /// Cancels a run: a queued run ends `cancelled` at once; in a running one every task and
    /// instance that hasn't started is cancelled, and the running ones are returned to be
    /// stopped (their results still count, and a failure among them isn't retried).
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
            self.finish(run_id, &run.dag.id, RunState::Cancelled, now)
                .await?;
            return Ok(Cancelled {
                running: Vec::new(),
                run_result: Some(RunState::Cancelled),
            });
        }
        let mut progress = progress_of(&run);
        let before = progress.snapshot();
        let (_, running) = progress.cancel();
        // A mapped task whose instances are all cancelled now ends as a whole.
        self.settle_all(run_id, &run.dag, &mut progress).await?;
        self.store(run_id, &mut progress, &before, Vec::new(), now)
            .await?;
        let run_result = progress.result();
        if let Some(result) = run_result {
            self.finish(run_id, &run.dag.id, result, now).await?;
        }
        Ok(Cancelled {
            running,
            run_result,
        })
    }

    /// Cancels one task of a run: one that hasn't started becomes `cancelled` at once, one
    /// that runs is marked so that whatever its attempt reports (or a rejection, an expired
    /// assignment, a lost worker) ends it `cancelled` without a retry, a failure staying a
    /// failure. The tasks downstream of it are cancelled when it is. For a mapped task, every
    /// instance of it.
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or task, or a database failure.
    pub async fn cancel_task(
        &self,
        run_id: &str,
        (task_id, map_index): (&TaskId, Option<i32>),
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
        if let Some(index) = map_index.filter(|i| *i >= 0) {
            return self
                .cancel_instance(run_id, &run, (task_id, index), now)
                .await;
        }
        // A run that just started may not have its instances yet either.
        let Some(state) = run
            .instances
            .iter()
            .find(|i| &i.task_id == task_id && i.map_index == -1)
            .map(|i| i.state)
        else {
            return Ok(TaskCancel::RunQueued);
        };
        let instances: Vec<(usize, S)> = run
            .instances
            .iter()
            .filter(|i| &i.task_id == task_id && i.map_index >= 0)
            .filter_map(|i| Some((usize::try_from(i.map_index).ok()?, i.state)))
            .collect();
        if state == S::Running && !instances.is_empty() {
            // An expanded mapped task: its instances that haven't started are cancelled, the
            // running ones stopped; it ends as a whole once they have.
            let mut progress = progress_of(&run);
            let before = progress.snapshot();
            let mut running = false;
            for (index, instance) in instances {
                match instance {
                    S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                        progress.set_instance(task_id, index, S::Cancelled);
                    }
                    S::Running | S::CleaningUp => running = true,
                    _ => {}
                }
            }
            if running {
                self.db.request_task_cancel(run_id, task_id, now).await?;
            }
            self.settle_all(run_id, &run.dag, &mut progress).await?;
            self.store(run_id, &mut progress, &before, Vec::new(), now)
                .await?;
            let result = progress.result();
            if let Some(result) = result {
                self.finish(run_id, &run.dag.id, result, now).await?;
            }
            return Ok(if running {
                TaskCancel::Stopping
            } else {
                TaskCancel::Cancelled(result)
            });
        }
        match state {
            S::Running | S::CleaningUp => {
                self.db.request_task_cancel(run_id, task_id, now).await?;
                Ok(TaskCancel::Stopping)
            }
            S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                let first = vec![update(task_id, -1, S::Cancelled)];
                Ok(TaskCancel::Cancelled(
                    self.advance(run_id, &run, first, now).await?.0,
                ))
            }
            S::Success | S::Failed | S::Cancelled | S::Skipped | S::UpstreamFailed => {
                Ok(TaskCancel::AlreadyFinished(state))
            }
        }
    }

    /// Cancels one instance of a mapped task (decision 82): one that hasn't started is
    /// cancelled at once (the task as a whole ends cancelled once its others have), a running
    /// one is stopped.
    async fn cancel_instance(
        &self,
        run_id: &str,
        run: &RunTasks,
        (task_id, map_index): (&TaskId, i32),
        now: Timestamp,
    ) -> Result<TaskCancel, ExecutionError> {
        use TaskInstanceState as S;
        let instance = run
            .instances
            .iter()
            .find(|i| &i.task_id == task_id && i.map_index == map_index)
            .ok_or_else(|| ExecutionError::UnknownInstance {
                run_id: run_id.to_owned(),
                task_id: task_id.clone(),
                map_index,
            })?;
        match instance.state {
            S::Running | S::CleaningUp => {
                self.db
                    .request_instance_cancel(run_id, task_id, map_index, now)
                    .await?;
                Ok(TaskCancel::Stopping)
            }
            S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                let first = vec![update(task_id, map_index, S::Cancelled)];
                Ok(TaskCancel::Cancelled(
                    self.advance(run_id, run, first, now).await?.0,
                ))
            }
            state => Ok(TaskCancel::AlreadyFinished(state)),
        }
    }

    /// Queues an `unschedulable` task again (`schedr tasks retry`); for a mapped task, its
    /// `unschedulable` instances.
    ///
    /// # Errors
    ///
    /// [`ExecutionError`] for an unknown run or task, or a database failure.
    pub async fn retry_task(
        &self,
        run_id: &str,
        (task_id, map_index): (&TaskId, Option<i32>),
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
        let mut of_task = run
            .instances
            .iter()
            .filter(|i| &i.task_id == task_id)
            .filter(|i| map_index.is_none_or(|m| i.map_index == m));
        let Some(instance) = of_task
            .clone()
            .find(|i| i.state == TaskInstanceState::Unschedulable)
            .or_else(|| of_task.next())
        else {
            if let Some(map_index) = map_index.filter(|_| !run.instances.is_empty()) {
                return Err(ExecutionError::UnknownInstance {
                    run_id: run_id.to_owned(),
                    task_id: task_id.clone(),
                    map_index,
                });
            }
            return Ok(TaskRetry::RunQueued);
        };
        if instance.state == TaskInstanceState::Unschedulable
            && self
                .db
                .retry_unschedulable(run_id, (task_id, map_index), now)
                .await?
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
        Ok(self.advance(run_id, &run, Vec::new(), now).await?.0)
    }

    async fn run(&self, run_id: &str) -> Result<RunTasks, ExecutionError> {
        self.db
            .run_tasks(run_id)
            .await?
            .ok_or_else(|| ExecutionError::UnknownRun(run_id.to_owned()))
    }

    /// Applies `first`, settles the run's other tasks, stores everything and finishes the run
    /// if it is done. Returns the run's result, if it finished, and the instances to stop.
    async fn advance(
        &self,
        run_id: &str,
        run: &RunTasks,
        first: Vec<InstanceUpdate>,
        now: Timestamp,
    ) -> Result<(Option<RunState>, Vec<(TaskId, i32)>), ExecutionError> {
        let mut progress = progress_of(run).with_chosen(self.chosen_of(run_id, run, &first).await?);
        let before = progress.snapshot();
        for change in &first {
            if let Ok(index) = usize::try_from(change.map_index) {
                progress.set_instance(&change.task_id, index, change.state);
                if let Some(outcome) = &change.outcome {
                    progress.set_instance_outcome(&change.task_id, index, outcome);
                }
                continue;
            }
            progress.set(&change.task_id, change.state);
            if let Some(outcome) = &change.outcome {
                progress.set_outcome(&change.task_id, outcome);
            }
        }
        self.settle_all(run_id, &run.dag, &mut progress).await?;
        let stops = self
            .store(run_id, &mut progress, &before, first, now)
            .await?;
        let result = progress.result();
        if let Some(result) = result {
            self.finish(run_id, &run.dag.id, result, now).await?;
        }
        Ok((result, stops))
    }

    /// Settles a run, expanding each mapped task as it becomes ready, until nothing changes.
    async fn settle_all(
        &self,
        run_id: &str,
        dag: &Dag,
        progress: &mut RunProgress<'_>,
    ) -> Result<(), ExecutionError> {
        loop {
            progress.settle();
            let ready = progress.take_expansions();
            if ready.is_empty() {
                return Ok(());
            }
            for task_id in ready {
                let expansion = self.expansion(run_id, dag, &task_id, progress).await?;
                progress.expand(&task_id, expansion);
            }
        }
    }

    /// How a mapped task expands: its dimensions' sizes from what it is mapped over (a lane:
    /// its upstream's instances), or why it can't (not a list, too many instances).
    async fn expansion(
        &self,
        run_id: &str,
        dag: &Dag,
        task_id: &TaskId,
        progress: &RunProgress<'_>,
    ) -> Result<Expansion, ExecutionError> {
        let Some(task) = dag.task(task_id) else {
            return Ok(Expansion::Fail(format!("no task `{task_id}`")));
        };
        let dims = mapping::dims(dag, task);
        let used: BTreeSet<&TaskId> = dims
            .iter()
            .filter_map(|(_, dim)| match dim {
                Dim::Values(value) => Some(value.references()),
                Dim::Lane(_) => None,
            })
            .flatten()
            .filter_map(Reference::upstream_task)
            .collect();
        let sources = mapping::sources(&self.db, dag, run_id, &used).await?;
        let mut sizes = Vec::with_capacity(dims.len());
        for (name, dim) in &dims {
            let what = name.map_or_else(
                || "`.expand_kwargs()`".to_owned(),
                |name| format!("mapped argument `{name}`"),
            );
            let size = match dim {
                Dim::Lane(upstream) => progress.instances(upstream).map_or(0, |i| i.states.len()),
                Dim::Values(value) => match mapping::resolve(value, &sources) {
                    Ok(serde_json::Value::Array(items)) => {
                        if name.is_none() && items.iter().any(|item| !item.is_object()) {
                            return Ok(Expansion::Fail(format!(
                                "{what}: every element must be a dict of arguments"
                            )));
                        }
                        items.len()
                    }
                    Ok(other) => {
                        return Ok(Expansion::Fail(format!(
                            "{what}: expected a list, got {}",
                            mapping::kind_of(&other)
                        )));
                    }
                    Err(error) => return Ok(Expansion::Fail(format!("{what}: {error}"))),
                },
            };
            sizes.push(size);
        }
        Ok(match mapping::count(&sizes) {
            Some(n) if n <= self.max_instances => Expansion::Sizes(sizes),
            n => Expansion::Fail(format!(
                "`{task_id}` would have {} instances, more than [mapping] max_instances ({})",
                n.map_or_else(|| "too many".to_owned(), |n| n.to_string()),
                self.max_instances
            )),
        })
    }

    /// Stores what changed since `before` (`first` as given: the attempt's own change) and
    /// marks the instances to stop. Returns those.
    async fn store(
        &self,
        run_id: &str,
        progress: &mut RunProgress<'_>,
        before: &Snapshot,
        first: Vec<InstanceUpdate>,
        now: Timestamp,
    ) -> Result<Vec<(TaskId, i32)>, ExecutionError> {
        let changes = progress.changes_since(before);
        if !changes.created.is_empty() {
            self.db
                .create_instances(run_id, &changes.created, now)
                .await?;
        }
        let mut updates = first;
        for (task_id, map_index, state) in changes.updated {
            let given = updates
                .iter()
                .any(|u| u.task_id == task_id && u.map_index == map_index && u.state == state);
            if given {
                continue;
            }
            let mut next = update(&task_id, map_index, state);
            if state == TaskInstanceState::Queued {
                next.ready_at = Some(now);
            }
            if map_index == -1 {
                next.reason = progress.reason(&task_id).map(str::to_owned);
                next.map_sizes = progress.instances(&task_id).map(|i| i.sizes.clone());
            }
            updates.push(next);
        }
        self.db.update_instances(run_id, &updates, now).await?;
        let mut stops = Vec::new();
        for (task_id, index) in progress.take_stops() {
            let map_index = i32::try_from(index).unwrap_or(i32::MAX);
            self.db
                .request_instance_cancel(run_id, &task_id, map_index, now)
                .await?;
            stops.push((task_id, map_index));
        }
        Ok(stops)
    }
}

impl Execution {
    /// What the run's `@branch` tasks chose (their outputs), for those that have succeeded or
    /// succeed with `first`.
    async fn chosen_of(
        &self,
        run_id: &str,
        run: &RunTasks,
        first: &[InstanceUpdate],
    ) -> Result<BTreeMap<TaskId, Vec<TaskId>>, ExecutionError> {
        let succeeded = |task: &TaskId| {
            first
                .iter()
                .any(|c| &c.task_id == task && c.state == TaskInstanceState::Success)
                || run
                    .instances
                    .iter()
                    .any(|i| &i.task_id == task && i.state == TaskInstanceState::Success)
        };
        let branches: Vec<&TaskId> = run
            .dag
            .tasks
            .iter()
            .filter(|t| matches!(t.kind, TaskKind::Python { branch: true, .. }))
            .map(|t| &t.id)
            .filter(|id| succeeded(id))
            .collect();
        if branches.is_empty() {
            return Ok(BTreeMap::new());
        }
        let outputs = self.db.outputs_of(run_id, &branches).await?;
        Ok(outputs
            .into_iter()
            .map(|(task, output)| {
                let chosen = output
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|id| id.as_str().and_then(|id| TaskId::new(id).ok()))
                    .collect();
                (task, chosen)
            })
            .collect())
    }
}

/// A run's progress as stored: its tasks (the `-1` rows) and the instances of the mapped
/// tasks that expanded.
fn progress_of(run: &RunTasks) -> RunProgress<'_> {
    let mut instances: BTreeMap<TaskId, Instances> = BTreeMap::new();
    for row in run.instances.iter().filter(|i| i.map_index >= 0) {
        let entry = instances
            .entry(row.task_id.clone())
            .or_insert_with(|| Instances {
                sizes: Vec::new(),
                states: Vec::new(),
                outcomes: Vec::new(),
            });
        entry.states.push(row.state);
        entry.outcomes.push(row.outcome.clone());
    }
    for row in run.instances.iter().filter(|i| i.map_index == -1) {
        if let Some(entry) = instances.get_mut(&row.task_id) {
            entry.sizes = row
                .map_sizes
                .clone()
                .unwrap_or_else(|| vec![entry.states.len()]);
        }
    }
    RunProgress::new(&run.dag, states_of(run))
        .with_outcomes(outcomes_of(run))
        .with_instances(instances)
}

fn outcomes_of(run: &RunTasks) -> BTreeMap<TaskId, String> {
    run.instances
        .iter()
        .filter(|i| i.map_index == -1)
        .filter_map(|i| Some((i.task_id.clone(), i.outcome.clone()?)))
        .collect()
}

fn states_of(run: &RunTasks) -> BTreeMap<TaskId, TaskInstanceState> {
    run.instances
        .iter()
        .filter(|i| i.map_index == -1)
        .map(|i| (i.task_id.clone(), i.state))
        .collect()
}

/// A plain state change; empty tasks (and mapped tasks as a whole) that succeed get the
/// success outcome.
fn update(task_id: &TaskId, map_index: i32, state: TaskInstanceState) -> InstanceUpdate {
    InstanceUpdate {
        task_id: task_id.clone(),
        map_index,
        state,
        ready_at: None,
        outcome: (state == TaskInstanceState::Success).then(|| SUCCESS.to_owned()),
        used_retry: false,
        reason: None,
        map_sizes: None,
    }
}
