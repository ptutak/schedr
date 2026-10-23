//! Applying commands: the master reads pending command rows, oldest first, and applies each
//! through the scheduler and the dispatcher (see PROJECT.md, Web UI and CLI).
//!
//! A command that can't be applied (an unknown DAG or run, a run already finished, `--for` in
//! the future, a payload that doesn't read) is **rejected with a reason**; an applied one
//! records what it produced (the run a trigger created, the runs a pause cancelled). A database
//! failure leaves the command pending: the next pass tries again.

use jiff::Timestamp;
use schedr_db::commands::PendingCommand;
use schedr_db::{Database, DbError};
use schedr_model::commands::{Command, CommandStatus};
use schedr_model::{DagId, TaskId};
use serde_json::json;

use crate::dispatch::{DispatchError, Dispatcher, Outgoing};
use crate::execution::{ExecutionError, TaskCancel, TaskRetry};
use crate::metrics::{CommandLabels, MasterMetrics};
use crate::scheduler::{Scheduler, SchedulerError};

#[cfg(test)]
mod tests;

/// Why applying commands stopped (the command stays pending).
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The dispatcher failed (a database failure while cancelling).
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

/// How a command was decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decided {
    /// The command's ID.
    pub id: String,
    /// `applied` or `rejected`.
    pub status: CommandStatus,
    /// Why it was rejected.
    pub reason: Option<String>,
    /// What it produced, when applied.
    pub result: Option<serde_json::Value>,
}

/// What a pass over the pending commands did.
#[derive(Debug, Default)]
pub struct Applied {
    /// The commands decided, in order.
    pub decided: Vec<Decided>,
    /// Messages for workers (cancelled attempts).
    pub outgoing: Vec<Outgoing>,
    /// Workers drained (`true`) or resumed (`false`) for maintenance.
    pub drained: Vec<(String, bool)>,
}

/// The outcome of one command before it is stored.
enum Outcome {
    Applied(serde_json::Value),
    Rejected(String),
}

/// Applies command rows.
#[derive(Debug, Clone)]
pub struct Commander {
    db: Database,
    metrics: MasterMetrics,
}

impl Commander {
    /// A commander over `db`.
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            metrics: MasterMetrics::default(),
        }
    }

    /// Counts decisions and their latency in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.metrics = metrics;
    }

    /// Applies every pending command, oldest first.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the database fails; the command being applied stays pending, the
    /// ones decided before it stay decided.
    pub async fn apply_pending(
        &self,
        scheduler: &Scheduler,
        dispatcher: &mut Dispatcher,
        now: Timestamp,
    ) -> Result<Applied, CommandError> {
        let mut applied = Applied::default();
        for pending in self.db.pending_commands().await? {
            let outcome = self
                .apply(&pending, scheduler, dispatcher, &mut applied, now)
                .await?;
            let (status, reason, result) = match outcome {
                Outcome::Applied(result) => (CommandStatus::Applied, None, Some(result)),
                Outcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason), None),
            };
            self.db
                .decide_command(&pending.id, status, reason.as_deref(), result.as_ref(), now)
                .await?;
            self.count(&pending, status, now);
            applied.decided.push(Decided {
                id: pending.id,
                status,
                reason,
                result,
            });
        }
        Ok(applied)
    }

    fn count(&self, pending: &PendingCommand, status: CommandStatus, now: Timestamp) {
        let kind = pending.command.as_ref().map_or("unreadable", Command::kind);
        self.metrics
            .commands
            .get_or_create(&CommandLabels {
                kind: kind.to_owned(),
                decision: match status {
                    CommandStatus::Applied => "applied",
                    CommandStatus::Rejected => "rejected",
                    CommandStatus::Pending => "pending",
                }
                .to_owned(),
            })
            .inc();
        let waited = now.duration_since(pending.requested_at).as_secs_f64();
        self.metrics.command_latency.observe(waited.max(0.0));
    }

    async fn apply(
        &self,
        pending: &PendingCommand,
        scheduler: &Scheduler,
        dispatcher: &mut Dispatcher,
        applied: &mut Applied,
        now: Timestamp,
    ) -> Result<Outcome, CommandError> {
        let outgoing = &mut applied.outgoing;
        let command = match &pending.command {
            Ok(command) => command,
            Err(reason) => return Ok(Outcome::Rejected(format!("invalid command: {reason}"))),
        };
        match command {
            Command::TriggerRun {
                dag_id,
                planned_for,
            } => {
                match scheduler
                    .trigger(dag_id, *planned_for, &pending.requested_by, now)
                    .await
                {
                    Ok(run) => Ok(Outcome::Applied(json!({"run_id": run.run_id}))),
                    Err(SchedulerError::Db(error)) => Err(error.into()),
                    Err(error) => Ok(Outcome::Rejected(error.to_string())),
                }
            }
            Command::CancelRun { run_id } => {
                let Some(run) = self.db.run_tasks(run_id).await? else {
                    return Ok(Outcome::Rejected(format!("no run `{run_id}`")));
                };
                if run.state.is_final() {
                    return Ok(Outcome::Rejected(format!(
                        "run `{run_id}` already finished ({})",
                        run.state
                    )));
                }
                let stopping = cancel(dispatcher, run_id, outgoing, now).await?;
                Ok(Outcome::Applied(json!({"stopping_attempts": stopping})))
            }
            Command::PauseDag {
                dag_id,
                cancel_runs,
            } => {
                if !self.db.set_paused(dag_id, true).await? {
                    return Ok(no_dag(dag_id));
                }
                let mut cancelled = Vec::new();
                if *cancel_runs {
                    for run_id in self.db.runs_in_progress(dag_id).await? {
                        cancel(dispatcher, &run_id, outgoing, now).await?;
                        cancelled.push(run_id);
                    }
                }
                Ok(Outcome::Applied(json!({"cancelled_runs": cancelled})))
            }
            Command::UnpauseDag { dag_id } => {
                if !self.db.set_paused(dag_id, false).await? {
                    return Ok(no_dag(dag_id));
                }
                Ok(Outcome::Applied(json!({})))
            }
            Command::CancelTask { run_id, task_id } => {
                cancel_task(dispatcher, run_id, task_id, outgoing, now).await
            }
            Command::RetryTask { run_id, task_id } => {
                retry_task(dispatcher, run_id, task_id, now).await
            }
            Command::DrainWorker { worker_id } => {
                if !self.db.set_worker_drained(worker_id, true).await? {
                    return Ok(no_worker(worker_id));
                }
                dispatcher.drain(worker_id);
                applied.drained.push((worker_id.clone(), true));
                let running = dispatcher
                    .workers()
                    .get(worker_id)
                    .map_or(0, |w| w.attempts.len());
                Ok(Outcome::Applied(json!({"running": running})))
            }
            Command::ResumeWorker { worker_id } => match self.db.resume_worker(worker_id).await? {
                None => Ok(no_worker(worker_id)),
                Some(false) => Ok(Outcome::Rejected(format!(
                    "worker `{worker_id}` isn't drained"
                ))),
                Some(true) => {
                    dispatcher.resume(worker_id, now).await?;
                    applied.drained.push((worker_id.clone(), false));
                    Ok(Outcome::Applied(json!({})))
                }
            },
        }
    }
}

/// Cancels one task through the dispatcher (`schedr tasks cancel`).
async fn cancel_task(
    dispatcher: &mut Dispatcher,
    run_id: &str,
    task_id: &TaskId,
    outgoing: &mut Vec<Outgoing>,
    now: Timestamp,
) -> Result<Outcome, CommandError> {
    let (cancelled, messages) = match dispatcher.cancel_task(run_id, task_id, now).await {
        Ok(done) => done,
        Err(DispatchError::Execution(
            unknown @ (ExecutionError::UnknownRun(_) | ExecutionError::UnknownTask { .. }),
        )) => return Ok(Outcome::Rejected(unknown.to_string())),
        Err(error) => return Err(error.into()),
    };
    let stopping = messages.len();
    outgoing.extend(messages);
    Ok(match cancelled {
        TaskCancel::Cancelled(_) => Outcome::Applied(json!({"state": "cancelled"})),
        TaskCancel::Stopping => {
            Outcome::Applied(json!({"state": "stopping", "stopping_attempts": stopping}))
        }
        TaskCancel::AlreadyFinished(state) => Outcome::Rejected(format!(
            "task `{task_id}` of run `{run_id}` already finished ({state})"
        )),
        TaskCancel::RunFinished(state) => {
            Outcome::Rejected(format!("run `{run_id}` already finished ({state})"))
        }
        TaskCancel::RunQueued => Outcome::Rejected(format!(
            "run `{run_id}` hasn't started its tasks yet: cancel the run \
             (`schedr runs cancel`), or try again once it runs"
        )),
    })
}

/// Queues an `unschedulable` task again through the dispatcher (`schedr tasks retry`).
async fn retry_task(
    dispatcher: &mut Dispatcher,
    run_id: &str,
    task_id: &TaskId,
    now: Timestamp,
) -> Result<Outcome, CommandError> {
    let retried = match dispatcher.retry_task(run_id, task_id, now).await {
        Ok(retried) => retried,
        Err(DispatchError::Execution(
            unknown @ (ExecutionError::UnknownRun(_) | ExecutionError::UnknownTask { .. }),
        )) => return Ok(Outcome::Rejected(unknown.to_string())),
        Err(error) => return Err(error.into()),
    };
    Ok(match retried {
        TaskRetry::Queued(_) => Outcome::Applied(json!({"state": "queued"})),
        TaskRetry::NotUnschedulable(state) => Outcome::Rejected(format!(
            "task `{task_id}` of run `{run_id}` is {state}, not unschedulable: only an \
             unschedulable task is retried (a failed one is retried by its `retries`)"
        )),
        TaskRetry::RunFinished(state) => Outcome::Rejected(format!(
            "run `{run_id}` already finished ({state}): trigger a new run"
        )),
        TaskRetry::RunQueued => Outcome::Rejected(format!(
            "run `{run_id}` hasn't started its tasks yet: nothing to retry"
        )),
    })
}

/// Cancels a run through the dispatcher; returns how many attempts are being stopped.
async fn cancel(
    dispatcher: &mut Dispatcher,
    run_id: &str,
    outgoing: &mut Vec<Outgoing>,
    now: Timestamp,
) -> Result<usize, CommandError> {
    match dispatcher.cancel_run(run_id, now).await {
        Ok(messages) => {
            let count = messages.len();
            outgoing.extend(messages);
            Ok(count)
        }
        Err(DispatchError::Execution(ExecutionError::UnknownRun(_))) => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn no_worker(worker_id: &str) -> Outcome {
    Outcome::Rejected(format!(
        "no worker `{worker_id}` (`schedr workers list` shows the workers the master has seen)"
    ))
}

fn no_dag(dag_id: &DagId) -> Outcome {
    Outcome::Rejected(format!("no DAG `{dag_id}`"))
}
