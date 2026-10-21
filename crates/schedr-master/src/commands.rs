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
use schedr_model::DagId;
use schedr_model::commands::{Command, CommandStatus};
use serde_json::json;

use crate::dispatch::{DispatchError, Dispatcher, Outgoing};
use crate::execution::ExecutionError;
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
}

impl Commander {
    /// A commander over `db`.
    #[must_use]
    pub const fn new(db: Database) -> Self {
        Self { db }
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
                .apply(&pending, scheduler, dispatcher, &mut applied.outgoing, now)
                .await?;
            let (status, reason, result) = match outcome {
                Outcome::Applied(result) => (CommandStatus::Applied, None, Some(result)),
                Outcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason), None),
            };
            self.db
                .decide_command(&pending.id, status, reason.as_deref(), result.as_ref(), now)
                .await?;
            applied.decided.push(Decided {
                id: pending.id,
                status,
                reason,
                result,
            });
        }
        Ok(applied)
    }

    async fn apply(
        &self,
        pending: &PendingCommand,
        scheduler: &Scheduler,
        dispatcher: &mut Dispatcher,
        outgoing: &mut Vec<Outgoing>,
        now: Timestamp,
    ) -> Result<Outcome, CommandError> {
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
        }
    }
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

fn no_dag(dag_id: &DagId) -> Outcome {
    Outcome::Rejected(format!("no DAG `{dag_id}`"))
}
