//! Commands: actions a person asks for (UI, CLI, API), written by the webserver as command
//! rows and applied by the master, which stays the only writer of scheduling state (see
//! PROJECT.md, Web UI and CLI: how the webserver gets state).

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::ids::{DagId, TaskId};

#[cfg(test)]
mod tests;

/// An action asked for by a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    /// Start a manual run, for `planned_for` (`--for`) or now.
    TriggerRun {
        /// The DAG.
        dag_id: DagId,
        /// The moment the run is for; now if not given.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        planned_for: Option<Timestamp>,
    },
    /// Cancel a run (queued or running).
    CancelRun {
        /// The run.
        run_id: String,
    },
    /// Stop the scheduler from creating runs of a DAG; optionally cancel its runs in progress.
    PauseDag {
        /// The DAG.
        dag_id: DagId,
        /// Also cancel its queued and running runs.
        #[serde(default)]
        cancel_runs: bool,
    },
    /// Let the scheduler create runs of a DAG again.
    UnpauseDag {
        /// The DAG.
        dag_id: DagId,
    },
    /// Cancel one task of a run (`schedr tasks cancel`): stopped if it runs, cancelled at
    /// once if it hasn't started; the tasks downstream of it are cancelled.
    CancelTask {
        /// The run.
        run_id: String,
        /// The task.
        task_id: TaskId,
    },
    /// Queue an `unschedulable` task of an unfinished run again (`schedr tasks retry`).
    RetryTask {
        /// The run.
        run_id: String,
        /// The task.
        task_id: TaskId,
    },
}

impl Command {
    /// The command's kind, as stored in its row (`trigger_run`, `cancel_run`, ...).
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::TriggerRun { .. } => "trigger_run",
            Self::CancelRun { .. } => "cancel_run",
            Self::PauseDag { .. } => "pause_dag",
            Self::UnpauseDag { .. } => "unpause_dag",
            Self::CancelTask { .. } => "cancel_task",
            Self::RetryTask { .. } => "retry_task",
        }
    }
}

/// Where a command stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    /// Written, not yet applied by the master.
    Pending,
    /// Applied.
    Applied,
    /// Not applied, with a reason.
    Rejected,
}
