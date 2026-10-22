//! Run kinds and the states of runs, task instances and attempts.
//!
//! See PROJECT.md: Task assignment (run states, task instance states, attempt statuses) and
//! Runs: manual runs, backfill and pausing (run kinds).

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// Who or what created a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    /// Created by the scheduler for a fire time.
    Scheduled,
    /// Started by a person (UI, CLI, API).
    Manual,
    /// One of the runs of a backfill.
    Backfill,
    /// Created by another DAG's run (`TriggerDagOperator`).
    Chained,
    /// Created by an event from the DAG's trigger (an event trigger).
    Event,
}

/// Where a run stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    /// Created and waiting for `max_active_runs` (only manual, backfill, chained and event runs).
    Queued,
    /// Tasks are being scheduled or running.
    Running,
    /// Finished without anything unexpected going wrong.
    Success,
    /// Something unexpected went wrong in it (a task or cleanup failed after its last retry).
    Failed,
    /// Cancelled by a user, with no unexpected failure.
    Cancelled,
}

impl RunState {
    /// Whether the run has finished: its result never changes again.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(self, Self::Success | Self::Failed | Self::Cancelled)
    }
}

/// Where a task stands in its run, decided by the master.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskInstanceState {
    /// Created; its upstream tasks haven't decided yet.
    Scheduled,
    /// Ready to run, waiting for a worker or a concurrency limit.
    Queued,
    /// Every eligible worker rejected it.
    Unschedulable,
    /// An attempt is running.
    Running,
    /// The attempt's process has ended and its per-task cleanup is running.
    CleaningUp,
    /// An attempt failed and the next one waits for `retry_delay`.
    UpForRetry,
    /// Succeeded (with an outcome).
    Success,
    /// Failed after its last retry.
    Failed,
    /// Cancelled by a user.
    Cancelled,
    /// Skipped (a branch, an outcome edge not taken, all upstream tasks skipped).
    Skipped,
    /// Will never run because an upstream task failed.
    UpstreamFailed,
}

impl TaskInstanceState {
    /// Whether the task will never change state again in this run.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failed | Self::Cancelled | Self::Skipped | Self::UpstreamFailed
        )
    }
}

/// How one attempt (one try) of a task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// Exit code 0, or the Python function returned normally.
    Success,
    /// A non-zero exit code or an unhandled exception.
    Failed,
    /// Exceeded its timeout and was killed by the worker.
    TimedOut,
    /// Killed by the memory watchdog for exceeding its budget.
    OomKilled,
    /// Cancelled by a user.
    Cancelled,
    /// Killed when a draining worker's grace period ended.
    Interrupted,
    /// The worker disappeared.
    Lost,
    /// No `Started` arrived in time after the assignment.
    AssignmentExpired,
}

/// What the master does after an attempt ended with a given status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetryRule {
    /// Nothing to retry: the attempt succeeded.
    NotNeeded,
    /// Retry if the task has retries left; the attempt uses one of them.
    UsesRetry,
    /// Run again without using a retry (the attempt wasn't the task's fault).
    RequeueWithoutRetry,
    /// Never retried.
    Never,
}

impl AttemptStatus {
    /// What happens after an attempt with this status (see PROJECT.md, Task assignment:
    /// retries are decided by the master).
    #[must_use]
    pub const fn retry_rule(self) -> RetryRule {
        match self {
            Self::Success => RetryRule::NotNeeded,
            Self::Failed | Self::TimedOut | Self::OomKilled | Self::Lost => RetryRule::UsesRetry,
            Self::Interrupted | Self::AssignmentExpired => RetryRule::RequeueWithoutRetry,
            Self::Cancelled => RetryRule::Never,
        }
    }
}

/// Implements `Display` as the serialized (snake case) name.
macro_rules! display_as_serialized {
    ($($ty:ty),+) => {$(
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let value = serde_json::to_value(self).map_err(|_| fmt::Error)?;
                f.write_str(value.as_str().ok_or(fmt::Error)?)
            }
        }
    )+};
}

display_as_serialized!(RunKind, RunState, TaskInstanceState, AttemptStatus);
