//! How a run's tasks move forward: readiness (`NoneFailed`), retries, cancelling and the run
//! result (see PROJECT.md, Outcomes, failures and joins, and Task assignment).
//!
//! Pure rules over task instance states; [`crate::execution`] stores what they decide.
//!
//! - Trigger rules count **edges**: an edge counts as its upstream task's state, except one
//!   whose upstream succeeded with an outcome the edge isn't taken on (`.on(...)`), which
//!   counts as skipped.
//! - `NoneFailed` (the default): a task is **ready** when every edge that wasn't skipped
//!   succeeded and at least one did (or it has none); **skipped** when all its edges were
//!   skipped; **upstream failed** as soon as one failed. `AllSucceeded`: ready when every edge
//!   succeeded; upstream failed as soon as one failed; otherwise skipped when one was skipped,
//!   once none can fail any more.
//! - An attempt that failed **uses a retry**; with retries left the task waits `retry_delay`
//!   (`up_for_retry`), otherwise it fails. Failures certain to repeat are never retried. An
//!   attempt interrupted by draining or never started runs again without using a retry.
//! - A run is **`failed`** if a task failed after its last retry, else **`cancelled`** if a
//!   user cancelled it (and something was cancelled), else **`success`**; decided once every
//!   task has finished.

use std::collections::BTreeMap;

use jiff::{SignedDuration, Timestamp};
use schedr_model::{
    AttemptStatus, DEFAULT_OUTCOME, Dag, RetryRule, RunIf, RunState, TaskId, TaskInstanceState,
    TaskKind, TaskSettings,
};

#[cfg(test)]
mod tests;

/// Error kinds of failures certain to repeat: never retried. A secret that doesn't exist or
/// isn't permitted, and what the same inputs always do again. (`secret_unavailable`, a secret
/// storage that is down, and `import_failed` / `task_not_found`, which new code can fix, are
/// retried.)
pub const NOT_RETRIED_ERRORS: [&str; 7] = [
    "secret_not_found",
    "secret_denied",
    "inputs_too_large",
    "render_failed",
    "input_mismatch",
    "output_not_serializable",
    "output_too_large",
];

/// What a task's upstream tasks decide for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Readiness {
    /// Some upstream task hasn't finished.
    Wait,
    /// It can run.
    Ready,
    /// The edges' skips decide it is skipped (all of them with `NoneFailed`, any with
    /// `AllSucceeded`).
    Skip,
    /// An upstream task failed.
    UpstreamFailed,
    /// An upstream task was cancelled.
    Cancel,
}

/// The trigger rule `rule` over the states of a task's edges (an edge not taken counts as
/// skipped).
#[must_use]
pub fn readiness(edges: &[TaskInstanceState], rule: RunIf) -> Readiness {
    use TaskInstanceState as S;
    if edges
        .iter()
        .any(|s| matches!(s, S::Failed | S::UpstreamFailed))
    {
        return Readiness::UpstreamFailed;
    }
    if edges.contains(&S::Cancelled) {
        return Readiness::Cancel;
    }
    if edges.iter().any(|s| !s.is_final()) {
        return Readiness::Wait;
    }
    let skipped = match rule {
        RunIf::NoneFailed => !edges.is_empty() && edges.iter().all(|s| *s == S::Skipped),
        RunIf::AllSucceeded => edges.contains(&S::Skipped),
    };
    if skipped {
        return Readiness::Skip;
    }
    Readiness::Ready
}

/// What happens to a task after one of its attempts ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AfterAttempt {
    /// It succeeded.
    Succeeded,
    /// It runs again at this moment (`up_for_retry` until then); the attempt used a retry.
    RetryAt(Timestamp),
    /// It failed after its last retry (or with a failure certain to repeat).
    Failed,
    /// It runs again now, without using a retry.
    Requeue,
    /// It was cancelled.
    Cancelled,
}

impl AfterAttempt {
    /// Whether the attempt used one of the task's retries.
    #[must_use]
    pub const fn uses_retry(self) -> bool {
        matches!(self, Self::RetryAt(_) | Self::Failed)
    }
}

/// Decides what follows an attempt with `status` (and the error kind schedr recognised, if
/// any), given how many retries the task used before it and whether the task or its run is
/// being cancelled (then nothing runs again, a failure stays a failure, and an attempt lost
/// with its worker is cancelled).
#[must_use]
pub fn after_attempt(
    status: AttemptStatus,
    error_kind: Option<&str>,
    retries_used: u32,
    settings: &TaskSettings,
    cancelling: bool,
    now: Timestamp,
) -> AfterAttempt {
    match status.retry_rule() {
        RetryRule::NotNeeded => AfterAttempt::Succeeded,
        RetryRule::Never => AfterAttempt::Cancelled,
        RetryRule::RequeueWithoutRetry if cancelling => AfterAttempt::Cancelled,
        RetryRule::RequeueWithoutRetry => AfterAttempt::Requeue,
        // The worker went while the task was being stopped: stopped, as asked.
        RetryRule::UsesRetry if cancelling && status == AttemptStatus::Lost => {
            AfterAttempt::Cancelled
        }
        RetryRule::UsesRetry => {
            let repeats = error_kind.is_some_and(|kind| NOT_RETRIED_ERRORS.contains(&kind));
            if cancelling || repeats || retries_used >= settings.retries {
                return AfterAttempt::Failed;
            }
            let delay = SignedDuration::from_millis(
                i64::try_from(settings.retry_delay_ms).unwrap_or(i64::MAX),
            );
            AfterAttempt::RetryAt(now.checked_add(delay).unwrap_or(Timestamp::MAX))
        }
    }
}

/// The run's result once every task has finished; `None` while some task hasn't.
#[must_use]
pub fn run_result(states: &[TaskInstanceState]) -> Option<RunState> {
    use TaskInstanceState as S;
    if states.iter().any(|s| !s.is_final()) {
        return None;
    }
    Some(if states.contains(&S::Failed) {
        RunState::Failed
    } else if states.contains(&S::Cancelled) {
        RunState::Cancelled
    } else {
        RunState::Success
    })
}

/// A run's task states, moved forward by the DAG's structure.
#[derive(Debug, Clone)]
pub struct RunProgress<'a> {
    dag: &'a Dag,
    states: BTreeMap<TaskId, TaskInstanceState>,
    /// The outcomes of the tasks that succeeded (`SUCCESS` where none is known).
    outcomes: BTreeMap<TaskId, String>,
    /// What each `@branch` task that succeeded chose (its output).
    chosen: BTreeMap<TaskId, Vec<TaskId>>,
}

impl<'a> RunProgress<'a> {
    /// The progress of a run of `dag` with the given task states (tasks not given are
    /// `scheduled`).
    #[must_use]
    pub fn new(dag: &'a Dag, states: BTreeMap<TaskId, TaskInstanceState>) -> Self {
        let mut all: BTreeMap<TaskId, TaskInstanceState> = dag
            .tasks
            .iter()
            .map(|t| (t.id.clone(), TaskInstanceState::Scheduled))
            .collect();
        all.extend(states);
        Self {
            dag,
            states: all,
            outcomes: BTreeMap::new(),
            chosen: BTreeMap::new(),
        }
    }

    /// The same, with what the `@branch` tasks that succeeded chose.
    #[must_use]
    pub fn with_chosen(mut self, chosen: BTreeMap<TaskId, Vec<TaskId>>) -> Self {
        self.chosen = chosen;
        self
    }

    /// The same, with the outcomes of the tasks that succeeded.
    #[must_use]
    pub fn with_outcomes(mut self, outcomes: BTreeMap<TaskId, String>) -> Self {
        self.outcomes = outcomes;
        self
    }

    /// Records a task's outcome (after an attempt that succeeded).
    pub fn set_outcome(&mut self, task: &TaskId, outcome: &str) {
        self.outcomes.insert(task.clone(), outcome.to_owned());
    }

    /// The state an edge into a task counts as: its upstream task's, or skipped when the
    /// upstream succeeded with an outcome the edge isn't taken on.
    fn edge_state(&self, edge: &schedr_model::Edge) -> TaskInstanceState {
        let state = self
            .states
            .get(&edge.from)
            .copied()
            .unwrap_or(TaskInstanceState::Scheduled);
        if state == TaskInstanceState::Success && edge.kind == schedr_model::EdgeKind::Selection {
            // Taken when the branch chose its target (one it chose nothing for: not taken).
            let chosen = self
                .chosen
                .get(&edge.from)
                .is_some_and(|chosen| chosen.contains(&edge.to));
            return if chosen {
                state
            } else {
                TaskInstanceState::Skipped
            };
        }
        if state == TaskInstanceState::Success {
            let outcome = self
                .outcomes
                .get(&edge.from)
                .map_or(DEFAULT_OUTCOME, String::as_str);
            if !edge.taken_on(outcome) {
                return TaskInstanceState::Skipped;
            }
        }
        state
    }

    /// Every task's state.
    #[must_use]
    pub const fn states(&self) -> &BTreeMap<TaskId, TaskInstanceState> {
        &self.states
    }

    /// Sets a task's state (after an attempt, or a retry delay that passed).
    pub fn set(&mut self, task: &TaskId, state: TaskInstanceState) {
        self.states.insert(task.clone(), state);
    }

    /// Moves `scheduled` tasks on as far as their upstream tasks allow: ready tasks become
    /// `queued` (empty tasks `success`, completed by the master), others `skipped`,
    /// `upstream_failed` or `cancelled`; repeated until nothing changes. Returns the tasks
    /// whose state changed, with their new state, in the order they changed.
    pub fn settle(&mut self) -> Vec<(TaskId, TaskInstanceState)> {
        let mut changes: Vec<(TaskId, TaskInstanceState)> = Vec::new();
        loop {
            let mut progressed = false;
            for task in &self.dag.tasks {
                if self.states[&task.id] != TaskInstanceState::Scheduled {
                    continue;
                }
                let edges: Vec<TaskInstanceState> = self
                    .dag
                    .edges_into(&task.id)
                    .map(|edge| self.edge_state(edge))
                    .collect();
                let next = match readiness(&edges, task.settings.run_if) {
                    Readiness::Wait => continue,
                    Readiness::Ready if task.kind == TaskKind::Empty => TaskInstanceState::Success,
                    Readiness::Ready => TaskInstanceState::Queued,
                    Readiness::Skip => TaskInstanceState::Skipped,
                    Readiness::UpstreamFailed => TaskInstanceState::UpstreamFailed,
                    Readiness::Cancel => TaskInstanceState::Cancelled,
                };
                self.states.insert(task.id.clone(), next);
                changes.retain(|(id, _)| id != &task.id);
                changes.push((task.id.clone(), next));
                progressed = true;
            }
            if !progressed {
                return changes;
            }
        }
    }

    /// Cancels the run: every task that hasn't started becomes `cancelled`. Returns those
    /// changes, and the tasks still running (their attempts are to be cancelled; they finish
    /// on their own and their results count).
    pub fn cancel(&mut self) -> (Vec<(TaskId, TaskInstanceState)>, Vec<TaskId>) {
        use TaskInstanceState as S;
        let mut changes = Vec::new();
        let mut running = Vec::new();
        for (id, state) in &mut self.states {
            match state {
                S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                    *state = S::Cancelled;
                    changes.push((id.clone(), S::Cancelled));
                }
                S::Running | S::CleaningUp => running.push(id.clone()),
                S::Success | S::Failed | S::Cancelled | S::Skipped | S::UpstreamFailed => {}
            }
        }
        (changes, running)
    }

    /// The run's result, once every task has finished.
    #[must_use]
    pub fn result(&self) -> Option<RunState> {
        run_result(&self.states.values().copied().collect::<Vec<_>>())
    }
}
