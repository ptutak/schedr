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
//!   task (and every instance of a mapped one) has finished.
//! - A **mapped task** (decisions 70, 74, 75): its `-1` state is the task as a whole. Once
//!   its upstream allows (a lane counting as ready once its upstream has expanded) it is
//!   **expanded**: its instances are created (or it fails, or succeeds with none), and it is
//!   `running`. Each instance waits for the whole-task edges and for its lanes' instances; an
//!   instance whose lane isn't taken is skipped. An instance that fails (or whose lane
//!   failed) fails the step at once: the others are cancelled, running ones stopped. Once
//!   every instance has finished, the task applies its own `run_if` to them as to edges.

use std::collections::{BTreeMap, BTreeSet};

use jiff::{SignedDuration, Timestamp};
use schedr_model::{
    AttemptStatus, DEFAULT_OUTCOME, Dag, Edge, RetryRule, RunIf, RunState, Task, TaskId,
    TaskInstanceState, TaskKind, TaskSettings,
};

use crate::mapping::{self, Dim};

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

/// A task instance: its task and `map_index` (-1: an unmapped task, or a mapped one as a
/// whole).
pub type InstanceKey = (TaskId, i32);

/// The instances of a mapped task that expanded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instances {
    /// The size of each dimension (the first varying slowest).
    pub sizes: Vec<usize>,
    /// Each instance's state, by `map_index`.
    pub states: Vec<TaskInstanceState>,
    /// Each instance's outcome, once it succeeded.
    pub outcomes: Vec<Option<String>>,
}

/// How a mapped task expands, as the master resolved what it is mapped over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expansion {
    /// Instances over these dimensions' sizes (none of them: a success with `[]`).
    Sizes(Vec<usize>),
    /// It can't: it fails, for this reason (decision 78).
    Fail(String),
}

/// The states of a run's tasks and instances at one moment, to tell what changed since.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    tasks: BTreeMap<TaskId, TaskInstanceState>,
    instances: BTreeMap<TaskId, Vec<TaskInstanceState>>,
}

/// What changed since a [`Snapshot`]: (task, `map_index`, new state).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changes {
    /// Rows that existed and changed.
    pub updated: Vec<(TaskId, i32, TaskInstanceState)>,
    /// Instances created by an expansion, in their current state.
    pub created: Vec<(TaskId, i32, TaskInstanceState)>,
}

/// A run's task states, moved forward by the DAG's structure.
#[derive(Debug, Clone)]
pub struct RunProgress<'a> {
    dag: &'a Dag,
    /// Each task's state (a mapped task's as a whole: its `-1` row).
    states: BTreeMap<TaskId, TaskInstanceState>,
    /// The outcomes of the tasks that succeeded (`SUCCESS` where none is known).
    outcomes: BTreeMap<TaskId, String>,
    /// What each `@branch` task that succeeded chose (its output).
    chosen: BTreeMap<TaskId, Vec<TaskId>>,
    /// The instances of the mapped tasks that expanded.
    instances: BTreeMap<TaskId, Instances>,
    /// Mapped tasks ready to expand: the master resolves what they map over, then
    /// [`Self::expand`].
    to_expand: BTreeSet<TaskId>,
    /// Running instances to stop: their step failed (decision 75a).
    stops: Vec<(TaskId, usize)>,
    /// Why mapped tasks failed without an attempt.
    reasons: BTreeMap<TaskId, String>,
    /// Whether settling a mapped task moved one of its instances (a step of [`Self::settle`]).
    instance_moved: bool,
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
            instances: BTreeMap::new(),
            to_expand: BTreeSet::new(),
            stops: Vec::new(),
            reasons: BTreeMap::new(),
            instance_moved: false,
        }
    }

    /// The same, with the instances of mapped tasks that expanded.
    #[must_use]
    pub fn with_instances(mut self, instances: BTreeMap<TaskId, Instances>) -> Self {
        self.instances = instances;
        self
    }

    /// A mapped task's instances, once it expanded.
    #[must_use]
    pub fn instances(&self, task: &TaskId) -> Option<&Instances> {
        self.instances.get(task)
    }

    /// Sets an instance's state (after one of its attempts).
    pub fn set_instance(&mut self, task: &TaskId, index: usize, state: TaskInstanceState) {
        if let Some(slot) = self
            .instances
            .get_mut(task)
            .and_then(|i| i.states.get_mut(index))
        {
            *slot = state;
        }
    }

    /// Records an instance's outcome (after an attempt that succeeded).
    pub fn set_instance_outcome(&mut self, task: &TaskId, index: usize, outcome: &str) {
        if let Some(slot) = self
            .instances
            .get_mut(task)
            .and_then(|i| i.outcomes.get_mut(index))
        {
            *slot = Some(outcome.to_owned());
        }
    }

    /// The mapped tasks ready to expand, taken (each comes back from [`Self::settle`] until
    /// expanded).
    pub fn take_expansions(&mut self) -> Vec<TaskId> {
        std::mem::take(&mut self.to_expand).into_iter().collect()
    }

    /// Expands a mapped task as the master resolved it: its instances `scheduled` (none: a
    /// success with `[]`), or failed with a reason.
    pub fn expand(&mut self, task: &TaskId, expansion: Expansion) {
        use TaskInstanceState as S;
        self.to_expand.remove(task);
        match expansion {
            Expansion::Fail(reason) => {
                self.states.insert(task.clone(), S::Failed);
                self.reasons.insert(task.clone(), reason);
            }
            Expansion::Sizes(sizes) => {
                let n = mapping::count(&sizes).unwrap_or(0);
                if n == 0 {
                    self.states.insert(task.clone(), S::Success);
                    self.outcomes
                        .insert(task.clone(), DEFAULT_OUTCOME.to_owned());
                    return;
                }
                self.states.insert(task.clone(), S::Running);
                // Instances already there (stored before their whole task's row was) keep
                // their states: they may be running or done.
                let instances = self
                    .instances
                    .entry(task.clone())
                    .or_insert_with(|| Instances {
                        sizes: Vec::new(),
                        states: vec![S::Scheduled; n],
                        outcomes: vec![None; n],
                    });
                instances.sizes = sizes;
            }
        }
    }

    /// Why a mapped task failed without an attempt, if it did.
    #[must_use]
    pub fn reason(&self, task: &TaskId) -> Option<&str> {
        self.reasons.get(task).map(String::as_str)
    }

    /// Running instances to stop, taken: their step failed (decision 75a).
    pub fn take_stops(&mut self) -> Vec<(TaskId, usize)> {
        std::mem::take(&mut self.stops)
    }

    /// Every state now, to tell later what changed.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            tasks: self.states.clone(),
            instances: self
                .instances
                .iter()
                .map(|(task, i)| (task.clone(), i.states.clone()))
                .collect(),
        }
    }

    /// What changed since `before`: rows to update, and instances an expansion created.
    #[must_use]
    pub fn changes_since(&self, before: &Snapshot) -> Changes {
        let mut changes = Changes::default();
        for (task, state) in &self.states {
            if before.tasks.get(task) != Some(state) {
                changes.updated.push((task.clone(), -1, *state));
            }
        }
        for (task, instances) in &self.instances {
            let old = before.instances.get(task);
            for (index, state) in instances.states.iter().enumerate() {
                let map_index = i32::try_from(index).unwrap_or(i32::MAX);
                match old.and_then(|o| o.get(index)) {
                    None => changes.created.push((task.clone(), map_index, *state)),
                    Some(was) if was != state => {
                        changes.updated.push((task.clone(), map_index, *state));
                    }
                    Some(_) => {}
                }
            }
        }
        changes
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
    /// `upstream_failed` or `cancelled`; mapped tasks ready to expand are listed for
    /// [`Self::take_expansions`], and expanded ones move their instances on; repeated until
    /// nothing changes. Returns the tasks (as a whole) whose state changed, with their new
    /// state, in the order they changed.
    pub fn settle(&mut self) -> Vec<(TaskId, TaskInstanceState)> {
        let mut changes: Vec<(TaskId, TaskInstanceState)> = Vec::new();
        let dag = self.dag;
        loop {
            let mut progressed = false;
            for task in &dag.tasks {
                if mapping::is_mapped(task) {
                    if let Some(next) = self.settle_mapped(task) {
                        self.states.insert(task.id.clone(), next);
                        changes.retain(|(id, _)| id != &task.id);
                        changes.push((task.id.clone(), next));
                        progressed = true;
                    }
                    progressed |= std::mem::take(&mut self.instance_moved);
                    continue;
                }
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

    /// Cancels the run: every task (and instance) that hasn't started becomes `cancelled`.
    /// Returns the tasks' changes, and the tasks and instances still running, by `map_index`
    /// (their attempts are to be cancelled; they finish on their own and their results
    /// count). A mapped task as a whole ends once its instances have ([`Self::settle`]).
    pub fn cancel(&mut self) -> (Vec<(TaskId, TaskInstanceState)>, Vec<InstanceKey>) {
        use TaskInstanceState as S;
        let mut changes = Vec::new();
        let mut running = Vec::new();
        for (id, state) in &mut self.states {
            match state {
                S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                    *state = S::Cancelled;
                    changes.push((id.clone(), S::Cancelled));
                }
                S::Running | S::CleaningUp if !self.instances.contains_key(id) => {
                    running.push((id.clone(), -1));
                }
                _ => {}
            }
        }
        for (id, instances) in &mut self.instances {
            for (index, state) in instances.states.iter_mut().enumerate() {
                match state {
                    S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                        *state = S::Cancelled;
                    }
                    S::Running | S::CleaningUp => {
                        running.push((id.clone(), i32::try_from(index).unwrap_or(i32::MAX)));
                    }
                    _ => {}
                }
            }
        }
        (changes, running)
    }

    /// The run's result, once every task and instance has finished.
    #[must_use]
    pub fn result(&self) -> Option<RunState> {
        let all: Vec<TaskInstanceState> = self
            .states
            .values()
            .copied()
            .chain(
                self.instances
                    .values()
                    .flat_map(|i| i.states.iter().copied()),
            )
            .collect();
        run_result(&all)
    }

    /// A mapped task's next state as a whole, if it changes: expansion asked for, instances
    /// moved on (noted in `instance_moved`), a failed step, or the result of its instances.
    fn settle_mapped(&mut self, task: &Task) -> Option<TaskInstanceState> {
        use TaskInstanceState as S;
        match self.states[&task.id] {
            S::Scheduled => {
                let edges: Vec<TaskInstanceState> = self
                    .dag
                    .edges_into(&task.id)
                    .map(|edge| self.whole_edge_state(edge))
                    .collect();
                match readiness(&edges, task.settings.run_if) {
                    Readiness::Wait => None,
                    Readiness::Ready => {
                        self.to_expand.insert(task.id.clone());
                        None
                    }
                    Readiness::Skip => Some(S::Skipped),
                    Readiness::UpstreamFailed => Some(S::UpstreamFailed),
                    Readiness::Cancel => Some(S::Cancelled),
                }
            }
            S::Running if self.instances.contains_key(&task.id) => self.settle_instances(task),
            _ => None,
        }
    }

    /// An edge into a mapped task that hasn't expanded: a lane counts as ready once its
    /// upstream has expanded (its count is known), any other edge as usual.
    fn whole_edge_state(&self, edge: &Edge) -> TaskInstanceState {
        use TaskInstanceState as S;
        if !mapping::is_lane(self.dag, edge) {
            return self.edge_state(edge);
        }
        match self.states.get(&edge.from).copied().unwrap_or(S::Scheduled) {
            S::Running if self.instances.contains_key(&edge.from) => S::Success,
            state => state,
        }
    }

    /// Moves an expanded task's instances on; returns its new state as a whole, if any.
    fn settle_instances(&mut self, task: &Task) -> Option<TaskInstanceState> {
        use TaskInstanceState as S;
        let dims = mapping::dims(self.dag, task);
        let sizes = self.instances[&task.id].sizes.clone();
        let count = self.instances[&task.id].states.len();
        for index in 0..count {
            if self.instances[&task.id].states[index] != S::Scheduled {
                continue;
            }
            let place = mapping::coordinates(index, &sizes);
            let mut lane_skipped = false;
            let mut edges = Vec::new();
            let dag = self.dag;
            for edge in dag.edges_into(&task.id) {
                if !mapping::is_lane(dag, edge) {
                    edges.push(self.edge_state(edge));
                    continue;
                }
                let state = dims
                    .iter()
                    .position(|(_, dim)| *dim == Dim::Lane(&edge.from))
                    .map_or(S::Scheduled, |d| self.lane_state(edge, place[d]));
                lane_skipped |= state == S::Skipped;
                edges.push(state);
            }
            let next = match readiness(&edges, task.settings.run_if) {
                Readiness::Wait => continue,
                Readiness::Ready if lane_skipped => S::Skipped,
                Readiness::Ready => S::Queued,
                Readiness::Skip => S::Skipped,
                Readiness::UpstreamFailed => S::UpstreamFailed,
                Readiness::Cancel => S::Cancelled,
            };
            self.set_instance(&task.id, index, next);
            self.instance_moved = true;
        }
        let states = self.instances[&task.id].states.clone();
        let failed = states.contains(&S::Failed);
        if failed || states.contains(&S::UpstreamFailed) {
            // The step failed: the others are cancelled, running ones stopped (75a).
            for (index, state) in states.iter().enumerate() {
                match state {
                    S::Scheduled | S::Queued | S::UpForRetry | S::Unschedulable => {
                        self.set_instance(&task.id, index, S::Cancelled);
                    }
                    S::Running | S::CleaningUp => self.stops.push((task.id.clone(), index)),
                    _ => {}
                }
            }
            self.instance_moved = true;
            return Some(if failed { S::Failed } else { S::UpstreamFailed });
        }
        if states.iter().any(|s| !s.is_final()) {
            return None;
        }
        // Its own rule over its instances, as over edges (75b D).
        let skipped = match task.settings.run_if {
            RunIf::NoneFailed => states.iter().all(|s| *s == S::Skipped),
            RunIf::AllSucceeded => states.contains(&S::Skipped),
        };
        let next = if states.contains(&S::Cancelled) {
            S::Cancelled
        } else if skipped {
            S::Skipped
        } else {
            self.outcomes
                .insert(task.id.clone(), DEFAULT_OUTCOME.to_owned());
            S::Success
        };
        self.instance_moved = true;
        Some(next)
    }

    /// The state a lane into an instance counts as: its upstream instance's, or skipped when
    /// that one succeeded with an outcome the edge isn't taken on.
    fn lane_state(&self, edge: &Edge, place: usize) -> TaskInstanceState {
        use TaskInstanceState as S;
        let Some(upstream) = self.instances.get(&edge.from) else {
            return S::Scheduled;
        };
        let state = upstream.states.get(place).copied().unwrap_or(S::Scheduled);
        if state != S::Success {
            return state;
        }
        let outcome = upstream
            .outcomes
            .get(place)
            .and_then(Option::as_deref)
            .unwrap_or(DEFAULT_OUTCOME);
        if edge.taken_on(outcome) {
            S::Success
        } else {
            S::Skipped
        }
    }
}
