//! How a run's tasks move forward: readiness (`NoneFailed`), retries, cancelling and the run
//! result (see archive/PROJECT.md, Outcomes, failures and joins, and Task assignment).
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
    AttemptStatus, DEFAULT_OUTCOME, Dag, Edge, EdgeKind, RetryRule, RunIf, RunState, Task, TaskId,
    TaskInstanceState, TaskKind, TaskSettings,
};

use crate::mapping::{self, Dim};

#[cfg(test)]
mod tests;

/// Error kinds of failures certain to repeat: never retried. A secret that doesn't exist or
/// isn't permitted, and what the same inputs always do again. (`secret_unavailable`, a secret
/// storage that is down, and `import_failed` / `task_not_found`, which new code can fix, are
/// retried.) A `TriggerDagOperator`'s parameters refused, or a target that takes no such run
/// (decisions 379, 398); a missing, held or paused target is retried.
pub const NOT_RETRIED_ERRORS: [&str; 13] = [
    schedr_model::runner::SWITCHED_OFF,
    schedr_model::runner::BLOCKED_EVENT_LOOP,
    "secret_not_found",
    "secret_denied",
    "connection_denied",
    "inputs_too_large",
    "render_failed",
    "input_mismatch",
    "output_not_serializable",
    "output_too_large",
    "result_not_stored",
    crate::dispatch::chained::PARAMS_REFUSED,
    crate::dispatch::chained::TARGET_REFUSED,
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

/// What the states of a `first_of`'s edges decide, `need` of them having to be taken (decision
/// 347): ready as soon as they are; waiting while they still can be; otherwise what stopped them.
#[must_use]
pub fn first_of_readiness(edges: &[TaskInstanceState], need: usize) -> Readiness {
    use TaskInstanceState as S;
    let taken = edges.iter().filter(|s| **s == S::Success).count();
    if taken >= need {
        return Readiness::Ready;
    }
    if taken + edges.iter().filter(|s| !s.is_final()).count() >= need {
        return Readiness::Wait;
    }
    if edges
        .iter()
        .any(|s| matches!(s, S::Failed | S::UpstreamFailed))
    {
        return Readiness::UpstreamFailed;
    }
    if edges.contains(&S::Cancelled) {
        return Readiness::Cancel;
    }
    Readiness::Skip
}

/// How many edges a `first_of` needs taken.
pub(crate) fn need_of(at_least: Option<u32>) -> usize {
    at_least.map_or(1, |n| usize::try_from(n).unwrap_or(usize::MAX))
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

/// Of two verdicts on a task, the one that decides: a failure, then a cancel, then waiting,
/// then a skip; ready only when both are.
const fn strongest(a: Readiness, b: Readiness) -> Readiness {
    const fn rank(r: Readiness) -> u8 {
        match r {
            Readiness::UpstreamFailed => 4,
            Readiness::Cancel => 3,
            Readiness::Wait => 2,
            Readiness::Skip => 1,
            Readiness::Ready => 0,
        }
    }
    if rank(a) >= rank(b) { a } else { b }
}

/// The reason an empty cleanup gets, where its log would be (decision 276).
pub const EMPTY_CLEANUP: &str = "no cleanup declared: nothing to clean";

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
    /// Whether a cleanup of the run failed.
    failed_cleanup: bool,
    /// Whether the run is being cancelled: it ends `cancelled` even if nothing was left to
    /// cancel but its resources' cleanups (decision 277).
    cancelling: bool,
    /// Running instances to stop: their step failed (decision 75a).
    stops: Vec<(TaskId, usize)>,
    /// Why mapped tasks failed without an attempt.
    reasons: BTreeMap<TaskId, String>,
    /// Whether settling a mapped task moved one of its instances (a step of [`Self::settle`]).
    instance_moved: bool,
    /// The `first_of` tasks that ended now, with the upstream tasks that can have ended them.
    winners: BTreeMap<TaskId, Vec<TaskId>>,
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
            failed_cleanup: false,
            cancelling: false,
            reasons: BTreeMap::new(),
            instance_moved: false,
            winners: BTreeMap::new(),
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

    /// A task's outcome, if it succeeded with one recorded.
    #[must_use]
    pub fn outcome(&self, task: &TaskId) -> Option<&str> {
        self.outcomes.get(task).map(String::as_str)
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
        // A mapped task's own outcome only says its instances ended: its `.on(...)` is
        // decided per instance ([`Self::edge_states`]); with none, the edge is taken, as the
        // task with an empty list succeeds.
        let mapped = self.dag.task(&edge.from).is_some_and(mapping::is_mapped);
        if state == TaskInstanceState::Success && !mapped {
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

    /// The states an edge counts as in the downstream task's rule: one, or for an edge
    /// narrowed with `.on(...)` from a mapped task that succeeded, one per instance, taken
    /// when it ended with one of those outcomes, so `RunIf::AllSucceeded` needs every
    /// instance to and `RunIf::NoneFailed` any (decision 120).
    fn edge_states(&self, edge: &Edge) -> Vec<TaskInstanceState> {
        use TaskInstanceState as S;
        if edge.kind == EdgeKind::Requirement {
            // Only stops a task ([`Self::required`]): never one of its rule's edges.
            return Vec::new();
        }
        let state = self.edge_state(edge);
        if state != S::Success
            || edge.on.is_empty()
            || edge.kind == schedr_model::EdgeKind::Selection
        {
            return vec![state];
        }
        match self.instances.get(&edge.from) {
            Some(instances) if !instances.states.is_empty() => (0..instances.states.len())
                .map(|place| {
                    if self.instance_taken(&edge.from, place, &edge.on) {
                        S::Success
                    } else {
                        S::Skipped
                    }
                })
                .collect(),
            _ => vec![state],
        }
    }

    /// The DAG it follows.
    #[must_use]
    pub const fn dag(&self) -> &'a Dag {
        self.dag
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
    /// nothing changes, or until a `first_of` ends ([`Self::take_winners`]: its output is
    /// made before what follows it moves). Returns the tasks (as a whole) whose state changed,
    /// with their new state, in the order they changed.
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
                    .flat_map(|edge| self.edge_states(edge))
                    .collect();
                let rule = match task.kind {
                    TaskKind::FirstOf { at_least } => first_of_readiness(&edges, need_of(at_least)),
                    _ => readiness(&edges, task.settings.run_if),
                };
                let next = match self.gate(task, rule) {
                    Readiness::Wait => continue,
                    Readiness::Ready if matches!(task.kind, TaskKind::FirstOf { .. }) => {
                        // One at a time, before anything after it moves: its output is made
                        // from its winners' and may not fit ([`Self::fail_first_of`]).
                        self.record_winners(task);
                        self.states
                            .insert(task.id.clone(), TaskInstanceState::Success);
                        changes.retain(|(id, _)| id != &task.id);
                        changes.push((task.id.clone(), TaskInstanceState::Success));
                        return changes;
                    }
                    Readiness::Ready if task.kind == TaskKind::Empty => {
                        if self.dag.resource_of_cleanup(&task.id).is_some() {
                            self.reasons
                                .insert(task.id.clone(), EMPTY_CLEANUP.to_owned());
                        }
                        TaskInstanceState::Success
                    }
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

    /// The upstream tasks a `first_of` that is ready can have ended with: those taken, in the
    /// order of its edges. Which of them did is told by when they ended ([`Self::take_winners`]).
    fn record_winners(&mut self, task: &Task) {
        let candidates: Vec<TaskId> = self
            .dag
            .edges_into(&task.id)
            .filter(|edge| edge.kind != EdgeKind::Requirement)
            .filter(|edge| self.edge_state(edge) == TaskInstanceState::Success)
            .map(|edge| edge.from.clone())
            .collect();
        self.winners.insert(task.id.clone(), candidates);
    }

    /// A `first_of` whose output can't be kept fails, with why.
    pub fn fail_first_of(&mut self, task: &TaskId, reason: String) {
        self.states.insert(task.clone(), TaskInstanceState::Failed);
        self.outcomes.remove(task);
        self.reasons.insert(task.clone(), reason);
    }

    /// The `first_of` tasks ended since this was last asked, each with what can have ended it.
    pub fn take_winners(&mut self) -> BTreeMap<TaskId, Vec<TaskId>> {
        std::mem::take(&mut self.winners)
    }

    /// Cancels the run: every task (and instance) that hasn't started becomes `cancelled`.
    /// Returns the tasks' changes, and the tasks and instances still running, by `map_index`
    /// (their attempts are to be cancelled; they finish on their own and their results
    /// count). A mapped task as a whole ends once its instances have ([`Self::settle`]).
    pub fn cancel(&mut self) -> (Vec<(TaskId, TaskInstanceState)>, Vec<InstanceKey>) {
        use TaskInstanceState as S;
        let mut changes = Vec::new();
        let mut running = Vec::new();
        self.cancelling = true;
        let dag = self.dag;
        for (id, state) in &mut self.states {
            if dag.resource_of_cleanup(id).is_some() {
                // A resource set up is freed all the same (decision 277); one that wasn't is
                // `cancelled` by its setup's edge.
                continue;
            }
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
        run_result(&all).map(|result| {
            if self.failed_cleanup {
                RunState::Failed
            } else if self.cancelling && result == RunState::Success {
                RunState::Cancelled
            } else {
                result
            }
        })
    }

    /// Notes that the run is being cancelled: it can't end `success`.
    #[must_use]
    pub const fn with_cancelling(mut self, cancelling: bool) -> Self {
        self.cancelling = cancelling;
        self
    }

    /// Notes that a cleanup failed: the run ends `failed` (see archive/PROJECT.md, Run result).
    #[must_use]
    pub const fn with_failed_cleanup(mut self, failed: bool) -> Self {
        self.failed_cleanup = failed;
        self
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
                    .flat_map(|edge| self.whole_edge_state(edge))
                    .collect();
                match self.gate(task, readiness(&edges, task.settings.run_if)) {
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
    fn whole_edge_state(&self, edge: &Edge) -> Vec<TaskInstanceState> {
        use TaskInstanceState as S;
        if edge.kind == EdgeKind::Requirement || !mapping::is_lane(self.dag, edge) {
            return self.whole_edge(edge);
        }
        vec![
            match self.states.get(&edge.from).copied().unwrap_or(S::Scheduled) {
                S::Running if self.instances.contains_key(&edge.from) => S::Success,
                state => state,
            },
        ]
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
            // An element narrowed with `.on(...)` that its upstream instance didn't end with
            // (decision 119).
            let mut lane_skipped = dims.iter().enumerate().any(|(d, (_, dim))| {
                mapping::narrowed_whole(self.dag, dim)
                    .is_some_and(|(upstream, on)| !self.instance_taken(upstream, place[d], on))
            });
            let mut edges = Vec::new();
            let dag = self.dag;
            for edge in dag.edges_into(&task.id) {
                if edge.kind == EdgeKind::Requirement || !mapping::is_lane(dag, edge) {
                    edges.extend(self.whole_edge(edge));
                    continue;
                }
                let state = dims
                    .iter()
                    .position(|(_, dim)| *dim == Dim::Lane(&edge.from))
                    .map_or(S::Scheduled, |d| self.lane_state(edge, place[d]));
                lane_skipped |= state == S::Skipped;
                edges.push(state);
            }
            let next = match self.gate(task, readiness(&edges, task.settings.run_if)) {
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

    /// What a task's requirement edges and, for a resource's cleanup, its users say on top of
    /// `rule`, its trigger rule over its other edges. A requirement never lets a task run, it
    /// can only stop it: its setup failed (`upstream_failed`), was cancelled, or was skipped
    /// (skipped, whatever the rule). A cleanup that could run waits until every user is done
    /// (decision 259): a task that will never run again counts, an `unschedulable` one doesn't.
    fn gate(&self, task: &Task, rule: Readiness) -> Readiness {
        use TaskInstanceState as S;
        let state = |id: &TaskId| self.states.get(id).copied().unwrap_or(S::Scheduled);
        let required: Vec<S> = self
            .dag
            .edges_into(&task.id)
            .filter(|e| e.kind == EdgeKind::Requirement)
            .map(|e| state(&e.from))
            .collect();
        let gated = if required
            .iter()
            .any(|s| matches!(s, S::Failed | S::UpstreamFailed))
        {
            Readiness::UpstreamFailed
        } else if required.contains(&S::Cancelled) {
            Readiness::Cancel
        } else if required.iter().any(|s| !s.is_final()) {
            Readiness::Wait
        } else if required.contains(&S::Skipped) {
            Readiness::Skip
        } else {
            Readiness::Ready
        };
        let decided = strongest(rule, gated);
        if decided != Readiness::Ready {
            return decided;
        }
        // A mapped user is done when its instances are too: one failing fails it as a whole
        // while the others are still being stopped.
        let done = |user: &TaskId| {
            state(user).is_final()
                && self
                    .instances
                    .get(user)
                    .is_none_or(|i| i.states.iter().all(|s| s.is_final()))
        };
        match self.dag.resource_of_cleanup(&task.id) {
            Some(resource) if !resource.users.iter().all(done) => Readiness::Wait,
            _ => Readiness::Ready,
        }
    }

    /// An edge that isn't a lane: as usual, except that one whose outcomes are taken per
    /// element counts as its upstream task's state ([`mapping::narrows_per_element`]).
    fn whole_edge(&self, edge: &Edge) -> Vec<TaskInstanceState> {
        if edge.kind == EdgeKind::Requirement {
            return Vec::new();
        }
        if mapping::narrows_per_element(self.dag, edge) {
            return vec![self.edge_state(edge)];
        }
        self.edge_states(edge)
    }

    /// Whether instance `place` of the mapped task `upstream` succeeded with one of `on`.
    fn instance_taken(&self, upstream: &TaskId, place: usize, on: &[String]) -> bool {
        let Some(instances) = self.instances.get(upstream) else {
            return false;
        };
        if instances.states.get(place) != Some(&TaskInstanceState::Success) {
            return false;
        }
        let outcome = instances
            .outcomes
            .get(place)
            .and_then(Option::as_deref)
            .unwrap_or(DEFAULT_OUTCOME);
        on.iter().any(|o| o == outcome)
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
