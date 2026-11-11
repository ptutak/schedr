//! Giving tasks to workers and handling what workers report about them (see archive/PROJECT.md,
//! Task assignment and Task rejections).
//!
//! - **Assign:** ready tasks, oldest ready first, each to the worker chosen by [`Workers::pick`];
//!   the assignment is stored before `AssignTask` is sent.
//! - **`Started`** means accepted. For an attempt that was taken back or ended, the worker is
//!   told to kill it.
//! - **`Rejected`** takes the assignment back (it isn't a try): a temporary reason queues the
//!   task again at once; "code not there yet" after [`CODE_RETRY_DELAY`], and `unschedulable`
//!   once every eligible worker said so and the code has been missing for the code wait; a
//!   worker-specific reason leaves that worker out for the task, which becomes
//!   `unschedulable` when every eligible worker rejected it, until the set of workers changes
//!   or a user retries it. A task being cancelled is `cancelled` instead.
//! - **Only the newest code runs:** a task goes only to workers that have its root at the
//!   root's current hash (and sends that hash for the worker to check), with a schedr version
//!   at least its parse's.
//! - **A task that waits** shows why: no active worker serves its queue, none is recent
//!   enough, none has the newest code yet, every one rejected it, or no free slot.
//! - **`Finished`** is stored (the first one counts) and always acknowledged.
//! - An attempt with no `Started` within the worker lease ends `assignment_expired` and the
//!   task runs again without using a retry; a lost worker's attempts end `lost`.
//!
//! The dispatcher only builds messages; the gRPC server sends them (milestone 1.14).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use prost::Message as _;
use schedr_db::attempts::{AttemptEnd, CleanupEnd, ReadyInstance, StoredAttempt};
use schedr_db::run_of::{Matching, OtherRun};
use schedr_db::{Database, DbError};
use schedr_model::TaskKind;
use schedr_model::connection::Registry;
use schedr_model::ctx::MAX_CTX_MESSAGE_SIZE;
use schedr_model::runner::{
    CleanupHandle, DAG_FILE_MISSING, ErrorSummary, ProcessUsage, TASK_NOT_FOUND,
};
use schedr_model::task::{CleanupRun, RunOf, TaskCleanup};
use schedr_model::task::{SensorWaitFor, WaitSource};
use schedr_model::{
    AttemptStatus, CleanupResult, Dag, DagId, Reference, SchedrVersion, TaskId, TaskInstanceState,
};
use schedr_proto::convert::{
    RejectReaction, attempt_status_from_proto, cleanup_result_from_proto, reject_reaction,
    run_kind_to_proto,
};
use schedr_proto::pb;

use crate::core::Arrivals;
use crate::execution::{AttemptResult, Execution, ExecutionError, TaskCancel, TaskRetry, WaitEnd};
use crate::limits::Active;
use crate::mapping::{self, InputsError};
use crate::metrics::{MasterMetrics, ReasonLabels};
use crate::quiet::Wakes;
use crate::workers::{Needs, Workers};

pub(crate) mod chained;
mod event_wait;
pub(crate) use event_wait::event_value;
#[cfg(test)]
mod tests;

/// How long a worker has to report `Started` after an assignment, by default; the master uses
/// the worker lease (decision 281, [`Dispatcher::with_start_timeout`]).
pub const START_TIMEOUT: SignedDuration = SignedDuration::from_secs(10);

/// How long a task whose assignment failed on its own is left out of the rounds before it is
/// tried again, unless set otherwise (see [`Dispatcher::assign_into`]).
pub const DEFAULT_SET_ASIDE: Duration = Duration::from_mins(1);

/// How long a lost worker's cleanup waits before it moves, by default: the default cancel
/// grace and some slack (decision 256).
const DEFAULT_CLEANUP_MOVE_WAIT: Duration = Duration::from_secs(40);

/// The acknowledgement of `attempt_id`'s `Finished`, for `worker_id`: its result is stored.
#[must_use]
pub fn finished_ack(worker_id: &str, attempt_id: &str) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(pb::master_message::Message::FinishedAck(pb::FinishedAck {
                attempt_id: attempt_id.to_owned(),
            })),
            clock: None,
        },
    }
}

/// The error kind of a result the database refused (not retried).
const RESULT_NOT_STORED: &str = "result_not_stored";

/// Why a moved cleanup waits, shown as `unschedulable` (decision 244).
const NO_CLEANUP_WORKER: &str = "no worker for the cleanup";

/// What is done with the task results kept for a worker's attempts as it goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeptResults {
    /// They end the attempts.
    Used,
    /// The database refused one: left out, the attempts fail instead (else the worker could
    /// never be retired, nor the workers after it).
    Refused,
}

/// How a commit cut off by the time limit or a lost connection is read back: whether the work
/// is stored, checked at once, then every `interval` for up to `window`, since a server slow
/// enough to have cut it off may still be committing it (`read_back_window`,
/// `read_back_interval`). Never seen meanwhile: not stored (a stored round's attempts are
/// taken back after the start timeout and run again). The master holds its lock meanwhile:
/// only after a commit cut off, a database incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBack {
    /// How long it goes on.
    pub window: Duration,
    /// How often it checks (zero is taken as 1 ms).
    pub interval: Duration,
}

impl Default for ReadBack {
    /// For 3 s, every 500 ms.
    fn default() -> Self {
        Self {
            window: Duration::from_secs(3),
            interval: Duration::from_millis(500),
        }
    }
}

impl ReadBack {
    /// Whether the work is stored, by `stored` (true once it is).
    pub async fn stored<F, Fut>(self, mut stored: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let interval = self.interval.max(Duration::from_millis(1));
        let deadline = tokio::time::Instant::now() + self.window;
        loop {
            if stored().await {
                return true;
            }
            if tokio::time::Instant::now() + interval > deadline {
                return false;
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// The most assignments a round makes (a few statements each, about 5-13 ms in stress runs):
/// thousands of ready tasks and free slots go over a few rounds, the longest ready first
/// (B18). Steady state stays under it (about 16 a round in scenario 1).
pub const ASSIGNMENTS_PER_ROUND: usize = 50;

/// The most assignments a round takes back as never started (B18).
pub const EXPIRIES_PER_ROUND: usize = 50;

/// Why a task of a DAG in an ID conflict waits (B193).
pub const DAG_IN_CONFLICT: &str = "waiting: DAG ID conflict";

/// The most waiting reasons a round writes (each a statement, about 3.5 ms in stress runs): a
/// burst of runs leaves thousands of tasks waiting, and their reasons (only shown) are filled in
/// over the following rounds rather than in one long one (B18). The waiting counts in the
/// metrics still cover every task.
pub const REASONS_PER_ROUND: usize = 100;

/// When a task whose code wasn't on the worker yet is tried again: about the git-sync
/// interval plus the settle time.
pub const CODE_RETRY_DELAY: SignedDuration = SignedDuration::from_secs(35);

/// How long a task's code may be missing on every eligible worker before the task is
/// `unschedulable` (`[master] code_wait`).
pub const DEFAULT_CODE_WAIT: Duration = Duration::from_mins(10);

/// Error kinds of an attempt whose task or DAG file isn't in the code it ran with; when the
/// DAG changed after the run started, its error summary says how.
pub const CODE_GONE_ERRORS: [&str; 2] = [TASK_NOT_FOUND, DAG_FILE_MISSING];

/// Inputs up to this size travel inside `AssignTask`; bigger ones are fetched separately.
pub const INLINE_INPUTS_LIMIT: usize = 4 * 1024 * 1024;

/// Inputs over this size make the task fail (`inputs_too_large`, never retried).
pub const MAX_INPUTS: usize = 64 * 1024 * 1024;

/// The outcome of a wait step that ran out of time (`schedr.outcomes.TIMED_OUT_NAME`).
const TIMED_OUT_OUTCOME: &str = "TimedOut";

/// The least an `@sensor(wait_for=...)`'s call gets once it is handed a run that ended within its
/// `max_wait`, though little or none of that is left (`SCHEDR_HIDDEN__MASTER__HANDED_CALL_MIN`).
pub const DEFAULT_HANDED_CALL_MIN: Duration = Duration::from_mins(1);

/// An attempt's failure the master itself decides.
fn failed_by_master(kind: &str, exception: &str, why: &str) -> AttemptEnd {
    AttemptEnd {
        error_kind: Some(kind.to_owned()),
        error_summary: Some(ErrorSummary::json(exception, why)),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    }
}

/// The error kind of a task whose inputs are over [`MAX_INPUTS`].
pub const INPUTS_TOO_LARGE: &str = "inputs_too_large";

/// The error kind of a task whose inputs don't fit its parameters (the runner's too); not
/// retried.
pub const INPUT_MISMATCH: &str = "input_mismatch";

/// The grace time between SIGTERM and SIGKILL when an attempt is cancelled (master
/// configuration; this is the default).
pub const DEFAULT_CANCEL_GRACE: Duration = Duration::from_secs(30);

/// A message for one worker.
#[derive(Debug, Clone, PartialEq)]
pub struct Outgoing {
    /// The worker.
    pub worker_id: String,
    /// The message.
    pub message: pb::MasterMessage,
}

/// Why a dispatcher operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// Moving a run forward failed.
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    /// A message from a worker can't be understood.
    #[error("message from worker `{worker_id}`: {reason}")]
    BadMessage {
        /// The worker.
        worker_id: String,
        /// What is wrong.
        reason: String,
    },
}

impl DispatchError {
    /// Whether it is about the data of a write, which would be refused again
    /// ([`DbError::is_data_fault`]).
    #[must_use]
    pub fn is_data_fault(&self) -> bool {
        match self {
            Self::Db(error) | Self::Execution(ExecutionError::Db(error)) => error.is_data_fault(),
            _ => false,
        }
    }
}

impl DispatchError {
    /// Whether it is a commit whose answer was lost: it may have stored.
    #[must_use]
    pub const fn commit_unknown(&self) -> bool {
        matches!(self, Self::Db(DbError::CommitUnknown))
    }
}

/// Tasks given to workers and their reports handled.
#[derive(Debug)]
pub struct Dispatcher {
    db: Database,
    execution: Execution,
    /// Creates the runs `TriggerDagOperator`s start (the core's, shared, once set).
    scheduler: crate::scheduler::Scheduler,
    workers: Workers,
    /// Workers a restarted master read back that haven't registered with it yet: within the
    /// lease it gave them, they may come back with their attempts in their journals (F14).
    away: BTreeSet<String>,
    cancel_grace: Duration,
    /// After how many host kills in a row a wait step is switched off (0: never).
    switch_off_after: u32,
    /// How long a wait step without a `max_wait` waits (`[sensors] default_max_wait`).
    sensor_max_wait: Duration,
    /// Workers that rejected a task instance for a worker-specific reason, with the detail.
    rejections: BTreeMap<String, BTreeMap<String, String>>,
    /// Task instances whose code a worker didn't have: since when, and which workers said so.
    code_missing: BTreeMap<String, CodeMissing>,
    code_wait: SignedDuration,
    /// Each root's accepted content hashes (its current code, and newer code whose parse is on
    /// its way) and the schedr version of its current parse: its tasks go to workers at one of
    /// those hashes and of at least that version.
    current_code: BTreeMap<String, (BTreeSet<String>, Option<SchedrVersion>)>,
    /// Each root's connections from its current parse (decision 174), with that parse's
    /// content hash: a connections job goes to a worker having the root at it.
    connections: BTreeMap<String, (Option<String>, Arc<Registry>)>,
    metrics: MasterMetrics,
    /// Attempts whose `Started` arrived but waits for the master: never taken back (B11).
    arrivals: Arrivals,
    /// Task instances whose assignment failed on its own, and until when they are left out.
    set_aside: BTreeMap<String, Timestamp>,
    /// How long they are left out.
    set_aside_for: SignedDuration,
    /// How long a lost worker's cleanup waits before it moves (decision 256).
    cleanup_move_wait: SignedDuration,
    /// How long a worker has to report `Started` before its assignment is taken back.
    start_timeout: SignedDuration,
    /// Master time, which its timers count on (B228; see [`crate::clock`]).
    steady: crate::clock::Steady,
    /// When each unstarted assignment was stored, in master time: it is taken back a start
    /// timeout after that (B228), the deadline it was sent with. One this master didn't store
    /// (after a restart) counts from when it first sees it: later, never earlier.
    unstarted_since: BTreeMap<String, Timestamp>,
    /// When each cleanup waiting to move may go to another worker, in master time (D256, B228);
    /// after a restart, a whole wait from when it is first seen.
    move_not_before: BTreeMap<String, Timestamp>,
    /// How far behind its cursor an event sensor reads back, for an event committed late
    /// (B185; the triggers' `event_lookback`).
    event_lookback: SignedDuration,
    /// How a commit cut off is read back.
    read_back: ReadBack,
    /// `CancelAttempt`s for running instances whose mapped task's step failed (75a), sent
    /// with the next step.
    stops: Vec<Outgoing>,
    /// Mapped lists resolved during a step, shared by a task's instances.
    resolved: mapping::Resolved,
    /// Attempts a reconnecting worker runs whose start the database refused to record (B128):
    /// not taken back as never started while a worker holds them.
    adopted_unrecorded: BTreeSet<String>,
    /// The open attempts counted for the concurrency limits (decision 88A).
    active: Active,
    /// Each DAG's current version, read at each round with tasks ready: the limits across runs
    /// are its (87A).
    current_versions: BTreeMap<DagId, String>,
    /// The run each `@sensor(wait_for=...)` assigned this round is given, by instance
    /// (decision 353): its function's first argument.
    handing: BTreeMap<String, serde_json::Value>,
    /// The least such a call gets of its `max_wait`, the run having ended within it.
    handed_call_min: Duration,
    /// The largest message a worker takes (`[grpc] max_message_size`): a run handed to
    /// an `@sensor(wait_for=...)` goes inside its assignment.
    max_message_size: usize,
}

/// The dispatcher's memory that a worker's message changes (see [`Dispatcher::memory`]).
#[derive(Debug, Clone)]
pub struct Memory {
    workers: Workers,
    away: BTreeSet<String>,
    rejections: BTreeMap<String, BTreeMap<String, String>>,
    code_missing: BTreeMap<String, CodeMissing>,
    /// `CancelAttempt`s queued for the next step: a rolled-back change queues none (B51).
    stops: Vec<Outgoing>,
    active: Active,
}

/// What a step's assignments count as they go.
#[derive(Debug, Default)]
struct Tally {
    /// Tasks left waiting, by why.
    waiting: BTreeMap<&'static str, i64>,
    /// Assignments made (at most [`ASSIGNMENTS_PER_ROUND`]).
    assigned: usize,
    /// Waiting reasons to write, `(instance ID, reason)`: at most [`REASONS_PER_ROUND`],
    /// written together at the step's end.
    reasons: Vec<(String, String)>,
}

/// A task's code missing on workers.
#[derive(Debug, Clone)]
struct CodeMissing {
    since: Timestamp,
    by: BTreeMap<String, String>,
}

impl Dispatcher {
    /// A dispatcher over `db` with no workers.
    #[must_use]
    pub fn new(db: Database, cancel_grace: Duration) -> Self {
        Self {
            execution: Execution::new(db.clone()),
            scheduler: crate::scheduler::Scheduler::new(db.clone()),
            db,
            workers: Workers::new(),
            away: BTreeSet::new(),
            cancel_grace,
            switch_off_after: 3,
            sensor_max_wait: Duration::from_hours(24),
            rejections: BTreeMap::new(),
            code_missing: BTreeMap::new(),
            code_wait: SignedDuration::ZERO,
            current_code: BTreeMap::new(),
            connections: BTreeMap::new(),
            metrics: MasterMetrics::default(),
            arrivals: Arrivals::default(),
            adopted_unrecorded: BTreeSet::new(),
            set_aside: BTreeMap::new(),
            handing: BTreeMap::new(),
            handed_call_min: DEFAULT_HANDED_CALL_MIN,
            max_message_size: crate::service::DEFAULT_MAX_MESSAGE_SIZE,
            cleanup_move_wait: SignedDuration::try_from(DEFAULT_CLEANUP_MOVE_WAIT)
                .unwrap_or(SignedDuration::MAX),
            start_timeout: START_TIMEOUT,
            steady: crate::clock::as_given,
            unstarted_since: BTreeMap::new(),
            move_not_before: BTreeMap::new(),
            event_lookback: SignedDuration::from_mins(1),
            set_aside_for: SignedDuration::try_from(DEFAULT_SET_ASIDE)
                .unwrap_or(SignedDuration::MAX),
            read_back: ReadBack::default(),
            stops: Vec::new(),
            resolved: mapping::Resolved::new(),
            active: Active::new(),
            current_versions: BTreeMap::new(),
        }
        .with_code_wait(DEFAULT_CODE_WAIT)
    }

    /// How long a wait step without a `max_wait` waits.
    #[must_use]
    pub const fn with_sensor_max_wait(mut self, max_wait: Duration) -> Self {
        self.sensor_max_wait = max_wait;
        self
    }

    /// After how many host kills in a row a wait step is switched off (0: never).
    #[must_use]
    pub const fn with_switch_off_after(mut self, times: u32) -> Self {
        self.switch_off_after = times;
        self
    }

    /// Creates the runs `TriggerDagOperator`s start with `scheduler` (the core's, sharing its
    /// wakes), not one of its own.
    pub fn set_scheduler(&mut self, scheduler: crate::scheduler::Scheduler) {
        self.scheduler = scheduler;
    }

    /// Counts assignments, rejections, ended attempts, waiting tasks and finished runs in
    /// `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.execution.set_metrics(metrics.clone());
        self.metrics = metrics;
    }

    /// Wakes the DAGs of the runs it finishes through `wakes`.
    pub fn set_wakes(&mut self, wakes: Wakes) {
        self.execution.set_wakes(wakes);
    }

    /// A copy of what a worker's message changes in memory (worker slots, rejections, code
    /// missing, the cancels queued for the next step), to [`Self::restore`] if the message's
    /// transaction rolls back: memory then shows only what is stored (code review of 2.10b;
    /// metrics already counted stay).
    #[must_use]
    pub fn memory(&self) -> Memory {
        Memory {
            workers: self.workers.clone(),
            away: self.away.clone(),
            rejections: self.rejections.clone(),
            code_missing: self.code_missing.clone(),
            stops: self.stops.clone(),
            active: self.active.clone(),
        }
    }

    /// Drops the concurrency limits' counts, read again from the database at the next round
    /// with tasks ready: after a commit whose answer was lost (it may have stored what the
    /// counts were changed for, or not), where neither the counts before nor after can be
    /// trusted. Put back as they were, an expiry stored would leave its attempt counted for
    /// good, its DAG's limit holding tasks back until a restart (Codex's review of M4.4).
    pub fn recount(&mut self) {
        self.active = Active::new();
    }

    /// Puts back a copy taken by [`Self::memory`] after its transaction failed; for a commit
    /// whose answer was lost the counts are read again instead ([`Self::recount`]).
    pub fn undo(&mut self, memory: Memory, error: &DispatchError) {
        self.restore(memory);
        if error.commit_unknown() {
            self.recount();
        }
    }

    /// Puts back a copy taken by [`Self::memory`].
    pub fn restore(&mut self, memory: Memory) {
        self.workers = memory.workers;
        self.away = memory.away;
        self.rejections = memory.rejections;
        self.code_missing = memory.code_missing;
        self.stops = memory.stops;
        self.active = memory.active;
    }

    /// The attempts a `CancelAttempt` is queued for, sent at the next step (for the
    /// fault-injection harness's check).
    #[cfg(test)]
    pub(crate) fn queued_cancels(&self) -> Vec<(String, String)> {
        self.stops
            .iter()
            .filter_map(|o| match &o.message.message {
                Some(pb::master_message::Message::CancelAttempt(c)) => {
                    Some((o.worker_id.clone(), c.attempt_id.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// The dispatcher reading which attempts' `Started` arrived from `arrivals` (the core's, which
    /// the streams' readers fill), so it doesn't take them back as never started.
    #[must_use]
    pub fn with_arrivals(mut self, arrivals: Arrivals) -> Self {
        self.arrivals = arrivals;
        self
    }

    /// The dispatcher reading back `lookback` behind an event sensor's cursor (B185).
    #[must_use]
    pub fn with_event_lookback(mut self, lookback: Duration) -> Self {
        self.event_lookback = SignedDuration::try_from(lookback).unwrap_or(SignedDuration::MAX);
        self
    }

    /// The dispatcher taking back an assignment with no `Started` after `timeout`: the worker
    /// lease, so an assignment held on its way (a one-way cut) is never started after its task
    /// ran elsewhere; the worker drops a stream silent for the lease (decision 281, B163).
    #[must_use]
    pub fn with_start_timeout(mut self, timeout: Duration) -> Self {
        self.start_timeout = SignedDuration::try_from(timeout).unwrap_or(SignedDuration::MAX);
        self
    }

    /// The dispatcher waiting `wait` before a lost worker's cleanup moves.
    #[must_use]
    pub fn with_cleanup_move_wait(mut self, wait: Duration) -> Self {
        self.cleanup_move_wait = SignedDuration::try_from(wait).unwrap_or(SignedDuration::MAX);
        self
    }

    /// The dispatcher leaving a task whose assignment fails on its own out for `set_aside`,
    /// and reading a commit cut off back as `read_back` says (`set_aside`,
    /// `read_back_window`, `read_back_interval`).
    #[must_use]
    pub fn with_set_aside(mut self, set_aside: Duration, read_back: ReadBack) -> Self {
        self.set_aside_for = SignedDuration::try_from(set_aside).unwrap_or(SignedDuration::MAX);
        self.read_back = read_back;
        self
    }

    /// The dispatcher giving an `@sensor(wait_for=...)`'s call at least `least` once it is handed
    /// its run (see [`DEFAULT_HANDED_CALL_MIN`]).
    #[must_use]
    pub const fn with_handed_call_min(mut self, least: Duration) -> Self {
        self.handed_call_min = least;
        self
    }

    /// The largest message a worker takes (`[grpc] max_message_size`).
    #[must_use]
    pub const fn with_max_message_size(mut self, bytes: usize) -> Self {
        self.max_message_size = bytes;
        self
    }

    /// What finished runs and tasks notify (`[notifications]`).
    pub fn set_notifications(
        &mut self,
        notifications: std::sync::Arc<crate::notifications::Notifications>,
    ) {
        self.execution.set_notifications(notifications);
    }

    /// The most instances a mapped task may have (`[mapping] max_instances`).
    pub const fn set_max_instances(&mut self, max_instances: usize) {
        self.execution.set_max_instances(max_instances);
    }

    /// The dispatcher with another code wait (see [`DEFAULT_CODE_WAIT`]).
    #[must_use]
    pub fn with_code_wait(mut self, code_wait: Duration) -> Self {
        self.code_wait = SignedDuration::try_from(code_wait).unwrap_or(SignedDuration::MAX);
        self
    }

    /// The registered workers.
    #[must_use]
    pub const fn workers(&self) -> &Workers {
        &self.workers
    }

    /// The open attempts counted for the concurrency limits.
    #[must_use]
    pub const fn active(&self) -> &Active {
        &self.active
    }

    /// A worker registered. A new worker or a restarted one gives `unschedulable` tasks
    /// another chance.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn register(
        &mut self,
        worker_id: &str,
        incarnation: &str,
        queues: &[String],
        slots: u32,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.away.remove(worker_id);
        if self.workers.register(worker_id, incarnation, queues, slots) {
            self.rejections.clear();
            self.code_missing.clear();
            self.db.requeue_unschedulable(now).await?;
        }
        Ok(())
    }

    /// A root's accepted content hashes and its current parse's schedr version (see
    /// [`crate::discovery`]): its tasks go only to workers that have the root at one of those
    /// hashes and are of at least that version.
    pub fn set_current_code(
        &mut self,
        root: &str,
        accepted: &BTreeSet<String>,
        version: Option<SchedrVersion>,
    ) {
        self.current_code
            .insert(root.to_owned(), (accepted.clone(), version));
    }

    /// Each root's connections (decision 174): a task's assignment carries those it declares.
    /// `parsed` is the content hash of each root's current parse (its registry's).
    pub fn set_connections(
        &mut self,
        registries: &BTreeMap<String, Arc<Registry>>,
        parsed: &BTreeMap<String, String>,
    ) {
        self.connections = registries
            .iter()
            .map(|(root, registry)| {
                (
                    root.clone(),
                    (parsed.get(root).cloned(), Arc::clone(registry)),
                )
            })
            .collect();
    }

    /// A worker's stream ended: no connections job goes to it until it registers again.
    pub fn worker_disconnected(&mut self, worker_id: &str) {
        self.workers.disconnected(worker_id);
    }

    /// A worker a restarted master read back, not registered with it yet (see `away`).
    pub fn worker_away(&mut self, worker_id: &str) {
        self.away.insert(worker_id.to_owned());
    }

    /// The workers that may still come back with their attempts in their journals: read back
    /// by a restarted master, or disconnected, and not lost yet (their leases running).
    fn may_come_back(&self) -> Vec<String> {
        let disconnected = self.workers.all().filter(|worker| !worker.connected);
        let mut ids: BTreeSet<String> = disconnected.map(|worker| worker.id.clone()).collect();
        ids.extend(self.away.iter().cloned());
        ids.into_iter().collect()
    }

    /// The roots having connections, by name.
    #[must_use]
    pub fn connection_roots(&self) -> Vec<String> {
        self.connections
            .iter()
            .filter(|(_, (_, registry))| {
                !registry.connections.is_empty() || !registry.refused.is_empty()
            })
            .map(|(root, _)| root.clone())
            .collect()
    }

    /// A root's connections, if it has any.
    #[must_use]
    pub fn registry(&self, root: &str) -> Option<&Arc<Registry>> {
        self.connections.get(root).map(|(_, registry)| registry)
    }

    /// The worker a connections job of `root` goes to (decision 193), with the hash it has
    /// the root at: `wanted`, else a connected, active worker having the root's current code,
    /// the one running the fewest attempts. Why none can, otherwise. The current code is the
    /// current parse's exactly (not newer code whose parse is on its way): its connections are
    /// the ones the job carries.
    ///
    /// # Errors
    ///
    /// Why no worker can take it, for the command's rejection.
    pub fn connection_worker(
        &self,
        root: &str,
        wanted: Option<&str>,
    ) -> Result<(String, String), String> {
        let Some(parsed) = self
            .connections
            .get(root)
            .and_then(|(hash, _)| hash.as_ref())
        else {
            return Err(format!("root `{root}` has no parsed code yet"));
        };
        let current =
            |w: &crate::workers::Worker| w.roots.get(root).filter(|hash| *hash == parsed).cloned();
        if let Some(id) = wanted {
            let worker = self
                .workers
                .get(id)
                .filter(|w| w.connected)
                .ok_or_else(|| format!("worker `{id}` isn't connected"))?;
            // As for a task: a draining or drained worker takes no new work.
            if worker.state != crate::workers::WorkerState::Active || worker.drained {
                return Err(format!("worker `{id}` is draining or drained"));
            }
            return current(worker)
                .map(|hash| (id.to_owned(), hash))
                .ok_or_else(|| {
                    format!("worker `{id}` doesn't have root `{root}` at its current code")
                });
        }
        self.workers
            .all()
            .filter(|w| w.state == crate::workers::WorkerState::Active && !w.drained && w.connected)
            .filter_map(|w| current(w).map(|hash| (w, hash)))
            .min_by(|(a, _), (b, _)| {
                a.attempts
                    .len()
                    .cmp(&b.attempts.len())
                    .then(a.id.cmp(&b.id))
            })
            .map(|(w, hash)| (w.id.clone(), hash))
            .ok_or_else(|| format!("no active worker has root `{root}` at its current code"))
    }

    /// A worker's schedr version: it gets only tasks of parses no newer.
    pub fn set_worker_version(&mut self, worker_id: &str, version: Option<SchedrVersion>) {
        self.workers.set_version(worker_id, version);
    }

    /// A worker reported a root's content hash.
    pub fn report_root(&mut self, worker_id: &str, root: &str, hash: &str) {
        self.workers.report_root(worker_id, root, hash);
    }

    /// A worker withdrew a root (decision 303): its tasks no longer go there.
    pub fn withdraw_root(&mut self, worker_id: &str, root: &str) {
        self.workers.withdraw_root(worker_id, root);
    }

    /// A reconnecting worker still runs `attempt_id` (or holds its result): it keeps its
    /// slot, and the attempt counts as started, so it isn't taken back for want of a
    /// `Started` lost with the old stream (B50).
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn adopt(
        &mut self,
        worker_id: &str,
        attempt_id: &str,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        match self.db.mark_adopted(attempt_id, now).await {
            Ok(()) => {}
            // Refused for its data, it would be refused at every registration, and the worker
            // could never send the attempt's `Finished` (B128, as decision 115): left
            // unrecorded, while its slot is taken all the same (the worker runs it).
            Err(error) if error.is_data_fault() => {
                tracing::warn!(attempt_id, worker_id, %error, "can't record an adopted attempt's start");
                self.adopted_unrecorded.insert(attempt_id.to_owned());
            }
            Err(error) => return Err(error.into()),
        }
        // Whatever it held before this reconnect, it holds the slot its kind runs on once.
        self.workers.release(attempt_id);
        match self.task_of_attempt(attempt_id).await {
            Some(task) => self.take_slot(&task, (worker_id, attempt_id), now),
            None => self.workers.reserve(worker_id, attempt_id, now),
        }
        Ok(())
    }

    /// The task an attempt runs, as its run's DAG has it.
    async fn task_of_attempt(&self, attempt_id: &str) -> Option<schedr_model::Task> {
        let attempt = self.db.attempt(attempt_id).await.ok().flatten()?;
        let dag = self.db.run_dag(&attempt.run_id).await.ok().flatten()?;
        dag.task(&attempt.task_id).cloned()
    }

    /// The inputs of an open attempt as JSON (see [`attempt_inputs`]).
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn attempt_inputs(&self, attempt_id: &str) -> Result<Option<Vec<u8>>, DispatchError> {
        attempt_inputs(&self.db, attempt_id).await
    }

    /// A worker is draining: it gets no new tasks.
    pub fn draining(&mut self, worker_id: &str) {
        self.workers.draining(worker_id);
    }

    /// A worker is drained for maintenance: it gets no new tasks, and its pokes move at once:
    /// the messages that say so.
    pub fn drain(&mut self, worker_id: &str) -> Vec<Outgoing> {
        self.workers.set_drained(worker_id, true);
        let pokes = self
            .workers
            .get(worker_id)
            .map(|worker| worker.pokes.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        pokes
            .iter()
            .map(|attempt_id| Outgoing {
                worker_id: worker_id.to_owned(),
                message: pb::MasterMessage {
                    message: Some(pb::master_message::Message::CancelAttempt(
                        pb::CancelAttempt {
                            attempt_id: attempt_id.clone(),
                            interrupted: true,
                        },
                    )),
                    clock: None,
                },
            })
            .collect()
    }

    /// A worker is drained, as it was before it registered: its pokes are moved once they are
    /// known again ([`Self::drain`]).
    pub fn set_drained(&mut self, worker_id: &str) {
        self.workers.set_drained(worker_id, true);
    }

    /// Whether an attempt on a worker is a poke, which a drain moves at once.
    #[must_use]
    pub fn is_poke(&self, worker_id: &str, attempt_id: &str) -> bool {
        self.workers
            .get(worker_id)
            .is_some_and(|worker| worker.pokes.contains(attempt_id))
    }

    /// A worker's watcher slots.
    pub fn set_watcher_slots(&mut self, worker_id: &str, slots: u32) {
        self.workers.set_watcher_slots(worker_id, slots);
    }

    /// A drained worker is resumed: like a new worker, it gives `unschedulable` tasks
    /// another chance.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn resume(&mut self, worker_id: &str, now: Timestamp) -> Result<(), DispatchError> {
        self.workers.set_drained(worker_id, false);
        self.rejections.clear();
        self.code_missing.clear();
        self.db.requeue_unschedulable(now).await?;
        Ok(())
    }

    /// One step of the loop: new runs get their tasks, retries whose delay passed are queued,
    /// assignments with no `Started` are taken back, and ready tasks are assigned.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn step(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, DispatchError> {
        let mut outgoing = Vec::new();
        self.step_into(now, &mut outgoing).await?;
        Ok(outgoing)
    }

    /// As [`Self::step`], into `outgoing`: the assignments made before a failure are there
    /// still (each stored before its message is built, B7).
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn step_into(
        &mut self,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        outgoing.append(&mut self.stops);
        self.resolved.clear();
        self.execution.prepare_runs(now).await?;
        self.execution.queue_due_retries(now).await?;
        self.expire(now).await?;
        self.assign_into(now, outgoing).await
    }

    /// Assigns ready tasks to workers with free slots.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn assign(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, DispatchError> {
        let mut outgoing = Vec::new();
        self.assign_into(now, &mut outgoing).await?;
        Ok(outgoing)
    }

    /// As [`Self::assign`], into `outgoing`: all of the step's assignments, or, after a failure,
    /// none (nothing of them stored either).
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn assign_into(
        &mut self,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        self.set_aside.retain(|_, until| *until > now);
        self.fail_waiting_users(now).await?;
        // The stops of a step a timeout failed go now, not a round later.
        outgoing.append(&mut self.stops);
        self.assign_cleanups(now, outgoing).await?;
        self.wind_down_removed(now).await?;
        let ready = self.db.ready_instances(now).await?;
        let ready = self.unless_held(ready, now).await?;
        let (waits, ready): (Vec<ReadyInstance>, Vec<ReadyInstance>) = ready
            .into_iter()
            .filter(|r| !self.set_aside.contains_key(&r.instance_id))
            .partition(|r| r.dag.task(&r.task_id).is_some_and(waits_for_a_run));
        let (chained, mut ready): (Vec<ReadyInstance>, Vec<ReadyInstance>) = ready
            .into_iter()
            .partition(|r| r.dag.task(&r.task_id).is_some_and(starts_a_run));
        self.chained_into(now, &chained, outgoing).await?;
        self.handing.clear();
        // The `@sensor(wait_for=...)`s whose run has come are assigned with the others.
        ready.extend(self.wait_for_runs(now, &waits).await?);
        // Nothing ready (an idle round): no transaction, nothing to keep.
        if ready.is_empty() {
            self.metrics.set_waiting(&[]);
            return Ok(());
        }
        self.read_limits().await?;
        // The step's assignments are one transaction: stored together, with one lease check
        // and one commit (T2, 2.10b). What they changed in memory (the slots reserved) and the
        // messages built for them go with a rollback.
        let (workers, active, built) = (self.workers.clone(), self.active.clone(), outgoing.len());
        let db = self.db.clone();
        let together = Box::pin(db.atomically(self.assign_each(now, &ready, outgoing))).await;
        let Err(error) = together else {
            // Counted once stored (a step redone one at a time would count them twice).
            self.count_assignments(outgoing.len() - built);
            return Ok(());
        };
        // A commit cut off may have stored them all (they are one transaction): one of its new
        // attempts there means it did, and its messages go; none, or no answer, means it
        // didn't (taken back; a stored one would expire after START_TIMEOUT).
        if matches!(error, DispatchError::Db(DbError::CommitUnknown))
            && self.stored(&outgoing[built..]).await
        {
            self.count_assignments(outgoing.len() - built);
            return Ok(());
        }
        self.workers = workers;
        self.active = active;
        if error.commit_unknown() {
            // Read back as not stored, or not read back at all.
            self.recount();
        }
        outgoing.truncate(built);
        if !task_specific(&error) {
            return Err(error);
        }
        // One task may fail every time (its data won't read, the database refuses it): one at
        // a time, the others go on and it is set aside, instead of every round failing on it.
        tracing::warn!(%error, "a round's assignments failed together: assigning one at a time");
        self.assign_one_by_one(now, &ready, outgoing).await
    }

    /// Cancels what the runs of removed DAGs haven't started (B192): each run whole or not at
    /// all, so a run that fails is found again the next round, never left with its tasks
    /// ended and itself running (Codex's review).
    async fn wind_down_removed(&mut self, now: Timestamp) -> Result<(), DispatchError> {
        for run_id in self.db.removed_dags_runs().await? {
            // One run that can't be wound down holds no other assignment up.
            if let Err(error) =
                Box::pin(self.db.atomically(self.execution.wind_down(&run_id, now))).await
            {
                tracing::warn!(run_id, %error, "a removed DAG's run not wound down yet");
            }
        }
        Ok(())
    }

    /// The ready tasks but those of a held DAG (its resources' cleanups still run): a removed
    /// DAG's are left out, [`Self::wind_down_removed`] cancelling them (B192); a DAG in an ID
    /// conflict's wait until it is resolved (B193, decision 107).
    async fn unless_held(
        &mut self,
        ready: Vec<ReadyInstance>,
        now: Timestamp,
    ) -> Result<Vec<ReadyInstance>, DispatchError> {
        if ready.is_empty() {
            return Ok(ready);
        }
        let held = self.db.held_dags().await?;
        let mut free = Vec::with_capacity(ready.len());
        let mut reasons = Vec::new();
        for instance in ready {
            let cleanup = instance
                .dag
                .resource_of_cleanup(&instance.task_id)
                .is_some();
            match held.get(instance.dag.id.as_str()) {
                Some(true) if !cleanup => {}
                Some(false) if !cleanup => {
                    if instance.queued_reason.as_deref() != Some(DAG_IN_CONFLICT) {
                        reasons.push((instance.instance_id.clone(), DAG_IN_CONFLICT.to_owned()));
                    }
                }
                _ => free.push(instance),
            }
        }
        if !reasons.is_empty() {
            self.db.set_queued_reasons(&reasons, now).await?;
        }
        Ok(free)
    }

    /// Fails each resource's user `unschedulable` for longer than the resource's
    /// `unschedulable_timeout` (the shortest, when it uses several), so the cleanup can run
    /// (decision 273).
    async fn fail_waiting_users(&mut self, now: Timestamp) -> Result<(), DispatchError> {
        for waiting in self.db.unschedulable_instances().await? {
            let Some(limit) = waiting
                .dag
                .resources
                .iter()
                .filter(|r| r.users.contains(&waiting.task_id))
                .filter_map(|r| r.unschedulable_timeout_ms)
                .min()
            else {
                continue;
            };
            let limit = SignedDuration::from_millis(i64::try_from(limit).unwrap_or(i64::MAX));
            if waiting.since.checked_add(limit).unwrap_or(Timestamp::MAX) > now {
                continue;
            }
            let reason = format!(
                "no worker within {limit:#}: it uses a resource whose `unschedulable_timeout` \
                 ran out"
            );
            // One transaction: the task's end and the run's, and the stops sent with them.
            self.whole(async |me| {
                let (_, stops) = me
                    .execution
                    .fail_unschedulable(
                        &waiting.run_id,
                        (&waiting.task_id, waiting.map_index),
                        reason,
                        now,
                    )
                    .await?;
                me.stop(&waiting.run_id, &stops).await
            })
            .await?;
        }
        Ok(())
    }

    /// The `wait_for` steps queued (decision 353): no worker takes them; each round each is
    /// ended once the run it waits for (or its task) has, or its `max_wait` ran out, and
    /// otherwise says what it waits for.
    async fn wait_for_runs(
        &mut self,
        now: Timestamp,
        waits: &[ReadyInstance],
    ) -> Result<Vec<ReadyInstance>, DispatchError> {
        let mut reasons = Vec::new();
        let mut handed = Vec::new();
        for waiting in waits {
            let (run_of, max_wait_ms) = match waiting.dag.task(&waiting.task_id).map(|t| &t.kind) {
                Some(TaskKind::WaitFor {
                    run_of,
                    max_wait_ms,
                }) => (run_of, max_wait_ms),
                Some(TaskKind::WaitForEvent { event, max_wait_ms }) => {
                    self.wait_for_event(waiting, event, *max_wait_ms, now, &mut reasons)
                        .await?;
                    continue;
                }
                Some(TaskKind::Sensor {
                    wait_for: Some(wait_for),
                    max_wait_ms,
                    ..
                }) => {
                    if self
                        .sensor_run(waiting, wait_for, *max_wait_ms, now, &mut reasons)
                        .await?
                    {
                        handed.push(waiting.clone());
                    }
                    continue;
                }
                _ => continue,
            };
            let Some(since) = self.db.ready_since(&waiting.instance_id).await? else {
                continue;
            };
            let deadline = self.deadline(since, *max_wait_ms);
            let found = self
                .matching_run(run_of, waiting.planned_for, since, None, deadline)
                .await?;
            let ended = match decide(found.as_ref()) {
                Some(ended) => ended,
                None if now >= deadline => WaitEnd::TimedOut,
                None => {
                    let reason = self.wait_reason(run_of).await?;
                    if waiting.queued_reason.as_deref() != Some(reason.as_str()) {
                        reasons.push((waiting.instance_id.clone(), reason));
                    }
                    continue;
                }
            };
            self.whole(async |me| {
                let (_, stops) = me
                    .execution
                    .wait_ended(&waiting.run_id, &waiting.task_id, ended, now)
                    .await?;
                me.stop(&waiting.run_id, &stops).await
            })
            .await?;
        }
        if !reasons.is_empty() {
            self.db.set_queued_reasons(&reasons, now).await?;
        }
        Ok(handed)
    }

    /// An `@sensor(wait_for=...)` queued (decision 353): once a matching run has ended (one its
    /// function hasn't said `NOT_YET` to), it is to be assigned with that run, which is recorded;
    /// past its `max_wait` it ends `TimedOut` here; else it says what it waits for. Returns
    /// whether it is to be assigned.
    async fn sensor_run(
        &mut self,
        waiting: &ReadyInstance,
        wait_for: &SensorWaitFor,
        max_wait_ms: Option<u64>,
        now: Timestamp,
        reasons: &mut Vec<(String, String)>,
    ) -> Result<bool, DispatchError> {
        let Some(wait) = self.db.sensor_wait(&waiting.instance_id, now).await? else {
            return Ok(false);
        };
        let deadline = self.deadline(wait.since, max_wait_ms);
        let run_of = match wait_for.source() {
            Some(WaitSource::RunOf(run_of)) => run_of,
            Some(WaitSource::Event(event)) => {
                return self
                    .sensor_event(waiting, event, &wait, (deadline, now), reasons)
                    .await;
            }
            None => return Ok(false),
        };
        // A retry gets the run it was handed, unless its function said `NOT_YET` to it: the
        // data a call failed on isn't swapped for a newer run's (decision 353).
        let again = wait
            .handed
            .clone()
            .filter(|(handed, _)| wait.waited.as_ref().is_none_or(|(seen, _)| seen != handed));
        let found = match again {
            Some(handed) => Some(handed),
            None => self
                .matching_run(
                    run_of,
                    waiting.planned_for,
                    wait.since,
                    wait.waited.as_ref(),
                    deadline,
                )
                .await?
                .filter(|run| run_ended(&run.state))
                .map(|run| (run.run_id, run.ended_at.unwrap_or(now))),
        };
        if let Some((run_id, ended)) = found
            && let Some(mut result) = self.db.run_result(&run_id, run_of.task.as_ref()).await?
        {
            // Another DAG's output, as anyone reading it through the API sees it (decision
            // 131; Grok's review of M4.11d2): secret fields and passwords masked.
            if let Some(output) = result.get_mut("output") {
                schedr_config::masked::mask_data(output);
            }
            self.db
                .hand_run(&waiting.instance_id, &run_id, ended)
                .await?;
            self.handing.insert(waiting.instance_id.clone(), result);
            return Ok(true);
        }
        if now >= deadline {
            // One transaction: the attempt's end and the step's, or neither.
            self.whole(async |me| me.time_out(waiting, now).await)
                .await?;
            return Ok(false);
        }
        let reason = self.wait_reason(run_of).await?;
        if waiting.queued_reason.as_deref() != Some(reason.as_str()) {
            reasons.push((waiting.instance_id.clone(), reason));
        }
        Ok(false)
    }

    /// When a wait waiting `since` with `max_wait_ms` (else `[sensors] default_max_wait`) times
    /// out.
    fn deadline(&self, since: Timestamp, max_wait_ms: Option<u64>) -> Timestamp {
        let max_wait = max_wait_ms.map_or(self.sensor_max_wait, Duration::from_millis);
        SignedDuration::try_from(max_wait)
            .ok()
            .and_then(|wait| since.checked_add(wait).ok())
            .unwrap_or(Timestamp::MAX)
    }

    /// The run of `run_of` a wait planned for `planned_for`, waiting `since`, matches (decision
    /// 353): with `lag` the newest planned for that moment, else the first to end since then;
    /// after `seen` (one its function said `NOT_YET` to), a later one. A run that ended after
    /// `deadline` still runs as far as the wait knows, though seen only now (a round late, a
    /// master restarting).
    async fn matching_run(
        &self,
        run_of: &RunOf,
        planned_for: Timestamp,
        since: Timestamp,
        seen: Option<&(String, Timestamp)>,
        deadline: Timestamp,
    ) -> Result<Option<OtherRun>, DispatchError> {
        // A lag past the times there are matches no run: the step waits out its time.
        let matching = match (run_of.lag_ms, seen) {
            (Some(lag), _) => planned_for
                .checked_sub(SignedDuration::from_millis(lag))
                .ok()
                .map(Matching::PlannedFor),
            (None, Some((run, at))) => Some(Matching::EndedAfterRun(*at, run.clone())),
            (None, None) => Some(Matching::EndedAfter(since)),
        };
        let task = run_of.task.as_ref().map(TaskId::as_str);
        let Some(matching) = matching else {
            return Ok(None);
        };
        let found = self.db.runs_of(run_of.dag.as_str(), task, matching).await?;
        Ok(found
            .into_iter()
            .next()
            // With `lag`, the one said NOT_YET to stays the newest until a newer one comes.
            .filter(|run| seen.is_none_or(|(seen, _)| &run.run_id != seen))
            .map(|mut run| {
                if run.ended_at.is_some_and(|at| at > deadline) {
                    "running".clone_into(&mut run.state);
                }
                run
            }))
    }

    /// What a `wait_for` step says it waits for; that its DAG isn't loaded, if it isn't (it may
    /// come from a folder that loads later, decision 353).
    async fn wait_reason(&self, run_of: &RunOf) -> Result<String, DispatchError> {
        let what = match &run_of.task {
            Some(task) => format!("task `{task}` of a run of `{}`", run_of.dag),
            None => format!("a run of `{}`", run_of.dag),
        };
        let loaded = self
            .db
            .stored_dag(&run_of.dag)
            .await?
            .is_some_and(|d| d.current.is_some());
        Ok(if loaded {
            format!("waiting for {what}")
        } else {
            format!("waiting for {what}; no DAG `{}` is loaded", run_of.dag)
        })
    }

    /// Reads what the concurrency limits need: the open attempts, once (the master's first
    /// round with tasks ready; then kept in memory), and each DAG's current version.
    async fn read_limits(&mut self) -> Result<(), DispatchError> {
        if !self.active.is_loaded() {
            self.active.load(self.db.active_attempts().await?);
        }
        self.current_versions = self.db.active_dag_versions().await?.into_iter().collect();
        Ok(())
    }

    /// The DAG version whose limits across runs hold for `ready`: its DAG's current one, or
    /// its run's own if the DAG has none now (removed, in conflict) or it can't be read.
    async fn current_dag(&self, ready: &ReadyInstance) -> Result<Arc<Dag>, DispatchError> {
        let Some(current) = self.current_versions.get(&ready.dag.id) else {
            return Ok(Arc::clone(&ready.dag));
        };
        if *current == ready.version_id {
            return Ok(Arc::clone(&ready.dag));
        }
        match self.db.dag_version(current).await {
            Ok(dag) => Ok(dag),
            Err(error) if error.is_data_fault() => {
                tracing::warn!(
                    %error,
                    dag_id = %ready.dag.id,
                    "the DAG's current version doesn't read: its run's own limits hold"
                );
                Ok(Arc::clone(&ready.dag))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Whether one of `ready`'s concurrency limits is reached: then it waits, counted and its
    /// reason written (at most so many a round, as for the other reasons).
    async fn held_back(
        &self,
        ready: &ReadyInstance,
        tally: &mut Tally,
    ) -> Result<bool, DispatchError> {
        if ready.dag.resource_of_cleanup(&ready.task_id).is_some() {
            // A resource's cleanup isn't held by any limit (decision 225); it needs a slot.
            return Ok(false);
        }
        let current = self.current_dag(ready).await?;
        let Some(reached) =
            self.active
                .reached((&current, &ready.dag), &ready.run_id, &ready.task_id)
        else {
            return Ok(false);
        };
        *tally.waiting.entry(reached.name).or_default() += 1;
        let reason = reached.reason();
        if ready.queued_reason.as_deref() != Some(reason.as_str())
            && tally.reasons.len() < REASONS_PER_ROUND
        {
            tally.reasons.push((ready.instance_id.clone(), reason));
        }
        Ok(true)
    }

    /// Counts `made` assignments, once stored.
    fn count_assignments(&self, made: usize) {
        self.metrics
            .assignments
            .inc_by(u64::try_from(made).unwrap_or(u64::MAX));
    }

    /// Whether the attempt of the first assignment in `built` is stored, after a commit whose
    /// outcome is unknown (see [`read_back`]; `false` if it can't be told).
    async fn stored(&self, built: &[Outgoing]) -> bool {
        let Some(attempt_id) = built.iter().find_map(|o| match &o.message.message {
            Some(pb::master_message::Message::AssignTask(assign)) => Some(&assign.attempt_id),
            _ => None,
        }) else {
            return false;
        };
        self.read_back
            .stored(|| async { matches!(self.db.attempt(attempt_id).await, Ok(Some(_))) })
            .await
    }

    /// The `ready` tasks assigned each in a transaction of its own; one whose assignment fails
    /// on its own is set aside for [`SET_ASIDE`], with a warning. An error that isn't one
    /// task's (the database gone, the lease lost), or every task failing, is the step's.
    async fn assign_one_by_one(
        &mut self,
        now: Timestamp,
        ready: &[ReadyInstance],
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        let mut tally = Tally::default();
        let (mut failed, mut made) = (Vec::new(), 0);
        for one in ready {
            let (workers, active, built) =
                (self.workers.clone(), self.active.clone(), outgoing.len());
            let db = self.db.clone();
            let assigned =
                Box::pin(db.atomically(self.assign_one(now, one, outgoing, &mut tally))).await;
            let Err(error) = assigned else {
                made += outgoing.len() - built;
                self.count_assignments(outgoing.len() - built);
                continue;
            };
            self.workers = workers;
            self.active = active;
            if error.commit_unknown() {
                // The rest of the round counts what is stored.
                self.recount();
                self.read_limits().await?;
            }
            outgoing.truncate(built);
            if !task_specific(&error) {
                return Err(error);
            }
            failed.push((one, error));
        }
        // Every one that tried failed (more than one, none assigned): the database's trouble,
        // not theirs. Those that only waited for a slot don't count.
        if made == 0 && failed.len() > 1 {
            return Err(failed.swap_remove(0).1);
        }
        for (one, error) in failed {
            let until = now.checked_add(self.set_aside_for).unwrap_or(now);
            tracing::warn!(
                %error,
                run_id = one.run_id,
                task_id = %one.task_id,
                %until,
                "a task's assignment failed on its own: set aside, the others go on"
            );
            self.set_aside.insert(one.instance_id.clone(), until);
        }
        // Its own transaction: shown only, and the assignments are stored already.
        let db = self.db.clone();
        let written = Box::pin(db.atomically(db.set_queued_reasons(&tally.reasons, now))).await;
        if let Err(error) = written {
            tracing::warn!(%error, "waiting reasons not written");
        }
        let waiting: Vec<(&'static str, i64)> = tally.waiting.into_iter().collect();
        self.metrics.set_waiting(&waiting);
        Ok(())
    }

    /// Assigns the `ready` tasks to workers with free slots, each assignment's message into
    /// `outgoing` once it is stored (in [`Self::assign_into`]'s transaction).
    async fn assign_each(
        &mut self,
        now: Timestamp,
        ready: &[ReadyInstance],
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        let mut tally = Tally::default();
        for one in ready {
            self.assign_one(now, one, outgoing, &mut tally).await?;
        }
        self.db.set_queued_reasons(&tally.reasons, now).await?;
        let waiting: Vec<(&'static str, i64)> = tally.waiting.into_iter().collect();
        self.metrics.set_waiting(&waiting);
        Ok(())
    }

    /// Assigns one ready task to a worker with a free slot, its message into `outgoing` once
    /// it is stored; or says why it waits.
    /// The run's parameters as JSON, for the task; empty for a DAG that takes none (the runner
    /// reads `{}`), so nothing is read.
    async fn params_json(&self, ready: &ReadyInstance) -> Result<Vec<u8>, DispatchError> {
        if ready.dag.params.is_none() {
            return Ok(Vec::new());
        }
        Ok(self
            .db
            .run_params(&ready.run_id)
            .await?
            .map(|p| p.to_string().into_bytes())
            .unwrap_or_default())
    }

    async fn assign_one(
        &mut self,
        now: Timestamp,
        ready: &ReadyInstance,
        outgoing: &mut Vec<Outgoing>,
        tally: &mut Tally,
    ) -> Result<(), DispatchError> {
        let Some(task) = ready.dag.task(&ready.task_id) else {
            return Ok(());
        };
        if self.ended_before_its_call(ready, task, now).await? {
            return Ok(());
        }
        // Its concurrency limits first: a task over one waits for it, whatever the workers.
        if self.held_back(ready, tally).await? {
            return Ok(());
        }
        let excluded: BTreeSet<String> = self
            .rejections
            .get(&ready.instance_id)
            .map(|r| r.keys().cloned().collect())
            .unwrap_or_default();
        let needs = self.needs_of(ready, task);
        // Workers that didn't have the code yet come last: the others are asked first.
        let mut without_code = excluded.clone();
        if let Some(missing) = self.code_missing.get(&ready.instance_id) {
            without_code.extend(missing.by.keys().cloned());
        }
        let picked = self
            .workers
            .pick(needs, &without_code)
            .or_else(|| self.workers.pick(needs, &excluded));
        // Enough for a round: the rest wait for the next one, counted as waiting (B18).
        if picked.is_some() && tally.assigned >= ASSIGNMENTS_PER_ROUND {
            *tally.waiting.entry("next_round").or_default() += 1;
            return Ok(());
        }
        let Some(worker_id) = picked.map(str::to_owned) else {
            let (why, reason) = self.why_waiting(needs, &excluded);
            *tally.waiting.entry(why).or_default() += 1;
            // At most so many a round: the rest in the next ones (B18).
            if ready.queued_reason.as_deref() != Some(reason.as_str())
                && tally.reasons.len() < REASONS_PER_ROUND
            {
                tally.reasons.push((ready.instance_id.clone(), reason));
            }
            return Ok(());
        };
        let Some((attempt_id, try_number)) = self
            .db
            .assign_attempt(&ready.instance_id, &worker_id, now)
            .await?
        else {
            return Ok(());
        };
        let inputs = mapping::inputs(
            &self.db,
            &ready.dag,
            &ready.run_id,
            (task, ready.map_index),
            &mut self.resolved,
        )
        .await;
        let inputs = match inputs {
            Ok(inputs) => inputs,
            Err(InputsError::Db(error)) => return Err(error.into()),
            Err(InputsError::NotArguments(why)) => {
                let end = failed_by_master(INPUT_MISMATCH, "InputMismatch", &why);
                self.end_ready(ready, &attempt_id, end, now).await?;
                return Ok(());
            }
        };
        let inputs = serde_json::to_vec(&inputs).unwrap_or_else(|_| b"{}".to_vec());
        let outcomes =
            upstream_outcomes(&self.db, &ready.dag, &ready.run_id, &ready.task_id).await?;
        let params = self.params_json(ready).await?;
        if inputs.len() > MAX_INPUTS {
            self.end(
                &attempt_id,
                &ready.run_id,
                (&ready.task_id, ready.map_index),
                inputs_too_large(),
                now,
            )
            .await?;
            return Ok(());
        }
        let mut message = self.assign_message(
            ready,
            &worker_id,
            &attempt_id,
            try_number,
            (inputs, outcomes, params),
        );
        message.start_by_ms = Some(self.unstarted(&attempt_id, now));
        self.add_event(ready, &mut message).await?;
        self.keep_deadline(ready, task, &mut message, now).await?;
        let Some(message) = self.if_it_fits(ready, &attempt_id, message, now).await? else {
            return Ok(());
        };
        self.take_slot(task, (&worker_id, &attempt_id), now);
        self.active.assigned(&attempt_id, ready);
        tally.assigned += 1;
        outgoing.push(Outgoing { worker_id, message });
        Ok(())
    }

    /// A worker reported `Started`. Returns a `CancelAttempt` if the attempt was taken back,
    /// ended or belongs to another worker.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn started(
        &mut self,
        worker_id: &str,
        started: &pb::Started,
        now: Timestamp,
    ) -> Result<Option<Outgoing>, DispatchError> {
        let method = match pb::StartMethod::try_from(started.start_method) {
            Ok(pb::StartMethod::Fork) => "fork",
            _ => "spawn",
        };
        let attempt = self.db.attempt(&started.attempt_id).await?;
        if let Some(attempt) = attempt.filter(|a| a.worker_id == worker_id)
            && self
                .db
                .mark_started(&started.attempt_id, method, now)
                .await?
        {
            // It found a worker: who rejected it before doesn't matter any more.
            self.rejections.remove(&attempt.instance_id);
            self.code_missing.remove(&attempt.instance_id);
            return Ok(None);
        }
        Ok(Some(cancel(worker_id, &started.attempt_id)))
    }

    /// An assignment stored at `now`: noted in master time for `expire`, and the master time
    /// by which it must start (B225), from the same reading, so the deadline sent is the one
    /// `expire` keeps.
    fn unstarted(&mut self, attempt_id: &str, now: Timestamp) -> i64 {
        let since = (self.steady)(now);
        self.unstarted_since.insert(attempt_id.to_owned(), since);
        since
            .checked_add(self.start_timeout)
            .unwrap_or(Timestamp::MAX)
            .as_millisecond()
    }

    /// Times this dispatcher on `steady` (see [`crate::clock`]).
    pub fn set_steady(&mut self, steady: crate::clock::Steady) {
        self.steady = steady;
    }

    /// A worker rejected an assignment.
    ///
    /// # Errors
    ///
    /// [`DispatchError::BadMessage`] for an unknown reason, or [`DispatchError::Db`].
    pub async fn rejected(
        &mut self,
        worker_id: &str,
        rejected: &pb::Rejected,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let reaction = reject_reaction(rejected.reason).map_err(|e| DispatchError::BadMessage {
            worker_id: worker_id.to_owned(),
            reason: e.to_string(),
        })?;
        let Some(attempt) = self.db.attempt(&rejected.attempt_id).await? else {
            return Ok(());
        };
        if attempt.moving && attempt.worker_id == worker_id && attempt.status.is_none() {
            // A moved cleanup goes on waiting, this worker asked last.
            self.rejections
                .entry(attempt.instance_id.clone())
                .or_default()
                .insert(worker_id.to_owned(), rejected.detail.clone());
            self.take_back_cleanup(&attempt, now).await?;
            return Ok(());
        }
        if attempt.worker_id != worker_id || attempt.status.is_some() || attempt.started {
            return Ok(());
        }
        let reason = pb::RejectReason::try_from(rejected.reason)
            .map_or("unknown", |r| r.as_str_name())
            .trim_start_matches("REJECT_REASON_")
            .to_lowercase();
        self.metrics
            .rejections
            .get_or_create(&ReasonLabels { reason })
            .inc();
        self.workers.release(&attempt.id);
        self.active.remove(&attempt.id);
        let run = self.db.run_tasks(&attempt.run_id).await?;
        let cancelling = run
            .as_ref()
            .is_some_and(|run| run.cancels_instance(&attempt.instance_id));
        if cancelling {
            // Never a try, and nothing runs again: the task is cancelled (B5).
            self.db
                .undo_assignment(&attempt.id, TaskInstanceState::Cancelled, None, None, now)
                .await?;
            self.execution.settle(&attempt.run_id, now).await?;
            return Ok(());
        }
        // The workers that could run it: once each has rejected it, it is unschedulable.
        let eligible: Option<Vec<String>> = run.and_then(|run| {
            let queue = &run.dag.task(&attempt.task_id)?.settings.queue;
            let needs = self.needs(&run.dag, queue, &run.parse_version);
            Some(self.workers.eligible(needs).map(|w| w.id.clone()).collect())
        });
        let (state, ready_at, reason) = match reaction {
            RejectReaction::Temporary => (TaskInstanceState::Queued, Some(now), None),
            RejectReaction::CodeNotThereYet => {
                self.not_here(&attempt, rejected, true, eligible.as_deref(), now)
            }
            RejectReaction::WorkerSpecific => {
                self.not_here(&attempt, rejected, false, eligible.as_deref(), now)
            }
        };
        self.db
            .undo_assignment(&attempt.id, state, ready_at, reason.as_deref(), now)
            .await?;
        Ok(())
    }

    /// A worker's task ended and its cleanup starts: the instance is `cleaning_up`, its
    /// attempt open until `Finished`, and the task's result kept, for a worker lost meanwhile
    /// (decision 229). Only from the worker it was assigned to (B46).
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn task_ended(
        &mut self,
        worker_id: &str,
        result: &pb::Finished,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        if let Some(attempt) = self.db.attempt(&result.attempt_id).await?
            && attempt.worker_id == worker_id
            && attempt.status.is_none()
        {
            self.db
                .set_task_result(&attempt.id, &result.encode_to_vec())
                .await?;
            self.execution
                .cleaning_up(&attempt.run_id, &attempt.task_id, attempt.map_index, now)
                .await?;
        }
        Ok(())
    }

    /// A worker reported `Finished`: stored once (only from the worker the attempt was assigned
    /// to, B46), and always acknowledged.
    ///
    /// # Errors
    ///
    /// [`DispatchError::BadMessage`] for an unknown status, or a database failure.
    pub async fn finished(
        &mut self,
        worker_id: &str,
        finished: &pb::Finished,
        now: Timestamp,
    ) -> Result<Outgoing, DispatchError> {
        let status =
            attempt_status_from_proto(finished.status).map_err(|e| DispatchError::BadMessage {
                worker_id: worker_id.to_owned(),
                reason: e.to_string(),
            })?;
        if let Some(attempt) = self.db.attempt(&finished.attempt_id).await? {
            // A cleanup under way when its worker was cut off (decision 255), reported when it
            // came back, while the cleanup waits to move: it ran, so it counts and doesn't
            // move (decision 282). The worker ran the task too, if it is the first move: its
            // own result stands (see `kept` below).
            let left_by_it = attempt.worker_id.is_empty()
                && attempt.status.is_none()
                && finished.cleanup.is_some()
                && self.db.cleanup_moved_from(&attempt.id).await?.as_deref() == Some(worker_id);
            // The handles the master never got, cleaned by the worker its cleanup first moved
            // from (B169): added to the attempt's cleanup, ended or not. Said to be a rest, it
            // never counts as the whole cleanup, even from the attempt's worker now.
            let rest_only = finished.cleanup.as_ref().is_some_and(|c| c.rest);
            if (rest_only || (attempt.worker_id != worker_id && !left_by_it))
                && let Some(rest) = &finished.cleanup
                && self.db.rest_owed_by(&attempt.id).await?.as_deref() == Some(worker_id)
            {
                let rest = cleanup_end(rest, worker_id)?;
                if self.db.add_rest_cleanup(&attempt.id, &rest).await? {
                    self.rest_cleaned(&attempt, &rest, now).await?;
                }
                return Ok(finished_ack(worker_id, &finished.attempt_id));
            }
            if rest_only {
                // Added already, or not this worker's to clean: ignored.
                return Ok(finished_ack(worker_id, &finished.attempt_id));
            }
            if attempt.worker_id != worker_id && !left_by_it {
                // Not its attempt (B46): only the worker it was assigned to ran it. Acknowledged
                // so the sender stops resending, never stored, as `Started` is refused.
                tracing::warn!(
                    worker_id,
                    attempt_id = %finished.attempt_id,
                    owner = %attempt.worker_id,
                    "a worker reported the end of another worker's attempt; ignored"
                );
                return Ok(finished_ack(worker_id, &finished.attempt_id));
            }
            self.count_strike(&attempt, finished, status, now).await?;
            let mut error_summary = (!finished.error_summary_json.is_empty())
                .then(|| String::from_utf8_lossy(&finished.error_summary_json).into_owned());
            // Only for a result not stored yet (a repeated one is dropped anyway).
            if attempt.status.is_none()
                && CODE_GONE_ERRORS.contains(&finished.error_kind.as_str())
                && let Some(context) = self.changed_since(&attempt).await?
            {
                error_summary = Some(with_context(error_summary.as_deref(), &context));
            }
            let cleanup = finished
                .cleanup
                .as_ref()
                .map(|c| cleanup_end(c, worker_id))
                .transpose()?;
            // A cleanup that moved knows nothing of the task: its result is the kept one
            // (a refused one is reported as `result_not_stored`, and stands). Only the worker
            // it first moved from ran the task: one it moved from later ran the cleanup alone.
            let ran_the_task = left_by_it && attempt.cleanup_moves == 1;
            let kept = if attempt.cleanup_moves > 0
                && !ran_the_task
                && finished.error_kind != RESULT_NOT_STORED
            {
                self.kept_result(&attempt).await?
            } else {
                None
            };
            let (end, result) = if let Some((kept, status)) = kept {
                ended(&kept, status, summary_of(&kept), cleanup)
            } else {
                ended(finished, status, error_summary, cleanup)
            };
            self.record(&attempt, &end, &result, now).await?;
        }
        Ok(finished_ack(worker_id, &finished.attempt_id))
    }

    /// Whether a wait step ended here, without a call: switched off, or out of time. A sensor
    /// handed a run that ended within its `max_wait` is called, though little of it is left
    /// (Grok's review of M4.11d2): the wait was answered in time.
    async fn ended_before_its_call(
        &mut self,
        ready: &ReadyInstance,
        task: &schedr_model::Task,
        now: Timestamp,
    ) -> Result<bool, DispatchError> {
        if !matches!(task.kind, schedr_model::TaskKind::Sensor { .. }) {
            return Ok(false);
        }
        let handed = self.handing.contains_key(&ready.instance_id);
        Ok(self.failed_as_switched_off(ready, now).await?
            || (!handed && self.timed_out_waiting(ready, task, now).await?))
    }

    /// Fails a wait step switched off, whatever the workers: a run isn't kept open by what won't
    /// run. Returns whether it was.
    async fn failed_as_switched_off(
        &mut self,
        ready: &ReadyInstance,
        now: Timestamp,
    ) -> Result<bool, DispatchError> {
        let Some(why) = self.switched_off(ready).await? else {
            return Ok(false);
        };
        if let Some((attempt_id, _)) = self.db.assign_attempt(&ready.instance_id, "", now).await? {
            let end = failed_by_master(schedr_model::runner::SWITCHED_OFF, "SwitchedOff", &why);
            self.end_ready(ready, &attempt_id, end, now).await?;
        }
        Ok(true)
    }

    fn needs_of<'a>(&'a self, ready: &'a ReadyInstance, task: &'a schedr_model::Task) -> Needs<'a> {
        Needs {
            wait_step: matches!(task.kind, schedr_model::TaskKind::Sensor { .. }),
            ..self.needs(&ready.dag, &task.settings.queue, &ready.parse_version)
        }
    }

    /// What is left of a wait step's `max_wait`, once it has started: it runs from its instance's
    /// first start, over retries and moves (decision 321). With it, that first start (for an
    /// `@sensor(wait_for=...)`, when it began to wait).
    async fn time_left(
        &self,
        ready: &ReadyInstance,
        task: &schedr_model::Task,
        now: Timestamp,
    ) -> Result<Option<(u64, Timestamp)>, DispatchError> {
        let schedr_model::TaskKind::Sensor {
            max_wait_ms,
            wait_for,
            ..
        } = &task.kind
        else {
            return Ok(None);
        };
        // An `@sensor(wait_for=...)`'s runs from when it began to wait, over its calls (353).
        let first = if wait_for.is_some() {
            self.db
                .sensor_wait(&ready.instance_id, now)
                .await?
                .map(|wait| wait.since)
        } else {
            self.db.first_start(&ready.instance_id).await?
        };
        let Some(first) = first else {
            return Ok(None);
        };
        let max_wait_ms = *max_wait_ms;
        let max_wait = max_wait_ms
            .unwrap_or_else(|| u64::try_from(self.sensor_max_wait.as_millis()).unwrap_or(u64::MAX));
        let gone = now.duration_since(first).as_millis().max(0);
        Ok(Some((
            max_wait.saturating_sub(u64::try_from(gone).unwrap_or(u64::MAX)),
            first,
        )))
    }

    /// Ends a wait step whose time ran out while it waited to run again: it would only time out,
    /// and no worker has to be free for that, or its function called past its time. Returns
    /// whether it did.
    async fn timed_out_waiting(
        &mut self,
        ready: &ReadyInstance,
        task: &schedr_model::Task,
        now: Timestamp,
    ) -> Result<bool, DispatchError> {
        if self
            .time_left(ready, task, now)
            .await?
            .map(|(left, _)| left)
            != Some(0)
        {
            return Ok(false);
        }
        self.time_out(ready, now).await?;
        Ok(true)
    }

    /// Ends a wait step `TimedOut` by the master, no worker hearing of it.
    async fn time_out(
        &mut self,
        ready: &ReadyInstance,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let Some((attempt_id, _)) = self.db.assign_attempt(&ready.instance_id, "", now).await?
        else {
            return Ok(());
        };
        if let Some(attempt) = self.db.attempt(&attempt_id).await? {
            let end = AttemptEnd::by_master(AttemptStatus::Success);
            let result = AttemptResult {
                output: Some(serde_json::json!({})),
                outcome: Some(TIMED_OUT_OUTCOME.to_owned()),
                ..by_master(AttemptStatus::Success)
            };
            self.record(&attempt, &end, &result, now).await?;
        }
        Ok(())
    }

    /// The message of an attempt just assigned, if it is within what a worker takes; over, the
    /// attempt ends and it never leaves (Codex's review of M4.11d2: a wait step's handed run
    /// rides in it).
    async fn if_it_fits(
        &mut self,
        ready: &ReadyInstance,
        attempt_id: &str,
        assign: pb::AssignTask,
        now: Timestamp,
    ) -> Result<Option<pb::MasterMessage>, DispatchError> {
        let message = pb::MasterMessage {
            message: Some(pb::master_message::Message::AssignTask(assign)),
            clock: None,
        };
        let Some(why) = too_big_to_send(&message, self.max_message_size) else {
            return Ok(Some(message));
        };
        self.handing.remove(&ready.instance_id);
        let end = failed_by_master(INPUTS_TOO_LARGE, "InputsTooLarge", &why);
        self.end_ready(ready, attempt_id, end, now).await?;
        Ok(None)
    }

    /// Takes the slot its kind of task runs on.
    fn take_slot(
        &mut self,
        task: &schedr_model::Task,
        (worker, attempt): (&str, &str),
        now: Timestamp,
    ) {
        match task.kind {
            schedr_model::TaskKind::Sensor { mode, .. } => {
                let poke = mode == schedr_model::SensorMode::Poke;
                self.workers.reserve_wait_step(worker, attempt, poke, now);
            }
            _ => self.workers.reserve(worker, attempt, now),
        }
    }

    /// A wait step's `max_wait` runs from its instance's first start, over retries and moves
    /// (decision 321): what it gets is what is left.
    async fn keep_deadline(
        &self,
        ready: &ReadyInstance,
        task: &schedr_model::Task,
        message: &mut pb::AssignTask,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let handed = self.handing.get(&ready.instance_id);
        let (left, first) = self.time_left(ready, task, now).await?.unzip();
        if left.is_none() && handed.is_none() {
            return Ok(());
        }
        let mut task = task.clone();
        if let schedr_model::TaskKind::Sensor {
            max_wait_ms,
            wait_for,
            arguments,
            ..
        } = &mut task.kind
        {
            if left.is_some() {
                *max_wait_ms = left;
            }
            // Tried again: `delay` (a plain `@sensor`) counts from its first try's start.
            if wait_for.is_none() {
                message.first_started = first.map(proto_time);
            }
            if handed.is_some() {
                let least = u64::try_from(self.handed_call_min.as_millis()).unwrap_or(u64::MAX);
                *max_wait_ms = Some(max_wait_ms.unwrap_or(least).max(least));
            }
            // The run it waited for, as its function's first argument (decision 353).
            if let (Some(wait_for), Some(run)) = (wait_for, handed) {
                arguments.insert(
                    wait_for.param.clone(),
                    schedr_model::value::Value::Literal(run.clone()),
                );
            }
        }
        message.task_json = serde_json::to_vec(&as_sent(&task)).unwrap_or_default();
        Ok(())
    }

    /// Ends an attempt just assigned, without its worker having heard of it.
    async fn end_ready(
        &mut self,
        ready: &ReadyInstance,
        attempt_id: &str,
        end: AttemptEnd,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let instance = (&ready.task_id, ready.map_index);
        self.end(attempt_id, &ready.run_id, instance, end, now)
            .await?;
        Ok(())
    }

    /// Why a wait step about to start can't, if it is switched off; on new code it is on again,
    /// with one chance (decision 338).
    async fn switched_off(&self, ready: &ReadyInstance) -> Result<Option<String>, DispatchError> {
        let (dag, task) = (ready.dag.id.as_str(), ready.task_id.as_str());
        if !self.is_switched_off(&ready.dag, task).await? {
            return Ok(None);
        }
        Ok(Some(format!(
            "this wait step is switched off: it got its host killed too many times in a row, \
             holding up the wait steps it shares a process with. New code switches it on, or \
             `schedr sensors switch-on {dag} {task}`"
        )))
    }

    /// Whether a wait step or trigger is switched off now; on new code it is on again, with one
    /// chance (decision 338).
    pub(crate) async fn is_switched_off(
        &self,
        dag: &schedr_model::Dag,
        task: &str,
    ) -> Result<bool, DispatchError> {
        let Some(strikes) = self.db.wait_step_strikes(dag.id.as_str(), task).await? else {
            return Ok(false);
        };
        if strikes.off_at.is_none() {
            return Ok(false);
        }
        if self.code_is_gone(dag, strikes.off_code.as_deref().unwrap_or_default()) {
            self.db.wait_step_on_new_code(dag.id.as_str(), task).await?;
            return Ok(false);
        }
        Ok(true)
    }

    /// Whether a DAG's code has moved on from `code`: workers run other code now, and none is
    /// let run it. What isn't known yet (after a restart) hasn't moved on.
    pub(crate) fn code_is_gone(&self, dag: &schedr_model::Dag, code: &str) -> bool {
        self.current_code
            .get(dag.location.root.as_str())
            .is_some_and(|(accepted, _)| !accepted.is_empty() && !accepted.contains(code))
    }

    /// Counts how a wait step's attempt ended, the first time it is told (decision 338).
    async fn count_strike(
        &self,
        attempt: &StoredAttempt,
        finished: &pb::Finished,
        status: AttemptStatus,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        if !finished.wait_step
            || attempt.status.is_some()
            || !(finished.struck || status == AttemptStatus::Success)
        {
            return Ok(());
        }
        let Some(dag) = self.db.run_dag(&attempt.run_id).await.ok().flatten() else {
            return Ok(());
        };
        let (dag_id, task_id) = (dag.id.as_str(), attempt.task_id.as_str());
        if !finished.struck {
            self.db.clear_wait_step(dag_id, task_id).await?;
            return Ok(());
        }
        // The code it ran: an old attempt says nothing of what came since.
        let code = finished.code_hash.as_str();
        let limit = self.switch_off_after;
        if self
            .db
            .strike_wait_step((dag_id, task_id), limit, code, now)
            .await?
        {
            tracing::warn!(dag_id, task_id, "a wait step was switched off");
        }
        Ok(())
    }

    /// The cleanup of the handles the master never got, added to `attempt`'s (B169): the
    /// instance's cleanup result takes a failed or `ok` one, and a failure fails the run if it
    /// is still open; an ended `success` turns a yellow one (decision 309), logged as an error
    /// and counted.
    async fn rest_cleaned(
        &self,
        attempt: &StoredAttempt,
        rest: &CleanupEnd,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        if rest.result == CleanupResult::NotNeeded {
            return Ok(());
        }
        self.db
            .set_cleanup_result(&attempt.instance_id, rest.result, now)
            .await?;
        if !rest.result.fails_the_run() {
            return Ok(());
        }
        if let Some(dag_id) = self
            .db
            .mark_late_failure(&attempt.run_id, LATE_CLEANUP_FAILED)
            .await?
        {
            tracing::error!(
                dag_id,
                run_id = %attempt.run_id,
                task_id = %attempt.task_id,
                attempt_id = %attempt.id,
                error_kind = rest.error_kind.as_deref().unwrap_or_default(),
                "a cleanup after the run's success failed: handles may be left behind; the run \
                 is a yellow success (late_cleanup_failed)"
            );
            self.metrics.run_late_failed(&dag_id, LATE_CLEANUP_FAILED);
            let error = rest
                .error_summary
                .as_deref()
                .map(schedr_db::notifications::readable);
            self.execution
                .late_cleanup_failed(&attempt.run_id, attempt.task_id.as_str(), error, now)
                .await?;
        }
        Ok(())
    }

    /// Takes back assignments with no `Started` within the start timeout: they end
    /// `assignment_expired` and their tasks run again without using a retry. One whose
    /// `Started` arrived but waits for a busy master isn't taken back: the task did start
    /// (B11); nor one whose worker may come back with it (F14). Returns how many.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn expire(&mut self, now: Timestamp) -> Result<usize, DispatchError> {
        // On master time (B228): each unstarted assignment counts from when this master first
        // saw it, never from a stored wall-clock time. All of them are read (no more than the
        // workers' slots), and at most so many end a round (several writes each, B18).
        let steady = (self.steady)(now);
        // Adopted attempts whose start wasn't recorded are kept while a worker holds them.
        let workers = &self.workers;
        self.adopted_unrecorded
            .retain(|attempt| workers.all().any(|w| w.attempts.contains(attempt)));
        // Nor those of a worker that may come back (F14): it may have run the task and
        // finished, the `Started` lost with the old master or stream; its journal says so when
        // it registers. Its lease run out, it is lost and they end with it. Left out by the
        // query.
        let coming_back = self.may_come_back();
        let unstarted = self
            .db
            .unstarted_attempts(Timestamp::MAX, &coming_back, u32::MAX)
            .await?;
        let cleanups = self
            .db
            .unstarted_cleanups(Timestamp::MAX, &coming_back, u32::MAX)
            .await?;
        let seen: BTreeSet<&str> = unstarted
            .iter()
            .chain(&cleanups)
            .map(|attempt| attempt.id.as_str())
            .collect();
        self.unstarted_since
            .retain(|id, _| seen.contains(id.as_str()));
        let mut expired = Vec::new();
        let mut taken_back = Vec::new();
        for (attempt, cleanup) in unstarted
            .into_iter()
            .map(|a| (a, false))
            .chain(cleanups.into_iter().map(|a| (a, true)))
        {
            let since = *self
                .unstarted_since
                .entry(attempt.id.clone())
                .or_insert(steady);
            // Its `Started` is here, waiting its turn, or it was adopted though that couldn't
            // be recorded (batch A's review): the worker has it.
            if steady.duration_since(since) <= self.start_timeout
                || self.arrivals.start_waiting(&attempt.id)
                || self.adopted_unrecorded.contains(&attempt.id)
            {
                continue;
            }
            if cleanup {
                // A moved cleanup never started goes on waiting.
                taken_back.push(attempt);
            } else if expired.len() < EXPIRIES_PER_ROUND {
                expired.push(attempt);
            }
        }
        for attempt in &taken_back {
            self.unstarted_since.remove(&attempt.id);
            self.take_back_cleanup(attempt, now).await?;
        }
        for attempt in &expired {
            let end = AttemptEnd::by_master(AttemptStatus::AssignmentExpired);
            let result = by_master(AttemptStatus::AssignmentExpired);
            // Whole or not at all, memory with it (B49): ending the attempt and moving its
            // task are separate writes, and the slot is freed in memory before them.
            self.whole(async |dispatcher| dispatcher.record(attempt, &end, &result, now).await)
                .await?;
        }
        Ok(expired.len())
    }

    /// A worker's agent restarted (a new incarnation): the attempts it had started and doesn't
    /// report in its journal end `lost` with a `WorkerRestarted` error summary, each using a
    /// retry, as for a lost worker (the crash may have been the task's own; see archive/PROJECT.md,
    /// Worker lifecycle). Attempts it hadn't started yet are left to expire without using a
    /// retry. Returns how many ended.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn incarnation_replaced(
        &mut self,
        worker_id: &str,
        reported: &BTreeSet<&str>,
        now: Timestamp,
    ) -> Result<usize, DispatchError> {
        // Whole or not at all, memory with it (B49).
        let ended = self
            .whole(async |dispatcher| {
                dispatcher
                    .end_unreported(worker_id, reported, KeptResults::Used, now)
                    .await
            })
            .await;
        match ended {
            Err(error) if error.is_data_fault() => {
                tracing::error!(worker_id, %error, "a kept result the database refuses");
                self.whole(async |dispatcher| {
                    dispatcher
                        .end_unreported(worker_id, reported, KeptResults::Refused, now)
                        .await
                })
                .await
            }
            ended => ended,
        }
    }

    async fn end_unreported(
        &mut self,
        worker_id: &str,
        reported: &BTreeSet<&str>,
        results: KeptResults,
        now: Timestamp,
    ) -> Result<usize, DispatchError> {
        let mut ended = 0;
        for attempt in self.db.open_attempts(Some(worker_id)).await? {
            if attempt.started && !reported.contains(attempt.id.as_str()) {
                self.end_lost(&attempt, restarted_summary(worker_id), results, now)
                    .await?;
                ended += 1;
            }
        }
        Ok(ended)
    }

    /// A worker is lost or gone: its started attempts end `lost` with a `WorkerLost` error
    /// summary (each uses a retry), the ones it never started `assignment_expired` with a
    /// `WorkerLeft` one (queued again, no retry), and it is forgotten.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn worker_lost(
        &mut self,
        worker_id: &str,
        results: KeptResults,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        // Whole or not at all, memory with it (B49): the worker is forgotten in memory first.
        self.whole(async |dispatcher| dispatcher.forget_worker(worker_id, results, now).await)
            .await
    }

    async fn forget_worker(
        &mut self,
        worker_id: &str,
        results: KeptResults,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.remove(worker_id);
        self.away.remove(worker_id);
        for attempt in self.db.open_attempts(Some(worker_id)).await? {
            if attempt.started {
                self.end_lost(&attempt, lost_summary(worker_id), results, now)
                    .await?;
                continue;
            }
            // One it never started (sent as it began draining, say) never ran there: taken
            // back like an expired assignment, queued again without using a retry (B17).
            let status = AttemptStatus::AssignmentExpired;
            let end = AttemptEnd {
                error_summary: Some(not_started_summary(worker_id)),
                ..AttemptEnd::by_master(status)
            };
            self.record(&attempt, &end, &by_master(status), now).await?;
        }
        Ok(())
    }

    /// Ends a started attempt whose worker can't report it any more: with the task's result if
    /// its process's end was reported (decision 229: it isn't run again), else `lost`, with
    /// `summary`. Its cleanup, if the task has one, didn't run (decision 220).
    async fn end_lost(
        &mut self,
        attempt: &StoredAttempt,
        summary: String,
        results: KeptResults,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        if results == KeptResults::Refused && attempt.result_kept {
            return self.end_refused(attempt, now).await;
        }
        let task_cleanup = self.task_cleanup(attempt).await?;
        let kept = self.kept_result(attempt).await?;
        if let Some(wanted) = &task_cleanup
            && wanted.anywhere
            && attempt.cleanup_moves <= wanted.retries
        {
            return self.move_cleanup(attempt, &summary, now).await;
        }
        let cleanup = task_cleanup
            .as_ref()
            .map(|wanted| cleanup_not_run(&attempt.worker_id, wanted));
        let (end, result) = if let Some((finished, status)) = kept {
            ended(&finished, status, summary_of(&finished), cleanup)
        } else {
            let result = AttemptResult {
                cleanup: cleanup.as_ref().map(|c| c.result),
                ..by_master(AttemptStatus::Lost)
            };
            let end = AttemptEnd {
                error_summary: Some(summary),
                cleanup,
                ..AttemptEnd::by_master(AttemptStatus::Lost)
            };
            (end, result)
        };
        self.record(attempt, &end, &result, now).await
    }

    /// Ends an attempt whose kept result the database refuses, as a refused `Finished` is
    /// (B106): failed, not retried, the same data would be refused again.
    async fn end_refused(
        &mut self,
        attempt: &StoredAttempt,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let wanted = self.task_cleanup(attempt).await?.unwrap_or_else(no_cleanup);
        let cleanup = cleanup_not_run(&attempt.worker_id, &wanted);
        let kind = RESULT_NOT_STORED.to_owned();
        let result = AttemptResult {
            error_kind: Some(kind.clone()),
            cleanup: Some(cleanup.result),
            ..by_master(AttemptStatus::Failed)
        };
        let end = AttemptEnd {
            error_kind: Some(kind),
            error_summary: Some(ErrorSummary::json(
                "ResultNotStored",
                "the task's result couldn't be stored, and its worker is gone",
            )),
            cleanup: Some(cleanup),
            ..AttemptEnd::by_master(AttemptStatus::Failed)
        };
        self.record(attempt, &end, &result, now).await
    }

    /// The cleanup of the attempt's task.
    async fn task_cleanup(
        &self,
        attempt: &StoredAttempt,
    ) -> Result<Option<TaskCleanup>, DispatchError> {
        let run = self.db.run_tasks(&attempt.run_id).await?;
        Ok(run.and_then(|run| run.dag.task(&attempt.task_id)?.cleanup.clone()))
    }

    /// The task's result kept for an attempt, and its status.
    async fn kept_result(
        &self,
        attempt: &StoredAttempt,
    ) -> Result<Option<(pb::Finished, AttemptStatus)>, DispatchError> {
        Ok(self
            .db
            .task_result(&attempt.id)
            .await?
            .and_then(|bytes| pb::Finished::decode(bytes.as_slice()).ok())
            .and_then(|finished| {
                let status = attempt_status_from_proto(finished.status).ok()?;
                Some((finished, status))
            }))
    }

    /// Moves an attempt's cleanup off a lost worker (`cleanup_anywhere`, decision 243): the
    /// attempt stays open, its instance `cleaning_up`, the task `lost` unless it reported its
    /// end, and a later round gives it to another worker.
    async fn move_cleanup(
        &mut self,
        attempt: &StoredAttempt,
        summary: &str,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.release(&attempt.id);
        let lost = pb::Finished {
            attempt_id: attempt.id.clone(),
            status: pb::AttemptStatus::Lost.into(),
            error_summary_json: summary.as_bytes().to_vec(),
            ..pb::Finished::default()
        };
        // A worker only cut off is stopping the task meanwhile. Kept in master time (B228);
        // the stored one is for people.
        let not_before = now
            .checked_add(self.cleanup_move_wait)
            .unwrap_or(Timestamp::MAX);
        let steady = (self.steady)(now)
            .checked_add(self.cleanup_move_wait)
            .unwrap_or(Timestamp::MAX);
        self.move_not_before.insert(attempt.id.clone(), steady);
        self.db
            .move_cleanup(&attempt.id, &lost.encode_to_vec(), not_before)
            .await?;
        self.execution
            .cleaning_up(&attempt.run_id, &attempt.task_id, attempt.map_index, now)
            .await?;
        Ok(())
    }

    /// A moved cleanup's worker refused it or never started it: it waits again, and that
    /// worker's slot is free.
    async fn take_back_cleanup(
        &mut self,
        attempt: &StoredAttempt,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.release(&attempt.id);
        self.db.unassign_cleanup(&attempt.id).await?;
        let until = now
            .checked_add(self.set_aside_for)
            .unwrap_or(Timestamp::MAX);
        self.set_aside.insert(attempt.instance_id.clone(), until);
        Ok(())
    }

    /// Gives the moved cleanups waiting to workers, ahead of the tasks waiting and not held by
    /// limits (decision 246): their attempts are counted already.
    async fn assign_cleanups(
        &mut self,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        let mut reasons = Vec::new();
        // All that wait, each moving once its wait in master time is over (B228): after a
        // restart, a whole wait from when this master first sees it.
        let waiting = self.db.waiting_cleanups(Timestamp::MAX).await?;
        let steady = (self.steady)(now);
        let first_seen = steady
            .checked_add(self.cleanup_move_wait)
            .unwrap_or(Timestamp::MAX);
        self.move_not_before
            .retain(|id, _| waiting.iter().any(|(waiting, _)| waiting == id));
        for (attempt_id, ready) in waiting {
            let not_before = *self
                .move_not_before
                .entry(attempt_id.clone())
                .or_insert(first_seen);
            if steady < not_before || self.set_aside.contains_key(&ready.instance_id) {
                continue;
            }
            let Some(task) = ready.dag.task(&ready.task_id) else {
                continue;
            };
            let needs = self.needs(&ready.dag, &task.settings.queue, &ready.parse_version);
            // A worker that refused it may take it yet: it does when no other can.
            let refused: BTreeSet<String> = self
                .rejections
                .get(&ready.instance_id)
                .map(|by| by.keys().cloned().collect())
                .unwrap_or_default();
            let Some(worker_id) = self
                .workers
                .pick(needs, &refused)
                .or_else(|| self.workers.pick(needs, &BTreeSet::new()))
                .map(str::to_owned)
            else {
                if ready.queued_reason.as_deref() != Some(NO_CLEANUP_WORKER) {
                    reasons.push((ready.instance_id.clone(), NO_CLEANUP_WORKER.to_owned()));
                }
                continue;
            };
            if !self.db.assign_cleanup(&attempt_id, &worker_id, now).await? {
                continue;
            }
            // One reading of master time for its deadline and its expiry, before the reads
            // that build its message (Grok's review of D442).
            let start_by = self.unstarted(&attempt_id, now);
            let mut message = self
                .cleanup_message(&ready, &worker_id, &attempt_id)
                .await?;
            message.start_by_ms = Some(start_by);
            self.workers.reserve(&worker_id, &attempt_id, now);
            outgoing.push(Outgoing {
                worker_id,
                message: pb::MasterMessage {
                    message: Some(pb::master_message::Message::AssignTask(message)),
                    clock: None,
                },
            });
        }
        if !reasons.is_empty() {
            self.db.set_queued_reasons(&reasons, now).await?;
        }
        Ok(())
    }

    /// The assignment of a moved cleanup: its task's, with its handles.
    async fn cleanup_message(
        &mut self,
        ready: &ReadyInstance,
        worker_id: &str,
        attempt_id: &str,
    ) -> Result<pb::AssignTask, DispatchError> {
        let task = ready.dag.task(&ready.task_id);
        let inputs = match task {
            Some(task) => {
                let inputs = mapping::inputs(
                    &self.db,
                    &ready.dag,
                    &ready.run_id,
                    (task, ready.map_index),
                    &mut self.resolved,
                )
                .await;
                match inputs {
                    Ok(inputs) => serde_json::to_vec(&inputs).unwrap_or_else(|_| b"{}".to_vec()),
                    Err(InputsError::Db(error)) => return Err(error.into()),
                    // It ran with them: what can't be read now fails on the worker, if at all.
                    Err(InputsError::NotArguments(_)) => b"{}".to_vec(),
                }
            }
            None => b"{}".to_vec(),
        };
        let outcomes =
            upstream_outcomes(&self.db, &ready.dag, &ready.run_id, &ready.task_id).await?;
        let params = self.params_json(ready).await?;
        let handles = self.db.cleanup_handles(attempt_id).await?;
        let mut message = self.assign_message(
            ready,
            worker_id,
            attempt_id,
            ready.try_number,
            (inputs, outcomes, params),
        );
        self.add_event(ready, &mut message).await?;
        message.cleanup_only = Some(pb::CleanupOnly {
            handle_json: handles.into_iter().map(String::into_bytes).collect(),
        });
        Ok(message)
    }

    /// Handles an attempt registered, kept for a cleanup on another worker (decision 242). Only
    /// from the worker it is assigned to (B46), while it is open, and up to what an attempt
    /// may hold (decision 228); a handle that doesn't read is left out.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn handles_added(
        &mut self,
        worker_id: &str,
        added: &pb::HandlesAdded,
    ) -> Result<(), DispatchError> {
        let Some(attempt) = self.db.attempt(&added.attempt_id).await? else {
            return Ok(());
        };
        if attempt.worker_id != worker_id || attempt.status.is_some() {
            return Ok(());
        }
        let handles: Option<Vec<String>> = added
            .handle_json
            .iter()
            .map(|json| {
                serde_json::from_slice::<CleanupHandle>(json).ok()?;
                String::from_utf8(json.clone()).ok()
            })
            .collect();
        let Some(handles) = handles else {
            tracing::warn!(
                worker_id,
                attempt_id = attempt.id,
                "a handle that doesn't read"
            );
            return Ok(());
        };
        let limit = MAX_CTX_MESSAGE_SIZE as usize;
        let fitted = self
            .db
            .add_cleanup_handles(&attempt.id, added.first, &handles, limit)
            .await?;
        if !fitted {
            tracing::warn!(
                worker_id,
                attempt_id = attempt.id,
                "more handles than an attempt may hold"
            );
        }
        Ok(())
    }

    /// Runs `work` in one transaction; if it fails, the memory it changed is put back as it
    /// was, so memory and database stay in step (B49). Call it with no transaction open:
    /// joined into an outer one, memory isn't put back if the outer one rolls back.
    async fn whole<T>(
        &mut self,
        work: impl AsyncFnOnce(&mut Self) -> Result<T, DispatchError>,
    ) -> Result<T, DispatchError> {
        let (db, memory) = (self.db.clone(), self.memory());
        let done = Box::pin(db.atomically(work(self))).await;
        if let Err(error) = &done {
            self.undo(memory, error);
        }
        done
    }

    /// Cancels one task of a run (see [`Execution::cancel_task`]); a `CancelAttempt` for its
    /// attempt if it runs.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] for an unknown run or task, or a database failure.
    pub async fn cancel_task(
        &mut self,
        run_id: &str,
        (task_id, map_index): (&TaskId, Option<i32>),
        now: Timestamp,
    ) -> Result<(TaskCancel, Vec<Outgoing>), DispatchError> {
        // -1 is the whole task, as the API lists it: every instance (execution agrees).
        let map_index = map_index.filter(|i| *i >= 0);
        let cancelled = self
            .execution
            .cancel_task(run_id, (task_id, map_index), now)
            .await?;
        if cancelled != TaskCancel::Stopping {
            return Ok((cancelled, Vec::new()));
        }
        let stopping = self
            .db
            .open_attempts(None)
            .await?
            .into_iter()
            .filter(|a| a.run_id == run_id && &a.task_id == task_id)
            .filter(|a| map_index.is_none_or(|i| a.map_index == i))
            .map(|a| cancel(&a.worker_id, &a.id))
            .collect();
        Ok((cancelled, stopping))
    }

    /// Queues an `unschedulable` task of an unfinished run again (see
    /// [`Execution::retry_task`]), forgetting which workers rejected it (`schedr tasks retry`).
    ///
    /// # Errors
    ///
    /// [`DispatchError`] for an unknown run or task, or a database failure.
    pub async fn retry_task(
        &mut self,
        run_id: &str,
        (task_id, map_index): (&TaskId, Option<i32>),
        now: Timestamp,
    ) -> Result<TaskRetry, DispatchError> {
        let retried = self
            .execution
            .retry_task(run_id, (task_id, map_index), now)
            .await?;
        if let TaskRetry::Queued(instance_id) = &retried {
            self.rejections.remove(instance_id);
            self.code_missing.remove(instance_id);
        }
        Ok(retried)
    }

    /// Cancels a run; returns a `CancelAttempt` for each of its running attempts.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] for an unknown run or a database failure.
    pub async fn cancel_run(
        &mut self,
        run_id: &str,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, DispatchError> {
        let cancelled = self.execution.cancel_run(run_id, now).await?;
        let running: BTreeSet<(&TaskId, i32)> =
            cancelled.running.iter().map(|(t, i)| (t, *i)).collect();
        Ok(self
            .db
            .open_attempts(None)
            .await?
            .into_iter()
            .filter(|a| a.run_id == run_id && running.contains(&(&a.task_id, a.map_index)))
            .map(|a| cancel(&a.worker_id, &a.id))
            .collect())
    }

    async fn record(
        &mut self,
        attempt: &StoredAttempt,
        end: &AttemptEnd,
        result: &AttemptResult,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.release(&attempt.id);
        self.active.remove(&attempt.id);
        if self.db.end_attempt(&attempt.id, end, now).await? {
            self.metrics.attempt_ended(&end.status);
            let recorded = self
                .execution
                .attempt_finished(
                    &attempt.run_id,
                    &attempt.task_id,
                    attempt.map_index,
                    result,
                    now,
                )
                .await?;
            self.stop(&attempt.run_id, &recorded.stops).await?;
        }
        Ok(())
    }

    /// Queues `CancelAttempt`s for running instances whose mapped task's step failed (75a).
    async fn stop(&mut self, run_id: &str, stops: &[(TaskId, i32)]) -> Result<(), DispatchError> {
        if stops.is_empty() {
            return Ok(());
        }
        let stopping: BTreeSet<(&TaskId, i32)> = stops.iter().map(|(t, i)| (t, *i)).collect();
        let attempts = self.db.open_attempts(None).await?;
        self.stops.extend(
            attempts
                .iter()
                .filter(|a| {
                    a.run_id == run_id
                        && !a.worker_id.is_empty()
                        && stopping.contains(&(&a.task_id, a.map_index))
                })
                .map(|a| cancel(&a.worker_id, &a.id)),
        );
        Ok(())
    }

    async fn end(
        &mut self,
        attempt_id: &str,
        run_id: &str,
        (task_id, map_index): (&TaskId, i32),
        end: AttemptEnd,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let result = AttemptResult {
            status: end.status,
            error_kind: end.error_kind.clone(),
            output: None,
            outcome: None,
            cleanup: None,
        };
        self.active.remove(attempt_id);
        if self.db.end_attempt(attempt_id, &end, now).await? {
            self.metrics.attempt_ended(&end.status);
            let recorded = self
                .execution
                .attempt_finished(run_id, task_id, map_index, &result, now)
                .await?;
            self.stop(run_id, &recorded.stops).await?;
        }
        Ok(())
    }

    /// Records a worker-specific rejection, or that the worker didn't have the code, and
    /// decides: `unschedulable` once every eligible worker said one or the other, and (if any
    /// said the code is missing) the code has been missing for the code wait; otherwise
    /// queued again, after [`CODE_RETRY_DELAY`] when the code was missing.
    fn not_here(
        &mut self,
        attempt: &StoredAttempt,
        rejected: &pb::Rejected,
        code: bool,
        eligible: Option<&[String]>,
        now: Timestamp,
    ) -> (TaskInstanceState, Option<Timestamp>, Option<String>) {
        let instance = &attempt.instance_id;
        let detail = (attempt.worker_id.clone(), rejected.detail.clone());
        if code {
            let missing = self
                .code_missing
                .entry(instance.clone())
                .or_insert_with(|| CodeMissing {
                    since: now,
                    by: BTreeMap::new(),
                });
            missing.by.insert(detail.0, detail.1);
        } else {
            let by = self.rejections.entry(instance.clone()).or_default();
            by.insert(detail.0, detail.1);
        }
        let specific = self.rejections.get(instance);
        let missing = self.code_missing.get(instance);
        let said = |worker: &str| {
            specific.is_some_and(|r| r.contains_key(worker))
                || missing.is_some_and(|m| m.by.contains_key(worker))
        };
        // No eligible worker at all (none synced yet, say) isn't every one rejecting it.
        let everyone =
            eligible.is_none_or(|workers| !workers.is_empty() && workers.iter().all(|w| said(w)));
        let waited = missing.map(|m| now.duration_since(m.since));
        let code_due = waited.is_none_or(|waited| waited >= self.code_wait);
        if everyone && code_due {
            let mut reasons: Vec<String> = specific
                .into_iter()
                .flatten()
                .map(|(w, d)| format!("{w}: {d}"))
                .collect();
            reasons.extend(
                missing
                    .into_iter()
                    .flat_map(|m| &m.by)
                    .map(|(w, d)| format!("{w}: {d}")),
            );
            let reason = match (specific, waited) {
                (None, Some(waited)) => format!(
                    "the code is missing on every eligible worker after {} min ({})",
                    waited.as_mins(),
                    reasons.join("; ")
                ),
                _ => format!("every eligible worker rejected it ({})", reasons.join("; ")),
            };
            return (TaskInstanceState::Unschedulable, None, Some(reason));
        }
        if code {
            (
                TaskInstanceState::Queued,
                now.checked_add(CODE_RETRY_DELAY).ok(),
                Some(format!(
                    "waiting for the code on the workers: {}",
                    rejected.detail
                )),
            )
        } else {
            (TaskInstanceState::Queued, Some(now), None)
        }
    }

    /// How the attempt's DAG changed since its run started, if it did: removed, moved to
    /// another file, or without the attempt's task.
    async fn changed_since(&self, attempt: &StoredAttempt) -> Result<Option<String>, DbError> {
        let Some(run) = self.db.run_tasks(&attempt.run_id).await? else {
            return Ok(None);
        };
        let ran = &run.dag;
        let place = |l: &schedr_model::DagLocation| format!("{}:{}", l.root, l.path);
        // This DAG alone (B136: every DAG was read, an unrelated one that doesn't read too).
        let stored = self.db.stored_dag(&ran.id).await?;
        let now = stored
            .as_ref()
            .filter(|d| !d.removed)
            .and_then(|d| d.current.as_ref());
        let task = &attempt.task_id;
        Ok(match now {
            None => Some(format!(
                "DAG `{}` was removed from `{}` after this run started",
                ran.id,
                place(&ran.location)
            )),
            Some(now) if now.dag.location != ran.location => Some(format!(
                "DAG `{}` moved from `{}` to `{}` after this run started",
                ran.id,
                place(&ran.location),
                place(&now.dag.location)
            )),
            Some(now) if now.dag.task(task).is_none() => Some(format!(
                "task `{task}` was removed from DAG `{}` (`{}`) after this run started",
                ran.id,
                place(&ran.location)
            )),
            Some(_) => None,
        })
    }

    /// What a task of `dag` in `queue`, of a parse by schedr `parse_version`, needs of a
    /// worker.
    fn needs<'a>(
        &'a self,
        dag: &'a schedr_model::Dag,
        queue: &'a str,
        parse_version: &str,
    ) -> Needs<'a> {
        let root = dag.location.root.as_str();
        let current = self.current_code.get(root);
        // The run's parse, and the current code's: the code that runs needs both.
        let version = parse_version
            .parse::<SchedrVersion>()
            .ok()
            .max(current.and_then(|(_, version)| *version));
        Needs {
            queue,
            code: current.map(|(hashes, _)| (root, hashes)),
            version,
            wait_step: false,
        }
    }

    /// Why no worker could be picked for a task with `needs`: the kind of reason (a metric's
    /// label) and the reason shown with the task.
    fn why_waiting(&self, needs: Needs<'_>, excluded: &BTreeSet<String>) -> (&'static str, String) {
        let queue = needs.queue;
        let serving: Vec<_> = self.workers.serving(queue).collect();
        if serving.is_empty() {
            if self.workers.drained_serving(queue) {
                return (
                    "drained",
                    format!(
                        "the workers serving queue `{queue}` are drained (`schedr workers resume`)"
                    ),
                );
            }
            return (
                "no_worker",
                format!("no active worker serves queue `{queue}`"),
            );
        }
        let recent: Vec<_> = serving
            .into_iter()
            .filter(|w| w.runs(needs.version))
            .collect();
        if recent.is_empty() {
            let version = needs.version.map(|v| v.to_string()).unwrap_or_default();
            return (
                "schedr_version",
                format!("no worker with schedr ≥ {version} serves queue `{queue}`"),
            );
        }
        let current: Vec<_> = recent
            .into_iter()
            .filter(|w| w.has_code(needs.code))
            .collect();
        if current.is_empty() {
            let root = needs.code.map(|(root, _)| root).unwrap_or_default();
            return (
                "newest_code",
                format!(
                    "waiting for workers serving queue `{queue}` to have the newest code of \
                     root `{root}`"
                ),
            );
        }
        if current.iter().all(|w| excluded.contains(&w.id)) {
            return (
                "rejected",
                format!("every active worker serving queue `{queue}` rejected it"),
            );
        }
        if needs.wait_step {
            return (
                "no_watcher_slot",
                format!("waiting for a free watcher slot on the workers serving queue `{queue}`"),
            );
        }
        (
            "no_slot",
            format!("waiting for a free slot on the workers serving queue `{queue}`"),
        )
    }

    /// The worker to run a DAG's trigger: as a wait step's, on a watcher slot, which it takes.
    /// Gives the assignment and the code it was picked for; `None` when no worker has a free one,
    /// or the root's current code isn't known yet (a parse on its way after a restart).
    pub fn place_trigger(
        &mut self,
        active: &schedr_db::runs::ActiveDag,
        instance: &str,
        (after, failures): (Option<String>, u32),
        now: Timestamp,
    ) -> Option<(Outgoing, String)> {
        let dag = &active.dag;
        let schedr_model::Trigger::Event { task, .. } = &dag.trigger else {
            return None;
        };
        let needs = Needs {
            wait_step: true,
            ..self.needs(dag, &task.settings.queue, "")
        };
        let root = &dag.location.root;
        self.current_code.get(root)?;
        let worker_id = self.workers.pick(needs, &BTreeSet::new())?.to_owned();
        let code = self.workers.get(&worker_id)?.roots.get(root)?.clone();
        let message = pb::AssignTrigger {
            instance_id: instance.to_owned(),
            dag_id: dag.id.to_string(),
            task_json: serde_json::to_vec(&as_sent(task)).unwrap_or_default(),
            dag_file: Some(pb::DagFileRef {
                root: root.clone(),
                path: dag.location.path.clone(),
            }),
            expected_code_hash: code.clone(),
            after,
            failures,
            cancel_grace_ms: u64::try_from(self.cancel_grace.as_millis()).unwrap_or(u64::MAX),
            connections_json: match self.registry(root) {
                Some(registry) if !task.connections.is_empty() => {
                    serde_json::to_vec(&registry.only(&task.connections)).unwrap_or_default()
                }
                _ => Vec::new(),
            },
        };
        self.workers
            .reserve_wait_step(&worker_id, instance, false, now);
        let outgoing = Outgoing {
            worker_id,
            message: pb::MasterMessage {
                message: Some(pb::master_message::Message::AssignTrigger(message)),
                clock: None,
            },
        };
        Some((outgoing, code))
    }

    /// A worker: what the trigger pass asks of the one running a trigger.
    #[must_use]
    pub fn worker(&self, id: &str) -> Option<&crate::workers::Worker> {
        self.workers.get(id)
    }

    /// A trigger keeps its watcher slot on the worker that came back with it.
    pub fn trigger_kept(&mut self, worker: &str, instance: &str, now: Timestamp) {
        self.workers.reserve_wait_step(worker, instance, false, now);
    }

    /// A trigger gave up its watcher slot.
    pub fn trigger_released(&mut self, instance: &str) {
        self.workers.release(instance);
    }

    /// What a run started by an event gets of it: nothing for any other run.
    async fn add_event(
        &self,
        ready: &ReadyInstance,
        message: &mut pb::AssignTask,
    ) -> Result<(), DispatchError> {
        if !ready.dag.trigger.brings_event() {
            return Ok(());
        }
        if let (Some(run), Some(event)) =
            (&mut message.run, self.db.run_event(&ready.run_id).await?)
        {
            run.event_json = serde_json::to_vec(&event).unwrap_or_default();
        }
        Ok(())
    }

    fn assign_message(
        &self,
        ready: &ReadyInstance,
        worker_id: &str,
        attempt_id: &str,
        try_number: u32,
        (inputs, outcomes, params): (Vec<u8>, BTreeMap<String, String>, Vec<u8>),
    ) -> pb::AssignTask {
        let task = ready.dag.task(&ready.task_id);
        let external = inputs.len() > INLINE_INPUTS_LIMIT;
        pb::AssignTask {
            attempt_id: attempt_id.to_owned(),
            task_id: ready.task_id.to_string(),
            map_index: ready.map_index,
            try_number,
            run: Some(pb::RunInfo {
                params_json: params,
                run_id: ready.run_id.clone(),
                kind: run_kind_to_proto(ready.run_kind),
                planned_for: Some(proto_time(ready.planned_for)),
                since: Some(proto_time(ready.since)),
                dag_id: ready.dag.id.to_string(),
                event_json: Vec::new(),
            }),
            task_json: task
                .and_then(|t| serde_json::to_vec(&as_sent(t)).ok())
                .unwrap_or_default(),
            dag_file: Some(pb::DagFileRef {
                root: ready.dag.location.root.clone(),
                path: ready.dag.location.path.clone(),
            }),
            // No current code known yet (a parse on its way after a restart): no check.
            // The hash the worker was picked for (it reported it); none while the root's
            // current code isn't known yet (a parse on its way after a restart): no check.
            expected_code_hash: self
                .current_code
                .get(&ready.dag.location.root)
                .and_then(|_| {
                    self.workers
                        .get(worker_id)?
                        .roots
                        .get(&ready.dag.location.root)
                })
                .cloned()
                .unwrap_or_default(),
            inputs_json: if external { Vec::new() } else { inputs },
            inputs_external: external,
            cancel_grace_ms: u64::try_from(self.cancel_grace.as_millis()).unwrap_or(u64::MAX),
            // The task's own budget and cap (decisions 436, 438); the agent reads them from the
            // task too.
            memory_budget_bytes: task.and_then(|t| t.settings.memory_bytes).unwrap_or(0),
            setrlimit: task.is_some_and(|t| t.settings.hard_memory_limit),
            upstream_outcomes: outcomes.into_iter().collect(),
            // Only what the task declares (decision 179): the agent serves no other, and a
            // root's whole registry could outgrow the worker's 4 MiB message.
            connections_json: match (task, self.registry(&ready.dag.location.root)) {
                (Some(task), Some(registry)) if !task.connections.is_empty() => {
                    serde_json::to_vec(&registry.only(&task.connections)).unwrap_or_default()
                }
                _ => Vec::new(),
            },
            cleanup_only: None,
            resource_cleanup: ready.dag.resource_of_cleanup(&ready.task_id).is_some(),
            first_started: None,
            // Set by who sends it, from when it was stored (`Self::start_by`).
            start_by_ms: None,
        }
    }
}

/// `task` as an assignment carries it: without what it maps over (B119: a literal list sent with
/// each instance, its square in all); the instance's element comes in its inputs (`$expand`).
/// A protocol timestamp as a time, if it is one.
#[must_use]
pub(crate) fn time_of(time: &prost_types::Timestamp) -> Option<Timestamp> {
    Timestamp::new(time.seconds, time.nanos).ok()
}

fn as_sent(task: &schedr_model::Task) -> schedr_model::Task {
    schedr_model::Task {
        id: task.id.clone(),
        kind: task.kind.clone(),
        settings: task.settings.clone(),
        output_type: task.output_type.clone(),
        outcomes: task.outcomes.clone(),
        group: task.group.clone(),
        expand: None,
        cleanup: task.cleanup.clone(),
        secrets: task.secrets.clone(),
        connections: task.connections.clone(),
    }
}

fn cancel(worker_id: &str, attempt_id: &str) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(pb::master_message::Message::CancelAttempt(
                pb::CancelAttempt {
                    interrupted: false,
                    attempt_id: attempt_id.to_owned(),
                },
            )),
            clock: None,
        },
    }
}

const fn by_master(status: AttemptStatus) -> AttemptResult {
    AttemptResult {
        status,
        error_kind: None,
        output: None,
        outcome: None,
        cleanup: None,
    }
}

/// An attempt's end and result as `finished` says, with `error_summary` and `cleanup` as read.
fn ended(
    finished: &pb::Finished,
    status: AttemptStatus,
    error_summary: Option<String>,
    cleanup: Option<CleanupEnd>,
) -> (AttemptEnd, AttemptResult) {
    let output = (status == AttemptStatus::Success)
        .then(|| serde_json::from_slice(&finished.output_json).ok())
        .flatten();
    let end = AttemptEnd {
        status,
        exit_code: finished.exit_code,
        signal: finished.signal,
        // 0: not measured (a worker without `/proc`, or before 2.8).
        peak_memory_bytes: (finished.peak_memory_bytes > 0).then_some(finished.peak_memory_bytes),
        usage: finished.usage.map(|u| ProcessUsage {
            user_cpu_micros: u.user_cpu_micros,
            system_cpu_micros: u.system_cpu_micros,
            peak_rss_bytes: u.peak_rss_bytes,
            major_faults: u.major_faults,
            voluntary_context_switches: u.voluntary_context_switches,
            involuntary_context_switches: u.involuntary_context_switches,
        }),
        core_dumped: finished.core_dumped,
        duration_ms: finished.duration_ms,
        error_kind: non_empty(&finished.error_kind),
        error_summary,
        code_hash: non_empty(&finished.code_hash),
        cleanup,
    };
    let result = AttemptResult {
        status,
        error_kind: end.error_kind.clone(),
        output,
        outcome: non_empty(&finished.outcome),
        cleanup: end.cleanup.as_ref().map(|c| c.result),
    };
    (end, result)
}

/// A result's error summary, as stored.
fn summary_of(finished: &pb::Finished) -> Option<String> {
    (!finished.error_summary_json.is_empty())
        .then(|| String::from_utf8_lossy(&finished.error_summary_json).into_owned())
}

/// Why a run is a yellow success: a cleanup after its end failed (decision 309).
pub const LATE_CLEANUP_FAILED: &str = "late_cleanup_failed";

/// A cleanup standing in when the task's is gone from its DAG.
const fn no_cleanup() -> TaskCleanup {
    TaskCleanup {
        run: CleanupRun::Python,
        timeout_ms: None,
        retries: 0,
        retry_delay_ms: 0,
        handles: None,
        anywhere: false,
    }
}

/// The cleanup of an attempt whose worker is gone: it didn't run, or not to its end, and that
/// fails the run (decision 220).
fn cleanup_not_run(worker_id: &str, wanted: &TaskCleanup) -> CleanupEnd {
    let message = if wanted.anywhere {
        format!(
            "the cleanup didn't run to its end: worker `{worker_id}` is gone, the last of {} it \
             may be moved to",
            wanted.retries.saturating_add(1)
        )
    } else {
        format!(
            "the cleanup didn't run to its end: worker `{worker_id}` is gone; with \
             `cleanup_anywhere=True` it runs on another worker"
        )
    };
    CleanupEnd {
        result: CleanupResult::WorkerLost,
        tries: 0,
        duration_ms: 0,
        error_kind: Some("worker_lost".to_owned()),
        error_summary: Some(ErrorSummary::json("WorkerLost", &message)),
    }
}

/// A `Finished`'s cleanup, read.
fn cleanup_end(cleanup: &pb::CleanupResult, worker_id: &str) -> Result<CleanupEnd, DispatchError> {
    Ok(CleanupEnd {
        result: cleanup_result_from_proto(cleanup.status).map_err(|e| {
            DispatchError::BadMessage {
                worker_id: worker_id.to_owned(),
                reason: e.to_string(),
            }
        })?,
        tries: cleanup.tries,
        duration_ms: cleanup.duration_ms,
        error_kind: non_empty(&cleanup.error_kind),
        error_summary: (!cleanup.error_summary_json.is_empty())
            .then(|| String::from_utf8_lossy(&cleanup.error_summary_json).into_owned()),
    })
}

/// The error summary of an attempt that was running when its worker's agent restarted (the
/// agent doesn't report it again: it may have been killed for using too much memory).
fn restarted_summary(worker_id: &str) -> String {
    ErrorSummary::json(
        "WorkerRestarted",
        &format!("worker `{worker_id}` restarted while the attempt ran (possibly out of memory)"),
    )
}

/// The error summary of an attempt whose worker was lost (no sign of life for the lease).
fn not_started_summary(worker_id: &str) -> String {
    ErrorSummary::json(
        "WorkerLeft",
        &format!("worker `{worker_id}` left before starting the attempt: it runs again"),
    )
}

fn lost_summary(worker_id: &str) -> String {
    ErrorSummary::json(
        "WorkerLost",
        &format!(
            "worker `{worker_id}` stopped answering while the attempt ran (its pod or node \
             gone, or out of memory)"
        ),
    )
}

/// An error summary (JSON text) with `context` before its message; a new one (`CodeChanged`)
/// without, keeping a summary that doesn't read as its text.
fn with_context(summary: Option<&str>, context: &str) -> String {
    match summary.map(|s| (s, serde_json::from_str::<ErrorSummary>(s))) {
        Some((_, Ok(mut summary))) => {
            summary.message = format!("{context}: {}", summary.message);
            serde_json::to_string(&summary).unwrap_or_default()
        }
        Some((text, Err(_))) => ErrorSummary::json("CodeChanged", &format!("{context}: {text}")),
        None => ErrorSummary::json("CodeChanged", context),
    }
}

/// Why `message` can't go to a worker that takes at most `limit` bytes, if it can't: a run
/// handed to a `wait_for=` function rides in it (decision 353; Grok's and Codex's reviews).
pub(crate) fn too_big_to_send(message: &pb::MasterMessage, limit: usize) -> Option<String> {
    let size = prost::Message::encoded_len(message);
    (size > limit).then(|| {
        format!(
            "its assignment, with the run it is given, is {size} bytes, over the {limit} a \
             worker takes ([grpc] max_message_size)"
        )
    })
}

fn inputs_too_large() -> AttemptEnd {
    AttemptEnd {
        error_kind: Some(INPUTS_TOO_LARGE.to_owned()),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    }
}

fn non_empty(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

fn proto_time(t: Timestamp) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.as_second(),
        nanos: t.subsec_nanosecond(),
    }
}

/// The inputs of an open attempt as JSON (what `FetchInputs` streams when they were too big for
/// `AssignTask`); `None` for an unknown or ended attempt. Only reads the database, so it needs
/// none of the master's state, nor its lock: a busy master doesn't hold a task's start up (B11).
///
/// # Errors
///
/// [`DispatchError::Db`] if the database fails.
pub async fn attempt_inputs(
    db: &Database,
    attempt_id: &str,
) -> Result<Option<Vec<u8>>, DispatchError> {
    let Some(attempt) = db.attempt(attempt_id).await? else {
        return Ok(None);
    };
    if attempt.status.is_some() {
        return Ok(None);
    }
    let Some(run) = db.run_tasks(&attempt.run_id).await? else {
        return Ok(None);
    };
    let Some(task) = run.dag.task(&attempt.task_id) else {
        return Ok(Some(b"{}".to_vec()));
    };
    let mut resolved = mapping::Resolved::new();
    let inputs = mapping::inputs(
        db,
        &run.dag,
        &attempt.run_id,
        (task, attempt.map_index),
        &mut resolved,
    )
    .await;
    let inputs = match inputs {
        Ok(inputs) => inputs,
        Err(InputsError::Db(error)) => return Err(error.into()),
        // Ended when it was assigned: never sent.
        Err(InputsError::NotArguments(_)) => return Ok(None),
    };
    Ok(Some(
        serde_json::to_vec(&inputs).unwrap_or_else(|_| b"{}".to_vec()),
    ))
}

/// The outcomes of the upstream tasks the task uses (`""` for a skipped one), for
/// `AssignTask`: `.on(...)` references and `.outcome` read them.
async fn upstream_outcomes(
    db: &Database,
    dag: &schedr_model::Dag,
    run_id: &str,
    task_id: &TaskId,
) -> Result<BTreeMap<String, String>, DispatchError> {
    let Some(task) = dag.task(task_id) else {
        return Ok(BTreeMap::new());
    };
    let used: BTreeSet<&TaskId> = task
        .references()
        .into_iter()
        .filter_map(Reference::upstream_task)
        .collect();
    if used.is_empty() {
        return Ok(BTreeMap::new());
    }
    let wanted: Vec<&TaskId> = used.into_iter().collect();
    Ok(db
        .upstream_outcomes(run_id, &wanted)
        .await?
        .into_iter()
        .map(|(task, outcome)| (task.to_string(), outcome.unwrap_or_default()))
        .collect())
}

/// Whether `error` is one task's own (data that won't read, a statement the database
/// refuses), so that setting the task aside lets the others go on; not the database gone, a
/// time limit, or the lease lost, which are every task's.
fn task_specific(error: &DispatchError) -> bool {
    let db = match error {
        DispatchError::Db(db) | DispatchError::Execution(ExecutionError::Db(db)) => db,
        DispatchError::Execution(_) | DispatchError::BadMessage { .. } => return true,
    };
    db.is_data_fault()
}

/// Whether a task waits for another DAG's run before anything runs it: a `wait_for` node, or an
/// `@sensor(wait_for=...)` (decision 353).
/// Whether a task is a `TriggerDagOperator`, which the master carries out (decision 361).
const fn starts_a_run(task: &schedr_model::Task) -> bool {
    matches!(task.kind, TaskKind::TriggerDag { .. })
}

fn waits_for_a_run(task: &schedr_model::Task) -> bool {
    matches!(
        task.kind,
        TaskKind::WaitFor { .. }
            | TaskKind::WaitForEvent { .. }
            | TaskKind::Sensor {
                wait_for: Some(_),
                ..
            }
    )
}

/// Whether a run found (or its task) has ended, whatever its state: an `@sensor(wait_for=...)`
/// is called for any (`missing`: the run ended without the task).
fn run_ended(state: &str) -> bool {
    matches!(
        state,
        "success" | "failed" | "upstream_failed" | "skipped" | "cancelled" | "missing"
    )
}

/// What the run found says (the newest planned for the moment, or the first to end): its ID
/// when it succeeded, `Failed` when it ended otherwise, nothing while it hasn't ended or none
/// is there.
fn decide(found: Option<&OtherRun>) -> Option<WaitEnd> {
    let run = found?;
    match run.state.as_str() {
        "success" => Some(WaitEnd::Ran {
            run_id: run.run_id.clone(),
        }),
        "failed" | "upstream_failed" | "skipped" | "cancelled" => Some(WaitEnd::Failed {
            state: run.state.clone(),
            run_id: run.run_id.clone(),
        }),
        _ => None,
    }
}
