//! Giving tasks to workers and handling what workers report about them (see PROJECT.md, Task
//! assignment and Task rejections).
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
//! - An attempt with no `Started` within [`START_TIMEOUT`] ends `assignment_expired` and the
//!   task runs again without using a retry; a lost worker's attempts end `lost`.
//!
//! The dispatcher only builds messages; the gRPC server sends them (milestone 1.14).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::attempts::{AttemptEnd, ReadyInstance, StoredAttempt};
use schedr_db::{Database, DbError};
use schedr_model::runner::{DAG_FILE_MISSING, ErrorSummary, TASK_NOT_FOUND};
use schedr_model::{AttemptStatus, Reference, SchedrVersion, TaskId, TaskInstanceState};
use schedr_proto::convert::{
    RejectReaction, attempt_status_from_proto, reject_reaction, run_kind_to_proto,
};
use schedr_proto::pb;

use crate::core::Arrivals;
use crate::execution::{AttemptResult, Execution, ExecutionError, TaskCancel, TaskRetry};
use crate::metrics::{MasterMetrics, ReasonLabels};
use crate::quiet::Wakes;
use crate::workers::{Needs, Workers};

#[cfg(test)]
mod tests;

/// How long a worker has to report `Started` after an assignment.
pub const START_TIMEOUT: SignedDuration = SignedDuration::from_secs(10);

/// How long a task whose assignment failed on its own is left out of the rounds before it is
/// tried again, unless set otherwise (see [`Dispatcher::assign_into`]).
pub const DEFAULT_SET_ASIDE: Duration = Duration::from_mins(1);

/// The acknowledgement of `attempt_id`'s `Finished`, for `worker_id`: its result is stored.
#[must_use]
pub fn finished_ack(worker_id: &str, attempt_id: &str) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(pb::master_message::Message::FinishedAck(pb::FinishedAck {
                attempt_id: attempt_id.to_owned(),
            })),
        },
    }
}

/// How a commit cut off by the time limit or a lost connection is read back: whether the work
/// is stored, checked at once, then every `interval` for up to `window`, since a server slow
/// enough to have cut it off may still be committing it (`[master] read_back_window`,
/// `read_back_interval`). Never seen meanwhile: not stored (a stored round's attempts are
/// taken back after [`START_TIMEOUT`] and run again). The master holds its lock meanwhile:
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

/// The error kind of a task whose inputs are over [`MAX_INPUTS`].
pub const INPUTS_TOO_LARGE: &str = "inputs_too_large";

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

/// Tasks given to workers and their reports handled.
#[derive(Debug)]
pub struct Dispatcher {
    db: Database,
    execution: Execution,
    workers: Workers,
    cancel_grace: Duration,
    /// Workers that rejected a task instance for a worker-specific reason, with the detail.
    rejections: BTreeMap<String, BTreeMap<String, String>>,
    /// Task instances whose code a worker didn't have: since when, and which workers said so.
    code_missing: BTreeMap<String, CodeMissing>,
    code_wait: SignedDuration,
    /// Each root's accepted content hashes (its current code, and newer code whose parse is on
    /// its way) and the schedr version of its current parse: its tasks go to workers at one of
    /// those hashes and of at least that version.
    current_code: BTreeMap<String, (BTreeSet<String>, Option<SchedrVersion>)>,
    metrics: MasterMetrics,
    /// Attempts whose `Started` arrived but waits for the master: never taken back (B11).
    arrivals: Arrivals,
    /// Task instances whose assignment failed on its own, and until when they are left out.
    set_aside: BTreeMap<String, Timestamp>,
    /// How long they are left out.
    set_aside_for: SignedDuration,
    /// How a commit cut off is read back.
    read_back: ReadBack,
}

/// The dispatcher's memory that a worker's message changes (see [`Dispatcher::memory`]).
#[derive(Debug, Clone)]
pub struct Memory {
    workers: Workers,
    rejections: BTreeMap<String, BTreeMap<String, String>>,
    code_missing: BTreeMap<String, CodeMissing>,
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
            db,
            workers: Workers::new(),
            cancel_grace,
            rejections: BTreeMap::new(),
            code_missing: BTreeMap::new(),
            code_wait: SignedDuration::ZERO,
            current_code: BTreeMap::new(),
            metrics: MasterMetrics::default(),
            arrivals: Arrivals::default(),
            set_aside: BTreeMap::new(),
            set_aside_for: SignedDuration::try_from(DEFAULT_SET_ASIDE)
                .unwrap_or(SignedDuration::MAX),
            read_back: ReadBack::default(),
        }
        .with_code_wait(DEFAULT_CODE_WAIT)
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
    /// missing), to [`Self::restore`] if the message's transaction rolls back: memory then
    /// shows only what is stored (code review of 2.10b; metrics already counted stay).
    #[must_use]
    pub fn memory(&self) -> Memory {
        Memory {
            workers: self.workers.clone(),
            rejections: self.rejections.clone(),
            code_missing: self.code_missing.clone(),
        }
    }

    /// Puts back a copy taken by [`Self::memory`].
    pub fn restore(&mut self, memory: Memory) {
        self.workers = memory.workers;
        self.rejections = memory.rejections;
        self.code_missing = memory.code_missing;
    }

    /// The dispatcher reading which attempts' `Started` arrived from `arrivals` (the core's, which
    /// the streams' readers fill), so it doesn't take them back as never started.
    #[must_use]
    pub fn with_arrivals(mut self, arrivals: Arrivals) -> Self {
        self.arrivals = arrivals;
        self
    }

    /// The dispatcher leaving a task whose assignment fails on its own out for `set_aside`,
    /// and reading a commit cut off back as `read_back` says (`[master] set_aside`,
    /// `read_back_window`, `read_back_interval`).
    #[must_use]
    pub fn with_set_aside(mut self, set_aside: Duration, read_back: ReadBack) -> Self {
        self.set_aside_for = SignedDuration::try_from(set_aside).unwrap_or(SignedDuration::MAX);
        self.read_back = read_back;
        self
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

    /// A worker's schedr version: it gets only tasks of parses no newer.
    pub fn set_worker_version(&mut self, worker_id: &str, version: Option<SchedrVersion>) {
        self.workers.set_version(worker_id, version);
    }

    /// A worker reported a root's content hash.
    pub fn report_root(&mut self, worker_id: &str, root: &str, hash: &str) {
        self.workers.report_root(worker_id, root, hash);
    }

    /// A reconnecting worker still runs `attempt_id`: it keeps its slot.
    pub fn adopt(&mut self, worker_id: &str, attempt_id: &str, now: Timestamp) {
        self.workers.reserve(worker_id, attempt_id, now);
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

    /// A worker is drained for maintenance: it gets no new tasks.
    pub fn drain(&mut self, worker_id: &str) {
        self.workers.set_drained(worker_id, true);
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
        let ready: Vec<ReadyInstance> = self
            .db
            .ready_instances(now)
            .await?
            .into_iter()
            .filter(|r| !self.set_aside.contains_key(&r.instance_id))
            .collect();
        // Nothing ready (an idle round): no transaction, nothing to keep.
        if ready.is_empty() {
            self.metrics.set_waiting(&[]);
            return Ok(());
        }
        // The step's assignments are one transaction: stored together, with one lease check
        // and one commit (T2, 2.10b). What they changed in memory (the slots reserved) and the
        // messages built for them go with a rollback.
        let (workers, built) = (self.workers.clone(), outgoing.len());
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
        outgoing.truncate(built);
        if !task_specific(&error) {
            return Err(error);
        }
        // One task may fail every time (its data won't read, the database refuses it): one at
        // a time, the others go on and it is set aside, instead of every round failing on it.
        tracing::warn!(%error, "a round's assignments failed together: assigning one at a time");
        self.assign_one_by_one(now, &ready, outgoing).await
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
            let (workers, built) = (self.workers.clone(), outgoing.len());
            let db = self.db.clone();
            let assigned =
                Box::pin(db.atomically(self.assign_one(now, one, outgoing, &mut tally))).await;
            let Err(error) = assigned else {
                made += outgoing.len() - built;
                self.count_assignments(outgoing.len() - built);
                continue;
            };
            self.workers = workers;
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
        let excluded: BTreeSet<String> = self
            .rejections
            .get(&ready.instance_id)
            .map(|r| r.keys().cloned().collect())
            .unwrap_or_default();
        let needs = self.needs(&ready.dag, &task.settings.queue, &ready.parse_version);
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
        let inputs = upstream_inputs(&self.db, &ready.dag, &ready.run_id, &ready.task_id).await?;
        if inputs.len() > MAX_INPUTS {
            self.end(
                &attempt_id,
                &ready.run_id,
                &ready.task_id,
                inputs_too_large(),
                now,
            )
            .await?;
            return Ok(());
        }
        self.workers.reserve(&worker_id, &attempt_id, now);
        tally.assigned += 1;
        let message = self.assign_message(ready, &worker_id, &attempt_id, try_number, inputs);
        outgoing.push(Outgoing {
            worker_id,
            message: pb::MasterMessage {
                message: Some(pb::master_message::Message::AssignTask(message)),
            },
        });
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
        let run = self.db.run_tasks(&attempt.run_id).await?;
        let cancelling = run.as_ref().is_some_and(|run| {
            run.cancel_requested
                || run
                    .instances
                    .iter()
                    .any(|i| i.id == attempt.instance_id && i.cancel_requested)
        });
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

    /// A worker reported `Finished`: stored once, and always acknowledged.
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
            let output = (status == AttemptStatus::Success)
                .then(|| serde_json::from_slice(&finished.output_json).ok())
                .flatten();
            let mut error_summary = (!finished.error_summary_json.is_empty())
                .then(|| String::from_utf8_lossy(&finished.error_summary_json).into_owned());
            // Only for a result not stored yet (a repeated one is dropped anyway).
            if attempt.status.is_none()
                && CODE_GONE_ERRORS.contains(&finished.error_kind.as_str())
                && let Some(context) = self.changed_since(&attempt).await?
            {
                error_summary = Some(with_context(error_summary.as_deref(), &context));
            }
            let end = AttemptEnd {
                status,
                exit_code: finished.exit_code,
                signal: finished.signal,
                // 0: not measured (a worker without `/proc`, or before 2.8).
                peak_memory_bytes: (finished.peak_memory_bytes > 0)
                    .then_some(finished.peak_memory_bytes),
                duration_ms: finished.duration_ms,
                error_kind: non_empty(&finished.error_kind),
                error_summary,
                code_hash: non_empty(&finished.code_hash),
            };
            let result = AttemptResult {
                status,
                error_kind: end.error_kind.clone(),
                output,
            };
            self.record(&attempt, &end, &result, now).await?;
        }
        Ok(finished_ack(worker_id, &finished.attempt_id))
    }

    /// Takes back assignments with no `Started` within [`START_TIMEOUT`]: they end
    /// `assignment_expired` and their tasks run again without using a retry. One whose
    /// `Started` arrived but waits for a busy master isn't taken back: the task did start
    /// (B11). Returns how many.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn expire(&mut self, now: Timestamp) -> Result<usize, DispatchError> {
        let cutoff = now.checked_sub(START_TIMEOUT).unwrap_or(Timestamp::MIN);
        let mut expired = self.db.unstarted_attempts(cutoff).await?;
        expired.retain(|attempt| !self.arrivals.start_waiting(&attempt.id));
        // At most so many a round (several writes each): after an outage, over a few (B18).
        expired.truncate(EXPIRIES_PER_ROUND);
        for attempt in &expired {
            let end = AttemptEnd::by_master(AttemptStatus::AssignmentExpired);
            self.record(
                attempt,
                &end,
                &by_master(AttemptStatus::AssignmentExpired),
                now,
            )
            .await?;
        }
        Ok(expired.len())
    }

    /// A worker's agent restarted (a new incarnation): the attempts it had started and doesn't
    /// report in its journal end `lost` with a `WorkerRestarted` error summary, each using a
    /// retry, as for a lost worker (the crash may have been the task's own; see PROJECT.md,
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
        let mut ended = 0;
        for attempt in self.db.open_attempts(Some(worker_id)).await? {
            if attempt.started && !reported.contains(attempt.id.as_str()) {
                let end = AttemptEnd {
                    error_summary: Some(restarted_summary(worker_id)),
                    ..AttemptEnd::by_master(AttemptStatus::Lost)
                };
                self.record(&attempt, &end, &by_master(AttemptStatus::Lost), now)
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
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.remove(worker_id);
        for attempt in self.db.open_attempts(Some(worker_id)).await? {
            // One it never started (sent as it began draining, say) never ran there: taken
            // back like an expired assignment, queued again without using a retry (B17).
            let (status, summary) = if attempt.started {
                (AttemptStatus::Lost, lost_summary(worker_id))
            } else {
                (
                    AttemptStatus::AssignmentExpired,
                    not_started_summary(worker_id),
                )
            };
            let end = AttemptEnd {
                error_summary: Some(summary),
                ..AttemptEnd::by_master(status)
            };
            self.record(&attempt, &end, &by_master(status), now).await?;
        }
        Ok(())
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
        task_id: &TaskId,
        now: Timestamp,
    ) -> Result<(TaskCancel, Vec<Outgoing>), DispatchError> {
        let cancelled = self.execution.cancel_task(run_id, task_id, now).await?;
        if cancelled != TaskCancel::Stopping {
            return Ok((cancelled, Vec::new()));
        }
        let stopping = self
            .db
            .open_attempts(None)
            .await?
            .into_iter()
            .filter(|a| a.run_id == run_id && &a.task_id == task_id)
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
        task_id: &TaskId,
        now: Timestamp,
    ) -> Result<TaskRetry, DispatchError> {
        let retried = self.execution.retry_task(run_id, task_id, now).await?;
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
        let running: BTreeSet<&TaskId> = cancelled.running.iter().collect();
        Ok(self
            .db
            .open_attempts(None)
            .await?
            .into_iter()
            .filter(|a| a.run_id == run_id && running.contains(&a.task_id))
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
        if self.db.end_attempt(&attempt.id, end, now).await? {
            self.metrics.attempt_ended(&end.status);
            self.execution
                .attempt_finished(&attempt.run_id, &attempt.task_id, result, now)
                .await?;
        }
        Ok(())
    }

    async fn end(
        &mut self,
        attempt_id: &str,
        run_id: &str,
        task_id: &TaskId,
        end: AttemptEnd,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let result = AttemptResult {
            status: end.status,
            error_kind: end.error_kind.clone(),
            output: None,
        };
        if self.db.end_attempt(attempt_id, &end, now).await? {
            self.metrics.attempt_ended(&end.status);
            self.execution
                .attempt_finished(run_id, task_id, &result, now)
                .await?;
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
        let stored = self.db.stored_dags().await?;
        let now = stored
            .iter()
            .find(|d| d.dag_id == ran.id)
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
        (
            "no_slot",
            format!("waiting for a free slot on the workers serving queue `{queue}`"),
        )
    }

    fn assign_message(
        &self,
        ready: &ReadyInstance,
        worker_id: &str,
        attempt_id: &str,
        try_number: u32,
        inputs: Vec<u8>,
    ) -> pb::AssignTask {
        let task = ready.dag.task(&ready.task_id);
        let external = inputs.len() > INLINE_INPUTS_LIMIT;
        pb::AssignTask {
            attempt_id: attempt_id.to_owned(),
            task_id: ready.task_id.to_string(),
            map_index: -1,
            try_number,
            run: Some(pb::RunInfo {
                run_id: ready.run_id.clone(),
                kind: run_kind_to_proto(ready.run_kind),
                planned_for: Some(proto_time(ready.planned_for)),
                since: Some(proto_time(ready.since)),
                dag_id: ready.dag.id.to_string(),
            }),
            task_json: task
                .and_then(|t| serde_json::to_vec(t).ok())
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
            memory_budget_bytes: 0,
            setrlimit: false,
        }
    }
}

fn cancel(worker_id: &str, attempt_id: &str) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(pb::master_message::Message::CancelAttempt(
                pb::CancelAttempt {
                    attempt_id: attempt_id.to_owned(),
                },
            )),
        },
    }
}

const fn by_master(status: AttemptStatus) -> AttemptResult {
    AttemptResult {
        status,
        error_kind: None,
        output: None,
    }
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
    Ok(Some(
        upstream_inputs(db, &run.dag, &attempt.run_id, &attempt.task_id).await?,
    ))
}

/// The upstream outputs the task uses, as JSON (`{task_id: output}`).
async fn upstream_inputs(
    db: &Database,
    dag: &schedr_model::Dag,
    run_id: &str,
    task_id: &TaskId,
) -> Result<Vec<u8>, DispatchError> {
    let Some(task) = dag.task(task_id) else {
        return Ok(b"{}".to_vec());
    };
    let used: BTreeSet<&TaskId> = task
        .references()
        .into_iter()
        .filter_map(Reference::upstream_task)
        .collect();
    if used.is_empty() {
        return Ok(b"{}".to_vec());
    }
    // Only the outputs it uses: not the whole run's, which grows with a DAG's width (B18).
    let wanted: Vec<&TaskId> = used.into_iter().collect();
    let outputs = db.outputs_of(run_id, &wanted).await?;
    let inputs: BTreeMap<&TaskId, &serde_json::Value> = outputs.iter().collect();
    Ok(serde_json::to_vec(&inputs).unwrap_or_else(|_| b"{}".to_vec()))
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
