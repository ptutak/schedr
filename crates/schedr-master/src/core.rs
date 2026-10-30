//! The master's protocol logic, apart from any transport: registering workers, their messages,
//! disconnects, the periodic tick, parse uploads and inputs (see PROJECT.md, Master–worker
//! protocol and Worker lifecycle).
//!
//! The gRPC service ([`crate::service`]) only moves messages between workers' streams and
//! this core, so everything here is tested without a network.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::workers::{WorkerRegistration, WorkerWarning};
use schedr_db::{Database, DbError};
use schedr_model::{SchedrVersion, WorkerState};
use schedr_proto::convert::parsed_file_from_proto;
use schedr_proto::pb::{self, master_message, worker_message};
use schedr_proto::{PROTOCOL_VERSION, check_worker_version};

use crate::commands::{Applied, CommandError, Commander};
use crate::discovery::{Discovery, ParseUpload};
use crate::dispatch::{
    DEFAULT_CANCEL_GRACE, DEFAULT_CODE_WAIT, DispatchError, Dispatcher, Outgoing,
};
use crate::metrics::MasterMetrics;
use crate::parse_jobs::{PARSE_JOB_TIMEOUT, ParseAssignment, ParseJobs, ParseKey};
use crate::quiet::{QuietSettings, Wakes};
use crate::scheduler::{Scheduler, SchedulerError};

#[cfg(test)]
mod tests;

/// The master's timing, sent to every worker in `Registered` so all behave the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// How often workers send `Heartbeat`.
    pub heartbeat_interval: Duration,
    /// A worker silent for this long is lost.
    pub lease_timeout: Duration,
    /// The time between SIGTERM and SIGKILL when an attempt is cancelled.
    pub cancel_grace: Duration,
    /// How long a worker has for a parse job.
    pub parse_job_timeout: Duration,
    /// How long a task's code may be missing on every eligible worker.
    pub code_wait: Duration,
    /// How long a task whose assignment fails on its own is set aside.
    pub set_aside: Duration,
    /// How a commit cut off is read back (see [`crate::dispatch::ReadBack`]).
    pub read_back: crate::dispatch::ReadBack,
    /// How often to try listening for commands again after it failed to start.
    pub listen_retry: Duration,
    /// Which DAGs a round visits (`[master] quiet_dag*`).
    pub quiet: QuietSettings,
    /// The largest message a worker may send (`[master] grpc_max_message_size`).
    pub max_message_size: usize,
    /// The most instances a mapped task may have (`[mapping] max_instances`).
    pub max_instances: usize,
}

impl Default for Settings {
    /// Heartbeat every 10 s, lost after 60 s, 30 s cancel grace, 10 min per parse job, 10 min
    /// for missing code, messages up to 64 MiB.
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            lease_timeout: Duration::from_secs(60),
            cancel_grace: DEFAULT_CANCEL_GRACE,
            parse_job_timeout: PARSE_JOB_TIMEOUT,
            code_wait: DEFAULT_CODE_WAIT,
            set_aside: crate::dispatch::DEFAULT_SET_ASIDE,
            read_back: crate::dispatch::ReadBack::default(),
            listen_retry: Duration::from_mins(30),
            quiet: QuietSettings::default(),
            max_message_size: crate::service::DEFAULT_MAX_MESSAGE_SIZE,
            max_instances: crate::execution::DEFAULT_MAX_INSTANCES,
        }
    }
}

/// Why the master refused or failed something.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// A registration is refused (the worker is told why and the stream ends).
    #[error("registration refused: {0}")]
    Refused(String),
    /// A message can't be understood.
    #[error("message from worker `{worker_id}`: {reason}")]
    BadMessage {
        /// The worker.
        worker_id: String,
        /// What is wrong.
        reason: String,
    },
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// Dispatching failed.
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    /// Applying commands failed.
    #[error(transparent)]
    Command(#[from] CommandError),
    /// The scheduler failed.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
}

/// A connected (or recently disconnected) worker.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Connection {
    incarnation: String,
    schedr_version: String,
    last_seen: Timestamp,
    /// When `last_seen` was last written to the registry (at most once per heartbeat
    /// interval).
    persisted: Timestamp,
    live: bool,
}

/// What has arrived from workers before the master handled it (cheap to clone; clones share
/// it). A stream's reader records each message as it is read, before the message waits for the
/// master's lock, so a busy master doesn't count its own delay against workers:
/// - when each worker's latest message arrived: the lease counts from it, so a master stuck in
///   a slow write doesn't declare workers lost whose messages keep arriving (B6);
/// - which attempts' `Started` (or `Finished`) arrived and wait to be handled: they aren't
///   taken back as never started (B11).
#[derive(Debug, Clone, Default)]
pub struct Arrivals(Arc<std::sync::Mutex<Arrived>>);

#[derive(Debug, Default)]
struct Arrived {
    last: BTreeMap<String, Timestamp>,
    /// Attempts whose start arrived and waits to be handled, with their worker.
    started: BTreeMap<String, String>,
}

impl Arrivals {
    fn inner(&self) -> std::sync::MutexGuard<'_, Arrived> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A message from `worker_id` arrived `at`.
    pub fn arrived(&self, worker_id: &str, at: Timestamp) {
        let mut inner = self.inner();
        let last = inner.last.entry(worker_id.to_owned()).or_insert(at);
        *last = (*last).max(at);
    }

    /// When the latest message from `worker_id` arrived, if one has since it was forgotten.
    #[must_use]
    pub fn last(&self, worker_id: &str) -> Option<Timestamp> {
        self.inner().last.get(worker_id).copied()
    }

    /// Forgets `worker_id` (it is gone), and the starts of its that wait: their messages won't
    /// be handled any more.
    pub fn forget(&self, worker_id: &str) {
        let mut inner = self.inner();
        inner.last.remove(worker_id);
        inner.started.retain(|_, worker| worker != worker_id);
    }

    /// A `Started` or `Finished` for `attempt_id` arrived from `worker_id`: the attempt started.
    pub fn start_arrived(&self, worker_id: &str, attempt_id: &str) {
        self.inner()
            .started
            .insert(attempt_id.to_owned(), worker_id.to_owned());
    }

    /// Whether a `Started` or `Finished` for `attempt_id` arrived and isn't handled yet.
    #[must_use]
    pub fn start_waiting(&self, attempt_id: &str) -> bool {
        self.inner().started.contains_key(attempt_id)
    }

    /// The message about `attempt_id`'s start was handled (or failed to be).
    pub fn start_handled(&self, attempt_id: &str) {
        self.inner().started.remove(attempt_id);
    }
}

/// The master's state between messages.
#[derive(Debug)]
pub struct MasterCore {
    db: Database,
    settings: Settings,
    discovery: Discovery,
    dispatcher: Dispatcher,
    scheduler: Scheduler,
    commander: Commander,
    connections: BTreeMap<String, Connection>,
    arrivals: Arrivals,
    metrics: MasterMetrics,
}

impl MasterCore {
    /// The master over `db`, with no workers yet.
    #[must_use]
    pub fn new(db: Database, settings: Settings) -> Self {
        let arrivals = Arrivals::default();
        // What changes runs or DAGs marks them, so the scheduler wakes them if quiet.
        let wakes = Wakes::default();
        let mut discovery = Discovery::new(
            db.clone(),
            SchedrVersion::current(),
            ParseJobs::new(settings.parse_job_timeout),
        )
        .with_max_message_size(settings.max_message_size);
        discovery.set_wakes(wakes.clone());
        let mut dispatcher = Dispatcher::new(db.clone(), settings.cancel_grace)
            .with_code_wait(settings.code_wait)
            .with_set_aside(settings.set_aside, settings.read_back)
            .with_arrivals(arrivals.clone());
        dispatcher.set_wakes(wakes.clone());
        dispatcher.set_max_instances(settings.max_instances);
        Self {
            discovery,
            dispatcher,
            scheduler: Scheduler::with_quiet(db.clone(), settings.quiet, wakes),
            commander: Commander::new(db.clone()),
            connections: BTreeMap::new(),
            arrivals,
            metrics: MasterMetrics::default(),
            settings,
            db,
        }
        // One set shared by every part, replaced by the caller's to register them.
        .with_metrics(MasterMetrics::default())
    }

    /// The master recording its metrics in `metrics` (registered by the caller).
    #[must_use]
    pub fn with_metrics(mut self, metrics: MasterMetrics) -> Self {
        self.dispatcher.set_metrics(metrics.clone());
        self.discovery.set_metrics(metrics.clone());
        self.commander.set_metrics(metrics.clone());
        self.scheduler.set_metrics(metrics.clone());
        self.metrics = metrics;
        self
    }

    /// Its metrics.
    #[must_use]
    pub const fn metrics(&self) -> &MasterMetrics {
        &self.metrics
    }

    /// Its database.
    #[must_use]
    pub const fn db(&self) -> &Database {
        &self.db
    }

    /// The master's timing.
    #[must_use]
    pub const fn settings(&self) -> Settings {
        self.settings
    }

    /// The dispatcher (its workers, for status and tests).
    #[must_use]
    pub const fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    /// When each worker's latest message arrived, for the streams' readers to record.
    #[must_use]
    pub fn arrivals(&self) -> Arrivals {
        self.arrivals.clone()
    }

    /// Reads the worker registry back at start: every worker not gone counts as disconnected
    /// from `now`, so its lease starts from zero (master downtime doesn't count against
    /// workers) and one that never comes back is lost, its attempts with it. Returns how many
    /// workers were restored.
    ///
    /// # Errors
    ///
    /// [`CoreError::Db`] if the database fails.
    pub async fn restore(&mut self, now: Timestamp) -> Result<usize, CoreError> {
        let mut restored = 0;
        for worker in self.db.workers().await? {
            if worker.state.is_gone() {
                continue;
            }
            self.db
                .set_worker_state(&worker.id, WorkerState::Disconnected, now)
                .await?;
            self.connections.insert(
                worker.id,
                Connection {
                    incarnation: worker.incarnation,
                    schedr_version: worker.schedr_version,
                    last_seen: now,
                    persisted: now,
                    live: false,
                },
            );
            restored += 1;
        }
        Ok(restored)
    }

    /// Whether a worker has a live stream.
    #[must_use]
    pub fn is_live(&self, worker_id: &str) -> bool {
        self.connections.get(worker_id).is_some_and(|c| c.live)
    }

    /// A worker registers (the first message on its stream). Returns `Registered`, with a
    /// decision per journal entry, and the parse jobs for its roots.
    ///
    /// # Errors
    ///
    /// [`CoreError::Refused`] for an incompatible protocol version or a worker ID that already
    /// has a live stream; [`CoreError::Db`] / [`CoreError::Dispatch`] if the database fails.
    pub async fn register(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<(pb::Registered, Vec<Outgoing>), CoreError> {
        check_worker_version(PROTOCOL_VERSION, register.protocol)
            .map_err(|e| CoreError::Refused(e.to_string()))?;
        let worker_id = register.worker_id.as_str();
        if worker_id.is_empty() {
            return Err(CoreError::Refused(
                "the worker sent no worker ID".to_owned(),
            ));
        }
        if self.is_live(worker_id) {
            return Err(CoreError::Refused(format!(
                "worker `{worker_id}` already has a live stream"
            )));
        }
        let accepted = self.accept(register, now).await;
        if accepted.is_err()
            && let Some(connection) = self.connections.get_mut(worker_id)
        {
            // A registration that failed half-way (the database) leaves no live stream behind:
            // the worker's next attempt is accepted, or its lease runs out.
            connection.live = false;
        }
        accepted
    }

    /// Accepts a registration that passed the checks: reconciles a replaced incarnation,
    /// records the worker, and decides on its journal.
    async fn accept(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<(pb::Registered, Vec<Outgoing>), CoreError> {
        let worker_id = register.worker_id.as_str();
        let replaced = self
            .connections
            .get(worker_id)
            .is_some_and(|known| known.incarnation != register.incarnation_id);
        if replaced {
            let reported = register
                .journal
                .iter()
                .map(|entry| entry.attempt_id.as_str())
                .collect();
            let ended = self
                .dispatcher
                .incarnation_replaced(worker_id, &reported, now)
                .await?;
            tracing::warn!(
                worker_id,
                ended,
                "worker restarted (a new incarnation): its unreported attempts are lost \
                 (possibly out of memory)"
            );
        }
        self.connections.insert(
            worker_id.to_owned(),
            Connection {
                incarnation: register.incarnation_id.clone(),
                schedr_version: register.schedr_version.clone(),
                last_seen: now,
                persisted: now,
                live: true,
            },
        );
        let protocol = register
            .protocol
            .as_ref()
            .map(|p| format!("{}.{}", p.major, p.minor))
            .unwrap_or_default();
        self.db
            .register_worker(
                &WorkerRegistration {
                    id: worker_id,
                    incarnation: &register.incarnation_id,
                    protocol_version: &protocol,
                    schedr_version: &register.schedr_version,
                    queues: &register.queues,
                    slots: register.slots,
                    log_endpoint: &register.log_endpoint,
                },
                now,
            )
            .await?;
        self.dispatcher
            .register(
                worker_id,
                &register.incarnation_id,
                &register.queues,
                register.slots,
                now,
            )
            .await?;
        let mut outgoing = self.report_roots(register, now).await?;
        let mut decisions = Vec::with_capacity(register.journal.len());
        for entry in &register.journal {
            let (decision, cancel) = self.journal_decision(worker_id, entry, now).await?;
            decisions.push(pb::JournalDecision {
                attempt_id: entry.attempt_id.clone(),
                decision: decision.into(),
            });
            outgoing.extend(cancel);
        }
        let registered = pb::Registered {
            timing: Some(pb::Timing {
                heartbeat_interval_ms: millis(self.settings.heartbeat_interval),
                lease_timeout_ms: millis(self.settings.lease_timeout),
                cancel_grace_ms: millis(self.settings.cancel_grace),
            }),
            decisions,
            max_message_size: u64::try_from(self.settings.max_message_size).unwrap_or(u64::MAX),
        };
        Ok((registered, outgoing))
    }

    /// A message on a registered worker's stream.
    ///
    /// # Errors
    ///
    /// [`CoreError::BadMessage`] for a message that doesn't fit (a second `Register`, unknown
    /// enum values), or a database failure.
    pub async fn handle(
        &mut self,
        worker_id: &str,
        message: worker_message::Message,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let heartbeat = SignedDuration::try_from(self.settings.heartbeat_interval)
            .unwrap_or(SignedDuration::MAX);
        if let Some(connection) = self.connections.get_mut(worker_id) {
            connection.last_seen = now;
            if now.duration_since(connection.persisted) >= heartbeat {
                connection.persisted = now;
                // A sign of life in the registry is for people: a failed write mustn't cost
                // the message it came with (the next interval writes it again).
                if let Err(error) = self.db.worker_seen(worker_id, now).await {
                    tracing::warn!(worker_id, %error, "worker's last heartbeat not recorded");
                }
            }
        }
        Ok(match message {
            worker_message::Message::Register(_) => {
                return Err(CoreError::BadMessage {
                    worker_id: worker_id.to_owned(),
                    reason: "`Register` is only the first message".to_owned(),
                });
            }
            worker_message::Message::Heartbeat(_) => Vec::new(),
            // For people, like the last heartbeat: a failed write is logged, not the message's
            // failure (the worker sends them again after its next registration).
            worker_message::Message::Warnings(reported) => {
                let warnings: Vec<WorkerWarning> = reported
                    .warnings
                    .into_iter()
                    .map(|w| WorkerWarning {
                        kind: w.kind,
                        interpreter: w.interpreter,
                        message: w.message,
                    })
                    .collect();
                if let Err(error) = self.db.set_worker_warnings(worker_id, &warnings).await {
                    tracing::warn!(worker_id, %error, "worker's warnings not recorded");
                }
                Vec::new()
            }
            // A message's writes are one transaction: stored whole or not at all, with one
            // lease check and one commit (T1, 2.10b).
            worker_message::Message::CodeVersionChanged(changed) => match changed.root {
                Some(root) => {
                    let db = self.db.clone();
                    Box::pin(db.atomically(self.root_reported(worker_id, &root, now))).await?
                }
                None => Vec::new(),
            },
            // Rolled back, their changes to memory are put back too; a commit cut off is read
            // back first (D1): stored, memory stays as the message left it.
            worker_message::Message::Started(started) => {
                let (db, memory) = (self.db.clone(), self.dispatcher.memory());
                let handled =
                    Box::pin(db.atomically(self.dispatcher.started(worker_id, &started, now)))
                        .await;
                self.arrivals.start_handled(&started.attempt_id);
                let id = started.attempt_id.as_str();
                match self
                    .settle(handled, memory, id, |a| a.is_some_and(|a| a.started))
                    .await?
                {
                    Some(out) => out.into_iter().collect(),
                    None => Vec::new(),
                }
            }
            worker_message::Message::Rejected(rejected) => {
                let (db, memory) = (self.db.clone(), self.dispatcher.memory());
                let handled =
                    Box::pin(db.atomically(self.dispatcher.rejected(worker_id, &rejected, now)))
                        .await;
                let id = rejected.attempt_id.as_str();
                // Undone, the attempt is deleted.
                self.settle(handled, memory, id, |a| a.is_none()).await?;
                Vec::new()
            }
            worker_message::Message::Finished(finished) => {
                let (db, memory) = (self.db.clone(), self.dispatcher.memory());
                let handled =
                    Box::pin(db.atomically(self.dispatcher.finished(worker_id, &finished, now)))
                        .await;
                self.arrivals.start_handled(&finished.attempt_id);
                let id = finished.attempt_id.as_str();
                let settled = self.settle(handled, memory, id, |a| {
                    a.is_some_and(|a| a.status.is_some())
                });
                vec![
                    settled
                        .await?
                        .unwrap_or_else(|| crate::dispatch::finished_ack(worker_id, id)),
                ]
            }
            worker_message::Message::Draining(_) => {
                self.dispatcher.draining(worker_id);
                self.db
                    .set_worker_state(worker_id, WorkerState::Draining, now)
                    .await?;
                let moved = self.discovery.stop_parsing(worker_id, now);
                moved.into_iter().map(assign_parse).collect()
            }
            worker_message::Message::Deregister(_) => self.deregistered(worker_id, now).await,
        })
    }

    /// A worker's stream ended: it may reconnect (same incarnation, tasks still running)
    /// within the lease timeout, or it is lost at the next tick after that.
    ///
    /// # Errors
    ///
    /// [`CoreError::Db`] if the registry can't be written.
    pub async fn disconnected(&mut self, worker_id: &str, now: Timestamp) -> Result<(), CoreError> {
        if let Some(connection) = self.connections.get_mut(worker_id)
            && connection.live
        {
            connection.live = false;
            self.db
                .set_worker_state(worker_id, WorkerState::Disconnected, now)
                .await?;
        }
        Ok(())
    }

    /// One pass of the master's loop: commands, the scheduler, dispatching, parse jobs past
    /// their timeout, and workers past their lease.
    ///
    /// # Errors
    ///
    /// The first failure; what was done before it stays done, and the next tick goes on.
    pub async fn tick(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, CoreError> {
        let (outgoing, result) = self.tick_partial(now).await;
        result.map(|()| outgoing)
    }

    /// As [`Self::tick`], with the messages built before a failure: each is built after the
    /// write it stands for is stored, so they are delivered whatever comes after (B7: a cancel
    /// dropped with its failed round was sent again only when the worker reconnected).
    pub async fn tick_partial(&mut self, now: Timestamp) -> (Vec<Outgoing>, Result<(), CoreError>) {
        let mut outgoing = Vec::new();
        let result = self.tick_into(&mut outgoing, now).await;
        (outgoing, result)
    }

    async fn tick_into(
        &mut self,
        outgoing: &mut Vec<Outgoing>,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let mut applied = Applied::default();
        let commands = self
            .commander
            .apply_into(&self.scheduler, &mut self.dispatcher, &mut applied, now)
            .await;
        outgoing.append(&mut applied.outgoing);
        // The drains and resumes decided before a failing command get their follow-up too:
        // being decided, they won't be applied again.
        for (worker_id, drained) in std::mem::take(&mut applied.drained) {
            if drained {
                let moved = self.discovery.stop_parsing(&worker_id, now);
                outgoing.extend(moved.into_iter().map(assign_parse));
            } else if self
                .dispatcher
                .workers()
                .get(&worker_id)
                .is_none_or(|w| w.state == crate::workers::WorkerState::Active)
            {
                // Not if the agent itself is draining (shutting down).
                let jobs = self.discovery.allow_parsing(&worker_id, now).await?;
                outgoing.extend(jobs.into_iter().map(assign_parse));
            }
        }
        commands?;
        self.scheduler.pass(now).await?;
        self.dispatcher.step_into(now, outgoing).await?;
        let expired = self.discovery.jobs_mut().expire(now);
        outgoing.extend(expired.into_iter().map(assign_parse));
        let lease =
            SignedDuration::try_from(self.settings.lease_timeout).unwrap_or(SignedDuration::MAX);
        let lost: Vec<String> = self
            .connections
            .iter()
            .filter(|(id, c)| {
                // The later of its last handled message and its last arrived one.
                let seen = self
                    .arrivals
                    .last(id)
                    .map_or(c.last_seen, |a| a.max(c.last_seen));
                now.duration_since(seen) > lease
            })
            .map(|(id, _)| id.clone())
            .collect();
        for worker_id in lost {
            self.forget(&worker_id, WorkerState::Lost, now, outgoing)
                .await?;
        }
        Ok(())
    }

    /// A worker uploads a parse result (unary call).
    ///
    /// # Errors
    ///
    /// [`CoreError::BadMessage`] for a result missing its root or with DAG JSON that doesn't
    /// read; a database failure.
    pub async fn upload(
        &mut self,
        result: pb::ParseResult,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let bad = |reason: String| CoreError::BadMessage {
            worker_id: result.worker_id.clone(),
            reason,
        };
        let root = result
            .root
            .clone()
            .ok_or_else(|| bad("a parse result without its root".to_owned()))?;
        let files = result
            .files
            .iter()
            .cloned()
            .map(parsed_file_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| bad(e.to_string()))?;
        self.discovery
            .parse_uploaded(
                ParseUpload {
                    worker_id: result.worker_id.clone(),
                    key: ParseKey {
                        root: root.root,
                        content_hash: root.content_hash,
                        schedr_version: result.schedr_version.clone(),
                    },
                    files,
                },
                now,
            )
            .await?;
        self.sync_code();
        Ok(())
    }

    /// An open attempt's inputs as JSON (`FetchInputs`); `None` for an unknown or ended one.
    ///
    /// # Errors
    ///
    /// [`CoreError::Dispatch`] if the database fails.
    pub async fn inputs(&self, attempt_id: &str) -> Result<Option<Vec<u8>>, CoreError> {
        Ok(self.dispatcher.attempt_inputs(attempt_id).await?)
    }

    /// What a worker message's handling comes to: its result when stored; when its commit was
    /// cut off and attempt `id` (or its absence) reads back as `landed` (see
    /// [`crate::dispatch::ReadBack`]), `Ok(None)`: stored after all, memory kept; otherwise
    /// the error, memory put back. A read that fails tells nothing.
    async fn settle<T>(
        &mut self,
        handled: Result<T, DispatchError>,
        memory: crate::dispatch::Memory,
        id: &str,
        landed: impl Fn(Option<&schedr_db::attempts::StoredAttempt>) -> bool,
    ) -> Result<Option<T>, CoreError> {
        match handled {
            Ok(done) => Ok(Some(done)),
            Err(DispatchError::Db(DbError::CommitUnknown))
                if self
                    .settings
                    .read_back
                    .stored(|| async {
                        self.db
                            .attempt(id)
                            .await
                            .is_ok_and(|attempt| landed(attempt.as_ref()))
                    })
                    .await =>
            {
                Ok(None)
            }
            Err(error) => {
                self.dispatcher.restore(memory);
                Err(error.into())
            }
        }
    }

    /// A registered worker's version, maintenance drain and roots: its reports replace what it
    /// reported before. Returns the parse jobs to send.
    async fn report_roots(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let worker_id = register.worker_id.as_str();
        let mut outgoing = Vec::new();
        self.dispatcher
            .set_worker_version(worker_id, register.schedr_version.parse().ok());
        // Drained for maintenance stays drained, whatever restarted (until resumed).
        if self.db.worker_drained(worker_id).await? {
            self.dispatcher.drain(worker_id);
            let moved = self.discovery.stop_parsing(worker_id, now);
            outgoing.extend(moved.into_iter().map(assign_parse));
        } else {
            outgoing.extend(
                self.discovery
                    .allow_parsing(worker_id, now)
                    .await?
                    .into_iter()
                    .map(assign_parse),
            );
        }
        // Its reports replace what it reported before: a root it no longer has stops counting.
        let keys: Vec<ParseKey> = register
            .roots
            .iter()
            .filter(|root| root.kind == i32::from(pb::RootKind::Dag))
            .map(|root| {
                self.dispatcher
                    .report_root(worker_id, &root.root, &root.content_hash);
                ParseKey {
                    root: root.root.clone(),
                    content_hash: root.content_hash.clone(),
                    schedr_version: register.schedr_version.clone(),
                }
            })
            .collect();
        let jobs = self.discovery.registered(worker_id, keys, now).await?;
        outgoing.extend(jobs.into_iter().map(assign_parse));
        self.sync_code();
        Ok(outgoing)
    }

    async fn root_reported(
        &mut self,
        worker_id: &str,
        root: &pb::RootHash,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        if root.kind != i32::from(pb::RootKind::Dag) {
            return Ok(Vec::new());
        }
        let version = self
            .connections
            .get(worker_id)
            .map(|c| c.schedr_version.clone())
            .unwrap_or_default();
        let key = ParseKey {
            root: root.root.clone(),
            content_hash: root.content_hash.clone(),
            schedr_version: version,
        };
        self.dispatcher
            .report_root(worker_id, &root.root, &root.content_hash);
        let assignment = self.discovery.root_reported(worker_id, key, now).await?;
        self.sync_code();
        Ok(assignment.into_iter().map(assign_parse).collect())
    }

    async fn journal_decision(
        &mut self,
        worker_id: &str,
        entry: &pb::JournalEntry,
        now: Timestamp,
    ) -> Result<(pb::AttemptDecision, Option<Outgoing>), CoreError> {
        let Some(attempt) = self.db.attempt(&entry.attempt_id).await? else {
            return Ok((pb::AttemptDecision::Kill, None));
        };
        if attempt.worker_id != worker_id {
            return Ok((pb::AttemptDecision::Kill, None));
        }
        let finished = entry.state == i32::from(pb::JournalState::Finished);
        Ok(match (attempt.status, finished) {
            // The master stored its result: the worker can forget it.
            (Some(_), true) => (pb::AttemptDecision::Ack, None),
            // The master ended it (lost, expired) while the worker still runs it.
            (Some(_), false) => (pb::AttemptDecision::Kill, None),
            // Still open: keep running (or send its `Finished` again). A cancel sent while
            // the worker was away was dropped, so it is sent again.
            (None, _) => {
                self.dispatcher.adopt(worker_id, &attempt.id, now);
                let cancelling = self
                    .db
                    .run_tasks(&attempt.run_id)
                    .await?
                    .is_some_and(|run| run.cancel_requested);
                let cancel = cancelling.then(|| Outgoing {
                    worker_id: worker_id.to_owned(),
                    message: pb::MasterMessage {
                        message: Some(master_message::Message::CancelAttempt(pb::CancelAttempt {
                            attempt_id: attempt.id.clone(),
                        })),
                    },
                });
                (pb::AttemptDecision::Continue, cancel)
            }
        })
    }

    /// A worker deregistered (`Offline`) or is lost (`Lost`): its attempts end `lost`, its
    /// parse jobs move on, and the registry keeps it listed in that state.
    async fn forget(
        &mut self,
        worker_id: &str,
        state: WorkerState,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        // Its state and the end of its attempts in one transaction, memory put back if it
        // fails, and the connection dropped only once everything below is stored: a failure
        // anywhere is retried whole by the next tick's lease check, which looks only at
        // connections (B42); each step can be done again. (`worker_lost` joins this
        // transaction; the snapshot here covers its memory too.)
        let (db, memory) = (self.db.clone(), self.dispatcher.memory());
        let stored = Box::pin(db.atomically(async {
            self.db.set_worker_state(worker_id, state, now).await?;
            self.dispatcher.worker_lost(worker_id, now).await?;
            Ok::<(), CoreError>(())
        }))
        .await;
        if let Err(error) = stored {
            self.dispatcher.restore(memory);
            return Err(error);
        }
        // Moved in memory: sent whatever fails after (B7).
        let moved = self.discovery.jobs_mut().worker_lost(worker_id, now);
        outgoing.extend(moved.into_iter().map(assign_parse));
        self.discovery.worker_gone(worker_id, now).await?;
        self.sync_code();
        self.connections.remove(worker_id);
        self.arrivals.forget(worker_id);
        Ok(())
    }

    /// A worker deregistered: forgotten as `Offline`. A failure is retried by the lease check
    /// (the connection stays); the parse jobs already moved go out either way, or they'd wait
    /// for their timeout.
    async fn deregistered(&mut self, worker_id: &str, now: Timestamp) -> Vec<Outgoing> {
        let mut outgoing = Vec::new();
        if let Err(error) = self
            .forget(worker_id, WorkerState::Offline, now, &mut outgoing)
            .await
        {
            tracing::error!(worker_id, %error, "a deregistered worker not forgotten yet");
        }
        outgoing
    }

    /// Tells the dispatcher each root's current code, as discovery decided it.
    fn sync_code(&mut self) {
        for (root, _, accepted, version) in self.discovery.current_code() {
            self.dispatcher.set_current_code(root, accepted, version);
        }
    }
}

fn assign_parse(assignment: ParseAssignment) -> Outgoing {
    Outgoing {
        worker_id: assignment.worker_id,
        message: pb::MasterMessage {
            message: Some(master_message::Message::AssignParse(pb::AssignParse {
                parse_job_id: assignment.job_id,
                root: Some(pb::RootHash {
                    kind: pb::RootKind::Dag.into(),
                    root: assignment.key.root,
                    content_hash: assignment.key.content_hash,
                }),
            })),
        },
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
