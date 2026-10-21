//! The master's protocol logic, apart from any transport: registering workers, their messages,
//! disconnects, the periodic tick, parse uploads and inputs (see PROJECT.md, Master–worker
//! protocol and Worker lifecycle).
//!
//! The gRPC service ([`crate::service`]) only moves messages between workers' streams and
//! this core, so everything here is tested without a network.

use std::collections::BTreeMap;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::{Database, DbError};
use schedr_model::SchedrVersion;
use schedr_proto::convert::parsed_file_from_proto;
use schedr_proto::pb::{self, master_message, worker_message};
use schedr_proto::{PROTOCOL_VERSION, check_worker_version};

use crate::commands::{CommandError, Commander};
use crate::discovery::{Discovery, ParseUpload};
use crate::dispatch::{DEFAULT_CANCEL_GRACE, DispatchError, Dispatcher, Outgoing};
use crate::parse_jobs::{PARSE_JOB_TIMEOUT, ParseAssignment, ParseJobs, ParseKey};
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
}

impl Default for Settings {
    /// Heartbeat every 10 s, lost after 60 s, 30 s cancel grace, 10 min per parse job.
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            lease_timeout: Duration::from_secs(60),
            cancel_grace: DEFAULT_CANCEL_GRACE,
            parse_job_timeout: PARSE_JOB_TIMEOUT,
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
    live: bool,
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
}

impl MasterCore {
    /// The master over `db`, with no workers yet.
    #[must_use]
    pub fn new(db: Database, settings: Settings) -> Self {
        Self {
            discovery: Discovery::new(
                db.clone(),
                SchedrVersion::current(),
                ParseJobs::new(settings.parse_job_timeout),
            ),
            dispatcher: Dispatcher::new(db.clone(), settings.cancel_grace),
            scheduler: Scheduler::new(db.clone()),
            commander: Commander::new(db.clone()),
            connections: BTreeMap::new(),
            settings,
            db,
        }
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
        self.connections.insert(
            worker_id.to_owned(),
            Connection {
                incarnation: register.incarnation_id.clone(),
                schedr_version: register.schedr_version.clone(),
                last_seen: now,
                live: true,
            },
        );
        self.dispatcher
            .register(
                worker_id,
                &register.incarnation_id,
                &register.queues,
                register.slots,
                now,
            )
            .await?;
        let mut decisions = Vec::with_capacity(register.journal.len());
        let mut outgoing = Vec::new();
        for entry in &register.journal {
            let (decision, cancel) = self.journal_decision(worker_id, entry, now).await?;
            decisions.push(pb::JournalDecision {
                attempt_id: entry.attempt_id.clone(),
                decision: decision.into(),
            });
            outgoing.extend(cancel);
        }
        for root in &register.roots {
            outgoing.extend(self.root_reported(worker_id, root, now).await?);
        }
        let registered = pb::Registered {
            timing: Some(pb::Timing {
                heartbeat_interval_ms: millis(self.settings.heartbeat_interval),
                lease_timeout_ms: millis(self.settings.lease_timeout),
                cancel_grace_ms: millis(self.settings.cancel_grace),
            }),
            decisions,
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
        if let Some(connection) = self.connections.get_mut(worker_id) {
            connection.last_seen = now;
        }
        Ok(match message {
            worker_message::Message::Register(_) => {
                return Err(CoreError::BadMessage {
                    worker_id: worker_id.to_owned(),
                    reason: "`Register` is only the first message".to_owned(),
                });
            }
            worker_message::Message::Heartbeat(_) => Vec::new(),
            worker_message::Message::CodeVersionChanged(changed) => match changed.root {
                Some(root) => self.root_reported(worker_id, &root, now).await?,
                None => Vec::new(),
            },
            worker_message::Message::Started(started) => self
                .dispatcher
                .started(worker_id, &started, now)
                .await?
                .into_iter()
                .collect(),
            worker_message::Message::Rejected(rejected) => {
                self.dispatcher.rejected(worker_id, &rejected, now).await?;
                Vec::new()
            }
            worker_message::Message::Finished(finished) => {
                vec![self.dispatcher.finished(worker_id, &finished, now).await?]
            }
            worker_message::Message::Draining(_) => {
                self.dispatcher.draining(worker_id);
                Vec::new()
            }
            worker_message::Message::Deregister(_) => self.forget(worker_id, now).await?,
        })
    }

    /// A worker's stream ended: it may reconnect (same incarnation, tasks still running)
    /// within the lease timeout, or it is lost at the next tick after that.
    pub fn disconnected(&mut self, worker_id: &str) {
        if let Some(connection) = self.connections.get_mut(worker_id) {
            connection.live = false;
        }
    }

    /// One pass of the master's loop: commands, the scheduler, dispatching, parse jobs past
    /// their timeout, and workers past their lease.
    ///
    /// # Errors
    ///
    /// The first failure; what was done before it stays done, and the next tick goes on.
    pub async fn tick(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, CoreError> {
        let applied = self
            .commander
            .apply_pending(&self.scheduler, &mut self.dispatcher, now)
            .await?;
        let mut outgoing = applied.outgoing;
        self.scheduler.pass(now).await?;
        outgoing.extend(self.dispatcher.step(now).await?);
        let expired = self.discovery.jobs_mut().expire(now);
        outgoing.extend(expired.into_iter().map(assign_parse));
        let lease =
            SignedDuration::try_from(self.settings.lease_timeout).unwrap_or(SignedDuration::MAX);
        let lost: Vec<String> = self
            .connections
            .iter()
            .filter(|(_, c)| now.duration_since(c.last_seen) > lease)
            .map(|(id, _)| id.clone())
            .collect();
        for worker_id in lost {
            outgoing.extend(self.forget(&worker_id, now).await?);
        }
        Ok(outgoing)
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
        let assignment = self.discovery.root_reported(worker_id, key, now).await?;
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

    /// A worker deregistered or is lost: its attempts end `lost`, its parse jobs move on.
    async fn forget(
        &mut self,
        worker_id: &str,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        self.connections.remove(worker_id);
        self.dispatcher.worker_lost(worker_id, now).await?;
        let moved = self.discovery.jobs_mut().worker_lost(worker_id, now);
        Ok(moved.into_iter().map(assign_parse).collect())
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
