//! The worker agent's main loop: one control stream to the master, attempts and parse jobs,
//! heartbeats and code watching (see PROJECT.md, Worker lifecycle and Task assignment).
//!
//! - **Connecting:** the agent waits for each root's first content hash (it settles too),
//!   registers with them and with the attempts it still runs or hasn't had acknowledged, then
//!   follows the master's decisions for those (continue, ack, kill).
//! - **Assignments:** a free slot, an assignment that reads and the task's code on this worker,
//!   or a rejection with the reason; then `Started`, the attempt runs, and `Finished` is sent
//!   until the master acknowledges it.
//! - **Parse jobs** parse a root's `.py` files and upload the result, if the root still has the
//!   job's hash (otherwise a newer hash was or will be reported).
//! - **The connection drops:** attempts keep running, and the agent connects again with
//!   backoff (up to 30 s).
//!
//! The work itself goes through an [`Executor`]: processes in production
//! ([`ProcessExecutor`]), something simpler in tests.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use schedr_model::runner::{ParseRequest, ParsedFile};
use schedr_proto::PROTOCOL_VERSION;
use schedr_proto::convert::parsed_file_to_proto;
use schedr_proto::pb::master_client::MasterClient;
use schedr_proto::pb::{self, master_message, worker_message};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::attempt::{AttemptSpec, NotRun, Plan, WorkerSetup, failed_before_start, finished, plan};
use crate::launch::{AGENT_VERSION, ProcessParseRunner, run_command, run_python};
use crate::parse::parse_files;
use crate::process::ProcessSpec;
use crate::status::StopReason;
use crate::supervise::Limits;
use crate::watch::{Poll, RootWatcher};

#[cfg(test)]
mod tests;

/// The error kind of an attempt whose process couldn't be started.
pub const START_FAILED: &str = "start_failed";

/// The longest wait between attempts to reach the master.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// The agent's configuration.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    /// The worker's ID (stable across restarts).
    pub worker_id: String,
    /// The queues it serves (none: `default`).
    pub queues: Vec<String>,
    /// Its task slots.
    pub slots: u32,
    /// How tasks run here (roots, interpreter, environment, logs).
    pub setup: WorkerSetup,
    /// How often roots are scanned.
    pub poll_interval: Duration,
    /// How long a changed root must stay unchanged before it is hashed.
    pub settle_time: Duration,
}

/// Runs attempts and parse jobs.
pub trait Executor: Clone + Send + Sync + 'static {
    /// Runs a planned attempt until it ends or `stop` says why to stop it; returns its
    /// `Finished`.
    fn run(
        &self,
        spec: AttemptSpec,
        plan: Plan,
        stop: oneshot::Receiver<StopReason>,
    ) -> impl Future<Output = pb::Finished> + Send;

    /// Parses `files` of the root `root` in `dir`.
    fn parse(
        &self,
        root: String,
        dir: PathBuf,
        files: Vec<String>,
    ) -> impl Future<Output = Result<Vec<ParsedFile>, String>> + Send;
}

/// Runs attempts and parse jobs as processes.
#[derive(Debug, Clone)]
pub struct ProcessExecutor {
    /// How tasks run here.
    pub setup: WorkerSetup,
    /// Where runner sockets go.
    pub socket_dir: PathBuf,
    /// How long one file may take to import.
    pub parse_file_timeout: Duration,
}

impl Executor for ProcessExecutor {
    async fn run(
        &self,
        spec: AttemptSpec,
        plan: Plan,
        stop: oneshot::Receiver<StopReason>,
    ) -> pb::Finished {
        let limits = Limits {
            timeout: spec.timeout(),
            grace: spec.cancel_grace,
        };
        let stop = async move {
            match stop.await {
                Ok(reason) => reason,
                Err(_) => std::future::pending().await,
            }
        };
        let _ = tokio::fs::create_dir_all(&self.setup.log_dir).await;
        let ran = match plan {
            Plan::Python { process, request } => {
                run_python(&process, *request, &self.socket_dir, limits, stop).await
            }
            Plan::Command(process) => run_command(&process, limits, stop).await,
        };
        match ran {
            Ok(ran) => finished(&spec.attempt_id, &ran),
            Err(error) => failed_before_start(&spec.attempt_id, START_FAILED, &error.to_string()),
        }
    }

    async fn parse(
        &self,
        root: String,
        dir: PathBuf,
        files: Vec<String>,
    ) -> Result<Vec<ParsedFile>, String> {
        let _ = tokio::fs::create_dir_all(&self.setup.log_dir).await;
        let mut runner = ProcessParseRunner {
            process: ProcessSpec {
                program: self.setup.python.clone(),
                args: vec![
                    "-m".to_owned(),
                    "schedr.runner".to_owned(),
                    "parse".to_owned(),
                ],
                env: self
                    .setup
                    .agent_env
                    .iter()
                    .filter(|(name, _)| crate::env::is_allowed(name))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                working_dir: dir.clone(),
                log_file: self.setup.log_dir.join(format!("parse-{root}.log")),
                max_output_size: 64 * 1024,
            },
            socket_dir: self.socket_dir.clone(),
        };
        let request = ParseRequest {
            root,
            root_dir: dir.to_string_lossy().into_owned(),
            files,
            sys_path: self.setup.sys_path(),
        };
        parse_files(&mut runner, request, self.parse_file_timeout)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Why a session with the master ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEnd {
    /// The agent is stopping.
    Stopped,
    /// The connection failed or the master ended it; connect again.
    Lost(String),
}

/// Something finished in the background.
enum Done {
    Attempt(pb::Finished),
    Parse {
        job_id: String,
        root: pb::RootHash,
        files: Result<Vec<ParsedFile>, String>,
    },
}

/// The worker agent.
#[derive(Debug)]
pub struct Agent<E> {
    settings: AgentSettings,
    executor: E,
    incarnation: String,
    watchers: BTreeMap<String, RootWatcher>,
    running: BTreeMap<String, oneshot::Sender<StopReason>>,
    unacked: BTreeMap<String, pb::Finished>,
    done_tx: mpsc::UnboundedSender<Done>,
    done_rx: mpsc::UnboundedReceiver<Done>,
    heartbeat: Duration,
}

impl<E: Executor> Agent<E> {
    /// An agent with a new incarnation ID.
    #[must_use]
    pub fn new(settings: AgentSettings, executor: E, incarnation: String) -> Self {
        let watchers = settings
            .setup
            .roots
            .iter()
            .map(|(name, dir)| (name.clone(), RootWatcher::new(dir, settings.settle_time)))
            .collect();
        let (done_tx, done_rx) = mpsc::unbounded_channel();
        Self {
            settings,
            executor,
            incarnation,
            watchers,
            running: BTreeMap::new(),
            unacked: BTreeMap::new(),
            done_tx,
            done_rx,
            heartbeat: Duration::from_secs(10),
        }
    }

    /// The attempts running now.
    #[must_use]
    pub fn running(&self) -> Vec<String> {
        self.running.keys().cloned().collect()
    }

    /// Runs until `stop` completes: connects with `connect`, and again with backoff whenever
    /// the connection drops.
    pub async fn run<C, F>(&mut self, connect: C, stop: impl Future<Output = ()>)
    where
        C: Fn() -> F,
        F: Future<Output = Result<MasterClient<Channel>, String>>,
    {
        tokio::pin!(stop);
        let mut backoff = Duration::from_millis(200);
        loop {
            let session = async {
                match connect().await {
                    Ok(client) => self.session(client).await,
                    Err(error) => SessionEnd::Lost(error),
                }
            };
            tokio::select! {
                () = &mut stop => return,
                end = session => match end {
                    SessionEnd::Stopped => return,
                    SessionEnd::Lost(reason) => {
                        tracing::warn!(reason, "master connection lost; connecting again");
                    }
                },
            }
            tokio::select! {
                () = &mut stop => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// The roots' content hashes, waiting for each to settle the first time.
    async fn hashes(&mut self) -> Vec<pb::RootHash> {
        let mut roots = Vec::new();
        for (name, watcher) in &mut self.watchers {
            loop {
                if let Some(hash) = watcher.hash() {
                    roots.push(root_hash(name, hash));
                    break;
                }
                match watcher.poll(Instant::now()) {
                    Ok(Poll::Settling(wait)) => tokio::time::sleep(wait).await,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(root = name.as_str(), %error, "can't scan DAG root");
                        tokio::time::sleep(self.settings.poll_interval).await;
                    }
                }
            }
        }
        roots
    }

    /// One session with the master: register, then handle messages until the connection ends.
    pub async fn session(&mut self, mut client: MasterClient<Channel>) -> SessionEnd {
        let roots = self.hashes().await;
        let (to_master, outbound) = mpsc::channel::<pb::WorkerMessage>(1024);
        let journal = self.journal();
        let register = pb::Register {
            worker_id: self.settings.worker_id.clone(),
            incarnation_id: self.incarnation.clone(),
            protocol: Some(PROTOCOL_VERSION),
            schedr_version: AGENT_VERSION.to_owned(),
            queues: self.settings.queues.clone(),
            slots: self.settings.slots,
            log_endpoint: String::new(),
            roots,
            secret_storages: Vec::new(),
            journal,
        };
        if to_master
            .send(wrap(worker_message::Message::Register(register)))
            .await
            .is_err()
        {
            return SessionEnd::Lost("the outbound stream closed".to_owned());
        }
        let mut inbound = match client.control(ReceiverStream::new(outbound)).await {
            Ok(response) => response.into_inner(),
            Err(status) => return SessionEnd::Lost(status.to_string()),
        };
        let mut heartbeat = tokio::time::interval(self.heartbeat);
        let mut watch = tokio::time::interval(self.settings.poll_interval);
        loop {
            tokio::select! {
                message = inbound.message() => match message {
                    Ok(Some(pb::MasterMessage { message: Some(message) })) => {
                        self.on_master(message, &to_master, &mut client, &mut heartbeat).await;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => return SessionEnd::Lost("the master ended the stream".to_owned()),
                    Err(status) => return SessionEnd::Lost(status.to_string()),
                },
                Some(done) = self.done_rx.recv() => self.on_done(done, &to_master, &mut client).await,
                _ = heartbeat.tick() => {
                    let message = worker_message::Message::Heartbeat(pb::Heartbeat {
                        free_slots: self.free_slots(),
                        running_attempt_ids: self.running(),
                    });
                    let _ = to_master.send(wrap(message)).await;
                }
                _ = watch.tick() => self.watch(&to_master).await,
            }
        }
    }

    async fn on_master(
        &mut self,
        message: master_message::Message,
        to_master: &mpsc::Sender<pb::WorkerMessage>,
        client: &mut MasterClient<Channel>,
        heartbeat: &mut tokio::time::Interval,
    ) {
        match message {
            master_message::Message::Registered(registered) => {
                if let Some(timing) = registered.timing.filter(|t| t.heartbeat_interval_ms > 0) {
                    self.heartbeat = Duration::from_millis(timing.heartbeat_interval_ms);
                    *heartbeat = tokio::time::interval(self.heartbeat);
                }
                for decision in registered.decisions {
                    self.decide(&decision, to_master).await;
                }
            }
            master_message::Message::AssignTask(assign) => {
                self.assign(assign, to_master, client).await;
            }
            master_message::Message::AssignParse(job) => self.parse(job),
            master_message::Message::CancelAttempt(cancel) => {
                if let Some(stop) = self.running.remove(&cancel.attempt_id) {
                    let _ = stop.send(StopReason::Cancelled);
                }
            }
            master_message::Message::FinishedAck(ack) => {
                self.unacked.remove(&ack.attempt_id);
            }
        }
    }

    async fn decide(
        &mut self,
        decision: &pb::JournalDecision,
        to_master: &mpsc::Sender<pb::WorkerMessage>,
    ) {
        let id = &decision.attempt_id;
        match pb::AttemptDecision::try_from(decision.decision) {
            Ok(pb::AttemptDecision::Ack) => {
                self.unacked.remove(id);
            }
            Ok(pb::AttemptDecision::Kill) => {
                self.unacked.remove(id);
                if let Some(stop) = self.running.remove(id) {
                    let _ = stop.send(StopReason::Cancelled);
                }
            }
            _ => {
                if let Some(finished) = self.unacked.get(id) {
                    let message = worker_message::Message::Finished(finished.clone());
                    let _ = to_master.send(wrap(message)).await;
                }
            }
        }
    }

    async fn assign(
        &mut self,
        assign: pb::AssignTask,
        to_master: &mpsc::Sender<pb::WorkerMessage>,
        client: &mut MasterClient<Channel>,
    ) {
        let reject = |reason: pb::RejectReason, detail: String| {
            wrap(worker_message::Message::Rejected(pb::Rejected {
                attempt_id: assign.attempt_id.clone(),
                reason: reason.into(),
                detail,
            }))
        };
        if self.free_slots() == 0 {
            let _ = to_master
                .send(reject(
                    pb::RejectReason::NoFreeSlot,
                    "no free slot".to_owned(),
                ))
                .await;
            return;
        }
        let external = if assign.inputs_external {
            match fetch_inputs(client, &assign.attempt_id).await {
                Ok(bytes) => Some(bytes),
                Err(error) => {
                    let _ = to_master
                        .send(reject(pb::RejectReason::InvalidSpecification, error))
                        .await;
                    return;
                }
            }
        } else {
            None
        };
        let spec = match AttemptSpec::from_assign(&assign, external.as_deref()) {
            Ok(spec) => spec,
            Err(error) => {
                let _ = to_master
                    .send(reject(
                        pb::RejectReason::InvalidSpecification,
                        error.to_string(),
                    ))
                    .await;
                return;
            }
        };
        let planned = plan(&spec, &self.settings.setup);
        let started = wrap(worker_message::Message::Started(pb::Started {
            attempt_id: spec.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        }));
        match planned {
            Err(NotRun::Rejected { reason, detail }) => {
                let _ = to_master.send(reject(reason, detail)).await;
            }
            Err(NotRun::Failed { kind, message }) => {
                let _ = to_master.send(started).await;
                let failed = failed_before_start(&spec.attempt_id, &kind, &message);
                let _ = self.done_tx.send(Done::Attempt(failed));
            }
            Ok(plan) => {
                let (stop, stopped) = oneshot::channel();
                self.running.insert(spec.attempt_id.clone(), stop);
                let _ = to_master.send(started).await;
                let executor = self.executor.clone();
                let done = self.done_tx.clone();
                tokio::spawn(async move {
                    let finished = executor.run(spec, plan, stopped).await;
                    let _ = done.send(Done::Attempt(finished));
                });
            }
        }
    }

    fn parse(&self, job: pb::AssignParse) {
        let Some(root) = job.root else {
            return;
        };
        let Some(watcher) = self.watchers.get(&root.root) else {
            tracing::warn!(
                root = root.root.as_str(),
                "parse job for a root this worker doesn't have"
            );
            return;
        };
        if watcher.hash() != Some(root.content_hash.as_str()) {
            // The root changed since: its new hash was (or will be) reported.
            return;
        }
        let files = watcher
            .snapshot()
            .map(crate::scan::Snapshot::python_files)
            .unwrap_or_default();
        let dir = watcher.dir().to_path_buf();
        let executor = self.executor.clone();
        let done = self.done_tx.clone();
        tokio::spawn(async move {
            let parsed = executor.parse(root.root.clone(), dir, files).await;
            let _ = done.send(Done::Parse {
                job_id: job.parse_job_id,
                root,
                files: parsed,
            });
        });
    }

    async fn on_done(
        &mut self,
        done: Done,
        to_master: &mpsc::Sender<pb::WorkerMessage>,
        client: &mut MasterClient<Channel>,
    ) {
        match done {
            Done::Attempt(finished) => {
                self.running.remove(&finished.attempt_id);
                self.unacked
                    .insert(finished.attempt_id.clone(), finished.clone());
                let _ = to_master
                    .send(wrap(worker_message::Message::Finished(finished)))
                    .await;
            }
            Done::Parse {
                job_id,
                root,
                files,
            } => match files {
                Ok(files) => {
                    let result = pb::ParseResult {
                        parse_job_id: job_id,
                        worker_id: self.settings.worker_id.clone(),
                        root: Some(root),
                        schedr_version: AGENT_VERSION.to_owned(),
                        files: files.iter().map(parsed_file_to_proto).collect(),
                    };
                    if let Err(status) = client.upload_parse_result(result).await {
                        tracing::error!(%status, "parse result not uploaded");
                    }
                }
                Err(error) => tracing::error!(root = root.root.as_str(), error, "parse job failed"),
            },
        }
    }

    async fn watch(&mut self, to_master: &mpsc::Sender<pb::WorkerMessage>) {
        for (name, watcher) in &mut self.watchers {
            match watcher.poll(Instant::now()) {
                Ok(Poll::Changed { hash, .. }) => {
                    let changed =
                        worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
                            root: Some(root_hash(name, &hash)),
                        });
                    let _ = to_master.send(wrap(changed)).await;
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(root = name.as_str(), %error, "can't scan DAG root"),
            }
        }
    }

    fn journal(&self) -> Vec<pb::JournalEntry> {
        let running = self.running.keys().map(|id| pb::JournalEntry {
            attempt_id: id.clone(),
            state: pb::JournalState::Running.into(),
        });
        let finished = self.unacked.keys().map(|id| pb::JournalEntry {
            attempt_id: id.clone(),
            state: pb::JournalState::Finished.into(),
        });
        running.chain(finished).collect()
    }

    fn free_slots(&self) -> u32 {
        self.settings
            .slots
            .saturating_sub(u32::try_from(self.running.len()).unwrap_or(u32::MAX))
    }
}

async fn fetch_inputs(
    client: &mut MasterClient<Channel>,
    attempt_id: &str,
) -> Result<Vec<u8>, String> {
    let mut stream = client
        .fetch_inputs(pb::FetchInputsRequest {
            attempt_id: attempt_id.to_owned(),
        })
        .await
        .map_err(|s| s.to_string())?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.message().await.map_err(|s| s.to_string())? {
        bytes.extend(chunk.data);
    }
    Ok(bytes)
}

fn root_hash(name: &str, hash: &str) -> pb::RootHash {
    pb::RootHash {
        kind: pb::RootKind::Dag.into(),
        root: name.to_owned(),
        content_hash: hash.to_owned(),
    }
}

const fn wrap(message: worker_message::Message) -> pb::WorkerMessage {
    pb::WorkerMessage {
        message: Some(message),
    }
}
