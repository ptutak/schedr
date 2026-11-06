//! The watcher hosts: a worker's wait steps, as coroutines in one long-lived process per code
//! set (decision 320; see PROJECT.md, Sensors and triggers).
//!
//! A host is started for the first wait step of an interpreter and set of root copies, and
//! stopped when its last one ends, so new code gets a new host while the old one finishes. All
//! a host's watchers share its socket: one task reads it and passes each message to its
//! attempt, one writes, so no attempt being dropped can leave half a message in it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, Instant};

use schedr_model::runner::{
    AgentMessage, OutputStream, RunnerMessage, RunnerMode, TaskRequest, WatchersRequest,
};
use schedr_model::task::{SensorMode, TaskKind};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{Mutex, Notify, mpsc, watch};

use crate::attempt::{Ran, WorkerSetup};
use crate::channel::{read_message, write_message};
use crate::handles::{AttemptHandles, HandleError};
use crate::launch::{
    LaunchError, PythonTask, connection_answer, secret_answer, start_runner, unless_stopped,
};
use crate::mask::{Mask, MaskSet, MaskStream};
use crate::process::{OsProcess, ProcessSpec};
use crate::secrets::AttemptSecrets;
use crate::status::{StopReason, Termination};
use crate::supervise::{Limits, Process, Signal, Stops, Supervised};
use crate::usage::Ended;

/// The exit code a failed watcher stands for: as a task's process that reports a failure.
const FAILED_EXIT_CODE: i32 = 1;

/// The signal number a cancelled watcher stands for: as a task's process that took it.
const TERMINATED: i32 = 15;

/// The signal number of a host that had to be killed.
const KILLED: i32 = 9;

/// The hosts of a worker, by code set (cheap to clone; clones share them).
#[derive(Debug, Clone, Default)]
pub struct WatcherHosts {
    hosts: Arc<Mutex<BTreeMap<Vec<String>, Arc<Host>>>>,
    next_watcher: Arc<AtomicU64>,
    /// The code sets hosts run, read by the agent's copy collection, which can't wait.
    code_sets: Arc<StdMutex<BTreeSet<Vec<String>>>>,
    /// The copy of each DAG root the worker runs now.
    code: Arc<StdMutex<Option<watch::Sender<Copies>>>>,
}

type Copies = BTreeMap<String, PathBuf>;

type Routes = Arc<StdMutex<HashMap<u64, mpsc::Sender<RunnerMessage>>>>;

/// The messages of a watcher waiting for its attempt: past them its host's socket waits too, so a
/// wait step printing faster than its log is written slows down, and the agent's memory holds at
/// most this many (each up to `_LOG_CHUNK`, 4 MiB, for text).
const ROUTE_MESSAGES: usize = 16;

/// A running host.
#[derive(Debug)]
struct Host {
    pid: i32,
    outbox: mpsc::UnboundedSender<AgentMessage>,
    routes: Routes,
    stop: Arc<Notify>,
    ended: watch::Receiver<Option<Termination>>,
    /// What its own log masks: every value of every watcher it has had (decision 334: output
    /// of a thread or C code can't be told apart, and may come after its watcher ended).
    mask: Arc<MaskSet>,
}

impl Host {
    /// Starts one, ready for its watchers.
    async fn start(setup: &WorkerSetup, sys_path: &[String]) -> Result<Self, LaunchError> {
        // Each value was accepted by the attempt's own set, under its `min_mask_length`.
        let mask = Arc::new(MaskSet::new(0));
        let spec = host_spec(setup, sys_path, Mask(Some(Arc::clone(&mask))));
        let runner = start_runner(&spec, RunnerMode::Watchers, |_| {}).await?;
        let pid = runner.process.id();
        let (mut reader, mut writer) = runner.stream.into_split();
        let (outbox, mut outgoing) = mpsc::unbounded_channel::<AgentMessage>();
        let _ = outbox.send(AgentMessage::Watchers(WatchersRequest {
            sys_path: sys_path.to_vec(),
        }));
        let stop = Arc::new(Notify::new());
        let routes = Routes::default();
        let writing = tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                if write_message(&mut writer, &message).await.is_err() {
                    return;
                }
            }
        });
        let reading = {
            let routes = Arc::clone(&routes);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                loop {
                    match read_message::<_, RunnerMessage>(&mut reader).await {
                        Ok(RunnerMessage::WatcherMessage { watcher, message }) => {
                            let route = routes
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .get(&watcher)
                                .cloned();
                            if let Some(route) = route {
                                let _ = route.send(*message).await;
                            }
                        }
                        Ok(other) => {
                            tracing::warn!(?other, "a watcher host sent something unexpected");
                        }
                        Err(error) => {
                            tracing::warn!(%error, pid, "a watcher host's channel ended");
                            stop.notify_one();
                            return;
                        }
                    }
                }
            })
        };
        let (ended_sender, ended) = watch::channel(None);
        let watch_over = HostEnd {
            stopping: Arc::clone(&stop),
            routes: Arc::clone(&routes),
            ended: ended_sender,
            drain: spec.output_drain,
        };
        tokio::spawn(watch_over.run(runner.process, reading, writing));
        Ok(Self {
            pid,
            outbox,
            routes,
            stop,
            ended,
            mask,
        })
    }

    fn alive(&self) -> bool {
        self.ended.borrow().is_none()
    }
}

/// Waits for a host's end, or stops it, and tells its watchers.
struct HostEnd {
    stopping: Arc<Notify>,
    routes: Routes,
    ended: watch::Sender<Option<Termination>>,
    drain: Duration,
}

impl HostEnd {
    async fn run(
        self,
        mut process: OsProcess,
        reading: tokio::task::JoinHandle<()>,
        writing: tokio::task::JoinHandle<()>,
    ) {
        let termination = tokio::select! {
            biased;
            () = self.stopping.notified() => {
                let _ = process.signal(Signal::Kill);
                process.wait().await
            }
            termination = process.wait() => termination,
        };
        // What it started goes with it.
        let _ = process.signal(Signal::Kill);
        let _ = tokio::time::timeout(self.drain, process.output()).await;
        reading.abort();
        writing.abort();
        let _ = self
            .ended
            .send(Some(termination.unwrap_or(Termination::Unknown)));
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// The host's process: its environment and folder are no run's, so they are the worker's; what
/// threads and C code print goes to its own log.
fn host_spec(setup: &WorkerSetup, sys_path: &[String], mask: Mask) -> ProcessSpec {
    let mut hasher = DefaultHasher::new();
    sys_path.hash(&mut hasher);
    ProcessSpec {
        program: setup.python.clone(),
        args: vec![
            // Not the working directory first: a root's file can't shadow what the runner imports (B178).
            "-P".to_owned(),
            // A thread a wait step starts stays its: what it prints goes to its log, and it can't
            // start a process either (decision 336).
            "-X".to_owned(),
            "thread_inherit_context=1".to_owned(),
            "-m".to_owned(),
            "schedr.runner".to_owned(),
            "watchers".to_owned(),
        ],
        env: setup
            .agent_env
            .iter()
            .filter(|(name, _)| crate::env::is_allowed(name) || setup.pass_env.contains(name))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        working_dir: setup.log_dir.clone(),
        log_file: setup
            .log_dir
            .join(format!("watchers-{:016x}.log", hasher.finish())),
        max_output_size: 64 * 1024,
        mask,
        start_timeout: setup.timeouts.runner_start,
        output_drain: setup.timeouts.output_drain,
    }
}

/// A watcher's place in its host, given up when dropped (a dropped attempt included).
struct Entered {
    hosts: WatcherHosts,
    host: Arc<Host>,
    watcher: u64,
    messages: mpsc::Receiver<RunnerMessage>,
    /// How long a cancel of it may take when no attempt waits for it any more.
    grace: Duration,
    left: bool,
}

impl WatcherHosts {
    /// A place in the code set's host, started if it has none.
    async fn enter(
        &self,
        setup: &WorkerSetup,
        sys_path: &[String],
        grace: Duration,
    ) -> Result<Entered, LaunchError> {
        let mut hosts = self.hosts.lock().await;
        let running = hosts.get(sys_path).filter(|host| host.alive()).cloned();
        let host = if let Some(host) = running {
            host
        } else {
            // Held from before it starts: nothing removes its copies under it.
            self.code_set(sys_path, true);
            let host = match Host::start(setup, sys_path).await {
                Ok(host) => Arc::new(host),
                Err(error) => {
                    self.code_set(sys_path, false);
                    return Err(error);
                }
            };
            hosts.insert(sys_path.to_vec(), Arc::clone(&host));
            host
        };
        let watcher = self.next_watcher.fetch_add(1, Ordering::Relaxed);
        let (route, messages) = mpsc::channel(ROUTE_MESSAGES);
        host.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(watcher, route);
        Ok(Entered {
            hosts: self.clone(),
            host,
            watcher,
            messages,
            grace,
            left: false,
        })
    }

    /// Takes a watcher out; a host left without any stops.
    async fn leave(&self, host: &Arc<Host>, watcher: u64) {
        let mut hosts = self.hosts.lock().await;
        let empty = {
            let mut routes = host.routes.lock().unwrap_or_else(PoisonError::into_inner);
            routes.remove(&watcher);
            routes.is_empty()
        };
        if empty {
            host.stop.notify_one();
            let mut gone = None;
            hosts.retain(|key, known| {
                let keep = !Arc::ptr_eq(known, host);
                if !keep {
                    gone = Some(key.clone());
                }
                keep
            });
            if let Some(key) = gone {
                self.code_set(&key, false);
            }
        }
    }

    fn code_set(&self, sys_path: &[String], running: bool) {
        let mut sets = self
            .code_sets
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if running {
            sets.insert(sys_path.to_vec());
        } else {
            sets.remove(sys_path);
        }
    }

    /// The folders of the code copies hosts run: they outlive the attempt that started them,
    /// when a poke moved.
    #[must_use]
    pub fn code_in_use(&self) -> Vec<String> {
        let sets = self
            .code_sets
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        sets.iter()
            .flatten()
            .filter_map(|dir| {
                let name = std::path::Path::new(dir).file_name()?;
                Some(name.to_string_lossy().into_owned())
            })
            .collect()
    }

    /// The worker's newest copy of each DAG root: running pokes move to it (decision 335).
    pub fn code_changed(&self, copies: &Copies) {
        self.code_channel().send_if_modified(|now| {
            let changed = now != copies;
            if changed {
                now.clone_from(copies);
            }
            changed
        });
    }

    fn code_channel(&self) -> watch::Sender<Copies> {
        self.code
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| watch::channel(Copies::new()).0)
            .clone()
    }

    /// How many hosts run, for tests and metrics.
    pub async fn running(&self) -> usize {
        self.hosts.lock().await.len()
    }
}

impl Entered {
    async fn leave(mut self) {
        self.left = true;
        self.hosts.leave(&self.host, self.watcher).await;
    }
}

impl Drop for Entered {
    fn drop(&mut self) {
        if self.left {
            return;
        }
        // No attempt waits for it any more (it ended first, or its agent is going away): it
        // is still cancelled and held to its grace, as Python can't cancel what doesn't yield.
        let _ = self.host.outbox.send(AgentMessage::CancelWatcher {
            watcher: self.watcher,
        });
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let hosts = self.hosts.clone();
        let host = Arc::clone(&self.host);
        let watcher = self.watcher;
        let grace = self.grace;
        let mut messages = std::mem::replace(&mut self.messages, mpsc::channel(1).1);
        runtime.spawn(async move {
            let ended = async {
                while let Some(message) = messages.recv().await {
                    if ends_a_watcher(&message) {
                        return;
                    }
                }
            };
            if tokio::time::timeout(grace, ended).await.is_err() {
                host.stop.notify_one();
            }
            hosts.leave(&host, watcher).await;
        });
    }
}

impl Entered {
    /// Its grace, for its host to hold what it left running to.
    fn cancel_grace_ms(&self) -> u64 {
        u64::try_from(self.grace.as_millis()).unwrap_or(u64::MAX)
    }
}

/// Whether a watcher says nothing after this.
const fn ends_a_watcher(message: &RunnerMessage) -> bool {
    matches!(
        message,
        RunnerMessage::TaskSucceeded { .. }
            | RunnerMessage::TaskFailed { .. }
            | RunnerMessage::WatcherCancelled
            | RunnerMessage::WatcherMoved
    )
}

/// What a watcher printed, masked, in its attempt's log.
struct AttemptLog<'a> {
    mask: &'a Mask,
    file: tokio::fs::File,
    stdout: Option<MaskStream<'a>>,
    stderr: Option<MaskStream<'a>>,
}

impl<'a> AttemptLog<'a> {
    async fn open(spec: &'a ProcessSpec) -> std::io::Result<Self> {
        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).append(true);
        let file = crate::process::open_regular(options, &spec.log_file).await?;
        let stream = || spec.mask.0.as_deref().map(MaskSet::stream);
        Ok(Self {
            mask: &spec.mask,
            file,
            stdout: stream(),
            stderr: stream(),
        })
    }

    async fn write(&mut self, stream: OutputStream, text: &str) {
        let masking = match stream {
            OutputStream::Stdout => &mut self.stdout,
            OutputStream::Stderr => &mut self.stderr,
        };
        let masked = match masking {
            Some(masking) => masking.push(text.as_bytes()),
            None => text.as_bytes().to_vec(),
        };
        let _ = self.file.write_all(&masked).await;
    }

    /// A line of the agent's, masked: it may quote what new code raised.
    async fn note(&mut self, text: &str) {
        let masked = match &self.mask.0 {
            Some(mask) => mask.mask(text.as_bytes()),
            None => text.as_bytes().to_vec(),
        };
        let _ = self.file.write_all(&masked).await;
    }

    async fn finish(mut self) {
        for masking in [&mut self.stdout, &mut self.stderr].into_iter().flatten() {
            let rest = masking.finish();
            let _ = self.file.write_all(&rest).await;
        }
        let _ = self.file.flush().await;
    }
}

/// Answers what watchers ask. Answers wait on a secret storage, so none holds up the rest, a
/// cancel's end above all; they stop when it is dropped.
struct Answers {
    secrets: Option<Arc<AttemptSecrets>>,
    handles: Option<Arc<AttemptHandles>>,
    pending: tokio::task::JoinSet<()>,
}

impl Answers {
    /// Takes what a watcher sent; gives back what isn't for it, which is the caller's.
    async fn serve(
        &mut self,
        message: RunnerMessage,
        host: &Arc<Host>,
        watcher: u64,
        log: &mut AttemptLog<'_>,
    ) -> Option<RunnerMessage> {
        if let RunnerMessage::WatcherLog { stream, text } = &message {
            log.write(*stream, text).await;
            return None;
        }
        if !matches!(
            message,
            RunnerMessage::SecretGet { .. }
                | RunnerMessage::HandleAdd { .. }
                | RunnerMessage::ConnectionGet { .. }
        ) {
            return Some(message);
        }
        let secrets = self.secrets.clone();
        let handles = self.handles.clone();
        let host = Arc::clone(host);
        self.pending.spawn(async move {
            let answer = answer_to(message, secrets.as_deref(), handles.as_deref()).await;
            let _ = host.outbox.send(AgentMessage::ToWatcher {
                watcher,
                message: Box::new(answer),
            });
        });
        None
    }
}

async fn answer_to(
    message: RunnerMessage,
    secrets: Option<&AttemptSecrets>,
    handles: Option<&AttemptHandles>,
) -> AgentMessage {
    match message {
        RunnerMessage::SecretGet { secret } => secret_answer(secrets, &secret).await,
        RunnerMessage::ConnectionGet { name } => connection_answer(secrets, &name).await,
        RunnerMessage::HandleAdd { handle } => {
            let added = match handles {
                Some(handles) => handles.add(handle).await,
                None => Err(HandleError::NoCleanup),
            };
            added.map_or_else(
                |error| AgentMessage::HandleRefused {
                    message: error.to_string(),
                },
                |()| AgentMessage::HandleAdded,
            )
        }
        other => AgentMessage::Refused {
            reason: format!("not a question: {}", crate::channel::describe(&other)),
        },
    }
}

/// A poke's move to new code (decision 335), under way.
enum Stage {
    /// Its new host is being found or started: nothing waits for that.
    Entering(tokio::task::JoinHandle<Result<Entered, LaunchError>>),
    /// Its place there, making no call until told; `stopping`: it could start, and the old
    /// one was told to stop.
    Entered { to: Entered, stopping: bool },
}

struct Moving {
    stage: Stage,
    /// What it runs there.
    request: TaskRequest,
    /// The new code's hash.
    code_hash: Option<String>,
}

/// A poke moving to new code when there is some; what never moves has none of these.
struct Poke<'a> {
    setup: &'a WorkerSetup,
    hosts: &'a WatcherHosts,
    mask: Mask,
    grace: Duration,
    /// As it runs now.
    request: TaskRequest,
    code: watch::Receiver<Copies>,
    /// The code last tried: new code that can't start it isn't tried again.
    tried: Vec<String>,
    max_wait: Option<Duration>,
    started: Instant,
    moving: Option<Moving>,
}

impl Poke<'_> {
    /// Starts a move, if there is new code it hasn't tried.
    fn begin_move(&mut self) {
        if self.moving.is_some() {
            return;
        }
        let copies = self.code.borrow_and_update().clone();
        let Some(root_dir) = copies.get(&self.request.dag_file.root) else {
            return;
        };
        let sys_path = self.setup.sys_path_at(&copies);
        if sys_path == self.tried {
            return;
        }
        self.tried.clone_from(&sys_path);
        // A copy's folder is its hash (`crate::scan`).
        let code_hash = root_dir
            .file_name()
            .map(|name| name.to_string_lossy().replacen('-', ":", 1));
        let mut request = self.request.clone();
        request.dag_file.root_dir = root_dir.to_string_lossy().into_owned();
        request.sys_path.clone_from(&sys_path);
        let (hosts, setup, grace) = (self.hosts.clone(), self.setup.clone(), self.grace);
        let entering = tokio::spawn(async move { hosts.enter(&setup, &sys_path, grace).await });
        self.moving = Some(Moving {
            stage: Stage::Entering(entering),
            request,
            code_hash,
        });
    }

    /// Its place in the new code's host came, or didn't: it starts there, not calling yet.
    async fn entered_new_host(
        &mut self,
        result: Result<Result<Entered, LaunchError>, tokio::task::JoinError>,
        log: &mut AttemptLog<'_>,
    ) {
        let Some(moving) = self.moving.as_mut() else {
            return;
        };
        match result {
            Ok(Ok(to)) => {
                // Before it can print anything there.
                if let Some(mask) = &self.mask.0 {
                    mask.also_in(Arc::clone(&to.host.mask));
                }
                let _ = to.host.outbox.send(AgentMessage::StartWatcher {
                    watcher: to.watcher,
                    task: Box::new(moving.request.clone()),
                    cancel_grace_ms: to.cancel_grace_ms(),
                    moved_in: true,
                });
                moving.stage = Stage::Entered {
                    to,
                    stopping: false,
                };
            }
            Ok(Err(error)) => self.stays(&error.to_string(), log).await,
            Err(error) => self.stays(&error.to_string(), log).await,
        }
    }

    /// New code can't start it: it stays on its code.
    async fn stays(&mut self, why: &str, log: &mut AttemptLog<'_>) {
        log.note(&format!(
            "new code can't start this wait step, it stays on its code: {why}\n"
        ))
        .await;
        if let Some(Moving {
            stage: Stage::Entered { to, .. },
            ..
        }) = self.moving.take()
        {
            to.leave().await;
        }
    }

    /// A message of the new code's host, about a move under way.
    async fn on_new_code_message(
        &mut self,
        message: Option<RunnerMessage>,
        old: &Entered,
        answers: &mut Answers,
        log: &mut AttemptLog<'_>,
    ) {
        let Some(Moving {
            stage: Stage::Entered { to, stopping },
            ..
        }) = self.moving.as_mut()
        else {
            return;
        };
        let why = match message {
            Some(RunnerMessage::TaskImported) if !*stopping => {
                *stopping = true;
                let _ = old.host.outbox.send(AgentMessage::MoveWatcher {
                    watcher: old.watcher,
                });
                return;
            }
            Some(RunnerMessage::TaskFailed { error, .. }) => {
                format!("{}: {}", error.exception, error.message)
            }
            None => "its host ended".to_owned(),
            Some(other) => {
                let (host, watcher) = (Arc::clone(&to.host), to.watcher);
                let _ = answers.serve(other, &host, watcher, log).await;
                return;
            }
        };
        self.stays(&why, log).await;
    }

    fn left_of_max_wait(&self) -> Option<u64> {
        self.max_wait.map(|wait| {
            u64::try_from(wait.saturating_sub(self.started.elapsed()).as_millis())
                .unwrap_or(u64::MAX)
        })
    }
}

/// Where a watcher is now, for what stops it from outside.
struct Place {
    host: StdMutex<Arc<Host>>,
    watcher: AtomicU64,
    /// It is being cancelled: it doesn't move any more.
    cancelling: AtomicBool,
    /// The hash of the code it moved to, if it moved.
    moved_to: StdMutex<Option<String>>,
}

impl Place {
    fn host(&self) -> Arc<Host> {
        Arc::clone(&self.host.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn moved_to(&self) -> Option<String> {
        self.moved_to
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn cancel(&self) {
        self.cancelling.store(true, Ordering::SeqCst);
        let _ = self.host().outbox.send(AgentMessage::CancelWatcher {
            watcher: self.watcher.load(Ordering::SeqCst),
        });
    }
}

/// Serves a watcher until its result, as a task's runner is served, moving a poke to new code
/// on the way; `None` if its host ended. Leaves its host on any end but being dropped.
async fn conversation(
    mut entered: Entered,
    mut poke: Option<Poke<'_>>,
    place: &Place,
    mut answers: Answers,
    log: &mut AttemptLog<'_>,
) -> Option<RunnerMessage> {
    let ended = loop {
        let still = !place.cancelling.load(Ordering::SeqCst);
        if let (Some(poke), true) = (&mut poke, still) {
            poke.begin_move();
        }
        let (code, moving) = match &mut poke {
            Some(poke) => (Some(&mut poke.code), poke.moving.as_mut()),
            None => (None, None),
        };
        let new_code = async {
            match code {
                Some(code) if still => {
                    if code.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
                _ => std::future::pending().await,
            }
        };
        let from_new = async {
            match moving.map(|moving| &mut moving.stage) {
                Some(Stage::Entering(entering)) => Happened::Entered(entering.await),
                Some(Stage::Entered { to, .. }) => Happened::New(to.messages.recv().await),
                None => std::future::pending().await,
            }
        };
        let happened = tokio::select! {
            biased;
            message = entered.messages.recv() => Happened::Old(message),
            happened = from_new => happened,
            () = new_code => Happened::NewCode,
            // A finished answer needs no one's attention.
            Some(_) = answers.pending.join_next() => Happened::NewCode,
        };
        match happened {
            Happened::NewCode | Happened::Old(Some(RunnerMessage::TaskImported)) => {}
            Happened::Old(None) => break None,
            Happened::Old(Some(RunnerMessage::WatcherMoved)) => {
                if place.cancelling.load(Ordering::SeqCst) {
                    break Some(RunnerMessage::WatcherCancelled);
                }
                let Some(poke) = poke.as_mut() else {
                    break None;
                };
                match moved(poke, log).await {
                    Ok(to) => {
                        *place.host.lock().unwrap_or_else(PoisonError::into_inner) =
                            Arc::clone(&to.0.host);
                        place.watcher.store(to.0.watcher, Ordering::SeqCst);
                        if to.1.is_some() {
                            *place
                                .moved_to
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner) = to.1;
                        }
                        let old = std::mem::replace(&mut entered, to.0);
                        old.leave().await;
                    }
                    Err(why) => {
                        log.note(&format!("this wait step can't go on: {why}\n"))
                            .await;
                        break None;
                    }
                }
            }
            Happened::Old(Some(message)) => {
                let host = Arc::clone(&entered.host);
                if let Some(message) = answers.serve(message, &host, entered.watcher, log).await {
                    break Some(message);
                }
            }
            Happened::Entered(result) => {
                if let Some(poke) = poke.as_mut() {
                    poke.entered_new_host(result, log).await;
                }
            }
            Happened::New(message) => {
                if let Some(poke) = poke.as_mut() {
                    poke.on_new_code_message(message, &entered, &mut answers, log)
                        .await;
                }
            }
        }
    };
    drop(poke);
    entered.leave().await;
    ended
}

/// The old host stopped its poke between calls: it goes on where it was to move to, or, the new
/// host gone by then, on its own code again; its deadline is kept either way. Gives its new
/// place, and the hash of the code it moved to.
async fn moved(
    poke: &mut Poke<'_>,
    log: &mut AttemptLog<'_>,
) -> Result<(Entered, Option<String>), String> {
    let left = poke.left_of_max_wait();
    if let Some(Moving {
        stage: Stage::Entered { to, .. },
        request,
        code_hash,
    }) = poke.moving.take()
    {
        let _ = to.host.outbox.send(AgentMessage::GoWatcher {
            watcher: to.watcher,
            max_wait_ms: left,
        });
        log.note("moved to new code\n").await;
        poke.request = request;
        return Ok((to, code_hash));
    }
    let to = poke
        .hosts
        .enter(poke.setup, &poke.request.sys_path, poke.grace)
        .await
        .map_err(|error| error.to_string())?;
    let _ = to.host.outbox.send(AgentMessage::StartWatcher {
        watcher: to.watcher,
        task: Box::new(poke.request.clone()),
        cancel_grace_ms: to.cancel_grace_ms(),
        moved_in: true,
    });
    let _ = to.host.outbox.send(AgentMessage::GoWatcher {
        watcher: to.watcher,
        max_wait_ms: left,
    });
    Ok((to, None))
}

enum Happened {
    Old(Option<RunnerMessage>),
    New(Option<RunnerMessage>),
    Entered(Result<Result<Entered, LaunchError>, tokio::task::JoinError>),
    NewCode,
}

/// Runs a wait step in its code set's host: `spawned` gets the host's process ID. Cancelling,
/// a timeout and a drain cancel its coroutine; one that doesn't end in its grace time has its
/// whole host killed, as Python can't cancel what doesn't yield.
///
/// # Errors
///
/// [`LaunchError`] if the host can't be started or its log can't be opened.
pub async fn run_watcher(
    hosts: &WatcherHosts,
    setup: &WorkerSetup,
    process: &ProcessSpec,
    task: PythonTask,
    limits: Limits,
    mut stop: Stops,
    spawned: impl FnOnce(i32),
) -> Result<Ran, LaunchError> {
    let PythonTask {
        request,
        secrets,
        handles,
    } = task;
    let started = Instant::now();
    let deadline = limits
        .timeout
        .map(|timeout| tokio::time::Instant::from_std(started) + timeout);
    let ran = |supervised, runner_result| Ran {
        supervised,
        runner_result,
        output: None,
        duration: started.elapsed(),
        start: None,
        forked: false,
        ended: Ended::default(),
        moved_to: None,
    };
    let sys_path = request.sys_path.clone();
    let entering = hosts.enter(setup, &sys_path, limits.grace);
    let entered = match unless_stopped(entering, Some((&mut stop, deadline))).await {
        Ok(entered) => entered?,
        Err(reason) => {
            let supervised = Supervised {
                termination: Termination::Unknown,
                stopped: Some(reason),
                killed: false,
            };
            return Ok(ran(supervised, None));
        }
    };
    spawned(entered.host.pid);
    if let Some(mask) = &process.mask.0 {
        mask.also_in(Arc::clone(&entered.host.mask));
    }
    let mut log = AttemptLog::open(process)
        .await
        .map_err(|source| LaunchError::Start {
            what: process.log_file.display().to_string(),
            source,
        })?;
    let poke = poke_of(
        hosts,
        setup,
        &request,
        started,
        (&process.mask, limits.grace),
    );
    let _ = entered.host.outbox.send(AgentMessage::StartWatcher {
        watcher: entered.watcher,
        task: Box::new(request),
        cancel_grace_ms: entered.cancel_grace_ms(),
        moved_in: false,
    });
    let place = Place {
        host: StdMutex::new(Arc::clone(&entered.host)),
        watcher: AtomicU64::new(entered.watcher),
        cancelling: AtomicBool::new(false),
        moved_to: StdMutex::new(None),
    };
    let (message, reason, killed) = {
        let answers = Answers {
            secrets,
            handles,
            pending: tokio::task::JoinSet::new(),
        };
        let talking = conversation(entered, poke, &place, answers, &mut log);
        let talking = Box::pin(talking);
        until_ended(talking, &mut stop, deadline, &place, limits).await
    };
    log.finish().await;
    let supervised = Supervised {
        termination: termination_of(message.as_ref(), killed, &place).await,
        stopped: reason,
        killed,
    };
    let result = message.filter(|message| !matches!(message, RunnerMessage::WatcherCancelled));
    Ok(Ran {
        moved_to: place.moved_to(),
        ..ran(supervised, result)
    })
}

/// Waits for a watcher's end, cancelling it at the first reason to stop. Gives its last message,
/// the reason, and whether its host had to be killed.
async fn until_ended(
    mut talking: impl Future<Output = Option<RunnerMessage>> + Unpin,
    stop: &mut Stops,
    deadline: Option<tokio::time::Instant>,
    place: &Place,
    limits: Limits,
) -> (Option<RunnerMessage>, Option<StopReason>, bool) {
    let until = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    };
    let reason = tokio::select! {
        biased;
        reason = stop.next_reason() => reason,
        () = until => StopReason::TimedOut,
        message = &mut talking => return (message, None, false),
    };
    place.cancel();
    let (message, killed) = cancelled(place, &mut talking, stop, limits, reason).await;
    (message, Some(reason), killed)
}

/// What a watcher's end stands for, as a process's would.
async fn termination_of(
    message: Option<&RunnerMessage>,
    killed: bool,
    place: &Place,
) -> Termination {
    match message {
        Some(RunnerMessage::TaskSucceeded { .. }) => Termination::Exited(0),
        Some(RunnerMessage::TaskFailed { .. }) => Termination::Exited(FAILED_EXIT_CODE),
        Some(RunnerMessage::WatcherCancelled) => Termination::Signaled(TERMINATED),
        Some(_) => Termination::Unknown,
        None if killed => Termination::Signaled(KILLED),
        None => ended_host(&place.host()).await,
    }
}

/// What moves a `@sensor.poke` to new code; a `@sensor` holds its progress, and never moves.
fn poke_of<'a>(
    hosts: &'a WatcherHosts,
    setup: &'a WorkerSetup,
    request: &TaskRequest,
    started: Instant,
    (mask, grace): (&Mask, Duration),
) -> Option<Poke<'a>> {
    let TaskKind::Sensor {
        mode: SensorMode::Poke,
        max_wait_ms,
        ..
    } = request.task.kind
    else {
        return None;
    };
    Some(Poke {
        setup,
        hosts,
        mask: mask.clone(),
        grace,
        request: request.clone(),
        code: hosts.code_channel().subscribe(),
        tried: request.sys_path.clone(),
        max_wait: max_wait_ms.map(Duration::from_millis),
        started,
        moving: None,
    })
}

/// How the host's end was told; its watchers are told after it.
async fn ended_host(host: &Host) -> Termination {
    let mut ended = host.ended.clone();
    match ended.wait_for(Option::is_some).await {
        Ok(termination) => (*termination).unwrap_or(Termination::Unknown),
        Err(_) => Termination::Unknown,
    }
}

/// Waits for a cancelled watcher to end, for its grace time: a later reason can only hurry it,
/// as for a process. Past it the host is killed. Returns its last message and whether it was.
async fn cancelled(
    place: &Place,
    talking: &mut (impl Future<Output = Option<RunnerMessage>> + Unpin),
    stop: &mut Stops,
    limits: Limits,
    reason: StopReason,
) -> (Option<RunnerMessage>, bool) {
    let grace = if reason == StopReason::Interrupted {
        limits.interrupt_grace
    } else {
        limits.grace
    };
    let mut kill_at = tokio::time::Instant::now() + grace;
    loop {
        tokio::select! {
            biased;
            later = stop.next_reason() => {
                if later == StopReason::Interrupted {
                    kill_at = kill_at.min(tokio::time::Instant::now() + limits.interrupt_grace);
                }
            }
            message = &mut *talking => return (message, false),
            () = tokio::time::sleep_until(kill_at) => {
                place.host().stop.notify_one();
                return (None, true);
            }
        }
    }
}
