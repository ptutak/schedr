//! Extensions' services (decision 518): one long-running process per worker and extension, for
//! what its attempts share (a client, a watch, a warm session), beside the plugins that run
//! each attempt.
//!
//! The worker starts it at load with its control socket (`SCHEDR_SERVICE_FD`), an inherited
//! end of a socket pair: it listens on nothing. It says `hello` (the extension and the versions
//! it serves), gets `welcome` with its declared secrets (`service_secrets`, read for this
//! start, never in its environment), and says `ready` within `service_ready_timeout`; only
//! then are its versions offered, and only those it serves of the configured ones. Each attempt
//! of the extension gets a private connection: a socket pair, one end the plugin's
//! (`SCHEDR_SERVICE_FD` in its environment), the other passed to the service with
//! `attempt_connected` (`SCM_RIGHTS`); the worker isn't in the data path. The service answers
//! `attempt_accepted` within `service_accept_timeout`, and only then does the plugin start
//! (DN32); unanswered, the attempt fails.
//!
//! A crash (its exit, a protocol it breaks, no `ready` in time) or memory past
//! `service_memory` ends it: its attempts' connections close with it, and their plugins fail
//! them (retries apply). It is started again after a wait doubling from
//! `SCHEDR_HIDDEN__WORKER__SERVICE_BACKOFF_FIRST` to `..._MAX`; past `service_restarts` within
//! `service_settle`, the extension is turned off on the worker until `service_settle` has passed
//! since the first of them (DN29), then tried again. Meanwhile its attempts are rejected as not
//! ready (`EXTENSION_NOT_READY`), waited for as code on its way is. Nothing pings it (DN30): a
//! hung one shows as attempts unanswered or timing out, and `service_timeouts` of them in a row
//! count as its crash (DN31, DN32). When the worker stops, its drain done, no attempt connects
//! any more; once the last connected one has ended, it gets SIGTERM, and SIGKILL after the
//! grace. A worker drained for maintenance keeps it.
//!
//! Its output goes to the worker's log, a line at a time, tagged with the extension, its
//! secrets masked.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags};
use rustix::process::{Pid, Signal, kill_process_group};
use schedr_extension::protocol::{
    SERVICE_PROTOCOL, SecretName as WireSecretName, SecretValue, ServiceMessage, ServiceTask,
    ToService,
};
use schedr_model::extension::{Declarations, EventKind, ExtensionEvent};
use schedr_model::secret::SecretName;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use crate::channel::{ChannelError, Messages};
use crate::figures::Figures;
use crate::mask::MaskSet;
use crate::secrets::SecretStores;

#[cfg(test)]
mod tests;

/// The environment variable naming a service's control socket, and a plugin's end of its
/// attempt's connection to the service.
pub const SERVICE_FD_ENV: &str = "SCHEDR_SERVICE_FD";

/// The most connection requests waiting for a service at once; more wait for room.
const CONNECTS_WAITING: usize = 64;

/// How a service's restarts are spaced (hidden timings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceTimings {
    /// The first wait after a crash.
    pub backoff_first: Duration,
    /// The longest.
    pub backoff_max: Duration,
}

impl Default for ServiceTimings {
    fn default() -> Self {
        Self {
            backoff_first: Duration::from_secs(1),
            backoff_max: Duration::from_mins(1),
        }
    }
}

impl ServiceTimings {
    /// The wait after `previous` (none: the first crash).
    #[must_use]
    pub fn next(&self, previous: Option<Duration>) -> Duration {
        previous
            .map_or(self.backoff_first, |wait| wait.saturating_mul(2))
            .min(self.backoff_max)
            .max(self.backoff_first.min(self.backoff_max))
    }
}

/// `[extensions.<name>]`'s service keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceConfig {
    /// Its program, then its arguments.
    pub command: Vec<String>,
    /// How long it has to say `ready`.
    pub ready_timeout: Duration,
    /// The memory its process group may use.
    pub memory: u64,
    /// How many restarts within `settle` it may have.
    pub restarts: u32,
    /// How many of its attempts timing out in a row, or their connections going unanswered,
    /// count as its crash (DN31, DN32); 0: never.
    pub timeouts: u32,
    /// How long it has to accept an attempt's connection (DN32).
    pub accept_timeout: Duration,
    /// The window restarts count in, and how long it is turned off past them.
    pub settle: Duration,
    /// The secrets it is given.
    pub secrets: Vec<SecretName>,
    /// The built-in service metrics collected (`builtin_service_metrics`): their names.
    pub builtin_metrics: Vec<String>,
}

/// A service to keep up.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// The extension.
    pub extension: String,
    /// Its keys.
    pub config: ServiceConfig,
    /// The versions configured for the extension: what it may offer.
    pub versions: BTreeSet<String>,
    /// Its environment (the worker's allow-listed one; [`SERVICE_FD_ENV`] is added).
    pub env: BTreeMap<String, String>,
    /// Its working directory.
    pub working_dir: PathBuf,
    /// How long it has after SIGTERM.
    pub grace: Duration,
    /// Its restarts' spacing.
    pub timings: ServiceTimings,
    /// Its extension's configured declarations and series cap: its metric points are kept under
    /// them (agy's review: declared otherwise only by a plugin's handshake, so a service's points
    /// were dropped until one ran).
    pub declarations: Declarations,
    /// The most series each metric keeps.
    pub max_series: u32,
}

/// What a service supervisor needs of the worker.
#[derive(Debug, Clone)]
pub struct ServiceDeps {
    /// Where its secrets come from.
    pub secrets: Arc<SecretStores>,
    /// Where its events and metrics go.
    pub figures: Figures,
    /// Where its processes are measured (`/proc`).
    pub proc_root: PathBuf,
    /// How often its memory is looked at: the worker's memory check
    /// (`SCHEDR_HIDDEN__WORKER__MEMORY_CHECK_INTERVAL`).
    pub memory_interval: Duration,
}

/// Where a service is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Started, not ready yet.
    Starting,
    /// Ready: serving these versions.
    Ready(BTreeSet<String>),
    /// Ended, to be started again: why.
    Down(String),
    /// Past its restarts: off until the settle has passed (why, and for how long from then).
    TurnedOff(String),
    /// Stopped for good: the worker stopping.
    Stopped,
}

impl State {
    /// Why an attempt can't connect now, if it can't.
    #[must_use]
    pub fn not_ready(&self, version: &str) -> Option<String> {
        match self {
            Self::Ready(versions) if versions.contains(version) => None,
            Self::Ready(versions) => Some(format!(
                "its service serves {}, not {version}",
                versions.iter().cloned().collect::<Vec<_>>().join(", ")
            )),
            Self::Starting => Some("its service is starting".to_owned()),
            Self::Down(why) => Some(format!("its service is restarting ({why})")),
            Self::TurnedOff(why) => Some(format!("its service is turned off ({why})")),
            Self::Stopped => Some("its service has stopped (the worker stops)".to_owned()),
        }
    }
}

/// An attempt's connection to its service: the plugin's end, and the hold that counts it
/// connected until it is dropped (the attempt's end).
#[derive(Debug)]
pub struct Link {
    /// The plugin's end, to pass as [`SERVICE_FD_ENV`].
    pub fd: OwnedFd,
    hold: Connected,
    /// The run of the service it reached.
    run: u64,
}

impl Link {
    /// The run of the service it reached, for [`Services::attempt_ended`].
    #[must_use]
    pub const fn run(&self) -> u64 {
        self.run
    }

    /// The plugin's end, to hand it, and the hold to keep until the attempt has ended.
    #[must_use]
    pub fn split(self) -> (OwnedFd, LinkHold) {
        (
            self.fd,
            LinkHold {
                _connected: self.hold,
            },
        )
    }
}

/// Keeps an attempt counted as connected to its service until it is dropped: a draining
/// worker stops the service only once none is.
#[derive(Debug)]
pub struct LinkHold {
    _connected: Connected,
}

/// Counts an attempt connected to a service while it lives.
#[derive(Debug)]
struct Connected(Arc<Counted>);

#[derive(Debug, Default)]
struct Counted {
    connected: AtomicUsize,
    changed: Notify,
    /// Its attempts timed out in a row (DN31): reset only by an attempt ending otherwise, not by
    /// an accepted connection (a service may accept and hang on the work).
    timed_out: std::sync::atomic::AtomicU32,
    /// Told when `timed_out` reaches the limit.
    hung: Notify,
    /// The service's run now, counted from 1: an attempt of an earlier run doesn't count
    /// towards this one's timeouts (agy's review).
    run: std::sync::atomic::AtomicU64,
    /// How many times it has become ready (DN37): told in each heartbeat, and with each
    /// refusal for its not being ready, so the master knows a refusal answered by order of
    /// count, whatever the order its messages are handled in.
    readies: std::sync::atomic::AtomicU64,
}

impl Drop for Connected {
    fn drop(&mut self) {
        self.0.connected.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
        self.0.changed.notify_one();
    }
}

struct Connect {
    attempt_id: String,
    task: ServiceTask,
    reply: oneshot::Sender<Result<Link, String>>,
}

#[derive(Debug, Clone)]
struct Handle {
    state: watch::Receiver<State>,
    connects: mpsc::Sender<Connect>,
    counted: Arc<Counted>,
    timeouts: u32,
}

impl std::fmt::Debug for Connect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connect")
            .field("attempt_id", &self.attempt_id)
            .finish_non_exhaustive()
    }
}

/// A worker's services, by extension.
#[derive(Debug, Clone)]
pub struct Services {
    by_extension: Arc<BTreeMap<String, Handle>>,
    /// Whether the worker is drained for maintenance, told to every service (DN33).
    drained: Arc<watch::Sender<bool>>,
}

impl Default for Services {
    fn default() -> Self {
        Self {
            by_extension: Arc::default(),
            drained: Arc::new(watch::channel(false).0),
        }
    }
}

impl Services {
    /// Starts each of `specs` and keeps it up until `stopping` turns true, then stops it after
    /// its last connected attempt.
    #[must_use]
    pub fn start(
        specs: Vec<ServiceSpec>,
        deps: &ServiceDeps,
        stopping: &watch::Receiver<bool>,
    ) -> Self {
        let drained = Arc::new(watch::channel(false).0);
        let by_extension = specs
            .into_iter()
            .map(|spec| {
                let (state, watched) = watch::channel(State::Starting);
                let (connects, requests) = mpsc::channel(CONNECTS_WAITING);
                let extension = spec.extension.clone();
                let counted = Arc::new(Counted::default());
                let timeouts = spec.config.timeouts;
                let supervised = supervise(
                    spec,
                    deps.clone(),
                    (state, Arc::clone(&counted)),
                    requests,
                    (stopping.clone(), drained.subscribe()),
                );
                tokio::spawn(supervised);
                (
                    extension,
                    Handle {
                        state: watched,
                        connects,
                        counted,
                        timeouts,
                    },
                )
            })
            .collect();
        Self {
            by_extension: Arc::new(by_extension),
            drained,
        }
    }

    /// The worker drained for maintenance, or resumed (DN33): each service told, kept running.
    pub fn maintenance_drained(&self, drained: bool) {
        self.drained.send_if_modified(|was| {
            let changed = *was != drained;
            *was = drained;
            changed
        });
    }

    /// Whether `extension` has a service here.
    #[must_use]
    pub fn has(&self, extension: &str) -> bool {
        self.by_extension.contains_key(extension)
    }

    /// Why an attempt of `extension` at `version` can't connect now, and how many times its
    /// service has become ready so far (DN37); none when it can, or the extension has no
    /// service.
    #[must_use]
    pub fn not_ready(&self, extension: &str, version: &str) -> Option<(String, u64)> {
        let handle = self.by_extension.get(extension)?;
        // The count first: a ready seen after it is a ready the master will hear of as newer.
        let readies = handle.counted.readies.load(Ordering::SeqCst);
        handle
            .state
            .borrow()
            .not_ready(version)
            .map(|why| (why, readies))
    }

    /// Each extension with a service, and how many times its service has become ready (DN37:
    /// told in each heartbeat).
    #[must_use]
    pub fn readies(&self) -> Vec<(String, u64)> {
        self.by_extension
            .iter()
            .map(|(name, handle)| (name.clone(), handle.counted.readies.load(Ordering::SeqCst)))
            .collect()
    }

    /// Waits until every service has stopped, at most `within`.
    pub async fn stopped(&self, within: Duration) {
        let all = self.by_extension.values().map(|handle| {
            let mut state = handle.state.clone();
            async move {
                let _ = state.wait_for(|state| *state == State::Stopped).await;
            }
        });
        let _ = tokio::time::timeout(within, futures_util::future::join_all(all)).await;
    }

    /// An attempt of `extension` connected to `run` of its service ended, `timed_out` or not:
    /// past `service_timeouts` in a row, the service counts as crashed (DN31). One of an earlier
    /// run counts for nothing.
    pub fn attempt_ended(&self, extension: &str, run: u64, timed_out: bool) {
        let Some(handle) = self.by_extension.get(extension) else {
            return;
        };
        if handle.counted.run.load(Ordering::SeqCst) != run {
            return;
        }
        if !timed_out {
            handle.counted.timed_out.store(0, Ordering::SeqCst);
            return;
        }
        let in_a_row = handle.counted.timed_out.fetch_add(1, Ordering::SeqCst) + 1;
        if handle.timeouts > 0 && in_a_row >= handle.timeouts {
            handle.counted.hung.notify_one();
        }
    }

    /// Where `extension`'s service is; none without one.
    #[must_use]
    pub fn state(&self, extension: &str) -> Option<State> {
        Some(self.by_extension.get(extension)?.state.borrow().clone())
    }

    /// A private connection for attempt `attempt_id` of `task` to its extension's service;
    /// `wait_ready` waits for the service to be ready first (a cleanup after its crash, agy's
    /// review), else one not ready is refused at once.
    ///
    /// # Errors
    ///
    /// Why not: no service, not ready (or turned off, or stopped), or it couldn't be handed the
    /// connection.
    pub async fn connect(
        &self,
        attempt_id: &str,
        task: ServiceTask,
        wait_ready: bool,
    ) -> Result<Link, String> {
        let handle = self
            .by_extension
            .get(&task.name)
            .ok_or_else(|| format!("extension `{}` has no service here", task.name))?;
        if wait_ready {
            let mut state = handle.state.clone();
            let _ = state
                .wait_for(|state| !matches!(state, State::Starting | State::Down(_)))
                .await;
        }
        if let Some(why) = handle.state.borrow().not_ready(&task.version) {
            return Err(why);
        }
        let (reply, answer) = oneshot::channel();
        let connect = Connect {
            attempt_id: attempt_id.to_owned(),
            task,
            reply,
        };
        handle
            .connects
            .send(connect)
            .await
            .map_err(|_| "its service has stopped".to_owned())?;
        answer
            .await
            .map_err(|_| "its service ended before connecting the attempt".to_owned())?
    }
}

/// How a run of the service ended.
#[derive(Debug, PartialEq, Eq)]
enum Ended {
    /// Stopped as asked (a drain).
    Stopped,
    /// Crashed: why.
    Crashed(String),
    /// Past its memory: what it used.
    OverMemory(u64),
}

/// Keeps `spec`'s service up until `stopping`, as the module says.
async fn supervise(
    spec: ServiceSpec,
    deps: ServiceDeps,
    (state, counted): (watch::Sender<State>, Arc<Counted>),
    mut requests: mpsc::Receiver<Connect>,
    (mut stopping, mut drained): (watch::Receiver<bool>, watch::Receiver<bool>),
) {
    let event = |kind: EventKind, message: &str| {
        deps.figures
            .event(ExtensionEvent::now(&spec.extension, None, kind, message));
    };
    // Its built-in metrics, declared beside whatever its extension declares.
    deps.figures.set_builtins(
        &spec.extension,
        spec.config
            .builtin_metrics
            .iter()
            .filter_map(|name| schedr_model::extension::builtin_service_metric(name))
            .collect(),
    );
    let mut restarts: VecDeque<Instant> = VecDeque::new();
    let mut wait = None;
    loop {
        if *stopping.borrow() {
            break;
        }
        // A new run: its timeouts count from nothing (DN31), an earlier run's attempts' not at all.
        counted.run.fetch_add(1, Ordering::SeqCst);
        counted.timed_out.store(0, Ordering::SeqCst);
        state.send_replace(State::Starting);
        let mut unanswered = 0;
        event(EventKind::ServiceStarted, "started");
        let mut ready_at = None;
        let ended = run_once(
            &spec,
            &deps,
            (&state, &mut ready_at),
            (&mut requests, &counted),
            (&mut stopping, &mut unanswered, &mut drained),
        )
        .await;
        let Some(why) = told_ended(&spec, ended, event) else {
            break;
        };
        // A run that was ready longer than the longest wait starts the waits over.
        if ready_at.is_some_and(|at: Instant| at.elapsed() >= spec.timings.backoff_max) {
            wait = None;
        }
        let now = Instant::now();
        restarts.push_back(now);
        while restarts
            .front()
            .is_some_and(|first| now.duration_since(*first) > spec.config.settle)
        {
            restarts.pop_front();
        }
        let restarts_allowed = usize::try_from(spec.config.restarts).unwrap_or(usize::MAX);
        let pause = if restarts.len() > restarts_allowed {
            let first = restarts.front().copied().unwrap_or(now);
            let off_for = spec.config.settle.saturating_sub(now.duration_since(first));
            let text = format!(
                "{} crashes within {}; the last: {why}",
                restarts.len(),
                show_duration(spec.config.settle)
            );
            tracing::error!(
                extension = spec.extension,
                why = text,
                "the extension's service turned off on this worker; tried again in {}",
                show_duration(off_for)
            );
            event(EventKind::ServiceTurnedOff, &text);
            state.send_replace(State::TurnedOff(text));
            restarts.clear();
            wait = None;
            off_for
        } else {
            state.send_replace(State::Down(why));
            let next = spec.timings.next(wait);
            wait = Some(next);
            next
        };
        if !refusing_for(pause, &state, &mut requests, &mut stopping).await {
            break;
        }
    }
    state.send_replace(State::Stopped);
    event(EventKind::ServiceStopped, "stopped");
    requests.close();
    while let Some(connect) = requests.recv().await {
        let _ = connect
            .reply
            .send(Err("its service has stopped".to_owned()));
    }
}

/// Why a run ended, logged and told as an event; none when it was stopped.
fn told_ended(spec: &ServiceSpec, ended: Ended, event: impl Fn(EventKind, &str)) -> Option<String> {
    match ended {
        Ended::Stopped => None,
        Ended::Crashed(why) => {
            tracing::warn!(
                extension = spec.extension,
                why,
                "the extension's service crashed"
            );
            event(EventKind::ServiceCrashed, &why);
            Some(why)
        }
        Ended::OverMemory(used) => {
            let why = format!(
                "{} used, past its service_memory of {}",
                crate::memory::show_size(used),
                crate::memory::show_size(spec.config.memory)
            );
            tracing::warn!(
                extension = spec.extension,
                why,
                "the extension's service killed"
            );
            event(EventKind::ServiceMemoryKilled, &why);
            Some(why)
        }
    }
}

/// Waits `pause`, refusing connections meanwhile with the state's reason: whether it waited it
/// out (false: stopping).
async fn refusing_for(
    pause: Duration,
    state: &watch::Sender<State>,
    requests: &mut mpsc::Receiver<Connect>,
    stopping: &mut watch::Receiver<bool>,
) -> bool {
    let until = tokio::time::Instant::now() + pause;
    loop {
        tokio::select! {
            () = tokio::time::sleep_until(until) => return true,
            changed = stopping.changed() => {
                if changed.is_err() || *stopping.borrow() {
                    return false;
                }
            }
            Some(connect) = requests.recv() => {
                let why = state.borrow().not_ready("").unwrap_or_default();
                let _ = connect.reply.send(Err(why));
            }
        }
    }
}

/// One run of the service, from its start to its end.
async fn run_once(
    spec: &ServiceSpec,
    deps: &ServiceDeps,
    (state, ready_at): (&watch::Sender<State>, &mut Option<Instant>),
    (requests, counted): (&mut mpsc::Receiver<Connect>, &Arc<Counted>),
    (stopping, unanswered, drained): (
        &mut watch::Receiver<bool>,
        &mut u32,
        &mut watch::Receiver<bool>,
    ),
) -> Ended {
    let started = Instant::now();
    let (ours, theirs) = match std::os::unix::net::UnixStream::pair() {
        Ok(pair) => pair,
        Err(e) => return Ended::Crashed(format!("its control socket can't be made: {e}")),
    };
    let mut env = spec.env.clone();
    env.insert(
        SERVICE_FD_ENV.to_owned(),
        std::os::fd::AsRawFd::as_raw_fd(&theirs).to_string(),
    );
    let Some((program, args)) = spec.config.command.split_first() else {
        return Ended::Crashed("it has no command".to_owned());
    };
    let spawned = crate::process::spawn_service(
        (Path::new(program), args),
        &env,
        &spec.working_dir,
        OwnedFd::from(theirs),
    )
    .await;
    let mut child = match spawned {
        Ok(child) => child,
        Err(e) => return Ended::Crashed(format!("it can't be started: {e}")),
    };
    let Some(group) = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
    else {
        return Ended::Crashed("the started service has no process ID".to_owned());
    };
    let mask = Arc::new(MaskSet::new(deps.secrets.min_mask_length));
    let output = [
        child
            .stdout
            .take()
            .map(|out| tag_output(spec.extension.clone(), "stdout", out, Arc::clone(&mask))),
        child
            .stderr
            .take()
            .map(|err| tag_output(spec.extension.clone(), "stderr", err, Arc::clone(&mask))),
    ];
    let ended = Box::pin(serve_control(
        spec,
        deps,
        (ours, &mask),
        (state, ready_at, started),
        (requests, counted, unanswered),
        (stopping, &mut child, group, drained),
    ))
    .await;
    let _ = kill_process_group(group, Signal::KILL);
    let _ = child.wait().await;
    for reader in output.into_iter().flatten() {
        let _ = tokio::time::timeout(Duration::from_secs(1), reader).await;
    }
    ended
}

/// The longest line of a service's output logged as one: a longer one is logged in pieces of
/// this size (Grok's review: a line with no end grew the worker's buffer without bound).
const MAX_LINE: usize = 16 * 1024;

/// Writes each line of a service's `stream` to the worker's log, tagged and masked; a line
/// past [`MAX_LINE`] in pieces.
fn tag_output(
    extension: String,
    stream: &'static str,
    output: impl AsyncRead + Unpin + Send + 'static,
    mask: Arc<MaskSet>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        read_lines(output, |line| {
            let text = String::from_utf8_lossy(line);
            let text = mask.mask_text(text.trim_end_matches(['\n', '\r']));
            tracing::info!(extension, stream, "service: {text}");
        })
        .await;
    })
}

/// Each line of `output`, a line past [`MAX_LINE`] in pieces of it, to `each`; never more than
/// one piece held.
async fn read_lines(output: impl AsyncRead + Unpin, mut each: impl FnMut(&[u8])) {
    let mut reader = BufReader::new(output);
    let mut line: Vec<u8> = Vec::new();
    loop {
        let available = match reader.fill_buf().await {
            Ok([]) | Err(_) => break,
            Ok(available) => available,
        };
        let room = MAX_LINE - line.len();
        let end = available
            .iter()
            .take(room)
            .position(|b| *b == b'\n')
            .map(|at| at + 1);
        let taken = end.unwrap_or_else(|| available.len().min(room));
        line.extend_from_slice(&available[..taken]);
        reader.consume(taken);
        if end.is_some() || line.len() >= MAX_LINE {
            each(&line);
            line.clear();
        }
    }
    if !line.is_empty() {
        each(&line);
    }
}

/// The control socket's conversation, from the hello to the service's end.
#[expect(
    clippy::too_many_lines,
    reason = "a service's one conversation, read best whole"
)]
async fn serve_control(
    spec: &ServiceSpec,
    deps: &ServiceDeps,
    (ours, mask): (std::os::unix::net::UnixStream, &Arc<MaskSet>),
    (state, ready_at, started): (&watch::Sender<State>, &mut Option<Instant>, Instant),
    (requests, counted, unanswered): (&mut mpsc::Receiver<Connect>, &Arc<Counted>, &mut u32),
    (stopping, child, group, drained): (
        &mut watch::Receiver<bool>,
        &mut tokio::process::Child,
        Pid,
        &mut watch::Receiver<bool>,
    ),
) -> Ended {
    let stream = match ours
        .set_nonblocking(true)
        .and_then(|()| UnixStream::from_std(ours))
    {
        Ok(stream) => stream,
        Err(e) => return Ended::Crashed(format!("its control socket: {e}")),
    };
    let (read, write) = stream.into_split();
    let mut messages = Messages::from(read);
    let mut talk = Talk {
        write,
        mask: Arc::clone(mask),
        bound: spec.config.accept_timeout,
        told: Told::default(),
    };
    let ready_by = tokio::time::Instant::from_std(started) + spec.config.ready_timeout;
    let mut memory_check = tokio::time::interval(deps.memory_interval);
    memory_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let late = |what: &str| {
        Ended::Crashed(format!(
            "{what} within its service_ready_timeout of {}",
            show_duration(spec.config.ready_timeout)
        ))
    };
    // Until it is ready, within the ready timeout: its process, its memory and the worker
    // stopping are watched as after (agy's review: a worker stopping waited for a service
    // starting, and one growing past its memory while starting wasn't killed).
    macro_rules! starting {
        ($future:expr, $what:expr) => {{
            let future = $future;
            tokio::pin!(future);
            loop {
                tokio::select! {
                    done = &mut future => break done,
                    () = tokio::time::sleep_until(ready_by) => return late($what),
                    ended = child.wait() => return Ended::Crashed(exited(ended)),
                    _ = memory_check.tick() => {
                        let used = measure(&deps.proc_root, group).await;
                        if used > spec.config.memory {
                            return Ended::OverMemory(used);
                        }
                    }
                    changed = stopping.changed() => {
                        if changed.is_err() || *stopping.borrow() {
                            return terminate(group, child, spec.grace).await;
                        }
                    }
                }
            }
        }};
    }
    let hello = starting!(messages.next::<ServiceMessage>(), "no hello");
    let versions = match check_hello(spec, hello) {
        Ok(versions) => versions,
        Err(why) => {
            let refused = ToService::ServiceRefused {
                reason: why.clone(),
            };
            let _ = talk.send(&refused, None).await;
            return Ended::Crashed(why);
        }
    };
    let secrets = match starting!(
        read_secrets(&deps.secrets, &spec.config.secrets, mask),
        "its secrets not read"
    ) {
        Ok(secrets) => secrets,
        Err(why) => return Ended::Crashed(why),
    };
    if let Err(why) = talk
        .send(&ToService::ServiceWelcome { secrets }, None)
        .await
    {
        return Ended::Crashed(why);
    }
    loop {
        match starting!(messages.next::<ServiceMessage>(), "not ready") {
            Ok(ServiceMessage::Ready) => break,
            Ok(other) => {
                let said = starting!(told(spec, deps, &mut talk, other, &versions), "not ready");
                if let Err(why) = said {
                    return Ended::Crashed(why);
                }
            }
            Err(e) => return Ended::Crashed(broken(&e)),
        }
    }
    // Its metric points kept under its configured declarations from now on.
    if !spec.declarations.metrics.is_empty() {
        for version in &versions {
            deps.figures.declare(
                &spec.extension,
                version,
                spec.declarations.clone(),
                spec.max_series,
            );
        }
    }
    // Its built-in metrics are kept once its version is declared, by the config above or a
    // plugin's hello: never declared alone (agy's review: an idle worker's built-ins-only
    // declarations, the newest heard, hid the plugin's metrics and its rollups).
    *ready_at = Some(Instant::now());
    // Ready first, then counted (agy's review): a refusal reads the count, then the state, so
    // one that saw this count saw it ready and refused nothing; one that refused saw the count
    // before, which this ready passes.
    state.send_replace(State::Ready(versions.clone()));
    counted.readies.fetch_add(1, Ordering::SeqCst);
    deps.figures.event(ExtensionEvent::now(
        &spec.extension,
        None,
        EventKind::ServiceReady,
        &format!(
            "ready, serving {}",
            versions.iter().cloned().collect::<Vec<_>>().join(", ")
        ),
    ));
    tracing::info!(
        extension = spec.extension,
        "the extension's service is ready"
    );
    // Started while drained for maintenance: told so at once (DN33).
    let mut told_drained = false;
    if *drained.borrow_and_update() {
        if let Err(why) = talk.send(&ToService::Drained, None).await {
            return Ended::Crashed(why);
        }
        told_drained = true;
    }
    // Connections handed over, waiting for their `attempt_accepted` (DN32).
    let mut accepting: BTreeMap<String, Accepting> = BTreeMap::new();
    loop {
        // Stopping: no new attempts; once the last connected one ended, it is stopped.
        if *stopping.borrow() {
            state.send_replace(State::Stopped);
            if counted.connected.load(Ordering::SeqCst) == 0 {
                return terminate(group, child, spec.grace).await;
            }
        }
        let first_due = accepting.values().map(|a| a.due).min();
        tokio::select! {
            said = messages.next::<ServiceMessage>() => match said {
                Ok(ServiceMessage::AttemptAccepted { attempt_id }) => {
                    if let Some(accepted) = accepting.remove(&attempt_id) {
                        builtin(spec, deps, &versions, "service_accept_seconds", accepted.since.elapsed().as_secs_f64());
                        // Answered: only the unanswered count starts over (DN32).
                        *unanswered = 0;
                        let _ = accepted.reply.send(Ok(accepted.link));
                    }
                }
                Ok(message) => {
                    if let Err(why) = told(spec, deps, &mut talk, message, &versions).await {
                        return Ended::Crashed(why);
                    }
                }
                Err(e) => return Ended::Crashed(broken(&e)),
            },
            () = sleep_until_or_never(first_due) => {
                let now = tokio::time::Instant::now();
                let late: Vec<String> = accepting
                    .iter()
                    .filter(|(_, a)| a.due <= now)
                    .map(|(id, _)| id.clone())
                    .collect();
                for attempt_id in late {
                    if let Some(late) = accepting.remove(&attempt_id) {
                        let _ = late.reply.send(Err(format!(
                            "its service didn't accept the attempt within its \
                             service_accept_timeout of {}",
                            show_duration(spec.config.accept_timeout)
                        )));
                    }
                    *unanswered += 1;
                    if spec.config.timeouts > 0 && *unanswered >= spec.config.timeouts {
                        return Ended::Crashed(format!(
                            "{unanswered} of its attempts' connections unanswered in a row: \
                             taken for hung"
                        ));
                    }
                }
            }
            ended = child.wait() => return Ended::Crashed(exited(ended)),
            // Its attempts timing out in a row (DN31): taken for hung.
            // Read, not reset (agy's review: a permit left from an earlier streak reset a live
            // count); a new run starts it over.
            () = counted.hung.notified() => {
                let in_a_row = counted.timed_out.load(Ordering::SeqCst);
                if spec.config.timeouts > 0 && in_a_row >= spec.config.timeouts {
                    return Ended::Crashed(format!(
                        "{in_a_row} of its attempts timed out in a row: taken for hung"
                    ));
                }
            }
            Some(connect) = requests.recv(), if !*stopping.borrow() => {
                match connect_attempt(&mut talk, &connect, &versions, counted).await {
                    // Its plugin starts once the service accepts it.
                    Ok(link) => {
                        let since = tokio::time::Instant::now();
                        let due = since + spec.config.accept_timeout;
                        let waiting = Accepting { due, since, link, reply: connect.reply };
                        accepting.insert(connect.attempt_id, waiting);
                    }
                    Err(why) => {
                        let broke = why.starts_with("its control socket");
                        let _ = connect.reply.send(Err(why.clone()));
                        if broke {
                            return Ended::Crashed(why);
                        }
                    }
                }
            }
            _ = memory_check.tick() => {
                let used = measure(&deps.proc_root, group).await;
                if used > spec.config.memory {
                    return Ended::OverMemory(used);
                }
                #[expect(clippy::cast_precision_loss, reason = "bytes, shown as a gauge")]
                builtin(spec, deps, &versions, "service_memory_bytes", used as f64);
            }
            changed = stopping.changed() => {
                if changed.is_err() {
                    return terminate(group, child, spec.grace).await;
                }
            }
            Ok(()) = drained.changed() => {
                let now_drained = *drained.borrow_and_update();
                if now_drained != told_drained {
                    let message = if now_drained { ToService::Drained } else { ToService::Resumed };
                    if let Err(why) = talk.send(&message, None).await {
                        return Ended::Crashed(why);
                    }
                    told_drained = now_drained;
                }
            }
            () = counted.changed.notified(), if *stopping.borrow() => {}
        }
    }
}

/// An attempt's connection handed to the service, waiting for its `attempt_accepted`.
struct Accepting {
    due: tokio::time::Instant,
    /// When it was handed over: its accept time.
    since: tokio::time::Instant,
    link: Link,
    reply: oneshot::Sender<Result<Link, String>>,
}

/// Sleeps until `due`, or for ever without one.
async fn sleep_until_or_never(due: Option<tokio::time::Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => std::future::pending().await,
    }
}

/// The versions a service's hello serves of the configured ones.
fn check_hello(
    spec: &ServiceSpec,
    hello: Result<ServiceMessage, ChannelError>,
) -> Result<BTreeSet<String>, String> {
    let (mode, protocol, name, versions) = match hello {
        Ok(ServiceMessage::ServiceHello {
            mode,
            protocol,
            name,
            versions,
            ..
        }) => (mode, protocol, name, versions),
        Ok(_) => return Err("its first message isn't `hello`".to_owned()),
        Err(e) => return Err(broken(&e)),
    };
    if mode != "service" {
        return Err(format!("its hello's mode is `{mode}`, not `service`"));
    }
    if protocol != SERVICE_PROTOCOL {
        return Err(format!(
            "it speaks service protocol {protocol}, this worker {SERVICE_PROTOCOL}"
        ));
    }
    if name != spec.extension {
        return Err(format!(
            "its hello names extension `{name}`, configured as `{}`",
            spec.extension
        ));
    }
    let serves: BTreeSet<String> = versions
        .into_iter()
        .filter(|version| spec.versions.contains(version))
        .collect();
    if serves.is_empty() {
        return Err(format!(
            "it serves none of the configured versions ({})",
            spec.versions.iter().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(serves)
}

/// Its declared secrets, read now, their values masked in its output from here on.
async fn read_secrets(
    stores: &SecretStores,
    declared: &[SecretName],
    mask: &MaskSet,
) -> Result<Vec<SecretValue>, String> {
    let mut values = Vec::with_capacity(declared.len());
    for name in declared {
        let value = stores
            .fetch(name)
            .await
            .map_err(|e| format!("its secret `{name}` can't be read: {e}"))?;
        mask_value(mask, &name.to_string(), &value);
        values.push(SecretValue {
            name: wire_name(name),
            value,
        });
    }
    Ok(values)
}

fn mask_value(mask: &MaskSet, label: &str, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(text) => {
            mask.add(label, text);
        }
        serde_json::Value::Object(keys) => {
            for (key, value) in keys {
                mask_value(mask, &format!("{label}#{key}"), value);
            }
        }
        _ => {}
    }
}

fn wire_name(name: &SecretName) -> WireSecretName {
    WireSecretName {
        storage: name.storage.clone(),
        path: name.path.clone(),
        key: name.key.clone(),
    }
}

/// The worker's end of a service's control socket: its writes bounded (agy's review: a
/// service not reading its socket held the supervisor in a write for ever), its mask, and what
/// it told so far.
struct Talk {
    write: OwnedWriteHalf,
    mask: Arc<MaskSet>,
    /// How long a write, or a secret read for it, may take: its `service_accept_timeout`.
    bound: Duration,
    told: Told,
}

impl Talk {
    /// Sends `message` (with `fd`), within the bound; an `Err` breaks the conversation.
    async fn send(&mut self, message: &impl Serialize, fd: Option<&OwnedFd>) -> Result<(), String> {
        match tokio::time::timeout(self.bound, send(&mut self.write, message, fd)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(format!("its control socket: {e}")),
            Err(_) => Err(format!(
                "its control socket: not read within its service_accept_timeout of {}",
                show_duration(self.bound)
            )),
        }
    }
}

/// What a service told, kept to bound what it tells: its last status and when, and the metrics
/// warned of.
#[derive(Default)]
struct Told {
    status: Option<(String, Instant)>,
    warned: BTreeSet<String>,
}

/// The fewest seconds between a service's statuses kept as events: past it, one is dropped
/// (agy's review: a noisy service pushed out the other extensions' events).
const STATUS_SPACING: Duration = Duration::from_secs(1);

/// Handles a message of the service's other than its ready: a secret asked for again, a
/// status, metric points; what it tells is masked with its secrets. An `Err` breaks the
/// conversation.
async fn told(
    spec: &ServiceSpec,
    deps: &ServiceDeps,
    talk: &mut Talk,
    message: ServiceMessage,
    versions: &BTreeSet<String>,
) -> Result<(), String> {
    match message {
        ServiceMessage::ServiceHello { .. } => Err("a second hello".to_owned()),
        // An accept of a connection no longer waiting (it was late): nothing to do.
        ServiceMessage::Ready | ServiceMessage::AttemptAccepted { .. } => Ok(()),
        ServiceMessage::SecretGet { secret } => {
            let name = SecretName {
                storage: secret.storage,
                path: secret.path,
                key: secret.key,
                default: None,
            };
            let answer = if spec.config.secrets.contains(&name) {
                match tokio::time::timeout(talk.bound, deps.secrets.fetch(&name)).await {
                    Ok(Ok(value)) => {
                        // A value changed since its start is masked from here on too.
                        mask_value(&talk.mask, &name.to_string(), &value);
                        ToService::ServiceSecret { value }
                    }
                    Ok(Err(e)) => ToService::ServiceSecretFailed {
                        kind: e.kind().to_owned(),
                        message: e.to_string(),
                    },
                    Err(_) => ToService::ServiceSecretFailed {
                        kind: "secret_unavailable".to_owned(),
                        message: format!("`{name}`: its storage didn't answer in time"),
                    },
                }
            } else {
                ToService::ServiceSecretFailed {
                    kind: "secret_denied".to_owned(),
                    message: format!(
                        "`{name}` isn't one of [extensions.{}] service_secrets",
                        spec.extension
                    ),
                }
            };
            talk.send(&answer, None).await
        }
        ServiceMessage::Status { phase, message } => {
            let text = match message {
                Some(message) => format!("{phase}: {message}"),
                None => phase,
            };
            let text = talk.mask.mask_text(&text);
            // Told when it changes, at most once a second.
            let kept = talk
                .told
                .status
                .as_ref()
                .is_none_or(|(last, at)| *last != text && at.elapsed() >= STATUS_SPACING);
            if kept {
                deps.figures.event(ExtensionEvent::now(
                    &spec.extension,
                    None,
                    EventKind::ServiceStatus,
                    &text,
                ));
                talk.told.status = Some((text, Instant::now()));
            }
            Ok(())
        }
        ServiceMessage::Metrics { points } => {
            kept_points(spec, deps, talk, versions, points);
            Ok(())
        }
    }
}

/// A built-in service metric's point (`service_*`, unlabelled), kept under the newest version
/// the service serves that has declarations, when it is collected.
fn builtin(
    spec: &ServiceSpec,
    deps: &ServiceDeps,
    versions: &BTreeSet<String>,
    name: &str,
    value: f64,
) {
    if !spec.config.builtin_metrics.iter().any(|n| n == name) {
        return;
    }
    let Some(version) = newest_declared(&deps.figures, &spec.extension, versions) else {
        return;
    };
    let point = schedr_model::extension::MetricPoint {
        name: name.to_owned(),
        labels: BTreeMap::new(),
        value,
    };
    let _ = deps.figures.observe(&spec.extension, &version, &point);
}

/// A service's metric points kept under the newest version it serves that has declarations;
/// one of the worker's own built-ins dropped, another undeclared warned of once.
fn kept_points(
    spec: &ServiceSpec,
    deps: &ServiceDeps,
    talk: &mut Talk,
    versions: &BTreeSet<String>,
    points: Vec<schedr_extension::protocol::MetricPoint>,
) {
    // Kept under the newest version it serves that has declarations.
    let version = newest_declared(&deps.figures, &spec.extension, versions);
    for point in points {
        // The worker's own measures aren't the service's to set (Grok's review): of the
        // built-ins, it sends its calls' times only.
        if point
            .name
            .starts_with(schedr_model::extension::BUILTIN_SERVICE_PREFIX)
            && point.name != SERVICE_CALL_SECONDS
        {
            continue;
        }
        let point = schedr_model::extension::MetricPoint {
            labels: point
                .labels
                .into_iter()
                .map(|(label, value)| (label, talk.mask.mask_text(&value)))
                .collect(),
            name: point.name,
            value: point.value,
        };
        let kept = version.as_deref().is_some_and(|version| {
            deps.figures
                .observe(&spec.extension, version, &point)
                .is_ok()
        });
        // A built-in one turned off (`builtin_service_metrics`): not collected, silently
        // (Python's `serve` sends its call times whatever the worker keeps).
        let builtin = point
            .name
            .starts_with(schedr_model::extension::BUILTIN_SERVICE_PREFIX);
        if !kept
            && !builtin
            && talk.told.warned.len() < MAX_WARNED
            && talk.told.warned.insert(point.name.clone())
        {
            tracing::warn!(
                extension = spec.extension,
                metric = point.name,
                "a service's metric point dropped: not declared for the versions it serves"
            );
        }
    }
}

/// The built-in metric a service sends itself: its calls' times.
const SERVICE_CALL_SECONDS: &str = "service_call_seconds";

/// The most metric names a service's run is warned of.
const MAX_WARNED: usize = 50;

/// The newest of `versions` with declarations kept for `extension`: versions compared by their
/// numbers where they are numbers (`1.10` after `1.2`).
fn newest_declared(
    figures: &Figures,
    extension: &str,
    versions: &BTreeSet<String>,
) -> Option<String> {
    versions
        .iter()
        .filter(|version| figures.is_declared(extension, version))
        .max_by(|a, b| version_order(a, b))
        .cloned()
}

/// `a` against `b`, part by part, numbers as numbers.
fn version_order(a: &str, b: &str) -> std::cmp::Ordering {
    let parts = |v: &str| -> Vec<(u64, String)> {
        v.split(['.', '-'])
            .map(|part| (part.parse().unwrap_or(0), part.to_owned()))
            .collect()
    };
    parts(a).cmp(&parts(b))
}

/// The memory of a service's process group, measured off the runtime's threads.
async fn measure(proc_root: &Path, group: Pid) -> u64 {
    let root = proc_root.to_owned();
    let id = group.as_raw_nonzero().get();
    tokio::task::spawn_blocking(move || crate::memory::group_memory(&root, id, true))
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
}

/// Hands `connect`'s attempt a private connection: the service's end with `attempt_connected`.
async fn connect_attempt(
    talk: &mut Talk,
    connect: &Connect,
    versions: &BTreeSet<String>,
    counted: &Arc<Counted>,
) -> Result<Link, String> {
    if !versions.contains(&connect.task.version) {
        return Err(format!(
            "its service doesn't serve {}",
            connect.task.version
        ));
    }
    let (plugin, service) = std::os::unix::net::UnixStream::pair()
        .map_err(|e| format!("the attempt's connection can't be made: {e}"))?;
    let message = ToService::AttemptConnected {
        attempt_id: connect.attempt_id.clone(),
        task: connect.task.clone(),
    };
    let service = OwnedFd::from(service);
    talk.send(&message, Some(&service)).await?;
    drop(service);
    counted.connected.fetch_add(1, Ordering::SeqCst);
    Ok(Link {
        fd: OwnedFd::from(plugin),
        hold: Connected(Arc::clone(counted)),
        run: counted.run.load(Ordering::SeqCst),
    })
}

/// SIGTERM to its group, SIGKILL once `grace` has passed.
async fn terminate(group: Pid, child: &mut tokio::process::Child, grace: Duration) -> Ended {
    let _ = kill_process_group(group, Signal::TERM);
    if tokio::time::timeout(grace, child.wait()).await.is_err() {
        let _ = kill_process_group(group, Signal::KILL);
    }
    Ended::Stopped
}

/// Sends `message` in the channel's framing, `fd` on its first bytes (`SCM_RIGHTS`) if given.
async fn send(
    write: &mut OwnedWriteHalf,
    message: &impl Serialize,
    fd: Option<&OwnedFd>,
) -> Result<(), ChannelError> {
    let body = serde_json::to_vec(message)?;
    let size = u32::try_from(body.len())
        .ok()
        .filter(|size| *size <= schedr_model::runner::MAX_MESSAGE_SIZE)
        .ok_or(ChannelError::TooLarge(body.len() as u64))?;
    let header = size.to_be_bytes();
    let mut sent = 0;
    if let Some(fd) = fd {
        let stream: &UnixStream = write.as_ref();
        let borrowed = [fd.as_fd()];
        while sent == 0 {
            stream.writable().await?;
            let result = stream.try_io(tokio::io::Interest::WRITABLE, || {
                let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
                let mut control = SendAncillaryBuffer::new(&mut space);
                control.push(SendAncillaryMessage::ScmRights(&borrowed));
                rustix::net::sendmsg(
                    stream,
                    &[IoSlice::new(&header)],
                    &mut control,
                    SendFlags::empty(),
                )
                .map_err(io::Error::from)
            });
            match result {
                Ok(n) => sent = n,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    write.write_all(&header[sent..]).await?;
    write.write_all(&body).await?;
    Ok(())
}

fn exited(ended: io::Result<std::process::ExitStatus>) -> String {
    match ended {
        Ok(status) => format!("it ended ({status})"),
        Err(e) => format!("it can't be waited for: {e}"),
    }
}

fn broken(error: &ChannelError) -> String {
    match error {
        ChannelError::Closed => "it closed its control socket".to_owned(),
        other => format!("its control socket: {other}"),
    }
}

fn show_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds >= 3600 && seconds.is_multiple_of(3600) {
        format!("{} h", seconds / 3600)
    } else if seconds >= 60 && seconds.is_multiple_of(60) {
        format!("{} min", seconds / 60)
    } else {
        format!("{seconds} s")
    }
}
