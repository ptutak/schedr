//! The fork servers, agent side (see PROJECT.md, How Python tasks are started: the fork
//! server): started with the worker, two per interpreter (one preloading the configured
//! libraries, a bare one), each restarted with backoff when it dies; a Python task asks the
//! preloaded one to fork its runner, then the bare one, and is spawned when neither can.
//!
//! A forked runner is not the agent's child: its **shim** (forked by the fork server) is its
//! parent and reports on a status socket the agent passes with the request,
//! `process_started` (the PID, once the process group exists) and `process_exited`. The shim
//! reaps the process only when the agent closes that socket ([`GroupHold`]), so its PID and
//! group can't be reused while the agent may still signal them.
//!
//! The thin operating-system layer under [`crate::launch`], exercised by the integration tests
//! (`tests/fork.rs`); the backoff is unit-tested.

use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags};
use rustix::process::{Pid, kill_process_group};
use schedr_model::runner::{
    AgentMessage, ForkRequest, ForkServerRequest, MAX_MESSAGE_SIZE, RunnerMessage, RunnerMode,
};
use schedr_proto::pb;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::net::unix::pipe;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::attempt::WorkerSetup;
use crate::channel::{ChannelError, read_message};
use crate::launch::{LaunchError, RUNNER_START_TIMEOUT, start_runner};
use crate::metrics::{ForkFallbackLabels, ForkServerLabels};
use crate::output::TailBuffer;
use crate::process::{OsProcess, ProcessSpec, collect, open_log};
use crate::status::Termination;
use crate::supervise::{Process, Signal};
use crate::usage::Ended;

#[cfg(test)]
mod tests;

/// How long a fork server may take to preload its libraries and answer.
pub const PRELOAD_TIMEOUT: Duration = Duration::from_secs(300);

/// How long a fork server may take to answer a `fork` request.
const FORK_ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// The first wait before restarting a fork server that died or couldn't start.
pub const BACKOFF_FIRST: Duration = Duration::from_secs(1);

/// The longest wait before a restart.
pub const BACKOFF_MAX: Duration = Duration::from_secs(300);

/// How long a fork server must have been up for the backoff to start again from
/// [`BACKOFF_FIRST`].
pub const BACKOFF_RESET_AFTER: Duration = Duration::from_secs(600);

/// Why a fork server came up disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// A preloaded library failed to import: maybe something not ready yet, so retried for
    /// `[worker] fork_server_import_retry` (decisions 19A, 20A).
    Import,
    /// Threads after preloading, or a version mismatch: the same on every start until the
    /// image changes, so never retried.
    Lasting,
}

/// Whether a fork server disabled for `cause`, failing for `failing_for` since its first
/// failure, is started again: a failing import within `window` (long enough for what isn't
/// ready at a pod's start; a failure still there after it is lasting: a missing package, a
/// typo).
#[must_use]
pub fn retried(cause: Cause, failing_for: Duration, window: Duration) -> bool {
    cause == Cause::Import && failing_for < window
}

/// The wait before the next restart: `previous` doubled up to [`BACKOFF_MAX`], or
/// [`BACKOFF_FIRST`] again after a server that was up for [`BACKOFF_RESET_AFTER`].
#[must_use]
pub fn next_backoff(previous: Duration, up_for: Option<Duration>) -> Duration {
    if up_for.is_some_and(|up| up >= BACKOFF_RESET_AFTER) {
        BACKOFF_FIRST
    } else {
        (previous * 2).min(BACKOFF_MAX)
    }
}

/// Which of an interpreter's two fork servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// It preloads the configured libraries.
    Preloaded,
    /// It preloads nothing (for tasks whose libraries don't survive the preloaded one).
    Bare,
}

impl Kind {
    /// The metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Preloaded => "preloaded",
            Self::Bare => "bare",
        }
    }
}

/// Why a task wasn't forked by one fork server (the metric's `reason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fallback {
    /// The fork server is disabled (a failing preload, threads, a version mismatch).
    Disabled,
    /// The fork server isn't running (starting, or waiting to restart).
    Down,
    /// It answered `fork_failed`.
    ForkFailed,
    /// The shim ended (or said nothing in time) before reporting the process.
    ShimDied,
}

impl Fallback {
    /// The metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Down => "down",
            Self::ForkFailed => "fork_failed",
            Self::ShimDied => "shim_died",
        }
    }
}

/// Where the fork servers' states and fallbacks are counted.
#[derive(Debug, Clone, Default)]
pub struct ForkMetrics {
    /// Fork servers by interpreter, kind and state (1 for the current state).
    pub servers: Family<ForkServerLabels, Gauge>,
    /// Python tasks that weren't forked by a fork server, by interpreter and reason.
    pub fallbacks: Family<ForkFallbackLabels, Counter>,
}

/// A fork server's state.
#[derive(Debug)]
enum State {
    /// Starting, or waiting to restart.
    Down,
    /// Taking requests on its control socket. `asking`: a request was sent and its answer not
    /// read yet; still set when the next request comes, that answer was abandoned (its reader
    /// was dropped), and the socket's answers no longer pair with requests.
    Up {
        control: UnixStream,
        next_id: u64,
        asking: bool,
    },
    /// Not used again until the worker restarts.
    Disabled(String),
}

impl State {
    const fn label(&self) -> &'static str {
        match self {
            Self::Down => "down",
            Self::Up { .. } => "up",
            Self::Disabled(_) => "disabled",
        }
    }
}

/// One fork server: its state, shared by its supervisor and the tasks asking it to fork.
#[derive(Debug)]
struct Server {
    kind: Kind,
    interpreter: String,
    state: Mutex<State>,
    metrics: ForkMetrics,
    board: Arc<WarningBoard>,
}

/// The disabled fork servers, as the worker's warnings (sent to the master).
#[derive(Debug)]
struct WarningBoard {
    interpreter: String,
    /// Whether the bare server is the only one (no preload list).
    bare_alone: bool,
    /// Why each disabled server is, by kind.
    reasons: std::sync::Mutex<std::collections::BTreeMap<&'static str, String>>,
    warnings: watch::Sender<Vec<pb::WorkerWarning>>,
}

impl WarningBoard {
    /// Records `kind`'s state, publishing the warnings if they changed.
    fn record(&self, kind: Kind, disabled: Option<&str>) {
        let mut reasons = self
            .reasons
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = match disabled {
            Some(reason) => {
                reasons.insert(kind.label(), reason.to_owned()).as_deref() != Some(reason)
            }
            None => reasons.remove(kind.label()).is_some(),
        };
        if changed {
            let warnings = reasons
                .iter()
                .rev()
                .map(|(label, reason)| self.warning(label, reason, &reasons))
                .collect();
            self.warnings.send_replace(warnings);
        }
    }

    /// The warning for server `label`, disabled for `reason`; what it means for tasks depends
    /// on whether the other server is disabled too (`disabled`).
    fn warning(
        &self,
        label: &str,
        reason: &str,
        disabled: &std::collections::BTreeMap<&'static str, String>,
    ) -> pb::WorkerWarning {
        let other = if label == "preloaded" {
            "bare"
        } else {
            "preloaded"
        };
        let then = if self.bare_alone || disabled.contains_key(other) {
            "tasks are spawned"
        } else if label == "preloaded" {
            "tasks are forked by the bare fork server, or spawned"
        } else {
            "tasks the preloaded fork server can't fork are spawned"
        };
        pb::WorkerWarning {
            kind: FORK_SERVER_DISABLED.to_owned(),
            interpreter: self.interpreter.clone(),
            message: format!("the {label} fork server is disabled: {reason}; {then}"),
        }
    }
}

/// The worker warning's kind for a disabled fork server.
pub const FORK_SERVER_DISABLED: &str = "fork_server_disabled";

impl Server {
    /// Moves to `state`, keeping the gauge's one-hot labels in step.
    fn set(&self, current: &mut State, state: State) {
        for label in ["down", "up", "disabled"] {
            let value = i64::from(label == state.label());
            self.metrics
                .servers
                .get_or_create(&ForkServerLabels {
                    interpreter: self.interpreter.clone(),
                    kind: self.kind.label().to_owned(),
                    state: label.to_owned(),
                })
                .set(value);
        }
        let disabled = match &state {
            State::Disabled(reason) => Some(reason.as_str()),
            _ => None,
        };
        self.board.record(self.kind, disabled);
        *current = state;
    }
}

/// An interpreter's fork servers, preloaded first (cheap to clone; clones share them).
#[derive(Debug, Clone)]
pub struct ForkServers {
    servers: Arc<Vec<Server>>,
    warnings: watch::Receiver<Vec<pb::WorkerWarning>>,
}

/// An interpreter's fork server settings (`[interpreters.<name>]` with `fork_server = true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkConfig {
    /// The interpreter's name.
    pub interpreter: String,
    /// The libraries the preloaded fork server imports.
    pub preload: Vec<String>,
    /// How long a failing import is retried (`[worker] fork_server_import_retry`).
    pub import_retry: Duration,
}

/// How to start an interpreter's fork servers.
#[derive(Debug, Clone)]
pub struct ForkSetup {
    /// The interpreter's name (the metrics' label).
    pub interpreter: String,
    /// The fork server's process: the interpreter, `-m schedr.runner fork_server`, its
    /// environment and directory; `log_file` is a template, each kind writes
    /// `fork-server-<kind>.log` next to it.
    pub process: ProcessSpec,
    /// The libraries the preloaded one imports; none: only the bare one is started.
    pub preload: Vec<String>,
    /// How long a failing import is retried (`[worker] fork_server_import_retry`).
    pub import_retry: Duration,
}

impl ForkServers {
    /// The DAG interpreter's fork servers for a worker set up as `setup`, started; `None`
    /// without `fork_server = true`, and off Linux (forking is unsafe on macOS: a warning
    /// says it is ignored, and tasks are spawned).
    #[must_use]
    pub fn for_worker(setup: &WorkerSetup, metrics: &ForkMetrics) -> Option<Self> {
        let config = setup.fork_server.as_ref()?;
        if !cfg!(target_os = "linux") {
            tracing::warn!(
                interpreter = config.interpreter,
                "fork_server = true is ignored: fork servers run on Linux only; tasks are spawned"
            );
            return None;
        }
        let process = ProcessSpec {
            program: setup.python.clone(),
            args: vec![
                "-m".to_owned(),
                "schedr.runner".to_owned(),
                "fork_server".to_owned(),
            ],
            env: setup
                .agent_env
                .iter()
                .filter(|(name, _)| crate::env::is_allowed(name))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            working_dir: setup.log_dir.clone(),
            log_file: setup.log_dir.join("fork-server.log"),
            max_output_size: 64 * 1024,
        };
        let setup = ForkSetup {
            interpreter: config.interpreter.clone(),
            process,
            preload: config.preload.clone(),
            import_retry: config.import_retry,
        };
        Some(Self::start(&setup, metrics))
    }

    /// Starts the fork servers in the background (each restarted with backoff while the
    /// returned value lives in any clone).
    #[must_use]
    pub fn start(setup: &ForkSetup, metrics: &ForkMetrics) -> Self {
        let mut kinds = vec![
            (Kind::Preloaded, setup.preload.clone()),
            (Kind::Bare, vec![]),
        ];
        if setup.preload.is_empty() {
            kinds.remove(0);
        }
        let (warnings, watching) = watch::channel(Vec::new());
        let board = Arc::new(WarningBoard {
            interpreter: setup.interpreter.clone(),
            bare_alone: kinds.len() == 1,
            reasons: std::sync::Mutex::default(),
            warnings,
        });
        let servers: Vec<Server> = kinds
            .iter()
            .map(|(kind, _)| Server {
                kind: *kind,
                interpreter: setup.interpreter.clone(),
                state: Mutex::new(State::Down),
                metrics: metrics.clone(),
                board: Arc::clone(&board),
            })
            .collect();
        for server in &servers {
            // Scraped as down from the start, not missing until it first starts.
            server.set(&mut State::Down, State::Down);
        }
        let servers = Arc::new(servers);
        for (index, (kind, preload)) in kinds.into_iter().enumerate() {
            let mut process = setup.process.clone();
            process.log_file = process
                .log_file
                .with_file_name(format!("fork-server-{}.log", kind.label()));
            let retry = setup.import_retry;
            tokio::spawn(keep_up(
                Arc::downgrade(&servers),
                index,
                process,
                preload,
                retry,
            ));
        }
        Self {
            servers,
            warnings: watching,
        }
    }

    /// The worker's warnings about these fork servers (one per disabled server), as they
    /// change.
    #[must_use]
    pub fn warnings(&self) -> watch::Receiver<Vec<pb::WorkerWarning>> {
        self.warnings.clone()
    }

    /// The disabled fork servers and why (for the worker's warnings).
    pub async fn disabled(&self) -> Vec<(Kind, String)> {
        let mut found = Vec::new();
        for server in self.servers.iter() {
            if let State::Disabled(reason) = &*server.state.lock().await {
                found.push((server.kind, reason.clone()));
            }
        }
        found
    }

    /// Forks a runner in `mode` for `spec` (its environment, directory, log file and output
    /// limit): from the preloaded server, else the bare one. Each one that couldn't is
    /// counted.
    ///
    /// # Errors
    ///
    /// The last server's [`Fallback`] when none forked it: the caller spawns it.
    pub async fn fork(&self, spec: &ProcessSpec, mode: RunnerMode) -> Result<Forked, Fallback> {
        let mut last = Fallback::Down;
        for server in self.servers.iter() {
            match fork_on(server, spec, mode).await {
                Ok(forked) => return Ok(forked),
                Err(fallback) => {
                    server
                        .metrics
                        .fallbacks
                        .get_or_create(&ForkFallbackLabels {
                            interpreter: server.interpreter.clone(),
                            reason: fallback.label().to_owned(),
                        })
                        .inc();
                    last = fallback;
                }
            }
        }
        Err(last)
    }
}

/// Keeps fork server `index` running while its [`ForkServers`] lives: starts it, restarts it
/// with backoff when it dies or can't start, retries a failing import for
/// `import_retry`, and stops for good once it is disabled otherwise.
async fn keep_up(
    servers: std::sync::Weak<Vec<Server>>,
    index: usize,
    process: ProcessSpec,
    preload: Vec<String>,
    import_retry: Duration,
) {
    let mut backoff = BACKOFF_FIRST / 2;
    // Since when its preload has been failing, and the reason last logged.
    let mut failing: Option<(Instant, String)> = None;
    loop {
        let up_for = match start_server(&process, &preload).await {
            Ok(Started::Ready(mut os, control)) => {
                {
                    let Some(all) = servers.upgrade() else {
                        return;
                    };
                    let server = &all[index];
                    let mut state = server.state.lock().await;
                    server.set(
                        &mut state,
                        State::Up {
                            control,
                            next_id: 1,
                            asking: false,
                        },
                    );
                    tracing::info!(kind = server.kind.label(), "fork server up");
                }
                failing = None;
                let up_at = Instant::now();
                let ended = os.wait().await;
                let Some(all) = servers.upgrade() else {
                    return;
                };
                let server = &all[index];
                let mut state = server.state.lock().await;
                if matches!(*state, State::Disabled(_)) {
                    return;
                }
                tracing::warn!(kind = server.kind.label(), ?ended, "fork server ended");
                server.set(&mut state, State::Down);
                Some(up_at.elapsed())
            }
            Ok(Started::Disabled(reason, cause)) => {
                let Some(all) = servers.upgrade() else {
                    return;
                };
                let server = &all[index];
                let kind = server.kind.label();
                let since = failing
                    .as_ref()
                    .map_or_else(Instant::now, |(since, _)| *since);
                let again = retried(cause, since.elapsed(), import_retry);
                if !again {
                    let gave_up = (cause == Cause::Import).then(|| since.elapsed().as_secs() / 60);
                    tracing::warn!(kind, reason, ?gave_up, "fork server disabled until restart");
                } else if failing.as_ref().is_none_or(|(_, said)| *said != reason) {
                    tracing::warn!(kind, reason, "fork server disabled; retrying");
                }
                let mut state = server.state.lock().await;
                server.set(&mut state, State::Disabled(reason.clone()));
                if !again {
                    return;
                }
                failing = Some((since, reason));
                None
            }
            Err(error) => {
                let Some(all) = servers.upgrade() else {
                    return;
                };
                let server = &all[index];
                tracing::warn!(kind = server.kind.label(), %error, "can't start the fork server");
                let mut state = server.state.lock().await;
                server.set(&mut state, State::Down);
                None
            }
        };
        backoff = next_backoff(backoff, up_for);
        tokio::time::sleep(backoff).await;
    }
}

/// How starting a fork server went.
enum Started {
    /// It preloaded and is single-threaded.
    Ready(Box<OsProcess>, UnixStream),
    /// It can't be used: why, and whether that may pass.
    Disabled(String, Cause),
}

/// Starts a fork server and has it preload `preload`.
async fn start_server(process: &ProcessSpec, preload: &[String]) -> Result<Started, LaunchError> {
    let runner = match start_runner(process, RunnerMode::ForkServer, |_| {}).await {
        Ok(runner) => runner,
        Err(LaunchError::Channel(ChannelError::Refused(reason))) => {
            return Ok(Started::Disabled(reason, Cause::Lasting));
        }
        Err(error) => return Err(error),
    };
    let (os, mut control) = (runner.process, runner.stream);
    let request = AgentMessage::ForkServer(ForkServerRequest {
        preload: preload.to_vec(),
    });
    crate::channel::write_message(&mut control, &request).await?;
    let answer = tokio::time::timeout(PRELOAD_TIMEOUT, read_message(&mut control)).await;
    match answer {
        Ok(Ok(RunnerMessage::ForkServerReady)) => Ok(Started::Ready(Box::new(os), control)),
        Ok(Ok(RunnerMessage::ForkServerDisabled { reason, threads })) => {
            let cause = if threads.is_empty() {
                Cause::Import
            } else {
                Cause::Lasting
            };
            Ok(Started::Disabled(reason, cause))
        }
        Ok(Ok(other)) => Err(ChannelError::Unexpected {
            expected: "fork_server_ready",
            got: format!("{other:?}"),
        }
        .into()),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(LaunchError::NoHello(PRELOAD_TIMEOUT)),
    }
}

/// A runner forked from a fork server, with the agent's ends of its sockets.
#[derive(Debug)]
pub struct Forked {
    /// The process (supervised like a spawned one).
    pub process: ForkedProcess,
    /// The agent's end of the runner's channel (it says hello on it next).
    pub stream: UnixStream,
    /// A copy of the status socket, holding the ended process: the caller drops it once
    /// nothing signals the group any more, whatever happened to `process` meanwhile.
    pub hold: GroupHold,
}

/// Asks `server` to fork a runner.
async fn fork_on(
    server: &Server,
    spec: &ProcessSpec,
    mode: RunnerMode,
) -> Result<Forked, Fallback> {
    let io = |_| Fallback::ForkFailed;
    let (ours, theirs) = std::os::unix::net::UnixStream::pair().map_err(io)?;
    let (status, status_theirs) = std::os::unix::net::UnixStream::pair().map_err(io)?;
    let (stdout, stdout_theirs) = std::io::pipe().map_err(io)?;
    let (stderr, stderr_theirs) = std::io::pipe().map_err(io)?;
    let log = open_log(spec).await.map_err(io)?;
    {
        let mut state = server.state.lock().await;
        let (control, id) = match &mut *state {
            State::Up { asking: true, .. } => {
                return Err(out_of_step(
                    server,
                    &mut state,
                    "an earlier answer was abandoned",
                ));
            }
            State::Up {
                control,
                next_id,
                asking,
            } => {
                let id = *next_id;
                *next_id += 1;
                *asking = true;
                (control, id)
            }
            State::Down => return Err(Fallback::Down),
            State::Disabled(_) => return Err(Fallback::Disabled),
        };
        let request = AgentMessage::Fork(ForkRequest {
            id,
            mode,
            env: spec.env.clone(),
            cwd: spec.working_dir.to_string_lossy().into_owned(),
        });
        let fds = [
            OwnedFd::from(theirs),
            OwnedFd::from(stdout_theirs),
            OwnedFd::from(stderr_theirs),
            OwnedFd::from(status_theirs),
        ];
        if send_with_fds(control, &request, &fds).await.is_err() {
            return Err(out_of_step(
                server,
                &mut state,
                "the request couldn't be sent",
            ));
        }
        let answer = tokio::time::timeout(FORK_ANSWER_TIMEOUT, read_message(control)).await;
        if let State::Up { asking, .. } = &mut *state {
            *asking = false;
        }
        answered(server, &mut state, id, answer)?;
        // Our copies of the process's ends go here: closed, so only the process holds them.
        drop(fds);
    }
    let hold = GroupHold {
        _open: OwnedFd::from(status.try_clone().map_err(io)?),
    };
    let mut status = tokio_stream(status).map_err(io)?;
    let started = tokio::time::timeout(RUNNER_START_TIMEOUT, read_message(&mut status)).await;
    let Ok(Ok(RunnerMessage::ProcessStarted { pid })) = started else {
        return Err(Fallback::ShimDied);
    };
    let group = Pid::from_raw(pid).ok_or(Fallback::ShimDied)?;
    let limit = spec.max_output_size;
    let stdout = pipe::Receiver::from_owned_fd(OwnedFd::from(stdout)).map_err(io)?;
    let stderr = pipe::Receiver::from_owned_fd(OwnedFd::from(stderr)).map_err(io)?;
    Ok(Forked {
        process: ForkedProcess {
            group,
            status: Some(status),
            ended: None,
            usage: Ended::default(),
            stdout: tokio::spawn(collect(stdout, Arc::clone(&log), limit)),
            stderr: tokio::spawn(collect(stderr, log, limit)),
        },
        stream: tokio_stream(ours).map_err(io)?,
        hold,
    })
}

fn tokio_stream(stream: std::os::unix::net::UnixStream) -> io::Result<UnixStream> {
    stream.set_nonblocking(true)?;
    UnixStream::from_std(stream)
}

/// What a fork server's answer to request `id` means: forked, a fallback, or the server let
/// go (disabled, or out of step).
fn answered(
    server: &Server,
    state: &mut State,
    id: u64,
    answer: Result<Result<RunnerMessage, ChannelError>, tokio::time::error::Elapsed>,
) -> Result<(), Fallback> {
    match answer {
        Ok(Ok(RunnerMessage::Forked { id: forked })) if forked == id => Ok(()),
        Ok(Ok(RunnerMessage::ForkFailed { id: failed, reason })) if failed == id => {
            tracing::warn!(kind = server.kind.label(), reason, "fork failed");
            Err(Fallback::ForkFailed)
        }
        Ok(Ok(RunnerMessage::ForkServerDisabled { reason, .. })) => {
            tracing::warn!(kind = server.kind.label(), reason, "fork server disabled");
            // Dropping its control socket ends it; its supervisor stops for good.
            server.set(state, State::Disabled(reason));
            Err(Fallback::Disabled)
        }
        Ok(Ok(other)) => {
            let why = format!("an answer that isn't this request's: {other:?}");
            Err(out_of_step(server, state, &why))
        }
        Ok(Err(error)) => {
            let why = format!("reading the answer: {error}");
            Err(out_of_step(server, state, &why))
        }
        Err(_) => Err(out_of_step(server, state, "no answer in time")),
    }
}

/// A fork server whose control socket no longer pairs answers with requests (an answer
/// abandoned, late, or not this request's; or a request half sent): it is let go as down.
/// Dropping the socket ends it, and its supervisor restarts it with backoff.
fn out_of_step(server: &Server, state: &mut State, why: &str) -> Fallback {
    tracing::warn!(
        kind = server.kind.label(),
        why,
        "fork server out of step; restarting it"
    );
    server.set(state, State::Down);
    Fallback::Down
}

/// Sends `message` with `fds` on its first bytes (`SCM_RIGHTS`), in the channel's framing.
async fn send_with_fds(
    stream: &mut UnixStream,
    message: &AgentMessage,
    fds: &[OwnedFd],
) -> Result<(), ChannelError> {
    let body = serde_json::to_vec(message)?;
    let size = u32::try_from(body.len())
        .ok()
        .filter(|size| *size <= MAX_MESSAGE_SIZE)
        .ok_or(ChannelError::TooLarge(body.len() as u64))?;
    let header = size.to_be_bytes();
    let borrowed: Vec<_> = fds.iter().map(AsFd::as_fd).collect();
    let mut sent = 0;
    while sent == 0 {
        stream.writable().await?;
        let result = stream.try_io(tokio::io::Interest::WRITABLE, || {
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(4))];
            let mut control = SendAncillaryBuffer::new(&mut space);
            control.push(SendAncillaryMessage::ScmRights(&borrowed));
            rustix::net::sendmsg(
                &*stream,
                &[IoSlice::new(&header)],
                &mut control,
                SendFlags::NOSIGNAL,
            )
            .map_err(io::Error::from)
        });
        match result {
            Ok(n) => sent = n,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
    }
    // The descriptors went with the first bytes; the rest is plain.
    stream.write_all(&header[sent..]).await?;
    stream.write_all(&body).await?;
    Ok(())
}

/// A copy of the agent's end of a forked process's status socket: while any copy is open, the
/// shim keeps the ended process unreaped, so its PID and group stay its own. Dropped once
/// nothing may signal the group any more (after the memory watchdog stopped), on every path.
#[derive(Debug)]
pub struct GroupHold {
    _open: OwnedFd,
}

/// A forked runner process: its group, its shim's status socket, and its output.
#[derive(Debug)]
pub struct ForkedProcess {
    group: Pid,
    status: Option<UnixStream>,
    ended: Option<Termination>,
    usage: Ended,
    stdout: JoinHandle<io::Result<TailBuffer>>,
    stderr: JoinHandle<io::Result<TailBuffer>>,
}

impl ForkedProcess {
    /// Its process ID, which is also its process group's ID.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.group.as_raw_nonzero().get()
    }

    /// The captured stdout and stderr, once both pipes are closed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error from reading a pipe or writing the log file.
    pub async fn output(self) -> io::Result<(TailBuffer, TailBuffer)> {
        let stdout = self.stdout.await.map_err(io::Error::other)??;
        let stderr = self.stderr.await.map_err(io::Error::other)??;
        Ok((stdout, stderr))
    }
}

impl Process for ForkedProcess {
    async fn wait(&mut self) -> io::Result<Termination> {
        if let Some(ended) = self.ended {
            return Ok(ended);
        }
        let Some(status) = &mut self.status else {
            return Ok(Termination::Unknown);
        };
        let ended = loop {
            match read_message(status).await {
                Ok(RunnerMessage::ProcessExited {
                    code,
                    signal,
                    core_dumped,
                    usage,
                }) => {
                    self.usage = Ended { usage, core_dumped };
                    break match (code, signal) {
                        (Some(code), _) => Termination::Exited(code),
                        (None, Some(signal)) => Termination::Signaled(signal),
                        (None, None) => Termination::Unknown,
                    };
                }
                Ok(_) => {}
                Err(_) => {
                    // The shim is gone: no one will report or reap the process. Stop
                    // whatever is left of its group.
                    let _ = kill_process_group(self.group, rustix::process::Signal::KILL);
                    break Termination::Unknown;
                }
            }
        };
        self.ended = Some(ended);
        Ok(ended)
    }

    fn signal(&mut self, signal: Signal) -> io::Result<()> {
        let sig = match signal {
            Signal::Term => rustix::process::Signal::TERM,
            Signal::Kill => rustix::process::Signal::KILL,
        };
        match kill_process_group(self.group, sig) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn ended(&self) -> Ended {
        self.usage
    }
}
