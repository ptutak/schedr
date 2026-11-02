//! Starting real processes for attempts and parse jobs: the runner with its end of a fresh
//! socket pair, commands as they are, each supervised (timeout, cancel, SIGTERM then SIGKILL).
//!
//! A runner gets its channel to the agent as an **inherited, already connected socket**
//! (`SCHEDR_AGENT_FD`), not a path to connect to: no socket file, no directory, no path-length
//! limit, and no other process can connect (see PROJECT.md, Rust/Python integration).
//!
//! The thin operating-system layer under [`crate::agent`], like [`crate::process`]: exercised
//! by the integration tests (`tests/runner.rs`); the decisions around it are unit-tested in
//! [`crate::attempt`], [`crate::parse`] and [`crate::supervise`].

use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use prometheus_client::metrics::counter::Counter;
use schedr_model::SecretName;
use schedr_model::ctx::CTX_FD_ENV;
use schedr_model::runner::{AgentMessage, ParseRequest, RunnerMessage, RunnerMode, TaskRequest};
use tokio::net::UnixStream;

use crate::attempt::{Ran, StartTimes};
use crate::channel::{ChannelError, accept_runner, read_message, write_message};
use crate::ctx::CtxInfo;
use crate::fork::{ForkServers, Forked, ForkedProcess, GroupHold};
use crate::memory::{GroupWatch, MemoryWatch, SharedSeen, over_budget_message, watch_group};
use crate::output::TailBuffer;
use crate::parse::{ParseRunner, ParseSession};
use crate::process::{OsProcess, ProcessSpec, spawn, spawn_passing};
use crate::secrets::{AttemptSecrets, SecretError};
use crate::status::{StopReason, Termination};
use crate::supervise::{Limits, Process, Signal, Stops, Supervised, supervise};
use crate::usage::Ended;

/// The environment variable with the runner's descriptor of its socket to the agent.
pub const FD_ENV: &str = "SCHEDR_AGENT_FD";

/// How long a started runner has to say hello.
pub const RUNNER_START_TIMEOUT: Duration = Duration::from_secs(30);

/// The version runners must have (the same `schedr` package as the agent).
pub const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Why a process couldn't be started or talked to.
#[derive(Debug, thiserror::Error)]
pub enum LaunchError {
    /// Starting it, its socket or its log file failed.
    #[error("can't start {what}: {source}")]
    Start {
        /// What was being started.
        what: String,
        /// The error.
        source: std::io::Error,
    },
    /// The runner didn't say hello in time.
    #[error("the runner didn't say hello within {0:?}")]
    NoHello(Duration),
    /// The runner connected in another mode than it was started in.
    #[error("the runner said it is in {got:?} mode, expected {expected:?}")]
    WrongMode {
        /// The mode it was started in.
        expected: RunnerMode,
        /// The mode it said.
        got: RunnerMode,
    },
    /// Talking to the runner failed.
    #[error(transparent)]
    Channel(#[from] ChannelError),
    /// Waiting for or signalling the process failed.
    #[error("supervising the process: {0}")]
    Supervise(std::io::Error),
}

/// A runner that connected and said hello.
#[derive(Debug)]
pub struct Runner {
    /// Its process.
    pub process: OsProcess,
    /// Its socket.
    pub stream: UnixStream,
    /// From starting its process to its hello.
    pub hello_after: Duration,
}

/// Starts the runner described by `spec` in `mode`, handing it its end of a new socket pair,
/// and completes the handshake; `spawned` gets its process ID as soon as it runs.
///
/// # Errors
///
/// [`LaunchError`] if it can't be started, doesn't say hello in time, or is in another mode or
/// version (it was refused then).
pub async fn start_runner(
    spec: &ProcessSpec,
    mode: RunnerMode,
    spawned: impl FnOnce(i32),
) -> Result<Runner, LaunchError> {
    let (process, mut stream, starting) = spawn_runner(spec, spawned).await?;
    match handshake(&mut stream, mode, None).await {
        Handshake::Said(Ok(())) => Ok(Runner {
            process,
            stream,
            hello_after: starting.elapsed(),
        }),
        Handshake::Said(Err(error)) => {
            abandon(process).await;
            Err(error)
        }
        // Without stops, the handshake isn't cut short.
        Handshake::Stopped(_) => unreachable!("a handshake without stops isn't stopped"),
    }
}

/// Starts the runner described by `spec`, handing it its end of a new socket pair; `spawned`
/// gets its process ID as soon as it runs. Returns the process, the agent's end of the
/// socket, and when it was started.
async fn spawn_runner(
    spec: &ProcessSpec,
    spawned: impl FnOnce(i32),
) -> Result<(OsProcess, UnixStream, Instant), LaunchError> {
    let start = |what: &str, source| LaunchError::Start {
        what: what.to_owned(),
        source,
    };
    let (ours, theirs) =
        std::os::unix::net::UnixStream::pair().map_err(|e| start("the runner's socket", e))?;
    let mut spec = spec.clone();
    spec.env
        .insert(FD_ENV.to_owned(), theirs.as_raw_fd().to_string());
    let starting = Instant::now();
    let process = spawn_passing(&spec, Some(OwnedFd::from(theirs)))
        .await
        .map_err(|e| start("the runner", e))?;
    spawned(process.id());
    ours.set_nonblocking(true)
        .map_err(|e| start("the runner's socket", e))?;
    let stream = UnixStream::from_std(ours).map_err(|e| start("the runner's socket", e))?;
    Ok((process, stream, starting))
}

/// How a runner's handshake ended.
enum Handshake {
    /// It said hello in `mode` (`Ok`), or didn't in time, or said something else.
    Said(Result<(), LaunchError>),
    /// A reason to stop the attempt came first (B56): its cancel, or its deadline passing.
    Stopped(StopReason),
}

/// Waits for the runner's hello in `mode`, for at most [`RUNNER_START_TIMEOUT`]. With
/// `stops`, an attempt's reasons to stop and its deadline (`None` for none) are raced
/// against it: the first to come ends the wait.
async fn handshake(
    stream: &mut UnixStream,
    mode: RunnerMode,
    stops: Option<(&mut Stops, Option<tokio::time::Instant>)>,
) -> Handshake {
    let hello = async {
        match tokio::time::timeout(RUNNER_START_TIMEOUT, accept_runner(stream, AGENT_VERSION)).await
        {
            Ok(Ok(said)) if said == mode => Ok(()),
            Ok(Ok(said)) => Err(LaunchError::WrongMode {
                expected: mode,
                got: said,
            }),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(LaunchError::NoHello(RUNNER_START_TIMEOUT)),
        }
    };
    match unless_stopped(hello, stops).await {
        Ok(said) => Handshake::Said(said),
        Err(reason) => Handshake::Stopped(reason),
    }
}

/// `work`'s result, unless a reason in `stops` or their deadline comes first (reasons first,
/// as in `supervise`); `work` is then dropped. Without `stops`, `work` alone.
async fn unless_stopped<T>(
    work: impl Future<Output = T>,
    stops: Option<(&mut Stops, Option<tokio::time::Instant>)>,
) -> Result<T, StopReason> {
    let Some((stops, deadline)) = stops else {
        return Ok(work.await);
    };
    let deadline = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        reason = stops.next_reason() => Err(reason),
        () = deadline => Err(StopReason::TimedOut),
        done = work => Ok(done),
    }
}

/// How long a stopped runner's output may take to reach its log. A process it started in a
/// session of its own outlives the kill and can hold the pipes open; the log is then left
/// as it is rather than waiting forever.
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Stops a runner (one that failed to start, or a finished parse session) and waits until
/// what it printed is in its log (the caller may show it), for at most
/// [`OUTPUT_DRAIN_TIMEOUT`].
async fn abandon(mut process: OsProcess) {
    let _ = process.signal(Signal::Kill);
    let _ = process.wait().await;
    let _ = tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, process.output()).await;
}

/// A task's runner process: spawned, or forked from a fork server.
#[derive(Debug)]
pub enum TaskProcess {
    /// A child of the agent.
    Spawned(OsProcess),
    /// A child of a fork server's shim.
    Forked(ForkedProcess),
}

impl TaskProcess {
    /// The captured stdout and stderr, once both pipes are closed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error from reading a pipe or writing the log file.
    pub async fn output(self) -> std::io::Result<(TailBuffer, TailBuffer)> {
        match self {
            Self::Spawned(process) => process.output().await,
            Self::Forked(process) => process.output().await,
        }
    }
}

impl Process for TaskProcess {
    async fn wait(&mut self) -> std::io::Result<Termination> {
        match self {
            Self::Spawned(process) => process.wait().await,
            Self::Forked(process) => process.wait().await,
        }
    }

    fn signal(&mut self, signal: Signal) -> std::io::Result<()> {
        match self {
            Self::Spawned(process) => process.signal(signal),
            Self::Forked(process) => process.signal(signal),
        }
    }

    fn group_alive(&self) -> bool {
        match self {
            Self::Spawned(process) => process.group_alive(),
            Self::Forked(process) => process.group_alive(),
        }
    }

    fn ended(&self) -> crate::usage::Ended {
        match self {
            Self::Spawned(process) => process.ended(),
            Self::Forked(process) => process.ended(),
        }
    }
}

/// A task's runner as its start left it.
enum Start {
    /// It said hello: its process, its socket, and how long it took.
    Ready(TaskProcess, UnixStream, Duration),
    /// A reason to stop the attempt came first (B56): the process still runs, for
    /// `supervise` to stop.
    Stopped(TaskProcess, StopReason),
    /// A reason to stop came while waiting for a fork server: no runner was started.
    NotStarted(StopReason),
}

/// Starts a Python task's runner, forked by `forks` if they can (from the preloaded server
/// only with `preload`), else spawned, and completes the handshake, unless a reason in `stops`
/// or their deadline comes first (an attempt's; a parse runner has none); `spawned` gets its process ID (and whether it was forked) as
/// soon as it runs, and `hold` a forked one's [`GroupHold`]. A forked runner that fails the
/// handshake isn't spawned again: it ran.
async fn start_task_runner(
    spec: &ProcessSpec,
    mode: RunnerMode,
    forks: Option<&ForkServers>,
    preload: bool,
    hold: &mut Option<GroupHold>,
    stops: Option<(&mut Stops, Option<tokio::time::Instant>)>,
    spawned: impl FnOnce(i32, bool),
) -> Result<Start, LaunchError> {
    let starting = Instant::now();
    let mut stops = stops;
    // Waiting for a fork server (another runner's fork may hold it) is part of the start
    // too (B56): a fork abandoned midway is the fork server's to recover from (out of step,
    // restarted), and its runner, its channel closed, ends.
    let forked = match forks {
        Some(forks) => {
            let again = stops
                .as_mut()
                .map(|(stops, deadline)| (&mut **stops, *deadline));
            match unless_stopped(forks.fork(spec, mode, preload), again).await {
                Ok(forked) => forked.ok(),
                Err(reason) => return Ok(Start::NotStarted(reason)),
            }
        }
        None => None,
    };
    let (mut process, mut stream) = if let Some(Forked {
        process,
        stream,
        hold: held,
    }) = forked
    {
        *hold = Some(held);
        spawned(process.id(), true);
        (TaskProcess::Forked(process), stream)
    } else {
        let (process, stream, _) = spawn_runner(spec, |pid| spawned(pid, false)).await?;
        (TaskProcess::Spawned(process), stream)
    };
    let error = match handshake(&mut stream, mode, stops).await {
        Handshake::Said(Ok(())) => return Ok(Start::Ready(process, stream, starting.elapsed())),
        Handshake::Stopped(reason) => return Ok(Start::Stopped(process, reason)),
        Handshake::Said(Err(error)) => error,
    };
    let _ = process.signal(Signal::Kill);
    let _ = process.wait().await;
    let _ = tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, process.output()).await;
    Err(error)
}

/// A Python task to run: the runner's request, and the attempt's secrets (it may read one
/// under a declared prefix while it runs).
#[derive(Debug)]
pub struct PythonTask {
    /// The request.
    pub request: TaskRequest,
    /// The attempt's secrets; none for a task without any.
    pub secrets: Option<Arc<AttemptSecrets>>,
}

impl From<TaskRequest> for PythonTask {
    fn from(request: TaskRequest) -> Self {
        Self {
            request,
            secrets: None,
        }
    }
}

/// Sends the task request and serves the runner until its result: its import time, and the
/// secrets it reads (`task_ctx.secret`). Returns the result (`None` if the channel broke)
/// and how long the import took, if it said.
async fn task_conversation(
    stream: &mut UnixStream,
    request: TaskRequest,
    secrets: Option<&AttemptSecrets>,
) -> (Option<RunnerMessage>, Option<Duration>) {
    let request = AgentMessage::Task(Box::new(request));
    if write_message(stream, &request).await.is_err() {
        return (None, None);
    }
    let sent = Instant::now();
    let mut import = None;
    loop {
        let message: Result<RunnerMessage, ChannelError> = read_message(stream).await;
        match message {
            // The first one counts; a repeat isn't the result.
            Ok(RunnerMessage::TaskImported) => {
                import.get_or_insert_with(|| sent.elapsed());
            }
            // Served in turn: the runner waits for each answer.
            Ok(RunnerMessage::SecretGet { secret }) => {
                let answer = secret_answer(secrets, &secret).await;
                if write_message(stream, &answer).await.is_err() {
                    return (None, import);
                }
            }
            message => return (message.ok(), import),
        }
    }
}

/// Runs a Python task: starts the runner (forked by `forks` when there are any and they can,
/// unless the task asks for a fresh process; only by the bare server with `preload` off),
/// sends the request, and waits for its result while supervising the process; `spawned` gets
/// the runner's process ID, and whether it was forked, as soon as it runs. A forked process's
/// [`GroupHold`] goes to `hold`, on every path (the caller drops it after its watchdog).
///
/// # Errors
///
/// [`LaunchError`] if the runner can't be started or supervised; a runner that dies or sends
/// nothing still gives a [`Ran`] (without a result).
pub async fn run_python(
    process: &ProcessSpec,
    task: impl Into<PythonTask>,
    limits: Limits,
    mut stop: Stops,
    forks: Option<&ForkServers>,
    hold: &mut Option<GroupHold>,
    spawned: impl FnOnce(i32, bool),
) -> Result<Ran, LaunchError> {
    let PythonTask { request, secrets } = task.into();
    let started = Instant::now();
    // The timeout counts from here: the runner's start is part of the attempt (B56).
    let deadline = limits
        .timeout
        .map(|timeout| tokio::time::Instant::from_std(started) + timeout);
    // The task's own choice: never forked (`fresh_process`), or not from the preloaded server.
    let settings = &request.task.settings;
    let forks = forks.filter(|_| !settings.fresh_process);
    let preload = settings.preload;
    let stops = Some((&mut stop, deadline));
    let start = start_task_runner(
        process,
        RunnerMode::Task,
        forks,
        preload,
        hold,
        stops,
        spawned,
    );
    let (mut process, mut stream, hello_after) = match start.await? {
        Start::Ready(process, stream, hello_after) => (process, stream, hello_after),
        // Cancelled or timed out before its hello: stopped as a running task is, the reason
        // given back for `supervise` to take first; no request was sent, so no result.
        Start::NotStarted(reason) => {
            return Ok(Ran {
                supervised: Supervised {
                    termination: Termination::Unknown,
                    stopped: Some(reason),
                    killed: false,
                },
                runner_result: None,
                output: None,
                duration: started.elapsed(),
                start: None,
                forked: false,
                ended: Ended::default(),
            });
        }
        Start::Stopped(mut process, reason) => {
            let forked = matches!(process, TaskProcess::Forked(_));
            stop.give_back(reason);
            let supervised = supervise(&mut process, limits, stop)
                .await
                .map_err(LaunchError::Supervise)?;
            let ended = process.ended();
            // A process it started in a session of its own may hold the pipes open.
            let _ = tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, process.output()).await;
            return Ok(Ran {
                supervised,
                runner_result: None,
                output: None,
                duration: started.elapsed(),
                start: None,
                forked,
                ended,
            });
        }
    };
    let limits = Limits {
        timeout: deadline
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now())),
        ..limits
    };
    let forked = matches!(process, TaskProcess::Forked(_));
    // Sent while supervised: a runner that doesn't read is stopped on time all the same (B56).
    let result = task_conversation(&mut stream, request, secrets.as_deref());
    let ((runner_result, import), supervised) = {
        // Boxed: the attempt's future stays small.
        let mut result = Box::pin(result);
        let mut supervised = Box::pin(supervise(&mut process, limits, stop));
        // Once the process has ended, its result is in the socket already: a conversation still
        // waiting (on a slow secret storage) is given up after a moment, not awaited.
        tokio::select! {
            conversation = &mut result => (conversation, supervised.await),
            supervised = &mut supervised => {
                let conversation = tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, result)
                    .await
                    .unwrap_or((None, None));
                (conversation, supervised)
            }
        }
    };
    let supervised = supervised.map_err(LaunchError::Supervise)?;
    let ended = process.ended();
    let _ = process.output().await;
    Ok(Ran {
        supervised,
        runner_result,
        output: None,
        duration: started.elapsed(),
        start: Some(StartTimes {
            runner: hello_after,
            import,
        }),
        forked,
        ended,
    })
}

/// Runs a command process under supervision, keeping the tails of its output; `spawned` gets
/// its process ID as soon as it runs. With `ctx`, the process (and every process it starts)
/// gets a context channel (`SCHEDR_CTX_FD`, see [`crate::ctx`]), served until it ends.
///
/// # Errors
///
/// [`LaunchError`] if it can't be started or supervised.
pub async fn run_command(
    spec: &ProcessSpec,
    ctx: Option<CtxInfo>,
    limits: Limits,
    stop: Stops,
    spawned: impl FnOnce(i32),
) -> Result<Ran, LaunchError> {
    let started = Instant::now();
    let start = |source| LaunchError::Start {
        what: spec.program.display().to_string(),
        source,
    };
    let (mut process, _serving) = match ctx {
        Some(info) => {
            let (ours, theirs) = crate::ctx::channel().map_err(start)?;
            let mut spec = spec.clone();
            spec.env
                .insert(CTX_FD_ENV.to_owned(), theirs.as_raw_fd().to_string());
            let serving = crate::ctx::serve(ours, info);
            let process = spawn_passing(&spec, Some(theirs)).await.map_err(start)?;
            (process, Some(serving))
        }
        None => (spawn(spec).await.map_err(start)?, None),
    };
    spawned(process.id());
    let supervised = supervise(&mut process, limits, stop)
        .await
        .map_err(LaunchError::Supervise)?;
    let ended = process.ended();
    let output = process.output().await.map_err(LaunchError::Supervise)?;
    Ok(Ran {
        supervised,
        runner_result: None,
        output: Some(output),
        start: None,
        duration: started.elapsed(),
        forked: false,
        ended,
    })
}

/// Starts runners in parse mode for one root (see [`crate::parse::parse_files`]).
#[derive(Debug, Clone)]
pub struct ProcessParseRunner {
    /// The runner process (interpreter, `-m schedr.runner parse`, environment, log file).
    pub process: ProcessSpec,
    /// How the memory watchdog watches it (each runner, like a task).
    pub memory: MemoryWatch,
    /// Where the watchdog's kills are counted.
    pub kills: Counter,
    /// The DAG interpreter's fork servers: a parse job forks from them like a task (a fresh
    /// child per job), spawned when they can't.
    pub forks: Option<ForkServers>,
}

/// A parse runner that connected and got its request.
#[derive(Debug)]
pub struct ProcessParseSession {
    process: TaskProcess,
    stream: UnixStream,
    /// The watchdog, killing the runner's group over its budget (stops when dropped).
    watching: Option<GroupWatch>,
    /// A forked runner's hold, let go after the watchdog (see [`GroupHold`]).
    hold: Option<GroupHold>,
    seen: SharedSeen,
    budget: Option<u64>,
}

impl ParseSession for ProcessParseSession {
    async fn next(&mut self) -> Result<RunnerMessage, ChannelError> {
        let message = read_message(&mut self.stream).await;
        let over = self
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .over;
        match (message, over, self.budget) {
            (Err(_), Some(used), Some(budget)) => {
                Err(ChannelError::OverMemory(over_budget_message(budget, used)))
            }
            (message, _, _) => message,
        }
    }

    async fn stop(self) {
        let Self {
            mut process,
            watching,
            hold,
            ..
        } = self;
        let _ = process.signal(Signal::Kill);
        let _ = process.wait().await;
        let _ = tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, process.output()).await;
        // The watchdog stops first; then the hold goes, and a fork server's shim reaps it.
        drop(watching);
        drop(hold);
    }
}

impl ParseRunner for ProcessParseRunner {
    type Session = ProcessParseSession;

    async fn start(&mut self, request: ParseRequest) -> Result<ProcessParseSession, ChannelError> {
        let seen = SharedSeen::default();
        // Declared before the watch, so it is dropped after it on any path (locals go in
        // reverse order), a cancelled start included.
        let mut hold = None;
        let mut watching = None;
        let forks = self.forks.as_ref();
        let started = start_task_runner(
            &self.process,
            RunnerMode::Parse,
            forks,
            true,
            &mut hold,
            None,
            |pgid, forked| {
                // Told through `seen`, read when the runner's connection breaks (no one listens
                // to its reason).
                watching = Some(watch_group(
                    self.memory.clone(),
                    pgid,
                    seen.clone(),
                    Stops::new().0,
                    self.kills.clone(),
                    forked,
                ));
            },
        )
        .await;
        let (process, stream) = match started {
            Ok(Start::Ready(process, stream, _)) => (process, stream),
            // Without stops, the handshake isn't cut short.
            Ok(Start::Stopped(..) | Start::NotStarted(_)) => {
                unreachable!("a parse runner's start has no stops")
            }
            Err(error) => {
                drop(watching);
                drop(hold);
                return Err(match error {
                    LaunchError::Channel(channel) => channel,
                    other => ChannelError::Io(std::io::Error::other(other.to_string())),
                });
            }
        };
        let mut session = ProcessParseSession {
            process,
            stream,
            watching,
            hold,
            seen,
            budget: self.memory.budget,
        };
        if let Err(error) = write_message(&mut session.stream, &AgentMessage::Parse(request)).await
        {
            session.stop().await;
            return Err(error);
        }
        Ok(session)
    }
}

/// The answer to a running Python task's `SecretGet`.
async fn secret_answer(secrets: Option<&AttemptSecrets>, secret: &SecretName) -> AgentMessage {
    let read = match secrets {
        Some(secrets) => secrets.read(secret).await,
        None => Err(SecretError::Denied(format!(
            "`{secret}` isn't declared in the task's `secrets=`"
        ))),
    };
    match read {
        Ok(value) => AgentMessage::Secret { value },
        Err(error) => AgentMessage::SecretFailed {
            kind: error.kind().to_owned(),
            message: error.to_string(),
        },
    }
}
