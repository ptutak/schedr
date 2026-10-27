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
use std::sync::PoisonError;
use std::time::{Duration, Instant};

use prometheus_client::metrics::counter::Counter;
use schedr_model::runner::{AgentMessage, ParseRequest, RunnerMessage, RunnerMode, TaskRequest};
use tokio::net::UnixStream;

use crate::attempt::{Ran, StartTimes};
use crate::channel::{ChannelError, accept_runner, read_message, write_message};
use crate::fork::{ForkServers, Forked, ForkedProcess, GroupHold};
use crate::memory::{GroupWatch, MemoryWatch, SharedSeen, over_budget_message, watch_group};
use crate::output::TailBuffer;
use crate::parse::{ParseRunner, ParseSession};
use crate::process::{OsProcess, ProcessSpec, spawn, spawn_passing};
use crate::status::Termination;
use crate::supervise::{Limits, Process, Signal, Stops, supervise};

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
    let mut stream = UnixStream::from_std(ours).map_err(|e| start("the runner's socket", e))?;
    match tokio::time::timeout(
        RUNNER_START_TIMEOUT,
        accept_runner(&mut stream, AGENT_VERSION),
    )
    .await
    {
        Ok(Ok(said)) if said == mode => {}
        Ok(Ok(said)) => {
            abandon(process).await;
            return Err(LaunchError::WrongMode {
                expected: mode,
                got: said,
            });
        }
        Ok(Err(error)) => {
            abandon(process).await;
            return Err(error.into());
        }
        Err(_) => {
            abandon(process).await;
            return Err(LaunchError::NoHello(RUNNER_START_TIMEOUT));
        }
    }
    Ok(Runner {
        process,
        stream,
        hello_after: starting.elapsed(),
    })
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
}

/// Starts a Python task's runner, forked by `forks` if they can, else spawned, and completes
/// the handshake; `spawned` gets its process ID (and whether it was forked) as soon as it
/// runs, and `hold` a forked one's [`GroupHold`]. A forked runner that fails the handshake
/// isn't spawned again: it ran.
async fn start_task_runner(
    spec: &ProcessSpec,
    forks: Option<&ForkServers>,
    hold: &mut Option<GroupHold>,
    spawned: impl FnOnce(i32, bool),
) -> Result<(TaskProcess, UnixStream, Duration), LaunchError> {
    let starting = Instant::now();
    if let Some(forks) = forks
        && let Ok(Forked {
            process,
            mut stream,
            hold: held,
        }) = forks.fork(spec, RunnerMode::Task).await
    {
        *hold = Some(held);
        spawned(process.id(), true);
        let mut process = TaskProcess::Forked(process);
        let said = tokio::time::timeout(
            RUNNER_START_TIMEOUT,
            accept_runner(&mut stream, AGENT_VERSION),
        )
        .await;
        let error = match said {
            Ok(Ok(RunnerMode::Task)) => return Ok((process, stream, starting.elapsed())),
            Ok(Ok(said)) => LaunchError::WrongMode {
                expected: RunnerMode::Task,
                got: said,
            },
            Ok(Err(error)) => error.into(),
            Err(_) => LaunchError::NoHello(RUNNER_START_TIMEOUT),
        };
        let _ = process.signal(Signal::Kill);
        let _ = process.wait().await;
        let _ = tokio::time::timeout(OUTPUT_DRAIN_TIMEOUT, process.output()).await;
        return Err(error);
    }
    let runner = start_runner(spec, RunnerMode::Task, |pid| spawned(pid, false)).await?;
    Ok((
        TaskProcess::Spawned(runner.process),
        runner.stream,
        runner.hello_after,
    ))
}

/// Runs a Python task: starts the runner (forked by `forks` when there are any and they can),
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
    request: TaskRequest,
    limits: Limits,
    stop: Stops,
    forks: Option<&ForkServers>,
    hold: &mut Option<GroupHold>,
    spawned: impl FnOnce(i32, bool),
) -> Result<Ran, LaunchError> {
    let started = Instant::now();
    let (mut process, mut stream, hello_after) =
        start_task_runner(process, forks, hold, spawned).await?;
    let forked = matches!(process, TaskProcess::Forked(_));
    write_message(&mut stream, &AgentMessage::Task(Box::new(request))).await?;
    let sent = Instant::now();
    let result = async {
        let mut import = None;
        loop {
            let message: Result<RunnerMessage, ChannelError> = read_message(&mut stream).await;
            match message {
                // The first one counts; a repeat isn't the result.
                Ok(RunnerMessage::TaskImported) => {
                    import.get_or_insert_with(|| sent.elapsed());
                }
                message => return (message.ok(), import),
            }
        }
    };
    let ((runner_result, import), supervised) =
        tokio::join!(result, supervise(&mut process, limits, stop));
    let supervised = supervised.map_err(LaunchError::Supervise)?;
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
    })
}

/// Runs a command process under supervision, keeping the tails of its output; `spawned` gets
/// its process ID as soon as it runs.
///
/// # Errors
///
/// [`LaunchError`] if it can't be started or supervised.
pub async fn run_command(
    spec: &ProcessSpec,
    limits: Limits,
    stop: Stops,
    spawned: impl FnOnce(i32),
) -> Result<Ran, LaunchError> {
    let started = Instant::now();
    let mut process = spawn(spec).await.map_err(|source| LaunchError::Start {
        what: spec.program.display().to_string(),
        source,
    })?;
    spawned(process.id());
    let supervised = supervise(&mut process, limits, stop)
        .await
        .map_err(LaunchError::Supervise)?;
    let output = process.output().await.map_err(LaunchError::Supervise)?;
    Ok(Ran {
        supervised,
        runner_result: None,
        output: Some(output),
        start: None,
        duration: started.elapsed(),
        forked: false,
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
}

/// A parse runner that connected and got its request.
#[derive(Debug)]
pub struct ProcessParseSession {
    runner: Runner,
    /// The watchdog, killing the runner's group over its budget (stops when dropped).
    _watching: Option<GroupWatch>,
    seen: SharedSeen,
    budget: Option<u64>,
}

impl ParseSession for ProcessParseSession {
    async fn next(&mut self) -> Result<RunnerMessage, ChannelError> {
        let message = read_message(&mut self.runner.stream).await;
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
        abandon(self.runner.process).await;
    }
}

impl ParseRunner for ProcessParseRunner {
    type Session = ProcessParseSession;

    async fn start(&mut self, request: ParseRequest) -> Result<ProcessParseSession, ChannelError> {
        let seen = SharedSeen::default();
        let mut watching = None;
        let mut runner = start_runner(&self.process, RunnerMode::Parse, |pgid| {
            // Told through `seen`, read when the runner's connection breaks (no one listens to
            // its reason).
            watching = Some(watch_group(
                self.memory.clone(),
                pgid,
                seen.clone(),
                Stops::new().0,
                self.kills.clone(),
                false,
            ));
        })
        .await
        .map_err(|e| match e {
            LaunchError::Channel(channel) => channel,
            other => ChannelError::Io(std::io::Error::other(other.to_string())),
        })?;
        write_message(&mut runner.stream, &AgentMessage::Parse(request)).await?;
        Ok(ProcessParseSession {
            runner,
            _watching: watching,
            seen,
            budget: self.memory.budget,
        })
    }
}
