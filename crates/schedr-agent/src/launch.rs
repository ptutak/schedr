//! Starting real processes for attempts and parse jobs: the runner with its end of a fresh
//! socket pair, commands as they are, each supervised (timeout, cancel, SIGTERM then SIGKILL).
//!
//! A runner gets its channel to the agent as an **inherited, already connected socket**
//! (`SCHEDR_AGENT_FD`), not a path to connect to: no socket file, no directory, no path-length
//! limit, and no other process can connect (see PROJECT.md, Rust/Python integration).
//!
//! The thin operating-system layer under [`crate::agent`], like [`crate::process`]: exercised
//! by the integration tests (milestone 1.17); the decisions around it are unit-tested in
//! [`crate::attempt`], [`crate::parse`] and [`crate::supervise`].

use std::os::fd::{AsRawFd, OwnedFd};
use std::time::{Duration, Instant};

use schedr_model::runner::{AgentMessage, ParseRequest, RunnerMessage, RunnerMode, TaskRequest};
use tokio::net::UnixStream;

use crate::attempt::Ran;
use crate::channel::{ChannelError, accept_runner, read_message, write_message};
use crate::parse::{ParseRunner, ParseSession};
use crate::process::{OsProcess, ProcessSpec, spawn, spawn_passing};
use crate::status::StopReason;
use crate::supervise::{Limits, Process, Signal, supervise};

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
}

/// Starts the runner described by `spec` in `mode`, handing it its end of a new socket pair,
/// and completes the handshake.
///
/// # Errors
///
/// [`LaunchError`] if it can't be started, doesn't say hello in time, or is in another mode or
/// version (it was refused then).
pub async fn start_runner(spec: &ProcessSpec, mode: RunnerMode) -> Result<Runner, LaunchError> {
    let start = |what: &str, source| LaunchError::Start {
        what: what.to_owned(),
        source,
    };
    let (ours, theirs) =
        std::os::unix::net::UnixStream::pair().map_err(|e| start("the runner's socket", e))?;
    let mut spec = spec.clone();
    spec.env
        .insert(FD_ENV.to_owned(), theirs.as_raw_fd().to_string());
    let process = spawn_passing(&spec, Some(OwnedFd::from(theirs)))
        .await
        .map_err(|e| start("the runner", e))?;
    ours.set_nonblocking(true)
        .map_err(|e| start("the runner's socket", e))?;
    let mut stream = UnixStream::from_std(ours).map_err(|e| start("the runner's socket", e))?;
    let said = tokio::time::timeout(
        RUNNER_START_TIMEOUT,
        accept_runner(&mut stream, AGENT_VERSION),
    )
    .await
    .map_err(|_| LaunchError::NoHello(RUNNER_START_TIMEOUT))??;
    if said != mode {
        return Err(LaunchError::WrongMode {
            expected: mode,
            got: said,
        });
    }
    Ok(Runner { process, stream })
}

/// Runs a Python task: starts the runner, sends the request, and waits for its result while
/// supervising the process.
///
/// # Errors
///
/// [`LaunchError`] if the runner can't be started or supervised; a runner that dies or sends
/// nothing still gives a [`Ran`] (without a result).
pub async fn run_python(
    process: &ProcessSpec,
    request: TaskRequest,
    limits: Limits,
    stop: impl Future<Output = StopReason>,
) -> Result<Ran, LaunchError> {
    let started = Instant::now();
    let Runner {
        mut process,
        mut stream,
    } = start_runner(process, RunnerMode::Task).await?;
    write_message(&mut stream, &AgentMessage::Task(Box::new(request))).await?;
    let result = async {
        let message: Result<RunnerMessage, ChannelError> = read_message(&mut stream).await;
        message.ok()
    };
    let (runner_result, supervised) = tokio::join!(result, supervise(&mut process, limits, stop));
    let supervised = supervised.map_err(LaunchError::Supervise)?;
    let _ = process.output().await;
    Ok(Ran {
        supervised,
        runner_result,
        output: None,
        duration: started.elapsed(),
    })
}

/// Runs a command process under supervision, keeping the tails of its output.
///
/// # Errors
///
/// [`LaunchError`] if it can't be started or supervised.
pub async fn run_command(
    spec: &ProcessSpec,
    limits: Limits,
    stop: impl Future<Output = StopReason>,
) -> Result<Ran, LaunchError> {
    let started = Instant::now();
    let mut process = spawn(spec).await.map_err(|source| LaunchError::Start {
        what: spec.program.display().to_string(),
        source,
    })?;
    let supervised = supervise(&mut process, limits, stop)
        .await
        .map_err(LaunchError::Supervise)?;
    let output = process.output().await.map_err(LaunchError::Supervise)?;
    Ok(Ran {
        supervised,
        runner_result: None,
        output: Some(output),
        duration: started.elapsed(),
    })
}

/// Starts runners in parse mode for one root (see [`crate::parse::parse_files`]).
#[derive(Debug, Clone)]
pub struct ProcessParseRunner {
    /// The runner process (interpreter, `-m schedr.runner parse`, environment, log file).
    pub process: ProcessSpec,
}

/// A parse runner that connected and got its request.
#[derive(Debug)]
pub struct ProcessParseSession {
    runner: Runner,
}

impl ParseSession for ProcessParseSession {
    async fn next(&mut self) -> Result<RunnerMessage, ChannelError> {
        read_message(&mut self.runner.stream).await
    }

    async fn stop(mut self) {
        let _ = self.runner.process.signal(Signal::Kill);
        let _ = self.runner.process.wait().await;
    }
}

impl ParseRunner for ProcessParseRunner {
    type Session = ProcessParseSession;

    async fn start(&mut self, request: ParseRequest) -> Result<ProcessParseSession, ChannelError> {
        let mut runner = start_runner(&self.process, RunnerMode::Parse)
            .await
            .map_err(|e| match e {
                LaunchError::Channel(channel) => channel,
                other => ChannelError::Io(std::io::Error::other(other.to_string())),
            })?;
        write_message(&mut runner.stream, &AgentMessage::Parse(request)).await?;
        Ok(ProcessParseSession { runner })
    }
}
