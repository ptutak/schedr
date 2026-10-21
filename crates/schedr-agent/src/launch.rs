//! Starting real processes for attempts and parse jobs: the runner on a fresh Unix socket,
//! commands as they are, each supervised (timeout, cancel, SIGTERM then SIGKILL).
//!
//! The thin operating-system layer under [`crate::agent`], like [`crate::process`]: exercised
//! by the integration tests (milestone 1.17); the decisions around it are unit-tested in
//! [`crate::attempt`], [`crate::parse`] and [`crate::supervise`].

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use schedr_model::runner::{AgentMessage, ParseRequest, RunnerMessage, RunnerMode, TaskRequest};
use tokio::net::{UnixListener, UnixStream};

use crate::attempt::Ran;
use crate::channel::{ChannelError, accept_runner, read_message, write_message};
use crate::parse::{ParseRunner, ParseSession};
use crate::process::{OsProcess, ProcessSpec, spawn};
use crate::status::StopReason;
use crate::supervise::{Limits, Process, Signal, supervise};

/// The environment variable naming the agent's socket, read by the runner.
pub const SOCKET_ENV: &str = "SCHEDR_AGENT_SOCKET";

/// How long a started runner has to connect and say hello.
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
    /// The runner didn't connect in time.
    #[error("the runner didn't connect within {0:?}")]
    NoConnection(Duration),
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

/// Starts the runner described by `spec` in `mode`, on a new socket in `socket_dir`, and
/// completes the handshake.
///
/// # Errors
///
/// [`LaunchError`] if it can't be started, doesn't connect in time, or is in another mode or
/// version (it was refused then).
pub async fn start_runner(
    spec: &ProcessSpec,
    socket_dir: &Path,
    mode: RunnerMode,
) -> Result<Runner, LaunchError> {
    let start = |what: &str, source| LaunchError::Start {
        what: what.to_owned(),
        source,
    };
    tokio::fs::create_dir_all(socket_dir)
        .await
        .map_err(|e| start("the runner's socket directory", e))?;
    let socket = socket_dir.join(format!("{}.sock", uuid_like()));
    let listener = UnixListener::bind(&socket).map_err(|e| start("the runner's socket", e))?;
    let mut spec = spec.clone();
    spec.env
        .insert(SOCKET_ENV.to_owned(), socket.to_string_lossy().into_owned());
    let process = spawn(&spec).await.map_err(|e| start("the runner", e))?;
    let accepted = tokio::time::timeout(RUNNER_START_TIMEOUT, listener.accept()).await;
    let _ = tokio::fs::remove_file(&socket).await;
    let (mut stream, _) = accepted
        .map_err(|_| LaunchError::NoConnection(RUNNER_START_TIMEOUT))?
        .map_err(|e| start("the runner's connection", e))?;
    let said = accept_runner(&mut stream, AGENT_VERSION).await?;
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
    socket_dir: &Path,
    limits: Limits,
    stop: impl Future<Output = StopReason>,
) -> Result<Ran, LaunchError> {
    let started = Instant::now();
    let Runner {
        mut process,
        mut stream,
    } = start_runner(process, socket_dir, RunnerMode::Task).await?;
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
    /// Where its sockets go.
    pub socket_dir: PathBuf,
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
        let mut runner = start_runner(&self.process, &self.socket_dir, RunnerMode::Parse)
            .await
            .map_err(|e| match e {
                LaunchError::Channel(channel) => channel,
                other => ChannelError::Io(std::io::Error::other(other.to_string())),
            })?;
        write_message(&mut runner.stream, &AgentMessage::Parse(request)).await?;
        Ok(ProcessParseSession { runner })
    }
}

/// A unique enough name for a socket file (the process ID and a counter).
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "runner-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
