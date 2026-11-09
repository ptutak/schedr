//! Parse jobs on the worker: the runner in `parse` mode over a root's files, with a per-file
//! timeout.
//!
//! Each file of a parse job gets a runner process of its own, a few at once (`[dags]
//! parse_concurrency`). If a file doesn't finish within the per-file timeout, or its runner dies
//! while importing it, the agent stops the runner and records an import error for that file
//! (see archive/PROJECT.md, Rust/Python integration: parsing granularity).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use futures_util::StreamExt as _;
use futures_util::stream::FuturesUnordered;
use schedr_model::runner::{AgentMessage, ErrorSummary, ParseRequest, ParsedFile, RunnerMessage};

use crate::channel::ChannelError;
use crate::jobs::ScriptRoot;

#[cfg(test)]
mod tests;

/// The exception name of the import error recorded for a file that timed out.
pub const TIMEOUT_EXCEPTION: &str = schedr_model::runner::PARSE_TIMEOUT_EXCEPTION;

/// The exception name of the import error recorded for a file whose runner died.
pub const RUNNER_DIED_EXCEPTION: &str = schedr_model::runner::RUNNER_DIED_EXCEPTION;

/// Why a parse job couldn't be finished.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// Talking to the runner failed outside a file (starting, the handshake, between files).
    #[error("parse runner: {0}")]
    Channel(#[from] ChannelError),
    /// The runner said nothing for the per-file timeout outside a file (e.g. while starting).
    #[error("the parse runner didn't respond within {0:?}")]
    Unresponsive(Duration),
}

/// A started runner in parse mode, after the handshake and the `parse` request.
pub trait ParseSession: Send {
    /// The runner's next message.
    fn next(&mut self) -> impl Future<Output = Result<RunnerMessage, ChannelError>> + Send;

    /// Answers the runner (a `ScriptOperator`'s declaration, decision 293).
    fn send(
        &mut self,
        message: AgentMessage,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send;

    /// Stops the runner (its whole process group) and waits for it.
    fn stop(self) -> impl Future<Output = ()> + Send;
}

/// Starts runners in parse mode.
pub trait ParseRunner: Sync {
    /// The started runner.
    type Session: ParseSession;

    /// Starts a runner, completes the handshake and sends it the request. Called for several
    /// files at once.
    fn start(
        &self,
        request: ParseRequest,
    ) -> impl Future<Output = Result<Self::Session, ChannelError>> + Send;
}

/// How a parse job runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseLimits {
    /// How long one file may take to import.
    pub per_file: Duration,
    /// How many files are parsed at once.
    pub concurrency: usize,
}

impl ParseLimits {
    /// One file at a time.
    #[must_use]
    pub const fn one_at_a_time(per_file: Duration) -> Self {
        Self {
            per_file,
            concurrency: 1,
        }
    }

    /// `[dags] parse_file_timeout` and `parse_concurrency`, the latter capped at the cores this
    /// process may use: more would only make the files' timeouts tighter.
    #[must_use]
    pub fn configured(per_file: Duration, concurrency: u32) -> Self {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Self {
            per_file,
            concurrency: capped(concurrency, cores),
        }
    }
}

fn capped(concurrency: u32, cores: usize) -> usize {
    usize::try_from(concurrency)
        .unwrap_or(usize::MAX)
        .min(cores)
        .max(1)
}

/// Parses the files of `request`, one at a time, each in a runner of its own: one file's imports
/// and what they change can't reach another's, as they can't reach a task's (decision 326).
///
/// # Errors
///
/// [`ParseError`] if a runner can't be started or talked to outside a file; errors inside a
/// file become that file's import error.
pub async fn parse_files<R: ParseRunner>(
    runner: &R,
    request: ParseRequest,
    per_file: Duration,
) -> Result<Vec<ParsedFile>, ParseError> {
    parse_files_with(
        runner,
        request,
        ParseLimits::one_at_a_time(per_file),
        &BTreeMap::new(),
    )
    .await
}

/// As [`parse_files`], `limits.concurrency` files at once, with the files in the order of the
/// request, answering a `ScriptOperator`'s questions from the script roots `scripts` (their
/// copies, decision 293) and recording in each file the script roots it asked about (decision
/// 304).
///
/// After a failure no new file starts, and those being parsed are finished: their runners end
/// as usual, not by being dropped.
///
/// # Errors
///
/// As [`parse_files`], the first failure.
pub async fn parse_files_with<R: ParseRunner>(
    runner: &R,
    mut request: ParseRequest,
    limits: ParseLimits,
    scripts: &BTreeMap<String, ScriptRoot>,
) -> Result<Vec<ParsedFile>, ParseError> {
    let files = std::mem::take(&mut request.files);
    let request = &request;
    let mut parsed: Vec<Option<ParsedFile>> = files.iter().map(|_| None).collect();
    let mut waiting = files.into_iter().enumerate();
    let mut parsing = FuturesUnordered::new();
    let mut failure = None;
    loop {
        while failure.is_none() && parsing.len() < limits.concurrency.max(1) {
            let Some((index, file)) = waiting.next() else {
                break;
            };
            parsing.push(parse_file(
                runner,
                request,
                index,
                file,
                limits.per_file,
                scripts,
            ));
        }
        match parsing.next().await {
            Some(Ok((index, file))) => parsed[index] = Some(file),
            Some(Err(error)) => failure = failure.or(Some(error)),
            None => break,
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(parsed.into_iter().flatten().collect()),
    }
}

/// One file, in a runner of its own.
async fn parse_file<R: ParseRunner>(
    runner: &R,
    request: &ParseRequest,
    index: usize,
    file: String,
    per_file: Duration,
    scripts: &BTreeMap<String, ScriptRoot>,
) -> Result<(usize, ParsedFile), ParseError> {
    let mut session = runner
        .start(ParseRequest {
            files: vec![file.clone()],
            ..request.clone()
        })
        .await?;
    let mut asked = BTreeSet::new();
    let step = run_session(&mut session, &file, per_file, (scripts, &mut asked)).await;
    session.stop().await;
    match step {
        Ok(parsed) => Ok((index, parsed)),
        Err(SessionEnd::Lost(error)) => Ok((
            index,
            ParsedFile {
                // What it asked before it was lost still counts (decision 304).
                script_roots: asked.into_iter().collect(),
                path: file,
                dags: Vec::new(),
                connections: Vec::new(),
                error: Some(error),
                warnings: Vec::new(),
            },
        )),
        Err(SessionEnd::Failed(error)) => Err(error),
    }
}

/// How a session ended early.
enum SessionEnd {
    /// The file being imported timed out or killed the runner.
    Lost(ErrorSummary),
    /// Nothing can be blamed on the file.
    Failed(ParseError),
}

async fn run_session<S: ParseSession>(
    session: &mut S,
    file: &str,
    per_file: Duration,
    (scripts, asked): (&BTreeMap<String, ScriptRoot>, &mut BTreeSet<String>),
) -> Result<ParsedFile, SessionEnd> {
    let mut started = false;
    let mut done = None;
    // A file's whole import has `per_file`, its `ScriptOperator`s' questions included.
    let mut deadline = tokio::time::Instant::now() + per_file;
    loop {
        // A runner still to say it started has `per_file` from now on.
        if !started {
            deadline = tokio::time::Instant::now() + per_file;
        }
        let message = match tokio::time::timeout_at(deadline, session.next()).await {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return Err(if started && done.is_none() {
                    SessionEnd::Lost(died(&error))
                } else {
                    SessionEnd::Failed(error.into())
                });
            }
            Err(_) => {
                return Err(if started && done.is_none() {
                    SessionEnd::Lost(timed_out(per_file))
                } else {
                    SessionEnd::Failed(ParseError::Unresponsive(per_file))
                });
            }
        };
        let importing = started && done.is_none();
        match message {
            RunnerMessage::FileStarted { path } if !started && path == file => {
                started = true;
                // Its import starts now.
                deadline = tokio::time::Instant::now() + per_file;
            }
            RunnerMessage::ScriptDeclaration { reference } if importing => {
                let (root, found) = crate::jobs::lookup(&reference, scripts);
                asked.extend(root);
                let answer = match found {
                    Ok(declaration) => AgentMessage::ScriptDeclaration(Box::new(declaration)),
                    Err(message) => AgentMessage::ScriptNotFound { message },
                };
                match tokio::time::timeout_at(deadline, session.send(answer)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => return Err(SessionEnd::Lost(died(&error))),
                    Err(_) => return Err(SessionEnd::Lost(timed_out(per_file))),
                }
            }
            RunnerMessage::FileParsed {
                path,
                dags,
                connections,
                error,
                warnings,
            } if importing && path == file => {
                done = Some(ParsedFile {
                    script_roots: std::mem::take(asked).into_iter().collect(),
                    path,
                    dags,
                    connections,
                    error,
                    warnings,
                });
                // The wait for its end is a wait of its own.
                deadline = tokio::time::Instant::now() + per_file;
            }
            RunnerMessage::ParseFinished if done.is_some() => {
                return done.ok_or(SessionEnd::Failed(ParseError::Unresponsive(per_file)));
            }
            other => {
                let expected = match (started, done.is_some()) {
                    (true, false) => "file_parsed for the file started",
                    (false, _) => "file_started for its file",
                    (true, true) => "parse_finished",
                };
                return Err(SessionEnd::Failed(ParseError::Channel(
                    ChannelError::Unexpected {
                        expected,
                        got: crate::channel::describe(&other),
                    },
                )));
            }
        }
    }
}

fn timed_out(per_file: Duration) -> ErrorSummary {
    ErrorSummary {
        exception: TIMEOUT_EXCEPTION.to_owned(),
        message: format!(
            "importing the file took longer than the parse timeout of {} s",
            per_file.as_secs_f64()
        ),
        traceback: String::new(),
    }
}

fn died(error: &ChannelError) -> ErrorSummary {
    ErrorSummary {
        exception: RUNNER_DIED_EXCEPTION.to_owned(),
        message: format!("the runner stopped while importing the file ({error})"),
        traceback: String::new(),
    }
}
