//! Parse jobs on the worker: the runner in `parse` mode over a root's files, with a per-file
//! timeout.
//!
//! One runner process parses the files of a parse job one at a time and reports each file. If
//! a file doesn't finish within the per-file timeout, or the runner dies while importing it,
//! the agent stops the runner, records an import error for that file and starts a new runner
//! for the remaining files (see PROJECT.md, Rust/Python integration: parsing granularity).

use std::collections::VecDeque;
use std::time::Duration;

use schedr_model::runner::{ErrorSummary, ParseRequest, ParsedFile, RunnerMessage};

use crate::channel::ChannelError;

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

    /// Stops the runner (its whole process group) and waits for it.
    fn stop(self) -> impl Future<Output = ()> + Send;
}

/// Starts runners in parse mode.
pub trait ParseRunner: Send {
    /// The started runner.
    type Session: ParseSession;

    /// Starts a runner, completes the handshake and sends it the request.
    fn start(
        &mut self,
        request: ParseRequest,
    ) -> impl Future<Output = Result<Self::Session, ChannelError>> + Send;
}

/// Parses the files of `request`, starting a new runner after each file that times out or
/// kills its runner; a file over the memory budget after others in the same runner is tried
/// again first in a fresh one before it is blamed.
///
/// # Errors
///
/// [`ParseError`] if a runner can't be started or talked to outside a file; errors inside a
/// file become that file's import error.
pub async fn parse_files<R: ParseRunner>(
    runner: &mut R,
    request: ParseRequest,
    per_file: Duration,
) -> Result<Vec<ParsedFile>, ParseError> {
    let mut remaining: VecDeque<String> = request.files.iter().cloned().collect();
    let mut parsed = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let mut session = runner
            .start(ParseRequest {
                files: remaining.iter().cloned().collect(),
                ..request.clone()
            })
            .await?;
        let step = run_session(&mut session, &mut remaining, &mut parsed, per_file).await;
        session.stop().await;
        match step {
            // Done, or the file goes first in a fresh runner (it alone is then measured).
            Ok(()) | Err(SessionEnd::FreshRunner) => {}
            Err(SessionEnd::Lost(path, error)) => {
                remaining.pop_front();
                parsed.push(ParsedFile {
                    path,
                    dags: Vec::new(),
                    error: Some(error),
                    warnings: Vec::new(),
                });
            }
            Err(SessionEnd::Failed(error)) => return Err(error),
        }
    }
    Ok(parsed)
}

/// How a session ended early.
enum SessionEnd {
    /// The file being imported timed out or killed the runner.
    Lost(String, ErrorSummary),
    /// The runner went over its memory budget while importing a file after others: what the
    /// earlier imports hold counts too, so the file is tried again first in a fresh runner.
    FreshRunner,
    /// Nothing can be blamed on a file.
    Failed(ParseError),
}

async fn run_session<S: ParseSession>(
    session: &mut S,
    remaining: &mut VecDeque<String>,
    parsed: &mut Vec<ParsedFile>,
    per_file: Duration,
) -> Result<(), SessionEnd> {
    let mut current: Option<String> = None;
    let mut imported = 0;
    loop {
        let message = match tokio::time::timeout(per_file, session.next()).await {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return Err(match current {
                    Some(_) if imported > 0 && matches!(error, ChannelError::OverMemory(_)) => {
                        SessionEnd::FreshRunner
                    }
                    Some(path) => SessionEnd::Lost(path, died(&error)),
                    None => SessionEnd::Failed(error.into()),
                });
            }
            Err(_) => {
                return Err(match current {
                    Some(path) => SessionEnd::Lost(path, timed_out(per_file)),
                    None => SessionEnd::Failed(ParseError::Unresponsive(per_file)),
                });
            }
        };
        match message {
            RunnerMessage::FileStarted { path }
                if current.is_none() && remaining.front() == Some(&path) =>
            {
                current = Some(path);
            }
            RunnerMessage::FileParsed {
                path,
                dags,
                error,
                warnings,
            } if current.as_ref() == Some(&path) => {
                current = None;
                imported += 1;
                remaining.pop_front();
                parsed.push(ParsedFile {
                    path,
                    dags,
                    error,
                    warnings,
                });
            }
            RunnerMessage::ParseFinished if current.is_none() && remaining.is_empty() => {
                return Ok(());
            }
            other => {
                let expected = match (&current, remaining.front()) {
                    (Some(_), _) => "file_parsed for the file started",
                    (None, Some(_)) => "file_started for the next file",
                    (None, None) => "parse_finished",
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
