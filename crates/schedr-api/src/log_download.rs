//! An attempt's whole log as a file (decision 514; Codex's review of M5.2i5): read from its
//! worker part by part, each line masked whole and passed on as it comes, so a line longer than
//! a part (a structured line with a 2 MiB message) is downloaded as it was written. The log
//! view's parts cut such a line, to never show a secret in pieces; here the line is held until
//! it ends, up to [`LINE_MAX`]: past it, what is held is masked and passed on.

use axum::body::Body;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use schedr_model::logs::{MASK_CONTEXT, MAX_CHUNK};

use crate::error::ApiError;
use crate::logs::{LogFetchError, WorkerLog};
use crate::routes::{LogPlace, log_source};
use crate::state::ApiState;
use crate::views::LogQuery;

#[cfg(test)]
mod tests;

/// The longest line held whole: past it, what is held goes on masked as it is.
pub const LINE_MAX: usize = 64 << 20;

/// What a download has read and holds: the bytes of a line not ended yet, the last ones
/// passed on (a value begun there is masked in what follows), and whether the rest of a token
/// past [`LINE_MAX`] is being skipped.
#[derive(Debug, Default)]
pub struct Lines {
    held: Vec<u8>,
    before: Vec<u8>,
    skipping: bool,
}

/// The note in place of a token too long to mask whole.
pub const NOT_SHOWN: &str = "… (a token over 64 MiB is not shown)\n";

impl Lines {
    /// `bytes` read next: the lines they end, masked; `last` (the log read to its end) passes
    /// on all that is held. A line past [`LINE_MAX`] goes on to its last whitespace, never
    /// within a token (Codex's second review of M5.2i5: a password's `@` came after the cut);
    /// a token past it is not shown, skipped to its line's end.
    #[must_use]
    pub fn take(&mut self, mut bytes: &[u8], last: bool) -> String {
        let mut out = String::new();
        if self.skipping {
            match bytes.iter().position(|b| *b == b'\n') {
                Some(at) => {
                    self.skipping = false;
                    bytes = &bytes[at + 1..];
                }
                None => return out,
            }
        }
        self.held.extend_from_slice(bytes);
        let end = if last {
            self.held.len()
        } else if let Some(at) = self.held.iter().rposition(|b| *b == b'\n') {
            at + 1
        } else if self.held.len() > LINE_MAX {
            let Some(at) = self.held.iter().rposition(u8::is_ascii_whitespace) else {
                self.held.clear();
                self.skipping = true;
                out.push_str(NOT_SHOWN);
                return out;
            };
            at + 1
        } else {
            return out;
        };
        let lines: Vec<u8> = self.held.drain(..end).collect();
        let text = String::from_utf8_lossy(&lines);
        out.push_str(&schedr_config::masked::mask_text_after(
            &String::from_utf8_lossy(&self.before),
            &text,
        ));
        // The context runs on across parts: a part passing on only a short line keeps what came
        // before it (agy's review of M5.2i5: a quoted key's start was forgotten, its value shown).
        let context = usize::try_from(MASK_CONTEXT).unwrap_or(usize::MAX);
        self.before.extend_from_slice(&lines);
        let drop = self.before.len().saturating_sub(context);
        self.before.drain(..drop);
        out
    }
}

/// `GET /api/v1/runs/{run_id}/tasks/{task_id}/attempts/{try_number}/log.txt`: the attempt's
/// whole log as it is now, masked line by line, as text. Not found or unavailable as its parts
/// are; a worker failing midway ends the file early.
///
/// # Errors
///
/// [`ApiError`]: no such attempt, no log (yet), or its worker gone or unreachable.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/tasks/{task_id}/attempts/{try_number}/log.txt",
    tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID"),
        ("try_number" = u32, Path, description = "The attempt's try number"),
        LogQuery
    ),
    responses(
        (status = 200, content_type = "text/plain", body = String),
        (status = 404, body = crate::views::ErrorBody),
        (status = 503, body = crate::views::ErrorBody)
    ))]
pub async fn download_log(
    State(state): State<ApiState>,
    Path((run_id, task_id, try_number)): Path<(String, String, u32)>,
    query: Result<Query<LogQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let LogPlace {
        attempt_id,
        worker_id,
        endpoint,
    } = log_source(&state, (&run_id, task_id), try_number, query.map_index).await?;
    // The first part read here: a log that can't be read is an answer, not an empty file.
    let first = read(&state, &endpoint, &attempt_id, &worker_id, 0).await?;
    let mut lines = Lines::default();
    let opening = lines.take(&first.bytes, ended(&first, 0));
    let next = read_on(&first, 0);
    let rest = futures_util::stream::unfold((lines, next), move |(mut lines, from)| {
        let (state, endpoint, attempt_id, worker_id) = (
            state.clone(),
            endpoint.clone(),
            attempt_id.clone(),
            worker_id.clone(),
        );
        async move {
            let from = from?;
            match read(&state, &endpoint, &attempt_id, &worker_id, from).await {
                Ok(log) => {
                    let text = lines.take(&log.bytes, ended(&log, from));
                    let next = read_on(&log, from);
                    Some((Ok::<_, std::io::Error>(text), (lines, next)))
                }
                Err(error) => Some((Err(std::io::Error::other(error.to_string())), (lines, None))),
            }
        }
    });
    let body = futures_util::StreamExt::chain(
        futures_util::stream::once(async move { Ok::<_, std::io::Error>(opening) }),
        rest,
    );
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        Body::from_stream(body),
    )
        .into_response())
}

/// Whether `log`, read from `from`, reaches the end of the log as it is now.
fn ended(log: &WorkerLog, from: u64) -> bool {
    let read = u64::try_from(log.bytes.len()).unwrap_or(u64::MAX);
    from + read >= log.size || log.bytes.is_empty()
}

/// Where to read next after `log`, read from `from`; none at its end.
fn read_on(log: &WorkerLog, from: u64) -> Option<u64> {
    let read = u64::try_from(log.bytes.len()).unwrap_or(u64::MAX);
    (!ended(log, from)).then_some(from + read)
}

/// A part of the log from `from`, from the worker that ran it.
async fn read(
    state: &ApiState,
    endpoint: &str,
    attempt_id: &str,
    worker_id: &str,
    from: u64,
) -> Result<WorkerLog, ApiError> {
    let log = match state
        .logs
        .fetch(endpoint, attempt_id, from, MAX_CHUNK)
        .await
    {
        Ok(log) => log,
        Err(LogFetchError::Missing { .. }) => {
            return Err(ApiError::NotFound(format!(
                "the log file is unavailable at worker `{worker_id}` (it may have been deleted)"
            )));
        }
        Err(LogFetchError::Unreachable(reason)) => {
            return Err(ApiError::Unavailable(format!(
                "worker `{worker_id}` can't be reached at {endpoint}: {reason}"
            )));
        }
    };
    if let Some(other) = log.worker_id.as_ref().filter(|other| *other != worker_id) {
        return Err(ApiError::Unavailable(format!(
            "reached worker `{other}` instead of `{worker_id}` at {endpoint}: its address is \
             stale; try again shortly"
        )));
    }
    Ok(log)
}
