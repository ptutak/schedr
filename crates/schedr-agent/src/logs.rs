//! The worker's log endpoint (see archive/PROJECT.md, Task logs): the webserver reads task logs
//! from the worker that ran them, a part at a time.
//!
//! - **`GET /logs/{attempt_id}?from=<byte>&limit=<bytes>`** answers the log's bytes from
//!   `from` (at most `limit`, between 4 bytes and 1 MiB), with headers saying which worker
//!   answered (`Schedr-Worker-Id`), how long the log is now (`Schedr-Log-Size`) and whether
//!   the attempt still runs here (`Schedr-Attempt-Running`, from the journal, checked before
//!   the file is read: an attempt said to be finished has all its output in the answer's
//!   size). Reading on from the size returned follows a running task's log. A trigger's log
//!   is named by its instance, running while the agent runs it ([`LiveLogs`], B272).
//! - **Errors** are JSON (`{"error": "…"}`): 400 for an ID that isn't an attempt ID, 404 for
//!   a log this worker doesn't have (deleted by retention, or another worker's).
//!
//! Access is decided by TLS (`tls.mode = "app"`: only clients with a certificate from the
//! CA) or by the mesh; the endpoint serves logs only, never actions.

use std::collections::BTreeSet;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use schedr_model::logs::{
    LOG_SIZE_HEADER, MAX_CHUNK, MAX_READ, MIN_CHUNK, RUNNING_HEADER, WORKER_ID_HEADER,
};

use crate::journal::{Journal, JournalRecord};

#[cfg(test)]
mod tests;

/// What the endpoint serves.
#[derive(Debug, Clone)]
pub struct LogEndpoint {
    /// This worker's ID, sent with every answer.
    pub worker_id: String,
    /// Where task logs are (`<log_dir>/<attempt_id>.log`).
    pub log_dir: PathBuf,
    /// The journal, which knows the attempts running here.
    pub journal: Journal,
    /// The logs written now that the journal doesn't know: the triggers running here.
    pub live: LiveLogs,
}

/// The logs being written that no journal record names: the triggers running on this worker
/// (B272), which are never attempts. Shared by the agent (which adds and removes them), the
/// log endpoint (which says they run) and the sweep (which spares them).
#[derive(Debug, Clone, Default)]
pub struct LiveLogs(Arc<Mutex<BTreeSet<String>>>);

impl LiveLogs {
    /// A log being written from now on, by its ID.
    pub fn add(&self, id: &str) {
        self.names().insert(id.to_owned());
    }

    /// A log no longer written.
    pub fn remove(&self, id: &str) {
        self.names().remove(id);
    }

    /// Whether this log is being written.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.names().contains(id)
    }

    /// The log files being written, as named in the log folder.
    #[must_use]
    pub fn files(&self) -> Vec<String> {
        self.names().iter().map(|id| format!("{id}.log")).collect()
    }

    fn names(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        // A set of names: whatever panicked holding it left it whole.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One handle is equal to itself only: two workers' sets are told apart.
impl PartialEq for LiveLogs {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The query of a log request.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogQuery {
    /// The first byte wanted (0 by default).
    pub from: Option<u64>,
    /// How many bytes at most ([`MAX_CHUNK`] by default; between [`MIN_CHUNK`] and [`MAX_READ`]).
    pub limit: Option<u64>,
}

/// An error answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogError {
    /// What went wrong.
    pub error: String,
}

/// The endpoint's routes.
pub fn router(endpoint: LogEndpoint) -> Router {
    Router::new()
        .route("/logs/{attempt_id}", get(get_log))
        .with_state(endpoint)
}

/// Whether `text` names an attempt's log: its UUID, or a hold of it (`<attempt>_<n>`, a moved
/// cleanup's, decision 535). Nothing else names a log file here.
#[must_use]
pub fn is_attempt_id(text: &str) -> bool {
    schedr_model::hold::HoldId::is_valid(text)
}

async fn get_log(
    State(endpoint): State<LogEndpoint>,
    Path(attempt_id): Path<String>,
    query: Result<Query<LogQuery>, QueryRejection>,
) -> Response {
    let mut headers = HeaderMap::new();
    if let Ok(id) = HeaderValue::from_str(&endpoint.worker_id) {
        headers.insert(WORKER_ID_HEADER, id);
    }
    let error = |status: StatusCode, error: String| {
        (status, headers.clone(), Json(LogError { error })).into_response()
    };
    let Ok(Query(query)) = query else {
        return error(
            StatusCode::BAD_REQUEST,
            "the query takes `from` and `limit`, as numbers of bytes".to_owned(),
        );
    };
    if !is_attempt_id(&attempt_id) {
        return error(
            StatusCode::BAD_REQUEST,
            format!("`{attempt_id}` isn't an attempt ID"),
        );
    }
    // Before the file: what ran before the attempt was recorded finished is all in the file.
    let journal = endpoint.journal.clone();
    let id = attempt_id.clone();
    let running = tokio::task::spawn_blocking(move || journal.record(&id))
        .await
        .ok()
        .and_then(Result::ok)
        .flatten()
        .is_some_and(|record| matches!(record, JournalRecord::Running(_)))
        || endpoint.live.contains(&attempt_id);
    let path = endpoint.log_dir.join(format!("{attempt_id}.log"));
    let read = read_part(
        &path,
        query.from.unwrap_or(0),
        query.limit.unwrap_or(MAX_CHUNK),
    )
    .await;
    let (size, bytes) = match read {
        Ok(part) => part,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return error(
                StatusCode::NOT_FOUND,
                format!(
                    "worker `{}` has no log of attempt `{attempt_id}` (it may have been deleted)",
                    endpoint.worker_id
                ),
            );
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "can't read a task log");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("worker `{}` can't read the log: {e}", endpoint.worker_id),
            );
        }
    };
    headers.insert(LOG_SIZE_HEADER, HeaderValue::from(size));
    headers.insert(
        RUNNING_HEADER,
        HeaderValue::from_static(if running { "true" } else { "false" }),
    );
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    (StatusCode::OK, headers, bytes).into_response()
}

/// The file's size and up to `limit` (between [`MIN_CHUNK`] and [`MAX_READ`]) bytes from
/// `from`.
async fn read_part(
    path: &std::path::Path,
    from: u64,
    limit: u64,
) -> std::io::Result<(u64, Vec<u8>)> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    let mut file = crate::process::open_regular(options, path).await?;
    let size = file.metadata().await?.len();
    let from = from.min(size);
    file.seek(SeekFrom::Start(from)).await?;
    let mut bytes = Vec::new();
    file.take(limit.clamp(MIN_CHUNK, MAX_READ).min(size - from))
        .read_to_end(&mut bytes)
        .await?;
    Ok((size, bytes))
}
