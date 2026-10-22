//! Task logs, read from the worker that ran the attempt (see PROJECT.md, Task logs): the
//! webserver proxies them, so clients never reach workers.
//!
//! A worker's address is what it advertised when it registered (`workers.log_endpoint`), so
//! it is found by the worker's ID. Every answer names the worker that gave it
//! (`Schedr-Worker-Id`): an address that now belongs to another worker is noticed instead of
//! passing for a deleted log. Requests to workers never go through an HTTP proxy, and in
//! `tls.mode = "app"` present the webserver's certificate and expect the worker's to name
//! `[tls] worker_name`.

use std::fmt;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use schedr_model::logs::{LOG_SIZE_HEADER, RUNNING_HEADER, WORKER_ID_HEADER};
use schedr_tls::{Tls, error_text};

#[cfg(test)]
mod tests;

/// How long connecting to a worker may take.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a whole request to a worker may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A part of a log, as a worker gave it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLog {
    /// The worker that answered.
    pub worker_id: Option<String>,
    /// The log's size when it was read.
    pub size: u64,
    /// Whether the attempt still runs there.
    pub running: bool,
    /// The bytes asked for.
    pub bytes: Vec<u8>,
}

/// Why a worker gave no log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFetchError {
    /// The worker has no such log (deleted, or another worker's).
    Missing {
        /// The worker that answered.
        worker_id: Option<String>,
    },
    /// The worker can't be reached, or failed.
    Unreachable(String),
}

/// The future of a [`LogSource::fetch`].
pub type LogFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WorkerLog, LogFetchError>> + Send + 'a>>;

/// Where the webserver gets logs from (workers over HTTP; something else in tests).
pub trait LogSource: Send + Sync + fmt::Debug {
    /// Up to `limit` bytes from `from` of the log of `attempt_id`, from the worker whose log
    /// endpoint is `endpoint`.
    fn fetch<'a>(
        &'a self,
        endpoint: &'a str,
        attempt_id: &'a str,
        from: u64,
        limit: u64,
    ) -> LogFuture<'a>;
}

/// Logs from workers over HTTP (TLS in `app` mode), never through a proxy.
#[derive(Debug)]
pub struct HttpLogSource {
    tls: Option<Tls>,
    /// The client, with the TLS generation it was built for.
    client: Mutex<Option<(u64, reqwest::Client)>>,
}

impl HttpLogSource {
    /// Logs over TLS with `tls` (`app` mode), or plain HTTP (`mesh` mode, standalone).
    #[must_use]
    pub const fn new(tls: Option<Tls>) -> Self {
        Self {
            tls,
            client: Mutex::new(None),
        }
    }

    /// The client for the material in use, rebuilt when the certificates changed.
    fn client(&self) -> Result<reqwest::Client, String> {
        let generation = self.tls.as_ref().map_or(0, Tls::generation);
        let mut cached = self.client.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((built, client)) = cached.as_ref()
            && *built == generation
        {
            return Ok(client.clone());
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT);
        if let Some(tls) = &self.tls {
            builder = builder.tls_backend_preconfigured((*tls.worker_client()).clone());
        }
        let client = builder
            .build()
            .map_err(|e| format!("can't set up the HTTP client: {e}"))?;
        *cached = Some((generation, client.clone()));
        Ok(client)
    }

    async fn get(
        &self,
        endpoint: &str,
        attempt_id: &str,
        from: u64,
        limit: u64,
    ) -> Result<WorkerLog, LogFetchError> {
        let https = endpoint.starts_with("https://");
        if https != self.tls.is_some() {
            let (theirs, ours) = if https {
                ("https", "mesh")
            } else {
                ("plain http", "app")
            };
            return Err(LogFetchError::Unreachable(format!(
                "it serves logs over {theirs} but [tls] mode is `{ours}` here: give every role \
                 the same [tls] mode"
            )));
        }
        let client = self.client().map_err(LogFetchError::Unreachable)?;
        let url = format!(
            "{}/logs/{attempt_id}?from={from}&limit={limit}",
            endpoint.trim_end_matches('/')
        );
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| LogFetchError::Unreachable(error_text(&e)))?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let worker_id = header(WORKER_ID_HEADER);
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(LogFetchError::Missing { worker_id });
        }
        let size = header(LOG_SIZE_HEADER).and_then(|s| s.parse().ok());
        let running = header(RUNNING_HEADER).is_some_and(|r| r == "true");
        let bytes = response
            .bytes()
            .await
            .map_err(|e| LogFetchError::Unreachable(error_text(&e)))?;
        if !status.is_success() {
            let message = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_owned))
                .unwrap_or_else(|| format!("it answered {status}"));
            return Err(LogFetchError::Unreachable(message));
        }
        let size = size.ok_or_else(|| {
            LogFetchError::Unreachable(format!("its answer has no `{LOG_SIZE_HEADER}`"))
        })?;
        Ok(WorkerLog {
            worker_id,
            size,
            running,
            bytes: bytes.to_vec(),
        })
    }
}

impl LogSource for HttpLogSource {
    fn fetch<'a>(
        &'a self,
        endpoint: &'a str,
        attempt_id: &'a str,
        from: u64,
        limit: u64,
    ) -> LogFuture<'a> {
        Box::pin(self.get(endpoint, attempt_id, from, limit))
    }
}

/// `bytes` as text, and how many of them it covers: a character cut at the end (more of the
/// log follows) is left for the next part, so following a log never garbles one; invalid
/// bytes become `U+FFFD`.
#[must_use]
pub fn text_of(bytes: &[u8], more: bool) -> (String, usize) {
    let whole = if more {
        bytes.len() - cut_at_end(bytes)
    } else {
        bytes.len()
    };
    (String::from_utf8_lossy(&bytes[..whole]).into_owned(), whole)
}

/// How many bytes at the end of `bytes` begin a UTF-8 character that isn't complete yet.
fn cut_at_end(bytes: &[u8]) -> usize {
    // A character has at most 4 bytes: its lead byte is among the last 3 if it is cut.
    for back in 1..=bytes.len().min(3) {
        let byte = bytes[bytes.len() - back];
        if byte & 0b1100_0000 == 0b1000_0000 {
            continue;
        }
        let needed = match byte {
            0xC0..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF7 => 4,
            _ => return 0,
        };
        return if back < needed { back } else { 0 };
    }
    0
}
