//! Task logs, read from the worker that ran the attempt (see archive/PROJECT.md, Task logs): the
//! webserver proxies them, so clients never reach workers.
//!
//! A worker's address is what it advertised when it registered (`workers.log_endpoint`), so
//! it is found by the worker's ID. Every answer names the worker that gave it
//! (`Schedr-Worker-Id`): an address that now belongs to another worker is noticed instead of
//! passing for a deleted log. Requests to workers never go through an HTTP proxy, and in
//! `tls.mode = "app"` present the webserver's certificate and expect the worker's to name
//! `[tls] worker_name`.

use std::fmt;
use std::fmt::Write as _;
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
    connect_timeout: Duration,
    request_timeout: Duration,
    /// The client, with the TLS generation it was built for.
    client: Mutex<Option<(u64, reqwest::Client)>>,
}

impl HttpLogSource {
    /// Logs over TLS with `tls` (`app` mode), or plain HTTP (`mesh` mode, standalone).
    #[must_use]
    pub const fn new(tls: Option<Tls>) -> Self {
        Self {
            tls,
            connect_timeout: CONNECT_TIMEOUT,
            request_timeout: REQUEST_TIMEOUT,
            client: Mutex::new(None),
        }
    }

    /// This, with the time connecting to a worker and a whole request may take (`[webserver]
    /// worker_connect_timeout`, `worker_request_timeout`).
    #[must_use]
    pub fn with_timeouts(self, connect: Duration, request: Duration) -> Self {
        Self {
            connect_timeout: connect,
            request_timeout: request,
            ..self
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
        // Never redirected (B133): a log comes only from the worker's own address, over the
        // TLS asked for.
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout);
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

/// A part of a log as shown: masked, whole lines only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaskedPart {
    /// The bytes skipped at its start (the rest of a line begun before it).
    pub skipped: usize,
    /// Its text, masked.
    pub content: String,
    /// The bytes it covers after the skipped ones.
    pub used: usize,
}

/// `bytes`, a part of a log, as shown: masked (a task may print an address with its
/// password), whole lines only, so no secret is shown in pieces (a reader picks where a part
/// starts and how long it is). Not starting a line (`at_line` false), it starts after its
/// first newline, all of it skipped when it has none. With `more` to come, it ends after its
/// last newline, the rest left for the next part. With none in it, it waits for its line's
/// end, unless it is `full` (as long as asked) and starts a line: then it is shown to its
/// last whitespace (its last token may go on), ending with a note that it is cut, and the
/// rest of that line is skipped, as a part not starting a line is. A short part shows a
/// line's start at most, never its pieces. `before`, the bytes just before the part, is
/// masked with it, not shown: a value begun there (a quoted one going on over lines) is
/// hidden in the part too.
#[must_use]
pub fn masked_lines(
    before: &[u8],
    bytes: &[u8],
    at_line: bool,
    more: bool,
    full: bool,
) -> MaskedPart {
    let newline = |b: &u8| *b == b'\n';
    let skipped = if at_line {
        0
    } else {
        bytes
            .iter()
            .position(newline)
            .map_or(bytes.len(), |at| at + 1)
    };
    let rest = &bytes[skipped..];
    let (lines, used) = if more {
        match rest.iter().rposition(newline) {
            Some(at) => (at + 1, at + 1),
            // Its last token may go on in the next part: not shown. Nothing to show
            // (no whitespace in a whole part): the line is passed over.
            None if full && at_line => (
                rest.iter()
                    .rposition(u8::is_ascii_whitespace)
                    .map_or(0, |at| at + 1),
                rest.len(),
            ),
            None => (0, 0),
        }
    } else {
        (rest.len(), rest.len())
    };
    let (text, whole) = text_of(&rest[..lines], more && lines == rest.len());
    // What precedes the text shown: the bytes before the part, and those skipped in it.
    let mut preceding = before.to_vec();
    preceding.extend_from_slice(&bytes[..skipped]);
    let mut content =
        schedr_config::masked::mask_text_after(&String::from_utf8_lossy(&preceding), &text);
    let cut = lines != used;
    if cut {
        let _ = writeln!(
            content,
            "… (the rest of this line is not shown: it is over {} bytes)",
            rest.len()
        );
    }
    MaskedPart {
        skipped,
        content,
        used: if cut { used } else { whole },
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
