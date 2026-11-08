//! Live pages (M3.3, decisions 34A and 23A): what changed in each deployment, relayed to the
//! browsers, and a running attempt's log as it grows.
//!
//! - **Relays:** one per deployment, started by the first browser that wants its changes: it
//!   reads the webserver's `GET /api/v1/changes` and hands each change to every browser
//!   following that deployment; when the stream ends or can't be opened it tries again (after
//!   1 s, doubling to 30 s), and once it is back sends `refresh`, since anything may have
//!   changed meanwhile.
//! - **`GET /events`:** a browser's one stream (decision 175: its pages share it through a
//!   `SharedWorker`, `assets/live.js`): every deployment's changes, each page deciding which
//!   concern it and reading its live parts again (`hx-trigger="live:change"`).
//! - **The log tail:** `GET /d/<deployment>/runs/<run>/tasks/<task>/tail?try=&from=` answers
//!   the log's next part (HTML, colours converted) and the element reading the one after it,
//!   polled about once a second while the attempt runs (no connection held).

use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Extension;
use axum::extract::rejection::QueryRejection;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use schedr_client::{ClientError, ServerEvent, Transport};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::page::prefix;
use crate::{Ui, ansi};

#[cfg(test)]
mod tests;

/// How the live pages are paced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiTimings {
    /// How often a stream gets a `ping` (`keep_alive`).
    pub keep_alive: Duration,
    /// A stream sends at most one `change` this often (`coalesce`).
    pub coalesce: Duration,
    /// How often the log tail reads the log (`tail_interval`).
    pub tail_interval: Duration,
    /// How long it tries a log out of reach (`tail_give_up`).
    pub tail_give_up: Duration,
    /// The first wait before a relay opens its stream again (`relay_retry_first`).
    pub retry_first: Duration,
    /// The longest one (`relay_retry_max`).
    pub retry_max: Duration,
}

impl Default for UiTimings {
    /// What every UI runs with.
    fn default() -> Self {
        Self {
            keep_alive: KEEP_ALIVE,
            coalesce: COALESCE,
            tail_interval: TAIL_INTERVAL,
            tail_give_up: TAIL_GIVE_UP,
            retry_first: RETRY_FIRST,
            retry_max: RETRY_MAX,
        }
    }
}

/// How often a page's stream gets a `ping` (and the log tail a comment).
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// A page's stream sends at most one `change` this often (per deployment).
pub const COALESCE: Duration = Duration::from_secs(1);

/// How often the log tail reads the log.
pub const TAIL_INTERVAL: Duration = Duration::from_secs(1);

/// How long the log tail keeps trying a log out of reach before it says so and stops.
pub const TAIL_GIVE_UP: Duration = Duration::from_secs(30);

/// At most this much of a log a read: the API's longest part, so a line is cut only past it
/// (the API passes whole lines only, a longer one cut; decision 131).
const TAIL_PART: u64 = schedr_model::logs::MAX_CHUNK;

/// The first wait before a relay opens its stream again; it doubles up to [`RETRY_MAX`].
const RETRY_FIRST: Duration = Duration::from_secs(1);

/// The longest wait between a relay's attempts.
const RETRY_MAX: Duration = Duration::from_secs(30);

/// Changes a slow browser may fall behind by before it gets `refresh` instead.
const BACKLOG: usize = 1_024;

/// A change in a deployment, as its webserver announced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A run (itself, its tasks or attempts).
    Run {
        /// The run.
        run_id: String,
        /// Its DAG.
        dag_id: String,
    },
    /// A DAG.
    Dag {
        /// The DAG.
        dag_id: String,
    },
    /// The workers.
    Workers,
    /// Anything may have changed.
    Refresh,
}

impl Change {
    /// The change a webserver's event says; `None` for one this UI doesn't know.
    #[must_use]
    pub fn of(event: &ServerEvent) -> Option<Self> {
        let data: serde_json::Value = serde_json::from_str(&event.data).unwrap_or_default();
        let text = |key: &str| data[key].as_str().map(str::to_owned);
        Some(match event.name.as_str() {
            "run" => Self::Run {
                run_id: text("run_id")?,
                dag_id: text("dag_id")?,
            },
            "dag" => Self::Dag {
                dag_id: text("dag_id")?,
            },
            "workers" => Self::Workers,
            "refresh" => Self::Refresh,
            _ => return None,
        })
    }
}

/// The relays, one per deployment (in the order of [`Ui::deployments`]), each started by its
/// first subscriber.
#[derive(Debug)]
pub struct Relays {
    senders: Vec<OnceLock<broadcast::Sender<Change>>>,
    /// When each deployment last changed, in milliseconds since the epoch: its last change
    /// relayed, or its relay's start (what came before that isn't known).
    changed: Vec<Arc<AtomicI64>>,
}

impl Relays {
    /// Relays for `deployments` deployments, none started.
    #[must_use]
    pub fn new(deployments: usize) -> Self {
        Self {
            senders: (0..deployments).map(|_| OnceLock::new()).collect(),
            changed: (0..deployments)
                .map(|_| Arc::new(AtomicI64::new(0)))
                .collect(),
        }
    }

    /// Whether deployment `index` may have changed after `since` (milliseconds since the
    /// epoch), by what its relay has seen.
    fn changed_after(&self, index: usize, since: i64) -> bool {
        self.changed[index].load(Ordering::Relaxed) > since
    }

    /// Deployment `index`'s changes from now on (its relay started if it isn't).
    fn subscribe<T: Transport + Send + Sync + 'static>(
        &self,
        ui: &Arc<Ui<T>>,
        index: usize,
    ) -> broadcast::Receiver<Change> {
        self.senders[index]
            .get_or_init(|| {
                let (sender, _) = broadcast::channel(BACKLOG);
                let changed = Arc::clone(&self.changed[index]);
                changed.store(now_millis(), Ordering::Relaxed);
                tokio::spawn(relay(Arc::clone(ui), index, sender.clone(), changed));
                sender
            })
            .subscribe()
    }
}

/// Reads deployment `index`'s events for as long as the UI runs (see the module's comment).
async fn relay<T: Transport + Send + Sync>(
    ui: Arc<Ui<T>>,
    index: usize,
    sender: broadcast::Sender<Change>,
    changed: Arc<AtomicI64>,
) {
    let deployment = &ui.deployments[index];
    let timings = ui.timings;
    let mut wait = timings.retry_first;
    let mut lost = false;
    loop {
        match deployment.client.events().await {
            Ok(mut events) => {
                if lost {
                    changed.store(now_millis(), Ordering::Relaxed);
                    let _ = sender.send(Change::Refresh);
                }
                wait = timings.retry_first;
                while let Some(Ok(event)) = events.next().await {
                    if let Some(change) = Change::of(&event) {
                        changed.store(now_millis(), Ordering::Relaxed);
                        // No one listening is fine.
                        let _ = sender.send(change);
                    }
                }
                tracing::debug!(
                    deployment = deployment.name,
                    "its events ended; opening again"
                );
            }
            Err(error) => {
                tracing::debug!(deployment = deployment.name, %error, "can't follow its events");
            }
        }
        lost = true;
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(timings.retry_max);
    }
}

/// `GET /events`'s query.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Follow {
    /// When the page opening the stream was made, in milliseconds since the epoch: a
    /// deployment changed after it is sent a `refresh` as soon as the stream opens (one the
    /// page may not show, B144).
    #[serde(default)]
    pub since: Option<i64>,
}

impl Change {
    /// The change as a stream sends it: what changed, in which deployment, and when it was
    /// relayed (milliseconds since the epoch, this UI's clock, as a page's `since`).
    fn json(&self, deployment: &str, at: i64) -> String {
        let (kind, run, dag) = match self {
            Self::Run { run_id, dag_id } => ("run", Some(run_id), Some(dag_id)),
            Self::Dag { dag_id } => ("dag", None, Some(dag_id)),
            Self::Workers => ("workers", None, None),
            Self::Refresh => ("refresh", None, None),
        };
        serde_json::json!({
            "deployment": deployment,
            "kind": kind,
            "run": run,
            "dag": dag,
            "at": at,
        })
        .to_string()
    }
}

/// `GET /events`: one browser's stream of every deployment's changes (decision 175: its tabs
/// share it, and each page decides which concern it). First a `hello` (now, in milliseconds
/// since the epoch), then a `change` event for each change (JSON: `deployment`, `kind`
/// `run`/`dag`/`workers`/`refresh`, `run`, `dag`, `at`), the same change at most once a
/// [`COALESCE`] per deployment (never dropped: sent when that time is up), and a `ping` every
/// [`KEEP_ALIVE`] (a page polls when even those stop coming: a proxy buffering the stream).
pub async fn events<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Extension(relays): Extension<Arc<Relays>>,
    follow: Result<Query<Follow>, QueryRejection>,
) -> Response {
    let Ok(Query(follow)) = follow else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let (sender, receiver) = mpsc::channel::<Result<Event, Infallible>>(64);
    // Sent before any change: the channel is empty.
    let _ = sender.try_send(Ok(Event::default()
        .event("hello")
        .data(now_millis().to_string())));
    for (index, deployment) in ui.deployments.iter().enumerate() {
        let changes = relays.subscribe(&ui, index);
        let missed = follow
            .since
            .is_some_and(|since| relays.changed_after(index, since));
        tokio::spawn(forward(
            (changes, missed),
            sender.clone(),
            deployment.name.clone(),
            ui.timings,
        ));
    }
    streamed(receiver, ui.timings.keep_alive)
}

/// Hands one deployment's changes to a browser's stream until it goes away: a change at once,
/// the same change again at most once a [`COALESCE`] (sent when that time is up, so the last is
/// never missed).
async fn forward(
    (mut changes, missed): (broadcast::Receiver<Change>, bool),
    sender: mpsc::Sender<Result<Event, Infallible>>,
    deployment: String,
    timings: UiTimings,
) {
    // When each change (still held back, or not) was last sent.
    let mut sent: Vec<(Change, tokio::time::Instant)> = Vec::new();
    // Changes held back, in order, each once.
    let mut held: Vec<Change> = Vec::new();
    if missed {
        held.push(Change::Refresh);
    }
    let mut pings = tokio::time::interval_at(
        tokio::time::Instant::now() + timings.keep_alive,
        timings.keep_alive,
    );
    loop {
        let now = tokio::time::Instant::now();
        sent.retain(|(_, at)| now.duration_since(*at) < timings.coalesce);
        // Those held back whose time is up, and when the next one's is.
        let (due, waiting): (Vec<Change>, Vec<Change>) = held
            .drain(..)
            .partition(|change| !sent.iter().any(|(c, _)| c == change));
        held = waiting;
        for change in due {
            sent.push((change.clone(), now));
            let event = Event::default()
                .event("change")
                .data(change.json(&deployment, now_millis()));
            if sender.send(Ok(event)).await.is_err() {
                return;
            }
        }
        let next = held
            .iter()
            .filter_map(|change| sent.iter().find(|(c, _)| c == change))
            .map(|(_, at)| *at + timings.coalesce)
            .min();
        let ping = tokio::select! {
            () = sender.closed() => return,
            _ = pings.tick() => true,
            () = tokio::time::sleep_until(next.unwrap_or(now)), if next.is_some() => false,
            change = changes.recv() => {
                let change = match change {
                    Ok(change) => change,
                    Err(broadcast::error::RecvError::Lagged(_)) => Change::Refresh,
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                if !held.contains(&change) {
                    held.push(change);
                }
                false
            }
        };
        if ping
            && sender
                .send(Ok(Event::default().event("ping").data("")))
                .await
                .is_err()
        {
            return;
        }
    }
}

/// Now, in milliseconds since the epoch.
fn now_millis() -> i64 {
    jiff::Timestamp::now().as_millisecond()
}

/// `receiver`'s events as a server-sent stream, a comment every [`KEEP_ALIVE`], proxies asked
/// not to buffer it (`X-Accel-Buffering: no`, nginx's).
fn streamed(receiver: mpsc::Receiver<Result<Event, Infallible>>, keep_alive: Duration) -> Response {
    (
        [("x-accel-buffering", "no")],
        Sse::new(ReceiverStream::new(receiver)).keep_alive(KeepAlive::new().interval(keep_alive)),
    )
        .into_response()
}

/// The line ending a tail that gave up, saying why.
fn gave_up(reason: &str) -> String {
    format!(
        "\n<span class=\"error\">The log can't be followed any more: {}</span>\n",
        ansi::to_html(reason)
    )
}

/// The log tail's query: which try, where its log continues, and how it was shown.
#[derive(Debug, Clone, Deserialize)]
pub struct TailQuery {
    /// The try.
    #[serde(rename = "try")]
    pub try_number: u32,
    /// The first byte not shown yet.
    pub from: u64,
    /// The instance of a mapped task.
    #[serde(default)]
    pub map_index: Option<u32>,
    /// The colours in force where the log continues (`ansi::Converter::sgr`).
    #[serde(default)]
    pub style: String,
    /// Since when the log has been out of reach, in milliseconds since the epoch.
    #[serde(default)]
    pub out: Option<i64>,
}

/// `GET /d/<deployment>/runs/<run>/tasks/<task>/tail?try=&from=` (decision 175: polled, so
/// it holds no connection): the log from `from` as HTML, the colours carried from the part
/// before (`style`), followed by the element reading the next part (in [`TAIL_INTERVAL`], at
/// once when more is waiting), which replaces itself with the answer. Nothing follows once the
/// attempt has ended and all of it is shown, or once the log can't be read (said where it is
/// shown): gone, or out of reach for [`TAIL_GIVE_UP`].
pub async fn tail<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id, task_id)): Path<(String, String, String)>,
    query: Result<Query<TailQuery>, QueryRejection>,
    uri: OriginalUri,
    headers: HeaderMap,
) -> Response {
    let Ok(Query(query)) = query else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let part = deployment
        .client
        .instance_log_part(
            &run_id,
            (&task_id, query.map_index),
            query.try_number,
            query.from,
            TAIL_PART,
        )
        .await;
    let mut colours = ansi::Converter::resume(&query.style);
    let (html, next) = match part {
        Ok(part) => {
            let html = colours.push(&part.content);
            // Bytes of an escape sequence cut at the part's end: read again with the rest.
            let from = part
                .next
                .saturating_sub(colours.held() as u64)
                .max(query.from);
            let done = !part.running && part.next >= part.size;
            // Most of a part read (whole lines only: rarely all of it), and more past it:
            // read on at once.
            let now =
                part.next.saturating_sub(query.from) >= TAIL_PART / 2 && part.next < part.size;
            (html, (!done).then_some((from, None, now)))
        }
        // Gone for good: the attempt has no such log (any more), or the request is wrong.
        Err(ClientError::Server { status, message }) if status != 503 => (gave_up(&message), None),
        // Out of reach (the worker, or the webserver): tried again for a while.
        Err(error) => {
            let out = query.out.unwrap_or_else(now_millis);
            let given = i64::try_from(ui.timings.tail_give_up.as_millis()).unwrap_or(i64::MAX);
            if now_millis().saturating_sub(out) >= given {
                (gave_up(&error.to_string()), None)
            } else {
                (String::new(), Some((query.from, Some(out), false)))
            }
        }
    };
    let mut body = html;
    if let Some((from, out, now)) = next {
        let mut url = format!(
            "{}{}?try={}&from={from}",
            prefix(&headers),
            uri.path(),
            query.try_number
        );
        if let Some(index) = query.map_index {
            let _ = write!(url, "&map_index={index}");
        }
        let style = colours.sgr();
        if !style.is_empty() {
            let _ = write!(url, "&style={}", style.replace(';', "%3B"));
        }
        if let Some(out) = out {
            let _ = write!(url, "&out={out}");
        }
        body.push_str(&tail_reader(&url, now, ui.timings.tail_interval));
    }
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

/// The element reading a log's next part from `url` (with the prefix): in [`TAIL_INTERVAL`],
/// or at once; again a second after the UI didn't answer (`retry`, sent by the page).
#[must_use]
pub fn tail_reader(url: &str, now: bool, interval: Duration) -> String {
    let wait = if now { 0 } else { interval.as_millis() };
    format!(
        "<span class=\"log-tail\" hx-get=\"{}\" hx-trigger=\"load delay:{wait}ms, retry delay:1s\" \
         hx-target=\"this\" hx-swap=\"outerHTML\" hx-disinherit=\"*\"></span>",
        url.replace('&', "&amp;").replace('"', "&quot;")
    )
}
