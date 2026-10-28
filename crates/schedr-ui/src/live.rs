//! Live pages (M3.3, decisions 34A and 23A): what changed in each deployment, relayed to the
//! browsers, and a running attempt's log as it grows.
//!
//! - **Relays:** one per deployment, started by the first browser that wants its changes: it
//!   reads the webserver's `GET /api/v1/events` and hands each change to every browser
//!   following that deployment; when the stream ends or can't be opened it tries again (after
//!   1 s, doubling to 30 s), and once it is back sends `refresh`, since anything may have
//!   changed meanwhile.
//! - **`GET /events`:** a page's own stream: a `change` event whenever something it shows may
//!   have changed (its run, its DAG, a list's DAGs or runs, the workers), its data the
//!   deployment's name; htmx then reloads the page's live parts (`hx-trigger="sse:change"`).
//! - **The log tail:** `GET /d/<deployment>/runs/<run>/tasks/<task>/tail?try=&from=` reads
//!   the log from `from` about once a second while the attempt runs and sends what is new as a
//!   `log` event (HTML, colours converted), then `end`.

use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Extension;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use schedr_client::{ClientError, ServerEvent, Transport};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{Ui, ansi};

#[cfg(test)]
mod tests;

/// How often a page's stream gets a `ping` (and the log tail a comment).
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// A page's stream sends at most one `change` this often (per deployment).
pub const COALESCE: Duration = Duration::from_secs(1);

/// How often the log tail reads the log.
pub const TAIL_INTERVAL: Duration = Duration::from_secs(1);

/// How long the log tail keeps trying a log out of reach before it says so and stops.
pub const TAIL_GIVE_UP: Duration = Duration::from_secs(30);

/// At most this much of a log a read.
const TAIL_PART: u64 = 64 * 1024;

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
}

impl Relays {
    /// Relays for `deployments` deployments, none started.
    #[must_use]
    pub fn new(deployments: usize) -> Self {
        Self {
            senders: (0..deployments).map(|_| OnceLock::new()).collect(),
        }
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
                tokio::spawn(relay(Arc::clone(ui), index, sender.clone()));
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
) {
    let deployment = &ui.deployments[index];
    let mut wait = RETRY_FIRST;
    let mut lost = false;
    loop {
        match deployment.client.events().await {
            Ok(mut events) => {
                if lost {
                    let _ = sender.send(Change::Refresh);
                }
                wait = RETRY_FIRST;
                while let Some(Ok(event)) = events.next().await {
                    if let Some(change) = Change::of(&event) {
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
        wait = (wait * 2).min(RETRY_MAX);
    }
}

/// What a page follows (`GET /events`'s query).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Follow {
    /// Only this deployment (every one when absent).
    #[serde(default)]
    pub deployment: Option<String>,
    /// Only this run.
    #[serde(default)]
    pub run: Option<String>,
    /// Only this DAG (its runs too).
    #[serde(default)]
    pub dag: Option<String>,
    /// What kinds of change matter, comma-separated: `run`, `dag`, `workers` (every kind when
    /// absent).
    #[serde(default)]
    pub kinds: Option<String>,
}

impl Follow {
    /// Whether `change` may change what the page shows.
    #[must_use]
    pub fn wants(&self, change: &Change) -> bool {
        let kind = |name: &str| {
            self.kinds
                .as_deref()
                .is_none_or(|kinds| kinds.split(',').any(|k| k == name))
        };
        match change {
            Change::Refresh => true,
            Change::Run { run_id, dag_id } => {
                kind("run")
                    && self.run.as_ref().is_none_or(|r| r == run_id)
                    && self.dag.as_ref().is_none_or(|d| d == dag_id)
            }
            Change::Dag { dag_id } => {
                kind("dag") && self.run.is_none() && self.dag.as_ref().is_none_or(|d| d == dag_id)
            }
            Change::Workers => kind("workers") && self.run.is_none() && self.dag.is_none(),
        }
    }
}

/// `GET /events`: a `change` event (its data the deployment's name) whenever what the page
/// follows may have changed, at most one a [`COALESCE`] per deployment (the last one never
/// dropped), and a `ping` every [`KEEP_ALIVE`] (the page polls when even those stop coming:
/// a proxy buffering the stream).
pub async fn events<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Extension(relays): Extension<Arc<Relays>>,
    follow: Result<Query<Follow>, QueryRejection>,
) -> Response {
    let Ok(Query(follow)) = follow else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let (sender, receiver) = mpsc::channel::<Result<Event, Infallible>>(16);
    for (index, deployment) in ui.deployments.iter().enumerate() {
        if follow
            .deployment
            .as_ref()
            .is_some_and(|d| *d != deployment.name)
        {
            continue;
        }
        let changes = relays.subscribe(&ui, index);
        tokio::spawn(forward(
            changes,
            sender.clone(),
            follow.clone(),
            deployment.name.clone(),
        ));
    }
    streamed(receiver)
}

/// Hands one deployment's changes that `follow` wants to a page's stream until the page goes
/// away: the first at once, then at most one every [`COALESCE`], a change in between sent when
/// that time is up (htmx then reloads once, and never misses the last change).
async fn forward(
    mut changes: broadcast::Receiver<Change>,
    sender: mpsc::Sender<Result<Event, Infallible>>,
    follow: Follow,
    deployment: String,
) {
    let mut last_sent: Option<tokio::time::Instant> = None;
    let mut pending = false;
    let mut pings = tokio::time::interval_at(tokio::time::Instant::now() + KEEP_ALIVE, KEEP_ALIVE);
    loop {
        let due = last_sent.map_or_else(tokio::time::Instant::now, |at| at + COALESCE);
        let send = tokio::select! {
            () = sender.closed() => return,
            _ = pings.tick() => Some(Event::default().event("ping").data("")),
            () = tokio::time::sleep_until(due), if pending => {
                pending = false;
                last_sent = Some(tokio::time::Instant::now());
                Some(Event::default().event("change").data(deployment.as_str()))
            }
            change = changes.recv() => {
                let change = match change {
                    Ok(change) => change,
                    Err(broadcast::error::RecvError::Lagged(_)) => Change::Refresh,
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                if !follow.wants(&change) {
                    None
                } else if last_sent.is_some_and(|at| at.elapsed() < COALESCE) {
                    pending = true;
                    None
                } else {
                    last_sent = Some(tokio::time::Instant::now());
                    Some(Event::default().event("change").data(deployment.as_str()))
                }
            }
        };
        if let Some(event) = send
            && sender.send(Ok(event)).await.is_err()
        {
            return;
        }
    }
}

/// `receiver`'s events as a server-sent stream, a comment every [`KEEP_ALIVE`], proxies asked
/// not to buffer it (`X-Accel-Buffering: no`, nginx's).
fn streamed(receiver: mpsc::Receiver<Result<Event, Infallible>>) -> Response {
    (
        [("x-accel-buffering", "no")],
        Sse::new(ReceiverStream::new(receiver)).keep_alive(KeepAlive::new().interval(KEEP_ALIVE)),
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

/// The log tail's query: which try, and where its log continues.
#[derive(Debug, Clone, Deserialize)]
pub struct TailQuery {
    /// The try.
    #[serde(rename = "try")]
    pub try_number: u32,
    /// The first byte not shown yet.
    pub from: u64,
}

/// `GET /d/<deployment>/runs/<run>/tasks/<task>/tail?try=&from=`: the log from `from` as it
/// grows (`log` events, HTML, the colours carried from one part to the next), then `end` once
/// the attempt has ended and all of it is sent, or once the log can't be read (said in a last
/// `log` event): gone, or out of reach for [`TAIL_GIVE_UP`].
pub async fn tail<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id, task_id)): Path<(String, String, String)>,
    query: Result<Query<TailQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(index) = ui.deployments.iter().position(|d| d.name == deployment) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let (sender, receiver) = mpsc::channel::<Result<Event, Infallible>>(16);
    tokio::spawn(async move {
        let client = &ui.deployments[index].client;
        let mut from = query.from;
        let mut colours = ansi::Converter::new();
        // Since when the log has been out of reach (a worker briefly unreachable is retried).
        let mut failing: Option<tokio::time::Instant> = None;
        loop {
            let part = client
                .log_part(&run_id, &task_id, query.try_number, from, TAIL_PART)
                .await;
            let mut more_now = false;
            let (html, done) = match part {
                Ok(part) => {
                    failing = None;
                    // A full part: more is waiting, read it at once.
                    more_now = part.next - part.from >= TAIL_PART;
                    from = part.next;
                    let html = colours.push(&part.content);
                    (html, !part.running && part.next >= part.size)
                }
                // Gone for good: the attempt has no such log (any more), or the request is
                // wrong. Said where the log is shown.
                Err(ClientError::Server { status, message }) if status != 503 => {
                    (gave_up(&message), true)
                }
                // Out of reach (the worker, or the webserver): tried again for a while.
                Err(error) => {
                    let since = *failing.get_or_insert_with(tokio::time::Instant::now);
                    if since.elapsed() >= TAIL_GIVE_UP {
                        (gave_up(&error.to_string()), true)
                    } else {
                        (String::new(), false)
                    }
                }
            };
            if !html.is_empty()
                && sender
                    .send(Ok(Event::default().event("log").data(html)))
                    .await
                    .is_err()
            {
                return;
            }
            if done {
                let _ = sender
                    .send(Ok(Event::default().event("end").data("")))
                    .await;
                return;
            }
            if sender.is_closed() {
                return;
            }
            if !more_now {
                tokio::time::sleep(TAIL_INTERVAL).await;
            }
        }
    });
    streamed(receiver)
}
