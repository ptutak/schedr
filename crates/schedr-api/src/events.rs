//! `GET /api/v1/changes`: what changed, as server-sent events, for the UI's live pages (see
//! archive/PROJECT.md, Web UI: 34A). Only what changed, never the new state: a client reads that
//! from the other endpoints.
//!
//! Events: `run` (`{"run_id", "dag_id"}`: the run, its tasks or attempts), `dag`
//! (`{"dag_id"}`), `workers` (`{}`), and `refresh` (`{}`: anything may have changed, when
//! changes were missed or can't be followed; then every `refresh_interval`, 15 s
//! by default). A comment
//! every [`KEEP_ALIVE`] keeps idle connections open through proxies.
//!
//! One follower per webserver reads the database's changes ([`Database::changes`]), started
//! by the first subscriber, and hands them to every connection.

use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use schedr_db::Database;
use schedr_db::changes::Change;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::state::ApiState;

#[cfg(test)]
mod tests;

/// How often `refresh` is sent when the database's changes can't be followed, unless set
/// otherwise (`refresh_interval`).
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// How often a follower checks that the database still announces its changes (turned off by
/// `schedr db migrate` meanwhile, the pages are refreshed instead).
const RECHECK: Duration = Duration::from_secs(60);

/// How often an idle connection gets a comment.
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// How long the follower waits after the database fails before trying again.
const RETRY: Duration = Duration::from_secs(1);

/// Changes a slow connection may fall behind by before it gets `refresh` instead.
const BACKLOG: usize = 1_024;

/// What the follower hands out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Live {
    /// Something changed.
    Change(Change),
    /// Anything may have changed.
    Refresh,
}

/// The webserver's feed of changes: a follower started by the first subscriber.
#[derive(Debug, Clone)]
pub struct Feed {
    db: Database,
    refresh: Duration,
    timings: EventTimings,
    sender: Arc<OnceLock<broadcast::Sender<Live>>>,
}

/// How the webserver follows the database's changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTimings {
    /// How often whether they are announced is looked at (`events_recheck`).
    pub recheck: Duration,
    /// How often a stream gets a comment (`events_keep_alive`).
    pub keep_alive: Duration,
    /// How soon a failure is tried again (`events_retry`).
    pub retry: Duration,
    /// How often changes are read where they aren't announced (`changes_poll_interval`).
    pub changes_poll: Duration,
}

impl Default for EventTimings {
    /// What every webserver runs with.
    fn default() -> Self {
        Self {
            recheck: RECHECK,
            keep_alive: KEEP_ALIVE,
            retry: RETRY,
            changes_poll: schedr_db::changes::POLL_INTERVAL,
        }
    }
}

impl Feed {
    /// A feed over `db`, not following it yet.
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            refresh: REFRESH_INTERVAL,
            timings: EventTimings::default(),
            sender: Arc::new(OnceLock::new()),
        }
    }

    /// How often a stream gets a comment.
    #[must_use]
    pub const fn keep_alive(&self) -> Duration {
        self.timings.keep_alive
    }

    /// This feed, following changes as `timings` say.
    #[must_use]
    pub fn with_timings(self, timings: EventTimings) -> Self {
        Self { timings, ..self }
    }

    /// This feed sending `refresh` every `interval` when changes can't be followed.
    #[must_use]
    pub fn with_refresh(self, interval: Duration) -> Self {
        Self {
            refresh: interval.max(Duration::from_secs(1)),
            ..self
        }
    }

    /// The changes from now on (the follower started if it isn't).
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Live> {
        self.sender
            .get_or_init(|| {
                let (sender, _) = broadcast::channel(BACKLOG);
                tokio::spawn(follow(
                    self.db.clone(),
                    sender.clone(),
                    self.refresh,
                    self.timings,
                ));
                sender
            })
            .subscribe()
    }
}

/// Reads the database's changes for as long as the webserver runs, sending each; when they
/// can't be followed, `refresh` every [`REFRESH_INTERVAL`] instead. A failure is `refresh`, then
/// another try.
async fn follow(
    db: Database,
    sender: broadcast::Sender<Live>,
    refresh: Duration,
    timings: EventTimings,
) {
    loop {
        let on = match db.change_notifications().await {
            Ok(on) => on,
            Err(error) => {
                tracing::warn!(%error, "can't read whether the database announces its changes");
                tokio::time::sleep(timings.retry).await;
                continue;
            }
        };
        let changes = if on {
            db.changes(timings.changes_poll).await
        } else {
            Ok(None)
        };
        match changes {
            Ok(Some(mut changes)) => {
                let mut check = tokio::time::interval_at(
                    tokio::time::Instant::now() + timings.recheck,
                    timings.recheck,
                );
                loop {
                    tokio::select! {
                        batch = changes.next() => match batch {
                            Ok(batch) => {
                                for change in batch {
                                    let live = match change {
                                        Change::Missed => Live::Refresh,
                                        change => Live::Change(change),
                                    };
                                    // No one listening is fine.
                                    let _ = sender.send(live);
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, "following the database's changes failed");
                                let _ = sender.send(Live::Refresh);
                                tokio::time::sleep(timings.retry).await;
                                break;
                            }
                        },
                        _ = check.tick() => {
                            // Turned off meanwhile (`schedr db migrate`): no more changes will
                            // come, so the pages are refreshed instead.
                            if matches!(db.change_notifications().await, Ok(false)) {
                                let _ = sender.send(Live::Refresh);
                                break;
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                if on {
                    tracing::info!(
                        "the database's changes can't be followed (notifications off, or a \
                         pooler in transaction mode): live pages refresh every {} s",
                        refresh.as_secs()
                    );
                } else {
                    tracing::info!(
                        "the database doesn't announce its changes (`[database] \
                         change_notifications = false`): live pages refresh every {} s",
                        refresh.as_secs()
                    );
                }
                let mut ticks =
                    tokio::time::interval_at(tokio::time::Instant::now() + refresh, refresh);
                loop {
                    ticks.tick().await;
                    let _ = sender.send(Live::Refresh);
                    // Turned on meanwhile: followed from now on.
                    if !on && matches!(db.change_notifications().await, Ok(true)) {
                        break;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "can't follow the database's changes; trying again");
                tokio::time::sleep(timings.retry).await;
            }
        }
    }
}

/// `live` as a server-sent event.
pub fn event(live: &Live) -> Event {
    let (name, data) = match live {
        Live::Change(Change::Run { run_id, dag_id }) => {
            ("run", json!({"run_id": run_id, "dag_id": dag_id.as_str()}))
        }
        Live::Change(Change::Dag { dag_id }) => ("dag", json!({"dag_id": dag_id.as_str()})),
        Live::Change(Change::Workers) => ("workers", json!({})),
        Live::Change(Change::Missed) | Live::Refresh => ("refresh", json!({})),
    };
    Event::default().event(name).data(data.to_string())
}

/// What changed, as it happens: `run`, `dag`, `workers` and `refresh` events (see the module
/// documentation).
#[utoipa::path(get, path = "/api/v1/changes", tag = "runs",
    responses((status = 200, content_type = "text/event-stream",
        description = "Server-sent events: `run` (`{\"run_id\", \"dag_id\"}`), `dag` \
            (`{\"dag_id\"}`), `workers` (`{}`) and `refresh` (`{}`: anything may have \
            changed)")))]
pub async fn events(State(state): State<ApiState>) -> impl axum::response::IntoResponse {
    let mut changes = state.feed.subscribe();
    let (sender, receiver) = mpsc::channel::<Result<Event, Infallible>>(16);
    tokio::spawn(async move {
        loop {
            let next = tokio::select! {
                () = sender.closed() => return,
                next = changes.recv() => next,
            };
            let live = match next {
                Ok(live) => live,
                // Fell behind: what was skipped may be anything.
                Err(broadcast::error::RecvError::Lagged(_)) => Live::Refresh,
                Err(broadcast::error::RecvError::Closed) => return,
            };
            // The client went away.
            if sender.send(Ok(event(&live))).await.is_err() {
                return;
            }
        }
    });
    // Proxies asked not to buffer it (nginx's header; others may anyway: the UI then polls).
    (
        [("x-accel-buffering", "no")],
        Sse::new(ReceiverStream::new(receiver))
            .keep_alive(KeepAlive::new().interval(state.feed.keep_alive())),
    )
}
