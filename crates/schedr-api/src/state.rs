//! What every request handler shares.

use std::sync::Arc;

use jiff::Timestamp;
use schedr_db::Database;
use schedr_model::SchedrVersion;

use crate::auth::Auth;
use crate::events::Feed;
use crate::logs::{HttpLogSource, LogSource};
use crate::metrics::ApiMetrics;

#[cfg(test)]
mod tests;

pub use crate::auth::ANONYMOUS;

/// The webserver's shared state.
#[derive(Debug, Clone)]
pub struct ApiState {
    /// The database (read directly; actions are command rows).
    pub db: Database,
    /// The server's version, for the version check.
    pub version: SchedrVersion,
    /// The server's full version, sent in `Schedr-Server-Version`.
    pub version_text: &'static str,
    /// Who the user is (`[webserver.auth]`, and the UI's proof: see [`crate::auth`]).
    pub auth: Auth,
    /// The clock (replaced in tests).
    pub clock: fn() -> Timestamp,
    /// Where task logs come from (the workers).
    pub logs: Arc<dyn LogSource>,
    /// The webserver's metrics.
    pub metrics: ApiMetrics,
    /// How long a connection test may take when its request doesn't say.
    pub connection_test_timeout: std::time::Duration,
    /// The workers' heartbeat interval: a poke trigger whose last call ended longer ago than
    /// three times its `every` plus this is stuck.
    pub heartbeat_interval: std::time::Duration,
    /// What changed, for `GET /api/v1/changes`.
    pub feed: Feed,
    /// The largest posted event's data (`[events] max_size`).
    pub max_event_size: usize,
    /// This webserver as a node of its deployment (`GET /api/v1/deployment`), its
    /// configuration masked.
    pub node: Arc<crate::views::NodeView>,
    /// What this process uses now (decision 463); none by default (the binary reads `/proc`).
    pub usage: fn() -> Option<schedr_model::usage::NodeUsage>,
}

impl ApiState {
    /// State over `db` for this build of schedr, with the system clock, reading logs from
    /// workers over plain HTTP (see [`Self::with_logs`]).
    #[must_use]
    pub fn new(db: Database, identity_header: Option<String>) -> Self {
        let feed = Feed::new(db.clone());
        Self {
            db,
            version: SchedrVersion::current(),
            version_text: env!("CARGO_PKG_VERSION"),
            auth: Auth::identity(identity_header),
            clock: Timestamp::now,
            logs: Arc::new(HttpLogSource::new(None)),
            metrics: ApiMetrics::default(),
            connection_test_timeout: crate::routes::CONNECTION_TEST_TIMEOUT,
            // The configuration's default; the webserver passes its own.
            heartbeat_interval: std::time::Duration::from_secs(10),
            feed,
            max_event_size: crate::named_events::DEFAULT_MAX_EVENT_SIZE,
            node: Arc::new(crate::views::NodeView {
                process_id: String::new(),
                host: String::new(),
                schedr_version: env!("CARGO_PKG_VERSION").to_owned(),
                config: serde_json::json!({}),
                usage: None,
            }),
            usage: || None,
        }
    }

    /// This state, giving a connection test `timeout` when its request doesn't say.
    #[must_use]
    pub fn with_connection_test_timeout(self, timeout: std::time::Duration) -> Self {
        Self {
            connection_test_timeout: timeout,
            ..self
        }
    }

    /// This state, judging a poke trigger stuck with `interval`, the workers' heartbeat.
    #[must_use]
    pub fn with_heartbeat_interval(self, interval: std::time::Duration) -> Self {
        Self {
            heartbeat_interval: interval,
            ..self
        }
    }

    /// This state, refusing posted events whose data is over `bytes` (`[events] max_size`).
    #[must_use]
    pub fn with_max_event_size(self, bytes: usize) -> Self {
        Self {
            max_event_size: bytes,
            ..self
        }
    }

    /// This state reading what its process uses with `usage` (decision 463).
    #[must_use]
    pub fn with_usage(self, usage: fn() -> Option<schedr_model::usage::NodeUsage>) -> Self {
        Self { usage, ..self }
    }

    /// This state's webserver shown as `node` (its configuration masked by the caller).
    #[must_use]
    pub fn with_node(self, node: crate::views::NodeView) -> Self {
        Self {
            node: Arc::new(node),
            ..self
        }
    }

    /// This state recording its metrics in `metrics` (registered by the caller).
    #[must_use]
    pub fn with_metrics(self, metrics: ApiMetrics) -> Self {
        Self { metrics, ..self }
    }

    /// This state's live pages refreshed every `interval` when the database's changes can't be
    /// followed (`refresh_interval`).
    #[must_use]
    pub fn with_refresh(self, interval: std::time::Duration) -> Self {
        Self {
            feed: self.feed.clone().with_refresh(interval),
            ..self
        }
    }

    /// This state following the database's changes as `timings` say.
    #[must_use]
    pub fn with_event_timings(self, timings: crate::events::EventTimings) -> Self {
        Self {
            feed: self.feed.clone().with_timings(timings),
            ..self
        }
    }

    /// This state telling who the user is by `auth`.
    #[must_use]
    pub fn with_auth(self, auth: Auth) -> Self {
        Self { auth, ..self }
    }

    /// This state reading logs from `logs` (over TLS in `app` mode).
    #[must_use]
    pub fn with_logs(self, logs: Arc<dyn LogSource>) -> Self {
        Self { logs, ..self }
    }
}
