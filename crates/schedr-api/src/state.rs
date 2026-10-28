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
    /// What changed, for `GET /api/v1/events`.
    pub feed: Feed,
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
            feed,
        }
    }

    /// This state recording its metrics in `metrics` (registered by the caller).
    #[must_use]
    pub fn with_metrics(self, metrics: ApiMetrics) -> Self {
        Self { metrics, ..self }
    }

    /// This state's live pages refreshed every `interval` when the database's changes can't be
    /// followed (`[webserver] refresh_interval`).
    #[must_use]
    pub fn with_refresh(self, interval: std::time::Duration) -> Self {
        Self {
            feed: Feed::new(self.db.clone()).with_refresh(interval),
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
