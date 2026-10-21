//! What every request handler shares.

use axum::http::HeaderMap;
use jiff::Timestamp;
use schedr_db::Database;
use schedr_model::SchedrVersion;

#[cfg(test)]
mod tests;

/// The user name recorded on commands when no identity is known.
pub const ANONYMOUS: &str = "anonymous";

/// The webserver's shared state.
#[derive(Debug, Clone)]
pub struct ApiState {
    /// The database (read directly; actions are command rows).
    pub db: Database,
    /// The server's version, for the version check.
    pub version: SchedrVersion,
    /// The server's full version, sent in `Schedr-Server-Version`.
    pub version_text: &'static str,
    /// The header naming the user, recorded on commands for audit (`[webserver.auth]
    /// identity_header`, e.g. `x-forwarded-user`); never used to allow or deny anything.
    pub identity_header: Option<String>,
    /// The clock (replaced in tests).
    pub clock: fn() -> Timestamp,
}

impl ApiState {
    /// State over `db` for this build of schedr, with the system clock.
    #[must_use]
    pub fn new(db: Database, identity_header: Option<String>) -> Self {
        Self {
            db,
            version: SchedrVersion::current(),
            version_text: env!("CARGO_PKG_VERSION"),
            identity_header,
            clock: Timestamp::now,
        }
    }

    /// The user a request comes from: the identity header's value, or `anonymous`.
    #[must_use]
    pub fn user(&self, headers: &HeaderMap) -> String {
        self.identity_header
            .as_deref()
            .and_then(|name| headers.get(name))
            .and_then(|value| value.to_str().ok())
            .filter(|name| !name.is_empty())
            .unwrap_or(ANONYMOUS)
            .to_owned()
    }
}
