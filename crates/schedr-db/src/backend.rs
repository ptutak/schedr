//! The two database backends and their migrations.
//!
//! Production uses Postgres, local development SQLite (see PROJECT.md, State storage). Each
//! backend has its own migration files because column types differ (`TIMESTAMPTZ` / `UUID` /
//! `JSONB` in Postgres, text in SQLite); both sets always have the same versions.

use sqlx::migrate::Migrator;

#[cfg(test)]
mod tests;

/// The migrations for SQLite, built into the binary.
static SQLITE_MIGRATIONS: Migrator = sqlx::migrate!("migrations/sqlite");

/// The migrations for Postgres, built into the binary.
static POSTGRES_MIGRATIONS: Migrator = sqlx::migrate!("migrations/postgres");

/// A database backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    /// SQLite: local development and standalone mode.
    Sqlite,
    /// Postgres: production.
    Postgres,
}

/// The URL doesn't name a supported database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unsupported database URL `{0}` (use `sqlite:...` or `postgres://...`)")]
pub struct UnsupportedUrl(pub String);

impl Backend {
    /// The backend a connection URL names: `sqlite:` or `postgres://` / `postgresql://`.
    ///
    /// # Errors
    ///
    /// Returns [`UnsupportedUrl`] for any other scheme. The URL in the error has any password
    /// removed.
    pub fn from_url(url: &str) -> Result<Self, UnsupportedUrl> {
        if url.starts_with("sqlite:") {
            Ok(Self::Sqlite)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres)
        } else {
            Err(UnsupportedUrl(redact_password(url)))
        }
    }

    /// The migrations for this backend.
    #[must_use]
    pub fn migrator(self) -> &'static Migrator {
        match self {
            Self::Sqlite => &SQLITE_MIGRATIONS,
            Self::Postgres => &POSTGRES_MIGRATIONS,
        }
    }

    /// The versions of this backend's migrations, in order.
    #[must_use]
    pub fn migration_versions(self) -> Vec<i64> {
        self.migrator().iter().map(|m| m.version).collect()
    }
}

/// `url` with the password of its user-info part replaced by `***`, for messages and logs.
#[must_use]
pub fn redact_password(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((user_info, host)) => match user_info.split_once(':') {
            Some((user, _password)) => format!("{scheme}://{user}:***@{host}{path}"),
            None => url.to_owned(),
        },
        None => url.to_owned(),
    }
}
