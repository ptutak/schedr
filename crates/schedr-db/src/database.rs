//! A connection pool to the schedr database, and schema migration and checks.

use std::str::FromStr;

use sqlx::migrate::MigrateError;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use crate::backend::{Backend, UnsupportedUrl};
use crate::status::SchemaStatus;

#[cfg(test)]
mod tests;

/// Why a database operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// The URL doesn't name a supported database.
    #[error(transparent)]
    UnsupportedUrl(#[from] UnsupportedUrl),
    /// A query or connection failed.
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Applying migrations failed.
    #[error("migration failed: {0}")]
    Migrate(#[from] MigrateError),
    /// The schema isn't usable by this binary: `schedr db migrate` must run first.
    #[error("the database schema is outdated ({status:?}); run `schedr db migrate` first")]
    SchemaNotUsable {
        /// What was found.
        status: SchemaStatus,
    },
}

/// A connection pool to the schedr database.
#[derive(Debug, Clone)]
pub enum Database {
    /// SQLite.
    Sqlite(SqlitePool),
    /// Postgres.
    Postgres(PgPool),
}

impl Database {
    /// Connects to the database a URL names. SQLite files are created if missing, and
    /// foreign keys are enforced. An in-memory SQLite database (`sqlite::memory:`) uses a
    /// single connection, since each connection would otherwise see its own empty database.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UnsupportedUrl`] for an unknown scheme, or [`DbError::Sqlx`] if the
    /// URL is malformed or the connection fails.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        match Backend::from_url(url)? {
            Backend::Sqlite => {
                let options = SqliteConnectOptions::from_str(url)?
                    .create_if_missing(true)
                    .foreign_keys(true);
                let max_connections = if url.contains(":memory:") { 1 } else { 8 };
                let pool = SqlitePoolOptions::new()
                    .max_connections(max_connections)
                    .connect_with(options)
                    .await?;
                Ok(Self::Sqlite(pool))
            }
            Backend::Postgres => Ok(Self::Postgres(PgPoolOptions::new().connect(url).await?)),
        }
    }

    /// The backend of this database.
    #[must_use]
    pub const fn backend(&self) -> Backend {
        match self {
            Self::Sqlite(_) => Backend::Sqlite,
            Self::Postgres(_) => Backend::Postgres,
        }
    }

    /// Applies the migrations not applied yet (what `schedr db migrate` does).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Migrate`] if a migration fails or the applied migrations don't match
    /// the built-in ones.
    pub async fn migrate(&self) -> Result<(), DbError> {
        let migrator = self.backend().migrator();
        match self {
            Self::Sqlite(pool) => migrator.run(pool).await?,
            Self::Postgres(pool) => migrator.run(pool).await?,
        }
        Ok(())
    }

    /// The versions of the migrations applied successfully, in order; empty if none.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlx`] if the query fails.
    pub async fn applied_migrations(&self) -> Result<Vec<i64>, DbError> {
        const EXISTS_SQLITE: &str =
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'";
        const EXISTS_POSTGRES: &str =
            "SELECT COUNT(*) FROM pg_tables WHERE tablename = '_sqlx_migrations'";
        const VERSIONS: &str =
            "SELECT version FROM _sqlx_migrations WHERE success ORDER BY version";
        let versions = match self {
            Self::Sqlite(pool) => {
                let exists: i64 = sqlx::query_scalar(EXISTS_SQLITE).fetch_one(pool).await?;
                if exists == 0 {
                    return Ok(Vec::new());
                }
                sqlx::query_scalar(VERSIONS).fetch_all(pool).await?
            }
            Self::Postgres(pool) => {
                let exists: i64 = sqlx::query_scalar(EXISTS_POSTGRES).fetch_one(pool).await?;
                if exists == 0 {
                    return Ok(Vec::new());
                }
                sqlx::query_scalar(VERSIONS).fetch_all(pool).await?
            }
        };
        Ok(versions)
    }

    /// How the database's schema compares with this binary's migrations.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sqlx`] if reading the applied migrations fails.
    pub async fn schema_status(&self) -> Result<SchemaStatus, DbError> {
        let applied = self.applied_migrations().await?;
        Ok(SchemaStatus::compare(
            &self.backend().migration_versions(),
            &applied,
        ))
    }

    /// Fails unless the schema is usable: what the roles check at startup.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::SchemaNotUsable`] for an uninitialized or outdated schema, or
    /// [`DbError::Sqlx`] if reading it fails.
    pub async fn require_usable_schema(&self) -> Result<SchemaStatus, DbError> {
        let status = self.schema_status().await?;
        if status.is_usable() {
            Ok(status)
        } else {
            Err(DbError::SchemaNotUsable { status })
        }
    }
}
