//! A connection pool to the schedr database, and schema migration and checks.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use sqlx::migrate::MigrateError;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use crate::backend::{Backend, UnsupportedUrl};
use crate::status::SchemaStatus;

#[cfg(test)]
mod tests;

/// The longest a query may take by default (`[database] query_timeout`).
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a database operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// The URL doesn't name a supported database.
    #[error(transparent)]
    UnsupportedUrl(#[from] UnsupportedUrl),
    /// A query or connection failed.
    #[error("database error: {0}{hint}", hint = pooler_hint(.0))]
    Sqlx(#[from] sqlx::Error),
    /// Applying migrations failed.
    #[error("migration failed: {0}")]
    Migrate(#[from] MigrateError),
    /// Stored data doesn't read back as the model (written by hand, or by an incompatible
    /// version).
    #[error("invalid stored data in {what}: {reason}")]
    InvalidData {
        /// The table and column.
        what: &'static str,
        /// Why it doesn't read.
        reason: String,
    },
    /// A query took longer than the limit ([`Database::connect_with_timeout`]): cut off here.
    #[error("a database query took longer than {} s (`[database] query_timeout`)", .0.as_secs_f64())]
    QueryTimeout(Duration),
    /// The `COMMIT` of a [`Database::atomically`] transaction was cut off by the time limit or
    /// lost with its connection: whether the transaction is stored isn't known (read it back).
    #[error("the transaction's commit was cut off; whether it is stored is unknown")]
    CommitUnknown,
    /// A statement of a [`Database::atomically`] transaction failed or was cut off, though the
    /// work went on: the transaction was rolled back, nothing of it stored.
    #[error("a statement of this transaction failed; nothing of it is stored")]
    TransactionFailed,
    /// A master's write found the lease no longer its own (another master took it over): it
    /// was refused, nothing of it stored ([`Database::fenced`]).
    #[error("this master no longer holds the database's lease (another took it over)")]
    NotLeading,
}

/// A connection pool to the schedr database, and the lease its writes are fenced by, if any.
#[derive(Debug, Clone)]
pub struct Database {
    pool: Pool,
    fence: Option<Arc<Fence>>,
    /// DAG definitions by version, parsed once (shared by clones; see [`DagCache`]).
    dags: Arc<DagCache>,
    /// The same for every clone: which database a transaction of
    /// [`Database::atomically`] belongs to.
    identity: Arc<()>,
    /// How the master learns of new commands (see [`Database::with_notifications`]).
    notifications: Arc<Notifications>,
    /// Writes made to fail, for tests (shared by clones; decision 98).
    #[cfg(any(test, feature = "testing"))]
    faults: Arc<crate::faults::Faults>,
}

/// `[database] notifications` and `notifications_url`.
#[derive(Debug, Clone)]
pub(crate) struct Notifications {
    /// On: a new command is announced, and the master listens.
    pub(crate) on: bool,
    /// The connection the master listens through, if not this database's own.
    pub(crate) url: Option<String>,
}

impl Default for Notifications {
    fn default() -> Self {
        Self {
            on: true,
            url: None,
        }
    }
}

/// DAG definitions by version ID, each parsed once: a version never changes once stored, so
/// what is cached never goes stale. Rounds read many rows of the same few versions (a wide
/// DAG's ready tasks, every active DAG); each brought and parsed its whole definition (B13).
/// Cleared when it holds [`DagCache::MAX`] versions (the current ones come back at once).
#[derive(Debug, Default)]
pub(crate) struct DagCache(
    std::sync::Mutex<std::collections::HashMap<String, Arc<schedr_model::Dag>>>,
);

impl DagCache {
    /// The most versions kept before clearing.
    pub(crate) const MAX: usize = 4096;

    pub(crate) fn get(&self, version_id: &str) -> Option<Arc<schedr_model::Dag>> {
        self.map().get(version_id).cloned()
    }

    pub(crate) fn insert(&self, version_id: &str, dag: Arc<schedr_model::Dag>) {
        let mut map = self.map();
        if map.len() >= Self::MAX {
            map.clear();
        }
        map.insert(version_id.to_owned(), dag);
    }

    fn map(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, Arc<schedr_model::Dag>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The connection pool of a [`Database`].
#[derive(Debug, Clone)]
pub enum Pool {
    /// SQLite.
    Sqlite(SqlitePool),
    /// Postgres.
    Postgres(PgPool),
}

/// The lease a master's writes need (see [`Database::fenced`]).
#[derive(Debug)]
pub(crate) struct Fence {
    /// The master's name in the lease.
    pub(crate) holder: String,
    /// The takeover counter when it took the lease.
    pub(crate) epoch: i32,
}

/// Whether a database error code is the database's own trouble rather than a statement's data:
/// on Postgres (SQLSTATE) the classes of transaction rollback (40: deadlock, serialization),
/// insufficient resources (53), object not ready (55: lock not available), operator
/// intervention (57: a statement cancelled at its limit, a shutdown), connection (08) and
/// system errors (58); on SQLite (a number) busy, locked, I/O, full and can't open.
fn databases_trouble(postgres: bool, code: &str) -> bool {
    if postgres {
        // SQLSTATE: five characters, often all digits (57014), so told by the backend.
        return ["40", "53", "55", "57", "08", "58"]
            .iter()
            .any(|class| code.starts_with(class));
    }
    code.parse::<i64>()
        .is_ok_and(|sqlite| matches!(sqlite & 0xff, 5 | 6 | 10 | 13 | 14))
}

/// The likely cause of an error showing a notification where a query's answer was expected:
/// a connection pooler in transaction mode lends a server connection that someone `LISTEN`s
/// on to other clients (B16); empty for any other error.
fn pooler_hint(error: &sqlx::Error) -> &'static str {
    if error.to_string().contains("NotificationResponse") {
        " (probably a connection pooler in transaction mode, PgBouncer or similar, in front of \
         Postgres while a master listens for notifications: set `[database] notifications = \
         false`, or `notifications_url` to a connection straight to Postgres)"
    } else {
        ""
    }
}

impl DbError {
    /// What a failed `COMMIT` means: refused by the database (a constraint checked at commit,
    /// a serialization failure: rolled back, the error as it is), or cut off or its connection
    /// lost ([`Self::CommitUnknown`]: it may be stored).
    #[must_use]
    pub fn from_commit(error: Self) -> Self {
        match error {
            Self::Sqlx(sqlx::Error::Database(_)) => error,
            _ => Self::CommitUnknown,
        }
    }

    /// Whether this is about the data a statement touched (stored data that won't read, a
    /// statement the database refuses for its data: a constraint, a trigger), not the
    /// database's own trouble (unreachable, a time limit, its own statement limit, a deadlock
    /// or serialization failure, a lock it couldn't get, an operator's shutdown, a busy SQLite,
    /// a full disk) or the lease: retrying the same statement fails the same way, while other
    /// statements may go through.
    #[must_use]
    pub fn is_data_fault(&self) -> bool {
        match self {
            Self::InvalidData { .. }
            | Self::Sqlx(sqlx::Error::Decode(_) | sqlx::Error::ColumnDecode { .. }) => true,
            Self::Sqlx(sqlx::Error::Database(error)) => {
                let postgres = error
                    .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                    .is_some();
                !error
                    .code()
                    .is_some_and(|code| databases_trouble(postgres, &code))
            }
            _ => false,
        }
    }

    /// A failed start of a write's transaction on Postgres: [`Self::NotLeading`] if it was the
    /// fence that refused it ([`Database::begin_postgres`]).
    pub(crate) fn from_begin(error: sqlx::Error) -> Self {
        match &error {
            sqlx::Error::Database(e) if e.code().as_deref() == Some(crate::lease::NOT_LEADING) => {
                Self::NotLeading
            }
            _ => Self::Sqlx(error),
        }
    }
}

impl From<Pool> for Database {
    fn from(pool: Pool) -> Self {
        Self {
            pool,
            fence: None,
            dags: Arc::default(),
            identity: Arc::default(),
            notifications: Arc::default(),
            #[cfg(any(test, feature = "testing"))]
            faults: Arc::default(),
        }
    }
}

impl Database {
    /// The writes made to fail (tests only).
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn faults(&self) -> &crate::faults::Faults {
        &self.faults
    }

    /// Connects to the database a URL names. SQLite files are created if missing, and
    /// foreign keys are enforced. An in-memory SQLite database (`sqlite::memory:`) uses a
    /// single connection, since each connection would otherwise see its own empty database.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UnsupportedUrl`] for an unknown scheme, or [`DbError::Sqlx`] if the
    /// URL is malformed or the connection fails.
    pub async fn connect(url: &str) -> Result<Self, DbError> {
        Self::open(url, None).await
    }

    /// Connects to the database at `url`, no query (or wait for a connection) taking longer
    /// than `query_timeout`, enforced here around each query ([`DbError::QueryTimeout`]; a
    /// server-side `statement_timeout`, set by the DBA, stops the work in Postgres too), and
    /// SQLite's busy timeout. The roles use it; migrations don't (a long migration must
    /// finish). It works through any connection pooler: nothing is set on the session.
    ///
    /// # Errors
    ///
    /// As [`Self::connect`].
    pub async fn connect_with_timeout(url: &str, query_timeout: Duration) -> Result<Self, DbError> {
        Self::open(url, Some(query_timeout)).await
    }

    async fn open(url: &str, query_timeout: Option<Duration>) -> Result<Self, DbError> {
        // Without a limit, sqlx's default wait for a connection.
        let acquire = query_timeout.unwrap_or(Duration::from_secs(30));
        match Backend::from_url(url)? {
            Backend::Sqlite => {
                let mut options = SqliteConnectOptions::from_str(url)?
                    .create_if_missing(true)
                    .foreign_keys(true);
                if let Some(timeout) = query_timeout {
                    options = options.busy_timeout(timeout);
                }
                let max_connections = if url.contains(":memory:") { 1 } else { 8 };
                let pool = SqlitePoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(acquire)
                    .connect_with(options)
                    .await?;
                Ok(Pool::Sqlite(pool).into())
            }
            Backend::Postgres => {
                // No session state (see `crate::lease`): no `SET` on the connection, and no
                // named prepared statements, which a pooler in transaction mode may not keep.
                let options = PgConnectOptions::from_str(url)?.statement_cache_capacity(0);
                // No ping before each use (a round trip per query): a connection that died
                // in the pool fails its query instead, and the caller's next try gets another.
                let pool = PgPoolOptions::new()
                    .test_before_acquire(false)
                    .acquire_timeout(acquire)
                    .connect_with(options)
                    .await?;
                Ok(Pool::Postgres(pool).into())
            }
        }
    }

    /// The connection pool.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// This database for the master holding the lease as `holder` with takeover counter
    /// `epoch`: each of its writes checks, in its own transaction, that the lease is still
    /// that, and is refused ([`DbError::NotLeading`]) once another master took it over. On
    /// Postgres the check holds the lease's row until the write commits, so a takeover waits
    /// for it; SQLite runs one write at a time anyway. A write sent before a takeover but
    /// reaching the database after it (held up by the network or a pooler's queue) is refused
    /// too: what the master's own clock can't rule out (see PROJECT.md, production path).
    #[must_use]
    pub fn fenced(&self, holder: &str, epoch: i64) -> Self {
        Self {
            pool: self.pool.clone(),
            dags: self.dags.clone(),
            identity: self.identity.clone(),
            notifications: self.notifications.clone(),
            #[cfg(any(test, feature = "testing"))]
            faults: self.faults.clone(),
            fence: Some(Arc::new(Fence {
                holder: holder.to_owned(),
                epoch: i32::try_from(epoch).unwrap_or(i32::MAX),
            })),
        }
    }

    /// The DAG of version `version_id`, from the cache or read and parsed once (B13).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored DAG doesn't read; [`DbError::InvalidData`]
    /// for an unknown version.
    pub async fn dag_version(&self, version_id: &str) -> Result<Arc<schedr_model::Dag>, DbError> {
        const JSON: &str = "SELECT dag_json{text} FROM dag_versions WHERE id = $1{uuid}";
        if let Some(dag) = self.dags.get(version_id) {
            return Ok(dag);
        }
        let json: Option<String> = crate::query::on_pool!(self, |pool, backend| {
            sqlx::query_scalar(crate::query::sql(backend, JSON))
                .bind(version_id)
                .fetch_optional(pool)
                .await?
        });
        let json = json.ok_or_else(|| DbError::InvalidData {
            what: "dag_versions.id",
            reason: format!("no DAG version `{version_id}`"),
        })?;
        let dag = Arc::new(crate::query::from_json::<schedr_model::Dag>(
            "dag_versions.dag_json",
            &json,
        )?);
        self.dags.insert(version_id, dag.clone());
        Ok(dag)
    }

    /// This database with notifications of new commands `on` or off (`[database]
    /// notifications`), the master listening through `url` if given (`notifications_url`) or
    /// through this database's own connections. Off: commands aren't announced, and the master
    /// polls (see [`Database::command_notifications`]).
    #[must_use]
    pub fn with_notifications(mut self, on: bool, url: Option<String>) -> Self {
        self.notifications = Arc::new(Notifications { on, url });
        self
    }

    /// Whether notifications are on, and the connection the master listens through.
    pub(crate) fn notifications(&self) -> &Notifications {
        &self.notifications
    }

    /// The same for every clone of this database (see [`Database::atomically`]).
    pub(crate) const fn identity(&self) -> &Arc<()> {
        &self.identity
    }

    /// The lease this database's writes are fenced by, if any.
    pub(crate) fn fence(&self) -> Option<&Fence> {
        self.fence.as_deref()
    }

    /// The longest a query may take: the pool's wait for a connection, which
    /// [`Self::connect_with_timeout`] sets to the query limit (sqlx's 30 s default otherwise).
    #[must_use]
    pub fn query_timeout(&self) -> Duration {
        match &self.pool {
            Pool::Sqlite(pool) => pool.options().get_acquire_timeout(),
            Pool::Postgres(pool) => pool.options().get_acquire_timeout(),
        }
    }

    /// `work` cut off after `limit` ([`DbError::QueryTimeout`]).
    pub(crate) async fn limited<T>(
        &self,
        limit: Duration,
        work: impl Future<Output = Result<T, DbError>>,
    ) -> Result<T, DbError> {
        tokio::time::timeout(limit, work)
            .await
            .unwrap_or(Err(DbError::QueryTimeout(limit)))
    }

    /// The backend of this database.
    #[must_use]
    pub const fn backend(&self) -> Backend {
        match &self.pool {
            Pool::Sqlite(_) => Backend::Sqlite,
            Pool::Postgres(_) => Backend::Postgres,
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
        match &self.pool {
            Pool::Sqlite(pool) => migrator.run(pool).await?,
            Pool::Postgres(pool) => migrator.run(pool).await?,
        }
        Ok(())
    }

    /// As [`Self::migrate`], without the migrator's own lock (a session advisory lock on
    /// Postgres, which connection poolers in transaction mode don't keep): for a master holding
    /// the lease, which already rules out a second one migrating (see [`crate::lease`]).
    ///
    /// # Errors
    ///
    /// As [`Self::migrate`].
    pub async fn migrate_as_lease_holder(&self) -> Result<(), DbError> {
        // `Migrator` isn't `Clone`: a copy from its (public, semver-exempt) fields, checked by
        // the tests at the pinned sqlx version.
        let locked = self.backend().migrator();
        let mut migrator = sqlx::migrate::Migrator {
            migrations: locked.migrations.clone(),
            ignore_missing: locked.ignore_missing,
            locking: locked.locking,
            no_tx: locked.no_tx,
            table_name: locked.table_name.clone(),
            create_schemas: locked.create_schemas.clone(),
        };
        migrator.set_locking(false);
        match &self.pool {
            Pool::Sqlite(pool) => migrator.run(pool).await?,
            Pool::Postgres(pool) => migrator.run(pool).await?,
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
        let versions = match &self.pool {
            Pool::Sqlite(pool) => {
                let exists: i64 = sqlx::query_scalar(EXISTS_SQLITE).fetch_one(pool).await?;
                if exists == 0 {
                    return Ok(Vec::new());
                }
                sqlx::query_scalar(VERSIONS).fetch_all(pool).await?
            }
            Pool::Postgres(pool) => {
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
}
