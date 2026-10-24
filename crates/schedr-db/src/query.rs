//! What the query modules share: one query text for both backends, and how times are stored.
//!
//! Queries are written once. `{uuid}`, `{ts}` and `{json}` after a parameter cast it for
//! Postgres (whose columns are typed) and vanish for SQLite (text columns); `{text}` after a
//! column reads it as text; `{ts<}expr{>ts}` reads a time as stored text (see [`db_time`]).
//! `INTEGER` columns are read as `i32`: Postgres keeps them in four bytes and refuses a wider
//! Rust type, SQLite reads either.

use jiff::Timestamp;
use sqlx::AssertSqlSafe;

use crate::backend::Backend;
use crate::database::DbError;

/// Runs `$body` with `$pool` bound to a connection (`&mut` the backend's connection type) and
/// `$backend` to its [`Backend`], once per backend (the queries are the same text, typed per
/// driver), within the database's query limit: past it, the enclosing function returns
/// [`DbError::QueryTimeout`] (the query is cut off here, whatever pooler is between; see
/// PROJECT.md, production path). Inside [`Database::atomically`] on this database the
/// connection is that transaction's, so the work reads its own writes; otherwise one from the
/// pool. For reads: writes go through [`in_tx`]. A body using `$pool` twice reborrows it
/// (`&mut *pool`).
///
/// [`Database::atomically`]: crate::database::Database::atomically
macro_rules! on_pool {
    ($db:expr, |$pool:ident, $backend:ident| $body:expr) => {{
        let db: &$crate::database::Database = $db;
        let run = async {
            Ok::<_, $crate::database::DbError>(if let Some(scope) = $crate::scope::current(db) {
                let mut scoped = scope.connection.lock().await;
                match &mut scoped.connection {
                    $crate::scope::Connection::Sqlite(connection) => {
                        let $pool: &mut sqlx::SqliteConnection = &mut **connection;
                        let $backend = $crate::backend::Backend::Sqlite;
                        $body
                    }
                    $crate::scope::Connection::Postgres(connection) => {
                        let $pool: &mut sqlx::PgConnection = &mut **connection;
                        let $backend = $crate::backend::Backend::Postgres;
                        $body
                    }
                }
            } else {
                match db.pool() {
                    $crate::database::Pool::Sqlite(pool) => {
                        let mut connection = pool.acquire().await?;
                        let $pool: &mut sqlx::SqliteConnection = &mut *connection;
                        let $backend = $crate::backend::Backend::Sqlite;
                        $body
                    }
                    $crate::database::Pool::Postgres(pool) => {
                        let mut connection = pool.acquire().await?;
                        let $pool: &mut sqlx::PgConnection = &mut *connection;
                        let $backend = $crate::backend::Backend::Postgres;
                        $body
                    }
                }
            })
        };
        db.limited(db.query_timeout(), run).await?
    }};
}
pub(crate) use on_pool;

/// How many query limits a write's transaction may take in all, by default: its statements
/// are each limited by Postgres itself (and SQLite's busy timeout); this bounds the whole,
/// waits for a connection and the network included.
pub(crate) const WRITE_LIMITS: u32 = 2;

/// As [`WRITE_LIMITS`], for a write of many statements (a parse's files, a code version's
/// DAG changes).
pub(crate) const BATCH_LIMITS: u32 = 6;

/// Runs `$body` in a transaction, `$tx` bound to it and `$backend` to its [`Backend`]: the
/// database's writes. A master's are fenced first (see [`Database::fenced`]); on Postgres
/// each statement is limited by the query limit in Postgres too (`statement_timeout` for this
/// transaction only, so it holds through any pooler, and a statement cut off stops there
/// instead of running on); the whole by `$limits` query limits ([`WRITE_LIMITS`] if not
/// given); on SQLite it takes the write lock at its start (`BEGIN IMMEDIATE`). The transaction
/// commits when `$body` ends without an error.
///
/// On Postgres the transaction's start, its limit and the fence are one message to the
/// server ([`Database::begin_postgres`]), so a write costs one round trip more than its own
/// statements (the `COMMIT`). A connection whose start failed may be left in an aborted
/// transaction, so it is closed rather than given back to the pool.
///
/// [`Database::fenced`]: crate::database::Database::fenced
macro_rules! in_tx {
    ($db:expr, |$tx:ident, $backend:ident| $body:expr) => {
        $crate::query::in_tx!($db, $crate::query::WRITE_LIMITS, |$tx, $backend| $body)
    };
    ($db:expr, $limits:expr, |$tx:ident, $backend:ident| $body:expr) => {{
        let db: &$crate::database::Database = $db;
        let run = async {
            // Inside `Database::atomically` on this database: its transaction (begun, limited
            // and fenced already; committed at its end).
            if let Some(scope) = $crate::scope::current(db) {
                let mut scoped = scope.connection.lock().await;
                return Ok::<_, $crate::database::DbError>(match &mut scoped.connection {
                    $crate::scope::Connection::Sqlite(connection) => {
                        let $backend = $crate::backend::Backend::Sqlite;
                        let $tx: &mut sqlx::SqliteConnection = &mut **connection;
                        $body
                    }
                    $crate::scope::Connection::Postgres(connection) => {
                        let $backend = $crate::backend::Backend::Postgres;
                        let $tx: &mut sqlx::PgConnection = &mut **connection;
                        $body
                    }
                });
            }
            Ok::<_, $crate::database::DbError>(match db.pool() {
                $crate::database::Pool::Sqlite(pool) => {
                    let $backend = $crate::backend::Backend::Sqlite;
                    // The write lock at once, waiting for it under the busy timeout: a
                    // transaction begun deferred that reads first (the fence) fails at once if
                    // another connection writes before it does (B14).
                    let mut $tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                    db.check_fence_sqlite(&mut *$tx).await?;
                    let done = $body;
                    $tx.commit().await?;
                    done
                }
                $crate::database::Pool::Postgres(pool) => {
                    let $backend = $crate::backend::Backend::Postgres;
                    let mut conn = pool.acquire().await?;
                    let (begun, failed) =
                        match sqlx::Connection::begin_with(&mut *conn, db.begin_postgres()).await {
                            Ok(tx) => (Some(tx), None),
                            Err(error) => (None, Some(error)),
                        };
                    if let Some(error) = failed {
                        // Ends the borrow of `conn` before it is marked.
                        drop(begun);
                        conn.close_on_drop();
                        return Err($crate::database::DbError::from_begin(error));
                    }
                    let Some(mut $tx) = begun else {
                        unreachable!("a failed start returned above")
                    };
                    let done = $body;
                    $tx.commit().await?;
                    done
                }
            })
        };
        db.limited(db.query_timeout() * $limits, run).await?
    }};
}
pub(crate) use in_tx;

#[cfg(test)]
mod tests;

/// The format times are stored in: UTC, always six fraction digits, so SQLite compares the
/// text in time order.
const TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";

/// A query in this crate's dialect, for one backend.
pub(crate) fn sql(backend: Backend, query: &'static str) -> AssertSqlSafe<String> {
    let casts: [(&str, &str); 6] = match backend {
        Backend::Sqlite => [
            ("{uuid}", ""),
            ("{ts}", ""),
            ("{json}", ""),
            ("{text}", ""),
            ("{ts<}", ""),
            ("{>ts}", ""),
        ],
        Backend::Postgres => [
            ("{uuid}", "::uuid"),
            ("{ts}", "::timestamptz"),
            ("{json}", "::jsonb"),
            ("{text}", "::text"),
            ("{ts<}", "to_char(("),
            (
                "{>ts}",
                r#") AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')"#,
            ),
        ],
    };
    let text = casts.iter().fold(query.to_owned(), |text, (marker, cast)| {
        text.replace(marker, cast)
    });
    // Safe: built only from the static queries of this crate and fixed casts.
    AssertSqlSafe(text)
}

/// A time as stored: `2026-10-21T08:00:00.000000Z` (microseconds, as Postgres keeps them).
#[must_use]
pub fn db_time(t: Timestamp) -> String {
    t.strftime(TIME_FORMAT).to_string()
}

/// A stored time read back.
pub(crate) fn parse_time(what: &'static str, text: &str) -> Result<Timestamp, DbError> {
    text.parse().map_err(|e: jiff::Error| DbError::InvalidData {
        what,
        reason: e.to_string(),
    })
}

/// Reads an optional stored time (see [`parse_time`]).
pub(crate) fn optional_time(
    what: &'static str,
    text: Option<String>,
) -> Result<Option<Timestamp>, DbError> {
    text.map(|t| parse_time(what, &t)).transpose()
}

/// Stored JSON of model types.
pub(crate) fn to_json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("model types always serialize")
}

/// Stored JSON read back.
pub(crate) fn from_json<T: serde::de::DeserializeOwned>(
    what: &'static str,
    text: &str,
) -> Result<T, DbError> {
    serde_json::from_str(text).map_err(|e| DbError::InvalidData {
        what,
        reason: e.to_string(),
    })
}

/// A stored enum name (a run kind, a state) read back.
pub(crate) fn from_name<T: serde::de::DeserializeOwned>(
    what: &'static str,
    name: String,
) -> Result<T, DbError> {
    serde_json::from_value(serde_json::Value::String(name)).map_err(|e| DbError::InvalidData {
        what,
        reason: e.to_string(),
    })
}
