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

/// Runs `$body` with `$pool` bound to the pool and `$backend` to its [`Backend`], once per
/// backend (the queries are the same text, typed per driver).
macro_rules! on_pool {
    ($db:expr, |$pool:ident, $backend:ident| $body:expr) => {
        match $db {
            $crate::database::Database::Sqlite($pool) => {
                let $backend = $crate::backend::Backend::Sqlite;
                $body
            }
            $crate::database::Database::Postgres($pool) => {
                let $backend = $crate::backend::Backend::Postgres;
                $body
            }
        }
    };
}
pub(crate) use on_pool;

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
