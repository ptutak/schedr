//! The master's lease: one master per database (see PROJECT.md, Components and roles:
//! production path). A one-row table, taken and renewed by one statement each, timed by the
//! **database's** clock, so it needs no session state: it works through any connection
//! pooler (`PgBouncer` in transaction pooling too), and on SQLite.
//!
//! The row says who holds the lease, until when, and how many times it changed hands (the
//! takeover counter, `epoch`): a master that finds the counter moved on knows another one led
//! meanwhile.

use std::time::Duration;

use crate::database::{Database, DbError, Pool};

#[cfg(test)]
mod tests;

/// The table, created before the migrations so a master can hold the lease while it migrates.
const CREATE_SQLITE: &str = "CREATE TABLE IF NOT EXISTS master_lease (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    holder     TEXT NOT NULL,
    epoch      INTEGER NOT NULL,
    expires_at TEXT NOT NULL
)";

const CREATE_POSTGRES: &str = "CREATE TABLE IF NOT EXISTS master_lease (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    holder     TEXT NOT NULL,
    epoch      INTEGER NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
)";

/// SQLite's clock in the stored time format (six fraction digits, so text compares in time
/// order); `$2` is the period as an SQLite modifier (`+15.000 seconds`).
const TAKE_SQLITE: &str = "INSERT INTO master_lease (id, holder, epoch, expires_at)
    VALUES (1, $1, 1, strftime('%Y-%m-%dT%H:%M:%f', 'now', $2) || '000Z')
    ON CONFLICT (id) DO UPDATE SET
        epoch = CASE WHEN master_lease.holder = excluded.holder
            THEN master_lease.epoch ELSE master_lease.epoch + 1 END,
        holder = excluded.holder,
        expires_at = excluded.expires_at
    WHERE master_lease.holder = excluded.holder
        OR master_lease.expires_at < strftime('%Y-%m-%dT%H:%M:%f', 'now') || '000Z'
    RETURNING epoch";

/// `$2` is the period in milliseconds.
const TAKE_POSTGRES: &str = "INSERT INTO master_lease (id, holder, epoch, expires_at)
    VALUES (1, $1, 1, clock_timestamp() + $2 * interval '1 millisecond')
    ON CONFLICT (id) DO UPDATE SET
        epoch = CASE WHEN master_lease.holder = excluded.holder
            THEN master_lease.epoch ELSE master_lease.epoch + 1 END,
        holder = excluded.holder,
        expires_at = excluded.expires_at
    WHERE master_lease.holder = excluded.holder
        OR master_lease.expires_at < clock_timestamp()
    RETURNING epoch";

/// Before a takeover on Postgres, in its transaction: the row locked against every master's
/// write in progress (they hold it `FOR KEY SHARE`), so the takeover waits for them to commit.
const LOCK_POSTGRES: &str = "SELECT 1 FROM master_lease WHERE id = 1 FOR UPDATE";

/// Renewing extends only the holder's own lease, at the same takeover counter: never taking
/// it back after a takeover. `$3` as in the take.
const RENEW_SQLITE: &str = "UPDATE master_lease
    SET expires_at = strftime('%Y-%m-%dT%H:%M:%f', 'now', $3) || '000Z'
    WHERE holder = $1 AND epoch = $2
    RETURNING epoch";

const RENEW_POSTGRES: &str = "UPDATE master_lease
    SET expires_at = clock_timestamp() + $3 * interval '1 millisecond'
    WHERE holder = $1 AND epoch = $2
    RETURNING epoch";

const RELEASE_SQLITE: &str = "UPDATE master_lease
    SET expires_at = strftime('%Y-%m-%dT%H:%M:%f', 'now', '-1 seconds') || '000Z'
    WHERE holder = $1";

const RELEASE_POSTGRES: &str = "UPDATE master_lease
    SET expires_at = clock_timestamp() - interval '1 second' WHERE holder = $1";

/// A master's write checks its lease first, in the write's transaction (see
/// [`Database::fenced`]).
const FENCE_SQLITE: &str = "SELECT 1 FROM master_lease WHERE holder = $1 AND epoch = $2";

/// As [`FENCE_SQLITE`] on Postgres, sent with the transaction's start (no parameters there:
/// the lease reaches it through `schedr.holder` and `schedr.epoch`, set for the transaction
/// only). The share lock lets renewals through (they change no key) but makes a takeover
/// wait; a lease taken over raises [`NOT_LEADING`].
const FENCE_POSTGRES: &str = "DO $fence$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM master_lease
        WHERE holder = current_setting('schedr.holder')
        AND epoch = current_setting('schedr.epoch')::integer
        FOR KEY SHARE)
    THEN RAISE EXCEPTION 'the lease was taken over' USING ERRCODE = 'SR001';
    END IF;
END $fence$;";

/// The SQLSTATE [`FENCE_POSTGRES`] raises.
pub(crate) const NOT_LEADING: &str = "SR001";

impl Database {
    /// Creates the lease's table if it isn't there (before the migrations, which create it
    /// too; a race between two masters creating it is an error the caller tries again).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails, [`DbError::QueryTimeout`] past the limit.
    pub async fn create_lease_table(&self) -> Result<(), DbError> {
        self.limited(self.query_timeout(), async {
            match self.pool() {
                Pool::Sqlite(pool) => {
                    sqlx::query(CREATE_SQLITE).execute(pool).await?;
                }
                Pool::Postgres(pool) => {
                    sqlx::query(CREATE_POSTGRES).execute(pool).await?;
                }
            }
            Ok(())
        })
        .await
    }

    /// Takes the lease for `holder` (a master's incarnation) until `period` from now by the
    /// database's clock: the takeover counter if `holder` holds it now, `None` if another
    /// master does and its lease hasn't expired. On Postgres a takeover waits for the writes
    /// of the master before it that are in progress.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails, [`DbError::QueryTimeout`] past the limit.
    pub async fn take_lease(&self, holder: &str, period: Duration) -> Result<Option<i64>, DbError> {
        let epoch: Option<i32> = self
            .limited(self.query_timeout(), async {
                Ok(match self.pool() {
                    Pool::Sqlite(pool) => {
                        sqlx::query_scalar(TAKE_SQLITE)
                            .bind(holder)
                            .bind(sqlite_period(period))
                            .fetch_optional(pool)
                            .await?
                    }
                    Pool::Postgres(pool) => {
                        // Not `pool.begin()`: a start cut short mustn't go back to the pool (B84).
                        let mut starting = crate::query::Starting::new(pool.acquire().await?);
                        let mut tx = starting.begin("BEGIN").await?;
                        sqlx::query(LOCK_POSTGRES).execute(&mut *tx).await?;
                        let epoch = sqlx::query_scalar(TAKE_POSTGRES)
                            .bind(holder)
                            .bind(millis(period))
                            .fetch_optional(&mut *tx)
                            .await?;
                        tx.commit().await?;
                        epoch
                    }
                })
            })
            .await?;
        Ok(epoch.map(i64::from))
    }

    /// Renews `holder`'s lease taken at takeover counter `epoch` until `period` from now by
    /// the database's clock: `false` if it is no longer `holder`'s at `epoch` (another master
    /// took it over; it is left as it is).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails, [`DbError::QueryTimeout`] past the limit.
    pub async fn renew_lease(
        &self,
        holder: &str,
        epoch: i64,
        period: Duration,
    ) -> Result<bool, DbError> {
        let epoch = i32::try_from(epoch).unwrap_or(i32::MAX);
        let renewed: Option<i32> = self
            .limited(self.query_timeout(), async {
                Ok(match self.pool() {
                    Pool::Sqlite(pool) => {
                        sqlx::query_scalar(RENEW_SQLITE)
                            .bind(holder)
                            .bind(epoch)
                            .bind(sqlite_period(period))
                            .fetch_optional(pool)
                            .await?
                    }
                    Pool::Postgres(pool) => {
                        sqlx::query_scalar(RENEW_POSTGRES)
                            .bind(holder)
                            .bind(epoch)
                            .bind(millis(period))
                            .fetch_optional(pool)
                            .await?
                    }
                })
            })
            .await?;
        Ok(renewed.is_some())
    }

    /// Gives the lease up if `holder` has it (a clean shutdown), so a standby takes over at
    /// once instead of when it expires.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails, [`DbError::QueryTimeout`] past the limit.
    pub async fn release_lease(&self, holder: &str) -> Result<(), DbError> {
        self.limited(self.query_timeout(), async {
            match self.pool() {
                Pool::Sqlite(pool) => {
                    sqlx::query(RELEASE_SQLITE)
                        .bind(holder)
                        .execute(pool)
                        .await?;
                }
                Pool::Postgres(pool) => {
                    sqlx::query(RELEASE_POSTGRES)
                        .bind(holder)
                        .execute(pool)
                        .await?;
                }
            }
            Ok(())
        })
        .await
    }

    /// In a write's transaction on SQLite: [`DbError::NotLeading`] if this database is a
    /// master's ([`Self::fenced`]) and its lease was taken over.
    pub(crate) async fn check_fence_sqlite(
        &self,
        tx: &mut sqlx::SqliteConnection,
    ) -> Result<(), DbError> {
        let Some(fence) = self.fence() else {
            return Ok(());
        };
        let held: Option<i32> = sqlx::query_scalar(FENCE_SQLITE)
            .bind(&fence.holder)
            .bind(fence.epoch)
            .fetch_optional(tx)
            .await?;
        held.map(|_| ()).ok_or(DbError::NotLeading)
    }

    /// The start of a write's transaction on Postgres, in one message: `BEGIN`, each
    /// statement limited to the query limit, the transaction ended by Postgres if it stays idle
    /// that long (a stalled client's locks released, B10), and the fence if this database is a
    /// master's
    /// (holding the lease's row until the write commits). Values are written into the text (a
    /// message of several statements takes no parameters), each as a quoted literal.
    pub(crate) fn begin_postgres(&self) -> sqlx::AssertSqlSafe<String> {
        let limit = format!("{}ms", self.query_timeout().as_millis().max(1));
        let fence = self.fence().map_or_else(String::new, |fence| {
            format!(
                " SELECT set_config('schedr.holder', {}, true), \
                 set_config('schedr.epoch', {}, true); {FENCE_POSTGRES}",
                literal(&fence.holder),
                literal(&fence.epoch.to_string()),
            )
        });
        // Safe: fixed text, and values only as literals quoted by `literal`.
        // The idle limit too: a transaction whose client stalls between statements (a network
        // partition, a frozen master) is ended by Postgres, and its locks with it, the lease's
        // row among them; it would otherwise block a takeover for as long as the stall (B10).
        sqlx::AssertSqlSafe(format!(
            "BEGIN; SELECT set_config('statement_timeout', {limit}, true), \
             set_config('idle_in_transaction_session_timeout', {limit}, true);{fence}",
            limit = literal(&limit)
        ))
    }
}

/// `text` as a Postgres string literal: `E'…'` with quotes and backslashes escaped, so it
/// reads the same whatever `standard_conforming_strings` is.
fn literal(text: &str) -> String {
    format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// A period as an SQLite time modifier (`+15.000 seconds`).
fn sqlite_period(period: Duration) -> String {
    format!("+{:.3} seconds", period.as_secs_f64())
}

/// A period in milliseconds, for Postgres.
fn millis(period: Duration) -> i64 {
    i64::try_from(period.as_millis()).unwrap_or(i64::MAX)
}
