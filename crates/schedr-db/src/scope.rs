//! Several writes in one transaction: [`Database::atomically`] runs a piece of work with every
//! query of this database (writes and reads, so it sees its own writes) going through one
//! transaction, committed once at its end or rolled back on an error. The master uses it for a
//! worker message's writes and for a round's assignments: one lease check and one commit
//! instead of one per write (see PROJECT.md, "Found by the stress suite").
//!
//! The transaction is found through a task-local: only the work's own future uses it, never
//! another task on the same database (the lease's renewals, a gauge refresh, an input fetch).

use std::future::Future;
use std::sync::Arc;

use sqlx::pool::PoolConnection;
use sqlx::{AssertSqlSafe, Postgres, Sqlite};

use crate::database::{Database, DbError, Pool};

tokio::task_local! {
    static CURRENT: Scope;
}

/// The transaction of the work running in this task, and the database it belongs to.
#[derive(Debug, Clone)]
pub(crate) struct Scope {
    database: Arc<()>,
    pub(crate) connection: Arc<tokio::sync::Mutex<Scoped>>,
}

/// The connection holding the transaction. Dropped while still open (the work was cancelled),
/// it is closed rather than given back to the pool in the middle of a transaction.
#[derive(Debug)]
pub(crate) struct Scoped {
    pub(crate) connection: Connection,
    open: bool,
}

/// One of the two backends' pooled connections.
#[derive(Debug)]
pub(crate) enum Connection {
    Sqlite(PoolConnection<Sqlite>),
    Postgres(PoolConnection<Postgres>),
}

impl Drop for Scoped {
    fn drop(&mut self) {
        if self.open {
            match &mut self.connection {
                Connection::Sqlite(connection) => connection.close_on_drop(),
                Connection::Postgres(connection) => connection.close_on_drop(),
            }
        }
    }
}

/// The transaction of the work running in this task, if it is on `db`.
pub(crate) fn current(db: &Database) -> Option<Scope> {
    CURRENT
        .try_with(Scope::clone)
        .ok()
        .filter(|scope| Arc::ptr_eq(&scope.database, db.identity()))
}

impl Scoped {
    /// Runs `statement` on the connection.
    async fn run(&mut self, statement: AssertSqlSafe<String>) -> Result<(), sqlx::Error> {
        match &mut self.connection {
            Connection::Sqlite(connection) => sqlx::raw_sql(statement)
                .execute(&mut **connection)
                .await
                .map(drop),
            Connection::Postgres(connection) => sqlx::raw_sql(statement)
                .execute(&mut **connection)
                .await
                .map(drop),
        }
    }

    /// Ends the transaction with `statement` (`COMMIT` or `ROLLBACK`); the connection goes back
    /// to the pool only if that worked (else it is closed, see [`Scoped`]'s `Drop`).
    async fn end(&mut self, statement: &str) -> Result<(), sqlx::Error> {
        self.run(AssertSqlSafe(statement.to_owned())).await?;
        self.open = false;
        Ok(())
    }
}

impl Database {
    /// Runs `work` in one transaction of this database: every query it makes on it (writes and
    /// reads) goes through the transaction, which commits when `work` returns `Ok` and rolls
    /// back when it returns `Err` (then nothing of it is stored). A fenced database checks its
    /// lease once, at the start, and on Postgres holds the lease's row until the end (see
    /// [`Database::fenced`]). Already inside one on this database, `work` joins it.
    ///
    /// # Errors
    ///
    /// `work`'s error, or [`DbError`] if the transaction can't start or commit
    /// ([`DbError::NotLeading`] for a master whose lease was taken over).
    pub async fn atomically<T, E>(&self, work: impl Future<Output = Result<T, E>>) -> Result<T, E>
    where
        E: From<DbError>,
    {
        if current(self).is_some() {
            return work.await;
        }
        let scoped = self
            .limited(self.query_timeout(), self.begin_scope())
            .await?;
        let scope = Scope {
            database: self.identity().clone(),
            connection: Arc::new(tokio::sync::Mutex::new(scoped)),
        };
        let result = CURRENT.scope(scope.clone(), work).await;
        let mut scoped = scope.connection.lock().await;
        if result.is_ok() {
            self.limited(self.query_timeout(), async {
                Ok(scoped.end("COMMIT").await?)
            })
            .await?;
        } else {
            // Nothing of it is stored either way: a rollback that fails closes the connection.
            let _ = self
                .limited(self.query_timeout(), async {
                    Ok(scoped.end("ROLLBACK").await?)
                })
                .await;
        }
        result
    }

    /// A connection with a transaction begun on it: on Postgres in one message with the
    /// statement limit and the fence ([`Database::begin_postgres`]); on SQLite taking the write
    /// lock at once (`BEGIN IMMEDIATE`), then the fence.
    async fn begin_scope(&self) -> Result<Scoped, DbError> {
        match self.pool() {
            Pool::Sqlite(pool) => {
                let mut scoped = Scoped {
                    connection: Connection::Sqlite(pool.acquire().await?),
                    open: true,
                };
                scoped
                    .run(AssertSqlSafe("BEGIN IMMEDIATE".to_owned()))
                    .await?;
                let Connection::Sqlite(connection) = &mut scoped.connection else {
                    unreachable!("an SQLite pool gives SQLite connections")
                };
                self.check_fence_sqlite(connection).await?;
                Ok(scoped)
            }
            Pool::Postgres(pool) => {
                let mut scoped = Scoped {
                    connection: Connection::Postgres(pool.acquire().await?),
                    open: true,
                };
                let begin = self.begin_postgres();
                scoped
                    .run(AssertSqlSafe(begin.0))
                    .await
                    .map_err(DbError::from_begin)?;
                Ok(scoped)
            }
        }
    }
}

#[cfg(test)]
mod tests;
