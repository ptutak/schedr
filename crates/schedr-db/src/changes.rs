//! What changed, for the UI's live pages (see PROJECT.md, Web UI: 34A): which run, which DAG,
//! or the workers. Only what changed, never the new state: the UI reads that from the API.
//!
//! The writes signal it themselves (triggers in the migrations): on Postgres a notification on
//! [`CHANGES_CHANNEL`] when the writing transaction commits, read through a listening
//! connection; on SQLite a row in `changes`, read by polling about once a second.

use std::collections::BTreeSet;
use std::time::Duration;

use schedr_model::DagId;
use sqlx::postgres::PgListener;

use crate::commands::Listening;
use crate::database::{Database, DbError, Pool};
use crate::query::{on_pool, sql};

#[cfg(test)]
mod tests;

/// The Postgres notification channel changes are announced on.
pub const CHANGES_CHANNEL: &str = "schedr_changes";

/// How often SQLite's `changes` table is read.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// At most this many rows of `changes` a read.
const POLL_ROWS: i64 = 1_000;

/// Something that changed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Change {
    /// A run: itself, its tasks or their attempts.
    Run {
        /// The run.
        run_id: String,
        /// Its DAG.
        dag_id: DagId,
    },
    /// A DAG: defined, changed, paused, removed.
    Dag {
        /// The DAG.
        dag_id: DagId,
    },
    /// The workers: one registered, changed state, or was seen.
    Workers,
    /// Changes were missed (more came than were kept): everything may have changed.
    Missed,
}

impl Change {
    /// A change as the triggers write it: `run <run id> <dag id>`, `dag <dag id>`, `workers`;
    /// `None` for anything else.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let mut words = text.split(' ');
        let change = match (words.next()?, words.next(), words.next()) {
            ("run", Some(run_id), Some(dag_id)) => Self::Run {
                run_id: run_id.to_owned(),
                dag_id: DagId::new(dag_id).ok()?,
            },
            ("dag", Some(dag_id), None) => Self::Dag {
                dag_id: DagId::new(dag_id).ok()?,
            },
            ("workers", None, None) => Self::Workers,
            _ => return None,
        };
        words.next().is_none().then_some(change)
    }
}

/// The changes as they happen: [`Changes::next`] waits for the next ones.
#[derive(Debug)]
pub struct Changes {
    source: Source,
}

#[derive(Debug)]
enum Source {
    Listen(PgListener),
    Poll { db: Database, after: i64 },
}

impl Database {
    /// The changes from now on; `None` when they can't be followed (Postgres with
    /// notifications off, or behind a connection pooler in transaction mode without a
    /// `notifications_url`): the UI then refreshes on a timer.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database can't be reached or checked.
    pub async fn changes(&self) -> Result<Option<Changes>, DbError> {
        match self.checked_listener().await? {
            Listening::NotPostgres => {
                const LAST: &str = "SELECT COALESCE(MAX(seq), 0) FROM changes";
                let after: i64 = on_pool!(self, |pool, backend| {
                    sqlx::query_scalar(sql(backend, LAST))
                        .fetch_one(pool)
                        .await?
                });
                Ok(Some(Changes {
                    source: Source::Poll {
                        db: self.clone(),
                        after,
                    },
                }))
            }
            Listening::Off | Listening::Unsafe { .. } => Ok(None),
            Listening::Ready(mut listener) => {
                listener.listen(CHANGES_CHANNEL).await?;
                Ok(Some(Changes {
                    source: Source::Listen(listener),
                }))
            }
        }
    }
}

/// The change triggers on Postgres, by table (`[database] change_notifications` turns them on
/// and off: [`Database::set_change_notifications`]).
const POSTGRES_TRIGGERS: [(&str, &str); 11] = [
    ("runs", "runs_inserted"),
    ("runs", "runs_updated"),
    ("task_instances", "task_instances_inserted"),
    ("task_instances", "task_instances_updated"),
    ("attempts", "attempts_inserted"),
    ("attempts", "attempts_updated"),
    ("dags", "dags_inserted"),
    ("dags", "dags_updated"),
    ("workers", "workers_inserted"),
    ("workers", "workers_updated"),
    ("workers", "workers_deleted"),
];

impl Database {
    /// Whether the writes announce their changes (the change triggers are on): on Postgres all
    /// of them enabled, on SQLite the `changes_switch` row.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn change_notifications(&self) -> Result<bool, DbError> {
        match self.pool() {
            Pool::Postgres(pool) => {
                let names: Vec<&str> = triggers().map(|(_, name)| name).collect();
                let on: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_trigger WHERE tgname = ANY($1) AND tgenabled <> 'D'",
                )
                .bind(&names)
                .fetch_one(pool)
                .await?;
                Ok(usize::try_from(on).is_ok_and(|on| on == names.len()))
            }
            Pool::Sqlite(pool) => {
                let on: i64 = sqlx::query_scalar("SELECT notify FROM changes_switch")
                    .fetch_one(pool)
                    .await?;
                Ok(on != 0)
            }
        }
    }

    /// Turns the change triggers on or off (`[database] change_notifications`), only if they
    /// aren't so already; whether anything changed. On Postgres it alters the tables (a brief
    /// exclusive lock on each): `schedr db migrate` and `--migrate` call it, holding the
    /// master's lease, never a running master.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails.
    pub async fn set_change_notifications(&self, on: bool) -> Result<bool, DbError> {
        if self.change_notifications().await? == on {
            return Ok(false);
        }
        match self.pool() {
            Pool::Postgres(pool) => {
                let verb = if on { "ENABLE" } else { "DISABLE" };
                // Not `pool.begin()`: a start cut short mustn't go back to the pool (B84).
                let mut starting = crate::query::Starting::new(pool.acquire().await?);
                let mut tx = starting.begin("BEGIN").await?;
                for (table, name) in triggers() {
                    let statement = format!("ALTER TABLE {table} {verb} TRIGGER {name}");
                    sqlx::query(sqlx::AssertSqlSafe(statement))
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
            }
            Pool::Sqlite(pool) => {
                sqlx::query("UPDATE changes_switch SET notify = $1")
                    .bind(i64::from(on))
                    .execute(pool)
                    .await?;
            }
        }
        Ok(true)
    }
}

/// [`POSTGRES_TRIGGERS`] (fixed names: formatting them into SQL is safe).
fn triggers() -> impl Iterator<Item = (&'static str, &'static str)> {
    POSTGRES_TRIGGERS.into_iter()
}

impl Changes {
    /// Waits for the next changes, each once, in order. A lost listening connection is
    /// re-established, and what changed meanwhile comes as [`Change::Missed`].
    ///
    /// # Errors
    ///
    /// [`DbError`] if the database fails (polling) or the connection can't be re-established.
    pub async fn next(&mut self) -> Result<Vec<Change>, DbError> {
        match &mut self.source {
            Source::Listen(listener) => {
                let mut changes = BTreeSet::new();
                match listener.try_recv().await? {
                    Some(first) => changes.extend(Change::parse(first.payload())),
                    // The connection was lost (and is re-established by the next call).
                    None => return Ok(vec![Change::Missed]),
                }
                // Those that already arrived come with it.
                while let Ok(Ok(Some(more))) =
                    tokio::time::timeout(Duration::ZERO, listener.try_recv()).await
                {
                    changes.extend(Change::parse(more.payload()));
                }
                Ok(changes.into_iter().collect())
            }
            Source::Poll { db, after } => loop {
                tokio::time::sleep(POLL_INTERVAL).await;
                let changes = read_after(db, after).await?;
                if !changes.is_empty() {
                    return Ok(changes);
                }
            },
        }
    }
}

/// The `changes` rows after `after`, each change once; moves `after` past them. A gap before
/// the first (rows pruned unread) is [`Change::Missed`].
async fn read_after(db: &Database, after: &mut i64) -> Result<Vec<Change>, DbError> {
    const AFTER: &str = "SELECT seq, change FROM changes WHERE seq > $1 ORDER BY seq LIMIT $2";
    let rows: Vec<(i64, String)> = on_pool!(db, |pool, backend| {
        sqlx::query_as(sql(backend, AFTER))
            .bind(*after)
            .bind(POLL_ROWS)
            .fetch_all(pool)
            .await?
    });
    let mut changes = BTreeSet::new();
    // `seq` counts from 1 and only the oldest rows are pruned: a first row past the next one
    // means rows were pruned unread (from an empty table's cursor, 0, too).
    if let Some((first, _)) = rows.first()
        && *first > *after + 1
    {
        changes.insert(Change::Missed);
    }
    if let Some((last, _)) = rows.last() {
        *after = *last;
    }
    changes.extend(rows.iter().filter_map(|(_, text)| Change::parse(text)));
    Ok(changes.into_iter().collect())
}
