//! Command rows: written by the webserver, applied by the master, kept as the audit trail.
//!
//! A command is decided once: `pending` becomes `applied` (with what it produced) or
//! `rejected` (with a reason), and never changes again.
//!
//! On Postgres a new command also sends a notification on [`COMMANDS_CHANNEL`], so the master
//! picks it up at once ([`Database::command_notifications`]); polling stays the fallback, and
//! the only way on SQLite (see PROJECT.md, Web UI and CLI).

use std::time::Duration;

use jiff::Timestamp;
use schedr_model::commands::{Command, CommandStatus};
use sqlx::postgres::PgListener;
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, from_name, on_pool, parse_time, sql, to_json};

#[cfg(test)]
mod tests;

/// The Postgres notification channel a new command is announced on.
pub const COMMANDS_CHANNEL: &str = "schedr_commands";

/// Notifications of new commands (Postgres only): a dedicated connection listening on
/// [`COMMANDS_CHANNEL`].
#[derive(Debug)]
pub struct CommandNotifications {
    listener: PgListener,
}

impl CommandNotifications {
    /// Waits for the next notification. A lost connection is re-established (a notification
    /// sent meanwhile is lost; polling picks the command up).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the connection fails and can't be re-established.
    pub async fn next(&mut self) -> Result<(), DbError> {
        self.listener.recv().await?;
        Ok(())
    }

    /// Takes the notifications that already arrived, without waiting: one tick handles every
    /// pending command, so a burst needs one tick, not one per command.
    pub async fn skip_arrived(&mut self) {
        while let Ok(Ok(_)) = tokio::time::timeout(Duration::ZERO, self.listener.recv()).await {}
    }
}

/// A pending command, as the master reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCommand {
    /// Its ID.
    pub id: String,
    /// The command, or why its stored payload doesn't read (then it is rejected).
    pub command: Result<Command, String>,
    /// Who asked for it.
    pub requested_by: String,
}

/// A command row, as the CLI and the UI read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRecord {
    /// Its ID.
    pub id: String,
    /// Its kind (`trigger_run`, ...).
    pub kind: String,
    /// Where it stands.
    pub status: CommandStatus,
    /// Why it was rejected.
    pub reason: Option<String>,
    /// What it produced when applied (e.g. `{"run_id": ...}`).
    pub result: Option<serde_json::Value>,
    /// Who asked for it.
    pub requested_by: String,
    /// When.
    pub requested_at: Timestamp,
    /// When the master decided it.
    pub decided_at: Option<Timestamp>,
}

type RecordRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<String>,
);

impl Database {
    /// Writes a command for the master; returns its ID.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn insert_command(
        &self,
        command: &Command,
        requested_by: &str,
        now: Timestamp,
    ) -> Result<String, DbError> {
        const INSERT: &str = "INSERT INTO commands (id, kind, payload, requested_by, requested_at) \
            VALUES ($1{uuid}, $2, $3{json}, $4, $5{ts})";
        let id = Uuid::now_v7().to_string();
        on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, INSERT))
                .bind(&id)
                .bind(command.kind())
                .bind(to_json(command))
                .bind(requested_by)
                .bind(db_time(now))
                .execute(pool)
                .await?;
        });
        if let Self::Postgres(pool) = self {
            // The command is stored: a failed notification only delays it to the master's
            // next poll, so it isn't an error (a retried request would store it twice).
            if let Err(error) = sqlx::query("SELECT pg_notify($1, $2)")
                .bind(COMMANDS_CHANNEL)
                .bind(&id)
                .execute(pool)
                .await
            {
                tracing::warn!(%error, command_id = %id, "command stored, notification failed");
            }
        }
        Ok(id)
    }

    /// Listens for new commands: notifications on Postgres; `None` on SQLite, which has none
    /// (the master polls).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the listening connection can't be opened.
    pub async fn command_notifications(&self) -> Result<Option<CommandNotifications>, DbError> {
        match self {
            Self::Sqlite(_) => Ok(None),
            Self::Postgres(pool) => {
                let mut listener = PgListener::connect_with(pool).await?;
                listener.listen(COMMANDS_CHANNEL).await?;
                Ok(Some(CommandNotifications { listener }))
            }
        }
    }

    /// The pending commands, oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn pending_commands(&self) -> Result<Vec<PendingCommand>, DbError> {
        const PENDING: &str = "SELECT id{text}, payload{text}, requested_by FROM commands \
            WHERE status = 'pending' ORDER BY requested_at, id";
        let rows: Vec<(String, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, PENDING))
                .fetch_all(pool)
                .await?
        });
        Ok(rows
            .into_iter()
            .map(|(id, payload, requested_by)| PendingCommand {
                id,
                command: serde_json::from_str(&payload).map_err(|e| e.to_string()),
                requested_by,
            })
            .collect())
    }

    /// Decides a pending command: `applied` with its result, or `rejected` with a reason.
    /// Returns whether it was still pending.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn decide_command(
        &self,
        id: &str,
        status: CommandStatus,
        reason: Option<&str>,
        result: Option<&serde_json::Value>,
        now: Timestamp,
    ) -> Result<bool, DbError> {
        const DECIDE: &str = "UPDATE commands SET status = $1, reason = $2, result = $3{json}, \
            decided_at = $4{ts} WHERE id = $5{uuid} AND status = 'pending'";
        let decided = on_pool!(self, |pool, backend| {
            sqlx::query(sql(backend, DECIDE))
                .bind(
                    serde_json::to_value(status)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned)),
                )
                .bind(reason)
                .bind(result.map(to_json))
                .bind(db_time(now))
                .bind(id)
                .execute(pool)
                .await?
                .rows_affected()
        });
        Ok(decided == 1)
    }

    /// A command row by ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn command(&self, id: &str) -> Result<Option<CommandRecord>, DbError> {
        const ONE: &str = "SELECT id{text}, kind, status, reason, result{text}, requested_by, \
            {ts<}requested_at{>ts}, {ts<}decided_at{>ts} FROM commands WHERE id = $1{uuid}";
        let row: Option<RecordRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ONE))
                .bind(id)
                .fetch_optional(pool)
                .await?
        });
        row.map(record).transpose()
    }
}

fn record(row: RecordRow) -> Result<CommandRecord, DbError> {
    let (id, kind, status, reason, result, requested_by, requested_at, decided_at) = row;
    Ok(CommandRecord {
        id,
        kind,
        status: from_name("commands.status", status)?,
        reason,
        result: result
            .map(|r| from_json("commands.result", &r))
            .transpose()?,
        requested_by,
        requested_at: parse_time("commands.requested_at", &requested_at)?,
        decided_at: decided_at
            .map(|t| parse_time("commands.decided_at", &t))
            .transpose()?,
    })
}
