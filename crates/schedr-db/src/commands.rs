//! Command rows: written by the webserver, applied by the master, kept as the audit trail.
//!
//! A command is decided once: `pending` becomes `applied` (with what it produced) or
//! `rejected` (with a reason), and never changes again.

use jiff::Timestamp;
use schedr_model::commands::{Command, CommandStatus};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, from_name, on_pool, parse_time, sql, to_json};

#[cfg(test)]
mod tests;

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
        Ok(id)
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
