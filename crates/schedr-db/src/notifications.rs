//! Notifications to deliver (decision 410): the outbox. A notification is written in the
//! transaction that records its event; the master's sender delivers it and deletes it, or
//! retries it later, or gives up on it.

use jiff::Timestamp;
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, parse_time, sql, to_json};

#[cfg(test)]
mod tests;

/// A notification to queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNotification {
    /// The DAG it is about.
    pub dag_id: String,
    /// The channel's name.
    pub channel: String,
    /// How it is sent: `webhook` (the body is posted) or `email` (the body holds the email).
    pub kind: String,
    /// A DAG webhook's URL; `None` for a server channel, looked up when it is sent.
    pub url: Option<String>,
    /// The event's name (`RUN_FAILED`, `MESSAGE`).
    pub event: String,
    /// The JSON to post, filled in.
    pub body: serde_json::Value,
}

/// A notification waiting to be delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    /// Its ID.
    pub id: String,
    /// The DAG it is about.
    pub dag_id: String,
    /// The channel's name.
    pub channel: String,
    /// How it is sent: `webhook` or `email`.
    pub kind: String,
    /// A DAG webhook's URL; `None` for a server channel.
    pub url: Option<String>,
    /// The event's name.
    pub event: String,
    /// The JSON to post.
    pub body: serde_json::Value,
    /// When it was queued.
    pub created_at: Timestamp,
    /// How many deliveries failed so far.
    pub tries: u32,
}

/// A failed task of a run, for a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedTask {
    /// Its ID.
    pub task_id: String,
    /// Its instance's index, for a mapped task; -1 otherwise.
    pub map_index: i32,
    /// Its last attempt's error summary (masked by the agent), if any.
    pub error: Option<String>,
}

type Row = (
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    i32,
);

impl Database {
    /// Queues `notifications`, due now.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn queue_notifications(
        &self,
        notifications: &[NewNotification],
        now: Timestamp,
    ) -> Result<(), DbError> {
        const INSERT: &str = "INSERT INTO notifications \
            (id, dag_id, channel, kind, url, event, body, created_at, next_try_at) \
            VALUES ($1{uuid}, $2, $3, $4, $5, $6, $7{json}, $8{ts}, $8{ts})";
        if notifications.is_empty() {
            return Ok(());
        }
        in_tx!(self, |tx, backend| {
            for notification in notifications {
                sqlx::query(sql(backend, INSERT))
                    .bind(Uuid::now_v7().to_string())
                    .bind(&notification.dag_id)
                    .bind(&notification.channel)
                    .bind(&notification.kind)
                    .bind(&notification.url)
                    .bind(&notification.event)
                    .bind(to_json(&notification.body))
                    .bind(db_time(now))
                    .execute(&mut *tx)
                    .await?;
            }
        });
        Ok(())
    }

    /// At most `limit` notifications due at `now`, the earliest due first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn due_notifications(
        &self,
        now: Timestamp,
        limit: u32,
    ) -> Result<Vec<Notification>, DbError> {
        const DUE: &str = "SELECT id{text}, dag_id, channel, kind, url, event, body{text}, \
            created_at{text}, tries FROM notifications WHERE next_try_at <= $1{ts} \
            ORDER BY next_try_at, id LIMIT $2";
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, DUE))
                .bind(db_time(now))
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(
                |(id, dag_id, channel, kind, url, event, body, created_at, tries)| {
                    Ok(Notification {
                        id,
                        dag_id,
                        channel,
                        kind,
                        url,
                        event,
                        body: from_json("notifications.body", &body)?,
                        created_at: parse_time("notifications.created_at", &created_at)?,
                        tries: u32::try_from(tries).unwrap_or(0),
                    })
                },
            )
            .collect()
    }

    /// Deletes notification `id`: delivered, or given up on.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn delete_notification(&self, id: &str) -> Result<(), DbError> {
        const DELETE: &str = "DELETE FROM notifications WHERE id = $1{uuid}";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, DELETE))
                .bind(id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Notification `id` failed with `error`: tried again from `next_try_at`.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn retry_notification(
        &self,
        id: &str,
        next_try_at: Timestamp,
        error: &str,
    ) -> Result<(), DbError> {
        const RETRY: &str = "UPDATE notifications SET tries = tries + 1, \
            next_try_at = $1{ts}, last_error = $2 WHERE id = $3{uuid}";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, RETRY))
                .bind(db_time(next_try_at))
                .bind(error)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Counts a notification attempt `attempt_id` sends itself, if it has sent fewer than
    /// `max`: whether it may.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn count_attempt_notification(
        &self,
        attempt_id: &str,
        max: u32,
    ) -> Result<bool, DbError> {
        const COUNT: &str = "UPDATE attempts SET notifications_sent = notifications_sent + 1 \
            WHERE id = $1{uuid} AND notifications_sent < $2";
        let counted = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, COUNT))
                .bind(attempt_id)
                .bind(i64::from(max))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(counted == 1)
    }

    /// Records attempt `attempt_id`'s call `request_key`: whether it is new (one sent again
    /// isn't).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn first_notification_request(
        &self,
        attempt_id: &str,
        request_key: &str,
    ) -> Result<bool, DbError> {
        const INSERT: &str = "INSERT INTO notification_requests (attempt_id, request_key) \
            VALUES ($1{uuid}, $2) ON CONFLICT DO NOTHING";
        let inserted = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, INSERT))
                .bind(attempt_id)
                .bind(request_key)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(inserted == 1)
    }

    /// Whether the DAG's run that ended last before `run_id` failed (for `RUN_RECOVERED`).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn previous_run_failed(&self, dag_id: &str, run_id: &str) -> Result<bool, DbError> {
        const PREVIOUS: &str = "SELECT state FROM runs WHERE dag_id = $1 AND id <> $2{uuid} \
            AND ended_at IS NOT NULL AND state IN ('success', 'failed') \
            ORDER BY ended_at DESC, id DESC LIMIT 1";
        let state: Option<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, PREVIOUS))
                .bind(dag_id)
                .bind(run_id)
                .fetch_optional(pool)
                .await?
        });
        Ok(state.is_some_and(|(state,)| state == "failed"))
    }

    /// At most `limit` failed tasks of run `run_id`, with their last attempt's error summary:
    /// a mapped task's failed instances one by one (its own row only when none of them failed:
    /// it failed as a whole, its expansion say).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn failed_tasks(&self, run_id: &str, limit: u32) -> Result<Vec<FailedTask>, DbError> {
        const FAILED: &str = "SELECT ti.task_id, ti.map_index, \
            (SELECT a.error_summary{text} FROM attempts a WHERE a.task_instance_id = ti.id \
             ORDER BY a.try_number DESC LIMIT 1) \
            FROM task_instances ti WHERE ti.run_id = $1{uuid} AND ti.state = 'failed' \
            AND (ti.map_index >= 0 OR NOT EXISTS (SELECT 1 FROM task_instances c \
                 WHERE c.run_id = ti.run_id AND c.task_id = ti.task_id AND c.map_index >= 0 \
                 AND c.state = 'failed')) \
            ORDER BY ti.task_id, ti.map_index LIMIT $2";
        let rows: Vec<(String, i32, Option<String>)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, FAILED))
                .bind(run_id)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        Ok(rows
            .into_iter()
            .map(|(task_id, map_index, error)| FailedTask {
                task_id,
                map_index,
                error: error.as_deref().map(readable),
            })
            .collect())
    }

    /// The error summary of task instance `instance_id`'s last attempt, if it has one.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn instance_error(&self, instance_id: &str) -> Result<Option<String>, DbError> {
        const ERROR: &str = "SELECT error_summary{text} FROM attempts \
            WHERE task_instance_id = $1{uuid} ORDER BY try_number DESC LIMIT 1";
        let row: Option<(Option<String>,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ERROR))
                .bind(instance_id)
                .fetch_optional(pool)
                .await?
        });
        Ok(row.and_then(|(error,)| error).as_deref().map(readable))
    }
}

/// An error summary as one line of text: `exception: message` (stored as JSON).
#[must_use]
pub fn readable(stored: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(stored) else {
        return stored.to_owned();
    };
    let field = |name: &str| value.get(name).and_then(serde_json::Value::as_str);
    match (field("exception"), field("message")) {
        (Some(exception), Some(message)) if !message.is_empty() => {
            format!("{exception}: {message}")
        }
        (Some(exception), _) => exception.to_owned(),
        _ => value
            .as_str()
            .map_or_else(|| stored.to_owned(), str::to_owned),
    }
}
