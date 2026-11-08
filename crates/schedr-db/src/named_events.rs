//! Events posted through the API (decision 354): `POST /api/v1/events`. Each post is an event of
//! its own, a repeat of a name and key too; a trigger starts a run per key it hasn't seen, and a
//! step waiting in a run matches one posted since its run started.

use jiff::Timestamp;
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_json, in_tx, on_pool, parse_time, sql, sql_text, to_json};

/// An event to post.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNamedEvent<'a> {
    /// Its name.
    pub name: &'a str,
    /// Its key.
    pub key: &'a str,
    /// Its data, as JSON.
    pub data: &'a serde_json::Value,
    /// When it happened; a run it starts is planned for it.
    pub at: Timestamp,
}

/// An event as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedEvent {
    /// Its ID.
    pub id: String,
    /// Its name.
    pub name: String,
    /// Its key.
    pub key: String,
    /// Its data.
    pub data: serde_json::Value,
    /// When it happened.
    pub at: Timestamp,
    /// When it was posted.
    pub posted_at: Timestamp,
    /// Who posted it.
    pub posted_by: String,
}

/// What a post did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Posted {
    /// Stored, under this ID.
    New(String),
    /// Sent again with its `Idempotency-Key`: the event it stored then.
    Again(String),
    /// The key was used for another event.
    KeyTaken,
}

/// The columns read, in [`row`]'s order.
const COLUMNS: &str =
    "id{text}, name, key, data{text}, {ts<}at{>ts}, {ts<}posted_at{>ts}, posted_by";

type Row = (String, String, String, String, String, String, String);

fn row((id, name, key, data, at, posted_at, posted_by): Row) -> Result<NamedEvent, DbError> {
    Ok(NamedEvent {
        id,
        name,
        key,
        data: from_json("named_events.data", &data)?,
        at: parse_time("named_events.at", &at)?,
        posted_at: parse_time("named_events.posted_at", &posted_at)?,
        posted_by,
    })
}

impl Database {
    /// Posts `event` for `posted_by` at `now`, once per `request_key` (the client's
    /// `Idempotency-Key`): sent again after its answer was lost, it finds the event it stored, as
    /// long as the same person sends the same event.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a statement fails or the stored event doesn't read.
    pub async fn post_named_event(
        &self,
        event: &NewNamedEvent<'_>,
        posted_by: &str,
        request_key: Option<&str>,
        now: Timestamp,
    ) -> Result<Posted, DbError> {
        const INSERT: &str = "INSERT INTO named_events \
            (id, name, key, data, at, posted_at, posted_by, request_key) \
            VALUES ($1{uuid}, $2, $3, $4{json}, $5{ts}, $6{ts}, $7, $8) \
            ON CONFLICT (posted_by, request_key) WHERE request_key IS NOT NULL DO NOTHING";
        const BY_KEY: &str = "SELECT id{text}, name, key, data{text} FROM named_events \
            WHERE posted_by = $1 AND request_key = $2";
        let id = Uuid::now_v7().to_string();
        let data = to_json(event.data);
        let earlier: Option<(String, String, String, String)> = in_tx!(self, |tx, backend| {
            let inserted = sqlx::query(sql(backend, INSERT))
                .bind(&id)
                .bind(event.name)
                .bind(event.key)
                .bind(&data)
                .bind(db_time(event.at))
                .bind(db_time(now))
                .bind(posted_by)
                .bind(request_key)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if inserted == 1 {
                None
            } else {
                Some(
                    sqlx::query_as(sql(backend, BY_KEY))
                        .bind(posted_by)
                        .bind(request_key)
                        .fetch_one(&mut *tx)
                        .await?,
                )
            }
        });
        let Some((earlier, name, key, stored)) = earlier else {
            return Ok(Posted::New(id));
        };
        let stored: serde_json::Value = from_json("named_events.data", &stored)?;
        Ok(
            if name == event.name && key == event.key && &stored == event.data {
                Posted::Again(earlier)
            } else {
                Posted::KeyTaken
            },
        )
    }

    /// The events named `name` posted from `since` on (strictly after `after`, a `(posted_at,
    /// id)` cursor, when given), oldest first, at most `limit`: what an `EventNamed` trigger
    /// hasn't seen.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn named_events_after(
        &self,
        name: &str,
        since: Timestamp,
        after: Option<(Timestamp, &str)>,
        limit: u32,
    ) -> Result<Vec<NamedEvent>, DbError> {
        let query = format!(
            "SELECT {COLUMNS} FROM named_events WHERE name = $1 AND posted_at >= $2{{ts}} \
             AND ($3{{ts}} IS NULL OR posted_at > $3{{ts}} OR (posted_at = $3{{ts}} \
             AND id > $4{{uuid}})) ORDER BY posted_at, id LIMIT $5"
        );
        let (after_at, after_id) = after.map_or((None, None), |(at, id)| (Some(at), Some(id)));
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &query))
                .bind(name)
                .bind(db_time(since))
                .bind(after_at.map(db_time))
                .bind(after_id)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(row).collect()
    }

    /// The events named `name` posted from `since` on whose key `dag_id`'s events don't have,
    /// oldest first, at most `limit`: what an `EventNamed` trigger starts runs for (decision 354).
    /// Read by the keys a DAG has, not strictly after the last one seen, so one stored late
    /// (its time taken before its commit) is still found; `since` bounds the look back.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn named_events_unseen(
        &self,
        name: &str,
        dag_id: &str,
        since: Timestamp,
        limit: u32,
    ) -> Result<Vec<NamedEvent>, DbError> {
        let query = format!(
            "SELECT {COLUMNS} FROM named_events n WHERE n.name = $1 AND n.posted_at >= $2{{ts}} \
             AND NOT EXISTS (SELECT 1 FROM events e WHERE e.dag_id = $3 AND e.key = n.key) \
             ORDER BY n.posted_at, n.id LIMIT $4"
        );
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &query))
                .bind(name)
                .bind(db_time(since))
                .bind(dag_id)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(row).collect()
    }

    /// The first event named `name` with `key` posted from `since` on: what a step waiting in a
    /// run started at `since` gets (decision 354: one posted before the step began waiting, but
    /// after its run started, counts).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the row doesn't read.
    pub async fn first_named_event(
        &self,
        name: &str,
        key: Option<&str>,
        since: Timestamp,
    ) -> Result<Option<NamedEvent>, DbError> {
        let query = format!(
            "SELECT {COLUMNS} FROM named_events WHERE name = $1 AND ($2 IS NULL OR key = $2) \
             AND posted_at >= $3{{ts}} ORDER BY posted_at, id LIMIT 1"
        );
        let found: Option<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &query))
                .bind(name)
                .bind(key)
                .bind(db_time(since))
                .fetch_optional(pool)
                .await?
        });
        found.map(row).transpose()
    }

    /// One event, by its ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the row doesn't read.
    pub async fn named_events_by_id(&self, id: &str) -> Result<Option<NamedEvent>, DbError> {
        let query = format!("SELECT {COLUMNS} FROM named_events WHERE id = $1{{uuid}}");
        let found: Option<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &query))
                .bind(id)
                .fetch_optional(pool)
                .await?
        });
        found.map(row).transpose()
    }

    /// The newest events, of `name` if given, at most `limit`.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn named_events(
        &self,
        name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<NamedEvent>, DbError> {
        let query = format!(
            "SELECT {COLUMNS} FROM named_events WHERE ($1 IS NULL OR name = $1) \
             ORDER BY posted_at DESC, id DESC LIMIT $2"
        );
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql_text(backend, &query))
                .bind(name)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(row).collect()
    }
}

#[cfg(test)]
mod tests;
