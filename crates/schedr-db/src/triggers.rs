//! Event-triggered DAGs' triggers and what they found (M4.11b): each new key starts a run, and the
//! last key acted on is the bookmark a trigger starts again from.

use jiff::Timestamp;
use schedr_model::RunEvent;

use crate::database::{Database, DbError};
use crate::query::{db_time, in_tx, on_pool, optional_time, sql, to_json};
use crate::runs::NewRun;

#[cfg(test)]
mod tests;

/// An event as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    /// The event.
    pub event: RunEvent,
    /// When the master got it.
    pub received_at: Timestamp,
}

type EventRow = (String, String, Option<String>, String);

fn stored_event((key, data, at, received_at): EventRow) -> Result<StoredEvent, DbError> {
    Ok(StoredEvent {
        event: RunEvent {
            key,
            data: crate::query::from_json("events.data", &data)?,
            at: optional_time("events.at", at)?,
        },
        received_at: crate::query::parse_time("events.received_at", &received_at)?,
    })
}

/// A DAG's trigger.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TriggerRecord {
    /// Its DAG.
    pub dag_id: String,
    /// This run of it, while a worker has it.
    pub instance_id: Option<String>,
    /// The worker that has it.
    pub worker_id: Option<String>,
    /// The DAG version it runs: a new one starts it again.
    pub dag_version_id: Option<String>,
    /// The hash of the root it runs from: other code starts it again.
    pub code_hash: Option<String>,
    /// The last key acted on: its `after` when it starts.
    pub bookmark: Option<String>,
    /// `planned_for` of the last event's run: the next one's `since`.
    pub last_planned_for: Option<Timestamp>,
    /// Why it ended last, if it failed.
    pub last_error: Option<String>,
    /// Its failures in a row.
    pub failures: u32,
    /// Not started again before this moment.
    pub not_before: Option<Timestamp>,
    /// Why it isn't to run although its DAG wants it.
    pub stopped_why: Option<String>,
    /// What it stopped on: its DAG version and code.
    pub stopped_on: Option<String>,
    /// When this run of it started.
    pub started_at: Option<Timestamp>,
    /// When it last found something, new or not.
    pub last_event_at: Option<Timestamp>,
    /// When it was last started again for finding nothing for too long.
    pub last_restart_at: Option<Timestamp>,
    /// When a poke's call last ended.
    pub last_poked_at: Option<Timestamp>,
}

/// Why a trigger isn't to run: too many queued event runs, until they have started.
pub const STOPPED_BACK_PRESSURE: &str = "back_pressure";
/// Why a trigger isn't to run: its generator returned, until a new version or new code.
pub const STOPPED_RETURNED: &str = "returned";

type Row = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    i32,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn record(row: Row) -> Result<TriggerRecord, DbError> {
    let (
        dag_id,
        instance_id,
        worker_id,
        version,
        code_hash,
        bookmark,
        last,
        last_error,
        failures,
        not_before,
        stopped_why,
        stopped_on,
        started_at,
        last_event_at,
        last_restart_at,
        last_poked_at,
    ) = row;
    Ok(TriggerRecord {
        dag_id,
        instance_id,
        worker_id,
        dag_version_id: version,
        code_hash,
        bookmark,
        last_planned_for: optional_time("trigger_progress.last_planned_for", last)?,
        last_error,
        failures: u32::try_from(failures).unwrap_or(0),
        not_before: optional_time("trigger_progress.not_before", not_before)?,
        stopped_why,
        stopped_on,
        started_at: optional_time("trigger_progress.started_at", started_at)?,
        last_event_at: optional_time("trigger_progress.last_event_at", last_event_at)?,
        last_restart_at: optional_time("trigger_progress.last_restart_at", last_restart_at)?,
        last_poked_at: optional_time("trigger_progress.last_poked_at", last_poked_at)?,
    })
}

impl Database {
    /// Every trigger the master has run, by DAG.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn triggers(&self) -> Result<Vec<TriggerRecord>, DbError> {
        const ALL: &str = "SELECT dag_id, instance_id{text}, worker_id, dag_version_id{text}, \
            code_hash, bookmark, {ts<}last_planned_for{>ts}, last_error, failures, {ts<}not_before{>ts}, \
            stopped_why, stopped_on, {ts<}started_at{>ts}, {ts<}last_event_at{>ts}, \
            {ts<}last_restart_at{>ts}, {ts<}last_poked_at{>ts} \
            FROM trigger_progress ORDER BY dag_id";
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ALL)).fetch_all(pool).await?
        });
        rows.into_iter().map(record).collect()
    }

    /// A DAG's trigger, if it has run or found anything.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn trigger(&self, dag_id: &str) -> Result<Option<TriggerRecord>, DbError> {
        const ONE: &str = "SELECT dag_id, instance_id{text}, worker_id, dag_version_id{text}, \
            code_hash, bookmark, {ts<}last_planned_for{>ts}, last_error, failures, {ts<}not_before{>ts}, \
            stopped_why, stopped_on, {ts<}started_at{>ts}, {ts<}last_event_at{>ts}, \
            {ts<}last_restart_at{>ts}, {ts<}last_poked_at{>ts} \
            FROM trigger_progress WHERE dag_id = $1";
        let row: Option<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ONE))
                .bind(dag_id)
                .fetch_optional(pool)
                .await?
        });
        row.map(record).transpose()
    }

    /// Records that a worker was given a DAG's trigger.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn trigger_assigned(
        &self,
        dag_id: &str,
        instance_id: &str,
        worker_id: &str,
        dag_version_id: &str,
        code_hash: &str,
        now: Timestamp,
    ) -> Result<(), DbError> {
        const SET: &str = "INSERT INTO trigger_progress \
            (dag_id, instance_id, worker_id, dag_version_id, code_hash, started_at) \
            VALUES ($1, $2, $3, $4, $5, $6{ts}) ON CONFLICT (dag_id) DO UPDATE SET \
            instance_id = excluded.instance_id, worker_id = excluded.worker_id, \
            dag_version_id = excluded.dag_version_id, code_hash = excluded.code_hash, \
            started_at = excluded.started_at, stopped_why = NULL, stopped_on = NULL";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(dag_id)
                .bind(instance_id)
                .bind(worker_id)
                .bind(dag_version_id)
                .bind(code_hash)
                .bind(db_time(now))
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Records that this run of a DAG's trigger ended, with why if it failed, and when it may
    /// start again; what isn't this run any more is left alone. Gives whether it was.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn trigger_ended(
        &self,
        dag_id: &str,
        instance_id: &str,
        error: Option<&str>,
        not_before: Option<Timestamp>,
    ) -> Result<bool, DbError> {
        const END: &str = "UPDATE trigger_progress SET instance_id = NULL, worker_id = NULL, \
            dag_version_id = NULL, code_hash = NULL, last_error = COALESCE($1, last_error), \
            failures = failures + $2, not_before = $3{ts} \
            WHERE dag_id = $4 AND instance_id = $5";
        let ended = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, END))
                .bind(error)
                .bind(i32::from(error.is_some()))
                .bind(not_before.map(db_time))
                .bind(dag_id)
                .bind(instance_id)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(ended == 1)
    }

    /// Stores what a trigger found, in one transaction: the event; for a key not seen before,
    /// `run`; and the key as the bookmark, whichever it was. Gives the run's ID, `None` for a
    /// repeat.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn store_event(
        &self,
        run_id: &str,
        run: &NewRun<'_>,
        event: &RunEvent,
    ) -> Result<Option<String>, DbError> {
        const EVENT: &str = "INSERT INTO events (dag_id, key, data, at, received_at) \
            VALUES ($1, $2, $3{json}, $4{ts}, $5{ts}) ON CONFLICT (dag_id, key) DO NOTHING";
        const BOOKMARK: &str = "INSERT INTO trigger_progress \
            (dag_id, bookmark, last_planned_for, failures, last_event_at) \
            VALUES ($1, $2, $3{ts}, 0, $4{ts}) ON CONFLICT (dag_id) DO UPDATE SET \
            bookmark = excluded.bookmark, failures = 0, last_error = NULL, \
            last_event_at = excluded.last_event_at, \
            last_planned_for = COALESCE(excluded.last_planned_for, trigger_progress.last_planned_for)";
        let dag_id = run.dag_id.as_str();
        Box::pin(self.atomically(async {
            let new = in_tx!(self, |tx, backend| {
                sqlx::query(sql(backend, EVENT))
                    .bind(dag_id)
                    .bind(&event.key)
                    .bind(to_json(&event.data))
                    .bind(event.at.map(db_time))
                    .bind(db_time(run.triggered_at))
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
            }) == 1;
            if new {
                self.insert_run_as(run_id, run).await?;
            }
            in_tx!(self, |tx, backend| {
                sqlx::query(sql(backend, BOOKMARK))
                    .bind(dag_id)
                    .bind(&event.key)
                    .bind(new.then(|| db_time(run.planned_for)))
                    .bind(db_time(run.triggered_at))
                    .execute(&mut *tx)
                    .await?;
            });
            Ok(new.then(|| run_id.to_owned()))
        }))
        .await
    }

    /// Stores a batch a trigger found (`@trigger.poke(batch=...)`, decision 352), in one
    /// transaction: each item as an event of its own; for the items whose keys weren't seen
    /// before, `run`, its event their list as stored (`raw`), keyed by the last of them; and the
    /// batch's `key`, its last item's, as the bookmark. Gives the run's ID, `None` when every item
    /// was seen before (B188: a window ending with a repeat lost the new items before it).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if a statement fails.
    pub async fn store_batch(
        &self,
        run_id: &str,
        run: &NewRun<'_>,
        key: &str,
        items: &[(RunEvent, serde_json::Value)],
    ) -> Result<Option<String>, DbError> {
        const EVENT: &str = "INSERT INTO events (dag_id, key, data, at, received_at) \
            VALUES ($1, $2, $3{json}, $4{ts}, $5{ts}) ON CONFLICT (dag_id, key) DO NOTHING";
        const BOOKMARK: &str = "INSERT INTO trigger_progress \
            (dag_id, bookmark, last_planned_for, failures, last_event_at) \
            VALUES ($1, $2, $3{ts}, 0, $4{ts}) ON CONFLICT (dag_id) DO UPDATE SET \
            bookmark = excluded.bookmark, failures = 0, last_error = NULL, \
            last_event_at = excluded.last_event_at, \
            last_planned_for = COALESCE(excluded.last_planned_for, trigger_progress.last_planned_for)";
        let dag_id = run.dag_id.as_str();
        Box::pin(self.atomically(async {
            let mut new = Vec::new();
            for (item, raw) in items {
                let stored = in_tx!(self, |tx, backend| {
                    sqlx::query(sql(backend, EVENT))
                        .bind(dag_id)
                        .bind(&item.key)
                        .bind(to_json(&item.data))
                        .bind(item.at.map(db_time))
                        .bind(db_time(run.triggered_at))
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                }) == 1;
                if stored {
                    new.push((item, raw));
                }
            }
            let started = new.last().map(|(last, _)| RunEvent {
                key: last.key.clone(),
                data: serde_json::Value::Array(new.iter().map(|(_, raw)| (*raw).clone()).collect()),
                at: None,
            });
            if let Some(event) = &started {
                let run = NewRun {
                    event: Some(event),
                    ..run.clone()
                };
                self.insert_run_as(run_id, &run).await?;
            }
            in_tx!(self, |tx, backend| {
                sqlx::query(sql(backend, BOOKMARK))
                    .bind(dag_id)
                    .bind(key)
                    .bind(started.is_some().then(|| db_time(run.planned_for)))
                    .bind(db_time(run.triggered_at))
                    .execute(&mut *tx)
                    .await?;
            });
            Ok(started.map(|_| run_id.to_owned()))
        }))
        .await
    }

    /// Says why a trigger isn't to run although its DAG wants it, and on what it stopped; `None`:
    /// it may run again.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn set_trigger_stopped(
        &self,
        dag_id: &str,
        why: Option<(&str, &str)>,
    ) -> Result<(), DbError> {
        const SET: &str =
            "UPDATE trigger_progress SET stopped_why = $1, stopped_on = $2 WHERE dag_id = $3";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(why.map(|(why, _)| why))
                .bind(why.map(|(_, on)| on))
                .bind(dag_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// Records that a trigger was started again for finding nothing for too long.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn trigger_idle_restart(&self, dag_id: &str, now: Timestamp) -> Result<(), DbError> {
        const SET: &str = "UPDATE trigger_progress SET last_restart_at = $1{ts} WHERE dag_id = $2";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(db_time(now))
                .bind(dag_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// How a poke's calls have gone: its failures in a row, and why the last one failed; one that
    /// went well clears them.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn trigger_poked(
        &self,
        dag_id: &str,
        instance_id: &str,
        at: Timestamp,
        error: Option<&str>,
        failures: u32,
    ) -> Result<(), DbError> {
        const SET: &str = "UPDATE trigger_progress SET last_poked_at = $1{ts}, last_error = $2, \
            failures = $3 WHERE dag_id = $4 AND instance_id = $5";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(db_time(at))
                .bind(error)
                .bind(i32::try_from(failures).unwrap_or(i32::MAX))
                .bind(dag_id)
                .bind(instance_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The queued runs of kind `event`, by DAG.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn queued_event_runs(&self) -> Result<Vec<(String, i64)>, DbError> {
        const COUNT: &str = "SELECT dag_id, COUNT(*) FROM runs \
            WHERE state = 'queued' AND kind = 'event' GROUP BY dag_id";
        let rows: Vec<(String, i64)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, COUNT)).fetch_all(pool).await?
        });
        Ok(rows)
    }

    /// Asks for a trigger to start again, whatever stopped it: what it waited for is forgotten,
    /// and one running looks as if of an older version, so the next pass stops it. `None` when it
    /// never ran.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn restart_trigger(&self, dag_id: &str) -> Result<(), DbError> {
        const SET: &str = "UPDATE trigger_progress SET stopped_why = NULL, stopped_on = NULL, \
            not_before = NULL, dag_version_id = NULL WHERE dag_id = $1";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(dag_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// An event of a DAG by its key, as stored.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn stored_event(&self, dag_id: &str, key: &str) -> Result<Option<RunEvent>, DbError> {
        const ONE: &str = "SELECT key, data{text}, {ts<}at{>ts}, {ts<}received_at{>ts} FROM events \
            WHERE dag_id = $1 AND key = $2";
        let row: Option<EventRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ONE))
                .bind(dag_id)
                .bind(key)
                .fetch_optional(pool)
                .await?
        });
        Ok(row.map(stored_event).transpose()?.map(|found| found.event))
    }

    /// A DAG's events, newest received first (then by key), to `limit`; those after the one at
    /// `after`, its time and key.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn events(
        &self,
        dag_id: &str,
        limit: u32,
        after: Option<(Timestamp, &str)>,
    ) -> Result<Vec<StoredEvent>, DbError> {
        const ALL: &str = "SELECT key, data{text}, {ts<}at{>ts}, {ts<}received_at{>ts} FROM events \
            WHERE dag_id = $1 AND ($2{ts} IS NULL OR received_at < $2{ts} \
                OR (received_at = $2{ts} AND key > $3{text})) \
            ORDER BY received_at DESC, key LIMIT $4";
        let rows: Vec<EventRow> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ALL))
                .bind(dag_id)
                .bind(after.map(|(at, _)| db_time(at)))
                .bind(after.map(|(_, key)| key))
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(stored_event).collect()
    }

    /// A DAG's queued runs of kind `event`.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn queued_event_runs_of(&self, dag_id: &str) -> Result<i64, DbError> {
        const COUNT: &str = "SELECT COUNT(*) FROM runs \
            WHERE state = 'queued' AND kind = 'event' AND dag_id = $1";
        let count: i64 = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, COUNT))
                .bind(dag_id)
                .fetch_one(pool)
                .await?
        });
        Ok(count)
    }

    /// The event that started a run; `None` for a run no event started.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn run_event(&self, run_id: &str) -> Result<Option<RunEvent>, DbError> {
        const EVENT: &str = "SELECT event{text} FROM runs WHERE id = $1{uuid}";
        let text: Option<Option<String>> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, EVENT))
                .bind(run_id)
                .fetch_optional(pool)
                .await?
        });
        text.flatten()
            .map(|t| crate::query::from_json("runs.event", &t))
            .transpose()
    }
}
