//! Event-triggered DAGs' triggers and what they found (M4.11b): each new key starts a run, and the
//! last key acted on is the bookmark a trigger starts again from.

use jiff::Timestamp;
use schedr_model::RunEvent;

use crate::database::{Database, DbError};
use crate::query::{db_time, in_tx, on_pool, optional_time, sql, to_json};
use crate::runs::NewRun;

#[cfg(test)]
mod tests;

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
}

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
    ) = row;
    Ok(TriggerRecord {
        dag_id,
        instance_id,
        worker_id,
        dag_version_id: version,
        code_hash,
        bookmark,
        last_planned_for: optional_time("triggers.last_planned_for", last)?,
        last_error,
        failures: u32::try_from(failures).unwrap_or(0),
        not_before: optional_time("triggers.not_before", not_before)?,
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
            code_hash, bookmark, {ts<}last_planned_for{>ts}, last_error, failures, {ts<}not_before{>ts} \
            FROM triggers ORDER BY dag_id";
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
            code_hash, bookmark, {ts<}last_planned_for{>ts}, last_error, failures, {ts<}not_before{>ts} \
            FROM triggers WHERE dag_id = $1";
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
    ) -> Result<(), DbError> {
        const SET: &str = "INSERT INTO triggers \
            (dag_id, instance_id, worker_id, dag_version_id, code_hash) \
            VALUES ($1, $2, $3, $4, $5) ON CONFLICT (dag_id) DO UPDATE SET \
            instance_id = excluded.instance_id, worker_id = excluded.worker_id, \
            dag_version_id = excluded.dag_version_id, code_hash = excluded.code_hash";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, SET))
                .bind(dag_id)
                .bind(instance_id)
                .bind(worker_id)
                .bind(dag_version_id)
                .bind(code_hash)
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
        const END: &str = "UPDATE triggers SET instance_id = NULL, worker_id = NULL, \
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
        const BOOKMARK: &str = "INSERT INTO triggers (dag_id, bookmark, last_planned_for, failures) \
            VALUES ($1, $2, $3{ts}, 0) ON CONFLICT (dag_id) DO UPDATE SET \
            bookmark = excluded.bookmark, failures = 0, last_error = NULL, \
            last_planned_for = COALESCE(excluded.last_planned_for, triggers.last_planned_for)";
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
                    .execute(&mut *tx)
                    .await?;
            });
            Ok(new.then(|| run_id.to_owned()))
        }))
        .await
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
