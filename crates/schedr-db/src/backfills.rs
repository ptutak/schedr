//! Backfills (decisions 363, 364, 400-404): a run per fire time of a range that has no
//! successful, queued or running run (or every fire time), created together with the backfill,
//! oldest first behind the other kinds of runs. The backfill's ID is the command's that asked
//! for it.

use std::collections::BTreeSet;

use jiff::Timestamp;
use schedr_model::{DagId, RunState};

use crate::database::{Database, DbError};
use crate::query::{db_time, from_name, in_tx, on_pool, parse_time, sql};

#[cfg(test)]
mod tests;

/// A backfill to store, with its runs (stored alongside, in the same transaction).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewBackfill<'a> {
    /// Its ID (the command's).
    pub id: &'a str,
    /// The DAG.
    pub dag_id: &'a DagId,
    /// The first moment of its range.
    pub range_from: Timestamp,
    /// The last moment of its range (now, for a range ending later).
    pub range_to: Timestamp,
    /// Whether every fire time got a run (`--all`).
    pub all_fire_times: bool,
    /// Who asked for it.
    pub requested_by: &'a str,
    /// When.
    pub created_at: Timestamp,
    /// How many fire times its range has.
    pub fire_times: u32,
    /// How many of them got a run.
    pub created_runs: u32,
}

/// A backfill as stored, with its runs counted by state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillRecord {
    /// Its ID.
    pub id: String,
    /// The DAG.
    pub dag_id: DagId,
    /// The first moment of its range.
    pub range_from: Timestamp,
    /// The last moment of its range.
    pub range_to: Timestamp,
    /// Whether every fire time got a run (`--all`).
    pub all_fire_times: bool,
    /// Who asked for it.
    pub requested_by: String,
    /// When.
    pub created_at: Timestamp,
    /// How many fire times its range has.
    pub fire_times: u32,
    /// How many of them got a run (the others were skipped).
    pub created_runs: u32,
    /// Its runs by state, each state once, in the order the query gave them.
    pub runs: Vec<(RunState, u32)>,
}

impl BackfillRecord {
    /// Whether some of its runs haven't finished.
    #[must_use]
    pub fn unfinished(&self) -> bool {
        self.runs
            .iter()
            .any(|(state, count)| !state.is_final() && *count > 0)
    }
}

type Row = (
    String,
    String,
    String,
    String,
    bool,
    String,
    String,
    i32,
    i32,
);

impl Database {
    /// Stores a backfill; `false` if one with its ID is there already (a command applied again).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the write fails.
    pub async fn insert_backfill(&self, backfill: &NewBackfill<'_>) -> Result<bool, DbError> {
        const INSERT: &str = "INSERT INTO backfills (id, dag_id, range_from, range_to, \
            all_fire_times, requested_by, created_at, fire_times, created_runs) \
            VALUES ($1{uuid}, $2, $3{ts}, $4{ts}, $5, $6, $7{ts}, $8, $9) ON CONFLICT DO NOTHING";
        let inserted = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, INSERT))
                .bind(backfill.id)
                .bind(backfill.dag_id.as_str())
                .bind(db_time(backfill.range_from))
                .bind(db_time(backfill.range_to))
                .bind(backfill.all_fire_times)
                .bind(backfill.requested_by)
                .bind(db_time(backfill.created_at))
                .bind(i64::from(backfill.fire_times))
                .bind(i64::from(backfill.created_runs))
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok(inserted == 1)
    }

    /// The `planned_for`s in `[from, to]` of the DAG's runs that succeeded or haven't finished:
    /// the fire times a backfill skips without `--all` (decision 400).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored time doesn't read.
    pub async fn covered_times(
        &self,
        dag_id: &DagId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<BTreeSet<Timestamp>, DbError> {
        const COVERED: &str = "SELECT DISTINCT {ts<}planned_for{>ts} FROM runs \
            WHERE dag_id = $1 AND planned_for >= $2{ts} AND planned_for <= $3{ts} \
            AND state IN ('success', 'queued', 'running')";
        let rows: Vec<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, COVERED))
                .bind(dag_id.as_str())
                .bind(db_time(from))
                .bind(db_time(to))
                .fetch_all(pool)
                .await?
        });
        rows.iter()
            .map(|(at,)| parse_time("runs.planned_for", at))
            .collect()
    }

    /// A backfill, with its runs counted by state; `None` for an unknown ID.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or stored data doesn't read.
    pub async fn backfill(&self, id: &str) -> Result<Option<BackfillRecord>, DbError> {
        const ONE: &str = "SELECT id{text}, dag_id, {ts<}range_from{>ts}, {ts<}range_to{>ts}, \
            all_fire_times, requested_by, {ts<}created_at{>ts}, fire_times, created_runs \
            FROM backfills WHERE id = $1{uuid}";
        const COUNTS: &str =
            "SELECT state, COUNT(*) FROM runs WHERE backfill_id = $1{uuid} GROUP BY state";
        let row: Option<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ONE))
                .bind(id)
                .fetch_optional(pool)
                .await?
        });
        let Some(row) = row else {
            return Ok(None);
        };
        let counts: Vec<(String, i64)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, COUNTS))
                .bind(&row.0)
                .fetch_all(pool)
                .await?
        });
        record(row, counts).map(Some)
    }

    /// The DAG's backfills, newest first, at most `limit`.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or stored data doesn't read.
    pub async fn dag_backfills(
        &self,
        dag_id: &DagId,
        limit: u32,
    ) -> Result<Vec<BackfillRecord>, DbError> {
        const OF_DAG: &str = "SELECT id{text}, dag_id, {ts<}range_from{>ts}, {ts<}range_to{>ts}, \
            all_fire_times, requested_by, {ts<}created_at{>ts}, fire_times, created_runs \
            FROM backfills WHERE dag_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2";
        // Their runs, counted in one query, not one per backfill.
        const COUNTS: &str = "SELECT backfill_id{text}, state, COUNT(*) FROM runs \
            WHERE backfill_id IN (SELECT id FROM backfills WHERE dag_id = $1 \
            ORDER BY created_at DESC, id DESC LIMIT $2) GROUP BY backfill_id, state";
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, OF_DAG))
                .bind(dag_id.as_str())
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        let counts: Vec<(String, String, i64)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, COUNTS))
                .bind(dag_id.as_str())
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|row| {
                let of_it: Vec<(String, i64)> = counts
                    .iter()
                    .filter(|(id, _, _)| *id == row.0)
                    .map(|(_, state, count)| (state.clone(), *count))
                    .collect();
                record(row, of_it)
            })
            .collect()
    }

    /// The backfill's runs that haven't finished, oldest first: what cancelling it cancels
    /// (decision 402).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn unfinished_backfill_runs(&self, id: &str) -> Result<Vec<String>, DbError> {
        const UNFINISHED: &str = "SELECT id{text} FROM runs WHERE backfill_id = $1{uuid} \
            AND state IN ('queued', 'running') ORDER BY planned_for, id";
        let rows: Vec<(String,)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, UNFINISHED))
                .bind(id)
                .fetch_all(pool)
                .await?
        });
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

/// A backfill's row with its runs' `counts` by state.
fn record(
    row: Row,
    counts: impl IntoIterator<Item = (String, i64)>,
) -> Result<BackfillRecord, DbError> {
    let (id, dag_id, from, to, all, by, at, fire_times, created) = row;
    let mut runs = Vec::new();
    for (state, count) in counts {
        runs.push((
            from_name("runs.state", state)?,
            u32::try_from(count).unwrap_or(u32::MAX),
        ));
    }
    Ok(BackfillRecord {
        id,
        dag_id: DagId::new(dag_id).map_err(|e| DbError::InvalidData {
            what: "backfills.dag_id",
            reason: e.to_string(),
        })?,
        range_from: parse_time("backfills.range_from", &from)?,
        range_to: parse_time("backfills.range_to", &to)?,
        all_fire_times: all,
        requested_by: by,
        created_at: parse_time("backfills.created_at", &at)?,
        fire_times: u32::try_from(fire_times).unwrap_or(0),
        created_runs: u32::try_from(created).unwrap_or(0),
        runs,
    })
}
