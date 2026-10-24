//! DAG runs: creating them, and what the scheduler reads to decide on new ones.
//!
//! A scheduled run is unique per (DAG, `planned_for`) (a partial unique index), so a master
//! that crashes after creating one can't create it twice; manual, backfill, chained and event runs
//! are exempt (see PROJECT.md, Runs: manual runs, backfill and pausing).

use std::collections::BTreeMap;
use std::sync::Arc;

use jiff::Timestamp;
use schedr_model::{Dag, DagId, RunKind, RunState};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_name, in_tx, on_pool, parse_time, sql};

#[cfg(test)]
mod tests;

/// A run to create.
#[derive(Debug, Clone)]
pub struct NewRun<'a> {
    /// The DAG.
    pub dag_id: &'a DagId,
    /// The DAG version it runs.
    pub dag_version_id: &'a str,
    /// Who or what created it.
    pub kind: RunKind,
    /// `running`, or `queued` to wait for `max_active_runs`.
    pub state: RunState,
    /// The moment it is for.
    pub planned_for: Timestamp,
    /// The start of its window.
    pub since: Timestamp,
    /// When it was created.
    pub triggered_at: Timestamp,
    /// `scheduler`, a user name, or the parent run.
    pub triggered_by: &'a str,
}

/// A run waiting for `max_active_runs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedRun {
    /// The run's ID.
    pub id: String,
    /// Its kind (which decides who gets a freed place first).
    pub kind: RunKind,
    /// When it was created.
    pub triggered_at: Timestamp,
}

/// What the scheduler needs of every DAG's runs, read at once for a whole round
/// ([`Database::run_overview`]): a few queries, however many DAGs there are (B12).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOverview {
    /// Running runs, per DAG.
    pub running: BTreeMap<String, u32>,
    /// The latest scheduled run's `planned_for`, per DAG.
    pub last_scheduled: BTreeMap<String, Timestamp>,
    /// Queued runs, per DAG, oldest first.
    pub queued: BTreeMap<String, Vec<QueuedRun>>,
}

impl RunOverview {
    /// `dag_id`'s running runs (0 if none).
    #[must_use]
    pub fn running(&self, dag_id: &DagId) -> u32 {
        self.running.get(dag_id.as_str()).copied().unwrap_or(0)
    }

    /// `dag_id`'s latest scheduled `planned_for`, if it has a scheduled run.
    #[must_use]
    pub fn last_scheduled(&self, dag_id: &DagId) -> Option<Timestamp> {
        self.last_scheduled.get(dag_id.as_str()).copied()
    }

    /// `dag_id`'s queued runs, oldest first.
    #[must_use]
    pub fn queued(&self, dag_id: &DagId) -> &[QueuedRun] {
        self.queued.get(dag_id.as_str()).map_or(&[], Vec::as_slice)
    }
}

/// A DAG the scheduler looks at: not removed, not in conflict, with a current version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveDag {
    /// Whether it is paused (no new scheduled runs; queued runs still start).
    pub paused: bool,
    /// When the master first stored it.
    pub first_seen: Timestamp,
    /// Its current version's ID.
    pub version_id: String,
    /// Its current version.
    pub dag: Arc<Dag>,
}

impl Database {
    /// Creates a run and returns its ID; `None` if it is a scheduled run for a `planned_for`
    /// that already has one.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn insert_run(&self, run: &NewRun<'_>) -> Result<Option<String>, DbError> {
        self.insert_run_as(&Uuid::now_v7().to_string(), run).await
    }

    /// As [`Self::insert_run`], with the run's ID given (a command's, so applying it again
    /// finds the run it made); `None` also when a run with this ID exists.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn insert_run_as(
        &self,
        id: &str,
        run: &NewRun<'_>,
    ) -> Result<Option<String>, DbError> {
        const INSERT: &str = "INSERT INTO runs \
            (id, dag_id, dag_version_id, kind, state, planned_for, since, triggered_at, \
             triggered_by, started_at) \
            VALUES ($1{uuid}, $2, $3{uuid}, $4, $5, $6{ts}, $7{ts}, $8{ts}, $9, $10{ts}) \
            ON CONFLICT DO NOTHING";
        let started_at = (run.state == RunState::Running).then(|| db_time(run.triggered_at));
        let inserted = in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, INSERT))
                .bind(id)
                .bind(run.dag_id.as_str())
                .bind(run.dag_version_id)
                .bind(run.kind.to_string())
                .bind(run.state.to_string())
                .bind(db_time(run.planned_for))
                .bind(db_time(run.since))
                .bind(db_time(run.triggered_at))
                .bind(run.triggered_by)
                .bind(started_at)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        });
        Ok((inserted == 1).then(|| id.to_owned()))
    }

    /// The latest `planned_for` of the DAG's scheduled runs: the scheduler's bookkeeping,
    /// which manual and backfill runs never change.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn last_scheduled_for(&self, dag_id: &DagId) -> Result<Option<Timestamp>, DbError> {
        const LAST: &str = "SELECT {ts<}MAX(planned_for){>ts} FROM runs \
            WHERE dag_id = $1 AND kind = 'scheduled'";
        self.optional_time(LAST, dag_id, None).await
    }

    /// The `planned_for` of the DAG's run nearest before `before`, of any kind.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or the stored time doesn't read.
    pub async fn nearest_earlier_run(
        &self,
        dag_id: &DagId,
        before: Timestamp,
    ) -> Result<Option<Timestamp>, DbError> {
        const NEAREST: &str = "SELECT {ts<}MAX(planned_for){>ts} FROM runs \
            WHERE dag_id = $1 AND planned_for < $2{ts}";
        self.optional_time(NEAREST, dag_id, Some(before)).await
    }

    async fn optional_time(
        &self,
        query: &'static str,
        dag_id: &DagId,
        at: Option<Timestamp>,
    ) -> Result<Option<Timestamp>, DbError> {
        let text: Option<String> = on_pool!(self, |pool, backend| {
            let mut q = sqlx::query_scalar(sql(backend, query)).bind(dag_id.as_str());
            if let Some(at) = at {
                q = q.bind(db_time(at));
            }
            q.fetch_one(pool).await?
        });
        text.map(|t| parse_time("runs.planned_for", &t)).transpose()
    }

    /// How many of the DAG's runs are running (what `max_active_runs` limits).
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn running_count(&self, dag_id: &DagId) -> Result<u32, DbError> {
        const COUNT: &str = "SELECT COUNT(*) FROM runs WHERE dag_id = $1 AND state = 'running'";
        let count: i64 = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, COUNT))
                .bind(dag_id.as_str())
                .fetch_one(pool)
                .await?
        });
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    /// The DAG's queued runs, oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored value doesn't read.
    pub async fn queued_runs(&self, dag_id: &DagId) -> Result<Vec<QueuedRun>, DbError> {
        const QUEUED: &str = "SELECT id{text}, kind, {ts<}triggered_at{>ts} FROM runs \
            WHERE dag_id = $1 AND state = 'queued' ORDER BY triggered_at, id";
        let rows: Vec<(String, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, QUEUED))
                .bind(dag_id.as_str())
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(id, kind, triggered_at)| {
                Ok(QueuedRun {
                    id,
                    kind: from_name("runs.kind", kind)?,
                    triggered_at: parse_time("runs.triggered_at", &triggered_at)?,
                })
            })
            .collect()
    }

    /// Every DAG's running count, latest scheduled `planned_for` and queued runs, in three
    /// queries: what a scheduler round reads, instead of three queries per DAG (B12). The
    /// master is the only writer of runs, so the round can work from it until it ends.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or a stored value doesn't read.
    pub async fn run_overview(&self) -> Result<RunOverview, DbError> {
        const RUNNING: &str =
            "SELECT dag_id, COUNT(*) FROM runs WHERE state = 'running' GROUP BY dag_id";
        const LAST: &str = "SELECT dag_id, {ts<}MAX(planned_for){>ts} FROM runs \
            WHERE kind = 'scheduled' GROUP BY dag_id";
        const QUEUED: &str = "SELECT dag_id, id{text}, kind, {ts<}triggered_at{>ts} FROM runs \
            WHERE state = 'queued' ORDER BY dag_id, triggered_at, id";
        type Rows = (
            Vec<(String, i64)>,
            Vec<(String, String)>,
            Vec<(String, String, String, String)>,
        );
        // One connection, one time limit, for all three.
        let (running, last, queued): Rows = on_pool!(self, |pool, backend| {
            (
                sqlx::query_as(sql(backend, RUNNING))
                    .fetch_all(&mut *pool)
                    .await?,
                sqlx::query_as(sql(backend, LAST))
                    .fetch_all(&mut *pool)
                    .await?,
                sqlx::query_as(sql(backend, QUEUED))
                    .fetch_all(&mut *pool)
                    .await?,
            )
        });
        let mut overview = RunOverview {
            running: running
                .into_iter()
                .map(|(dag, count)| (dag, u32::try_from(count).unwrap_or(u32::MAX)))
                .collect(),
            last_scheduled: last
                .into_iter()
                .map(|(dag, at)| Ok((dag, parse_time("runs.planned_for", &at)?)))
                .collect::<Result<_, DbError>>()?,
            queued: BTreeMap::new(),
        };
        for (dag, id, kind, triggered_at) in queued {
            overview.queued.entry(dag).or_default().push(QueuedRun {
                id,
                kind: from_name("runs.kind", kind)?,
                triggered_at: parse_time("runs.triggered_at", &triggered_at)?,
            });
        }
        Ok(overview)
    }

    /// The DAG's runs not finished (`queued` or `running`), oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the query fails.
    pub async fn runs_in_progress(&self, dag_id: &DagId) -> Result<Vec<String>, DbError> {
        const IN_PROGRESS: &str = "SELECT id{text} FROM runs \
            WHERE dag_id = $1 AND state IN ('queued', 'running') ORDER BY triggered_at, id";
        let ids: Vec<String> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, IN_PROGRESS))
                .bind(dag_id.as_str())
                .fetch_all(pool)
                .await?
        });
        Ok(ids)
    }

    /// Starts a queued run: it becomes `running`.
    ///
    /// # Errors
    ///
    /// [`DbError::Sqlx`] if the statement fails.
    pub async fn start_run(&self, run_id: &str, now: Timestamp) -> Result<(), DbError> {
        const START: &str = "UPDATE runs SET state = 'running', started_at = $1{ts} \
            WHERE id = $2{uuid} AND state = 'queued'";
        in_tx!(self, |tx, backend| {
            sqlx::query(sql(backend, START))
                .bind(db_time(now))
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
        });
        Ok(())
    }

    /// The DAGs the scheduler looks at: not removed, not in conflict, with a current
    /// version; in DAG ID order.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn active_dags(&self) -> Result<Vec<ActiveDag>, DbError> {
        const ACTIVE: &str = "SELECT d.paused, {ts<}d.first_seen_at{>ts}, d.current_version_id{text} \
            FROM dags d WHERE d.current_version_id IS NOT NULL \
            AND d.removed_at IS NULL AND d.conflict IS NULL ORDER BY d.dag_id";
        let rows: Vec<(bool, String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, ACTIVE)).fetch_all(pool).await?
        });
        let mut active = Vec::with_capacity(rows.len());
        for (paused, first_seen, version_id) in rows {
            // The definition from the cache: parsed once per version, not every round (B13).
            let dag = self.dag_version(&version_id).await?;
            active.push(ActiveDag {
                paused,
                first_seen: parse_time("dags.first_seen_at", &first_seen)?,
                version_id,
                dag,
            });
        }
        Ok(active)
    }
}
