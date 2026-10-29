//! DAG runs: creating them, and what the scheduler reads to decide on new ones.
//!
//! A scheduled run is unique per (DAG, `planned_for`) (a partial unique index), so a master
//! that crashes after creating one can't create it twice; manual, backfill, chained and event runs
//! are exempt (see PROJECT.md, Runs: manual runs, backfill and pausing).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use jiff::Timestamp;
use schedr_model::{Dag, DagId, RunKind, RunState};
use uuid::Uuid;

use crate::database::{Database, DbError};
use crate::query::{db_time, from_name, in_tx, on_pool, parse_time, sql, sql_text};

#[cfg(test)]
mod tests;

/// The most DAG IDs one query names (well under both databases' parameter limits).
const DAGS_PER_QUERY: usize = 500;

/// ` AND <column> IN ($1, …, $count)`, or nothing for no IDs.
fn among(column: &str, count: usize) -> String {
    if count == 0 {
        return String::new();
    }
    let wanted: Vec<String> = (1..=count).map(|n| format!("${n}")).collect();
    format!(" AND {column} IN ({})", wanted.join(", "))
}

fn dag_id_of(text: String) -> Result<DagId, DbError> {
    DagId::new(text).map_err(|e| DbError::InvalidData {
        what: "dag_id",
        reason: e.to_string(),
    })
}

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
    /// Why it is held, when the file defining it fails to import in its root's current parse
    /// (decision 72): no run is created or started, and manual triggers are refused.
    pub held: Option<Held>,
    /// When the master first stored it.
    pub first_seen: Timestamp,
    /// Its current version's ID.
    pub version_id: String,
    /// Its current version.
    pub dag: Arc<Dag>,
}

/// Why a DAG is held: its file's import error in its root's current parse (decision 72).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    /// The file, relative to its root.
    pub path: String,
    /// The error's exception class name.
    pub exception: String,
    /// The error's message.
    pub message: String,
}

impl Held {
    /// Why `dag_id` gets no runs, for people (the API, the UI, a refused trigger, the log):
    /// it is the DAG's **file** that failed, so every DAG of the file is held, and what to do
    /// about it (decisions 72, 72.3).
    #[must_use]
    pub fn reason(&self, dag_id: &str) -> String {
        use schedr_model::runner::{
            INVALID_DAG_EXCEPTION, INVALID_PATH_EXCEPTION, PARSE_RESULT_TOO_LARGE_EXCEPTION,
        };
        let path = &self.path;
        let message = &self.message;
        let what = match self.exception.as_str() {
            PARSE_RESULT_TOO_LARGE_EXCEPTION => format!(
                "the parse result of its file `{path}` (the JSON of all the file's DAGs \
                 together, not of `{dag_id}` alone) is too large for the master: {message}. \
                 Every DAG of the file is held until it fits: move large DAGs to files of their \
                 own, keep large data out of the DAGs' arguments (load it when the task runs), \
                 or raise [master] grpc_max_message_size"
            ),
            INVALID_DAG_EXCEPTION => format!(
                "its file `{path}` imports, but defines a DAG the master's checks refuse \
                 ({message}). Every DAG of the file is held while one is invalid: fix that DAG, \
                 or move it to a file of its own"
            ),
            INVALID_PATH_EXCEPTION => format!(
                "its file `{path}` isn't inside its root ({message}). Every DAG of the file is \
                 held until it is"
            ),
            exception => format!(
                "its file `{path}` fails to import ({exception}: {message}), so its tasks \
                 couldn't run. Every DAG of the file is held until it imports"
            ),
        };
        format!("DAG `{dag_id}` is held, no runs are created and it can't be triggered: {what}")
    }
    /// The current parses' import errors by (root, path), the files whose DAGs are held: a
    /// transient error (a timeout, a runner that died) holds nothing, since the file is parsed
    /// again and its DAGs keep their last good version meanwhile (decision 72.2).
    #[must_use]
    pub fn by_file(errors: &[crate::dags::ImportErrorRecord]) -> HashMap<(String, String), Self> {
        errors
            .iter()
            .filter(|e| !e.error.is_transient())
            .map(|e| {
                let held = Self {
                    path: e.path.clone(),
                    exception: e.error.exception.clone(),
                    message: e.error.message.clone(),
                };
                ((e.root.clone(), e.path.clone()), held)
            })
            .collect()
    }

    /// Whether `dag`'s file is among `held` files, and why.
    #[must_use]
    pub fn of(dag: &Dag, held: &HashMap<(String, String), Self>) -> Option<Self> {
        held.get(&(dag.location.root.clone(), dag.location.path.clone()))
            .cloned()
    }
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
        let mut overview = RunOverview::default();
        self.overview_into(&mut overview, &[]).await?;
        Ok(overview)
    }

    /// As [`Self::run_overview`], for `dags` only (a round visiting some DAGs, see PROJECT.md,
    /// "Quiet DAGs"): its cost follows them, not every DAG. Nothing for no DAGs.
    ///
    /// # Errors
    ///
    /// [`DbError`] if a query fails or a stored value doesn't read.
    pub async fn run_overview_of(&self, dags: &[&DagId]) -> Result<RunOverview, DbError> {
        let mut overview = RunOverview::default();
        for chunk in dags.chunks(DAGS_PER_QUERY) {
            self.overview_into(&mut overview, chunk).await?;
        }
        Ok(overview)
    }

    /// Adds the overview of `dags` (every DAG when empty) to `overview`.
    async fn overview_into(
        &self,
        overview: &mut RunOverview,
        dags: &[&DagId],
    ) -> Result<(), DbError> {
        type Rows = (
            Vec<(String, i64)>,
            Vec<(String, String)>,
            Vec<(String, String, String, String)>,
        );
        let among = among("dag_id", dags.len());
        let running = format!(
            "SELECT dag_id, COUNT(*) FROM runs WHERE state = 'running'{among} GROUP BY dag_id"
        );
        let last = format!(
            "SELECT dag_id, {{ts<}}MAX(planned_for){{>ts}} FROM runs \
             WHERE kind = 'scheduled'{among} GROUP BY dag_id"
        );
        let queued = format!(
            "SELECT dag_id, id{{text}}, kind, {{ts<}}triggered_at{{>ts}} FROM runs \
             WHERE state = 'queued'{among} ORDER BY dag_id, triggered_at, id"
        );
        // One connection, one time limit, for all three.
        let (running, last, queued): Rows = on_pool!(self, |pool, backend| {
            let mut running_query = sqlx::query_as(sql_text(backend, &running));
            let mut last_query = sqlx::query_as(sql_text(backend, &last));
            let mut queued_query = sqlx::query_as(sql_text(backend, &queued));
            for dag in dags {
                running_query = running_query.bind(dag.as_str());
                last_query = last_query.bind(dag.as_str());
                queued_query = queued_query.bind(dag.as_str());
            }
            (
                running_query.fetch_all(&mut *pool).await?,
                last_query.fetch_all(&mut *pool).await?,
                queued_query.fetch_all(&mut *pool).await?,
            )
        });
        for (dag, count) in running {
            overview
                .running
                .insert(dag, u32::try_from(count).unwrap_or(u32::MAX));
        }
        for (dag, at) in last {
            overview
                .last_scheduled
                .insert(dag, parse_time("runs.planned_for", &at)?);
        }
        for (dag, id, kind, triggered_at) in queued {
            overview.queued.entry(dag).or_default().push(QueuedRun {
                id,
                kind: from_name("runs.kind", kind)?,
                triggered_at: parse_time("runs.triggered_at", &triggered_at)?,
            });
        }
        Ok(())
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
        let mut active = Vec::new();
        self.active_into(&mut active, &[]).await?;
        Ok(active)
    }

    /// The DAGs with queued runs (from any source: manual, chained, event, backfill), by the
    /// partial index of unfinished runs: what wakes a quiet DAG whatever stored the run.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored DAG ID doesn't read.
    pub async fn dags_with_queued_runs(&self) -> Result<Vec<DagId>, DbError> {
        const QUEUED: &str = "SELECT DISTINCT dag_id FROM runs WHERE state = 'queued'";
        let rows: Vec<String> = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql(backend, QUEUED))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter().map(dag_id_of).collect()
    }

    /// The ID and current version of every DAG the scheduler looks at (as
    /// [`Self::active_dags`], without their definitions): one small query, for the quiet DAGs'
    /// check against memory.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a stored DAG ID doesn't read.
    pub async fn active_dag_versions(&self) -> Result<Vec<(DagId, String)>, DbError> {
        const VERSIONS: &str = "SELECT dag_id, current_version_id{text} FROM dags \
            WHERE current_version_id IS NOT NULL AND removed_at IS NULL AND conflict IS NULL \
            ORDER BY dag_id";
        let rows: Vec<(String, String)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, VERSIONS))
                .fetch_all(pool)
                .await?
        });
        rows.into_iter()
            .map(|(dag, version)| Ok((dag_id_of(dag)?, version)))
            .collect()
    }

    /// As [`Self::active_dags`], for `dags` only: those of them that can run.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or stored data doesn't read.
    pub async fn active_dags_of(&self, dags: &[&DagId]) -> Result<Vec<ActiveDag>, DbError> {
        let mut active = Vec::new();
        for chunk in dags.chunks(DAGS_PER_QUERY) {
            self.active_into(&mut active, chunk).await?;
        }
        active.sort_by(|a, b| a.dag.id.cmp(&b.dag.id));
        Ok(active)
    }

    /// Adds the active DAGs among `dags` (every DAG when empty) to `active`.
    async fn active_into(
        &self,
        active: &mut Vec<ActiveDag>,
        dags: &[&DagId],
    ) -> Result<(), DbError> {
        let text = format!(
            "SELECT d.paused, {{ts<}}d.first_seen_at{{>ts}}, d.current_version_id{{text}} \
             FROM dags d WHERE d.current_version_id IS NOT NULL \
             AND d.removed_at IS NULL AND d.conflict IS NULL{} ORDER BY d.dag_id",
            among("d.dag_id", dags.len())
        );
        let rows: Vec<(bool, String, String)> = on_pool!(self, |pool, backend| {
            let mut query = sqlx::query_as(sql_text(backend, &text));
            for dag in dags {
                query = query.bind(dag.as_str());
            }
            query.fetch_all(pool).await?
        });
        active.reserve(rows.len());
        // Usually none: a small query, once a round (decision 72).
        let held = Held::by_file(&self.current_import_errors().await?);
        for (paused, first_seen, version_id) in rows {
            // The definition from the cache: parsed once per version, not every round (B13).
            let dag = self.dag_version(&version_id).await?;
            active.push(ActiveDag {
                paused,
                held: Held::of(&dag, &held),
                first_seen: parse_time("dags.first_seen_at", &first_seen)?,
                version_id,
                dag,
            });
        }
        Ok(())
    }
}
