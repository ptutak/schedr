//! The scheduler pass: new scheduled runs, queued runs started, and manual runs.
//!
//! Each pass looks at every DAG that can run (not removed, not in conflict, with a version):
//! an unpaused DAG gets runs for its due fire times ([`Plan::scheduled_runs`]), then queued
//! runs take the places left ([`runs_to_start`]). Pausing stops only the first part. Runs are
//! stored before anything else happens to them, and a scheduled run is unique per
//! `planned_for`, so a pass repeated after a crash creates nothing twice.

use jiff::Timestamp;
use schedr_db::runs::{ActiveDag, NewRun, QueuedRun};
use schedr_db::{Database, DbError};
use schedr_model::{DagId, RunKind, RunState};

use crate::fire_times::{FireTimes, FireTimesError};
use crate::planning::{ManualRunError, Plan, RunTimes, manual_planned_for, runs_to_start};

#[cfg(test)]
mod tests;

/// Who created scheduled runs, as recorded in `triggered_by`.
pub const SCHEDULER: &str = "scheduler";

/// Why a scheduler operation failed.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The DAG doesn't exist, was removed, is in conflict or has no version.
    #[error("DAG `{0}` can't run: it doesn't exist, was removed or is in conflict")]
    NotRunnable(DagId),
    /// A manual run's time isn't allowed.
    #[error(transparent)]
    ManualRun(#[from] ManualRunError),
    /// The fire times of the DAG's stored trigger can't be evaluated (it passed validation
    /// with another version of the master).
    #[error("DAG `{dag_id}`: {error}")]
    FireTimes {
        /// The DAG.
        dag_id: DagId,
        /// Why.
        error: FireTimesError,
    },
}

/// A run created by the scheduler or a person.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedRun {
    /// The DAG.
    pub dag_id: DagId,
    /// The run's ID.
    pub run_id: String,
    /// Its kind.
    pub kind: RunKind,
    /// Its time and window.
    pub times: RunTimes,
}

/// What a pass did.
#[derive(Debug, Default)]
pub struct Pass {
    /// Scheduled runs created.
    pub created: Vec<CreatedRun>,
    /// Queued runs started, by ID.
    pub started: Vec<String>,
    /// DAGs skipped because their schedule can't be evaluated.
    pub skipped: Vec<SchedulerError>,
}

/// The scheduler over a database.
#[derive(Debug, Clone)]
pub struct Scheduler {
    db: Database,
}

impl Scheduler {
    /// A scheduler storing runs in `db`.
    #[must_use]
    pub const fn new(db: Database) -> Self {
        Self { db }
    }

    /// One pass at `now` over every DAG that can run.
    ///
    /// # Errors
    ///
    /// [`SchedulerError::Db`] if the database fails (the pass stops; what was stored stays).
    pub async fn pass(&self, now: Timestamp) -> Result<Pass, SchedulerError> {
        let mut pass = Pass::default();
        // Every DAG's runs at once: a round costs a few queries, not three per DAG (B12).
        let overview = self.db.run_overview().await?;
        for active in self.db.active_dags().await? {
            let fire_times = match FireTimes::new(&active.dag.trigger) {
                Ok(fire_times) => fire_times,
                Err(error) => {
                    pass.skipped.push(SchedulerError::FireTimes {
                        dag_id: active.dag.id.clone(),
                        error,
                    });
                    continue;
                }
            };
            let plan = Plan {
                dag: &active.dag,
                fire_times: &fire_times,
                first_seen: active.first_seen,
            };
            let mut running = overview.running(&active.dag.id);
            if !active.paused {
                let last = overview.last_scheduled(&active.dag.id);
                for times in plan.scheduled_runs(last, running, now) {
                    let new = new_run(&active, RunKind::Scheduled, times, now, SCHEDULER);
                    if let Some(run_id) = self.db.insert_run(&new).await? {
                        running += 1;
                        pass.created
                            .push(created(&active, run_id, RunKind::Scheduled, times));
                    }
                }
            }
            pass.started.extend(
                self.start_queued(overview.queued(&active.dag.id), &plan, running, now)
                    .await?,
            );
        }
        Ok(pass)
    }

    /// Creates a manual run for `requested` (`--for`) or now, by `user`; it starts at once
    /// if the DAG has a free place, and waits as `queued` otherwise. A paused DAG's manual
    /// runs run normally.
    ///
    /// # Errors
    ///
    /// [`SchedulerError::NotRunnable`], [`SchedulerError::ManualRun`] for a time in the future,
    /// [`SchedulerError::FireTimes`] or [`SchedulerError::Db`].
    ///
    pub async fn trigger(
        &self,
        dag_id: &DagId,
        requested: Option<Timestamp>,
        user: &str,
        now: Timestamp,
    ) -> Result<CreatedRun, SchedulerError> {
        self.trigger_as(None, dag_id, requested, user, now).await
    }

    /// As [`Self::trigger`], the run's ID given (`trigger_run` passes its command's): a run
    /// with that ID already there (the command applied before its decision was stored) is
    /// the one returned, not created again (B7).
    ///
    /// # Errors
    ///
    /// As [`Self::trigger`].
    pub async fn trigger_as(
        &self,
        run_id: Option<&str>,
        dag_id: &DagId,
        requested: Option<Timestamp>,
        user: &str,
        now: Timestamp,
    ) -> Result<CreatedRun, SchedulerError> {
        let active = self
            .db
            .active_dags()
            .await?
            .into_iter()
            .find(|d| &d.dag.id == dag_id)
            .ok_or_else(|| SchedulerError::NotRunnable(dag_id.clone()))?;
        let fire_times =
            FireTimes::new(&active.dag.trigger).map_err(|error| SchedulerError::FireTimes {
                dag_id: dag_id.clone(),
                error,
            })?;
        let plan = Plan {
            dag: &active.dag,
            fire_times: &fire_times,
            first_seen: active.first_seen,
        };
        let planned_for = manual_planned_for(requested, now);
        let nearest = self.db.nearest_earlier_run(dag_id, planned_for).await?;
        let times = plan.manual_times(requested, now, nearest)?;
        let mut new = new_run(&active, RunKind::Manual, times, now, user);
        new.state = RunState::Queued;
        // Only scheduled runs are unique per `planned_for`: a manual one is stored unless a
        // run with its given ID is there already, and then that one is it.
        let run_id = run_id.map_or_else(|| uuid::Uuid::now_v7().to_string(), str::to_owned);
        self.db.insert_run_as(&run_id, &new).await?;
        let running = self.db.running_count(dag_id).await?;
        let queued = self.db.queued_runs(dag_id).await?;
        self.start_queued(&queued, &plan, running, now).await?;
        Ok(created(&active, run_id, RunKind::Manual, times))
    }

    /// Starts the DAG's `queued` runs that fit next to its `running` ones.
    async fn start_queued(
        &self,
        queued: &[QueuedRun],
        plan: &Plan<'_>,
        running: u32,
        now: Timestamp,
    ) -> Result<Vec<String>, SchedulerError> {
        let mut started = Vec::new();
        for run in runs_to_start(queued, plan.free_places(running)) {
            self.db.start_run(&run.id, now).await?;
            started.push(run.id.clone());
        }
        Ok(started)
    }
}

fn created(active: &ActiveDag, run_id: String, kind: RunKind, times: RunTimes) -> CreatedRun {
    CreatedRun {
        dag_id: active.dag.id.clone(),
        run_id,
        kind,
        times,
    }
}

fn new_run<'a>(
    active: &'a ActiveDag,
    kind: RunKind,
    times: RunTimes,
    now: Timestamp,
    by: &'a str,
) -> NewRun<'a> {
    NewRun {
        dag_id: &active.dag.id,
        dag_version_id: &active.version_id,
        kind,
        state: RunState::Running,
        planned_for: times.planned_for,
        since: times.since,
        triggered_at: now,
        triggered_by: by,
    }
}
