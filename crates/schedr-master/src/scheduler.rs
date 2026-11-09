//! The scheduler pass: new scheduled runs, queued runs started, and manual runs.
//!
//! Each pass looks at the DAGs that can run (not removed, not in conflict, with a version) that
//! it visits: an unpaused DAG gets runs for its due fire times ([`Plan::scheduled_runs`]), then queued
//! runs take the places left ([`runs_to_start`]). Pausing stops only the first part. Runs are
//! stored before anything else happens to them, and a scheduled run is unique per
//! `planned_for`, so a pass repeated after a crash creates nothing twice.
//!
//! Which DAGs a pass looks at is [`QuietDags`]'s choice: every DAG, or the busy ones and the
//! quiet ones woken (see archive/PROJECT.md, "Quiet DAGs").

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use jiff::Timestamp;
use schedr_db::runs::{ActiveDag, Held, NewRun, QueuedRun};
use schedr_db::{Database, DbError};
use schedr_model::{DagId, RunKind, RunState};

use crate::fire_times::{FireTimes, FireTimesError};
use crate::metrics::MasterMetrics;
use crate::planning::{ManualRunError, Plan, RunTimes, manual_planned_for, runs_to_start};
use crate::quiet::{QuietDags, QuietSettings, Round, Visit, Visited, WakeReason, Wakes};

/// The most scheduled runs a round creates for one DAG: a DAG catching up after a long pause
/// goes over a few rounds, the earliest first (B18).
pub const SCHEDULED_PER_ROUND: usize = 10;
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
    /// The DAG's file fails to import in its root's current parse (decision 72): no run is
    /// created until it imports again.
    #[error("{}", held.reason(dag_id.as_str()))]
    Held {
        /// The DAG.
        dag_id: DagId,
        /// Why.
        held: Held,
    },
    /// An event trigger starts its runs, and the run asked for brings no event.
    #[error(
        "DAG `{0}`: its runs start from events, so this one needs an event (`--event <key>` for one \
         it found, or `--event-key` and `--event-data`)"
    )]
    NeedsEvent(DagId),
    /// A run brings an event, and no event trigger starts the DAG's runs.
    #[error("DAG `{0}`: no event trigger starts its runs, so its tasks read no event")]
    NoEventTrigger(DagId),
    /// A manual run's time isn't allowed.
    #[error(transparent)]
    ManualRun(#[from] ManualRunError),
    /// The run's parameters are refused (decision 133): the reason is shown.
    #[error("DAG `{dag_id}`: {reason}")]
    Params {
        /// The DAG.
        dag_id: DagId,
        /// Why.
        reason: String,
    },
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

/// The scheduler over a database (cheap to clone; clones share which DAGs are quiet).
#[derive(Debug, Clone)]
pub struct Scheduler {
    db: Database,
    quiet: Arc<Mutex<QuietDags>>,
    wakes: Wakes,
}

impl Scheduler {
    /// A scheduler storing runs in `db`, with the default quiet DAG settings.
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self::with_quiet(db, QuietSettings::default(), Wakes::default())
    }

    /// A scheduler storing runs in `db`, skipping quiet DAGs as `settings` say, woken by
    /// `wakes` (shared with what changes runs).
    #[must_use]
    pub fn with_quiet(db: Database, settings: QuietSettings, wakes: Wakes) -> Self {
        Self {
            db,
            quiet: Arc::new(Mutex::new(QuietDags::new(settings, wakes.clone()))),
            wakes,
        }
    }

    /// Counts busy and quiet DAGs, and wakes, in `metrics`.
    pub fn set_metrics(&self, metrics: MasterMetrics) {
        self.quiet().set_metrics(metrics);
    }

    /// The marks that wake quiet DAGs.
    #[must_use]
    pub const fn wakes(&self) -> &Wakes {
        &self.wakes
    }

    fn quiet(&self) -> MutexGuard<'_, QuietDags> {
        self.quiet.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// One pass at `now` over the DAGs that can run: every one, or the busy ones and the
    /// quiet ones woken.
    ///
    /// # Errors
    ///
    /// [`SchedulerError::Db`] if the database fails (the pass stops; what was stored stays).
    pub async fn pass(&self, now: Timestamp) -> Result<Pass, SchedulerError> {
        // Any queued run wakes its DAG, whatever stored it (one small query by the index of
        // unfinished runs): no path that queues a run needs to remember a mark.
        if self.quiet().enabled() {
            for dag in self.db.dags_with_queued_runs().await? {
                self.wakes.wake(&dag, WakeReason::RunQueued);
            }
        }
        let round = self.quiet().round(now);
        let result = self.visit(round, now).await;
        if result.is_err() {
            // Nothing the round would have changed is kept: the next one retries it.
            self.quiet().round_failed();
        }
        result
    }

    /// The round's work: the DAGs `round` names visited at `now`.
    async fn visit(&self, round: Round, now: Timestamp) -> Result<Pass, SchedulerError> {
        let mut pass = Pass::default();
        let visit = match round.visit {
            Visit::Some(mut dags) if round.check => {
                // Every DAG's ID and version against memory: one missed by every mark too.
                let versions = self.db.active_dag_versions().await?;
                dags.extend(self.quiet().checked(&versions));
                dags.sort();
                dags.dedup();
                Visit::Some(dags)
            }
            visit => visit,
        };
        // The DAGs' runs at once: a round costs a few queries, not three per DAG (B12), and
        // reads only the DAGs it visits.
        let (overview, active_dags) = match &visit {
            Visit::All => (self.db.run_overview().await?, self.db.active_dags().await?),
            Visit::Some(dags) => {
                let dags: Vec<&DagId> = dags.iter().collect();
                (
                    self.db.run_overview_of(&dags).await?,
                    self.db.active_dags_of(&dags).await?,
                )
            }
        };
        let mut seen = HashSet::with_capacity(active_dags.len());
        for active in active_dags {
            seen.insert(active.dag.id.clone());
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
            let queued = overview.queued(&active.dag.id);
            let mut new_runs = 0;
            // Held (decision 72): its fire times create nothing (missed, as when paused) and
            // its queued runs wait.
            let held = active.held.as_ref();
            if !active.paused && held.is_none() {
                let last = overview.last_scheduled(&active.dag.id);
                // At most so many a DAG a round: a long catch-up goes over a few (B18).
                for times in plan
                    .scheduled_runs(last, running, now)
                    .into_iter()
                    .take(SCHEDULED_PER_ROUND)
                {
                    // The defaults, checked when the DAG was validated.
                    let params = match run_params(&active, None) {
                        Ok(params) => params,
                        Err(error) => {
                            pass.skipped.push(error);
                            break;
                        }
                    };
                    let new = new_run(&active, RunKind::Scheduled, times, now, SCHEDULER, &params);
                    if let Some(run_id) = self.db.insert_run(&new).await? {
                        running += 1;
                        new_runs += 1;
                        pass.created
                            .push(created(&active, run_id, RunKind::Scheduled, times));
                    }
                }
            }
            if held.is_none() {
                pass.started
                    .extend(self.start_queued(queued, &plan, running, now).await?);
            }
            let why = held.map(|h| h.reason(active.dag.id.as_str()));
            let found = Visited {
                version: &active.version_id,
                active: running > 0 || new_runs > 0 || !queued.is_empty(),
                // Not paused when held: visited at its fire times, to see the hold lift.
                paused: active.paused,
                held: why.as_deref(),
                next_fire: plan.next_fire_after(now),
            };
            self.quiet().visited(&active.dag.id, found, now);
        }
        self.quiet().round_done(&visit, &seen);
        Ok(pass)
    }

    /// Creates a manual run for `requested` (`--for`) or now, by `user`; it starts at once
    /// if the DAG has a free place, and waits as `queued` otherwise. A paused DAG's manual
    /// runs run normally.
    ///
    /// # Errors
    ///
    /// [`SchedulerError::NotRunnable`], [`SchedulerError::Held`] while its file fails to
    /// import, [`SchedulerError::ManualRun`] for a time in the future,
    /// [`SchedulerError::FireTimes`] or [`SchedulerError::Db`].
    ///
    pub async fn trigger(
        &self,
        dag_id: &DagId,
        requested: Option<Timestamp>,
        user: &str,
        now: Timestamp,
    ) -> Result<CreatedRun, SchedulerError> {
        self.trigger_as(None, dag_id, requested, (None, None), user, now)
            .await
    }

    /// As [`Self::trigger`], the run's ID given (`trigger_run` passes its command's): a run
    /// with that ID already there (the command applied before its decision was stored) is
    /// the one returned, not created again (B7). `params`: the values given for the run's
    /// parameters, over the DAG's defaults.
    ///
    /// # Errors
    ///
    /// As [`Self::trigger`], and [`SchedulerError::Params`] for parameters refused.
    pub async fn trigger_as(
        &self,
        run_id: Option<&str>,
        dag_id: &DagId,
        requested: Option<Timestamp>,
        (params, event): (
            Option<&serde_json::Map<String, serde_json::Value>>,
            Option<&schedr_model::RunEvent>,
        ),
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
        if let Some(held) = active.held {
            return Err(SchedulerError::Held {
                dag_id: dag_id.clone(),
                held,
            });
        }
        if active.dag.trigger.brings_event() != event.is_some() {
            return Err(if event.is_some() {
                SchedulerError::NoEventTrigger(dag_id.clone())
            } else {
                SchedulerError::NeedsEvent(dag_id.clone())
            });
        }
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
        let params = run_params(&active, params)?;
        let mut new = new_run(&active, RunKind::Manual, times, now, user, &params);
        new.state = RunState::Queued;
        new.event = event;
        self.wakes.wake(dag_id, WakeReason::RunQueued);
        // Only scheduled runs are unique per `planned_for`: a manual one is stored unless a
        // run with its given ID is there already, and then that one is it.
        let run_id = run_id.map_or_else(|| uuid::Uuid::now_v7().to_string(), str::to_owned);
        self.db.insert_run_as(&run_id, &new).await?;
        let running = self.db.running_count(dag_id).await?;
        let queued = self.db.queued_runs(dag_id).await?;
        self.start_queued(&queued, &plan, running, now).await?;
        Ok(created(&active, run_id, RunKind::Manual, times))
    }

    /// Stores what a DAG's trigger found: a key not seen before queues a run, as a manual one
    /// (`since` is the previous event's `planned_for`), a repeat moves the bookmark only. Gives
    /// the run's ID, `None` for a repeat.
    ///
    /// # Errors
    ///
    /// [`SchedulerError::Params`] for parameters refused, and what storing it fails with.
    pub async fn event_run(
        &self,
        active: &ActiveDag,
        event: &schedr_model::RunEvent,
        since: Option<Timestamp>,
        now: Timestamp,
    ) -> Result<Option<String>, SchedulerError> {
        let planned_for = event.at.unwrap_or(now);
        // An event may be for a moment before the last one's (a late webhook, a backfill): its
        // window is then empty, never inverted (Grok's review of M4.11e).
        let times = RunTimes {
            planned_for,
            since: since
                .filter(|since| *since <= planned_for)
                .unwrap_or(planned_for),
        };
        let params = run_params(active, None)?;
        let mut new = new_run(active, RunKind::Event, times, now, EVENT_USER, &params);
        new.state = RunState::Queued;
        new.event = Some(event);
        let run_id = uuid::Uuid::now_v7().to_string();
        let stored = self.db.store_event(&run_id, &new, event).await?;
        if stored.is_some() {
            self.wakes.wake(&active.dag.id, WakeReason::RunQueued);
        }
        Ok(stored)
    }

    /// Stores a batch a trigger found (decision 352): each item an event, a run for the new ones
    /// only (B188), as [`Self::event_run`] does for one. Gives the run's ID, `None` when every
    /// item was seen before.
    ///
    /// # Errors
    ///
    /// As [`Self::event_run`].
    pub async fn batch_run(
        &self,
        active: &ActiveDag,
        batch: &schedr_model::RunEvent,
        items: &[(schedr_model::RunEvent, serde_json::Value)],
        since: Option<Timestamp>,
        now: Timestamp,
    ) -> Result<Option<String>, SchedulerError> {
        let planned_for = batch.at.unwrap_or(now);
        let times = RunTimes {
            planned_for,
            since: since
                .filter(|since| *since <= planned_for)
                .unwrap_or(planned_for),
        };
        let params = run_params(active, None)?;
        let mut new = new_run(active, RunKind::Event, times, now, EVENT_USER, &params);
        new.state = RunState::Queued;
        let run_id = uuid::Uuid::now_v7().to_string();
        let stored = self
            .db
            .store_batch(&run_id, &new, &batch.key, items)
            .await?;
        if stored.is_some() {
            self.wakes.wake(&active.dag.id, WakeReason::RunQueued);
        }
        Ok(stored)
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

/// A run's parameters: `given`'s over the DAG's defaults, checked (decision 133).
fn run_params(
    active: &ActiveDag,
    given: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<serde_json::Value, SchedulerError> {
    crate::params::run_params(active.dag.params.as_ref(), given).map_err(|reason| {
        SchedulerError::Params {
            dag_id: active.dag.id.clone(),
            reason,
        }
    })
}

/// Who an event's run was started by.
const EVENT_USER: &str = "trigger";

fn new_run<'a>(
    active: &'a ActiveDag,
    kind: RunKind,
    times: RunTimes,
    now: Timestamp,
    by: &'a str,
    params: &'a serde_json::Value,
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
        params,
        event: None,
    }
}
