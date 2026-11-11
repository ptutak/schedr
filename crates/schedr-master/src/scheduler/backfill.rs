//! Creating a backfill (decisions 363, 364, 400-404; see archive/PROJECT.md, Runs: manual
//! runs, backfill and pausing): one `backfill` run per fire time of the range with no
//! successful, queued or running run (every one with `--all`), each for one schedule period,
//! all created at once with the backfill, queued oldest first behind the other kinds. A paused
//! DAG's backfill runs start too (decision 401).

use jiff::Timestamp;
use schedr_db::backfills::NewBackfill;
use schedr_model::{DagId, RunKind, RunState};

use super::{Scheduler, SchedulerError, new_run, run_params};
use crate::fire_times::FireTimes;
use crate::planning::{Plan, RunTimes};
use crate::quiet::WakeReason;

/// The most fire times a backfill's range may have unless `[backfill] max_runs` says
/// otherwise, those already run included: a year of a daily DAG, a month of an hourly one.
pub const DEFAULT_MAX_RUNS: u32 = 1000;

/// What a backfill is asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillAsk<'a> {
    /// The range's first moment.
    pub from: Timestamp,
    /// Its last moment; now if not given (a later one ends at now).
    pub to: Option<Timestamp>,
    /// A run for every fire time, not only those missing one (`--all`).
    pub all: bool,
    /// The runs' parameters, over the DAG's defaults, checked once.
    pub params: Option<&'a serde_json::Map<String, serde_json::Value>>,
    /// Count only: nothing is created (`--dry-run`).
    pub dry_run: bool,
}

/// What a backfill found, or created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillCount {
    /// The fire times of its range.
    pub fire_times: u32,
    /// How many got a run (or would).
    pub created: u32,
    /// How many were skipped: a run of theirs succeeded or hasn't finished.
    pub skipped: u32,
    /// The backfill's ID; `None` for a dry run or one with nothing to create.
    pub backfill_id: Option<String>,
}

impl Scheduler {
    /// Creates a backfill of `dag_id` as `ask` says, with the ID `id` (a command's: a command
    /// applied again finds it stored), its runs by `user`, its range of at most `max_runs` fire
    /// times.
    ///
    /// # Errors
    ///
    /// [`SchedulerError::NotRunnable`] or [`SchedulerError::Held`] as for a manual run;
    /// [`SchedulerError::Backfill`] for a DAG without fire times, a range with none or more
    /// than `max_runs` fire times, a range ending before it starts; [`SchedulerError::Params`]
    /// for parameters refused; what storing it fails with.
    pub async fn backfill(
        &self,
        id: &str,
        dag_id: &DagId,
        ask: BackfillAsk<'_>,
        (user, max_runs): (&str, u32),
        now: Timestamp,
    ) -> Result<BackfillCount, SchedulerError> {
        let refused = |why: String| SchedulerError::Backfill {
            dag_id: dag_id.clone(),
            why,
        };
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
        let fire_times =
            FireTimes::new(&active.dag.trigger).map_err(|error| SchedulerError::FireTimes {
                dag_id: dag_id.clone(),
                error,
            })?;
        if matches!(fire_times, FireTimes::None) {
            return Err(refused(
                "its trigger has no fire times (a backfill takes `Cron`, `Every` or `Once`); \
                 start a run with `schedr runs trigger` instead"
                    .to_owned(),
            ));
        }
        let to = ask.to.map_or(now, |to| to.min(now));
        if to < ask.from {
            return Err(refused(format!(
                "the range ends ({to}) before it starts ({}); a range ending later than now ends now",
                ask.from
            )));
        }
        let times = fire_times_in(&fire_times, ask.from, to, max_runs);
        let Some(times) = times else {
            return Err(refused(format!(
                "its range has more than {max_runs} fire times, over `[backfill] max_runs`: \
                 split it"
            )));
        };
        if times.is_empty() {
            return Err(refused(format!("no fire time from {} to {to}", ask.from)));
        }
        // Checked once, before anything is created.
        let params = run_params(&active, ask.params)?;
        let covered = if ask.all {
            std::collections::BTreeSet::new()
        } else {
            self.db.covered_times(dag_id, ask.from, to).await?
        };
        let missing: Vec<Timestamp> = times
            .iter()
            .copied()
            .filter(|t| !covered.contains(t))
            .collect();
        let count = |backfill_id| BackfillCount {
            fire_times: u32::try_from(times.len()).unwrap_or(u32::MAX),
            created: u32::try_from(missing.len()).unwrap_or(u32::MAX),
            skipped: u32::try_from(times.len() - missing.len()).unwrap_or(u32::MAX),
            backfill_id,
        };
        if ask.dry_run || missing.is_empty() {
            return Ok(count(None));
        }
        let backfill = NewBackfill {
            id,
            dag_id,
            range_from: ask.from,
            range_to: to,
            all_fire_times: ask.all,
            requested_by: user,
            created_at: now,
            fire_times: count(None).fire_times,
            created_runs: count(None).created,
        };
        self.store(
            &active,
            &fire_times,
            &backfill,
            (&times, &missing, &params),
            now,
        )
        .await?;
        Ok(count(Some(id.to_owned())))
    }

    /// Stores `backfill` and a queued run per `missing` fire time (of the range's `times`), in
    /// one transaction, then starts those that fit `max_active_runs`.
    async fn store(
        &self,
        active: &schedr_db::runs::ActiveDag,
        fire_times: &FireTimes,
        backfill: &NewBackfill<'_>,
        (times, missing, params): (&[Timestamp], &[Timestamp], &serde_json::Value),
        now: Timestamp,
    ) -> Result<(), SchedulerError> {
        let plan = Plan {
            dag: &active.dag,
            fire_times,
            first_seen: active.first_seen,
        };
        let sinces: std::collections::BTreeMap<Timestamp, Timestamp> = times
            .iter()
            .copied()
            .zip(plan.fire_time_sinces(times))
            .collect();
        let db = self.db.clone();
        // The backfill and its runs together: all of them or none.
        Box::pin(db.atomically(async {
            if !self.db.insert_backfill(backfill).await? {
                // Created by this command already (its round failed after the commit).
                return Ok::<(), SchedulerError>(());
            }
            for planned_for in missing {
                let window = RunTimes {
                    planned_for: *planned_for,
                    since: sinces
                        .get(planned_for)
                        .copied()
                        .unwrap_or_else(|| plan.fire_time_since(*planned_for)),
                };
                let by = backfill.requested_by;
                let mut new = new_run(active, RunKind::Backfill, window, now, by, params);
                new.state = RunState::Queued;
                new.backfill_id = Some(backfill.id);
                let run_id = uuid::Uuid::now_v7().to_string();
                self.db.insert_run_as(&run_id, &new).await?;
            }
            Ok(())
        }))
        .await?;
        self.wakes.wake(backfill.dag_id, WakeReason::RunQueued);
        let running = self.db.running_count(backfill.dag_id).await?;
        let queued = self.db.queued_runs(backfill.dag_id).await?;
        self.start_queued(&queued, &plan, running, now).await?;
        Ok(())
    }
}

/// The fire times in `[from, to]`, oldest first; `None` if there are more than `max`.
fn fire_times_in(
    fire_times: &FireTimes,
    from: Timestamp,
    to: Timestamp,
    max: u32,
) -> Option<Vec<Timestamp>> {
    fire_times.between(from, to, usize::try_from(max).unwrap_or(usize::MAX))
}
