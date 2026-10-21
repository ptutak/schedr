//! The rules for run times and new runs (see PROJECT.md, Native API: run time semantics, and
//! Runs: manual runs, backfill and pausing).
//!
//! - A run on a **fire time** covers one schedule period: `since` is the previous fire time,
//!   or `initial_since` on the DAG's first fire time at or after `start`.
//! - A **manual run** not on a fire time covers what happened since the nearest earlier run
//!   of any kind.
//! - The **scheduler** creates runs for fire times in `(last scheduled run, now]`, from `start`
//!   (the moment the DAG was first stored, without one) up to `end`. With `catchup` off only
//!   the most recent such fire time gets a run; with it on every one does, in order. Either
//!   way at most `max_active_runs` runs run at a time; a fire time that finds no place is
//!   missed, and the same rules decide when a place frees up.
//! - **Queued runs** (manual, triggered, backfill) take the places left: manual and triggered
//!   first, then backfill, oldest first within each.

use jiff::{SignedDuration, Timestamp};
use schedr_db::runs::QueuedRun;
use schedr_model::{Dag, RunKind};

use crate::fire_times::FireTimes;

#[cfg(test)]
mod tests;

/// How far in the future `--for` may be, for clock skew between the CLI and the master.
pub const FOR_TOLERANCE: SignedDuration = SignedDuration::from_mins(1);

/// The most runs the scheduler creates for one DAG in one pass (a `catchup` backlog with no
/// `max_active_runs` limit continues in the next pass).
pub const MAX_RUNS_PER_PASS: usize = 100;

/// A run's time and window: it covers `(since, planned_for]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunTimes {
    /// The moment the run is for.
    pub planned_for: Timestamp,
    /// The start of its window.
    pub since: Timestamp,
}

/// A manual run that can't be created.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TriggerError {
    /// `--for` is in the future.
    #[error("`--for` must not be in the future ({planned_for} is after now, {now})")]
    InFuture {
        /// The requested time.
        planned_for: Timestamp,
        /// The master's clock.
        now: Timestamp,
    },
}

/// A DAG's schedule with what its rules need.
#[derive(Debug, Clone)]
pub struct Plan<'a> {
    /// The DAG.
    pub dag: &'a Dag,
    /// Its fire times.
    pub fire_times: &'a FireTimes,
    /// When the master first stored it (its start, without `start`).
    pub first_seen: Timestamp,
}

impl Plan<'_> {
    /// The moment scheduled runs start from: `start`, or when the DAG was first stored.
    #[must_use]
    pub fn effective_start(&self) -> Timestamp {
        self.dag.start.unwrap_or(self.first_seen)
    }

    /// The window of a run on a fire time: one schedule period.
    #[must_use]
    pub fn fire_time_since(&self, planned_for: Timestamp) -> Timestamp {
        if let Some(initial) = self.dag.initial_since
            && self.fire_times.next_at_or_after(self.effective_start()) == Some(planned_for)
        {
            return initial;
        }
        self.fire_times
            .previous_before(planned_for)
            .or(self.dag.initial_since)
            .unwrap_or(planned_for)
    }

    /// The times of a manual run: for `requested` (`--for`) or `now`. On a fire time it gets
    /// one schedule period; otherwise it covers what happened since `nearest_earlier` (the
    /// `planned_for` of the nearest earlier run of any kind, before [`manual_planned_for`]),
    /// else since `initial_since`, else since the previous fire time; with none of these its
    /// window is empty.
    ///
    /// # Errors
    ///
    /// [`TriggerError::InFuture`] if `requested` is after now (beyond [`FOR_TOLERANCE`]).
    pub fn manual_times(
        &self,
        requested: Option<Timestamp>,
        now: Timestamp,
        nearest_earlier: Option<Timestamp>,
    ) -> Result<RunTimes, TriggerError> {
        let planned_for = manual_planned_for(requested, now);
        if now
            .checked_add(FOR_TOLERANCE)
            .is_ok_and(|limit| planned_for > limit)
        {
            return Err(TriggerError::InFuture { planned_for, now });
        }
        let since = if self.fire_times.is_fire_time(planned_for) {
            self.fire_time_since(planned_for)
        } else {
            nearest_earlier
                .or(self.dag.initial_since)
                .or_else(|| self.fire_times.previous_before(planned_for))
                .unwrap_or(planned_for)
        };
        Ok(RunTimes { planned_for, since })
    }

    /// The scheduled runs to create at `now`, given the latest `planned_for` of the DAG's
    /// scheduled runs and how many of its runs are running. A paused DAG creates none (its
    /// fire times are missed); the caller checks that.
    #[must_use]
    pub fn scheduled_runs(
        &self,
        last_scheduled: Option<Timestamp>,
        running: u32,
        now: Timestamp,
    ) -> Vec<RunTimes> {
        let free = self.free_places(running).min(MAX_RUNS_PER_PASS);
        if free == 0 {
            return Vec::new();
        }
        let upper = self.dag.end.map_or(now, |end| end.min(now));
        let start = self.effective_start();
        let after = |t: Timestamp| t >= start && last_scheduled.is_none_or(|last| t > last);
        let mut due = Vec::new();
        if self.dag.catchup {
            let from = match last_scheduled {
                Some(last) if last >= start => self.fire_times.next_after(last),
                _ => self.fire_times.next_at_or_after(start),
            };
            let mut next = from;
            while let Some(t) = next.filter(|t| *t <= upper && due.len() < free) {
                due.push(t);
                next = self.fire_times.next_after(t);
            }
        } else if let Some(latest) = self
            .fire_times
            .last_at_or_before(upper)
            .filter(|t| after(*t))
        {
            due.push(latest);
        }
        due.into_iter()
            .map(|planned_for| RunTimes {
                planned_for,
                since: self.fire_time_since(planned_for),
            })
            .collect()
    }

    /// How many more runs may run (`usize::MAX` without a limit).
    #[must_use]
    pub fn free_places(&self, running: u32) -> usize {
        self.dag.max_active_runs.map_or(usize::MAX, |limit| {
            usize::try_from(limit.saturating_sub(running)).unwrap_or(usize::MAX)
        })
    }
}

/// The moment a manual run is for: `requested` (`--for`), or `now`.
#[must_use]
pub fn manual_planned_for(requested: Option<Timestamp>, now: Timestamp) -> Timestamp {
    requested.unwrap_or(now)
}

/// The queued runs to start with `free` places: manual and triggered runs first, then
/// backfill, oldest first within each.
#[must_use]
pub fn runs_to_start(queued: &[QueuedRun], free: usize) -> Vec<&QueuedRun> {
    let mut ordered: Vec<&QueuedRun> = queued.iter().collect();
    ordered.sort_by_key(|run| (priority(run.kind), run.triggered_at, run.id.clone()));
    ordered.truncate(free);
    ordered
}

const fn priority(kind: RunKind) -> u8 {
    match kind {
        RunKind::Scheduled => 0,
        RunKind::Manual | RunKind::Triggered => 1,
        RunKind::Backfill => 2,
    }
}
