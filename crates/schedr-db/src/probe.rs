//! The round's probe (decision 485): one statement a tick answering, for each read a master's
//! round makes, whether that read would find anything. Inside [`Database::in_round`], a read
//! whose check said no returns nothing without going to the database; reads anywhere else (a
//! worker's turn, the API) are never skipped.
//!
//! Each check is its read's own `FROM ... WHERE` text, shared through a macro defined next to
//! the read, so the two can't drift apart; where a read takes a parameter the probe doesn't
//! have, its check drops that condition and says yes more often, never less. A write
//! transaction ending anywhere in this process (counted by `Database::wrote`) makes the next
//! read whose check said no take the probe again: work a round stores for its own later parts (a
//! run started, then its instances, then their assignment) is found in the same round.
//!
//! A read bounded by a time (tasks ready, retries due, scheduled DAGs due) passes the time it
//! asks for: a probe covering an earlier time is taken again at that one, so work a part stored
//! at its own, later clock is found too. The probe looks ahead by what the round gives it (the
//! tick), so a round's parts, a few milliseconds apart, share one probe.

use std::sync::Arc;

use jiff::Timestamp;
use sqlx::Row as _;

use crate::database::{Database, DbError};
use crate::query::{db_time, on_pool, sql_text};

#[cfg(test)]
mod tests;

/// A read a round may skip, by what its check found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Check {
    /// [`Database::pending_commands`].
    Commands,
    /// [`Database::dags_with_queued_runs`].
    QueuedRuns,
    /// [`Database::scheduled_due`].
    Scheduled,
    /// [`Database::queued_event_runs`].
    QueuedEventRuns,
    /// [`Database::newly_running`].
    NewlyRunning,
    /// [`Database::due_retries`].
    DueRetries,
    /// [`Database::replaced_attempts`].
    ReplacedAttempts,
    /// [`Database::unschedulable_instances`].
    Unschedulable,
    /// [`Database::removed_dags_runs`].
    RemovedDagsRuns,
    /// [`Database::chained_attempts`].
    ChainedAttempts,
    /// [`Database::ready_instances`].
    Ready,
    /// [`Database::waiting_cleanups`] (any time: the read may be asked for later ones).
    WaitingCleanups,
    /// [`Database::unstarted_attempts`] (any cutoff, any worker).
    Unstarted,
    /// [`Database::unstarted_cleanups`] (any cutoff, any worker).
    UnstartedCleanups,
    /// The master's trigger pass: a trigger of a kind the pass looks after switched on, or one
    /// running on a worker (to be stopped when it no longer should run).
    Triggers,
    /// A trigger the trigger loop looks after whose stored next moment has come or isn't known
    /// (decision 486): the tick wakes the loop.
    TriggersDue,
}

impl Check {
    /// Every check, in the probe's column order.
    pub const ALL: [Self; 16] = [
        Self::Commands,
        Self::QueuedRuns,
        Self::Scheduled,
        Self::QueuedEventRuns,
        Self::NewlyRunning,
        Self::DueRetries,
        Self::ReplacedAttempts,
        Self::Unschedulable,
        Self::RemovedDagsRuns,
        Self::ChainedAttempts,
        Self::Ready,
        Self::WaitingCleanups,
        Self::Unstarted,
        Self::UnstartedCleanups,
        Self::Triggers,
        Self::TriggersDue,
    ];

    /// Its `FROM ... WHERE` text; `$1` is the round's time.
    const fn from(self) -> &'static str {
        match self {
            Self::Commands => crate::commands::pending_from!(),
            Self::QueuedRuns => crate::runs::queued_from!(),
            Self::Scheduled => crate::dags::scheduled_due_from!(),
            Self::QueuedEventRuns => crate::triggers::queued_event_from!(),
            Self::NewlyRunning => crate::instances::newly_running_from!(),
            Self::DueRetries => crate::instances::due_retries_from!(),
            Self::ReplacedAttempts => crate::attempts::replaced_from!(),
            Self::Unschedulable => crate::attempts::unschedulable_from!(),
            Self::RemovedDagsRuns => crate::dags::removed_dags_runs_from!(),
            Self::ChainedAttempts => crate::attempts::chained_from!(),
            Self::Ready => crate::attempts::ready_from!(),
            Self::WaitingCleanups => crate::attempts::waiting_cleanups_any_time_from!(),
            Self::Unstarted => crate::attempts::unstarted_any_time_from!(),
            Self::UnstartedCleanups => crate::attempts::unstarted_cleanups_any_time_from!(),
            Self::Triggers => crate::triggers::trigger_pass_from!(),
            Self::TriggersDue => crate::triggers::triggers_due_from!(),
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

/// What a probe found: for each check, whether its read would find anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe {
    found: [bool; Check::ALL.len()],
}

impl Probe {
    /// Whether `check`'s read would find anything.
    #[must_use]
    pub const fn found(&self, check: Check) -> bool {
        self.found[check.index()]
    }
}

/// The probe statement: one `EXISTS` a check.
fn statement() -> &'static str {
    static STATEMENT: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let checks: Vec<String> = Check::ALL
            .iter()
            .map(|check| format!("EXISTS (SELECT 1 {})", check.from()))
            .collect();
        format!("SELECT {}", checks.join(", "))
    });
    &STATEMENT
}

tokio::task_local! {
    static ROUND: Round;
}

/// A round under way on one database: its time, how far its probe looks ahead, and its last
/// probe.
#[derive(Clone)]
struct Round {
    database: Arc<()>,
    now: Timestamp,
    ahead: jiff::SignedDuration,
    seen: Arc<std::sync::Mutex<Option<Seen>>>,
}

#[derive(Clone, Copy)]
struct Seen {
    /// [`Database::write_ends`] when it was taken.
    writes: u64,
    /// The time it was taken for: a time-bounded check covers what is due by then.
    until: Timestamp,
    probe: Probe,
}

impl Database {
    /// What the probe finds at `now` (decision 485).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn probe(&self, now: Timestamp) -> Result<Probe, DbError> {
        let found: Vec<bool> = on_pool!(self, |pool, backend| {
            let row = sqlx::query(sql_text(backend, statement()))
                .bind(db_time(now))
                .fetch_one(pool)
                .await?;
            (0..Check::ALL.len())
                .map(|i| row.try_get::<bool, _>(i))
                .collect::<Result<Vec<bool>, sqlx::Error>>()?
        });
        let mut probe = Probe {
            found: [false; Check::ALL.len()],
        };
        probe.found.copy_from_slice(&found);
        Ok(probe)
    }

    /// Whether `check`'s read would find anything at `now`: its one `EXISTS` alone, for a loop
    /// that needs only it (the trigger loop, decision 486).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the statement fails.
    pub async fn exists(&self, check: Check, now: Timestamp) -> Result<bool, DbError> {
        let statement = format!("SELECT EXISTS (SELECT 1 {})", check.from());
        let found: bool = on_pool!(self, |pool, backend| {
            sqlx::query_scalar(sql_text(backend, &statement))
                .bind(db_time(now))
                .fetch_one(pool)
                .await?
        });
        Ok(found)
    }

    /// Runs `work` as a master's round at `now`: its reads that the probe says find nothing are
    /// skipped (decision 485). The probe is taken at the first such read, looking `ahead` (the
    /// tick: a time-bounded check counts what is due by then), and again after any write
    /// transaction ended since, or for a time-bounded read past what it covered.
    pub async fn in_round<F: std::future::Future>(
        &self,
        now: Timestamp,
        ahead: std::time::Duration,
        work: F,
    ) -> F::Output {
        let round = Round {
            database: self.identity().clone(),
            now,
            ahead: jiff::SignedDuration::try_from(ahead).unwrap_or(jiff::SignedDuration::ZERO),
            seen: Arc::default(),
        };
        ROUND.scope(round, work).await
    }

    /// Whether this read, `check`'s, is skipped: inside a round on this database whose probe
    /// found nothing for it (taking the probe first if none is current).
    ///
    /// # Errors
    ///
    /// [`DbError`] if the probe fails.
    pub async fn skipped(&self, check: Check) -> Result<bool, DbError> {
        self.skipped_by(check, None).await
    }

    /// As [`Self::skipped`], for a read bounded by `now` (what is due by then): a probe taken
    /// for an earlier time is taken again.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the probe fails.
    pub async fn skipped_at(&self, check: Check, now: Timestamp) -> Result<bool, DbError> {
        self.skipped_by(check, Some(now)).await
    }

    async fn skipped_by(&self, check: Check, at: Option<Timestamp>) -> Result<bool, DbError> {
        let Some(round) = ROUND
            .try_with(Clone::clone)
            .ok()
            .filter(|round| Arc::ptr_eq(&round.database, self.identity()))
        else {
            return Ok(false);
        };
        let writes = self.write_ends();
        let current = *round
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let probe = match current {
            Some(seen) if seen.writes == writes && at.is_none_or(|at| at <= seen.until) => {
                seen.probe
            }
            // Found by the last probe, before a write or for an earlier time: the read goes
            // ahead without a new probe (not skipping is always safe), so a busy round probes
            // only for checks that said no.
            Some(seen) if seen.probe.found(check) => return Ok(false),
            _ => {
                let from = at.map_or(round.now, |at| at.max(round.now));
                let until = from.checked_add(round.ahead).unwrap_or(from);
                let probe = self.probe(until).await?;
                *round
                    .seen
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Seen {
                    writes,
                    until,
                    probe,
                });
                probe
            }
        };
        Ok(!probe.found(check))
    }
}
