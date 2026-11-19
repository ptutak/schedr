//! The cluster's slow-equals (decision 509): how long a worker's waiting queue's head may wait
//! before the worker gives back a task. Each heartbeat reports its queue's head age; the master
//! answers with the median of the latest age of every worker with a queue (an empty one counting
//! 0; a worker without a queue, which says 0 busy or not, not counted) times a factor, never
//! below a floor. When every worker is as busy, nothing goes back; at a burst's
//! end idle workers pull the median down, and a queue still holding work hands it over.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// The factor over the median (`SCHEDR_HIDDEN__MASTER__SLOW_EQUALS_FACTOR`).
pub const FACTOR: u32 = 3;

/// The floor (`SCHEDR_HIDDEN__MASTER__SLOW_EQUALS_FLOOR`): below about four give-backs' cost
/// (a round, an assignment and a start, ~250 ms each) a give-back can't pay.
pub const FLOOR: Duration = Duration::from_secs(1);

/// How the slow-equals is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlowEqualsSettings {
    /// The factor over the median of the workers' head ages.
    pub factor: u32,
    /// The least it is.
    pub floor: Duration,
    /// An age older than this is left out: its worker is gone or silent (three heartbeats).
    pub stale_after: Duration,
}

impl Default for SlowEqualsSettings {
    fn default() -> Self {
        Self {
            factor: FACTOR,
            floor: FLOOR,
            stale_after: Duration::from_secs(30),
        }
    }
}

/// Each worker's latest queue head age, as its heartbeats said, shared by every stream's
/// reader (not behind the master's lock).
#[derive(Debug, Default)]
pub struct SlowEquals {
    settings: SlowEqualsSettings,
    ages: Mutex<BTreeMap<String, (Duration, Instant)>>,
}

impl SlowEquals {
    /// None heard yet.
    #[must_use]
    pub fn new(settings: SlowEqualsSettings) -> Self {
        Self {
            settings,
            ages: Mutex::default(),
        }
    }

    /// `worker_id`'s heartbeat said its queue's head waited `age` (zero: empty; `None`: it has
    /// no queue, so nothing is noted): the slow-equals now returned for its `HeartbeatAck`.
    pub fn heard(&self, worker_id: &str, age: Option<Duration>, now: Instant) -> Duration {
        let mut ages = self
            .ages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // One without a queue now (restarted so) counts no older age (Codex's review of M5.2e).
        match age {
            Some(age) => ages.insert(worker_id.to_owned(), (age, now)),
            None => ages.remove(worker_id),
        };
        let stale = self.settings.stale_after;
        ages.retain(|_, (_, at)| now.saturating_duration_since(*at) <= stale);
        let mut fresh: Vec<Duration> = ages.values().map(|(age, _)| *age).collect();
        fresh.sort_unstable();
        median(&fresh)
            .saturating_mul(self.settings.factor)
            .max(self.settings.floor)
    }
}

/// The median of sorted `ages` (the mean of the middle two for an even count); zero for none.
fn median(ages: &[Duration]) -> Duration {
    match ages.len() {
        0 => Duration::ZERO,
        n if n % 2 == 1 => ages[n / 2],
        n => (ages[n / 2 - 1] + ages[n / 2]) / 2,
    }
}

#[cfg(test)]
mod tests;
