//! When a loop last turned, for liveness probes (see PROJECT.md, Observability): the master's
//! scheduling loop and the worker agent's event loop beat, and the ops port reads how long ago.
//!
//! The clock is monotonic (from when the process started), so a wall clock that steps never
//! makes a loop look stuck or alive.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// Never beaten.
const NEVER: u64 = u64::MAX;

/// When a loop last turned (cheap to clone; clones share it).
#[derive(Debug, Clone)]
pub struct Beat {
    /// Milliseconds since [`origin`] at the last beat, or [`NEVER`].
    last: Arc<AtomicU64>,
}

impl Default for Beat {
    fn default() -> Self {
        // The origin is set before any beat or question about one.
        origin();
        Self {
            last: Arc::new(AtomicU64::new(NEVER)),
        }
    }
}

/// The process's monotonic origin.
fn origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

fn millis(at: Instant) -> u64 {
    u64::try_from(at.saturating_duration_since(origin()).as_millis()).unwrap_or(NEVER - 1)
}

impl Beat {
    /// The loop turned now.
    pub fn beat(&self) {
        self.beat_at(Instant::now());
    }

    /// The loop turned at `at` (tests use their own times).
    pub fn beat_at(&self, at: Instant) {
        self.last.store(millis(at), Ordering::Relaxed);
    }

    /// How long before `now` the loop last turned; `None` if it never did.
    #[must_use]
    pub fn since(&self, now: Instant) -> Option<Duration> {
        let last = self.last.load(Ordering::Relaxed);
        (last != NEVER).then(|| Duration::from_millis(millis(now).saturating_sub(last)))
    }
}
