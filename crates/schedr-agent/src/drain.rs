//! Draining the worker on shutdown (SIGTERM): what happens when, decided apart from the agent
//! so it is tested with plain instants (see PROJECT.md, Worker lifecycle: drain).
//!
//! - The running attempts get until the **termination grace period** minus a margin; whatever
//!   still runs then is stopped and ends `interrupted` (run again without using a retry). A
//!   second shutdown request moves the deadline to now.
//! - Once nothing runs, the agent waits (at most [`ACK_WAIT`]) for the master to acknowledge
//!   the last results, then deregisters and ends; without a master it ends at once (the
//!   journal keeps the results for the next start).

use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// How long before the end of the grace period the remaining attempts are stopped, so their
/// results reach the master before the process is killed.
pub const DRAIN_MARGIN: Duration = Duration::from_secs(5);

/// How long a drained agent waits for the master to acknowledge the last results.
pub const ACK_WAIT: Duration = Duration::from_secs(5);

/// What a draining agent does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainStep {
    /// Keep waiting for the running attempts (or the acknowledgements).
    Wait,
    /// Stop the attempts still running: the deadline passed.
    Interrupt,
    /// Done: deregister and end.
    Finish,
}

/// A drain in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Drain {
    deadline: Instant,
    interrupted: bool,
    idle_since: Option<Instant>,
}

impl Drain {
    /// A drain starting at `now` with the grace period `grace`.
    #[must_use]
    pub fn new(now: Instant, grace: Duration) -> Self {
        Self {
            deadline: now + grace.saturating_sub(DRAIN_MARGIN),
            interrupted: false,
            idle_since: None,
        }
    }

    /// A second shutdown request: the remaining attempts are stopped at once.
    pub fn force(&mut self, now: Instant) {
        self.deadline = self.deadline.min(now);
    }

    /// The next step at `now`, with `running` attempts, `unacked` results and whether the
    /// master is connected.
    pub fn step(
        &mut self,
        now: Instant,
        running: usize,
        unacked: usize,
        connected: bool,
    ) -> DrainStep {
        if running > 0 {
            self.idle_since = None;
            if !self.interrupted && now >= self.deadline {
                self.interrupted = true;
                return DrainStep::Interrupt;
            }
            return DrainStep::Wait;
        }
        if unacked == 0 || !connected {
            return DrainStep::Finish;
        }
        let idle = *self.idle_since.get_or_insert(now);
        if now.duration_since(idle) >= ACK_WAIT {
            DrainStep::Finish
        } else {
            DrainStep::Wait
        }
    }
}
