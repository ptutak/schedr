//! Draining the worker on shutdown (SIGTERM): what happens when, decided apart from the agent
//! so it is tested with plain instants (see PROJECT.md, Worker lifecycle: drain).
//!
//! - The running attempts get until the **termination grace period** minus the drain's
//!   terminate and report periods ([`DrainPeriods`]); whatever still runs then gets SIGTERM,
//!   SIGKILL after the terminate period, and ends `interrupted` (run again without using a
//!   retry). The report period is left for the results to reach the master before the pod is
//!   killed (B8). A second shutdown request moves the deadline to now.
//! - Once nothing runs, the agent waits (at most [`ACK_WAIT`], never past the end of the grace
//!   period) for the master to acknowledge the last results, then deregisters and ends;
//!   without a master it ends at once (the journal keeps the results for the next start).

use std::time::{Duration, Instant};

#[cfg(test)]
mod tests;

/// How a drain's time is split (`[worker] termination_grace_period`, `drain_terminate_period`,
/// `drain_report_period`; the configuration checks that the last two fit in the first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainPeriods {
    /// The whole drain: the pod's `terminationGracePeriodSeconds`.
    pub grace: Duration,
    /// From the SIGTERM of a task still running when the time is up to its SIGKILL.
    pub terminate_period: Duration,
    /// From that SIGKILL to the end of the grace period: reporting and deregistering.
    pub report_period: Duration,
    /// How long the last results may wait for the master's acknowledgement (`drain_ack_wait`).
    pub ack_wait: Duration,
}

impl DrainPeriods {
    /// Whether a cleanup try of `room` fits in the grace period, after what a drain keeps for
    /// itself. One that doesn't still runs on: the agent waits for it, but whatever ends the
    /// worker at the end of the grace period (a pod's kill, a service manager) would lose it.
    #[must_use]
    pub fn cleanup_fits(&self, room: Duration) -> bool {
        room.saturating_add(self.terminate_period)
            .saturating_add(self.report_period)
            <= self.grace
    }

    /// What to say at a worker's start when `[worker] cleanup_timeout`, what a task's cleanup
    /// gets unless it says otherwise, doesn't fit: said once, not for each task using it.
    #[must_use]
    pub fn default_cleanup_warning(&self, default_timeout: Duration) -> Option<String> {
        (!self.cleanup_fits(default_timeout)).then(|| {
            format!(
                "`[worker] cleanup_timeout` ({default_timeout:?}) doesn't fit in \
                 `termination_grace_period` ({:?}): whatever ends the worker at the end of a \
                 drain (a pod's kill, a service manager) would lose a cleanup still running",
                self.grace
            )
        })
    }
}

impl Default for DrainPeriods {
    /// 30 s (Kubernetes' default), of which 3 s to SIGKILL and 2 s to report.
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(30),
            terminate_period: Duration::from_secs(3),
            report_period: Duration::from_secs(2),
            ack_wait: ACK_WAIT,
        }
    }
}

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
    end: Instant,
    interrupted: bool,
    idle_since: Option<Instant>,
    ack_wait: Duration,
}

impl Drain {
    /// A drain starting at `now`, its time split as `periods` says.
    #[must_use]
    pub fn new(now: Instant, periods: DrainPeriods) -> Self {
        let stop_and_report = periods.terminate_period + periods.report_period;
        Self {
            deadline: now + periods.grace.saturating_sub(stop_and_report),
            end: now + periods.grace,
            interrupted: false,
            idle_since: None,
            ack_wait: periods.ack_wait,
        }
    }

    /// When the grace period ends (the pod is killed then).
    #[must_use]
    pub const fn end(&self) -> Instant {
        self.end
    }

    /// Whether an attempt whose cleanup needs `room` must be stopped now, leaving that room
    /// for one try of it (see PROJECT.md, Per-task cleanup).
    #[must_use]
    pub fn stop_due(&self, now: Instant, room: Duration) -> bool {
        now.checked_add(room)
            .is_none_or(|latest| latest >= self.deadline)
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
        if now.duration_since(idle) >= self.ack_wait || now >= self.end {
            DrainStep::Finish
        } else {
            DrainStep::Wait
        }
    }
}
