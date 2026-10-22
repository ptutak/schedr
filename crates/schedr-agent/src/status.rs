//! How an attempt ended: from the process's termination and why the agent stopped it.

use schedr_model::AttemptStatus;

#[cfg(test)]
mod tests;

/// How a process ended, as the operating system reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    /// It exited with this code.
    Exited(i32),
    /// It was killed by this signal.
    Signaled(i32),
}

/// Why the agent stopped a process before it ended by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// It exceeded its timeout.
    TimedOut,
    /// A user cancelled it.
    Cancelled,
    /// A draining worker's grace period ended.
    Interrupted,
    /// It exceeded its memory budget (the watchdog, see [`crate::memory`]).
    OomKilled,
}

impl Termination {
    /// The exit code reported in a `ProcessResult` (`128 + signal` for a signal).
    #[must_use]
    pub const fn exit_code(self) -> i32 {
        match self {
            Self::Exited(code) => code,
            Self::Signaled(signal) => crate::output::signal_exit_code(signal),
        }
    }
}

/// The attempt's status.
///
/// A process the agent stopped gets the status of the reason, however it then ended (a task
/// that exits 0 on SIGTERM after a timeout still timed out). Otherwise it succeeded if it
/// exited with one of `success_codes` (`[0]`, or the codes of a declared outcome enum), and
/// failed for any other exit code or a signal.
#[must_use]
pub fn attempt_status(
    termination: Termination,
    stopped: Option<StopReason>,
    success_codes: &[i32],
) -> AttemptStatus {
    match (stopped, termination) {
        (Some(StopReason::TimedOut), _) => AttemptStatus::TimedOut,
        (Some(StopReason::Cancelled), _) => AttemptStatus::Cancelled,
        (Some(StopReason::Interrupted), _) => AttemptStatus::Interrupted,
        (Some(StopReason::OomKilled), _) => AttemptStatus::OomKilled,
        (None, Termination::Exited(code)) if success_codes.contains(&code) => {
            AttemptStatus::Success
        }
        (None, _) => AttemptStatus::Failed,
    }
}
