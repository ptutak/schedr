//! Supervising a task process: waiting for it, its timeout, cancellation, and stopping it
//! with SIGTERM, then SIGKILL after the grace time (see PROJECT.md, Master–worker protocol:
//! cancellation).
//!
//! The logic works on the [`Process`] trait, so it is tested with a fake process and a
//! paused clock; [`crate::process::OsProcess`] is the real one.

use std::future::Future;
use std::io;
use std::time::Duration;

use crate::status::{StopReason, Termination};

#[cfg(test)]
mod tests;

/// A signal the agent sends to a task's whole process group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Ask it to stop (SIGTERM).
    Term,
    /// Stop it (SIGKILL).
    Kill,
}

/// A running task process.
pub trait Process {
    /// Waits until the process ends.
    fn wait(&mut self) -> impl Future<Output = io::Result<Termination>> + Send;

    /// Sends a signal to the process's whole group; a group that is already gone is fine.
    ///
    /// # Errors
    ///
    /// Returns the operating system's error for anything but a group that is gone.
    fn signal(&mut self, signal: Signal) -> io::Result<()>;
}

/// The limits of one attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The attempt's timeout; `None` for none.
    pub timeout: Option<Duration>,
    /// The time between SIGTERM and SIGKILL (the master's cancel grace time).
    pub grace: Duration,
    /// The same for a draining worker's interrupt, which must end before the pod is killed
    /// (`[worker] drain_terminate_period`).
    pub interrupt_grace: Duration,
}

/// How a supervised process ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Supervised {
    /// How it ended.
    pub termination: Termination,
    /// Why the agent stopped it, if it did.
    pub stopped: Option<StopReason>,
    /// Whether it needed SIGKILL (it didn't end within the grace time after SIGTERM).
    pub killed: bool,
}

/// Waits for `process`; on timeout, or when `stop` completes (cancel, drain), sends SIGTERM to
/// its group, and SIGKILL if it hasn't ended after the grace time (the interrupt grace for a
/// drain's interrupt); over its memory budget
/// (`stop` says [`StopReason::OomKilled`]), SIGKILL at once.
///
/// # Errors
///
/// Returns an I/O error from waiting or signalling.
pub async fn supervise<P: Process>(
    process: &mut P,
    limits: Limits,
    stop: impl Future<Output = StopReason>,
) -> io::Result<Supervised> {
    let deadline = async {
        match limits.timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    let reason = tokio::select! {
        termination = process.wait() => {
            return Ok(Supervised { termination: termination?, stopped: None, killed: false });
        }
        () = deadline => StopReason::TimedOut,
        reason = stop => reason,
    };
    if reason == StopReason::OomKilled {
        // Over its memory budget: a spike can't wait for a grace period.
        process.signal(Signal::Kill)?;
        return Ok(Supervised {
            termination: process.wait().await?,
            stopped: Some(reason),
            killed: true,
        });
    }
    process.signal(Signal::Term)?;
    let grace = if reason == StopReason::Interrupted {
        limits.interrupt_grace
    } else {
        limits.grace
    };
    let ended = tokio::time::timeout(grace, process.wait()).await;
    let (termination, killed) = if let Ok(termination) = ended {
        (termination?, false)
    } else {
        process.signal(Signal::Kill)?;
        (process.wait().await?, true)
    };
    Ok(Supervised {
        termination,
        stopped: Some(reason),
        killed,
    })
}
