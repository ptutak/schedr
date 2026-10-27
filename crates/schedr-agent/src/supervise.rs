//! Supervising a task process: waiting for it, its timeout, cancellation, and stopping it
//! with SIGTERM, then SIGKILL after the grace time (see PROJECT.md, Master–worker protocol:
//! cancellation).
//!
//! The logic works on the [`Process`] trait, so it is tested with a fake process and a
//! paused clock; [`crate::process::OsProcess`] is the real one.

use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::sync::mpsc;

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
    /// What it used and whether it dumped core, once [`Process::wait`] returned; nothing
    /// known by default.
    fn ended(&self) -> crate::usage::Ended {
        crate::usage::Ended::default()
    }
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

/// The reasons to stop an attempt, in the order they come: the first decides its status;
/// a drain's interrupt after it only hurries its SIGKILL (B9). The agent keeps a
/// [`StopSender`] for as long as the attempt runs.
#[derive(Debug)]
pub struct Stops {
    reasons: mpsc::UnboundedReceiver<StopReason>,
    sender: StopSender,
}

/// Sends an attempt its reasons to stop (cheap to clone).
pub type StopSender = mpsc::UnboundedSender<StopReason>;

impl Stops {
    /// A new channel of reasons, and its sender.
    #[must_use]
    pub fn new() -> (StopSender, Self) {
        let (sender, reasons) = mpsc::unbounded_channel();
        (sender.clone(), Self { reasons, sender })
    }

    /// Another sender to this channel (the memory watchdog's).
    #[must_use]
    pub fn sender(&self) -> StopSender {
        self.sender.clone()
    }

    /// The next reason; never ready while none comes (the channel keeps a sender of its own,
    /// so it never ends).
    pub async fn next_reason(&mut self) -> StopReason {
        match self.reasons.recv().await {
            Some(reason) => reason,
            None => std::future::pending().await,
        }
    }
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

/// Waits for `process`; on timeout, or at the first reason from `stop` (cancel, drain), sends
/// SIGTERM to its group, and SIGKILL if it hasn't ended after the grace time (the interrupt
/// grace for a drain's interrupt, also when the interrupt comes during another reason's
/// grace); over its memory budget ([`StopReason::OomKilled`]), SIGKILL at once.
///
/// # Errors
///
/// Returns an I/O error from waiting or signalling.
pub async fn supervise<P: Process>(
    process: &mut P,
    limits: Limits,
    mut stops: Stops,
) -> io::Result<Supervised> {
    let deadline = async {
        match limits.timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    // Reasons first: the memory watchdog sends its reason before its SIGKILL, so an attempt
    // it killed is `oom_killed`, not just a process that ended.
    let reason = tokio::select! {
        biased;
        reason = stops.next_reason() => reason,
        () = deadline => StopReason::TimedOut,
        termination = process.wait() => return Ok(ended(termination?, None, false)),
    };
    if reason == StopReason::OomKilled {
        // Over its memory budget: a spike can't wait for a grace period.
        process.signal(Signal::Kill)?;
        return Ok(ended(process.wait().await?, Some(reason), true));
    }
    process.signal(Signal::Term)?;
    let grace = if reason == StopReason::Interrupted {
        limits.interrupt_grace
    } else {
        limits.grace
    };
    let mut kill_at = tokio::time::Instant::now() + grace;
    // Until it ends or the grace is over, a later reason can only hurry the SIGKILL: a
    // drain's interrupt to at most `interrupt_grace` from now (B9), the memory watchdog to
    // now. The status stays the first reason's.
    loop {
        tokio::select! {
            biased;
            later = stops.next_reason() => match later {
                StopReason::Interrupted => {
                    let hurried = tokio::time::Instant::now() + limits.interrupt_grace;
                    kill_at = kill_at.min(hurried);
                }
                StopReason::OomKilled => break,
                StopReason::TimedOut | StopReason::Cancelled => {}
            },
            termination = process.wait() => {
                return Ok(ended(termination?, Some(reason), false));
            }
            () = tokio::time::sleep_until(kill_at) => break,
        }
    }
    process.signal(Signal::Kill)?;
    Ok(ended(process.wait().await?, Some(reason), true))
}

fn ended(termination: Termination, stopped: Option<StopReason>, killed: bool) -> Supervised {
    Supervised {
        termination,
        stopped,
        killed,
    }
}
