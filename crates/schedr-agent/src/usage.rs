//! What a spawned process used, read from its zombie before it is reaped (decision 18A, see
//! PROJECT.md, How Python tasks are started: the fork server).
//!
//! The workspace forbids `unsafe`, and no safe call returns a child's `rusage`; but between
//! its exit and its reaping a process is a zombie whose `/proc/<pid>/stat` still holds its
//! CPU time (with the children it reaped) and its faults. Its memory is gone, so the peak RSS
//! isn't known, and its `status` has its leader thread's context switches only, so those
//! aren't reported either. The agent waits for the exit without reaping (`waitid` with
//! `WNOWAIT`), reads them, then reaps. A forked process's usage comes from its shim instead
//! (the parent's `rusage`, all of it).

use std::io;
use std::path::Path;

use rustix::process::{Pid, WaitId, WaitIdOptions, waitid};
use schedr_model::runner::ProcessUsage;

#[cfg(test)]
mod tests;

/// How a process ended, besides its exit status: what it used and whether it dumped core.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Ended {
    /// What it used; `None` if it couldn't be read.
    pub usage: Option<ProcessUsage>,
    /// Whether it dumped core.
    pub core_dumped: bool,
}

/// The usage of zombie `pid` from `proc_root` (`/proc`): CPU time with its reaped children's,
/// in microseconds of `ticks` per second, and major faults (its own and its reaped
/// children's); no peak RSS and no context switches. `None` if it can't be read or isn't a zombie (a process
/// still running would give a partial answer).
#[must_use]
pub fn zombie_usage(proc_root: &Path, pid: i32, ticks: u64) -> Option<ProcessUsage> {
    let dir = proc_root.join(pid.to_string());
    let stat = std::fs::read_to_string(dir.join("stat")).ok()?;
    let fields: Vec<&str> = stat[stat.rfind(')')? + 1..].split_whitespace().collect();
    // After the command: field 3 (state) is [0], so field n is [n - 3].
    if *fields.first()? != "Z" {
        return None;
    }
    let field = |n: usize| fields.get(n - 3)?.parse::<u64>().ok();
    let micros = |t: u64| t.saturating_mul(1_000_000) / ticks.max(1);
    Some(ProcessUsage {
        user_cpu_micros: micros(field(14)? + field(16)?),
        system_cpu_micros: micros(field(15)? + field(17)?),
        peak_rss_bytes: None,
        major_faults: field(12)? + field(13)?,
        // Not the context switches: a zombie's `status` has its leader thread's only, not
        // its other threads' or its reaped children's (`rusage` has them; no safe call
        // returns a child's).
        voluntary_context_switches: None,
        involuntary_context_switches: None,
    })
}

/// Waits until child `pid` has exited, without reaping it (`waitid` with `WNOWAIT`, on the
/// blocking pool: one parked thread per running process), so its zombie can be read first.
///
/// # Errors
///
/// The operating system's error (e.g. `ECHILD` for a process that isn't an unreaped child).
pub async fn exited(pid: Pid) -> io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let options = WaitIdOptions::EXITED | WaitIdOptions::NOWAIT;
        loop {
            match waitid(WaitId::Pid(pid), options) {
                Ok(_) => return Ok(()),
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(io::Error::from(error)),
            }
        }
    })
    .await
    .map_err(io::Error::other)?
}

/// The clock ticks per second of `/proc`'s times.
#[must_use]
pub fn clock_ticks() -> u64 {
    rustix::param::clock_ticks_per_second()
}
