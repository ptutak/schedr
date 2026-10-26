//! The memory watchdog (see PROJECT.md, Worker memory protection and crash recovery): every
//! task shares the pod's memory, and the kernel's out-of-memory killer would take every task
//! and the agent with it, so the agent measures each task and kills only the one over its
//! budget.
//!
//! - **The limit** is the smallest cgroup v2 `memory.max` from the agent's own cgroup up to
//!   the root (`/proc/self/cgroup`), so it is found in a pod and on a host alike. None found
//!   (`max`, cgroup v1): nothing is killed, peak memory is still measured. No `/proc`:
//!   nothing is measured and nothing is killed.
//! - **The budget** per task is `[worker] memory_budget`, else (limit − 10 %) ÷ slots.
//! - **A task's memory** is the PSS (`/proc/<pid>/smaps_rollup`) of every process in its
//!   process group (what SIGTERM and SIGKILL reach), RSS where PSS can't be read: PSS splits
//!   shared pages between the processes sharing them, so a tree isn't counted many times.
//! - **Sampling** every `[worker] memory_check_interval` (1 s); over the budget the group is
//!   killed at once (SIGKILL: a spike can't wait for a grace period), and the attempt ends
//!   `oom_killed`. The highest sample is the attempt's peak memory.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use rustix::process::{Pid, Signal, kill_process_group};
use tokio::task::JoinHandle;

use crate::status::StopReason;
use crate::supervise::StopSender;

#[cfg(test)]
mod tests;

/// The smallest `[worker] memory_budget` accepted: below it no Python process starts.
pub const MIN_BUDGET: u64 = 16 << 20;

/// The part of the pod's memory limit kept out of the task budgets: for schedr's own process
/// (the agent, and in standalone the master and the API too).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reserve {
    /// A percentage of the limit.
    Percent(u8),
    /// A fixed number of bytes.
    Bytes(u64),
}

/// The reserve of `schedr worker` when `[worker] memory_reserve` isn't set.
pub const WORKER_RESERVE: Reserve = Reserve::Percent(10);

/// The reserve of `schedr standalone` (its process also runs the master and the API) when
/// `[worker] memory_reserve` isn't set.
pub const STANDALONE_RESERVE: Reserve = Reserve::Percent(25);

/// How the watchdog watches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryWatch {
    /// Each task's budget; `None`: measured, never killed.
    pub budget: Option<u64>,
    /// How often a task's memory is sampled.
    pub interval: Duration,
    /// Where `/proc` is (a fake directory in tests).
    pub proc_root: PathBuf,
}

impl Default for MemoryWatch {
    /// No budget, sampled every second from `/proc`.
    fn default() -> Self {
        Self {
            budget: None,
            interval: Duration::from_secs(1),
            proc_root: PathBuf::from("/proc"),
        }
    }
}

/// What the watchdog saw of one attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemorySeen {
    /// The highest sample, in bytes (0: never measured).
    pub peak: u64,
    /// The sample over the budget that got the task killed.
    pub over: Option<u64>,
}

/// The watchdog's record of one attempt, shared with whoever reports it.
pub type SharedSeen = Arc<Mutex<MemorySeen>>;

/// The smallest cgroup v2 `memory.max` on the way from the agent's cgroup (in
/// `<proc_root>/self/cgroup`) up to `cgroup_root`; `None` when there is none (unlimited,
/// cgroup v1).
///
/// # Errors
///
/// An I/O error if `<proc_root>/self/cgroup` can't be read.
pub fn cgroup_limit(proc_root: &Path, cgroup_root: &Path) -> io::Result<Option<u64>> {
    let cgroups = std::fs::read_to_string(proc_root.join("self/cgroup"))?;
    // cgroup v2 is the line `0::<path>`.
    let Some(path) = cgroups.lines().find_map(|line| line.strip_prefix("0::")) else {
        return Ok(None);
    };
    let mut dir = cgroup_root.join(path.trim_start_matches('/'));
    let mut limit: Option<u64> = None;
    loop {
        if let Ok(text) = std::fs::read_to_string(dir.join("memory.max"))
            && let Ok(bytes) = text.trim().parse::<u64>()
        {
            limit = Some(limit.map_or(bytes, |l| l.min(bytes)));
        }
        if dir == cgroup_root || !dir.pop() || !dir.starts_with(cgroup_root) {
            return Ok(limit);
        }
    }
}

/// Each task's budget: `configured`, else (`limit` − `reserve`) ÷ `slots`; `None` without
/// either.
#[must_use]
pub fn budget(
    configured: Option<u64>,
    limit: Option<u64>,
    slots: u32,
    reserve: Reserve,
) -> Option<u64> {
    configured.or_else(|| {
        let limit = limit?;
        let kept = match reserve {
            Reserve::Percent(percent) => limit / 100 * u64::from(percent),
            Reserve::Bytes(bytes) => bytes,
        };
        Some(limit.saturating_sub(kept) / u64::from(slots.max(1)))
    })
}

/// The memory of the processes in group `pgid`, in bytes (PSS, else RSS); 0 when none is
/// left.
#[must_use]
pub fn group_memory(proc_root: &Path, pgid: i32) -> u64 {
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return 0;
    };
    let page = page_size();
    let mut total = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let dir = entry.path();
        // A process that ends meanwhile is simply not counted.
        let Ok(stat) = std::fs::read_to_string(dir.join("stat")) else {
            continue;
        };
        if process_group(&stat) != Some(pgid) {
            continue;
        }
        total += pss(&dir).or_else(|| rss(&dir, page)).unwrap_or(0);
    }
    total
}

/// The process group in a `/proc/<pid>/stat` line (field 5; the command, field 2, is in
/// parentheses and may hold spaces or parentheses itself).
#[must_use]
pub fn process_group(stat: &str) -> Option<i32> {
    let after = &stat[stat.rfind(')')? + 1..];
    after.split_whitespace().nth(2)?.parse().ok()
}

fn pss(dir: &Path) -> Option<u64> {
    let rollup = std::fs::read_to_string(dir.join("smaps_rollup")).ok()?;
    let kib: u64 = rollup
        .lines()
        .find_map(|line| line.strip_prefix("Pss:"))?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .ok()?;
    Some(kib * 1024)
}

fn rss(dir: &Path, page: u64) -> Option<u64> {
    let statm = std::fs::read_to_string(dir.join("statm")).ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * page)
}

fn page_size() -> u64 {
    u64::try_from(rustix::param::page_size()).unwrap_or(4096)
}

/// Samples group `pgid` every `watch.interval`, recording the peak in `seen`; completes with
/// the sample once one is over the budget (never without a budget).
pub async fn over_budget(watch: &MemoryWatch, pgid: i32, seen: &SharedSeen) -> u64 {
    let mut tick = tokio::time::interval(watch.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let root = watch.proc_root.clone();
        let used = tokio::task::spawn_blocking(move || group_memory(&root, pgid))
            .await
            .unwrap_or(0);
        let mut record = seen.lock().unwrap_or_else(PoisonError::into_inner);
        record.peak = record.peak.max(used);
        if let Some(budget) = watch.budget
            && used > budget
        {
            record.over = Some(used);
            return used;
        }
    }
}

/// A group's watch, running until it is dropped (so a process group ID the kernel gives to
/// someone else later is never watched, let alone killed).
#[derive(Debug)]
pub struct GroupWatch {
    task: JoinHandle<()>,
}

impl Drop for GroupWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Watches group `pgid` (recording in `seen`) from now until the returned watch is dropped:
/// over the budget it tells `stop` ([`StopReason::OomKilled`]), kills the group at once
/// (SIGKILL) and counts it in `kills`. The reason goes first, so the supervisor has it before
/// the process's end. Started as soon as the process is, it covers a runner's start and a
/// stopped task's grace period too.
#[must_use]
pub fn watch_group(
    watch: MemoryWatch,
    pgid: i32,
    seen: SharedSeen,
    stop: StopSender,
    kills: Counter,
) -> GroupWatch {
    let task = tokio::spawn(async move {
        over_budget(&watch, pgid, &seen).await;
        let _ = stop.send(StopReason::OomKilled);
        if let Some(group) = Pid::from_raw(pgid) {
            // A group that is gone already is fine.
            let _ = kill_process_group(group, Signal::KILL);
        }
        kills.inc();
    });
    GroupWatch { task }
}

/// `bytes` as people read sizes: `512 B`, `64 KiB`, `1.5 GiB`.
#[must_use]
pub fn show_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "a size shown to one decimal place"
    )]
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    let text = format!("{value:.1}");
    let text = text.strip_suffix(".0").unwrap_or(&text);
    format!("{text} {}", UNITS[unit])
}

/// The message of an attempt killed for its memory.
#[must_use]
pub fn over_budget_message(budget: u64, used: u64) -> String {
    format!(
        "exceeded memory budget {} ({} used)",
        show_size(budget),
        show_size(used)
    )
}
