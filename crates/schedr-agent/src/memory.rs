//! The memory watchdog (see archive/PROJECT.md, Worker memory protection and crash recovery): every
//! task shares the pod's memory, and the kernel's out-of-memory killer would take every task
//! and the agent with it, so the agent measures each task and kills only the one over its
//! budget.
//!
//! - **The limit** is the smallest cgroup v2 `memory.max` from the agent's own cgroup up to
//!   the root (`/proc/self/cgroup`), so it is found in a pod and on a host alike. None found
//!   (`max`, cgroup v1): no task is killed but one asking for its own budget, peak memory is
//!   still measured. No `/proc`: nothing is measured and nothing is killed.
//! - **The budget** per task is its own (`memory=`, decision 436), else `[worker]
//!   memory_budget`, else (limit − 10 %) ÷ slots. A task asking for more than the limit less
//!   the reserve is turned away (decision 437); its cleanup, moved here, gets at most that.
//! - **A task's memory** is the PSS (`/proc/<pid>/smaps_rollup`) of every process in its
//!   process group (what SIGTERM and SIGKILL reach), RSS where PSS can't be read: PSS splits
//!   shared pages between the processes sharing them, so a tree isn't counted many times. Not
//!   for a forked task: its RSS counts the pages it shares with its fork server, so a sample
//!   without every PSS is skipped (no kill on it).
//! - **Sampling** every `memory_check_interval` (1 s); over the budget the group is
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
    /// The most one task may ask for (`memory=`, decision 437): the limit less the reserve;
    /// `None` without a limit (any is taken).
    pub ceiling: Option<u64>,
    /// `schedr` itself, for `schedr task-exec` (a hard memory cap, decision 50); `None`:
    /// this worker can't cap a task.
    pub task_exec: Option<PathBuf>,
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
            ceiling: None,
            task_exec: None,
            interval: Duration::from_secs(1),
            proc_root: PathBuf::from("/proc"),
        }
    }
}

impl MemoryWatch {
    /// This watch for a task asking for its own budget (`memory=`, decision 436), at most the
    /// ceiling: a moved cleanup, never turned away, gets what this worker can give.
    #[must_use]
    pub fn for_task(&self, own: Option<u64>) -> Self {
        let own = own.map(|own| self.ceiling.map_or(own, |ceiling| own.min(ceiling)));
        Self {
            budget: own.or(self.budget),
            ..self.clone()
        }
    }
}

/// What the watchdog saw of one attempt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemorySeen {
    /// The highest sample, in bytes (0: never measured).
    pub peak: u64,
    /// The latest sample, in bytes (0: never measured): what the heartbeat reports
    /// (decision 463).
    pub last: u64,
    /// The sample over the budget that got the task killed.
    pub over: Option<u64>,
}

/// The watchdog's record of one attempt, shared with whoever reports it.
pub type SharedSeen = Arc<Mutex<MemorySeen>>;

/// The running tasks' memory, each named, as their watchdogs last measured it (decision 463):
/// what the heartbeat reports (cheap to clone; clones share it).
#[derive(Debug, Clone, Default)]
pub struct RunningMemory(
    Arc<Mutex<std::collections::BTreeMap<String, (schedr_model::usage::TaskMemory, SharedSeen)>>>,
);

/// A task in [`RunningMemory`] until it is dropped.
#[derive(Debug)]
pub struct Watched {
    memory: RunningMemory,
    attempt_id: String,
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.memory.inner().remove(&self.attempt_id);
    }
}

impl RunningMemory {
    fn inner(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        std::collections::BTreeMap<String, (schedr_model::usage::TaskMemory, SharedSeen)>,
    > {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts the task `named` (its `bytes` ignored) with what its watchdog records in `seen`,
    /// until the returned value is dropped.
    #[must_use]
    pub fn watch(&self, named: schedr_model::usage::TaskMemory, seen: SharedSeen) -> Watched {
        let attempt_id = named.attempt_id.clone();
        self.inner().insert(attempt_id.clone(), (named, seen));
        Watched {
            memory: self.clone(),
            attempt_id,
        }
    }

    /// Each running task with its latest sample, largest first.
    #[must_use]
    pub fn now(&self) -> Vec<schedr_model::usage::TaskMemory> {
        let mut tasks: Vec<schedr_model::usage::TaskMemory> = self
            .inner()
            .values()
            .map(|(named, seen)| schedr_model::usage::TaskMemory {
                bytes: seen.lock().unwrap_or_else(PoisonError::into_inner).last,
                ..named.clone()
            })
            .collect();
        tasks.sort_by(|a, b| {
            b.bytes
                .cmp(&a.bytes)
                .then_with(|| a.attempt_id.cmp(&b.attempt_id))
        });
        tasks
    }
}

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

/// The memory of the agent's own cgroup v2 now (its `memory.current`, from its line in
/// `<proc_root>/self/cgroup`); `None` outside a cgroup v2 or where it can't be read.
#[must_use]
pub fn cgroup_current(proc_root: &Path, cgroup_root: &Path) -> Option<u64> {
    let cgroups = std::fs::read_to_string(proc_root.join("self/cgroup")).ok()?;
    let path = cgroups.lines().find_map(|line| line.strip_prefix("0::"))?;
    let dir = cgroup_root.join(path.trim_start_matches('/'));
    std::fs::read_to_string(dir.join("memory.current"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// This process's memory and CPU from `proc_root` (`self/stat`, `self/statm`), in pages of
/// `page` bytes and `ticks` a second, and its container's from `cgroup_root` (decision 463);
/// `None` without `/proc`.
#[must_use]
pub fn node_usage(
    proc_root: &Path,
    cgroup_root: &Path,
    page: u64,
    ticks: u64,
) -> Option<schedr_model::usage::NodeUsage> {
    let stat = std::fs::read_to_string(proc_root.join("self/stat")).ok()?;
    let statm = std::fs::read_to_string(proc_root.join("self/statm")).ok()?;
    // After the command (field 2, in parentheses): field 3 is [0].
    let fields: Vec<&str> = stat[stat.rfind(')')? + 1..].split_whitespace().collect();
    let field = |n: usize| fields.get(n - 3)?.parse::<u64>().ok();
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    #[expect(clippy::cast_precision_loss, reason = "seconds from clock ticks")]
    let cpu_seconds = (field(14)? + field(15)?) as f64 / ticks.max(1) as f64;
    Some(schedr_model::usage::NodeUsage {
        resident_bytes: resident_pages.saturating_mul(page),
        cpu_seconds,
        container_bytes: cgroup_current(proc_root, cgroup_root),
        container_limit_bytes: cgroup_limit(proc_root, cgroup_root).ok().flatten(),
    })
}

/// [`node_usage`] of this process, from `/proc` and `/sys/fs/cgroup`.
#[must_use]
pub fn this_node() -> Option<schedr_model::usage::NodeUsage> {
    node_usage(
        Path::new("/proc"),
        Path::new("/sys/fs/cgroup"),
        u64::try_from(rustix::param::page_size()).unwrap_or(4096),
        rustix::param::clock_ticks_per_second(),
    )
}

/// The most one task may take of `limit`: what `reserve` leaves (decision 437).
#[must_use]
pub fn ceiling(limit: u64, reserve: Reserve) -> u64 {
    let kept = match reserve {
        Reserve::Percent(percent) => limit / 100 * u64::from(percent),
        Reserve::Bytes(bytes) => bytes,
    };
    limit.saturating_sub(kept)
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
    configured.or_else(|| Some(ceiling(limit?, reserve) / u64::from(slots.max(1))))
}

/// The memory of the processes in group `pgid`, in bytes (PSS, else RSS where
/// `rss_fallback`); 0 when none is left. A zombie counts as nothing. `None` when a process's PSS can't be read and RSS
/// mustn't stand in: a forked process's RSS counts the pages it shares with its fork server
/// (most of the server's), so it would look far bigger than it is.
#[must_use]
pub fn group_memory(proc_root: &Path, pgid: i32, rss_fallback: bool) -> Option<u64> {
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return Some(0);
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
        // An unreaped child has no memory left, and no PSS to read (B54). A process whose
        // main thread ended while others run still has its memory, read through one of them
        // (the main thread's own files no longer show it).
        let dir = if crate::process::stat_fields(&stat)
            .is_some_and(|(state, _)| crate::process::ended(state))
        {
            match crate::process::live_thread(&dir) {
                Ok(Some(thread)) => thread,
                Ok(None) => continue,
                Err(_) => dir,
            }
        } else {
            dir
        };
        match pss(&dir) {
            Some(bytes) => total += bytes,
            None if rss_fallback => total += rss(&dir, page).unwrap_or(0),
            None => return None,
        }
    }
    Some(total)
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
/// the sample once one is over the budget (never without a budget). A sample that can't be
/// read (see [`group_memory`]) is skipped: no kill on a guess.
pub async fn over_budget(
    watch: &MemoryWatch,
    pgid: i32,
    seen: &SharedSeen,
    rss_fallback: bool,
) -> u64 {
    let mut tick = tokio::time::interval(watch.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let root = watch.proc_root.clone();
        let Some(used) =
            tokio::task::spawn_blocking(move || group_memory(&root, pgid, rss_fallback))
                .await
                .unwrap_or(None)
        else {
            continue;
        };
        let mut record = seen.lock().unwrap_or_else(PoisonError::into_inner);
        record.peak = record.peak.max(used);
        record.last = used;
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
/// stopped task's grace period too. `forked`: the group's processes share pages with a fork
/// server, so RSS never stands in for PSS (see [`group_memory`]).
#[must_use]
pub fn watch_group(
    watch: MemoryWatch,
    pgid: i32,
    seen: SharedSeen,
    stop: StopSender,
    kills: Counter,
    forked: bool,
) -> GroupWatch {
    let task = tokio::spawn(async move {
        over_budget(&watch, pgid, &seen, !forked).await;
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
