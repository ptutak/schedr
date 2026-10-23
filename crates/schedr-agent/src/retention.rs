//! Log retention on the worker (see PROJECT.md, Task logs): logs older than
//! `[worker] log_retention` (30 days by default) are deleted, and beyond `[worker]
//! log_max_size` (no limit by default) the oldest go first. A log whose attempt is in the
//! journal (running, or its result not yet acknowledged) is never deleted, though it counts
//! towards the size limit. Parse logs
//! (`parse-<root>.log`) follow the same rules.
//!
//! A sweep runs when the worker starts and then every hour.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::journal::Journal;
use crate::metrics::AgentMetrics;

#[cfg(test)]
mod tests;

/// How often logs are swept.
pub const SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// What to keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRetention {
    /// Logs older than this (by their last change) are deleted.
    pub max_age: Duration,
    /// Beyond this many bytes, the oldest logs are deleted; `None`: no limit.
    pub max_size: Option<u64>,
}

/// What a sweep deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Files deleted.
    pub files: u64,
    /// Their bytes.
    pub bytes: u64,
    /// The bytes of logs left.
    pub kept: u64,
}

struct LogFile {
    path: PathBuf,
    modified: SystemTime,
    size: u64,
}

/// Deletes from `log_dir` what `retention` doesn't keep at `now`, sparing the logs of
/// attempts in `journal`. A file that can't be deleted is logged and counted as kept.
///
/// # Errors
///
/// An I/O error if the directory can't be read (a missing one has nothing to sweep).
pub fn sweep(
    log_dir: &Path,
    journal: &Journal,
    retention: LogRetention,
    now: SystemTime,
) -> io::Result<Swept> {
    let kept: Vec<String> = match journal.records() {
        Ok(records) => records
            .into_iter()
            .map(|(id, _)| format!("{id}.log"))
            .collect(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    let entries = match std::fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Swept::default()),
        Err(e) => return Err(e),
    };
    let mut files = Vec::new();
    // Spared logs count towards the size limit too: it is about the disk.
    let mut spared: u64 = 0;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let is_log = Path::new(&name).extension().is_some_and(|e| e == "log");
        if !metadata.is_file() || !is_log {
            continue;
        }
        if kept.contains(&name) {
            spared += metadata.len();
            continue;
        }
        files.push(LogFile {
            path: entry.path(),
            modified: metadata.modified().unwrap_or(now),
            size: metadata.len(),
        });
    }
    // Oldest first: deleted first when over the size limit.
    files.sort_by_key(|f| f.modified);
    let mut total: u64 = spared + files.iter().map(|f| f.size).sum::<u64>();
    let mut swept = Swept::default();
    for file in files {
        let expired = now
            .duration_since(file.modified)
            .is_ok_and(|age| age > retention.max_age);
        let over = retention.max_size.is_some_and(|max| total > max);
        if !expired && !over {
            continue;
        }
        match std::fs::remove_file(&file.path) {
            Ok(()) => {
                total -= file.size;
                swept.files += 1;
                swept.bytes += file.size;
            }
            Err(error) => {
                tracing::warn!(path = %file.path.display(), %error, "can't delete an old log");
            }
        }
    }
    swept.kept = total;
    Ok(swept)
}

/// Sweeps now and every [`SWEEP_INTERVAL`] until `stop` completes, recording the bytes kept
/// and deleted in `metrics`.
pub async fn sweep_logs(
    log_dir: PathBuf,
    journal: Journal,
    retention: LogRetention,
    metrics: AgentMetrics,
    stop: impl Future<Output = ()>,
) {
    tokio::pin!(stop);
    let mut tick = tokio::time::interval(SWEEP_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = &mut stop => return,
            _ = tick.tick() => {}
        }
        let (dir, journal) = (log_dir.clone(), journal.clone());
        let swept = tokio::task::spawn_blocking(move || {
            sweep(&dir, &journal, retention, SystemTime::now())
        })
        .await;
        if let Ok(Ok(swept)) = &swept {
            metrics
                .log_bytes
                .set(i64::try_from(swept.kept).unwrap_or(i64::MAX));
            metrics.log_swept_bytes.inc_by(swept.bytes);
        }
        match swept {
            Ok(Ok(swept)) if swept.files > 0 => tracing::info!(
                files = swept.files,
                bytes = swept.bytes,
                "deleted logs past their retention"
            ),
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "can't sweep the log directory"),
            Err(error) => tracing::warn!(%error, "sweeping the logs failed"),
        }
    }
}
