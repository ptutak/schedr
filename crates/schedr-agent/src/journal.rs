//! The journal on disk: one file per attempt the agent started and the master hasn't
//! acknowledged, so a restarted agent still reports a result it had and stops a task process
//! its previous incarnation left behind (see PROJECT.md, Worker lifecycle).
//!
//! - **Files:** `<journal_dir>/<attempt_id>.json`, each replaced atomically (a temporary file,
//!   fsync, rename, fsync of the directory) before the message it backs is sent: `running`
//!   before `Started` (and again with the process's identity once it is spawned), `finished`
//!   before `Finished`. A file goes when the master acknowledges the result or decides to
//!   forget the attempt.
//! - **On start:** temporary files left by a crash are removed, finished results are reported
//!   again, and the process groups of attempts that were running are stopped (SIGTERM, then
//!   SIGKILL after a grace time); those attempts aren't reported, so the master ends them
//!   `lost`.
//! - **Process identity:** a process group ID and the start time of its leader (from
//!   `/proc/<pid>/stat`), so a recycled PID isn't taken for the task. Without `/proc`, or once
//!   the leader is gone, nothing is stopped.
//!
//! - **One agent per journal:** it holds a lock (see [`Journal::lock`]).
//!
//! Its operations block (fsync); the agent runs them off its event loop.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use prost::Message;
use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};
use schedr_proto::pb;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// The prefix of a file being written; such files are removed on start.
const TEMP_PREFIX: &str = ".tmp-";

/// The lock file: one agent per journal.
pub const LOCK_FILE: &str = ".lock";

/// How often a stopped orphan is checked for being gone.
const ORPHAN_POLL: Duration = Duration::from_millis(50);

/// How long a killed orphan's group has to go (its memory freed) before the agent goes on.
const ORPHAN_KILL_WAIT: Duration = Duration::from_secs(5);

/// What identifies a task's process group across an agent restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    /// The process group ID (the PID of its leader).
    pub pgid: i32,
    /// The leader's start time, in clock ticks since boot (field 22 of `/proc/<pid>/stat`).
    pub start_time: u64,
}

/// What the journal knows about an attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum JournalRecord {
    /// Started; its process, once spawned and identified.
    Running(Option<ProcessIdentity>),
    /// Ended with this result, not yet acknowledged.
    Finished(pb::Finished),
}

/// A journal file as stored.
#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum Stored {
    Running {
        attempt_id: String,
        process: Option<ProcessIdentity>,
    },
    Finished {
        attempt_id: String,
        /// The `Finished` message, protobuf-encoded, as hex.
        finished: String,
    },
}

/// The journal directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Journal {
    dir: PathBuf,
}

impl Journal {
    /// The journal in `dir` (created when first written).
    #[must_use]
    pub const fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Its directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Takes the journal's lock for as long as the returned file is open: one agent per
    /// journal (another one's recovery would stop this one's tasks).
    ///
    /// # Errors
    ///
    /// An I/O error if the directory or lock file can't be created, or `WouldBlock` if another
    /// process holds the lock.
    pub fn lock(&self) -> io::Result<File> {
        fs::create_dir_all(&self.dir)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.dir.join(LOCK_FILE))?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
        Ok(file)
    }

    /// Records `record` for an attempt, replacing what was there; it is on disk when this
    /// returns.
    ///
    /// # Errors
    ///
    /// An I/O error if the directory or file can't be written, or `InvalidInput` for an
    /// attempt ID that isn't a safe file name.
    pub fn write(&self, attempt_id: &str, record: &JournalRecord) -> io::Result<()> {
        let path = self.path(attempt_id)?;
        let stored = match record {
            JournalRecord::Running(process) => Stored::Running {
                attempt_id: attempt_id.to_owned(),
                process: *process,
            },
            JournalRecord::Finished(finished) => Stored::Finished {
                attempt_id: attempt_id.to_owned(),
                finished: to_hex(&finished.encode_to_vec()),
            },
        };
        let json = serde_json::to_vec(&stored).map_err(io::Error::other)?;
        fs::create_dir_all(&self.dir)?;
        let temp = self.dir.join(format!("{TEMP_PREFIX}{attempt_id}"));
        let mut file = File::create(&temp)?;
        file.write_all(&json)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, &path)?;
        self.sync_dir()
    }

    /// Forgets an attempt; one that isn't recorded is fine.
    ///
    /// # Errors
    ///
    /// An I/O error if the file can't be removed, or `InvalidInput` for an attempt ID that
    /// isn't a safe file name.
    pub fn remove(&self, attempt_id: &str) -> io::Result<()> {
        match fs::remove_file(self.path(attempt_id)?) {
            Ok(()) => self.sync_dir(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Removes temporary files left by a crash and reads every attempt recorded, by ID; a file
    /// that doesn't read is logged and removed (it was never acted on). Only for the agent
    /// holding the journal, before it starts writing.
    ///
    /// # Errors
    ///
    /// An I/O error if the directory can't be read or created.
    pub fn recover(&self) -> io::Result<Vec<(String, JournalRecord)>> {
        fs::create_dir_all(&self.dir)?;
        self.scan(true)
    }

    /// Every attempt recorded, by ID, changing nothing: files being written or that don't
    /// read are left out (safe while an agent writes the journal).
    ///
    /// # Errors
    ///
    /// An I/O error if the directory can't be read.
    pub fn records(&self) -> io::Result<Vec<(String, JournalRecord)>> {
        self.scan(false)
    }

    fn scan(&self, clean: bool) -> io::Result<Vec<(String, JournalRecord)>> {
        let mut records = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            if name.starts_with(TEMP_PREFIX) {
                if clean {
                    remove_logged(&path, "temporary journal file");
                }
                continue;
            }
            let Some(attempt_id) = name.strip_suffix(".json") else {
                continue;
            };
            match read(&path, attempt_id) {
                Ok(record) => records.push((attempt_id.to_owned(), record)),
                Err(error) if clean => {
                    tracing::warn!(path = %path.display(), %error, "unreadable journal file");
                    remove_logged(&path, "unreadable journal file");
                }
                Err(_) => {}
            }
        }
        records.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(records)
    }

    fn path(&self, attempt_id: &str) -> io::Result<PathBuf> {
        let safe = !attempt_id.is_empty()
            && attempt_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if safe {
            Ok(self.dir.join(format!("{attempt_id}.json")))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("attempt ID `{attempt_id}` isn't a safe file name"),
            ))
        }
    }

    fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.dir)?.sync_all()
    }
}

/// Removes a file recovery doesn't want; a failure is logged, not fatal (the records that do
/// read must still be reported).
fn remove_logged(path: &Path, what: &str) {
    if let Err(error) = fs::remove_file(path) {
        tracing::warn!(path = %path.display(), %error, "can't remove {what}");
    }
}

fn read(path: &Path, attempt_id: &str) -> io::Result<JournalRecord> {
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);
    let stored: Stored =
        serde_json::from_slice(&fs::read(path)?).map_err(|e| invalid(e.to_string()))?;
    let (id, record) = match stored {
        Stored::Running {
            attempt_id,
            process,
        } => (attempt_id, JournalRecord::Running(process)),
        Stored::Finished {
            attempt_id,
            finished,
        } => {
            let bytes = from_hex(&finished).ok_or_else(|| invalid("not hex".to_owned()))?;
            let finished =
                pb::Finished::decode(bytes.as_slice()).map_err(|e| invalid(e.to_string()))?;
            (attempt_id, JournalRecord::Finished(finished))
        }
    };
    if id == attempt_id {
        Ok(record)
    } else {
        Err(invalid(format!("it is for attempt `{id}`")))
    }
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut hex, b| {
        let _ = write!(hex, "{b:02x}");
        hex
    })
}

fn from_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// The identity of the process group led by `pid`.
///
/// # Errors
///
/// An I/O error if `/proc/<pid>/stat` can't be read (no such process, or no `/proc`), or
/// `InvalidData` if it doesn't parse.
pub fn process_identity(pid: i32) -> io::Result<ProcessIdentity> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    // The command name (field 2) is in parentheses and may contain anything; the fields after
    // its closing parenthesis start at field 3.
    let start_time = stat
        .rsplit_once(')')
        .and_then(|(_, rest)| rest.split_whitespace().nth(22 - 3))
        .and_then(|field| field.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("`/proc/{pid}/stat` has no start time"),
            )
        })?;
    Ok(ProcessIdentity {
        pgid: pid,
        start_time,
    })
}

/// How stopping an orphaned process group went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Orphan {
    /// Its leader isn't running (any more, or the PID is another process's now): left alone.
    Gone,
    /// It ended after SIGTERM.
    Stopped,
    /// It needed SIGKILL.
    Killed,
}

/// Stops the process group `identity` names, if its leader still runs: SIGTERM, and SIGKILL
/// if the group hasn't ended after `grace`, then waits for it to go (at most 5 s).
///
/// # Errors
///
/// An I/O error from signalling the group.
pub async fn stop_orphan(identity: ProcessIdentity, grace: Duration) -> io::Result<Orphan> {
    if process_identity(identity.pgid).ok() != Some(identity) {
        return Ok(Orphan::Gone);
    }
    let Some(group) = Pid::from_raw(identity.pgid) else {
        return Ok(Orphan::Gone);
    };
    let signal = |signal| match kill_process_group(group, signal) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
        Err(error) => Err(io::Error::from(error)),
    };
    signal(Signal::TERM)?;
    if gone_within(group, grace).await {
        return Ok(Orphan::Stopped);
    }
    signal(Signal::KILL)?;
    if !gone_within(group, ORPHAN_KILL_WAIT).await {
        tracing::warn!(
            pgid = identity.pgid,
            "a killed task process group hasn't gone yet"
        );
    }
    Ok(Orphan::Killed)
}

/// Whether the process group goes within `wait`.
async fn gone_within(group: Pid, wait: Duration) -> bool {
    let deadline = Instant::now() + wait;
    loop {
        if test_kill_process_group(group).is_err() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(ORPHAN_POLL).await;
    }
}
