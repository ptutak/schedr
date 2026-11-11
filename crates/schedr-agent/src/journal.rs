//! The journal on disk: one file per attempt the agent started and the master hasn't
//! acknowledged, so a restarted agent still reports a result it had and stops a task process
//! its previous incarnation left behind (see archive/PROJECT.md, Worker lifecycle).
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
//!   `/proc/<pid>/stat`), so a recycled PID isn't taken for the task. Without `/proc` the
//!   agent warns once and nothing is stopped; once the leader is gone, nothing is stopped.
//!
//! - **Cleanups:** an attempt of a task with a cleanup also keeps `<attempt_id>.attempt`, what
//!   its cleanup needs to run after a restart (the assignment and where its code is), written
//!   before `Started`, and `<attempt_id>.handles`, its handles, a line each, on disk before
//!   the task hears they are registered. Its record is `cleaning` from the task's end, with the
//!   task's result: the cleanup runs again from there (see archive/PROJECT.md, Per-task cleanup).
//!
//! - **One agent per journal:** it holds a lock (see [`Journal::lock`]).
//!
//! Its operations block (fsync); the agent runs them off its event loop.

use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use prost::Message;
use rustix::process::{Pid, Signal, kill_process_group};
use schedr_model::runner::CleanupHandle;
use schedr_proto::pb;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// The prefix of a file being written; such files are removed on start.
const TEMP_PREFIX: &str = ".tmp-";

/// What a cleanup needs after a restart: `<attempt_id>.attempt`.
const KEPT_EXTENSION: &str = "attempt";

/// An attempt's handles, a line each: `<attempt_id>.handles`.
const HANDLES_EXTENSION: &str = "handles";

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
    /// The task ended with this result and its cleanup runs, in this process once identified.
    Cleaning(pb::Finished, Option<ProcessIdentity>),
}

/// What a task's cleanup needs to run after an agent restart.
#[derive(Debug, Clone, PartialEq)]
pub struct KeptAttempt {
    /// Its assignment, the inputs inside it however they came.
    pub assign: pb::AssignTask,
    /// The copy of the code it ran with; `None`: the configured root.
    pub code_dir: Option<PathBuf>,
    /// The hash of that code.
    pub code_hash: Option<String>,
    /// The copies of the other DAG roots on its `sys.path`, by root.
    pub root_copies: std::collections::BTreeMap<String, PathBuf>,
    /// The copy of the script root its script and cleanup script came from.
    pub script_dir: Option<PathBuf>,
}

/// A [`KeptAttempt`] as stored.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredAttempt {
    /// The `AssignTask` message, protobuf-encoded, as hex.
    assign: String,
    code_dir: Option<PathBuf>,
    code_hash: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    root_copies: std::collections::BTreeMap<String, PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    script_dir: Option<PathBuf>,
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
    Cleaning {
        attempt_id: String,
        /// The task's `Finished` message, protobuf-encoded, as hex.
        finished: String,
        process: Option<ProcessIdentity>,
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
        let json = encoded(attempt_id, record)?;
        self.replace(attempt_id, &path, &json)
    }

    /// Records several attempts' records, as [`Self::write`] does each, with their files
    /// written and flushed to disk side by side and the directory flushed once (M4B.5, B184):
    /// a burst of results costs about one flush's time, not two flushes each. On disk when this
    /// returns, all of them; an error leaves those written before it.
    ///
    /// # Errors
    ///
    /// The first I/O error, or `InvalidInput` for an attempt ID that isn't a safe file name
    /// (then nothing is written).
    pub fn write_many(&self, records: &[(&str, &JournalRecord)]) -> io::Result<()> {
        let files = records
            .iter()
            .map(|(attempt_id, record)| {
                Ok((
                    *attempt_id,
                    self.path(attempt_id)?,
                    encoded(attempt_id, record)?,
                ))
            })
            .collect::<io::Result<Vec<_>>>()?;
        fs::create_dir_all(&self.dir)?;
        let written: Vec<io::Result<()>> = std::thread::scope(|scope| {
            let threads: Vec<_> = files
                .iter()
                .map(|(attempt_id, _, json)| {
                    let temp = self.dir.join(format!("{TEMP_PREFIX}{attempt_id}"));
                    scope.spawn(move || -> io::Result<()> {
                        let mut file = private_file().truncate(true).open(&temp)?;
                        file.write_all(json)?;
                        file.sync_all()
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|t| {
                    t.join()
                        .unwrap_or_else(|_| Err(io::Error::other("a write panicked")))
                })
                .collect()
        });
        for ((attempt_id, path, _), done) in files.iter().zip(written) {
            done?;
            fs::rename(self.dir.join(format!("{TEMP_PREFIX}{attempt_id}")), path)?;
        }
        self.sync_dir()
    }
}

/// A record as its file holds it.
fn encoded(attempt_id: &str, record: &JournalRecord) -> io::Result<Vec<u8>> {
    {
        let stored = match record {
            JournalRecord::Running(process) => Stored::Running {
                attempt_id: attempt_id.to_owned(),
                process: *process,
            },
            JournalRecord::Finished(finished) => Stored::Finished {
                attempt_id: attempt_id.to_owned(),
                finished: to_hex(&finished.encode_to_vec()),
            },
            JournalRecord::Cleaning(finished, process) => Stored::Cleaning {
                attempt_id: attempt_id.to_owned(),
                finished: to_hex(&finished.encode_to_vec()),
                process: *process,
            },
        };
        serde_json::to_vec(&stored).map_err(io::Error::other)
    }
}

impl Journal {
    /// Replaces `path` with `content`, atomically; on disk when this returns.
    fn replace(&self, attempt_id: &str, path: &Path, content: &[u8]) -> io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        let temp = self.dir.join(format!("{TEMP_PREFIX}{attempt_id}"));
        let mut file = private_file().truncate(true).open(&temp)?;
        file.write_all(content)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)?;
        self.sync_dir()
    }

    /// Keeps what an attempt's cleanup needs to run after a restart; on disk when this
    /// returns.
    ///
    /// # Errors
    ///
    /// An I/O error if it can't be written, or `InvalidInput` for an attempt ID that isn't a
    /// safe file name.
    pub fn keep(&self, attempt_id: &str, kept: &KeptAttempt) -> io::Result<()> {
        let path = self.path(attempt_id)?.with_extension(KEPT_EXTENSION);
        let stored = StoredAttempt {
            assign: to_hex(&kept.assign.encode_to_vec()),
            code_dir: kept.code_dir.clone(),
            code_hash: kept.code_hash.clone(),
            root_copies: kept.root_copies.clone(),
            script_dir: kept.script_dir.clone(),
        };
        let json = serde_json::to_vec(&stored).map_err(io::Error::other)?;
        self.replace(attempt_id, &path, &json)
    }

    /// What was kept for an attempt's cleanup; `None` if nothing was.
    ///
    /// # Errors
    ///
    /// An I/O error if it can't be read, `InvalidData` if it doesn't read as what was written,
    /// or `InvalidInput` for an attempt ID that isn't a safe file name.
    pub fn kept(&self, attempt_id: &str) -> io::Result<Option<KeptAttempt>> {
        let path = self.path(attempt_id)?.with_extension(KEPT_EXTENSION);
        let Some(bytes) = read_if_there(&path)? else {
            return Ok(None);
        };
        let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);
        let stored: StoredAttempt =
            serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
        let assign = from_hex(&stored.assign).ok_or_else(|| invalid("not hex".to_owned()))?;
        Ok(Some(KeptAttempt {
            assign: pb::AssignTask::decode(assign.as_slice())
                .map_err(|e| invalid(e.to_string()))?,
            code_dir: stored.code_dir,
            code_hash: stored.code_hash,
            root_copies: stored.root_copies,
            script_dir: stored.script_dir,
        }))
    }

    /// Adds a handle to an attempt's; on disk when this returns.
    ///
    /// # Errors
    ///
    /// An I/O error if it can't be written, or `InvalidInput` for an attempt ID that isn't a
    /// safe file name.
    pub fn add_handle(&self, attempt_id: &str, handle: &CleanupHandle) -> io::Result<()> {
        let path = self.path(attempt_id)?.with_extension(HANDLES_EXTENSION);
        let mut line = serde_json::to_vec(handle).map_err(io::Error::other)?;
        line.push(b'\n');
        fs::create_dir_all(&self.dir)?;
        let new = !path.exists();
        let mut file = private_file().append(true).open(&path)?;
        file.write_all(&line)?;
        file.sync_all()?;
        if new {
            self.sync_dir()?;
        }
        Ok(())
    }

    /// An attempt's handles, in registration order. A last line with no end, cut off by a
    /// crash, was never acknowledged to the task, and is left out.
    ///
    /// # Errors
    ///
    /// An I/O error if they can't be read, `InvalidData` if a complete line doesn't read, or
    /// `InvalidInput` for an attempt ID that isn't a safe file name.
    pub fn handles(&self, attempt_id: &str) -> io::Result<Vec<CleanupHandle>> {
        let path = self.path(attempt_id)?.with_extension(HANDLES_EXTENSION);
        let bytes = read_if_there(&path)?.unwrap_or_default();
        let complete = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(&[][..], |end| &bytes[..end]);
        if complete.is_empty() {
            return Ok(Vec::new());
        }
        complete
            .split(|byte| *byte == b'\n')
            .map(|line| {
                serde_json::from_slice(line)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            })
            .collect()
    }

    /// Forgets an attempt; one that isn't recorded is fine.
    ///
    /// # Errors
    ///
    /// An I/O error if the file can't be removed, or `InvalidInput` for an attempt ID that
    /// isn't a safe file name.
    pub fn remove(&self, attempt_id: &str) -> io::Result<()> {
        let path = self.path(attempt_id)?;
        // The record last: what is left of a crash here is removed on the next start.
        for extension in [KEPT_EXTENSION, HANDLES_EXTENSION] {
            match fs::remove_file(path.with_extension(extension)) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
        }
        match fs::remove_file(path) {
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

    /// What is recorded for one attempt, changing nothing; `None` when nothing is (or the
    /// file is being written, or doesn't read).
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an attempt ID that isn't a safe file name.
    pub fn record(&self, attempt_id: &str) -> io::Result<Option<JournalRecord>> {
        Ok(read(&self.path(attempt_id)?, attempt_id).ok())
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
        if clean {
            self.remove_strays(&records)?;
        }
        Ok(records)
    }

    /// Removes the kept files of attempts with no record (a crash between the two).
    fn remove_strays(&self, records: &[(String, JournalRecord)]) -> io::Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let extension = path.extension().and_then(|e| e.to_str());
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            if matches!(extension, Some(KEPT_EXTENSION | HANDLES_EXTENSION))
                && !records.iter().any(|(id, _)| id == stem)
            {
                remove_logged(&path, "journal file with no record");
            }
        }
        Ok(())
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

/// A journal file being opened: only its owner reads it, what it holds being the task's own.
fn private_file() -> fs::OpenOptions {
    let mut options = fs::OpenOptions::new();
    options.create(true).write(true).mode(0o600);
    options
}

/// A file's content; `None` if it isn't there.
fn read_if_there(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
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
        Stored::Cleaning {
            attempt_id,
            finished,
            process,
        } => {
            let bytes = from_hex(&finished).ok_or_else(|| invalid("not hex".to_owned()))?;
            let finished =
                pb::Finished::decode(bytes.as_slice()).map_err(|e| invalid(e.to_string()))?;
            (attempt_id, JournalRecord::Cleaning(finished, process))
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

/// The group a journal's `pgid` names; never group 1 (B88): signalling it is `kill(-1)`,
/// every process the agent may signal, and no task's group is init's.
fn orphan_group(pgid: i32) -> Option<Pid> {
    if pgid <= 1 {
        return None;
    }
    Pid::from_raw(pgid)
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

/// How an orphan is waited for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrphanWaits {
    /// How often it is looked at (`orphan_poll`).
    pub poll: Duration,
    /// How long it may take to go after SIGKILL (`orphan_kill_wait`).
    pub kill_wait: Duration,
}

impl Default for OrphanWaits {
    /// 50 ms and 5 s.
    fn default() -> Self {
        Self {
            poll: ORPHAN_POLL,
            kill_wait: ORPHAN_KILL_WAIT,
        }
    }
}

/// Stops the process group `identity` names, if its leader still runs: SIGTERM, and SIGKILL
/// if the group hasn't ended after `grace`, then waits for it to go (at most 5 s).
///
/// # Errors
///
/// An I/O error from signalling the group.
pub async fn stop_orphan(
    identity: ProcessIdentity,
    grace: Duration,
    waits: OrphanWaits,
) -> io::Result<Orphan> {
    if process_identity(identity.pgid).ok() != Some(identity) {
        return Ok(Orphan::Gone);
    }
    let Some(group) = orphan_group(identity.pgid) else {
        return Ok(Orphan::Gone);
    };
    let signal = |signal| match kill_process_group(group, signal) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
        Err(error) => Err(io::Error::from(error)),
    };
    signal(Signal::TERM)?;
    if gone_within(group, grace, waits.poll).await {
        return Ok(Orphan::Stopped);
    }
    signal(Signal::KILL)?;
    if !gone_within(group, waits.kill_wait, waits.poll).await {
        tracing::warn!(
            pgid = identity.pgid,
            "a killed task process group hasn't gone yet"
        );
    }
    Ok(Orphan::Killed)
}

/// Whether the process group goes within `wait`.
async fn gone_within(group: Pid, wait: Duration, poll: Duration) -> bool {
    let deadline = Instant::now() + wait;
    loop {
        if !crate::process::group_running(group) {
            return true;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        tokio::time::sleep(poll.min(left)).await;
    }
}
