//! The real task process: started in its own process group with a clean environment, its
//! stdout and stderr written to the task's log file and kept (their tail) for the
//! `ProcessResult`.
//!
//! This is the thin operating-system layer under [`crate::supervise`]; it is exercised by
//! the integration tests (`tests/processes.rs`), the logic around it by unit tests.

use std::collections::BTreeMap;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use rustix::process::{Pid, kill_process_group, test_kill_process_group};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::output::TailBuffer;
use crate::status::Termination;
use crate::supervise::{Process, Signal};
use crate::usage::{Ended, clock_ticks, exited, zombie_usage};

#[cfg(test)]
mod tests;

/// What to start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSpec {
    /// The program.
    pub program: PathBuf,
    /// Its arguments.
    pub args: Vec<String>,
    /// Its whole environment (see [`crate::env::task_environment`]).
    pub env: BTreeMap<String, String>,
    /// Its working directory.
    pub working_dir: PathBuf,
    /// The task's log file; stdout and stderr are appended to it.
    pub log_file: PathBuf,
    /// The output size limit per stream, in bytes.
    pub max_output_size: usize,
}

/// A started task process.
#[derive(Debug)]
pub struct OsProcess {
    child: Child,
    group: Pid,
    stdout: JoinHandle<io::Result<TailBuffer>>,
    stderr: JoinHandle<io::Result<TailBuffer>>,
    ended: Ended,
    /// Whether its zombie was read already (`wait` may be called again).
    read: bool,
}

/// Processes are started one at a time: a descriptor made inheritable for one child (see
/// [`spawn_passing`]) must not leak into another started at the same moment.
static SPAWNING: Mutex<()> = Mutex::const_new(());

/// Starts a task process.
///
/// # Errors
///
/// Returns an I/O error if the log file can't be opened or the program can't be started.
pub async fn spawn(spec: &ProcessSpec) -> io::Result<OsProcess> {
    spawn_passing(spec, None).await
}

/// Starts a process that inherits `pass` (open, at the same descriptor number); the parent's
/// copy is closed once the child has it. Used to hand a runner its end of the agent's socket
/// pair.
///
/// # Errors
///
/// Returns an I/O error if the log file can't be opened, the descriptor can't be made
/// inheritable, or the program can't be started.
pub async fn spawn_passing(spec: &ProcessSpec, pass: Option<OwnedFd>) -> io::Result<OsProcess> {
    let log = open_log(spec).await?;
    let spawning = SPAWNING.lock().await;
    if let Some(fd) = &pass {
        rustix::io::fcntl_setfd(fd, rustix::io::FdFlags::empty())?;
    }
    let child = Command::new(&spec.program)
        .args(&spec.args)
        .env_clear()
        .envs(&spec.env)
        .current_dir(&spec.working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn();
    drop(pass);
    drop(spawning);
    let mut child = child?;
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw);
    let group = pid.ok_or_else(|| io::Error::other("the started process has no pid"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("no stdout pipe"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("no stderr pipe"))?;
    Ok(OsProcess {
        ended: Ended::default(),
        read: false,
        child,
        group,
        stdout: tokio::spawn(collect(stdout, Arc::clone(&log), spec.max_output_size)),
        stderr: tokio::spawn(collect(stderr, log, spec.max_output_size)),
    })
}

/// The task's log file, opened for appending, shared by its stdout and stderr collectors.
///
/// # Errors
///
/// Returns an I/O error if it can't be opened.
pub(crate) async fn open_log(spec: &ProcessSpec) -> io::Result<SharedLog> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).append(true);
    let log = open_regular(options, &spec.log_file).await?;
    Ok(Arc::new(Mutex::new(log)))
}

/// Opens `path` with `options` only if it is a regular file with no other name, never
/// following a symbolic link (B62): a link planted where a log goes, symbolic or hard, would
/// make the agent read or write its target, and a FIFO would block the open (`O_NONBLOCK`,
/// which a regular file ignores).
///
/// # Errors
///
/// Returns the I/O error of the open (`ELOOP` for a symbolic link), or `InvalidInput` for a
/// file that isn't regular or has another hard link.
pub(crate) async fn open_regular(
    mut options: tokio::fs::OpenOptions,
    path: &Path,
) -> io::Result<tokio::fs::File> {
    use rustix::fs::OFlags;
    options.custom_flags((OFlags::NOFOLLOW | OFlags::NONBLOCK).bits().cast_signed());
    let file = options.open(path).await?;
    let metadata = file.metadata().await?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("`{}` isn't a regular file", path.display()),
        ));
    }
    if std::os::unix::fs::MetadataExt::nlink(&metadata) != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("`{}` has another hard link", path.display()),
        ));
    }
    Ok(file)
}

/// A log file written by a process's two output collectors.
pub(crate) type SharedLog = Arc<Mutex<tokio::fs::File>>;

/// Copies a pipe into the log file, keeping its tail.
pub(crate) async fn collect(
    mut pipe: impl AsyncRead + Unpin,
    log: SharedLog,
    limit: usize,
) -> io::Result<TailBuffer> {
    let mut tail = TailBuffer::new(limit);
    let mut chunk = vec![0; 64 * 1024];
    loop {
        let n = pipe.read(&mut chunk).await?;
        if n == 0 {
            // A tokio file write may still be in flight when `write_all` returns: the log is
            // complete only once flushed.
            log.lock().await.flush().await?;
            return Ok(tail);
        }
        tail.push(&chunk[..n]);
        log.lock().await.write_all(&chunk[..n]).await?;
    }
}

impl OsProcess {
    /// Its process ID, which is also its process group's ID.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.group.as_raw_nonzero().get()
    }

    /// The captured stdout and stderr, once both pipes are closed.
    ///
    /// # Errors
    ///
    /// Returns an I/O error from reading a pipe or writing the log file.
    pub async fn output(self) -> io::Result<(TailBuffer, TailBuffer)> {
        let stdout = self.stdout.await.map_err(io::Error::other)??;
        let stderr = self.stderr.await.map_err(io::Error::other)??;
        Ok((stdout, stderr))
    }
}

/// Whether a process of `group` still runs. A zombie (ended, not yet reaped) doesn't: a forked
/// task's shim holds its leader as one, and an agent that is a container's first process (or
/// a subreaper) adopts its tasks' orphans, which stay zombies until it reaps them. A process
/// whose main thread ended (`pthread_exit`) shows as a zombie too while its other threads run:
/// it still runs. Read from `/proc`; without it, whether the group has any process at all.
#[must_use]
pub fn group_running(group: Pid) -> bool {
    if test_kill_process_group(group).is_err() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return true;
    };
    let group = group.as_raw_nonzero().get();
    entries.flatten().any(|entry| {
        let is_pid = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.bytes().all(|b| b.is_ascii_digit()));
        is_pid
            && std::fs::read_to_string(entry.path().join("stat")).is_ok_and(|stat| {
                stat_fields(&stat).is_some_and(|(state, pgrp)| {
                    pgrp == group && (!ended(state) || a_thread_runs(&entry.path()))
                })
            })
    })
}

/// Whether a thread of the process at `/proc/<pid>` hasn't ended; if its threads can't be
/// listed, it may still run.
fn a_thread_runs(process: &Path) -> bool {
    let Ok(threads) = std::fs::read_dir(process.join("task")) else {
        return true;
    };
    threads.flatten().any(|thread| {
        std::fs::read_to_string(thread.path().join("stat"))
            .is_ok_and(|stat| stat_fields(&stat).is_some_and(|(state, _)| !ended(state)))
    })
}

/// The state (field 3) and process group (field 5) of a `/proc/.../stat` line.
fn stat_fields(stat: &str) -> Option<(&str, i32)> {
    // The command name (field 2) is in parentheses and may contain anything: the fields
    // after it start at its closing parenthesis.
    let (_, rest) = stat.rsplit_once(')')?;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?;
    let pgrp = fields.nth(1)?.parse().ok()?;
    Some((state, pgrp))
}

/// Whether a state means ended: a zombie, or dead.
fn ended(state: &str) -> bool {
    matches!(state, "Z" | "X" | "x")
}

impl Process for OsProcess {
    async fn wait(&mut self) -> io::Result<Termination> {
        // Its zombie is read before `wait` reaps it (decision 18A).
        if !self.read {
            if exited(self.group).await.is_ok() {
                self.ended.usage = zombie_usage(Path::new("/proc"), self.id(), clock_ticks());
            }
            self.read = true;
        }
        let status = self.child.wait().await?;
        self.ended.core_dumped = status.core_dumped();
        Ok(match (status.code(), status.signal()) {
            (Some(code), _) => Termination::Exited(code),
            (None, Some(signal)) => Termination::Signaled(signal),
            (None, None) => Termination::Exited(-1),
        })
    }

    fn signal(&mut self, signal: Signal) -> io::Result<()> {
        let sig = match signal {
            Signal::Term => rustix::process::Signal::TERM,
            Signal::Kill => rustix::process::Signal::KILL,
        };
        match kill_process_group(self.group, sig) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn group_alive(&self) -> bool {
        group_running(self.group)
    }

    fn ended(&self) -> Ended {
        self.ended
    }
}
