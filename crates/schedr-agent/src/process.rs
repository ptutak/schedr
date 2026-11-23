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
use std::time::Duration;

use rustix::process::{Pid, kill_process_group, test_kill_process_group};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::mask::{Mask, MaskSet};
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
    /// The secret values masked in its output, on their way to the log and the tail.
    pub mask: Mask,
    /// How long a runner has to say hello, if it is one (`[worker] runner_start_timeout`).
    pub start_timeout: Duration,
    /// How long its output may take to reach its log once it is stopped.
    pub output_drain: Duration,
    /// A `cleanup_anywhere` cleanup's barrier (B255, decision 504): a forked process waits at
    /// it itself; a spawned one through `schedr task-exec`, in its program and arguments.
    pub barrier: Option<schedr_model::runner::ForkBarrier>,
}

/// A started task process.
#[derive(Debug)]
pub struct OsProcess {
    child: Child,
    group: Pid,
    /// Its output's collectors, taken by [`Self::output`].
    stdout: Option<JoinHandle<io::Result<TailBuffer>>>,
    stderr: Option<JoinHandle<io::Result<TailBuffer>>>,
    /// Tells the readers to end with what they have (B274: [`Self::output_within`]).
    stop_reading: tokio::sync::watch::Sender<bool>,
    ended: Ended,
    /// Whether its zombie was read already (`wait` may be called again).
    read: bool,
    /// Whether it was reaped: its process group's ID may be another's since.
    reaped: bool,
}

impl Drop for OsProcess {
    /// A process dropped before it was reaped is killed with its whole group, as a forked one
    /// is: `kill_on_drop` kills the leader alone, and a dropped parse job or attempt must not
    /// leave its children running unwatched (Codex's review of M5.2d). Not yet reaped, the
    /// leader still holds the group's ID.
    fn drop(&mut self) {
        if !self.reaped {
            let _ = kill_process_group(self.group, rustix::process::Signal::KILL);
        }
    }
}

/// Processes are started one at a time: a descriptor made inheritable for one child (see
/// [`spawn_passing`]) must not leak into another started at the same moment.
pub(crate) static SPAWNING: Mutex<()> = Mutex::const_new(());

/// Starts a task process.
///
/// # Errors
///
/// Returns an I/O error if the log file can't be opened or the program can't be started.
pub async fn spawn(spec: &ProcessSpec) -> io::Result<OsProcess> {
    spawn_passing(spec, Vec::new()).await
}

/// Starts a process that inherits each of `pass` (open, at the same descriptor numbers); the
/// parent's copies are closed once the child has them. Used to hand a runner its end of the
/// agent's socket pair, and a plugin with a service its end of the service's (decision 518).
///
/// # Errors
///
/// Returns an I/O error if the log file can't be opened, the descriptor can't be made
/// inheritable, or the program can't be started.
pub async fn spawn_passing(spec: &ProcessSpec, pass: Vec<OwnedFd>) -> io::Result<OsProcess> {
    let log = open_log(spec).await?;
    let spawning = SPAWNING.lock().await;
    for fd in &pass {
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
    let (stop_reading, _) = tokio::sync::watch::channel(false);
    Ok(OsProcess {
        ended: Ended::default(),
        read: false,
        reaped: false,
        child,
        group,
        stdout: Some(tokio::spawn(collect(
            stdout,
            Arc::clone(&log),
            spec.max_output_size,
            spec.mask.clone(),
            stop_reading.subscribe(),
        ))),
        stderr: Some(tokio::spawn(collect(
            stderr,
            log,
            spec.max_output_size,
            spec.mask.clone(),
            stop_reading.subscribe(),
        ))),
        stop_reading,
    })
}

/// Starts an extension's service (decision 518): `program` with `args` in `dir`, given only
/// `env`, in a process group of its own, inheriting `pass` (its control socket's end); its
/// output piped, for the worker's log. Started one at a time with every other process, as
/// [`spawn_passing`]'s, so the descriptor reaches it alone.
///
/// # Errors
///
/// When the descriptor can't be made inheritable or the program can't be started.
pub async fn spawn_service(
    (program, args): (&Path, &[String]),
    env: &BTreeMap<String, String>,
    dir: &Path,
    pass: OwnedFd,
) -> io::Result<Child> {
    let spawning = SPAWNING.lock().await;
    rustix::io::fcntl_setfd(&pass, rustix::io::FdFlags::empty())?;
    let child = Command::new(program)
        .args(args)
        .env_clear()
        .envs(env)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn();
    drop(pass);
    drop(spawning);
    child
}

/// The mode of the log files the worker creates (`[worker] log_mode`, B182): set once when the
/// worker starts (one per process), owner-only until then.
static LOG_MODE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0o600);

/// Sets the mode the worker's log files get (`[worker] log_mode`): set on each file as it is
/// opened, so the umask doesn't change it.
pub fn set_log_mode(mode: u32) {
    LOG_MODE.store(mode, std::sync::atomic::Ordering::Relaxed);
}

/// Whether the worker stores its tasks' structured log lines readable (`[worker] log_format =
/// "plain"`, decision 514): set once when it starts; as written (JSON) until then.
static LOG_PLAIN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Sets how the worker stores structured log lines: readable (`plain`) or as written.
pub fn set_log_plain(plain: bool) {
    LOG_PLAIN.store(plain, std::sync::atomic::Ordering::Relaxed);
}

/// The most of one line held to see whether it is a structured one: a longer line is written
/// as it comes.
const STRUCTURED_LINE_MAX: usize = 256 * 1024;

/// Turns a stream's structured lines into their readable form (`log_format = "plain"`), holding
/// an unfinished line until its end; every other line passes as it is.
#[derive(Debug, Default)]
pub(crate) struct Plainer {
    held: Vec<u8>,
    /// A line too long to hold, being passed on until its end.
    passing: bool,
}

impl Plainer {
    /// What `bytes` give the log now.
    pub(crate) fn push(&mut self, mut bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len());
        while !bytes.is_empty() {
            let end = bytes.iter().position(|b| *b == b'\n');
            let (part, rest, ended) = match end {
                Some(i) => (&bytes[..=i], &bytes[i + 1..], true),
                None => (bytes, &[][..], false),
            };
            bytes = rest;
            if self.passing {
                out.extend_from_slice(part);
                self.passing = !ended;
                continue;
            }
            self.held.extend_from_slice(part);
            if ended {
                let line = std::mem::take(&mut self.held);
                out.extend(rendered_line(&line));
            } else if self.held.len() > STRUCTURED_LINE_MAX {
                out.append(&mut self.held);
                self.passing = true;
            }
        }
        out
    }

    /// The rest, at the stream's end.
    pub(crate) fn finish(&mut self) -> Vec<u8> {
        let line = std::mem::take(&mut self.held);
        rendered_line(&line)
    }
}

/// One line (with its end of line, if it has one) in its readable form, when structured.
fn rendered_line(line: &[u8]) -> Vec<u8> {
    let Ok(text) = std::str::from_utf8(line) else {
        return line.to_vec();
    };
    match schedr_model::log_line::LogLine::parse(text) {
        Some(parsed) => {
            let mut out = parsed.plain().into_bytes();
            if line.ends_with(b"\n") {
                out.push(b'\n');
            }
            out
        }
        None => line.to_vec(),
    }
}

/// Readies the worker's log directory for `mode` (B182), at its start: made if missing, its own
/// mode the log mode's readers' (0700 for 0600, 0750 for 0640, 0755 for 0644), and every log
/// already in it given `mode`, so those written before (an upgrade, a looser mode then) aren't
/// left readable until retention deletes them. A link in it is left alone, never followed.
///
/// # Errors
///
/// The I/O error of making the directory or setting a mode.
pub fn secure_log_dir(dir: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::create_dir_all(dir)?;
    let readers = |bit: u32, both: u32| if mode & bit == 0 { 0 } else { both };
    let dir_mode = 0o700 | readers(0o040, 0o050) | readers(0o004, 0o005);
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(dir_mode))?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() && metadata.permissions().mode() & 0o7777 != mode {
            std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(mode))?;
        }
    }
    Ok(())
}

/// Opens a log at `path` for appending, as [`open_regular`] does, with the worker's log mode:
/// set on the file itself, so neither the umask nor a mode from before (an upgrade, a mode
/// tightened since) decides who reads it.
///
/// # Errors
///
/// The open's I/O error, or setting the mode's.
pub(crate) async fn open_log_file(path: &Path) -> io::Result<tokio::fs::File> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = LOG_MODE.load(std::sync::atomic::Ordering::Relaxed);
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).append(true).mode(mode);
    let file = open_regular(options, path).await?;
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .await?;
    Ok(file)
}

/// The task's log file, opened for appending, shared by its stdout and stderr collectors.
///
/// # Errors
///
/// Returns an I/O error if it can't be opened.
pub(crate) async fn open_log(spec: &ProcessSpec) -> io::Result<SharedLog> {
    let log = open_log_file(&spec.log_file).await?;
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

/// Copies a pipe into the log file, keeping its tail; secret values are masked on the way.
pub(crate) async fn collect(
    mut pipe: impl AsyncRead + Unpin,
    log: SharedLog,
    limit: usize,
    mask: Mask,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> io::Result<TailBuffer> {
    let mut tail = TailBuffer::new(limit);
    let mut chunk = vec![0; 64 * 1024];
    let mut stream = mask.0.as_deref().map(MaskSet::stream);
    let mut plainer = LOG_PLAIN
        .load(std::sync::atomic::Ordering::Relaxed)
        .then(Plainer::default);
    loop {
        // Told to stop (B274): ended as at the pipe's end, with what it has.
        let n = tokio::select! {
            biased;
            read = pipe.read(&mut chunk) => read?,
            _ = stop.wait_for(|stop| *stop) => 0,
        };
        if n == 0 {
            if let Some(stream) = &mut stream {
                let mut rest = stream.finish();
                if let Some(plainer) = &mut plainer {
                    rest = plainer.push(&rest);
                }
                tail.push(&rest);
                log.lock().await.write_all(&rest).await?;
            }
            if let Some(plainer) = &mut plainer {
                let rest = plainer.finish();
                tail.push(&rest);
                log.lock().await.write_all(&rest).await?;
            }
            // A tokio file write may still be in flight when `write_all` returns: the log is
            // complete only once flushed.
            log.lock().await.flush().await?;
            return Ok(tail);
        }
        let masked;
        let mut bytes = match &mut stream {
            Some(stream) => {
                masked = stream.push(&chunk[..n]);
                masked.as_slice()
            }
            None => &chunk[..n],
        };
        let readable;
        if let Some(plainer) = &mut plainer {
            readable = plainer.push(bytes);
            bytes = readable.as_slice();
        }
        tail.push(bytes);
        log.lock().await.write_all(bytes).await?;
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
    pub async fn output(mut self) -> io::Result<(TailBuffer, TailBuffer)> {
        let (Some(stdout), Some(stderr)) = (self.stdout.take(), self.stderr.take()) else {
            return Err(io::Error::other("the output was taken already"));
        };
        let stdout = stdout.await.map_err(io::Error::other)??;
        let stderr = stderr.await.map_err(io::Error::other)??;
        Ok((stdout, stderr))
    }

    /// The end of a task whose leader exited: its output read for at most `drain`, then what
    /// is left of its process group killed (B274, the user's option B: nothing outlives its
    /// task, nor holds its slot by keeping its output open). What was read is kept.
    ///
    /// # Errors
    ///
    /// As [`Self::output`].
    pub async fn finish(
        mut self,
        drain: std::time::Duration,
    ) -> io::Result<(TailBuffer, TailBuffer)> {
        let (Some(stdout), Some(stderr)) = (self.stdout.take(), self.stderr.take()) else {
            return Err(io::Error::other("the output was taken already"));
        };
        finish_group(self.group, (stdout, stderr), &self.stop_reading, drain).await
    }
}

/// Both readers' tails, waiting at most `drain` for both to end at their pipe's end; then
/// `group`, what is left of the task, killed, and readers still waiting told to stop (a process
/// that left the group may still hold a pipe).
pub(crate) async fn finish_group(
    group: Pid,
    (mut stdout, mut stderr): (
        JoinHandle<io::Result<TailBuffer>>,
        JoinHandle<io::Result<TailBuffer>>,
    ),
    stop: &tokio::sync::watch::Sender<bool>,
    drain: std::time::Duration,
) -> io::Result<(TailBuffer, TailBuffer)> {
    // Each handle awaited to its end once: a result read within the drain is kept.
    let deadline = tokio::time::Instant::now() + drain;
    let out = tokio::time::timeout_at(deadline, &mut stdout).await.ok();
    let err = tokio::time::timeout_at(deadline, &mut stderr).await.ok();
    // Safe with the leader reaped: Linux never gives a new process an ID still in use as a
    // group's, so this is the task's group or none (`ESRCH`, ignored).
    let _ = kill_process_group(group, rustix::process::Signal::KILL);
    if out.is_none() || err.is_none() {
        stop.send_replace(true);
    }
    let out = match out {
        Some(out) => out,
        None => stdout.await,
    };
    let err = match err {
        Some(err) => err,
        None => stderr.await,
    };
    Ok((
        out.map_err(io::Error::other)??,
        err.map_err(io::Error::other)??,
    ))
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
    live_thread(process).map_or(true, |thread| thread.is_some())
}

/// A thread of the process at `/proc/<pid>` that hasn't ended, as `/proc/<pid>/task/<tid>`;
/// `None` if every one has.
///
/// # Errors
///
/// If its threads can't be listed.
pub(crate) fn live_thread(process: &Path) -> io::Result<Option<std::path::PathBuf>> {
    Ok(std::fs::read_dir(process.join("task"))?
        .flatten()
        .map(|thread| thread.path())
        .find(|thread| {
            std::fs::read_to_string(thread.join("stat"))
                .is_ok_and(|stat| stat_fields(&stat).is_some_and(|(state, _)| !ended(state)))
        }))
}

/// The state (field 3) and process group (field 5) of a `/proc/.../stat` line.
pub(crate) fn stat_fields(stat: &str) -> Option<(&str, i32)> {
    // The command name (field 2) is in parentheses and may contain anything: the fields
    // after it start at its closing parenthesis.
    let (_, rest) = stat.rsplit_once(')')?;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?;
    let pgrp = fields.nth(1)?.parse().ok()?;
    Some((state, pgrp))
}

/// Whether a state means ended: a zombie, or dead.
pub(crate) fn ended(state: &str) -> bool {
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
        self.reaped = true;
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
