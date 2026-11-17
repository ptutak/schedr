//! A `cleanup_anywhere` cleanup's barrier (B255, decision 504): its process, started through
//! `schedr task-exec --barrier`, waits before running anything; only once it waits does the
//! worker ask the master whether the cleanup is still this worker's, and a `g` lets it through.
//!
//! The process's own wait, well within the lease, starts before it says it waits, so before the
//! question leaves; the master can't move the cleanup until the worker is lost, a lease after
//! that question at the earliest. An answer late for any reason (the worker frozen, the stream
//! cut, a slow master) finds the process gone, never a cleanup that has moved meanwhile.
//!
//! The exchange on the barrier's socket: the process connects (it waits); the worker writes
//! `g` (go) or closes it (no); the process writes back `p` (passed: it runs now, the cleanup
//! started) or `l` (too late: its wait was over) and exits.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{oneshot, watch};

use crate::attempt::{NotRun, Plan};
use crate::process::ProcessSpec;

#[cfg(test)]
mod tests;

/// How a barrier's process waits before the master's lease is told: the default lease's half.
pub const UNTOLD_WAIT: Duration = Duration::from_secs(30);

/// Asks the master, through the agent, whether the attempt's cleanup (only the rest of its
/// handles, if said) may run here; the answer comes on the receiver, dropped when none can come
/// (no stream, a new one).
pub type Ask = Arc<dyn Fn(String, bool) -> oneshot::Receiver<bool> + Send + Sync>;

/// What a `cleanup_anywhere` cleanup's barrier asks with (B255, decision 504).
#[derive(Clone)]
pub struct CleanupAsk {
    /// Sends the question.
    pub ask: Ask,
    /// How long its process waits at the barrier: half the lease the master last told.
    pub wait: watch::Receiver<Duration>,
}

impl std::fmt::Debug for CleanupAsk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CleanupAsk")
            .field("wait", &*self.wait.borrow())
            .finish_non_exhaustive()
    }
}

/// A spec's equality is about what it runs, not whom it asks.
impl PartialEq for CleanupAsk {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/// How a try at the barrier went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Word {
    /// Let through: the cleanup started.
    Passed,
    /// The master said it isn't this worker's: it won't run here.
    Refused,
    /// The process waited, the master's word didn't come (or came too late): nothing ran, and
    /// the try isn't counted.
    Unanswered,
    /// The process never said it waits (it couldn't start): a failed try, counted.
    NeverWaited,
}

/// A barrier for one try: the plan to run, and the word, ready once the process has passed or
/// given up.
pub struct Barrier {
    /// The try's plan, started through the barrier.
    pub plan: Plan,
    /// How it went.
    pub word: tokio::task::JoinHandle<Word>,
}

/// `plan`'s process put behind a barrier: `exec` (`schedr` itself) waits at a socket of its own
/// and runs the program in its place once let through (a forked one waits itself). The question
/// goes out for `attempt_id` (only its `rest`, if said) only once the process waits.
///
/// # Errors
///
/// [`NotRun`] if the socket can't be made or the program's path isn't text.
pub fn put(
    attempt_id: &str,
    rest: bool,
    ask: &CleanupAsk,
    exec: &Path,
    plan: Plan,
) -> Result<Barrier, NotRun> {
    let wait = *ask.wait.borrow();
    let socket = socket_path().map_err(|error| not_run(&format!("no barrier socket: {error}")))?;
    let listener = tokio::net::UnixListener::bind(&socket)
        .map_err(|error| not_run(&format!("no barrier socket: {error}")))?;
    let args = vec![
        "task-exec".to_owned(),
        "--barrier".to_owned(),
        socket.display().to_string(),
        "--barrier-wait-ms".to_owned(),
        wait.as_millis().to_string(),
        "--".to_owned(),
    ];
    let barrier = schedr_model::runner::ForkBarrier {
        socket: socket.display().to_string(),
        wait_ms: u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
    };
    // Spawned, through `schedr task-exec`; forked, the process waits at it itself.
    let wrap = |mut process: ProcessSpec| -> Result<ProcessSpec, NotRun> {
        process.barrier = Some(barrier.clone());
        let program = std::mem::replace(&mut process.program, exec.to_path_buf());
        let program = program
            .into_os_string()
            .into_string()
            .map_err(|path| not_run(&format!("{} isn't UTF-8", Path::new(&path).display())))?;
        let mut all = args.clone();
        all.push(program);
        all.append(&mut process.args);
        process.args = all;
        Ok(process)
    };
    let plan = match plan {
        Plan::Python { process, request } => Plan::Python {
            process: wrap(process)?,
            request,
        },
        Plan::Command(process) => Plan::Command(wrap(process)?),
    };
    let (attempt_id, ask) = (attempt_id.to_owned(), Arc::clone(&ask.ask));
    let word = tokio::spawn(async move {
        let word = exchange(&listener, wait, || ask(attempt_id, rest)).await;
        let _ = std::fs::remove_file(&socket);
        word
    });
    Ok(Barrier { plan, word })
}

/// The worker's side of the exchange, `wait` each for the process to say it waits, for the
/// master's word and for the process's reply.
async fn exchange(
    listener: &tokio::net::UnixListener,
    wait: Duration,
    ask: impl FnOnce() -> oneshot::Receiver<bool>,
) -> Word {
    let Ok(Ok((mut stream, _))) = tokio::time::timeout(wait, listener.accept()).await else {
        return Word::NeverWaited;
    };
    match tokio::time::timeout(wait, ask()).await {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => return Word::Refused,
        _ => return Word::Unanswered,
    }
    if stream.write_all(b"g").await.is_err() {
        return Word::Unanswered;
    }
    let mut reply = [0_u8; 1];
    match tokio::time::timeout(wait, stream.read(&mut reply)).await {
        Ok(Ok(1)) if reply[0] == b'p' => Word::Passed,
        _ => Word::Unanswered,
    }
}

/// A fresh socket path in this process's own folder for them (only its user can reach it).
fn socket_path() -> std::io::Result<PathBuf> {
    static FOLDER: std::sync::OnceLock<Result<PathBuf, String>> = std::sync::OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let folder = FOLDER
        .get_or_init(|| {
            use std::os::unix::fs::DirBuilderExt as _;
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.subsec_nanos());
            let folder = std::env::temp_dir()
                .join(format!("schedr-barriers-{}-{nanos:x}", std::process::id()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&folder)
                .map(|()| folder)
                .map_err(|error| error.to_string())
        })
        .clone()
        .map_err(std::io::Error::other)?;
    Ok(folder.join(NEXT.fetch_add(1, Ordering::Relaxed).to_string()))
}

fn not_run(message: &str) -> NotRun {
    NotRun::Failed {
        kind: crate::agent::START_FAILED.to_owned(),
        message: message.to_owned(),
    }
}
