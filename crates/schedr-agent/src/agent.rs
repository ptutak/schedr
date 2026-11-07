//! The worker agent's main loop: one control stream to the master, attempts and parse jobs,
//! heartbeats and code watching (see PROJECT.md, Worker lifecycle and Task assignment).
//!
//! - **Connecting:** the agent waits for each root's first content hash (it settles too),
//!   registers with them and with the attempts it still runs or hasn't had acknowledged, then
//!   follows the master's decisions for those (continue, ack, kill).
//! - **Assignments:** a free slot, an assignment that reads and the task's code on this worker
//!   at the hash the master expects, or a rejection with the reason; then `Started`, the
//!   attempt runs, and `Finished` (naming the code it ran) is sent until the master
//!   acknowledges it.
//! - **Parse jobs** parse a root's `.py` files and upload the result, if the root still has the
//!   job's hash (otherwise a newer hash was or will be reported).
//! - **The connection drops:** attempts keep running, and the agent connects again with
//!   backoff (up to 30 s).
//! - **Shutdown** (SIGTERM): the agent drains (see [`crate::drain`]): it says `Draining` (again
//!   after any reconnect), rejects new assignments, gives the running attempts until the
//!   termination grace period ends, interrupts the rest, and deregisters once the master has
//!   their results. A second request interrupts at once.
//! - **The journal** on disk backs `Started` and `Finished` (see [`crate::journal`]): an
//!   attempt that can't be recorded is rejected, and on start the agent reports the results
//!   its previous incarnation had and stops the processes it left running. Without `/proc`
//!   it warns once and leaves them.
//!
//! The work itself goes through an [`Executor`]: processes in production
//! ([`ProcessExecutor`]), something simpler in tests.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use schedr_model::TaskKind;
use schedr_model::beat::Beat;
use schedr_model::connection::Registry;
use schedr_model::runner::{
    CleanupHandle, DAG_FILE_MISSING, ErrorSummary, ParseRequest, ParsedFile,
};
use schedr_model::task::{CleanupRun, TaskCleanup, default_outcomes};
use schedr_proto::PROTOCOL_VERSION;
use schedr_proto::convert::attempt_status_from_proto;
use schedr_proto::convert::{parse_upload_parts, parsed_file_to_proto};
use schedr_proto::pb::master_client::MasterClient;
use schedr_proto::pb::{self, master_message, worker_message};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;

use crate::attempt::{
    AttemptSpec, CleanupGate, Gate, NotRun, Pass, Plan, Ran, WorkerSetup, failed_before_start,
    finished, plan, plan_at, use_cleanup_script, with_resource_value,
};
use crate::connection_jobs::ConnectionJob;
use crate::ctx::CtxInfo;
use crate::drain::{Drain, DrainPeriods, DrainStep};
use crate::fork::ForkServers;
use crate::handles::{AttemptHandles, HandleSink};
use crate::journal::{
    Journal, JournalRecord, KeptAttempt, Orphan, ProcessIdentity, process_identity, stop_orphan,
};
use crate::launch::{AGENT_VERSION, ProcessParseRunner, PythonTask, run_command, run_python};
use crate::mask::Mask;
use crate::memory::{MemorySeen, SharedSeen, over_budget_message, watch_group};
use crate::metrics::AgentMetrics;
use crate::process::ProcessSpec;
use crate::resource::AttemptResource;
use crate::secrets::{AttemptSecrets, SecretStores};
use crate::status::StopReason;
use crate::supervise::{Limits, StopSender, Stops};
use crate::watch::{Poll, RootWatcher};

#[cfg(test)]
mod tests;

/// The error kind of an attempt whose process couldn't be started.
pub const START_FAILED: &str = "start_failed";

/// The longest wait between attempts to reach the master.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// How long a process left running by a previous incarnation has between SIGTERM and SIGKILL.
pub const ORPHAN_GRACE: Duration = Duration::from_secs(10);

/// The first wait after the master connection drops; it doubles up to [`MAX_BACKOFF`], and
/// starts again here after a session in which the master registered the worker.
pub const FIRST_BACKOFF: Duration = Duration::from_millis(200);

/// The longest wait between two tries to reach the master: `max`, but at most half the
/// master's worker lease once it has told it (B168), so a worker still alive reaches a master
/// back from a restart before that master's lease on it runs out; else the master would take
/// it for lost and move its cleanups with only the handles it got (a handle registered while
/// the master was away left behind).
#[must_use]
pub fn reconnect_cap(max: Duration, lease: Option<Duration>) -> Duration {
    lease.map_or(max, |lease| max.min(lease / 2))
}

/// A backoff with jitter: between half of `backoff` and all of it, picked by `seed` (so many
/// workers losing the master at once don't come back in lockstep).
#[must_use]
pub fn jittered(backoff: Duration, seed: u32) -> Duration {
    let fraction = 0.5 + 0.5 * f64::from(seed) / f64::from(u32::MAX);
    backoff.mul_f64(fraction)
}

/// The agent's configuration.
#[derive(Debug, Clone)]
pub struct AgentSettings {
    /// The worker's ID (stable across restarts).
    pub worker_id: String,
    /// The queues it serves (none: `default`).
    pub queues: Vec<String>,
    /// Its task slots.
    pub slots: u32,
    /// Its slots for wait steps (`[watchers] slots`).
    pub watcher_slots: u32,
    /// How tasks run here (roots, interpreter, environment, logs).
    pub setup: WorkerSetup,
    /// How often roots are scanned.
    pub poll_interval: Duration,
    /// How long a changed root must stay unchanged before it is hashed.
    pub settle_time: Duration,
    /// Where the journal is kept (see [`crate::journal`]).
    pub journal_dir: PathBuf,
    /// Where each root's code is copied, a folder per root (decision 102): parse jobs and
    /// tasks import from the copy of the hash they are for.
    pub code_dir: PathBuf,
    /// How long the running attempts get when the agent is asked to stop (the pod's
    /// `terminationGracePeriodSeconds`), and how the end of that time is split.
    pub drain: DrainPeriods,
    /// Where the webserver reaches this worker's log endpoint (e.g.
    /// `https://w-0.schedr-worker.etl.svc.cluster.local:8081`), sent in `Register`.
    pub log_endpoint: String,
    /// The worker as a node of its deployment, sent in `Register` for the Deployments page.
    pub node: NodeIdentity,
    /// Its secret storages (names sent in `Register`).
    pub secrets: Arc<SecretStores>,
}

/// A worker as a node of its deployment (decisions 121-123).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeIdentity {
    /// The process it runs in (shared with a master in standalone).
    pub process_id: String,
    /// Its effective configuration as JSON, masked (no secret in clear: it leaves the
    /// process).
    pub config_json: String,
}

/// Told the identity of an attempt's process once it is spawned, and whether it was forked
/// from a fork server: the task's, then each try of its cleanup's.
pub type Spawned = Arc<dyn Fn(ProcessIdentity, bool) + Send + Sync>;

/// Told a task's result when its process has ended and its cleanup starts (decision 229).
pub type TaskEnded = Box<dyn FnOnce(pb::Finished) + Send>;

/// What an attempt's processes share: the task's and each try of its cleanup.
#[derive(Clone, Copy)]
struct Shared<'a> {
    secrets: &'a Arc<AttemptSecrets>,
    handles: &'a Arc<AttemptHandles>,
    /// Set when a process starts.
    started: &'a AtomicBool,
    /// Told each process.
    spawned: &'a Spawned,
}

/// The copies of the other roots a parse sees (decision 102).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParseCode {
    /// The script roots, for a `ScriptOperator`'s declaration (decision 293).
    pub scripts: BTreeMap<String, crate::jobs::ScriptRoot>,
    /// The other DAG roots' copies, on `sys.path` in place of their configured paths.
    pub roots: BTreeMap<String, PathBuf>,
}

/// Runs attempts and parse jobs.
pub trait Executor: Clone + Send + Sync + 'static {
    /// Runs a planned attempt until it ends or `stop` says why to stop it (a later reason
    /// may hurry a stop under way, see [`crate::supervise::supervise`]), telling `spawned` its
    /// process, and `ended` the task's result before its cleanup starts; returns its
    /// `Finished`.
    fn run(
        &self,
        spec: AttemptSpec,
        plan: Plan,
        stop: Stops,
        spawned: Spawned,
        ended: TaskEnded,
    ) -> impl Future<Output = pb::Finished> + Send;

    /// Runs the cleanup of an attempt an earlier agent left, whose task ended as `ended`, with
    /// the `handles` the journal kept; returns `ended` with the cleanup's result.
    fn cleanup(
        &self,
        spec: AttemptSpec,
        ended: pb::Finished,
        handles: Vec<CleanupHandle>,
        spawned: Spawned,
    ) -> impl Future<Output = pb::Finished> + Send;

    /// What is wrong with the worker now, as it changes (sent to the master); none by default.
    fn warnings(&self) -> watch::Receiver<Vec<pb::WorkerWarning>> {
        watch::channel(Vec::new()).1
    }

    /// The worker's newest copy of each DAG root: what runs may move to it.
    fn code_changed(&self, _copies: BTreeMap<String, PathBuf>) {}

    /// Folders of code copies in use besides the attempts' own.
    fn code_in_use(&self) -> Vec<String> {
        Vec::new()
    }

    /// The wait step hosts as last looked at, for the heartbeat: it can't wait.
    fn watcher_hosts(&self) -> Vec<pb::WatcherHost> {
        Vec::new()
    }

    /// Parses `files` of the root `root` in `dir`, with the other roots' copies `code`.
    fn parse(
        &self,
        root: String,
        dir: PathBuf,
        files: Vec<String>,
        code: ParseCode,
    ) -> impl Future<Output = Result<Vec<ParsedFile>, String>> + Send;

    /// Checks a root's connections, or tests one (`schedr connections check` / `test`); a
    /// report for each. An executor without secrets says it can't.
    /// A test tells `spawned` its process, as an attempt does (the journal records it, so
    /// a worker killed meanwhile stops it when it starts again).
    fn connection_job(
        &self,
        job: ConnectionJob,
        spawned: Spawned,
    ) -> impl Future<Output = Vec<schedr_model::commands::ConnectionReport>> + Send {
        drop(spawned);
        async move {
            job.registry
                .connections
                .keys()
                .chain(job.registry.refused.keys())
                .map(|name| {
                    crate::connection_jobs::failed(
                        name,
                        "this worker can't check connections".to_owned(),
                    )
                })
                .collect()
        }
    }
}

/// Warned once when `/proc` is missing, so a crash leaves task processes running.
pub(crate) const NO_PROC_WARNING: &str = "no /proc: orphaned tasks aren't stopped after a crash";

/// Set once [`NO_PROC_WARNING`] has been logged in this process.
static NO_PROC_SAID: AtomicBool = AtomicBool::new(false);

/// How to report that a task's process couldn't be identified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentityWarning {
    /// `/proc` is not a directory. Logged once per process, as [`NO_PROC_WARNING`].
    NoProc,
    /// Any other failure. Logged for this task.
    ThisTask,
}

/// What to log when [`process_identity`] failed.
///
/// `proc_present` is whether `/proc` is a directory. When it isn't, the failure is that
/// `/proc` is absent (macOS): [`IdentityWarning::NoProc`] the first time (`said` flips), then
/// nothing. When it is, every failure is [`IdentityWarning::ThisTask`] — a process that has
/// already exited is `NotFound` too, and that still warns per task. `said` is the
/// process-wide flag in the agent; a test passes its own.
#[must_use]
pub(crate) fn identity_warning(proc_present: bool, said: &AtomicBool) -> Option<IdentityWarning> {
    if proc_present {
        Some(IdentityWarning::ThisTask)
    } else if said.swap(true, Ordering::Relaxed) {
        None
    } else {
        Some(IdentityWarning::NoProc)
    }
}

/// Logs [`identity_warning`] for this process (`/proc` as it is on this machine).
fn warn_unidentified(pid: i32, error: &std::io::Error) {
    match identity_warning(Path::new("/proc").is_dir(), &NO_PROC_SAID) {
        Some(IdentityWarning::NoProc) => tracing::warn!("{}", NO_PROC_WARNING),
        Some(IdentityWarning::ThisTask) => {
            tracing::warn!(pid, %error, "can't identify the task's process");
        }
        None => {}
    }
}

/// Runs attempts and parse jobs as processes.
#[derive(Debug, Clone)]
pub struct ProcessExecutor {
    /// How tasks run here.
    pub setup: WorkerSetup,
    /// How long one file may take to import, and how many are parsed at once.
    pub parse: crate::parse::ParseLimits,
    /// Where the watchdog's kills are counted.
    pub metrics: AgentMetrics,
    /// From an interrupted task's SIGTERM to its SIGKILL (`[worker] drain_terminate_period`).
    pub drain_terminate_period: Duration,
    /// The DAG interpreter's fork servers, when it has them (`fork_server = true`, Linux).
    pub forks: Option<ForkServers>,
    /// The hosts running this worker's wait steps.
    pub watchers: crate::watchers::WatcherHosts,
    /// The worker's secret storages.
    pub secrets: Arc<SecretStores>,
}

impl Executor for ProcessExecutor {
    fn code_changed(&self, copies: BTreeMap<String, PathBuf>) {
        self.watchers.code_changed(&copies);
    }

    fn code_in_use(&self) -> Vec<String> {
        self.watchers.code_in_use()
    }

    fn watcher_hosts(&self) -> Vec<pb::WatcherHost> {
        self.watchers.reports()
    }

    fn warnings(&self) -> watch::Receiver<Vec<pb::WorkerWarning>> {
        self.forks
            .as_ref()
            .map_or_else(|| watch::channel(Vec::new()).1, ForkServers::warnings)
    }

    async fn run(
        &self,
        spec: AttemptSpec,
        plan: Plan,
        stop: Stops,
        spawned: Spawned,
        ended: TaskEnded,
    ) -> pb::Finished {
        let connection_dir = self.setup.connection_dir.join(&spec.attempt_id);
        let has_cleanup = spec.task.cleanup.is_some();
        // Boxed: the attempt's future is large, and every caller would hold it.
        let mut finished = Box::pin(self.run_attempt(spec, plan, stop, spawned, ended)).await;
        // Its process never started: nothing to clean up after (decision 216).
        if has_cleanup && finished.cleanup.is_none() {
            finished.cleanup = Some(pb::CleanupResult {
                status: pb::CleanupStatus::NotNeeded.into(),
                ..pb::CleanupResult::default()
            });
        }
        // Its connections' files go with it (decision 190): whichever way it ended.
        remove_connection_files(&connection_dir).await;
        finished
    }

    async fn cleanup(
        &self,
        mut spec: AttemptSpec,
        mut ended: pb::Finished,
        handles: Vec<CleanupHandle>,
        spawned: Spawned,
    ) -> pb::Finished {
        let connection_dir = self.setup.connection_dir.join(&spec.attempt_id);
        let result = Box::pin(self.kept_cleanup(&mut spec, handles, &spawned)).await;
        ended.cleanup = Some(result);
        remove_connection_files(&connection_dir).await;
        ended
    }

    async fn connection_job(
        &self,
        job: ConnectionJob,
        spawned: Spawned,
    ) -> Vec<schedr_model::commands::ConnectionReport> {
        use crate::connection_jobs::{failed, out_of_time};
        // Read fresh: a secret just fixed is what the check or test sees.
        let secrets = Arc::new(self.secrets.uncached());
        let Some((name, _)) = job.test.clone() else {
            let check = crate::connection_jobs::check(&secrets, &job.registry);
            return match tokio::time::timeout_at(job.deadline, check).await {
                Ok(reports) => reports,
                Err(_) => job
                    .registry
                    .names()
                    .map(|name| failed(name, out_of_time("check")))
                    .collect(),
            };
        };
        // What is left of its time once its turn came; all of it, secrets read included.
        let left = job
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        let mut spec =
            match crate::connection_jobs::test_spec(&job, &name, left, job.attempt_id.clone()) {
                Ok(spec) => spec,
                Err(why) => return vec![failed(&name, why)],
            };
        spec.stand_in_secrets();
        // At the copy of the code the job was sent for, as a task's (decision 102).
        let planned = match &spec.code_dir {
            Some(dir) => plan_at(&spec, &self.setup, dir),
            None => crate::attempt::plan(&spec, &self.setup),
        };
        let plan = match planned {
            Ok(plan) => plan,
            Err(NotRun::Rejected { detail, .. }) => return vec![failed(&name, detail)],
            Err(NotRun::Failed { message, .. }) => return vec![failed(&name, message)],
        };
        let started = Instant::now();
        let log = self.setup.log_file(&spec.attempt_id);
        let (stop, stops) = Stops::new();
        let fresh = Self {
            secrets,
            ..self.clone()
        };
        let run = fresh.run(spec, plan, stops, spawned, Box::new(|_| {}));
        tokio::pin!(run);
        // The job's deadline also covers planning and waiting for its turn; the attempt's
        // timeout covers its secret reads and process together.
        let finished = tokio::select! {
            finished = &mut run => finished,
            () = tokio::time::sleep_until(job.deadline) => {
                let _ = stop.send(StopReason::TimedOut);
                run.await
            }
        };
        // No task's: nothing serves it.
        let _ = tokio::fs::remove_file(&log).await;
        vec![crate::connection_jobs::test_report(
            &name,
            &finished,
            started.elapsed(),
        )]
    }

    async fn parse(
        &self,
        root: String,
        dir: PathBuf,
        files: Vec<String>,
        code: ParseCode,
    ) -> Result<Vec<ParsedFile>, String> {
        self.parse_files(root, dir, files, code).await
    }
}

impl ProcessExecutor {
    async fn run_attempt(
        &self,
        mut spec: AttemptSpec,
        mut plan: Plan,
        stop: Stops,
        spawned: Spawned,
        ended: TaskEnded,
    ) -> pb::Finished {
        let fetching = Instant::now();
        let _ = tokio::fs::create_dir_all(&self.setup.log_dir).await;
        let timeout = spec.timeout();
        let mut stop = stop;
        // A slow storage gives way to a cancel, a drain's interrupt or the timeout: nothing
        // has started, so nothing needs stopping.
        let fetched = tokio::select! {
            fetched = self.fetch_secrets(&mut spec, &mut plan) => Ok(fetched),
            reason = stop.next_reason() => Err(reason),
            () = sleep_for(timeout) => Err(StopReason::TimedOut),
        };
        let secrets = match fetched {
            Ok(Ok(secrets)) => secrets,
            Ok(Err(finished)) => return *finished,
            Err(reason) => return stopped_before_start(&spec, reason, fetching.elapsed()),
        };
        let elapsed = fetching.elapsed();
        let timeout = timeout.map(|timeout| timeout.saturating_sub(elapsed));
        // Fetching may finish in the same poll as its deadline: do not start a process
        // with no time left, whichever branch of the select won.
        if timeout.is_some_and(|left| left.is_zero()) {
            return stopped_before_start(&spec, StopReason::TimedOut, elapsed);
        }
        let python = matches!(plan, Plan::Python { .. });
        let mut handles =
            AttemptHandles::new(spec.task.cleanup.as_ref(), Arc::clone(&secrets.mask));
        if let Some(journal) = &spec.journal {
            handles = handles.journaled(journal.clone(), spec.attempt_id.clone());
        }
        if let Some(sink) = &spec.handle_sink {
            handles = handles.forwarded(sink.clone());
        }
        let handles = Arc::new(handles);
        let started = AtomicBool::new(false);
        let shared = Shared {
            secrets: &secrets,
            handles: &handles,
            started: &started,
            spawned: &spawned,
        };
        let mut finished = self.run_planned(&spec, plan, timeout, stop, shared).await;
        ended_with_secrets(&mut finished, &secrets, python);
        if let Some(cleanup) = &spec.task.cleanup
            && started.load(Ordering::SeqCst)
        {
            ended(finished.clone());
            // Dropped, the attempt is no longer this worker's: what it reports here is ignored.
            let pass = match spec.cleanup_gate.clone() {
                Some(gate) => gate.passes().await,
                None => Some(Pass::All),
            };
            if let Some(pass) = pass {
                if let Pass::From(from) = pass {
                    handles.skip(from);
                }
                let mut result = self.run_cleanup(&spec, cleanup, shared).await;
                result.rest = matches!(pass, Pass::From(_));
                finished.cleanup = Some(result);
            }
        }
        let log = self.setup.log_file(&spec.attempt_id);
        // Read on demand while it ran: too short to mask, warned now.
        let short = too_short_lines(&secrets, self.secrets.min_mask_length);
        append_log(&log, &(short + &masked_lines(&secrets))).await;
        finished
    }

    /// Fetches the secrets the attempt needs, then plans it again with their values (it was
    /// planned with stand-ins). A failure ends it: nothing runs with a missing secret.
    async fn fetch_secrets(
        &self,
        spec: &mut AttemptSpec,
        plan: &mut Plan,
    ) -> Result<Arc<AttemptSecrets>, Box<pb::Finished>> {
        let needed = spec.needed_secrets();
        let fail = |kind: &str, message: &str| {
            Box::new(failed_before_start(&spec.attempt_id, kind, message))
        };
        let secrets = AttemptSecrets::fetch(
            Arc::clone(&self.secrets),
            &needed,
            spec.task.secrets.clone(),
        )
        .await
        .map_err(|e| fail(e.kind(), &e.to_string()))?
        .with_connections(Arc::clone(&spec.connections), spec.task.connections.clone())
        .with_connection_dir(self.setup.connection_dir.join(&spec.attempt_id));
        secrets
            .open_declared()
            .await
            .map_err(|e| fail(e.kind(), &e.to_string()))?;
        if needed.is_empty() && spec.task.secrets.is_empty() && !secrets.has_connections() {
            // Nothing to fetch or mask; kept to remember a read refused as undeclared, the
            // attempt's error kind if it then fails (decision 168).
            return Ok(Arc::new(secrets));
        }
        if let TaskKind::Command { connection_env, .. } = &spec.task.kind {
            spec.connection_env = command_connections(&secrets, connection_env)
                .await
                .map_err(|(kind, message)| fail(kind, &message))?;
        }
        spec.secrets = secrets.fetched();
        spec.mask = Mask(Some(Arc::clone(&secrets.mask)));
        let planned = match &spec.code_dir {
            Some(dir) => plan_at(spec, &self.setup, dir),
            None => crate::attempt::plan(spec, &self.setup),
        };
        *plan = planned.map_err(|not_run| match not_run {
            NotRun::Failed { kind, message } => fail(&kind, &message),
            // Started already: what can't run now fails.
            NotRun::Rejected { detail, .. } => fail(DAG_FILE_MISSING, &detail),
        })?;
        let short = too_short_lines(&secrets, self.secrets.min_mask_length);
        append_log(&self.setup.log_file(&spec.attempt_id), &short).await;
        Ok(Arc::new(secrets))
    }

    /// The cleanup of an attempt an earlier agent left: with its kept code (a missing copy is
    /// a failed cleanup) and its secrets fetched again.
    async fn kept_cleanup(
        &self,
        spec: &mut AttemptSpec,
        handles: Vec<CleanupHandle>,
        spawned: &Spawned,
    ) -> pb::CleanupResult {
        let failed = |kind: &str, summary: Vec<u8>| pb::CleanupResult {
            status: pb::CleanupStatus::Failed.into(),
            error_kind: kind.to_owned(),
            error_summary_json: summary,
            ..pb::CleanupResult::default()
        };
        let Some(cleanup) = spec.task.cleanup.clone() else {
            return pb::CleanupResult {
                status: pb::CleanupStatus::NotNeeded.into(),
                ..pb::CleanupResult::default()
            };
        };
        // Only the cleanup runs: what the task itself needed doesn't matter here.
        use_cleanup_script(&mut spec.task);
        if let Some(dir) = [&spec.code_dir, &spec.script_dir]
            .into_iter()
            .flatten()
            .find(|dir| !dir.exists())
        {
            let message = format!("the attempt's code is gone from `{}`", dir.display());
            let summary = ErrorSummary::json("CodeGone", &message).into_bytes();
            return failed(DAG_FILE_MISSING, summary);
        }
        let _ = tokio::fs::create_dir_all(&self.setup.log_dir).await;
        spec.stand_in_secrets();
        let planned = match &spec.code_dir {
            Some(dir) => plan_at(spec, &self.setup, dir),
            None => crate::attempt::plan(spec, &self.setup),
        };
        let mut plan = match planned {
            Ok(plan) => plan,
            Err(NotRun::Failed { kind, message }) => {
                return failed(
                    &kind,
                    ErrorSummary::json("CleanupNotRun", &message).into_bytes(),
                );
            }
            Err(NotRun::Rejected { detail, .. }) => {
                let summary = ErrorSummary::json("CleanupNotRun", &detail).into_bytes();
                return failed(DAG_FILE_MISSING, summary);
            }
        };
        // A slow storage can't hold the slot for ever: this is the cleanup's time.
        let timeout = cleanup
            .timeout_ms
            .map_or(self.setup.timeouts.cleanup, Duration::from_millis);
        let secrets = match tokio::time::timeout(timeout, self.fetch_secrets(spec, &mut plan)).await
        {
            Ok(Ok(secrets)) => secrets,
            Ok(Err(finished)) => return failed(&finished.error_kind, finished.error_summary_json),
            Err(_) => {
                let message = "the cleanup's secrets weren't read within its timeout";
                let summary = ErrorSummary::json("CleanupNotRun", message).into_bytes();
                return failed("timed_out", summary);
            }
        };
        let handles = Arc::new(AttemptHandles::restored(handles, Arc::clone(&secrets.mask)));
        let started = AtomicBool::new(true);
        let shared = Shared {
            secrets: &secrets,
            handles: &handles,
            started: &started,
            spawned,
        };
        self.run_cleanup(spec, &cleanup, shared).await
    }

    /// Runs the attempt's cleanup, in a fresh process each try, with the same budget and
    /// watchdog as the task. Nothing stops it but its own timeout: a cancel doesn't (decision
    /// 219). The slot stays taken through its tries and delays.
    async fn run_cleanup(
        &self,
        spec: &AttemptSpec,
        cleanup: &TaskCleanup,
        shared: Shared<'_>,
    ) -> pb::CleanupResult {
        shared.handles.close();
        let mut spec = spec.clone();
        spec.task.outcomes = default_outcomes();
        match (&mut spec.task.kind, &cleanup.run) {
            (
                TaskKind::Command {
                    command, resource, ..
                },
                CleanupRun::Command(line),
            ) => {
                command.clone_from(line);
                // A setup's own cleanup (for a setup failing halfway) reports no value.
                *resource = None;
            }
            (TaskKind::Script { .. }, CleanupRun::Script { .. }) => {
                use_cleanup_script(&mut spec.task);
            }
            (TaskKind::Python { .. }, CleanupRun::Python) => {
                spec.cleanup_handles = Some(shared.handles.list());
            }
            // Refused when the DAG is stored; run as it is, it would run the task again.
            _ => {
                let message = "the task's cleanup doesn't match its kind";
                return pb::CleanupResult {
                    status: pb::CleanupStatus::Failed.into(),
                    error_kind: "invalid_cleanup".to_owned(),
                    error_summary_json: ErrorSummary::json("InvalidCleanup", message).into_bytes(),
                    ..pb::CleanupResult::default()
                };
            }
        }
        let log = self.setup.log_file(&spec.attempt_id);
        let timeout = cleanup
            .timeout_ms
            .map_or(self.setup.timeouts.cleanup, Duration::from_millis);
        let mut result = pb::CleanupResult::default();
        for try_number in 1..=cleanup.retries.saturating_add(1) {
            if try_number > 1 {
                tokio::time::sleep(Duration::from_millis(cleanup.retry_delay_ms)).await;
            }
            append_log(&log, &format!("cleanup: try {try_number}\n")).await;
            let planned = match &spec.code_dir {
                Some(dir) => plan_at(&spec, &self.setup, dir),
                None => crate::attempt::plan(&spec, &self.setup),
            };
            let (_sender, stops) = Stops::new();
            let mut ended = match planned {
                Ok(plan) => {
                    let python = matches!(plan, Plan::Python { .. });
                    let mut ended = self
                        .run_planned(&spec, plan, Some(timeout), stops, shared)
                        .await;
                    ended_with_secrets(&mut ended, shared.secrets, python);
                    ended
                }
                Err(NotRun::Failed { kind, message }) => {
                    failed_before_start(&spec.attempt_id, &kind, &message)
                }
                Err(NotRun::Rejected { detail, .. }) => {
                    failed_before_start(&spec.attempt_id, DAG_FILE_MISSING, &detail)
                }
            };
            result.tries = try_number;
            result.duration_ms += ended.duration_ms.unwrap_or(0);
            if ended.status == i32::from(pb::AttemptStatus::Success) {
                result.status = pb::CleanupStatus::Ok.into();
                result.error_kind.clear();
                result.error_summary_json.clear();
                return result;
            }
            result.status = pb::CleanupStatus::Failed.into();
            result.error_kind = std::mem::take(&mut ended.error_kind);
            result.error_summary_json = std::mem::take(&mut ended.error_summary_json);
        }
        result
    }

    async fn run_planned(
        &self,
        spec: &AttemptSpec,
        plan: Plan,
        timeout: Option<Duration>,
        stop: Stops,
        shared: Shared<'_>,
    ) -> pb::Finished {
        let Shared {
            secrets,
            handles,
            started,
            spawned,
        } = shared;
        let limits = Limits {
            timeout,
            grace: spec.cancel_grace,
            interrupt_grace: self.drain_terminate_period,
            poll: self.setup.timeouts.stop_poll,
        };
        let seen = SharedSeen::default();
        // The watchdog's kill is one more reason to stop, in the same channel.
        let oom = stop.sender();
        // Declared before the watch, so it is dropped after it on any path (locals go in
        // reverse order), a dropped attempt included.
        let mut hold = None;
        let mut watching = None;
        let shared_spawned = spawned;
        let spawned = |pid, forked| {
            started.store(true, Ordering::SeqCst);
            // The task's process leads its own group; watched from its first moment.
            watching = Some(watch_group(
                self.setup.memory.clone(),
                pid,
                seen.clone(),
                oom,
                self.metrics.watchdog_kills.clone(),
                forked,
            ));
            match process_identity(pid) {
                Ok(identity) => shared_spawned(identity, forked),
                Err(error) => warn_unidentified(pid, &error),
            }
        };
        let resource =
            AttemptResource::of(&spec.task, &spec.inputs, Arc::clone(&secrets.mask)).map(Arc::new);
        let ran = match plan {
            // In a process shared with other wait steps: no budget of its own (the host's).
            Plan::Python { process, request }
                if matches!(request.task.kind, schedr_model::TaskKind::Sensor { .. }) =>
            {
                let task = PythonTask {
                    request: *request,
                    secrets: Some(Arc::clone(secrets)),
                    handles: Some(Arc::clone(handles)),
                };
                crate::watchers::run_watcher(
                    &self.watchers,
                    &self.setup,
                    &process,
                    task,
                    limits,
                    stop,
                    |pid| {
                        started.store(true, Ordering::SeqCst);
                        match process_identity(pid) {
                            Ok(identity) => shared_spawned(identity, false),
                            Err(error) => warn_unidentified(pid, &error),
                        }
                    },
                )
                .await
            }
            Plan::Python { process, request } => {
                let forks = self.forks.as_ref();
                let task = PythonTask {
                    request: *request,
                    secrets: Some(Arc::clone(secrets)),
                    handles: Some(Arc::clone(handles)),
                };
                run_python(&process, task, limits, stop, forks, &mut hold, spawned).await
            }
            Plan::Command(process) => {
                let mut ctx = CtxInfo::of(&spec.run, AGENT_VERSION);
                ctx.secrets = Some(Arc::clone(secrets));
                ctx.handles = Some(Arc::clone(handles));
                ctx.resource.clone_from(&resource);
                run_command(&process, Some(ctx), limits, stop, |pid| spawned(pid, false)).await
            }
        };
        // The watchdog stops first; then a forked process's hold goes, and its shim reaps it:
        // nothing signals its group after that.
        drop(watching);
        drop(hold);
        if let Ok(Ran {
            start: Some(start),
            forked,
            ..
        }) = &ran
        {
            self.metrics
                .task_started(if *forked { "fork" } else { "spawn" }, start);
        }
        let mut finished = match ran {
            Ok(ran) => finished(&spec.attempt_id, &spec.task, &ran),
            Err(error) => failed_before_start(&spec.attempt_id, START_FAILED, &error.to_string()),
        };
        if let Some(outcome) = resource.as_ref().and_then(|r| r.outcome()) {
            with_resource_value(&mut finished, outcome);
        }
        let seen = *seen.lock().unwrap_or_else(PoisonError::into_inner);
        with_memory(&mut finished, seen, self.setup.memory.budget);
        finished
    }

    async fn parse_files(
        &self,
        root: String,
        dir: PathBuf,
        files: Vec<String>,
        code: ParseCode,
    ) -> Result<Vec<ParsedFile>, String> {
        // A library in the root is shipped and hashed, never loaded here (decision 305).
        let searched = dir.clone();
        let files =
            tokio::task::spawn_blocking(move || crate::scan::split_dag_files(&searched, files).0)
                .await
                .map_err(|e| e.to_string())?;
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let _ = tokio::fs::create_dir_all(&self.setup.log_dir).await;
        let runner = ProcessParseRunner {
            process: ProcessSpec {
                program: self.setup.python.clone(),
                args: vec![
                    // Not the working directory first: a root's file can't shadow what the runner imports (B178).
                    "-P".to_owned(),
                    "-m".to_owned(),
                    "schedr.runner".to_owned(),
                    "parse".to_owned(),
                ],
                env: self
                    .setup
                    .agent_env
                    .iter()
                    // As a task's (`PYTHONPATH` changes what imports find).
                    .filter(|(name, _)| {
                        crate::env::is_allowed(name) || self.setup.pass_env.contains(name)
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                working_dir: dir.clone(),
                log_file: self.setup.log_dir.join(format!("parse-{root}.log")),
                max_output_size: 64 * 1024,
                mask: crate::mask::Mask::default(),
                start_timeout: self.setup.timeouts.runner_start,
                output_drain: self.setup.timeouts.output_drain,
            },
            memory: self.setup.memory.clone(),
            kills: self.metrics.watchdog_kills.clone(),
            forks: self.forks.clone(),
        };
        let mut copies = code.roots;
        copies.insert(root.clone(), dir.clone());
        let request = ParseRequest {
            sys_path: self.setup.sys_path_at(&copies),
            root,
            root_dir: dir.to_string_lossy().into_owned(),
            files,
        };
        crate::parse::parse_files_with(&runner, request, self.parse, &code.scripts)
            .await
            .map_err(|e| e.to_string())
    }
}

/// The code an attempt started on, unless it says itself which it ended on: a moved poke
/// (decision 335).
fn with_code_hash(finished: &mut pb::Finished, started_on: String) {
    if finished.code_hash.is_empty() {
        finished.code_hash = started_on;
    }
}

/// Adds what the watchdog saw to an attempt's `Finished`: its peak memory, and for one killed
/// over its budget the error summary saying so.
pub fn with_memory(finished: &mut pb::Finished, seen: MemorySeen, budget: Option<u64>) {
    finished.peak_memory_bytes = seen.peak;
    if let (Some(used), Some(budget)) = (seen.over, budget)
        && finished.status == i32::from(pb::AttemptStatus::OomKilled)
    {
        finished.error_summary_json =
            ErrorSummary::json(MEMORY_EXCEPTION, &over_budget_message(budget, used)).into_bytes();
    }
}

/// How often the agent's loop beats at least (liveness, see [`Agent::beat`]).
pub const ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// The most heartbeats between two sends of an unacknowledged result: the gap doubles from
/// one (B41), so a long stall of the master queues a few copies of each result, not one a
/// heartbeat.
pub const MAX_RESEND_GAP: u32 = 32;

/// How many times a result is sent again over one session before the agent gives the session
/// up and reconnects: its registration's journal then settles the result (acknowledged, or
/// sent again on `Continue`), and no session queues more copies of a result than this.
pub const MAX_RESENDS: u32 = 8;

/// The exception named in the error summary of an attempt killed over its memory budget.
pub const MEMORY_EXCEPTION: &str = "MemoryBudgetExceeded";

/// Why a session with the master ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEnd {
    /// The agent is stopping.
    Stopped,
    /// The connection failed or the master ended it; connect again.
    Lost(String),
}

/// Something happened in the background.
enum Done {
    Spawned {
        attempt_id: String,
        identity: ProcessIdentity,
        forked: bool,
    },
    /// A task with a cleanup: its process ended and the cleanup starts.
    TaskEnded(pb::Finished),
    /// A handle was journaled: on to the master.
    Handle {
        attempt_id: String,
        position: u32,
        handle: CleanupHandle,
    },
    Attempt(pb::Finished),
    Parse {
        job_id: String,
        root: pb::RootHash,
        /// The content hashes of the copies it used (kept meanwhile).
        copies: Vec<String>,
        files: Result<Vec<ParsedFile>, String>,
    },
    Connections {
        job_id: String,
        /// The hash whose copy of the code it held.
        hash: Option<String>,
        /// A test's attempt, recorded in the journal while its process ran.
        attempt_id: Option<String>,
        reports: Vec<schedr_model::commands::ConnectionReport>,
    },
}

/// An attempt an earlier agent left, its cleanup still to run.
#[derive(Debug)]
struct WaitingCleanup {
    spec: AttemptSpec,
    handles: Vec<CleanupHandle>,
    /// The task's result; `None`: it was running, and is `lost`.
    ended: Option<pb::Finished>,
    /// Only the handles the master didn't have (`CLEAN_REST`, B169): its result says so.
    rest: bool,
}

/// How many connections jobs' answers the agent keeps while no master listens.
const UNSENT_ANSWERS: usize = 16;

/// The worker agent.
#[derive(Debug)]
pub struct Agent<E> {
    settings: AgentSettings,
    executor: E,
    incarnation: String,
    journal: Journal,
    watchers: BTreeMap<String, RootWatcher>,
    /// The attempts running, with the way to stop each (taken once it is asked to stop).
    running: BTreeMap<String, StopSender>,
    /// Those of them that are wait steps, on watcher slots: whether each is a poke.
    wait_steps: BTreeMap<String, bool>,
    /// The attempts in their cleanup, with the task's result (sent again on a reconnect).
    cleaning: BTreeMap<String, pb::Finished>,
    /// Attempts an earlier agent left, whose cleanups wait for the master's `Continue`
    /// (decision 248); in `running` too, for their slots.
    waiting: BTreeMap<String, WaitingCleanup>,
    /// What each running task's cleanup needs of a drain (one try's timeout).
    cleanup_room: BTreeMap<String, Duration>,
    /// The attempts a drain has stopped.
    stopped: std::collections::BTreeSet<String>,
    /// The gate of each running attempt with `cleanup_anywhere`.
    gates: BTreeMap<String, watch::Sender<Gate>>,
    /// The attempts whose cleanup runs on the handles the master didn't have (B169).
    rest_cleaning: BTreeSet<String>,
    /// When the master was last heard from.
    heard: tokio::time::Instant,
    /// `heard` plus the lease, for the cleanup gates (see [`Self::publish_deadline`]).
    deadline: watch::Sender<Option<tokio::time::Instant>>,
    /// The master's worker lease, once it has told it.
    lease: Option<Duration>,
    /// The tasks with `cleanup_anywhere` were stopped; till the next registration.
    cut_off: bool,
    /// The tasks warned of a `cleanup_timeout` that doesn't fit the grace period (decision 250).
    warned_room: std::collections::BTreeSet<(String, String)>,
    unacked: BTreeMap<String, pb::Finished>,
    /// Connections jobs' answers made while no master listened, sent once one does (the
    /// newest [`UNSENT_ANSWERS`]; the master calls an unanswered job lost in time anyway).
    unsent_answers: std::collections::VecDeque<pb::ConnectionJobDone>,
    /// The content hash of each started attempt's root when it started (sent in `Finished`).
    code_hashes: BTreeMap<String, String>,
    /// The content hashes of the other roots' copies a task runs from (its script root, the
    /// other DAG roots on its `sys.path`), while it runs (kept meanwhile).
    other_copies: BTreeMap<String, Vec<String>>,
    /// The script roots each DAG root uses, as the master last said (decision 304).
    uses: BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Each root's hash as last reported to the master (decisions 301, 303; B167); a root
    /// withdrawn, or never reported, isn't in it.
    reported: BTreeMap<String, String>,
    /// The parse jobs running, by the hash they parse (their copy is kept meanwhile).
    parsing: BTreeMap<String, usize>,
    /// Held by the connections job running (decision 199: one at a time).
    connection_jobs: Arc<tokio::sync::Mutex<()>>,
    done_tx: mpsc::UnboundedSender<Done>,
    done_rx: mpsc::UnboundedReceiver<Done>,
    /// What is wrong with the worker now (the executor's), sent after `Register` and on change.
    warnings: watch::Receiver<Vec<pb::WorkerWarning>>,
    heartbeat: Duration,
    /// Whether the master registered this agent in the current session.
    registered: bool,
    /// The largest message the master accepts (from `Registered`; gRPC's default before).
    master_limit: usize,
    /// Shutdown requests so far (none in a session run without [`Agent::run`]).
    shutdown: Option<watch::Receiver<u32>>,
    /// The drain, once shutdown was asked for.
    drain: Option<Drain>,
    /// When the agent's event loop last turned (the worker's liveness).
    beat: Beat,
    metrics: AgentMetrics,
}

/// A command's connections given as variables (`connections={"DB": warehouse}`, decision
/// 184): their files written, their variables merged; two giving one variable are refused.
async fn command_connections(
    secrets: &AttemptSecrets,
    given: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, (&'static str, String)> {
    let mut variables: BTreeMap<String, String> = BTreeMap::new();
    let mut from: BTreeMap<String, &str> = BTreeMap::new();
    for (prefix, name) in given {
        let more = secrets
            .connection_env(name, prefix)
            .await
            .map_err(|e| (e.kind(), e.to_string()))?;
        for (variable, value) in more {
            if let Some(other) = from.insert(variable.clone(), prefix) {
                return Err((
                    crate::attempt::RENDER_FAILED,
                    format!(
                        "`{variable}` is a variable of the connections given as {other} and as \
                         {prefix}: use prefixes that don't start one another"
                    ),
                ));
            }
            variables.insert(variable, value);
        }
    }
    Ok(variables)
}

/// Removes an attempt's connection files; a failure is logged (the next start sweeps them).
async fn remove_connection_files(dir: &Path) {
    match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(dir = %dir.display(), %error, "can't remove an attempt's connection files");
        }
    }
}

/// Removes the connection files the previous agent's attempts left (decision 190): all
/// but those of the processes it couldn't stop.
async fn sweep_connection_files(dir: &Path, alive: &std::collections::BTreeSet<String>) {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(dir = %dir.display(), %error, "can't read the connection files' folder");
            return;
        }
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !alive.contains(&name) {
            remove_connection_files(&entry.path()).await;
        }
    }
}

impl<E: Executor> Agent<E> {
    /// An agent with a new incarnation ID.
    #[must_use]
    pub fn new(settings: AgentSettings, executor: E, incarnation: String) -> Self {
        let watchers = settings
            .setup
            .roots
            .iter()
            .chain(&settings.setup.script_roots)
            .map(|(name, dir)| {
                let store = settings.code_dir.join(code_folder(name));
                (
                    name.clone(),
                    RootWatcher::new(dir, store, settings.settle_time),
                )
            })
            .collect();
        let (done_tx, done_rx) = mpsc::unbounded_channel();
        let journal = Journal::new(settings.journal_dir.clone());
        let warnings = executor.warnings();
        Self {
            settings,
            executor,
            warnings,
            incarnation,
            journal,
            watchers,
            running: BTreeMap::new(),
            wait_steps: BTreeMap::new(),
            cleaning: BTreeMap::new(),
            waiting: BTreeMap::new(),
            cleanup_room: BTreeMap::new(),
            stopped: std::collections::BTreeSet::new(),
            gates: BTreeMap::new(),
            rest_cleaning: BTreeSet::new(),
            heard: tokio::time::Instant::now(),
            deadline: watch::channel(None).0,
            lease: None,
            cut_off: false,
            warned_room: std::collections::BTreeSet::new(),
            unacked: BTreeMap::new(),
            unsent_answers: std::collections::VecDeque::new(),
            code_hashes: BTreeMap::new(),
            other_copies: BTreeMap::new(),
            uses: BTreeMap::new(),
            reported: BTreeMap::new(),
            parsing: BTreeMap::new(),
            connection_jobs: Arc::new(tokio::sync::Mutex::new(())),
            done_tx,
            done_rx,
            heartbeat: Duration::from_secs(10),
            registered: false,
            master_limit: GRPC_DEFAULT_LIMIT,
            shutdown: None,
            drain: None,
            beat: Beat::default(),
            metrics: AgentMetrics::default(),
        }
        .with_metrics(AgentMetrics::default())
    }

    /// The agent recording its metrics in `metrics` (registered by the caller).
    #[must_use]
    pub fn with_metrics(mut self, metrics: AgentMetrics) -> Self {
        metrics.slots.set(i64::from(self.settings.slots));
        metrics
            .watchers
            .slots
            .set(i64::from(self.settings.watcher_slots));
        self.metrics = metrics;
        self
    }

    /// When the agent's event loop last turned: at least every [`ALIVE_INTERVAL`] whether or
    /// not the master is reachable (it is the agent's own health).
    #[must_use]
    pub fn beat(&self) -> Beat {
        self.beat.clone()
    }

    /// The loop is alive: beat, and update the gauges of slots and the journal.
    fn alive(&self) {
        self.beat.beat();
        let running = i64::try_from(self.running.len()).unwrap_or(i64::MAX);
        let unacked = i64::try_from(self.unacked.len()).unwrap_or(i64::MAX);
        self.metrics
            .slots_used
            .set(i64::try_from(self.tasks_running()).unwrap_or(i64::MAX));
        self.metrics
            .watchers
            .slots_used
            .set(i64::try_from(self.wait_steps.len()).unwrap_or(i64::MAX));
        self.metrics.journal_records.set(running + unacked);
    }

    /// The attempts running now.
    #[must_use]
    pub fn running(&self) -> Vec<String> {
        self.running.keys().cloned().collect()
    }

    /// Runs until drained after a shutdown request (`shutdown` counts them; see
    /// [`crate::drain`]): recovers the journal, connects with `connect`, and again whenever
    /// the connection drops, after a backoff with jitter that doubles up to [`MAX_BACKOFF`] and
    /// starts from [`FIRST_BACKOFF`] again after a session in which the master registered it.
    pub async fn run<C, F>(&mut self, connect: C, mut shutdown: watch::Receiver<u32>)
    where
        C: Fn() -> F,
        F: Future<Output = Result<MasterClient<Channel>, String>>,
    {
        let requested = *shutdown.borrow_and_update();
        self.shutdown = Some(shutdown);
        self.recover().await;
        self.on_shutdown(requested);
        let timeouts = self.settings.setup.timeouts;
        let mut backoff = timeouts.reconnect_first;
        loop {
            self.registered = false;
            let Some(connected) = self.offline(connect()).await else {
                return;
            };
            let end = match connected {
                Ok(client) => self.session(client).await,
                Err(error) => SessionEnd::Lost(error),
            };
            match end {
                SessionEnd::Stopped => return,
                SessionEnd::Lost(reason) => {
                    tracing::warn!(reason, "master connection lost; connecting again");
                    self.hold_cleanups();
                }
            }
            if self.registered {
                backoff = timeouts.reconnect_first;
            }
            // The clock's nanoseconds (below 10^9), spread over all of u32 by a
            // multiplicative hash, so the jitter covers its whole range.
            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos())
                .wrapping_mul(2_654_435_761);
            if self
                .offline(tokio::time::sleep(jittered(
                    backoff.min(reconnect_cap(timeouts.reconnect_max, self.lease)),
                    seed,
                )))
                .await
                .is_none()
            {
                return;
            }
            backoff = (backoff * 2).min(reconnect_cap(timeouts.reconnect_max, self.lease));
        }
    }

    /// Waits for `work` without a master: what ends meanwhile is still journalled (results
    /// are reported in the next `Register`), and a drain goes on. `None` once drained.
    async fn offline<T>(&mut self, work: impl Future<Output = T>) -> Option<T> {
        tokio::pin!(work);
        let timeouts = self.settings.setup.timeouts;
        let mut tick = tokio::time::interval(timeouts.drain_tick);
        let mut alive = tokio::time::interval(timeouts.alive);
        loop {
            let cut_off_at = self.cut_off_at();
            tokio::select! {
                _ = alive.tick() => self.alive(),
                () = at(cut_off_at) => self.stop_cut_off(),
                out = &mut work => return Some(out),
                Some(done) = self.done_rx.recv() => self.on_done(done, None).await,
                count = next_request(&mut self.shutdown) => {
                    self.on_shutdown(count);
                }
                _ = tick.tick(), if self.drain.is_some() => {
                    if self.drain_step(false) == DrainStep::Finish {
                        return None;
                    }
                }
            }
        }
    }

    /// Fetches an attempt's inputs, which can take long, with the session's timers going on:
    /// its loop waits for this. A chunk is word from the master, unless it comes past the
    /// lease (decision 281). `None` when the master went silent for the lease (on a stream
    /// opened at `opened`): the session ends, and nothing is started.
    async fn fetch_with_timers(
        &mut self,
        client: &mut MasterClient<Channel>,
        attempt_id: &str,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
        (heartbeat, opened): (&mut tokio::time::Interval, tokio::time::Instant),
    ) -> Option<Result<Vec<u8>, String>> {
        let (heard, seen) = watch::channel(self.heard.max(opened));
        let fetching = fetch_inputs(client, attempt_id, &heard, self.lease);
        tokio::pin!(fetching);
        loop {
            self.heard = self.heard.max(*seen.borrow());
            self.publish_deadline();
            let give_up_at = earliest(self.cut_off_at(), self.silent_at(opened));
            tokio::select! {
                fetched = &mut fetching => {
                    return match fetched {
                        Fetched::Bytes(bytes) => Some(Ok(bytes)),
                        Fetched::Failed(error) => Some(Err(error)),
                        Fetched::Silent => {
                            self.cut_off_if_late();
                            None
                        }
                    };
                }
                _ = heartbeat.tick() => {
                    let _ = to_master.send(self.heartbeat_message());
                }
                () = at(give_up_at) => {
                    self.heard = self.heard.max(*seen.borrow());
                    if self.silent_too_long(opened) {
                        return None;
                    }
                }
            }
        }
    }

    /// Whether this is a `Registered` older than the lease it states: the master may have
    /// counted this worker lost since, moved its cleanups and taken back what it assigned
    /// with it (decision 281), so its decisions no longer stand; a worker without a lease yet
    /// (its first session) included. Asked again, it answers as things are now.
    fn answer_too_late(message: &master_message::Message, age: Duration) -> bool {
        let master_message::Message::Registered(registered) = message else {
            return false;
        };
        registered.timing.as_ref().is_some_and(|t| {
            t.lease_timeout_ms > 0 && age.as_millis() >= u128::from(t.lease_timeout_ms)
        })
    }

    /// The attempts a drain waits for: not a cleanup waiting for the master, with none to
    /// ask. The journal keeps it.
    fn drain_running(&self) -> usize {
        let held = self
            .gates
            .iter()
            .filter(|(id, gate)| *gate.borrow() == Gate::Held && self.cleaning.contains_key(*id))
            .count();
        self.running
            .len()
            .saturating_sub(self.waiting.len().saturating_add(held))
    }

    /// What the master hears at once on a new stream.
    fn send_state(&mut self, to_master: &mpsc::UnboundedSender<pb::WorkerMessage>) {
        if self.drain.is_some() {
            let _ = to_master.send(wrap(worker_message::Message::Draining(pb::Draining {})));
        }
        // All of them, after every registration: each report replaces the master's record.
        let warnings = self.warnings.borrow_and_update().clone();
        let _ = to_master.send(wrap(worker_message::Message::Warnings(pb::Warnings {
            warnings,
        })));
    }

    /// The `Register` opening a session.
    fn register_message(&self, roots: Vec<pb::RootHash>) -> pb::Register {
        pb::Register {
            worker_id: self.settings.worker_id.clone(),
            incarnation_id: self.incarnation.clone(),
            protocol: Some(PROTOCOL_VERSION),
            schedr_version: AGENT_VERSION.to_owned(),
            queues: self.settings.queues.clone(),
            slots: self.settings.slots,
            watcher_slots: self.settings.watcher_slots,
            log_endpoint: self.settings.log_endpoint.clone(),
            roots,
            secret_storages: self.settings.secrets.names(),
            journal: self.journal_entries(),
            process_id: self.settings.node.process_id.clone(),
            config_json: self.settings.node.config_json.clone(),
        }
    }

    /// A task with `cleanup_anywhere` gets a gate before its cleanup.
    fn set_gate(&mut self, spec: &mut AttemptSpec) {
        if spec.task.cleanup.as_ref().is_some_and(|c| c.anywhere) {
            let (gate, passes) = watch::channel(Gate::Open);
            self.gates.insert(spec.attempt_id.clone(), gate);
            spec.cleanup_gate = Some(CleanupGate {
                gate: passes,
                deadline: self.deadline.subscribe(),
            });
        }
    }

    /// Tells the cleanup gates when the master's word runs out (`heard` plus the lease).
    fn publish_deadline(&self) {
        let deadline = self.lease.map(|lease| self.heard + lease);
        self.deadline.send_if_modified(|now| {
            let changed = *now != deadline;
            *now = deadline;
            changed
        });
    }

    /// When the tasks with `cleanup_anywhere` are stopped, with no word from the master: the
    /// worker lease after the last, as the master counts it lost (decision 252).
    fn cut_off_at(&self) -> Option<tokio::time::Instant> {
        if self.cut_off || self.gates.is_empty() {
            return None;
        }
        self.lease.map(|lease| self.heard + lease)
    }

    /// When a stream opened at `opened` is given up with no word from the master, whatever
    /// runs: the lease after the master's last word, or after the stream opened. An assignment
    /// held on its way (a one-way cut) is then never started after the master took it back
    /// at the lease (decision 281, B163).
    fn silent_at(&self, opened: tokio::time::Instant) -> Option<tokio::time::Instant> {
        self.lease.map(|lease| self.heard.max(opened) + lease)
    }

    /// Whether a stream opened at `opened` has been silent for the lease
    /// ([`Self::silent_at`]); past the lease the tasks are cut off ([`Self::cut_off_if_late`]).
    fn silent_too_long(&mut self, opened: tokio::time::Instant) -> bool {
        let silent = self.gone_silent(opened);
        self.cut_off_if_late() || silent
    }

    /// Whether the lease has run out with no word from the master ([`Self::silent_at`]).
    fn gone_silent(&self, opened: tokio::time::Instant) -> bool {
        self.silent_at(opened)
            .is_some_and(|at| at <= tokio::time::Instant::now())
    }

    /// Cuts the tasks off ([`Self::stop_cut_off`]) if the lease has run out since the master's
    /// last word; whether it had.
    fn cut_off_if_late(&mut self) -> bool {
        let late = self
            .cut_off_at()
            .is_some_and(|at| at <= tokio::time::Instant::now());
        if late {
            self.stop_cut_off();
        }
        late
    }

    /// Stops the tasks whose cleanup may move: it mustn't run during them. Their cleanups wait
    /// for the master's answer.
    fn stop_cut_off(&mut self) {
        self.cut_off = true;
        self.hold_cleanups();
        for id in self.gates.keys() {
            if !self.cleaning.contains_key(id)
                && let Some(stop) = self.running.get(id)
            {
                tracing::warn!(
                    attempt_id = id,
                    "no word from the master: stopping the task"
                );
                let _ = stop.send(StopReason::CutOff);
            }
        }
    }

    /// With no master to ask, a cleanup with `cleanup_anywhere` doesn't start here: it may be
    /// moved (decision 254). One under way goes on (decision 255).
    fn hold_cleanups(&self) {
        for gate in self.gates.values() {
            gate.send_if_modified(|gate| {
                let open = *gate == Gate::Open;
                if open {
                    *gate = Gate::Held;
                }
                open
            });
        }
    }

    /// A shutdown request arrived (`count` so far): the first starts the drain, a later one
    /// interrupts at once. Returns whether the drain just started.
    fn on_shutdown(&mut self, count: u32) -> bool {
        let now = Instant::now();
        if let Some(drain) = &mut self.drain {
            drain.force(now);
            tracing::warn!("asked to stop again: interrupting the running tasks");
            return false;
        }
        if count == 0 {
            return false;
        }
        let mut drain = Drain::new(now, self.settings.drain);
        if count > 1 {
            drain.force(now);
        }
        self.drain = Some(drain);
        tracing::info!(
            running = self.running.len(),
            grace = ?self.settings.drain.grace,
            "draining: no new tasks; the running ones get until the grace period ends"
        );
        true
    }

    /// The drain's next step; stops the attempts still running when their time is up.
    fn drain_step(&mut self, connected: bool) -> DrainStep {
        let running = self.drain_running();
        let Some(drain) = &mut self.drain else {
            return DrainStep::Wait;
        };
        let now = Instant::now();
        let step = drain.step(now, running, self.unacked.len(), connected);
        // Each early enough for one try of its cleanup; one in its cleanup isn't stopped. Also
        // those already being stopped (cancelled, timed out): their SIGKILL comes no later than
        // the drain's (B9).
        for (id, stop) in &self.running {
            let room = self.cleanup_room.get(id).copied().unwrap_or_default();
            // A poke goes at once: it runs on elsewhere, losing nothing (decision 335).
            let poke = self.wait_steps.get(id).copied().unwrap_or(false);
            if !self.cleaning.contains_key(id)
                && (poke || drain.stop_due(now, room))
                && self.stopped.insert(id.clone())
            {
                let _ = stop.send(StopReason::Interrupted);
            }
        }
        if step == DrainStep::Interrupt {
            tracing::warn!(
                running = self.running.len(),
                "the grace period is ending: interrupting the running tasks"
            );
        }
        step
    }

    /// Takes over from a previous incarnation: its unacknowledged results are reported again,
    /// and the processes it left running are stopped and their attempts forgotten (the master
    /// ends them `lost`). A journal that can't be read is logged; assignments are then
    /// rejected until it can be written.
    pub async fn recover(&mut self) {
        let journal = self.journal.clone();
        let records = match tokio::task::spawn_blocking(move || journal.recover()).await {
            Ok(Ok(records)) => records,
            Ok(Err(error)) => {
                tracing::error!(dir = %self.journal.dir().display(), %error, "can't read the journal");
                return;
            }
            Err(error) => {
                tracing::error!(%error, "reading the journal failed");
                return;
            }
        };
        let mut orphans = tokio::task::JoinSet::new();
        let mut ended = BTreeMap::new();
        for (attempt_id, record) in records {
            let process = match record {
                JournalRecord::Finished(finished) => {
                    self.unacked.insert(attempt_id, finished);
                    continue;
                }
                JournalRecord::Running(process) => process,
                JournalRecord::Cleaning(finished, process) => {
                    ended.insert(attempt_id.clone(), finished);
                    process
                }
            };
            let timeouts = self.settings.setup.timeouts;
            orphans.spawn(async move {
                let Some(identity) = process else {
                    return Ok(attempt_id);
                };
                match stop_orphan(identity, timeouts.orphan_grace, timeouts.orphan).await {
                    Ok(Orphan::Gone) => {}
                    Ok(how) => tracing::warn!(
                        attempt_id,
                        pgid = identity.pgid,
                        ?how,
                        "stopped a task process left by the previous agent"
                    ),
                    Err(error) => {
                        // Kept in the journal: the next start tries again.
                        tracing::error!(
                            attempt_id,
                            pgid = identity.pgid,
                            %error,
                            "can't stop a task process left by the previous agent"
                        );
                        return Err(attempt_id);
                    }
                }
                Ok(attempt_id)
            });
        }
        let mut alive = std::collections::BTreeSet::new();
        while let Some(stopped) = orphans.join_next().await {
            match stopped {
                Ok(Ok(attempt_id)) => {
                    let ended = ended.remove(&attempt_id);
                    if !self.wait_for_cleanup(&attempt_id, ended).await {
                        self.forget(&attempt_id).await;
                    }
                }
                Ok(Err(attempt_id)) => {
                    alive.insert(attempt_id);
                }
                Err(_) => {}
            }
        }
        sweep_connection_files(&self.settings.setup.connection_dir, &alive).await;
    }

    /// Sets aside the cleanup of an attempt the previous agent left, if the journal kept what
    /// it needs: it runs on the master's `Continue` (decision 248). Returns whether it did.
    async fn wait_for_cleanup(&mut self, attempt_id: &str, ended: Option<pb::Finished>) -> bool {
        let journal = self.journal.clone();
        let id = attempt_id.to_owned();
        let read = tokio::task::spawn_blocking(move || {
            Ok::<_, std::io::Error>((journal.kept(&id)?, journal.handles(&id)?))
        })
        .await;
        let (kept, journaled) = match read {
            Ok(Ok((Some(kept), handles))) => (kept, handles),
            Ok(Ok((None, _))) => return false,
            // Run without what it needs, it could report success and leave them behind: the
            // master ends its cleanup as not run, which fails the run.
            Ok(Err(error)) => {
                tracing::error!(attempt_id, %error, "can't read a kept attempt; no cleanup here");
                return false;
            }
            Err(error) => {
                tracing::error!(attempt_id, %error, "reading a kept attempt failed");
                return false;
            }
        };
        // A moved cleanup's handles came in its assignment, kept whole before it started.
        let handles = match kept.assign.cleanup_only.as_ref().map(moved_handles) {
            None => journaled,
            Some(Ok(handles)) => handles,
            Some(Err(error)) => {
                tracing::error!(attempt_id, %error, "a moved cleanup's handles don't read");
                return false;
            }
        };
        let mut spec = match AttemptSpec::from_assign(&kept.assign, None) {
            Ok(spec) if spec.task.cleanup.is_some() => spec,
            Ok(_) => return false,
            Err(error) => {
                tracing::warn!(attempt_id, %error, "can't read a kept attempt; its cleanup is lost");
                return false;
            }
        };
        spec.code_dir = kept.code_dir;
        spec.root_copies = kept.root_copies;
        spec.script_dir = kept.script_dir;
        if let Some(hash) = kept.code_hash {
            self.code_hashes.insert(attempt_id.to_owned(), hash);
        }
        self.remember_other_copies(&spec);
        let (stop, _) = Stops::new();
        self.running.insert(attempt_id.to_owned(), stop);
        let waiting = WaitingCleanup {
            spec,
            handles,
            ended,
            rest: false,
        };
        self.waiting.insert(attempt_id.to_owned(), waiting);
        true
    }

    /// The master said `Continue`: a cleanup the previous agent left starts; the task's end is
    /// reported first (`lost`, if it hadn't ended).
    async fn start_waiting(
        &mut self,
        attempt_id: &str,
        waiting: WaitingCleanup,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
    ) {
        let ended = waiting.ended.unwrap_or_else(|| restarted(attempt_id));
        let done = self.done_tx.clone();
        let on_spawn = self.done_tx.clone();
        let id = attempt_id.to_owned();
        let spawned: Spawned = Arc::new(move |identity, forked| {
            let _ = on_spawn.send(Done::Spawned {
                attempt_id: id.clone(),
                identity,
                forked,
            });
        });
        self.task_ended(ended.clone(), Some(to_master)).await;
        let executor = self.executor.clone();
        let (spec, handles, rest) = (waiting.spec, waiting.handles, waiting.rest);
        tokio::spawn(async move {
            let mut finished = executor.cleanup(spec, ended, handles, spawned).await;
            if let Some(cleanup) = finished.cleanup.as_mut() {
                cleanup.rest = rest;
            }
            let _ = done.send(Done::Attempt(finished));
        });
    }

    /// A task with a cleanup ended: on disk, then reported.
    async fn task_ended(
        &mut self,
        mut finished: pb::Finished,
        to_master: Option<&mpsc::UnboundedSender<pb::WorkerMessage>>,
    ) {
        fit_result(&mut finished, self.master_limit);
        // A moved poke says itself which code it ended on (decision 335).
        if let (true, Some(hash)) = (
            finished.code_hash.is_empty(),
            self.code_hashes.get(&finished.attempt_id),
        ) {
            finished.code_hash.clone_from(hash);
        }
        let id = finished.attempt_id.clone();
        self.cleanup_room.remove(&id);
        let record = JournalRecord::Cleaning(finished.clone(), None);
        if let Err(error) = self.record(&id, record).await {
            tracing::warn!(attempt_id = id, %error, "can't record the task's end");
        }
        self.cleaning.insert(id, finished.clone());
        if let Some(to_master) = to_master {
            let _ = to_master.send(task_ended_message(finished));
        }
    }

    /// Records `record` for an attempt in the journal.
    async fn record(&self, attempt_id: &str, record: JournalRecord) -> std::io::Result<()> {
        let journal = self.journal.clone();
        let attempt_id = attempt_id.to_owned();
        tokio::task::spawn_blocking(move || journal.write(&attempt_id, &record))
            .await
            .map_err(std::io::Error::other)?
    }

    /// Removes an attempt from the journal; a failure is logged (the file is then reported
    /// again after a restart, and the master decides on it).
    async fn forget(&self, attempt_id: &str) {
        let journal = self.journal.clone();
        let id = attempt_id.to_owned();
        let removed = tokio::task::spawn_blocking(move || journal.remove(&id))
            .await
            .map_err(std::io::Error::other)
            .and_then(|removed| removed);
        if let Err(error) = removed {
            tracing::warn!(attempt_id, %error, "can't remove the attempt from the journal");
        }
    }

    /// The roots' reported hashes, waiting for each to settle the first time; a DAG root
    /// using a script root this worker lacks is left out (decision 303).
    async fn hashes(&mut self) -> Vec<pb::RootHash> {
        for (name, watcher) in &mut self.watchers {
            loop {
                if watcher.hash().is_some() {
                    break;
                }
                match watcher.poll(Instant::now()) {
                    Ok(Poll::Settling(wait)) => tokio::time::sleep(wait).await,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(root = name.as_str(), %error, "can't scan the root");
                        tokio::time::sleep(self.settings.poll_interval).await;
                    }
                }
            }
        }
        self.reported = self
            .watchers
            .keys()
            .filter_map(|name| Some((name.clone(), self.reported_hash(name)?)))
            .collect();
        self.reported
            .iter()
            .map(|(name, hash)| root_hash(&self.settings.setup, name, hash))
            .collect()
    }

    /// The hash this worker reports for root `name`, `None` before its first scan or for a
    /// DAG root it can't offer (decision 303). A script root's is its content hash marked as
    /// a script root's, so a parse of the same files as a DAG root never counts for it
    /// (B167); a DAG root's folds in the reported hashes of the script roots it uses (decision
    /// 301) and the content hashes of the other DAG roots, all on its `sys.path` (decision 327,
    /// B173), so a change to any of them is new code for it.
    fn reported_hash(&self, name: &str) -> Option<String> {
        let content = self.watchers.get(name)?.hash()?;
        let scripts = &self.settings.setup.script_roots;
        if scripts.contains_key(name) {
            return Some(fold_hash("script", content, &[]));
        }
        let mut folded = Vec::new();
        // Their content hashes, not their reported ones: DAG roots can import each other.
        for other in self
            .settings
            .setup
            .roots
            .keys()
            .filter(|other| *other != name)
        {
            let hash = self.watchers.get(other)?.hash()?;
            folded.push((format!("dag:{other}"), hash.to_owned()));
        }
        for script in self.uses.get(name).into_iter().flatten() {
            if !scripts.contains_key(script) {
                return None;
            }
            folded.push((script.clone(), self.reported_hash(script)?));
        }
        if folded.is_empty() {
            return Some(content.to_owned());
        }
        Some(fold_hash("dag", content, &folded))
    }

    /// Reports each root whose hash changed since it was last reported: a new hash, or an
    /// empty one for a root withdrawn (decision 303).
    fn report_changes(&mut self, to_master: &mpsc::UnboundedSender<pb::WorkerMessage>) {
        self.executor.code_changed(
            self.watchers
                .iter()
                .filter(|(name, _)| !self.settings.setup.script_roots.contains_key(*name))
                .filter_map(|(name, watcher)| {
                    Some((name.clone(), watcher.code_dir()?.to_path_buf()))
                })
                .collect(),
        );
        let names: Vec<String> = self.watchers.keys().cloned().collect();
        for name in names {
            let now = self.reported_hash(&name);
            if now.as_ref() == self.reported.get(&name) {
                continue;
            }
            if now.is_none() {
                let missing: Vec<&str> = self
                    .uses
                    .get(&name)
                    .into_iter()
                    .flatten()
                    .filter(|script| !self.settings.setup.script_roots.contains_key(*script))
                    .map(String::as_str)
                    .collect();
                tracing::warn!(
                    root = name.as_str(),
                    missing = missing.join(", "),
                    "DAG root withdrawn: its DAGs use script roots this worker doesn't have"
                );
            }
            let hash = now.clone().unwrap_or_default();
            match now {
                Some(hash) => self.reported.insert(name.clone(), hash),
                None => self.reported.remove(&name),
            };
            let changed = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
                root: Some(root_hash(&self.settings.setup, &name, &hash)),
            });
            let _ = to_master.send(wrap(changed));
        }
    }

    /// The master said which script roots each DAG root uses (decision 304).
    fn set_uses(
        &mut self,
        roots: Vec<pb::RootUses>,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
    ) {
        self.uses = roots
            .into_iter()
            .map(|uses| (uses.root, uses.script_roots.into_iter().collect()))
            .collect();
        self.report_changes(to_master);
    }

    /// One session with the master: register, then handle messages until the connection ends.
    pub async fn session(&mut self, mut client: MasterClient<Channel>) -> SessionEnd {
        let roots = self.hashes().await;
        // Unbounded: the loop never waits on the master (its liveness is the agent's own); the
        // messages are few and small (heartbeats, results, reports).
        let (to_master, outbound) = mpsc::unbounded_channel::<pb::WorkerMessage>();
        let register = self.register_message(roots);
        if to_master
            .send(wrap(worker_message::Message::Register(register)))
            .is_err()
        {
            return SessionEnd::Lost("the outbound stream closed".to_owned());
        }
        let opened = tokio::time::Instant::now();
        let give_up_at = earliest(self.cut_off_at(), self.silent_at(opened));
        let control = tokio::select! {
            control = client.control(UnboundedReceiverStream::new(outbound)) => control,
            () = at(give_up_at) => {
                self.cut_off_if_late();
                return SessionEnd::Lost(NO_WORD.to_owned());
            }
        };
        let mut inbound = match control {
            Ok(response) => response.into_inner(),
            Err(status) => return SessionEnd::Lost(status.to_string()),
        };
        self.send_state(&to_master);
        let mut warnings_open = true;
        let mut heartbeat = heartbeat_timer(self.heartbeat);
        let mut watch = tokio::time::interval(self.settings.poll_interval);
        let timeouts = self.settings.setup.timeouts;
        let mut tick = tokio::time::interval(timeouts.drain_tick);
        let mut alive = tokio::time::interval(timeouts.alive);
        // The unacknowledged results' resends (B41: the master failed to store one and kept
        // the stream).
        let mut resends = Resends::default();
        loop {
            let give_up_at = earliest(self.cut_off_at(), self.silent_at(opened));
            tokio::select! {
                () = at(give_up_at) => {
                    self.cut_off_if_late();
                    return SessionEnd::Lost(NO_WORD.to_owned());
                }
                count = next_request(&mut self.shutdown) => {
                    if self.on_shutdown(count) {
                        let draining = worker_message::Message::Draining(pb::Draining {});
                        let _ = to_master.send(wrap(draining));
                    }
                }
                _ = tick.tick(), if self.drain.is_some() => {
                    if self.drain_step(true) == DrainStep::Finish {
                        let bye = worker_message::Message::Deregister(pb::Deregister {});
                        let _ = to_master.send(wrap(bye));
                        // Our side ends; the master's ends once it has handled everything
                        // (not waited for past the end of the grace period).
                        drop(to_master);
                        let left = self.drain.map_or(timeouts.deregister, |drain| {
                            drain.end().saturating_duration_since(Instant::now())
                        });
                        let _ = tokio::time::timeout(timeouts.deregister.min(left), async {
                            while let Ok(Some(_)) = inbound.message().await {}
                        })
                        .await;
                        return SessionEnd::Stopped;
                    }
                }
                message = inbound.message() => match message {
                    Ok(Some(pb::MasterMessage { message: Some(message) })) => {
                        if Self::answer_too_late(&message, opened.elapsed()) {
                            return SessionEnd::Lost(TOO_LATE.to_owned());
                        }
                        // Past the lease, a message (one held while this worker was frozen,
                        // B162, or on its way through a one-way cut, B163) doesn't count as
                        // the master's word.
                        if self.silent_too_long(opened) {
                            return SessionEnd::Lost(NO_WORD.to_owned());
                        }
                        self.heard = tokio::time::Instant::now();
                        self.publish_deadline();
                        self.on_master(message, &to_master, &mut client, (&mut heartbeat, opened)).await;
                        // Past the lease while handling it (a slow fetch of inputs).
                        if self.cut_off || self.gone_silent(opened) {
                            return SessionEnd::Lost(NO_WORD.to_owned());
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => return SessionEnd::Lost("the master ended the stream".to_owned()),
                    Err(status) => return SessionEnd::Lost(status.to_string()),
                },
                Some(done) = self.done_rx.recv() => {
                    self.on_done(done, Some((&to_master, &mut client))).await;
                }
                _ = heartbeat.tick() => {
                    if let Some(end) = self.send_heartbeat(&mut resends, &to_master) {
                        return end;
                    }
                }
                _ = watch.tick() => self.watch(&to_master),
                _ = alive.tick() => self.alive(),
                changed = self.warnings.changed(), if warnings_open => {
                    if changed.is_ok() {
                        let warnings = self.warnings.borrow_and_update().clone();
                        let report = pb::Warnings { warnings };
                        let _ = to_master.send(wrap(worker_message::Message::Warnings(report)));
                    } else {
                        // No one reports any more: nothing will change.
                        warnings_open = false;
                    }
                }
            }
        }
    }

    /// A heartbeat, and the unacknowledged results due to be sent again; the session's end
    /// if one was sent too often already.
    fn heartbeat_message(&self) -> pb::WorkerMessage {
        wrap(worker_message::Message::Heartbeat(pb::Heartbeat {
            free_slots: self.free_slots(),
            free_watcher_slots: self.free_watcher_slots(),
            running_attempt_ids: self.running(),
            watcher_hosts: self.executor.watcher_hosts(),
        }))
    }

    fn send_heartbeat(
        &self,
        resends: &mut Resends,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
    ) -> Option<SessionEnd> {
        let _ = to_master.send(self.heartbeat_message());
        let Some(due) = resends.due(self.unacked.keys()) else {
            return Some(SessionEnd::Lost(format!(
                "a result is still unacknowledged after {MAX_RESENDS} sends; reconnecting, so \
                 the master's reconciliation settles it"
            )));
        };
        for id in due {
            self.send_again(&id, to_master);
        }
        None
    }

    async fn on_master(
        &mut self,
        message: master_message::Message,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
        client: &mut MasterClient<Channel>,
        (heartbeat, opened): (&mut tokio::time::Interval, tokio::time::Instant),
    ) {
        match message {
            master_message::Message::Registered(registered) => {
                self.registered = true;
                if registered.max_message_size > 0 {
                    self.master_limit =
                        usize::try_from(registered.max_message_size).unwrap_or(usize::MAX);
                }
                if let Some(timing) = registered.timing.filter(|t| t.heartbeat_interval_ms > 0) {
                    self.heartbeat = Duration::from_millis(timing.heartbeat_interval_ms);
                    *heartbeat = heartbeat_timer(self.heartbeat);
                    self.lease = (timing.lease_timeout_ms > 0)
                        .then(|| Duration::from_millis(timing.lease_timeout_ms));
                    self.publish_deadline();
                }
                self.cut_off = false;
                for decision in registered.decisions {
                    self.decide(&decision, to_master).await;
                }
                for done in self.unsent_answers.drain(..) {
                    let _ = to_master.send(wrap(worker_message::Message::ConnectionJobDone(done)));
                }
                self.set_uses(registered.script_uses, to_master);
            }
            master_message::Message::ScriptUses(uses) => self.set_uses(uses.roots, to_master),
            master_message::Message::AssignTask(assign) => {
                self.assign(assign, to_master, client, (heartbeat, opened))
                    .await;
            }
            master_message::Message::AssignParse(job) => self.parse(job),
            // Heard: that is all it says.
            master_message::Message::HeartbeatAck(_) => {}
            master_message::Message::ConnectionJob(job) => self.connection_job(job),
            master_message::Message::CancelAttempt(cancel) => {
                // It stays running (its slot taken) until its process has ended and reported;
                // a repeated cancel changes nothing.
                if let Some(stop) = self.running.get(&cancel.attempt_id) {
                    let _ = stop.send(if cancel.interrupted {
                        StopReason::Interrupted
                    } else {
                        StopReason::Cancelled
                    });
                }
            }
            master_message::Message::FinishedAck(ack) => {
                if self.unacked.remove(&ack.attempt_id).is_some() {
                    self.forget(&ack.attempt_id).await;
                }
            }
        }
    }

    async fn decide(
        &mut self,
        decision: &pb::JournalDecision,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
    ) {
        let id = &decision.attempt_id;
        match pb::AttemptDecision::try_from(decision.decision) {
            Ok(pb::AttemptDecision::Ack) => {
                if self.unacked.remove(id).is_some() {
                    self.forget(id).await;
                }
            }
            Ok(pb::AttemptDecision::CleanRest) => {
                // Ended, its cleanup moved without the handles from `cleanup_from` on: they
                // are cleaned here, the master adding the result to the attempt's (B169).
                let from = decision.cleanup_from;
                self.rest_cleaning.insert(id.clone());
                if let Some(gate) = self.gates.get(id) {
                    gate.send_replace(Gate::Rest(from));
                }
                if let Some(mut waiting) = self.waiting.remove(id) {
                    let from = (from as usize).min(waiting.handles.len());
                    waiting.handles.drain(..from);
                    waiting.rest = true;
                    self.start_waiting(id, waiting, to_master).await;
                }
                // The master ended the task: stopped here, the rest cleaned once it stops.
                if let Some(stop) = self.running.get(id) {
                    let _ = stop.send(StopReason::Cancelled);
                }
            }
            Ok(pb::AttemptDecision::Kill) => {
                if self.unacked.remove(id).is_some() {
                    self.forget(id).await;
                }
                if let Some(gate) = self.gates.get(id) {
                    gate.send_replace(Gate::Dropped);
                }
                if self.waiting.remove(id).is_some() {
                    // The master ended it, or it moved: its cleanup isn't run here.
                    self.running.remove(id);
                    self.release_code(id);
                    self.forget(id).await;
                    self.collect_code();
                }
                if let Some(stop) = self.running.get(id) {
                    let _ = stop.send(StopReason::Cancelled);
                }
            }
            _ => {
                if let Some(gate) = self.gates.get(id) {
                    gate.send_replace(Gate::Open);
                }
                if self.running.contains_key(id) {
                    self.send_handles(id, to_master).await;
                }
                if let Some(waiting) = self.waiting.remove(id) {
                    self.start_waiting(id, waiting, to_master).await;
                } else if let Some(ended) = self.cleaning.get(id) {
                    // Its report may have gone with the old stream; fitted to the limit now,
                    // as a result sent again is (B47).
                    let mut ended = ended.clone();
                    fit_result(&mut ended, self.master_limit);
                    let _ = to_master.send(task_ended_message(ended));
                } else {
                    self.send_again(id, to_master);
                }
            }
        }
    }

    /// Sends an attempt's handles again: some may have gone with the old stream, or never
    /// left (decision 242). The master keeps what it has.
    async fn send_handles(
        &self,
        attempt_id: &str,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
    ) {
        let journal = self.journal.clone();
        let id = attempt_id.to_owned();
        let read = tokio::task::spawn_blocking(move || journal.handles(&id)).await;
        match read {
            Ok(Ok(handles)) if !handles.is_empty() => {
                let _ = to_master.send(handles_message(attempt_id.to_owned(), 0, &handles));
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(attempt_id, %error, "can't read the handles to send them again");
            }
            Err(error) => tracing::warn!(attempt_id, %error, "reading the handles failed"),
        }
    }

    /// Sends unacknowledged result `id` again, fitted to the master's limit now: it was
    /// journalled under the limit of its time, which a master since may have lowered (B47).
    /// The journal keeps it whole, for a master accepting it.
    fn send_again(&self, id: &str, to_master: &mpsc::UnboundedSender<pb::WorkerMessage>) {
        if let Some(finished) = self.unacked.get(id) {
            let mut finished = finished.clone();
            fit_result(&mut finished, self.master_limit);
            let _ = to_master.send(wrap(worker_message::Message::Finished(finished)));
        }
    }

    /// An assignment's inputs fetched from the master when they came apart from it: `None`
    /// when it isn't started, rejected (a failed fetch) or the master gone silent (the session
    /// ends, and the master takes it back at the lease).
    async fn external_inputs(
        &mut self,
        assign: &pb::AssignTask,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
        client: &mut MasterClient<Channel>,
        timers: (&mut tokio::time::Interval, tokio::time::Instant),
    ) -> Option<Option<Vec<u8>>> {
        if !assign.inputs_external {
            return Some(None);
        }
        match self
            .fetch_with_timers(client, &assign.attempt_id, to_master, timers)
            .await?
        {
            Ok(bytes) => Some(Some(bytes)),
            Err(detail) => {
                let rejected = pb::Rejected {
                    attempt_id: assign.attempt_id.clone(),
                    reason: pb::RejectReason::InvalidSpecification.into(),
                    detail,
                };
                let _ = to_master.send(wrap(worker_message::Message::Rejected(rejected)));
                None
            }
        }
    }

    async fn assign(
        &mut self,
        assign: pb::AssignTask,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
        client: &mut MasterClient<Channel>,
        timers: (&mut tokio::time::Interval, tokio::time::Instant),
    ) {
        let reject = |reason: pb::RejectReason, detail: String| {
            wrap(worker_message::Message::Rejected(pb::Rejected {
                attempt_id: assign.attempt_id.clone(),
                reason: reason.into(),
                detail,
            }))
        };
        if assign.cleanup_only.is_some() && self.cleaning_rest(&assign.attempt_id) {
            // Its whole cleanup, given here while this worker cleans the handles the master
            // didn't have (B169): refused, it goes elsewhere or comes back after.
            let detail = "cleaning the rest of this attempt's handles".to_owned();
            let _ = to_master.send(reject(pb::RejectReason::NoFreeSlot, detail));
            return;
        }
        if self.running.contains_key(&assign.attempt_id)
            || self.unacked.contains_key(&assign.attempt_id)
        {
            // Already started here: its journal entry must not be replaced.
            tracing::warn!(
                attempt_id = assign.attempt_id.as_str(),
                "assignment of an attempt this worker already has; ignored"
            );
            return;
        }
        if let Some((reason, detail)) = self.cannot_take(assigned_a_wait_step(&assign)) {
            let _ = to_master.send(reject(reason, detail.to_owned()));
            return;
        }
        let Some(external) = self
            .external_inputs(&assign, to_master, client, timers)
            .await
        else {
            return;
        };
        let mut spec = match AttemptSpec::from_assign(&assign, external.as_deref()) {
            Ok(spec) => spec,
            Err(error) => {
                let _ = to_master.send(reject(
                    pb::RejectReason::InvalidSpecification,
                    error.to_string(),
                ));
                return;
            }
        };
        if let Some(detail) = self.code_mismatch(&assign) {
            let _ = to_master.send(reject(pb::RejectReason::CodeMismatch, detail));
            return;
        }
        // Planned with stand-ins for its secrets (fetched once it is started, so a slow
        // storage never holds up the agent): whether it runs here doesn't depend on them.
        spec.stand_in_secrets();
        let planned = self.plan(&mut spec, &assign);
        if !matches!(planned, Err(NotRun::Rejected { .. }))
            && let Err(error) = self.record_start(&spec, &assign, external.as_deref()).await
        {
            let detail = format!(
                "can't write the journal in `{}`: {error}; check the worker's data directory",
                self.journal.dir().display()
            );
            let _ = to_master.send(reject(pb::RejectReason::DiskNotWritable, detail));
            return;
        }
        self.remember_code(&assign, &spec);
        self.note_cleanup(&mut spec, &assign);
        if let Some(only) = &assign.cleanup_only
            && !matches!(planned, Err(NotRun::Rejected { .. }))
        {
            match moved_handles(only) {
                Ok(handles) => self.moved_cleanup(spec, handles, to_master).await,
                // Run without them, it could report success and leave them behind.
                Err(error) => {
                    self.release_code(&spec.attempt_id);
                    self.forget(&spec.attempt_id).await;
                    let detail = format!("a handle that doesn't read: {error}");
                    let _ = to_master.send(reject(pb::RejectReason::InvalidSpecification, detail));
                }
            }
            return;
        }
        let started = wrap(worker_message::Message::Started(pb::Started {
            attempt_id: spec.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        }));
        match planned {
            Err(NotRun::Rejected { reason, detail }) => {
                self.release_code(&spec.attempt_id);
                let _ = to_master.send(reject(reason, detail));
            }
            Err(NotRun::Failed { kind, message }) => {
                let _ = to_master.send(started);
                let failed = failed_before_start(&spec.attempt_id, &kind, &message);
                let _ = self.done_tx.send(Done::Attempt(failed));
            }
            Ok(plan) => {
                let (stop, stopped) = Stops::new();
                self.running.insert(spec.attempt_id.clone(), stop);
                self.note_wait_step(&spec);
                self.set_gate(&mut spec);
                let _ = to_master.send(started);
                self.launch(spec, plan, stopped);
            }
        }
    }

    /// Runs the cleanup of an attempt whose worker was lost (decision 243). Its task's result
    /// is the master's: what is reported here only stands for it.
    async fn moved_cleanup(
        &mut self,
        spec: AttemptSpec,
        handles: Vec<CleanupHandle>,
        to_master: &mpsc::UnboundedSender<pb::WorkerMessage>,
    ) {
        let id = spec.attempt_id.clone();
        let _ = to_master.send(wrap(worker_message::Message::Started(pb::Started {
            attempt_id: id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        })));
        let (stop, _) = Stops::new();
        self.running.insert(id.clone(), stop);
        let waiting = WaitingCleanup {
            spec,
            handles,
            ended: None,
            rest: false,
        };
        self.start_waiting(&id, waiting, to_master).await;
    }

    /// Journals an attempt about to start: first what its cleanup would need after a restart.
    async fn record_start(
        &self,
        spec: &AttemptSpec,
        assign: &pb::AssignTask,
        external: Option<&[u8]>,
    ) -> std::io::Result<()> {
        if spec.task.cleanup.is_some() {
            let mut assign = assign.clone();
            if let Some(inputs) = external {
                assign.inputs_json = inputs.to_vec();
                assign.inputs_external = false;
            }
            let kept = KeptAttempt {
                assign,
                code_dir: spec.code_dir.clone(),
                code_hash: self.code_hash_of(spec),
                root_copies: spec.root_copies.clone(),
                script_dir: spec.script_dir.clone(),
            };
            let journal = self.journal.clone();
            let id = spec.attempt_id.clone();
            tokio::task::spawn_blocking(move || journal.keep(&id, &kept))
                .await
                .map_err(std::io::Error::other)
                .and_then(|kept| kept)?;
        }
        self.record(&spec.attempt_id, JournalRecord::Running(None))
            .await
    }

    /// The hash of the code `spec` was planned against.
    fn code_hash_of(&self, spec: &AttemptSpec) -> Option<String> {
        self.watchers
            .get(&spec.dag_root)
            .and_then(RootWatcher::hash)
            .map(str::to_owned)
    }

    /// A task with a cleanup: its handles go to the journal, a drain leaves room for one try,
    /// and a timeout too long for the grace period is warned of, once per task (decision 250).
    fn note_cleanup(&mut self, spec: &mut AttemptSpec, assign: &pb::AssignTask) {
        let Some(cleanup) = &spec.task.cleanup else {
            return;
        };
        spec.journal = Some(self.journal.clone());
        let done = self.done_tx.clone();
        let attempt_id = spec.attempt_id.clone();
        spec.handle_sink = Some(HandleSink(Arc::new(move |position, handle| {
            let _ = done.send(Done::Handle {
                attempt_id: attempt_id.clone(),
                position,
                handle: handle.clone(),
            });
        })));
        let room = cleanup
            .timeout_ms
            .map_or(self.settings.setup.timeouts.cleanup, Duration::from_millis);
        self.cleanup_room.insert(spec.attempt_id.clone(), room);
        let drain = self.settings.drain;
        let dag = assign.run.as_ref().map_or("", |run| run.dag_id.as_str());
        // The default is said once, at the worker's start: here, what the task itself asks for.
        if cleanup.timeout_ms.is_some()
            && !drain.cleanup_fits(room)
            && self
                .warned_room
                .insert((dag.to_owned(), assign.task_id.clone()))
        {
            tracing::warn!(
                dag,
                task = assign.task_id.as_str(),
                cleanup_timeout = ?room,
                grace = ?drain.grace,
                "the task's `cleanup_timeout` doesn't fit in `termination_grace_period`: whatever \
                 ends the worker at the end of a drain (a pod's kill, a service manager) would \
                 lose its cleanup"
            );
        }
    }

    /// Runs a started attempt in the background; what happens is reported on `done_tx`.
    fn launch(&self, spec: AttemptSpec, plan: Plan, stopped: Stops) {
        let executor = self.executor.clone();
        let done = self.done_tx.clone();
        let on_spawn = self.done_tx.clone();
        let attempt_id = spec.attempt_id.clone();
        let spawned: Spawned = Arc::new(move |identity, forked| {
            let _ = on_spawn.send(Done::Spawned {
                attempt_id: attempt_id.clone(),
                identity,
                forked,
            });
        });
        let on_end = self.done_tx.clone();
        let ended: TaskEnded = Box::new(move |finished| {
            let _ = on_end.send(Done::TaskEnded(finished));
        });
        tokio::spawn(async move {
            let finished = executor.run(spec, plan, stopped, spawned, ended).await;
            let _ = done.send(Done::Attempt(finished));
        });
    }

    /// Keeps the other roots' copies a task was planned against while it runs: its script
    /// root's, the other DAG roots' on its `sys.path` (held by their folder, named after the
    /// hash).
    fn remember_other_copies(&mut self, spec: &AttemptSpec) {
        let held: Vec<String> = spec
            .root_copies
            .values()
            .chain(&spec.script_dir)
            .filter_map(|dir| Some(dir.file_name()?.to_string_lossy().into_owned()))
            .collect();
        if !held.is_empty() {
            self.other_copies.insert(spec.attempt_id.clone(), held);
        }
    }

    fn release_code(&mut self, attempt_id: &str) -> Option<String> {
        self.other_copies.remove(attempt_id);
        self.rest_cleaning.remove(attempt_id);
        self.code_hashes.remove(attempt_id)
    }

    /// Notes the hash of the task's root now, for its `Finished`.
    fn remember_code(&mut self, assign: &pb::AssignTask, spec: &AttemptSpec) {
        let attempt_id = spec.attempt_id.as_str();
        self.remember_other_copies(spec);
        if let Some(hash) = assign
            .dag_file
            .as_ref()
            .and_then(|f| self.watchers.get(&f.root))
            .and_then(RootWatcher::hash)
        {
            self.code_hashes
                .insert(attempt_id.to_owned(), hash.to_owned());
        }
    }

    /// Why this worker takes no new attempt now, if it doesn't: draining, or no free slot.
    fn cannot_take(&self, wait_step: bool) -> Option<(pb::RejectReason, &'static str)> {
        if self.drain.is_some() {
            Some((pb::RejectReason::Draining, "the worker is draining"))
        } else if wait_step && self.free_watcher_slots() == 0 {
            Some((pb::RejectReason::NoFreeSlot, "no free watcher slot"))
        } else if !wait_step && self.free_slots() == 0 {
            Some((pb::RejectReason::NoFreeSlot, "no free slot"))
        } else {
            None
        }
    }

    /// Plans an attempt here; a DAG file missing from the code the master expects fails it
    /// (the file is gone) instead of rejecting it as "code not there yet".
    fn plan(&self, spec: &mut AttemptSpec, assign: &pb::AssignTask) -> Result<Plan, NotRun> {
        spec.default_timeout(assign, &self.settings.setup.timeouts);
        // The copy of the code the root's reported hash is of (decision 102); the configured
        // path until the root's first scan.
        let code = self
            .watchers
            .get(&spec.dag_root)
            .and_then(RootWatcher::code_dir);
        spec.code_dir = code.map(Path::to_path_buf);
        spec.root_copies = self
            .watchers
            .iter()
            .filter(|(name, _)| {
                **name != spec.dag_root && !self.settings.setup.script_roots.contains_key(*name)
            })
            .filter_map(|(name, watcher)| Some((name.clone(), watcher.code_dir()?.to_path_buf())))
            .collect();
        // A script task's script root: its copy at the hash folded into the task's (301).
        if let TaskKind::Script { job, .. } = &spec.task.kind {
            let root = job.split_once(':').map_or(job.as_str(), |(root, _)| root);
            spec.script_dir = self
                .watchers
                .get(root)
                .filter(|_| self.settings.setup.script_roots.contains_key(root))
                .and_then(RootWatcher::code_dir)
                .map(Path::to_path_buf);
        }
        // A moved cleanup runs only the cleanup: what the task itself needed doesn't matter here.
        let mut moved = None;
        if assign.cleanup_only.is_some() {
            let mut cleanup = spec.clone();
            use_cleanup_script(&mut cleanup.task);
            moved = Some(cleanup);
        }
        let planning = moved.as_ref().unwrap_or(spec);
        let planned = match code {
            Some(dir) => plan_at(planning, &self.settings.setup, dir),
            None => plan(planning, &self.settings.setup),
        };
        match planned {
            Err(NotRun::Rejected {
                reason: pb::RejectReason::DagFileMissing | pb::RejectReason::ScriptReferenceNotFound,
                detail,
            }) if self.has_expected_code(assign) => Err(NotRun::Failed {
                kind: DAG_FILE_MISSING.to_owned(),
                message: format!("{detail}, in the newest code of its root: the file is gone"),
            }),
            planned => planned,
        }
    }

    /// Whether the task's root is here at the hash the master expects.
    fn has_expected_code(&self, assign: &pb::AssignTask) -> bool {
        let expected = assign.expected_code_hash.as_str();
        !expected.is_empty()
            && assign
                .dag_file
                .as_ref()
                .and_then(|f| self.reported_hash(&f.root))
                .as_deref()
                == Some(expected)
    }

    /// Why the task's root here isn't the code the master expects (`None` when it is, or
    /// when the root isn't here at all: planning rejects that).
    fn code_mismatch(&self, assign: &pb::AssignTask) -> Option<String> {
        let root = assign.dag_file.as_ref()?.root.as_str();
        let scanned = self
            .watchers
            .get(root)
            .and_then(RootWatcher::hash)
            .is_some();
        let Some(here) = self.reported_hash(root) else {
            // Withdrawn (303): it lacks a script root the root uses, so runs none of its tasks.
            return scanned.then(|| {
                format!(
                    "root `{root}` is withdrawn here: a script root it uses isn't on this worker"
                )
            });
        };
        let expected = assign.expected_code_hash.as_str();
        (!expected.is_empty() && here != expected)
            .then(|| format!("root `{root}` here is at {here}, the task expects {expected}"))
    }

    /// Removes the copies of code (decision 102) nothing needs any more: each root's but its
    /// current hash's and those a running attempt or parse job imports from.
    fn collect_code(&self) {
        let mut in_use: std::collections::BTreeSet<String> = self
            .code_hashes
            .values()
            .chain(self.other_copies.values().flatten())
            .chain(self.parsing.keys())
            .map(|hash| hash.replace(':', "-"))
            .collect();
        in_use.extend(self.executor.code_in_use());
        for watcher in self.watchers.values() {
            let Ok(entries) = std::fs::read_dir(watcher.store()) else {
                continue;
            };
            let current = watcher.code_dir();
            for entry in entries.flatten() {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                if Some(path.as_path()) == current || in_use.contains(&name) {
                    continue;
                }
                if let Err(error) = crate::scan::remove_capture(&path) {
                    tracing::warn!(path = %path.display(), %error, "can't remove old DAG code");
                }
            }
        }
    }

    fn parse(&mut self, job: pb::AssignParse) {
        let Some(root) = job.root else {
            return;
        };
        if self.drain.is_some() {
            // Sent before the master knew: it gives the job to another worker.
            tracing::info!(
                root = root.root.as_str(),
                "parse job while draining; left to others"
            );
            return;
        }
        let Some(watcher) = self.watchers.get(&root.root) else {
            tracing::warn!(
                root = root.root.as_str(),
                "parse job for a root this worker doesn't have"
            );
            return;
        };
        if self.reported_hash(&root.root).as_deref() != Some(root.content_hash.as_str()) {
            // The root changed since: its new hash was (or will be) reported.
            return;
        }
        let Some(copy) = watcher.hash().map(str::to_owned) else {
            return;
        };
        let (Some(snapshot), Some(code)) = (watcher.snapshot(), watcher.code_dir()) else {
            return;
        };
        // The copy of the code that hash is of (decision 102), not the configured path, which
        // git-sync may have moved on (B58).
        let mut files = snapshot.python_files();
        // Only the files chosen, for `schedr dags test` of one file (decision 117).
        if let Some(only) = self.settings.setup.parse_only.get(&root.root) {
            files.retain(|file| only.contains(file));
        }
        let dir = code.to_path_buf();
        let done = self.done_tx.clone();
        if self.settings.setup.script_roots.contains_key(&root.root) {
            // Read here, in Rust, never imported (decision 293).
            let files: Vec<String> = snapshot.files().keys().cloned().collect();
            let wanted: Vec<String> = files
                .iter()
                .filter(|file| crate::jobs::is_config(file))
                .cloned()
                .collect();
            *self.parsing.entry(copy.clone()).or_default() += 1;
            tokio::task::spawn_blocking(move || {
                let parsed = crate::jobs::read_root(&root.root, &dir, (&files, &wanted), None);
                let _ = done.send(Done::Parse {
                    job_id: job.parse_job_id,
                    root,
                    copies: vec![copy],
                    files: Ok(parsed),
                });
            });
            return;
        }
        let (code, mut copies) = self.parse_code(&root.root);
        copies.push(copy);
        for copy in &copies {
            *self.parsing.entry(copy.clone()).or_default() += 1;
        }
        let executor = self.executor.clone();
        tokio::spawn(async move {
            let parsed = executor.parse(root.root.clone(), dir, files, code).await;
            let _ = done.send(Done::Parse {
                job_id: job.parse_job_id,
                root,
                copies,
                files: parsed,
            });
        });
    }

    /// The copies of the other roots a parse of `root` sees (script roots for a
    /// `ScriptOperator`, DAG roots on `sys.path`), with their content hashes to hold meanwhile.
    fn parse_code(&self, root: &str) -> (ParseCode, Vec<String>) {
        let mut code = ParseCode::default();
        let mut held = Vec::new();
        for (name, watcher) in &self.watchers {
            let (Some(hash), Some(dir)) = (watcher.hash(), watcher.code_dir()) else {
                continue;
            };
            if name == root {
                continue;
            }
            if let Some(configured) = self.settings.setup.script_roots.get(name) {
                code.scripts.insert(
                    name.clone(),
                    crate::jobs::ScriptRoot {
                        configured: configured.clone(),
                        copy: dir.to_path_buf(),
                    },
                );
            } else {
                code.roots.insert(name.clone(), dir.to_path_buf());
            }
            held.push(hash.to_owned());
        }
        (code, held)
    }

    /// A connections job (decisions 174, 193): run beside the tasks, no slot, one at a time;
    /// a job this worker can't run is answered with why.
    fn connection_job(&mut self, job: pb::ConnectionJob) {
        let job_id = job.job_id.clone();
        let root = job.root.unwrap_or_default();
        let registry: Registry = match serde_json::from_slice(&job.connections_json) {
            Ok(registry) => registry,
            Err(e) => {
                tracing::warn!(job_id, error = %e, "a connections job that doesn't read");
                return;
            }
        };
        // The copy of the code it was sent for (a test imports from it), held meanwhile.
        let watcher = self.watchers.get(&root.root);
        let current = self.reported_hash(&root.root).as_deref() == Some(root.content_hash.as_str());
        let code_dir = watcher
            .filter(|_| current)
            .and_then(|w| w.code_dir().map(Path::to_path_buf));
        // The copy's content hash, by which it is kept meanwhile.
        let hash = code_dir
            .as_ref()
            .and_then(|_| watcher.and_then(RootWatcher::hash).map(str::to_owned));
        if let Some(hash) = &hash {
            *self.parsing.entry(hash.clone()).or_default() += 1;
        }
        let test = job
            .test
            .map(|t| (t.name, Duration::from_millis(t.timeout_ms)));
        if test.is_some() && code_dir.is_none() {
            let name = test.map(|(name, _)| name).unwrap_or_default();
            let why = format!(
                "this worker no longer has root `{}` at the code it was sent for: try again",
                root.root
            );
            let _ = self.done_tx.send(Done::Connections {
                job_id,
                hash,
                attempt_id: None,
                reports: vec![crate::connection_jobs::failed(&name, why)],
            });
            return;
        }
        // Its time counts from now, its wait for its turn included: the caller waits no
        // longer than that and a margin.
        let allowed = test.as_ref().map_or(
            Duration::from_millis(schedr_model::commands::CONNECTION_CHECK_TIMEOUT_MS),
            |(_, timeout)| *timeout,
        );
        let deadline = tokio::time::Instant::now() + allowed;
        // A name of its own for its folders and its journal record.
        let attempt_id = format!("connection-test-{}", jiff::Timestamp::now().as_nanosecond());
        let tested = test.is_some().then(|| attempt_id.clone());
        let job = ConnectionJob {
            root: root.root,
            code_dir,
            registry: Arc::new(registry),
            test,
            cancel_grace: self.settings.setup.timeouts.connection_cancel_grace,
            attempt_id: attempt_id.clone(),
            deadline,
        };
        let executor = self.executor.clone();
        let done = self.done_tx.clone();
        let on_spawn = self.done_tx.clone();
        // Recorded as an attempt's process: a worker killed meanwhile stops it on its start.
        let spawned: Spawned = Arc::new(move |identity, _| {
            let _ = on_spawn.send(Done::Spawned {
                attempt_id: attempt_id.clone(),
                identity,
                forked: false,
            });
        });
        let one_at_a_time = Arc::clone(&self.connection_jobs);
        tokio::spawn(async move {
            let reports = match tokio::time::timeout_at(deadline, one_at_a_time.lock()).await {
                Ok(_turn) => executor.connection_job(job, spawned).await,
                Err(_) => job
                    .registry
                    .names()
                    .map(|name| {
                        crate::connection_jobs::failed(
                            name,
                            crate::connection_jobs::out_of_time(
                                "wait for its turn (one connections job at a time per worker)",
                            ),
                        )
                    })
                    .collect(),
            };
            let _ = done.send(Done::Connections {
                job_id,
                hash,
                attempt_id: tested,
                reports,
            });
        });
    }

    /// Records what ended, and reports it over `link` (the outbound stream and the client)
    /// when connected.
    async fn on_done(
        &mut self,
        done: Done,
        link: Option<(
            &mpsc::UnboundedSender<pb::WorkerMessage>,
            &mut MasterClient<Channel>,
        )>,
    ) {
        match done {
            Done::Spawned {
                attempt_id,
                identity,
                forked,
            } => {
                // `Started` went before the process existed, as `spawn`; a repeat sets the
                // method (the master keeps the first start time).
                let ended = self.cleaning.get(&attempt_id).cloned();
                if forked
                    && ended.is_none()
                    && let Some((to_master, _)) = &link
                {
                    let _ = to_master.send(wrap(worker_message::Message::Started(pb::Started {
                        attempt_id: attempt_id.clone(),
                        start_method: pb::StartMethod::Fork.into(),
                    })));
                }
                let record = match ended {
                    Some(ended) => JournalRecord::Cleaning(ended, Some(identity)),
                    None => JournalRecord::Running(Some(identity)),
                };
                if let Err(error) = self.record(&attempt_id, record).await {
                    tracing::warn!(attempt_id, %error, "can't record the task's process");
                }
            }
            Done::TaskEnded(finished) => {
                self.task_ended(finished, link.map(|(to_master, _)| to_master))
                    .await;
            }
            Done::Handle {
                attempt_id,
                position,
                handle,
            } => {
                if let Some((to_master, _)) = link {
                    let _ = to_master.send(handles_message(attempt_id, position, &[handle]));
                }
            }
            Done::Attempt(mut finished) => {
                fit_result(&mut finished, self.master_limit);
                let status = attempt_status_from_proto(finished.status)
                    .map_or_else(|_| "unknown".to_owned(), |s| s.to_string());
                self.metrics.attempt_ended(&status);
                if let Some(hash) = self.release_code(&finished.attempt_id) {
                    with_code_hash(&mut finished, hash);
                    self.collect_code();
                }
                if let Err(error) = self
                    .record(
                        &finished.attempt_id,
                        JournalRecord::Finished(finished.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        attempt_id = finished.attempt_id.as_str(),
                        %error,
                        "can't record the result; it is lost if the agent restarts before the \
                         master acknowledges it"
                    );
                }
                self.ended_running(&finished.attempt_id);
                self.cleaning.remove(&finished.attempt_id);
                self.cleanup_room.remove(&finished.attempt_id);
                self.stopped.remove(&finished.attempt_id);
                self.gates.remove(&finished.attempt_id);
                self.unacked
                    .insert(finished.attempt_id.clone(), finished.clone());
                if let Some((to_master, _)) = link {
                    let _ = to_master.send(wrap(worker_message::Message::Finished(finished)));
                }
            }
            Done::Parse {
                job_id,
                root,
                copies,
                files,
            } => {
                for copy in copies {
                    if let Some(count) = self.parsing.get_mut(&copy) {
                        *count -= 1;
                        if *count == 0 {
                            self.parsing.remove(&copy);
                        }
                    }
                }
                self.collect_code();
                self.report_parse(job_id, root, files, link);
            }
            Done::Connections {
                job_id,
                hash,
                attempt_id,
                reports,
            } => {
                self.connections_done(job_id, hash, attempt_id, &reports, link.map(|(to, _)| to))
                    .await;
            }
        }
    }

    /// A connections job ended: its code and journal record let go, its answer sent (kept
    /// while no master listens).
    async fn connections_done(
        &mut self,
        job_id: String,
        hash: Option<String>,
        attempt_id: Option<String>,
        reports: &[schedr_model::commands::ConnectionReport],
        to_master: Option<&mpsc::UnboundedSender<pb::WorkerMessage>>,
    ) {
        if let Some(attempt_id) = attempt_id {
            self.forget(&attempt_id).await;
        }
        if let Some(hash) = hash
            && let Some(count) = self.parsing.get_mut(&hash)
        {
            *count -= 1;
            if *count == 0 {
                self.parsing.remove(&hash);
            }
            self.collect_code();
        }
        let report_json = reports
            .iter()
            .filter_map(|report| serde_json::to_vec(report).ok())
            .collect();
        let done = pb::ConnectionJobDone {
            job_id,
            report_json,
        };
        if let Some(to_master) = to_master {
            let _ = to_master.send(wrap(worker_message::Message::ConnectionJobDone(done)));
        } else {
            // Sent once registered again (the master drops an answer it gave up on).
            if self.unsent_answers.len() == UNSENT_ANSWERS {
                self.unsent_answers.pop_front();
            }
            self.unsent_answers.push_back(done);
        }
    }

    /// Uploads a parse job's result over `link`, or says why it can't.
    fn report_parse(
        &self,
        job_id: String,
        root: pb::RootHash,
        files: Result<Vec<ParsedFile>, String>,
        link: Option<(
            &mpsc::UnboundedSender<pb::WorkerMessage>,
            &mut MasterClient<Channel>,
        )>,
    ) {
        match (files, link) {
            (Ok(_), None) => tracing::warn!(
                root = root.root.as_str(),
                "parse result dropped: no master connection (the job is given out again)"
            ),
            (Ok(files), Some((_, client))) => {
                let limit = self.master_limit;
                let result = pb::ParseResult {
                    parse_job_id: job_id,
                    worker_id: self.settings.worker_id.clone(),
                    root: Some(root),
                    schedr_version: AGENT_VERSION.to_owned(),
                    files: files
                        .iter()
                        .map(|file| fitted_file(parsed_file_to_proto(file), limit))
                        .collect(),
                };
                // In a task of its own: a large root's upload takes a while, and the
                // session's loop must keep sending heartbeats meanwhile (B34). A file a
                // message, so only one file's size counts against the limit (B35).
                let parts = tokio_stream::iter(parse_upload_parts(result));
                let mut client = client.clone();
                tokio::spawn(async move {
                    if let Err(status) = client.upload_parse_result(parts).await {
                        tracing::error!(%status, "parse result not uploaded");
                    }
                });
            }
            (Err(error), _) => {
                tracing::error!(root = root.root.as_str(), error, "parse job failed");
            }
        }
    }

    fn watch(&mut self, to_master: &mpsc::UnboundedSender<pb::WorkerMessage>) {
        let mut changed_any = false;
        for (name, watcher) in &mut self.watchers {
            match watcher.poll(Instant::now()) {
                Ok(Poll::Changed { .. }) => changed_any = true,
                Ok(_) => {}
                Err(error) => tracing::warn!(root = name.as_str(), %error, "can't scan the root"),
            }
        }
        if changed_any {
            // A script root's change is new code for the DAG roots using it too (decision 301).
            self.report_changes(to_master);
            // The previous code's copy, unless something still runs from it.
            self.collect_code();
        }
    }

    /// Whether this worker cleans the rest of attempt `id`'s handles (`CLEAN_REST`, B169).
    fn cleaning_rest(&self, id: &str) -> bool {
        self.rest_cleaning.contains(id)
    }

    /// How many cleanup handles attempt `id` has journaled: the master compares it with what
    /// it has (B169). Read from the journal, which has a handle before the loop hears of it
    /// (Codex's review of B169: a `Register` built in between said one fewer).
    fn handle_count(&self, id: &str) -> u32 {
        let waiting = self.waiting.get(id).map_or(0, |w| w.handles.len());
        let journaled = self.journal.handles(id).map_or_else(
            |error| {
                tracing::warn!(attempt_id = id, %error, "can't read the journaled handles");
                0
            },
            |handles| handles.len(),
        );
        u32::try_from(journaled.max(waiting)).unwrap_or(u32::MAX)
    }

    fn journal_entries(&self) -> Vec<pb::JournalEntry> {
        let running = self.running.keys().map(|id| pb::JournalEntry {
            attempt_id: id.clone(),
            state: pb::JournalState::Running.into(),
            handles: self.handle_count(id),
        });
        let finished = self.unacked.keys().map(|id| pb::JournalEntry {
            attempt_id: id.clone(),
            state: pb::JournalState::Finished.into(),
            handles: self.handle_count(id),
        });
        running.chain(finished).collect()
    }

    fn free_slots(&self) -> u32 {
        if self.drain.is_some() {
            return 0;
        }
        self.settings
            .slots
            .saturating_sub(u32::try_from(self.tasks_running()).unwrap_or(u32::MAX))
    }

    fn free_watcher_slots(&self) -> u32 {
        if self.drain.is_some() {
            return 0;
        }
        self.settings
            .watcher_slots
            .saturating_sub(u32::try_from(self.wait_steps.len()).unwrap_or(u32::MAX))
    }

    fn note_wait_step(&mut self, spec: &AttemptSpec) {
        if let schedr_model::TaskKind::Sensor { mode, .. } = spec.task.kind {
            self.wait_steps.insert(
                spec.attempt_id.clone(),
                mode == schedr_model::SensorMode::Poke,
            );
        }
    }

    fn ended_running(&mut self, attempt_id: &str) {
        self.running.remove(attempt_id);
        self.wait_steps.remove(attempt_id);
    }

    /// The attempts running on task slots.
    fn tasks_running(&self) -> usize {
        self.running.len().saturating_sub(self.wait_steps.len())
    }
}

/// Whether an assignment is of a wait step; one that doesn't read is found out later.
fn assigned_a_wait_step(assign: &pb::AssignTask) -> bool {
    serde_json::from_slice::<schedr_model::Task>(&assign.task_json)
        .is_ok_and(|task| matches!(task.kind, schedr_model::TaskKind::Sensor { .. }))
}

/// A heartbeat timer of `period`: a tick missed (the loop busy, the process paused) is
/// delayed, not caught up in a burst.
fn heartbeat_timer(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

/// When each unacknowledged result is sent again over a session, counted in heartbeats:
/// first at the second heartbeat that finds it unacknowledged (at least one interval after it
/// was sent), then after twice the last gap, up to [`MAX_RESEND_GAP`]; at most
/// [`MAX_RESENDS`] times.
#[derive(Debug, Default)]
struct Resends {
    /// Heartbeats so far.
    beat: u64,
    /// Per result: the heartbeat it is due at, the gap after that, and the sends so far.
    due: BTreeMap<String, Resend>,
}

#[derive(Debug, Clone, Copy)]
struct Resend {
    at: u64,
    gap: u32,
    sent: u32,
}

impl Resends {
    /// A heartbeat, `unacked` the results unacknowledged now: those due to be sent again; none
    /// when one has been sent [`MAX_RESENDS`] times already and is due again (the session is
    /// given up).
    fn due<'a>(&mut self, unacked: impl Iterator<Item = &'a String>) -> Option<Vec<String>> {
        self.beat += 1;
        let beat = self.beat;
        let mut due = BTreeMap::new();
        let mut send = Vec::new();
        for id in unacked {
            let mut resend = self.due.get(id).copied().unwrap_or(Resend {
                at: beat + 1,
                gap: 1,
                sent: 0,
            });
            if resend.at <= beat {
                if resend.sent == MAX_RESENDS {
                    return None;
                }
                send.push(id.clone());
                resend.gap = resend.gap.saturating_mul(2).min(MAX_RESEND_GAP);
                resend.at = beat + u64::from(resend.gap);
                resend.sent += 1;
            }
            due.insert(id.clone(), resend);
        }
        self.due = due;
        Some(send)
    }
}

/// The folder of root `name`'s code copies under the code directory: its name with anything
/// but letters, digits, `-` and `_` replaced, and a hash of the real name, so two roots never
/// share one (each root's cleanup removes what its folder holds).
fn code_folder(name: &str) -> String {
    use sha2::Digest as _;
    let readable: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let digest = sha2::Sha256::digest(name.as_bytes());
    format!(
        "{readable}-{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3]
    )
}

/// What a fetch of inputs ended with.
enum Fetched {
    Bytes(Vec<u8>),
    Failed(String),
    /// A chunk came past the `lease` after the master's last word: not word any more.
    Silent,
}

/// Fetches an attempt's inputs, noting each chunk in `heard` (from its value now) unless it
/// came past `lease` after the last.
async fn fetch_inputs(
    client: &mut MasterClient<Channel>,
    attempt_id: &str,
    heard: &watch::Sender<tokio::time::Instant>,
    lease: Option<Duration>,
) -> Fetched {
    let stream = client
        .fetch_inputs(pb::FetchInputsRequest {
            attempt_id: attempt_id.to_owned(),
        })
        .await;
    let mut stream = match stream {
        Ok(response) => response.into_inner(),
        Err(status) => return Fetched::Failed(status.to_string()),
    };
    let mut bytes = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(chunk)) => {
                let now = tokio::time::Instant::now();
                if lease.is_some_and(|lease| now >= *heard.borrow() + lease) {
                    return Fetched::Silent;
                }
                heard.send_replace(now);
                bytes.extend(chunk.data);
            }
            Ok(None) => return Fetched::Bytes(bytes),
            Err(status) => return Fetched::Failed(status.to_string()),
        }
    }
}

/// A root's hash as reported, of its kind: a script root or a DAG root.
/// A reported hash made from a root's content hash: `kind` (`script` or `dag`) and the
/// reported hashes of the script roots it uses, by name, folded in (decision 301; B167).
#[must_use]
pub fn fold_hash(kind: &str, content: &str, used: &[(String, String)]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    hasher.update(b"\n");
    hasher.update(content.as_bytes());
    for (name, hash) in used {
        hasher.update(b"\n");
        hasher.update(name.as_bytes());
        hasher.update(b"=");
        hasher.update(hash.as_bytes());
    }
    let digest = hasher.finalize();
    let mut text = String::with_capacity(crate::scan::HASH_PREFIX.len() + 64);
    text.push_str(crate::scan::HASH_PREFIX);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}

fn root_hash(setup: &WorkerSetup, name: &str, hash: &str) -> pb::RootHash {
    let kind = if setup.script_roots.contains_key(name) {
        pb::RootKind::Script
    } else {
        pb::RootKind::Dag
    };
    pb::RootHash {
        kind: kind.into(),
        root: name.to_owned(),
        content_hash: hash.to_owned(),
    }
}

/// gRPC's own message limit, assumed until the master says its own in `Registered`.
const GRPC_DEFAULT_LIMIT: usize = 4 << 20;

/// Room a message needs around its largest field (ids, versions, framing).
const MESSAGE_OVERHEAD: usize = 64 << 10;

/// A result that can't reach the master (bigger than its message `limit`) becomes a failed
/// attempt, `output_too_large` (not retried), before it is journalled: sent as it was, it
/// would end the worker's stream, and again on every `Register` (B33). The cleanup's error
/// gives way first: it must not cost the task its result.
fn fit_result(finished: &mut pb::Finished, limit: usize) {
    let fits = |finished: &pb::Finished| {
        prost::Message::encoded_len(finished).saturating_add(MESSAGE_OVERHEAD) <= limit
    };
    if fits(finished) {
        return;
    }
    if let Some(cleanup) = finished.cleanup.as_mut()
        && !cleanup.error_summary_json.is_empty()
    {
        let message = format!(
            "the cleanup's error took {} bytes, too many to report",
            cleanup.error_summary_json.len()
        );
        cleanup.error_summary_json = ErrorSummary::json("ResultTooLarge", &message).into_bytes();
        if fits(finished) {
            return;
        }
    }
    let size = prost::Message::encoded_len(finished);
    let message = format!(
        "the result is {size} bytes, more than the master accepts ({limit} bytes, [master] \
         grpc_max_message_size): lower the task's max_output_size, or raise the setting"
    );
    tracing::error!(attempt_id = finished.attempt_id.as_str(), %message, "result too large");
    finished.status = pb::AttemptStatus::Failed.into();
    finished.output_json = Vec::new();
    finished.outcome = String::new();
    "output_too_large".clone_into(&mut finished.error_kind);
    finished.error_summary_json = ErrorSummary::json("ResultTooLarge", &message).into_bytes();
}

/// A parsed file as uploaded: itself, or, when it is too big for the master's message
/// `limit`, an import error saying so and naming the setting (B35).
fn fitted_file(file: pb::ParsedFile, limit: usize) -> pb::ParsedFile {
    let size = prost::Message::encoded_len(&file);
    if size.saturating_add(MESSAGE_OVERHEAD) <= limit {
        return file;
    }
    let message = format!(
        "the file's parse result is {size} bytes, more than the master accepts ({limit} \
         bytes, [master] grpc_max_message_size): raise the setting, or split the file"
    );
    tracing::error!(path = file.path.as_str(), %message, "parsed file not uploaded");
    pb::ParsedFile {
        path: file.path,
        dag_json: Vec::new(),
        import_error: Some(pb::ImportError {
            exception: schedr_model::runner::PARSE_RESULT_TOO_LARGE_EXCEPTION.to_owned(),
            message,
            traceback: String::new(),
        }),
        connection_json: Vec::new(),
        // Kept: its DAG root still uses them (decision 304).
        script_roots: file.script_roots,
    }
}

/// How long a deregistered agent waits for the master to end the stream.
pub const DEREGISTER_WAIT: Duration = Duration::from_secs(5);

/// How often a drain checks its deadline and whether it is done.
pub const DRAIN_TICK: Duration = Duration::from_millis(100);

/// The next change of the shutdown request count; never, without one (or once it is gone).
async fn next_request(shutdown: &mut Option<watch::Receiver<u32>>) -> u32 {
    if let Some(requests) = shutdown
        && requests.changed().await.is_ok()
    {
        return *requests.borrow();
    }
    std::future::pending().await
}

/// Handles for the master, from place `first` on.
fn handles_message(attempt_id: String, first: u32, handles: &[CleanupHandle]) -> pb::WorkerMessage {
    wrap(worker_message::Message::HandlesAdded(pb::HandlesAdded {
        attempt_id,
        first,
        handle_json: handles
            .iter()
            .filter_map(|handle| serde_json::to_vec(handle).ok())
            .collect(),
    }))
}

/// A task's end, as the master hears of it.
fn task_ended_message(result: pb::Finished) -> pb::WorkerMessage {
    wrap(worker_message::Message::TaskEnded(pb::TaskEnded {
        result: Some(result),
    }))
}

/// Why a session ends when the master has gone silent.
const NO_WORD: &str = "no word from the master for the worker lease";

/// The earlier of two moments, either of them maybe none.
fn earliest(
    a: Option<tokio::time::Instant>,
    b: Option<tokio::time::Instant>,
) -> Option<tokio::time::Instant> {
    a.into_iter().chain(b).min()
}

/// Why a session ends when the answer to its `Register` no longer stands.
const TOO_LATE: &str = "the master's answer came after the worker lease; asking again";

/// Waits until `when`; forever without one.
async fn at(when: Option<tokio::time::Instant>) {
    match when {
        Some(when) => tokio::time::sleep_until(when).await,
        None => std::future::pending().await,
    }
}

/// The handles a moved cleanup came with.
fn moved_handles(only: &pb::CleanupOnly) -> Result<Vec<CleanupHandle>, serde_json::Error> {
    only.handle_json
        .iter()
        .map(|json| serde_json::from_slice(json))
        .collect()
}

/// The result of a task the agent's restart stopped: `lost`, as the master would end it, and
/// using a retry (the crash may have been its own).
fn restarted(attempt_id: &str) -> pb::Finished {
    let message = "the worker's agent restarted while the attempt ran (possibly out of memory)";
    pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: pb::AttemptStatus::Lost.into(),
        error_summary_json: ErrorSummary::json("WorkerRestarted", message).into_bytes(),
        ..pb::Finished::default()
    }
}

const fn wrap(message: worker_message::Message) -> pb::WorkerMessage {
    pb::WorkerMessage {
        message: Some(message),
    }
}

/// The result of an attempt that had secrets: its Python output and error summary masked
/// (a command's are its masked streams already), and a failure after a failed on-demand read
/// given that read's error kind (decision 168).
fn ended_with_secrets(finished: &mut pb::Finished, secrets: &AttemptSecrets, python: bool) {
    if python && !secrets.mask.is_empty() {
        for field in [&mut finished.output_json, &mut finished.error_summary_json] {
            if field.is_empty() {
                continue;
            }
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(field) {
                *field = serde_json::to_vec(&secrets.mask.mask_json(&value)).unwrap_or_default();
            } else {
                *field = secrets.mask.mask(field);
            }
        }
    }
    let failed = finished.status == i32::from(pb::AttemptStatus::Failed);
    if failed
        && finished.error_kind.is_empty()
        && let Some(kind) = secrets.failed_kind()
    {
        kind.clone_into(&mut finished.error_kind);
    }
}

/// The log's warning for each secret too short to be masked (never its value), once: those
/// noted since the last call.
fn too_short_lines(secrets: &AttemptSecrets, min: usize) -> String {
    use std::fmt::Write as _;
    let short = std::mem::take(
        &mut *secrets
            .too_short
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    let mut lines = String::new();
    for name in &short {
        let _ = writeln!(
            lines,
            "schedr: `{name}` is too short to be masked (under {min} characters, \
             [secret_storage] min_mask_length): it shows in this log as it is"
        );
    }
    lines
}

/// The log's note of each secret masked (decision 167: an output may have changed).
fn masked_lines(secrets: &AttemptSecrets) -> String {
    use std::fmt::Write as _;
    let mut lines = String::new();
    for (label, count) in secrets.mask.counts() {
        let times = if count == 1 {
            "once".to_owned()
        } else {
            format!("{count} times")
        };
        let _ = writeln!(
            lines,
            "schedr: `{label}` was masked {times} in this task's log and output (`****`): \
             every text equal to its value is masked, also where it isn't the secret"
        );
    }
    // A value can be a word of the note, or its own path or key: masked here too.
    secrets.mask.mask_text(&lines)
}

/// Appends the agent's own lines to a task's log (never through a link, as the log is
/// opened for the task).
async fn append_log(path: &Path, text: &str) {
    if text.is_empty() {
        return;
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).append(true);
    match crate::process::open_regular(options, path).await {
        Ok(mut file) => {
            use tokio::io::AsyncWriteExt as _;
            let _ = file.write_all(text.as_bytes()).await;
            let _ = file.flush().await;
        }
        Err(error) => tracing::warn!(%error, path = %path.display(), "can't write to a task log"),
    }
}

/// Sleeps for `timeout`; forever without one.
async fn sleep_for(timeout: Option<Duration>) {
    match timeout {
        Some(timeout) => tokio::time::sleep(timeout).await,
        None => std::future::pending().await,
    }
}

/// An attempt stopped while its secrets were fetched: ended as a stop before its process
/// started is (cancelled, interrupted or timed out).
fn stopped_before_start(
    spec: &AttemptSpec,
    reason: StopReason,
    duration: Duration,
) -> pb::Finished {
    let ran = Ran {
        supervised: crate::supervise::Supervised {
            termination: crate::status::Termination::Unknown,
            stopped: Some(reason),
            killed: false,
        },
        runner_result: None,
        output: None,
        duration,
        start: None,
        forked: false,
        ended: crate::usage::Ended::default(),
        moved_to: None,
        host_lost: None,
        struck: false,
    };
    finished(&spec.attempt_id, &spec.task, &ran)
}
