//! One attempt on the worker, from `AssignTask` to `Finished`: what to run and how it ended.
//!
//! Pure: [`AttemptSpec::from_assign`] reads the assignment, [`plan`] decides how to run it (a
//! Python task through the runner, a command rendered here) or why not (a rejection, or a
//! failure such as a reference that can't be rendered), and [`finished`] turns how the process
//! ended into the `Finished` message (see PROJECT.md, Task assignment). Starting processes is
//! [`crate::launch`]'s job.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use schedr_model::connection::Registry;
use schedr_model::runner::CleanupHandle;
use schedr_model::runner::{
    DagFile, ErrorSummary, RunInfo, RunnerMessage, SecretValue, TaskRequest,
};
use schedr_model::task::CleanupRun;
use schedr_model::{AttemptStatus, SecretDecl, SecretName, Task, TaskKind};
use schedr_proto::convert::{attempt_status_to_proto, run_kind_from_proto};
use schedr_proto::pb;
use serde_json::Value;

use crate::env::{RunVars, task_environment};
use crate::mask::Mask;
use crate::output::{TailBuffer, process_result};
use crate::process::ProcessSpec;
use crate::render::{RenderContext, command_line, render};
use crate::status::attempt_status;
use crate::supervise::Supervised;

#[cfg(test)]
mod tests;

/// Whether a task's cleanup may start here, with `cleanup_anywhere`: while the worker is cut
/// off, the cleanup may be on another worker already.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// It may.
    Open,
    /// Not until the master's answer.
    Held,
    /// Never: the master answered `Kill`.
    Dropped,
    /// On the handles from this one on: its cleanup moved without them (B169).
    Rest(u32),
}

/// What a cleanup passing its [`CleanupGate`] runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// All its handles.
    All,
    /// The handles from this one on (B169: its cleanup moved without them).
    From(usize),
}

/// The agent's [`Gate`] for one attempt, and when the master's word runs out: the last word
/// plus the worker lease (`None` before the master told its lease).
#[derive(Debug, Clone)]
pub struct CleanupGate {
    /// What the agent says.
    pub gate: tokio::sync::watch::Receiver<Gate>,
    /// When the master's word runs out.
    pub deadline: tokio::sync::watch::Receiver<Option<tokio::time::Instant>>,
}

/// Compared by what it says now, for a spec's equality.
impl PartialEq for CleanupGate {
    fn eq(&self, other: &Self) -> bool {
        *self.gate.borrow() == *other.gate.borrow()
    }
}

impl CleanupGate {
    /// Waits while held, and while open past the deadline: the agent's loop hasn't held it yet
    /// (B162: a worker frozen past its lease saw its task end before its overdue timer, and ran
    /// a cleanup the master had moved); it does next, and the master's answer decides, unless
    /// the master's word moves the deadline on first (a heartbeat answered just in time).
    /// Returns whether the cleanup may start, and on which handles.
    pub async fn passes(mut self) -> Option<Pass> {
        loop {
            let gate = *self.gate.borrow_and_update();
            let late = self
                .deadline
                .borrow_and_update()
                .is_some_and(|at| tokio::time::Instant::now() >= at);
            let changed = match gate {
                Gate::Dropped => return None,
                Gate::Rest(from) => return Some(Pass::From(from as usize)),
                Gate::Open if !late => return Some(Pass::All),
                Gate::Open => tokio::select! {
                    changed = self.gate.changed() => changed.is_ok(),
                    changed = self.deadline.changed() => changed.is_ok(),
                },
                Gate::Held => self.gate.changed().await.is_ok(),
            };
            if !changed {
                return None;
            }
        }
    }
}

/// The error kind of a command whose templates can't be rendered (a missing or truncated
/// input, a value with no text form).
pub const RENDER_FAILED: &str = "render_failed";

/// The error kind of a Python task whose runner ended without reporting a result.
pub const RUNNER_FAILED: &str = "runner_failed";

/// The error kind of a `CommandResource` setup that exited 0 without reporting its value.
pub const RESOURCE_NOT_REPORTED: &str = "resource_not_reported";

/// The error kind of a forked task whose end isn't known: its shim ended before reporting it.
pub const PROCESS_STATUS_UNKNOWN: &str = "process_status_unknown";

/// An assignment, read.
#[derive(Debug, Clone, PartialEq)]
pub struct AttemptSpec {
    /// The attempt's ID.
    pub attempt_id: String,
    /// The task.
    pub task: Task,
    /// The DAG root and file the task is defined in.
    pub dag_root: String,
    /// The file, relative to the root.
    pub dag_path: String,
    /// The run information.
    pub run: RunInfo,
    /// The upstream outputs the task uses, by task ID.
    pub inputs: BTreeMap<String, Value>,
    /// The upstream tasks' outcomes, by task ID: a name, or `null` for a skipped one.
    pub outcomes: BTreeMap<String, Value>,
    /// The time between SIGTERM and SIGKILL when it is stopped.
    pub cancel_grace: Duration,
    /// The secrets fetched for it, by name ([`AttemptSpec::needed_secrets`]): filled into its
    /// templates and sent to its runner. Empty until fetched.
    pub secrets: BTreeMap<SecretName, Value>,
    /// The values masked in its output.
    pub mask: Mask,
    /// The copy of its root's code it was planned against (decision 102); `None`: the
    /// configured root.
    pub code_dir: Option<PathBuf>,
    /// For a script task, the copy of its script root's code it was planned against; `None`:
    /// the configured script root.
    pub script_dir: Option<PathBuf>,
    /// The copies of the other DAG roots it was planned against, on a Python task's
    /// `sys.path` in place of their configured paths (a library in another root comes from
    /// one copy for the whole task).
    pub root_copies: BTreeMap<String, PathBuf>,
    /// Its root's connections (decision 174): rendered when the task opens one.
    pub connections: Arc<Registry>,
    /// A command's connections given as variables (`connections={"DB": warehouse}`, decision
    /// 184), by variable (`DB_URL`). Empty until its secrets are fetched.
    pub connection_env: BTreeMap<String, String>,
    /// Set for a connection test (decision 174): the runner opens and checks it, no task.
    pub connection_test: Option<String>,
    /// Set for a Python task's cleanup: the attempt's handles, for its function.
    pub cleanup_handles: Option<Vec<CleanupHandle>>,
    /// The journal its handles go to; `None` keeps them in memory only.
    pub journal: Option<crate::journal::Journal>,
    /// Told each handle, to send it on to the master; `None` keeps them here.
    pub handle_sink: Option<crate::handles::HandleSink>,
    /// Passed between the task's end and its cleanup, with `cleanup_anywhere` (decision 253).
    pub cleanup_gate: Option<CleanupGate>,
}

/// Why an assignment can't be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid assignment: {0}")]
pub struct InvalidAssignment(pub String);

impl AttemptSpec {
    /// Reads an assignment; `inputs` are the ones fetched separately when the assignment
    /// says they are external.
    ///
    /// # Errors
    ///
    /// [`InvalidAssignment`] for a task, run or inputs that don't read (the worker rejects it
    /// as an invalid specification).
    pub fn from_assign(
        assign: &pb::AssignTask,
        external_inputs: Option<&[u8]>,
    ) -> Result<Self, InvalidAssignment> {
        let invalid =
            |what: &str, e: &dyn std::fmt::Display| InvalidAssignment(format!("{what}: {e}"));
        let task: Task =
            serde_json::from_slice(&assign.task_json).map_err(|e| invalid("task", &e))?;
        let run = assign
            .run
            .as_ref()
            .ok_or_else(|| InvalidAssignment("no run information".to_owned()))?;
        let dag_file = assign
            .dag_file
            .as_ref()
            .ok_or_else(|| InvalidAssignment("no DAG file".to_owned()))?;
        let inputs_json = match external_inputs {
            Some(bytes) => bytes,
            None => assign.inputs_json.as_slice(),
        };
        let inputs = if inputs_json.is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_slice(inputs_json).map_err(|e| invalid("inputs", &e))?
        };
        Ok(Self {
            attempt_id: assign.attempt_id.clone(),
            task,
            dag_root: dag_file.root.clone(),
            dag_path: dag_file.path.clone(),
            run: RunInfo {
                dag_id: run.dag_id.clone(),
                run_id: run.run_id.clone(),
                kind: run_kind_from_proto(&run.kind).map_err(|e| invalid("run kind", &e))?,
                planned_for: time(run.planned_for.as_ref())
                    .ok_or_else(|| invalid("planned_for", &"missing"))?,
                since: time(run.since.as_ref()).ok_or_else(|| invalid("since", &"missing"))?,
                try_number: assign.try_number,
                map_index: u32::try_from(assign.map_index).ok(),
                params: if run.params_json.is_empty() {
                    serde_json::json!({})
                } else {
                    serde_json::from_slice(&run.params_json)
                        .map_err(|e| invalid("run params", &e))?
                },
            },
            inputs,
            outcomes: assign
                .upstream_outcomes
                .iter()
                .map(|(task, outcome)| {
                    let outcome = if outcome.is_empty() {
                        Value::Null
                    } else {
                        Value::String(outcome.clone())
                    };
                    (task.clone(), outcome)
                })
                .collect(),
            cancel_grace: Duration::from_millis(assign.cancel_grace_ms),
            secrets: BTreeMap::new(),
            mask: Mask::default(),
            code_dir: None,
            script_dir: None,
            root_copies: BTreeMap::new(),
            connections: Arc::new(if assign.connections_json.is_empty() {
                Registry::default()
            } else {
                serde_json::from_slice(&assign.connections_json)
                    .map_err(|e| invalid("connections", &e))?
            }),
            connection_env: BTreeMap::new(),
            connection_test: None,
            cleanup_handles: None,
            journal: None,
            handle_sink: None,
            cleanup_gate: None,
        })
    }

    /// The secrets fetched before it starts: those its references name, and those it
    /// declares (prefixes aside, read when the task asks), each once.
    #[must_use]
    pub fn needed_secrets(&self) -> Vec<SecretName> {
        let mut needed: Vec<SecretName> =
            self.task.secret_references().into_iter().cloned().collect();
        for declared in &self.task.secrets {
            if let SecretDecl::Secret(name) = declared
                && !needed.contains(name)
            {
                needed.push(name.clone());
            }
        }
        needed
    }

    /// Stand-ins for the secrets it needs (a key's `""`, a whole secret's `{}`): planning it
    /// before they are fetched shows whether it can run here at all.
    pub fn stand_in_secrets(&mut self) {
        self.secrets = self
            .needed_secrets()
            .into_iter()
            .map(|name| {
                let value = if name.key.is_some() {
                    Value::String(String::new())
                } else {
                    Value::Object(serde_json::Map::new())
                };
                (name, value)
            })
            .collect();
    }

    /// Gives a resource's cleanup without a timeout of its own the worker's
    /// `resource_cleanup_timeout`: a hung one would hold its run, which doesn't cancel it.
    pub fn default_timeout(&mut self, assign: &pb::AssignTask, timeouts: &WorkerTimeouts) {
        if assign.resource_cleanup && self.task.settings.timeout_ms.is_none() {
            self.task.settings.timeout_ms =
                Some(u64::try_from(timeouts.resource_cleanup.as_millis()).unwrap_or(u64::MAX));
        }
        if let TaskKind::Sensor {
            max_wait_ms: wait @ None,
            ..
        } = &mut self.task.kind
        {
            *wait = Some(u64::try_from(timeouts.sensor_max_wait.as_millis()).unwrap_or(u64::MAX));
        }
        if let TaskKind::Sensor {
            max_wait_ms: Some(wait),
            ..
        } = self.task.kind
        {
            // It ends itself at its `max_wait`, with `TimedOut`. This is for one that ignores the
            // cancel: past its grace, it is stopped like any task over its time.
            let margin = self.cancel_grace.saturating_add(timeouts.runner_start);
            let margin = u64::try_from(margin.as_millis()).unwrap_or(u64::MAX);
            self.task.settings.timeout_ms = Some(wait.saturating_add(margin));
        }
    }

    /// The task's timeout, if any.
    #[must_use]
    pub fn timeout(&self) -> Option<Duration> {
        self.task.settings.timeout_ms.map(Duration::from_millis)
    }
}

/// How long what a worker starts may take (`[worker]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerTimeouts {
    /// A runner's hello (`runner_start_timeout`).
    pub runner_start: Duration,
    /// A fork server's preloading (`fork_server_preload_timeout`).
    pub fork_preload: Duration,
    /// One try of a cleanup that sets no timeout of its own (`cleanup_timeout`).
    pub cleanup: Duration,
    /// How long a wait step without a `max_wait` waits (`[sensors] default_max_wait`).
    pub sensor_max_wait: Duration,
    /// One try of a resource's cleanup that sets no `timeout` (`resource_cleanup_timeout`).
    pub resource_cleanup: Duration,
    /// How long a stopped process's output may take to reach its log (`output_drain_timeout`).
    pub output_drain: Duration,
    /// How long a fork server may take to answer a request (`fork_answer_timeout`).
    pub fork_answer: Duration,
    /// How a fork server's restarts are spaced.
    pub fork_backoff: crate::fork::ForkBackoff,
    /// How often old logs are swept (`log_sweep_interval`).
    pub log_sweep: Duration,
    /// How an orphan is stopped.
    pub orphan: crate::journal::OrphanWaits,
    /// How long an orphan gets to end on SIGTERM (`orphan_grace`).
    pub orphan_grace: Duration,
    /// The first wait after the master connection drops (`reconnect_backoff_first`).
    pub reconnect_first: Duration,
    /// The longest one (`reconnect_backoff_max`).
    pub reconnect_max: Duration,
    /// How often the event loop turns at least, however quiet (`alive_interval`).
    pub alive: Duration,
    /// How long deregistering may take (`deregister_wait`).
    pub deregister: Duration,
    /// How often a drain looks at its state (`drain_tick`).
    pub drain_tick: Duration,
    /// How often a stopped process's group is looked at (`stop_poll`).
    pub stop_poll: Duration,
    /// How long a connection test's process gets from SIGTERM to SIGKILL
    /// (`connection_job_cancel_grace`).
    pub connection_cancel_grace: Duration,
    /// How long the log endpoint waits after a failed accept (`accept_retry`).
    pub accept_retry: Duration,
}

impl Default for WorkerTimeouts {
    /// What every worker runs with.
    fn default() -> Self {
        Self {
            runner_start: crate::launch::RUNNER_START_TIMEOUT,
            fork_preload: crate::fork::PRELOAD_TIMEOUT,
            cleanup: Duration::from_mins(5),
            sensor_max_wait: Duration::from_hours(24),
            resource_cleanup: Duration::from_mins(30),
            output_drain: crate::launch::OUTPUT_DRAIN_TIMEOUT,
            fork_answer: crate::fork::FORK_ANSWER_TIMEOUT,
            fork_backoff: crate::fork::ForkBackoff::default(),
            log_sweep: crate::retention::SWEEP_INTERVAL,
            orphan: crate::journal::OrphanWaits::default(),
            orphan_grace: crate::agent::ORPHAN_GRACE,
            reconnect_first: crate::agent::FIRST_BACKOFF,
            reconnect_max: crate::agent::MAX_BACKOFF,
            alive: crate::agent::ALIVE_INTERVAL,
            deregister: crate::agent::DEREGISTER_WAIT,
            drain_tick: crate::agent::DRAIN_TICK,
            stop_poll: crate::supervise::STOP_POLL,
            connection_cancel_grace: crate::connection_jobs::CANCEL_GRACE,
            accept_retry: Duration::from_millis(100),
        }
    }
}

/// What the worker knows to run tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSetup {
    /// The DAG roots, by name.
    pub roots: BTreeMap<String, PathBuf>,
    /// The roots of which only some `.py` files are parsed, those files by root (decision
    /// 117: `schedr dags test` of one file); a root not here is parsed whole.
    pub parse_only: BTreeMap<String, Vec<String>>,
    /// The script roots, by name (`SCHEDR_SCRIPT_PATH`): read for their `schedr.toml` files,
    /// never imported.
    pub script_roots: BTreeMap<String, PathBuf>,
    /// The interpreters it has (`[interpreters]`): each name's executable, for scripts.
    pub interpreters: BTreeMap<String, PathBuf>,
    /// The interpreter for Python tasks (`[dags] interpreter`).
    pub python: PathBuf,
    /// The agent's own environment (the allow-listed variables pass to tasks).
    pub agent_env: BTreeMap<String, String>,
    /// Agent variables passed to tasks on top of the allow-list (`[worker] pass_env`).
    pub pass_env: Vec<String>,
    /// Where task log files go.
    pub log_dir: PathBuf,
    /// Where a command's connection files go (decision 184): a folder per attempt, mode
    /// 0700, removed when it ends.
    pub connection_dir: PathBuf,
    /// How the memory watchdog watches tasks and parse runners.
    pub memory: crate::memory::MemoryWatch,
    /// How long what it starts may take.
    pub timeouts: WorkerTimeouts,
    /// The DAG interpreter's fork servers, when it has `fork_server = true`.
    pub fork_server: Option<crate::fork::ForkConfig>,
}

impl WorkerSetup {
    /// The log file of an attempt.
    #[must_use]
    pub fn log_file(&self, attempt_id: &str) -> PathBuf {
        self.log_dir.join(format!("{attempt_id}.log"))
    }

    /// Every root directory, as `sys.path` entries for the runner.
    #[must_use]
    pub fn sys_path(&self) -> Vec<String> {
        self.roots
            .values()
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    }

    /// As [`Self::sys_path`], with each root of `copies` at its copy instead (decision 102).
    #[must_use]
    pub fn sys_path_at(&self, copies: &BTreeMap<String, PathBuf>) -> Vec<String> {
        self.roots
            .iter()
            .map(|(name, p)| copies.get(name).unwrap_or(p))
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    }

    /// As [`Self::sys_path`], with root `root` at `dir` instead (the copy of its code,
    /// decision 102), so its modules come from the same code as its DAG file.
    #[must_use]
    pub fn sys_path_with(&self, root: &str, dir: &Path) -> Vec<String> {
        self.roots
            .iter()
            .map(|(name, p)| if name == root { dir } else { p.as_path() })
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    }
}

/// How to run an attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// A Python task: start the runner in task mode and send it this request.
    Python {
        /// The runner process (interpreter, `-m schedr.runner task`, environment, log).
        process: ProcessSpec,
        /// The request.
        request: Box<TaskRequest>,
    },
    /// A command: start this process.
    Command(ProcessSpec),
}

/// Why an attempt isn't run as planned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotRun {
    /// Rejected (not a try): with the protocol's reason and a detail.
    Rejected {
        /// The reason.
        reason: pb::RejectReason,
        /// What exactly, for the master's message.
        detail: String,
    },
    /// Failed before starting (e.g. a reference that can't be rendered).
    Failed {
        /// The error kind.
        kind: String,
        /// What went wrong.
        message: String,
    },
}

/// Plans an attempt on this worker.
///
/// # Errors
///
/// [`NotRun::Rejected`] when the DAG file isn't here (code not there yet) or the task can't
/// run on a worker (an empty task); [`NotRun::Failed`] when a command's templates can't be
/// rendered or its environment sets a reserved variable.
pub fn plan(spec: &AttemptSpec, setup: &WorkerSetup) -> Result<Plan, NotRun> {
    let root_dir = setup
        .roots
        .get(&spec.dag_root)
        .ok_or_else(|| NotRun::Rejected {
            reason: pb::RejectReason::DagFileMissing,
            detail: format!("DAG root `{}` isn't on this worker", spec.dag_root),
        })?;
    plan_at(spec, setup, root_dir)
}

/// As [`plan`], with the task's root at `root_dir`: the agent's copy of the code whose hash
/// it reported (decision 102), so the code that runs is the code hashed whatever happens to
/// the configured root meanwhile (git-sync moving its link and deleting the old checkout, an
/// edit in place).
///
/// # Errors
///
/// As [`plan`].
pub fn plan_at(spec: &AttemptSpec, setup: &WorkerSetup, root_dir: &Path) -> Result<Plan, NotRun> {
    let file = root_dir.join(&spec.dag_path);
    if !file.is_file() {
        return Err(NotRun::Rejected {
            reason: pb::RejectReason::DagFileMissing,
            detail: format!("{} not found on this worker", file.display()),
        });
    }
    let vars = run_vars(spec);
    let log_file = setup.log_file(&spec.attempt_id);
    let max_output_size = usize::try_from(spec.task.settings.max_output_size).unwrap_or(usize::MAX);
    match &spec.task.kind {
        TaskKind::Python { .. } | TaskKind::Sensor { .. } => {
            let env = task_environment(&setup.agent_env, &setup.pass_env, &BTreeMap::new(), &vars)
                .map_err(|e| failed(RENDER_FAILED, &e))?;
            Ok(Plan::Python {
                process: ProcessSpec {
                    program: setup.python.clone(),
                    args: vec![
                        // Not the working directory first: a root's file can't shadow what the runner imports (B178).
                        "-P".to_owned(),
                        "-m".to_owned(),
                        "schedr.runner".to_owned(),
                        "task".to_owned(),
                    ],
                    env,
                    working_dir: root_dir.to_path_buf(),
                    log_file,
                    max_output_size,
                    mask: spec.mask.clone(),
                    start_timeout: setup.timeouts.runner_start,
                    output_drain: setup.timeouts.output_drain,
                },
                request: Box::new(TaskRequest {
                    dag_file: DagFile {
                        root: spec.dag_root.clone(),
                        root_dir: root_dir.to_string_lossy().into_owned(),
                        path: spec.dag_path.clone(),
                    },
                    dag_id: spec.run.dag_id.clone(),
                    task: spec.task.clone(),
                    inputs: spec.inputs.clone(),
                    outcomes: spec.outcomes.clone(),
                    run: spec.run.clone(),
                    sys_path: setup.sys_path_at(
                        &spec
                            .root_copies
                            .clone()
                            .into_iter()
                            .chain([(spec.dag_root.clone(), root_dir.to_path_buf())])
                            .collect(),
                    ),
                    secrets: spec
                        .secrets
                        .iter()
                        .map(|(name, value)| SecretValue {
                            name: name.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                    connection_test: spec.connection_test.clone(),
                    cleanup: spec.cleanup_handles.clone(),
                }),
            })
        }
        TaskKind::Command { command, env, .. } => {
            let ctx = render_context(spec);
            let (program, args) =
                command_line(command, &ctx).map_err(|e| failed(RENDER_FAILED, &e))?;
            let mut task_env = render_env(env, &ctx)?;
            add_connection_env(&mut task_env, &spec.connection_env)?;
            let env = task_environment(&setup.agent_env, &setup.pass_env, &task_env, &vars)
                .map_err(|e| failed(RENDER_FAILED, &e))?;
            Ok(Plan::Command(ProcessSpec {
                program: PathBuf::from(program),
                args,
                env,
                working_dir: file
                    .parent()
                    .map_or_else(|| root_dir.to_path_buf(), Path::to_path_buf),
                log_file,
                max_output_size,
                mask: spec.mask.clone(),
                start_timeout: setup.timeouts.runner_start,
                output_drain: setup.timeouts.output_drain,
            }))
        }
        TaskKind::Empty => Err(NotRun::Rejected {
            reason: pb::RejectReason::InvalidSpecification,
            detail: "empty tasks are completed by the master".to_owned(),
        }),
        TaskKind::Script { .. } => plan_script(spec, setup, root_dir, &vars),
    }
}

/// A script task's process (decision 293): its interpreter running the script from its
/// script root's copy.
fn plan_script(
    spec: &AttemptSpec,
    setup: &WorkerSetup,
    root_dir: &Path,
    vars: &RunVars,
) -> Result<Plan, NotRun> {
    let TaskKind::Script {
        job,
        script,
        interpreter,
        args,
        env,
        working_dir,
        ..
    } = &spec.task.kind
    else {
        unreachable!("planned as a script")
    };
    let log_file = setup.log_file(&spec.attempt_id);
    let max_output_size = usize::try_from(spec.task.settings.max_output_size).unwrap_or(usize::MAX);
    // Checked when it was read; checked again, as the path is about to run (decision 293).
    if !crate::jobs::stays_inside(script)
        || !(working_dir.is_empty() || crate::jobs::stays_inside(working_dir))
    {
        return Err(NotRun::Failed {
            kind: RENDER_FAILED.to_owned(),
            message: format!("`{job}`: its script or working directory leaves its script root"),
        });
    }
    let script_dir = script_dir(spec, setup, job, root_dir)?;
    let program = setup
        .interpreters
        .get(interpreter)
        .ok_or_else(|| NotRun::Rejected {
            reason: pb::RejectReason::InterpreterUndefined,
            detail: format!(
                "interpreter `{interpreter}` of `{job}` isn't defined on this worker \
                     ([interpreters])"
            ),
        })?;
    let file = script_dir.join(script);
    if !file.is_file() {
        return Err(NotRun::Rejected {
            reason: pb::RejectReason::ScriptReferenceNotFound,
            detail: format!("`{job}`: its script `{script}` isn't on this worker"),
        });
    }
    can_run_cleanup_script(spec, setup, &script_dir, job)?;
    let ctx = render_context(spec);
    let mut argv = vec![file.to_string_lossy().into_owned()];
    for template in args {
        argv.push(render(template, &ctx).map_err(|e| failed(RENDER_FAILED, &e))?);
    }
    let mut task_env = render_env(env, &ctx)?;
    // Its root's libraries importable from anywhere in it (a Python script).
    task_env
        .entry(PYTHONPATH.to_owned())
        .or_insert_with(|| python_path(&script_dir, setup));
    let env = task_environment(&setup.agent_env, &setup.pass_env, &task_env, vars)
        .map_err(|e| failed(RENDER_FAILED, &e))?;
    Ok(Plan::Command(ProcessSpec {
        program: program.clone(),
        args: argv,
        env,
        working_dir: script_dir.join(working_dir),
        log_file,
        max_output_size,
        mask: spec.mask.clone(),
        start_timeout: setup.timeouts.runner_start,
        output_drain: setup.timeouts.output_drain,
    }))
}

/// Makes a script task run its cleanup script instead (with the task's environment and working
/// directory); any other task is left as it is. A setup's own cleanup reports no value.
pub fn use_cleanup_script(task: &mut Task) {
    let Some(CleanupRun::Script {
        script: own_script,
        interpreter: own_interpreter,
        args: own_args,
    }) = task.cleanup.as_ref().map(|cleanup| &cleanup.run)
    else {
        return;
    };
    if let TaskKind::Script {
        script,
        interpreter,
        args,
        resource,
        ..
    } = &mut task.kind
    {
        script.clone_from(own_script);
        interpreter.clone_from(own_interpreter);
        args.clone_from(own_args);
        *resource = None;
    }
}

/// A worker that couldn't run the task's cleanup script doesn't take the task: its cleanup
/// would fail after the task, and a moved one should go to a worker that can run it.
fn can_run_cleanup_script(
    spec: &AttemptSpec,
    setup: &WorkerSetup,
    script_dir: &Path,
    job: &str,
) -> Result<(), NotRun> {
    let Some(CleanupRun::Script {
        script,
        interpreter,
        ..
    }) = spec.task.cleanup.as_ref().map(|cleanup| &cleanup.run)
    else {
        return Ok(());
    };
    if !crate::jobs::stays_inside(script) {
        return Err(NotRun::Failed {
            kind: RENDER_FAILED.to_owned(),
            message: format!("`{job}`: its cleanup script leaves its script root"),
        });
    }
    if !setup.interpreters.contains_key(interpreter) {
        return Err(NotRun::Rejected {
            reason: pb::RejectReason::InterpreterUndefined,
            detail: format!(
                "interpreter `{interpreter}` of `{job}`'s cleanup isn't defined on this worker \
                 ([interpreters])"
            ),
        });
    }
    if !script_dir.join(script).is_file() {
        return Err(NotRun::Rejected {
            reason: pb::RejectReason::ScriptReferenceNotFound,
            detail: format!("`{job}`: its cleanup script `{script}` isn't on this worker"),
        });
    }
    Ok(())
}

/// The variable a Python script finds its root's libraries through.
const PYTHONPATH: &str = "PYTHONPATH";

/// A script's `PYTHONPATH`: its script root's copy, then the agent's own when the worker
/// passes it on (`[worker] pass_env`).
fn python_path(script_dir: &Path, setup: &WorkerSetup) -> String {
    let own = script_dir.to_string_lossy().into_owned();
    match setup.agent_env.get(PYTHONPATH) {
        Some(passed) if setup.pass_env.iter().any(|name| name == PYTHONPATH) => {
            format!("{own}:{passed}")
        }
        _ => own,
    }
}

/// The script root a script task's `job` (`<root>:<path>#<name>`) is in, as this worker has
/// it: the copy it was planned against, else the configured directory; the task's own root
/// when it is a standalone job's.
fn script_dir(
    spec: &AttemptSpec,
    setup: &WorkerSetup,
    job: &str,
    root_dir: &Path,
) -> Result<PathBuf, NotRun> {
    let root = job.split_once(':').map_or(job, |(root, _)| root);
    if let Some(dir) = &spec.script_dir {
        return Ok(dir.clone());
    }
    if root == spec.dag_root {
        return Ok(root_dir.to_path_buf());
    }
    setup
        .script_roots
        .get(root)
        .cloned()
        .ok_or_else(|| NotRun::Rejected {
            reason: pb::RejectReason::ScriptRootUnknown,
            detail: format!("script root `{root}` of `{job}` isn't on this worker"),
        })
}

/// What a command's or a script's templates are rendered with.
fn render_context(spec: &AttemptSpec) -> RenderContext {
    let mut ctx = RenderContext::new(
        &spec.run.dag_id,
        &spec.run.run_id,
        &spec.run.kind.to_string(),
        &spec.run.planned_for.to_string(),
        &spec.run.since.to_string(),
    );
    ctx.inputs.clone_from(&spec.inputs);
    ctx.outcomes.clone_from(&spec.outcomes);
    ctx.params.clone_from(&spec.run.params);
    ctx.secrets.clone_from(&spec.secrets);
    ctx
}

/// A task's `env`, rendered.
fn render_env(
    env: &BTreeMap<String, schedr_model::value::Template>,
    ctx: &RenderContext,
) -> Result<BTreeMap<String, String>, NotRun> {
    env.iter()
        .map(|(name, template)| Ok((name.clone(), render(template, ctx)?)))
        .collect::<Result<BTreeMap<_, _>, crate::render::RenderError>>()
        .map_err(|e| failed(RENDER_FAILED, &e))
}

/// How a started attempt ended.
#[derive(Debug, Clone)]
pub struct Ran {
    /// The process's end, and why the agent stopped it, if it did.
    pub supervised: Supervised,
    /// For a Python task: the runner's result message, if it sent one.
    pub runner_result: Option<RunnerMessage>,
    /// For a command: the tails of its stdout and stderr.
    pub output: Option<(TailBuffer, TailBuffer)>,
    /// How long it ran.
    pub duration: Duration,
    /// For a Python task that said hello: how long it took to start.
    pub start: Option<StartTimes>,
    /// Whether it was forked from a fork server (else spawned).
    pub forked: bool,
    /// What it used and whether it dumped core.
    pub ended: crate::usage::Ended,
    /// The code it ended on, when that isn't the code it started on: a poke moved to new code
    /// (decision 335).
    pub moved_to: Option<String>,
}

/// How long a Python task's process took to start, by phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartTimes {
    /// From starting the process to its runner's hello: the interpreter and `schedr`.
    pub runner: Duration,
    /// From sending the task to `task_imported`: the DAG file imported and the arguments
    /// built; `None` when that failed or the runner sent nothing.
    pub import: Option<Duration>,
}

/// The `Finished` message for an attempt of `task` that ran. A command succeeds with an exit
/// code of one of its outcomes (`SUCCESS` for 0 unless it declares others), which becomes its
/// outcome; any other code is a failure.
#[must_use]
pub fn finished(attempt_id: &str, task: &Task, ran: &Ran) -> pb::Finished {
    let termination = ran.supervised.termination;
    let success_codes: Vec<i32> = if ran.output.is_some() {
        task.outcomes.iter().filter_map(|o| o.exit_code).collect()
    } else {
        vec![0]
    };
    let status = attempt_status(termination, ran.supervised.stopped, &success_codes);
    let mut message = pb::Finished {
        code_hash: ran.moved_to.clone().unwrap_or_default(),
        attempt_id: attempt_id.to_owned(),
        status: attempt_status_to_proto(status).into(),
        duration_ms: Some(u64::try_from(ran.duration.as_millis()).unwrap_or(u64::MAX)),
        usage: ran.ended.usage.map(|u| pb::ProcessUsage {
            user_cpu_micros: u.user_cpu_micros,
            system_cpu_micros: u.system_cpu_micros,
            peak_rss_bytes: u.peak_rss_bytes,
            major_faults: u.major_faults,
            voluntary_context_switches: u.voluntary_context_switches,
            involuntary_context_switches: u.involuntary_context_switches,
        }),
        core_dumped: ran.ended.core_dumped,
        ..pb::Finished::default()
    };
    match termination {
        crate::status::Termination::Exited(code) => message.exit_code = Some(code),
        crate::status::Termination::Signaled(signal) => message.signal = Some(signal),
        crate::status::Termination::Unknown => {}
    }
    if let Some((stdout, stderr)) = &ran.output {
        let result = process_result(stdout, stderr, termination.exit_code());
        let bytes = serde_json::to_vec(&result).unwrap_or_default();
        if status == AttemptStatus::Success {
            message.output_json = bytes;
            task.outcome_of_exit(termination.exit_code())
                .unwrap_or(schedr_model::DEFAULT_OUTCOME)
                .clone_into(&mut message.outcome);
        } else {
            message.error_summary_json = bytes;
        }
        return message;
    }
    match (&ran.runner_result, status) {
        (Some(RunnerMessage::TaskSucceeded { output, outcome }), AttemptStatus::Success) => {
            message.output_json = serde_json::to_vec(output).unwrap_or_default();
            message.outcome.clone_from(outcome);
        }
        (Some(RunnerMessage::TaskFailed { error, kind }), _) => {
            message.error_summary_json = serde_json::to_vec(error).unwrap_or_default();
            message.error_kind = kind.clone().unwrap_or_default();
            message.status = attempt_status_to_proto(status_or_failed(status)).into();
        }
        (_, AttemptStatus::Success) => {
            // Exited 0 without a result: the runner didn't do its job.
            message.status = attempt_status_to_proto(AttemptStatus::Failed).into();
            RUNNER_FAILED.clone_into(&mut message.error_kind);
            message.error_summary_json =
                summary("RunnerFailed", "the runner exited without a result");
        }
        (None, _) if termination == crate::status::Termination::Unknown => {
            PROCESS_STATUS_UNKNOWN.clone_into(&mut message.error_kind);
            message.error_summary_json = summary(
                "ProcessStatusUnknown",
                "the task's process ended, but how isn't known: the fork server's shim that \
                 reports it ended first (what was left of its process group was killed)",
            );
        }
        _ => {}
    }
    message
}

/// A `CommandResource` setup's result: on success its output is the value it reported, not
/// its `ProcessResult`; with nothing reported (and a value declared) it fails.
pub fn with_resource_value(message: &mut pb::Finished, outcome: Result<serde_json::Value, ()>) {
    if message.status != i32::from(pb::AttemptStatus::Success) {
        return;
    }
    let Ok(value) = outcome else {
        message.status = attempt_status_to_proto(AttemptStatus::Failed).into();
        message.output_json = Vec::new();
        message.outcome = String::new();
        RESOURCE_NOT_REPORTED.clone_into(&mut message.error_kind);
        message.error_summary_json = summary(
            "ResourceNotReported",
            "resource value not reported: the setup exited 0 without `schedr task-ctx resource \
             set`",
        );
        return;
    };
    message.output_json = serde_json::to_vec(&value).unwrap_or_default();
}

/// The `Finished` message for an attempt that failed before starting.
#[must_use]
pub fn failed_before_start(attempt_id: &str, kind: &str, message: &str) -> pb::Finished {
    pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: pb::AttemptStatus::Failed.into(),
        error_kind: kind.to_owned(),
        error_summary_json: summary("TaskNotStarted", message),
        ..pb::Finished::default()
    }
}

/// A failure stays a failure; anything the agent stopped keeps its own status.
const fn status_or_failed(status: AttemptStatus) -> AttemptStatus {
    match status {
        AttemptStatus::Success => AttemptStatus::Failed,
        other => other,
    }
}

fn summary(exception: &str, message: &str) -> Vec<u8> {
    ErrorSummary::json(exception, message).into_bytes()
}

/// A command's connections' variables added to its `env=`; one in both is refused.
fn add_connection_env(
    env: &mut BTreeMap<String, String>,
    connection_env: &BTreeMap<String, String>,
) -> Result<(), NotRun> {
    for (name, value) in connection_env {
        if env.contains_key(name) {
            return Err(failed(
                RENDER_FAILED,
                &format!(
                    "`{name}` is in the task's `env=` and is a variable of a connection it is \
                     given: rename one"
                ),
            ));
        }
        env.insert(name.clone(), value.clone());
    }
    Ok(())
}

fn failed(kind: &str, error: &dyn std::fmt::Display) -> NotRun {
    NotRun::Failed {
        kind: kind.to_owned(),
        message: error.to_string(),
    }
}

fn run_vars(spec: &AttemptSpec) -> RunVars {
    RunVars {
        dag_id: spec.run.dag_id.clone(),
        run_id: spec.run.run_id.clone(),
        run_kind: spec.run.kind.to_string(),
        task_id: spec.task.id.to_string(),
        map_index: spec.run.map_index,
        try_number: spec.run.try_number,
        planned_for: spec.run.planned_for.to_string(),
        since: spec.run.since.to_string(),
    }
}

fn time(t: Option<&prost_types::Timestamp>) -> Option<Timestamp> {
    let t = t?;
    Timestamp::new(t.seconds, t.nanos).ok()
}
