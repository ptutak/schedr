//! One attempt on the worker, from `AssignTask` to `Finished`: what to run and how it ended.
//!
//! Pure: [`AttemptSpec::from_assign`] reads the assignment, [`plan`] decides how to run it (a
//! Python task through the runner, a command rendered here) or why not (a rejection, or a
//! failure such as a reference that can't be rendered), and [`finished`] turns how the process
//! ended into the `Finished` message (see PROJECT.md, Task assignment). Starting processes is
//! [`crate::launch`]'s job.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use jiff::Timestamp;
use schedr_model::runner::{DagFile, ErrorSummary, RunInfo, RunnerMessage, TaskRequest};
use schedr_model::{AttemptStatus, Task, TaskKind};
use schedr_proto::convert::{attempt_status_to_proto, run_kind_from_proto};
use schedr_proto::pb;
use serde_json::Value;

use crate::env::{RunVars, task_environment};
use crate::output::{TailBuffer, process_result};
use crate::process::ProcessSpec;
use crate::render::{RenderContext, command_line, render};
use crate::status::attempt_status;
use crate::supervise::Supervised;

#[cfg(test)]
mod tests;

/// The error kind of a command whose templates can't be rendered (a missing or truncated
/// input, a value with no text form).
pub const RENDER_FAILED: &str = "render_failed";

/// The error kind of a Python task whose runner ended without reporting a result.
pub const RUNNER_FAILED: &str = "runner_failed";

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
    /// The time between SIGTERM and SIGKILL when it is stopped.
    pub cancel_grace: Duration,
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
            },
            inputs,
            cancel_grace: Duration::from_millis(assign.cancel_grace_ms),
        })
    }

    /// The task's timeout, if any.
    #[must_use]
    pub fn timeout(&self) -> Option<Duration> {
        self.task.settings.timeout_ms.map(Duration::from_millis)
    }
}

/// What the worker knows to run tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSetup {
    /// The DAG roots, by name.
    pub roots: BTreeMap<String, PathBuf>,
    /// The interpreter for Python tasks (`[dags] interpreter`).
    pub python: PathBuf,
    /// The agent's own environment (the allow-listed variables pass to tasks).
    pub agent_env: BTreeMap<String, String>,
    /// Agent variables passed to tasks on top of the allow-list (`[worker] pass_env`).
    pub pass_env: Vec<String>,
    /// Where task log files go.
    pub log_dir: PathBuf,
    /// How the memory watchdog watches tasks and parse runners.
    pub memory: crate::memory::MemoryWatch,
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
        TaskKind::Python { .. } => {
            let env = task_environment(&setup.agent_env, &setup.pass_env, &BTreeMap::new(), &vars)
                .map_err(|e| failed(RENDER_FAILED, &e))?;
            Ok(Plan::Python {
                process: ProcessSpec {
                    program: setup.python.clone(),
                    args: vec![
                        "-m".to_owned(),
                        "schedr.runner".to_owned(),
                        "task".to_owned(),
                    ],
                    env,
                    working_dir: root_dir.clone(),
                    log_file,
                    max_output_size,
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
                    run: spec.run.clone(),
                    sys_path: setup.sys_path(),
                }),
            })
        }
        TaskKind::Command { command, env } => {
            let mut ctx = RenderContext::new(
                &spec.run.dag_id,
                &spec.run.run_id,
                &spec.run.kind.to_string(),
                &spec.run.planned_for.to_string(),
                &spec.run.since.to_string(),
            );
            ctx.inputs.clone_from(&spec.inputs);
            let (program, args) =
                command_line(command, &ctx).map_err(|e| failed(RENDER_FAILED, &e))?;
            let task_env = env
                .iter()
                .map(|(name, template)| Ok((name.clone(), render(template, &ctx, false)?)))
                .collect::<Result<BTreeMap<_, _>, crate::render::RenderError>>()
                .map_err(|e| failed(RENDER_FAILED, &e))?;
            let env = task_environment(&setup.agent_env, &setup.pass_env, &task_env, &vars)
                .map_err(|e| failed(RENDER_FAILED, &e))?;
            Ok(Plan::Command(ProcessSpec {
                program: PathBuf::from(program),
                args,
                env,
                working_dir: file
                    .parent()
                    .map_or_else(|| root_dir.clone(), Path::to_path_buf),
                log_file,
                max_output_size,
            }))
        }
        TaskKind::Empty => Err(NotRun::Rejected {
            reason: pb::RejectReason::InvalidSpecification,
            detail: "empty tasks are completed by the master".to_owned(),
        }),
    }
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

/// The `Finished` message for an attempt that ran.
#[must_use]
pub fn finished(attempt_id: &str, ran: &Ran) -> pb::Finished {
    let termination = ran.supervised.termination;
    let status = attempt_status(termination, ran.supervised.stopped, &[0]);
    let mut message = pb::Finished {
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
            "SUCCESS".clone_into(&mut message.outcome);
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
