//! Messages between the worker agent and the Python runner (`python -m schedr.runner`).
//!
//! They travel over the runner's inherited socket (`SCHEDR_AGENT_FD`), each one as a 4-byte
//! big-endian length followed by that many bytes of UTF-8 JSON (see PROJECT.md, Rust/Python
//! integration: the agent <-> runner channel). Every message is an object tagged by `type`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::dag::Dag;

#[cfg(test)]
mod tests;

/// The largest message either side accepts, in bytes (the cap on inputs per task, 64 MiB).
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// The error kind of a task its DAG file no longer defines (the runner's
/// `task_not_found`).
pub const TASK_NOT_FOUND: &str = "task_not_found";

/// The error kind of an attempt whose DAG file is missing although the worker has the root at
/// the hash the master expects (the newest code): the file is gone, not on its way.
pub const DAG_FILE_MISSING: &str = "dag_file_missing";

/// What the runner was started to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMode {
    /// Import DAG files and report the DAGs they define.
    Parse,
    /// Run one task attempt.
    Task,
    /// Preload libraries, then fork a process per request (Linux only).
    ForkServer,
}

/// A structured error summary: why a DAG file couldn't be imported, or why a task failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ErrorSummary {
    /// The exception's class name, e.g. `ModuleNotFoundError`; `ParseTimeout` or
    /// `RunnerDied` when the agent stopped a file that took too long or lost its runner.
    pub exception: String,
    /// The exception's message.
    pub message: String,
    /// The last lines of the traceback (empty for a timeout).
    pub traceback: String,
}

impl ErrorSummary {
    /// The JSON text of a summary schedr itself wrote (no traceback), e.g. when the agent
    /// lost the runner or the master lost the worker.
    #[must_use]
    pub fn json(exception: &str, message: &str) -> String {
        let summary = Self {
            exception: exception.to_owned(),
            message: message.to_owned(),
            traceback: String::new(),
        };
        serde_json::to_string(&summary).unwrap_or_default()
    }
}

/// One file of a parse job: the DAGs it defines, or why it couldn't be imported. The agent
/// builds it from `file_parsed`, uploads it to the master, which stores it as is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedFile {
    /// The path relative to the root.
    pub path: String,
    /// The DAGs it defines (none for a helper module or a failed import).
    pub dags: Vec<Dag>,
    /// Why importing it failed, if it did.
    pub error: Option<ErrorSummary>,
    /// The Python warnings importing it raised (`Category: message`), for `schedr dags
    /// check`; neither uploaded to the master nor stored.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// A message from the runner to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunnerMessage {
    /// The first message: who is speaking and what for.
    Hello {
        /// The installed `schedr` package version; it must equal the agent's.
        schedr_version: String,
        /// The mode the runner was started in.
        mode: RunnerMode,
    },
    /// The runner starts importing a file (the agent times each file).
    FileStarted {
        /// The file's path relative to the root.
        path: String,
    },
    /// A file was imported, or failed to import.
    FileParsed {
        /// The file's path relative to the root.
        path: String,
        /// The DAGs it defines (none for a helper module).
        dags: Vec<Dag>,
        /// Why importing it failed, if it did; then `dags` is empty.
        error: Option<ErrorSummary>,
        /// The Python warnings importing it raised (`Category: message`), e.g. a
        /// `TypingHint`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
    },
    /// Every requested file was handled.
    ParseFinished,
    /// The task's DAG file is imported and its arguments built; its function is called next
    /// (the agent times the start from it). Not sent when that fails.
    TaskImported,
    /// The task's function returned; its output is encoded and within the size limit.
    TaskSucceeded {
        /// The output as stored JSON (tagged extra types).
        output: serde_json::Value,
        /// The outcome's name: `SUCCESS`, an outcome union member's class name, or an `Outcome` member's name.
        outcome: String,
    },
    /// The task failed: an exception, or an output that can't be stored.
    TaskFailed {
        /// What went wrong.
        error: ErrorSummary,
        /// A failure schedr recognises, e.g. `output_not_serializable`; `None` for an
        /// exception from the task's own code.
        kind: Option<String>,
    },
    /// The fork server preloaded its libraries and is single-threaded: it takes `fork`
    /// requests.
    ForkServerReady,
    /// The fork server can't be used: tasks fall back (to the bare fork server, then to
    /// spawn). It answers every later `fork` with `fork_failed` until the agent closes the
    /// channel (exiting with a request unread would reset the connection and could lose this
    /// message). Also the answer to a `fork` when a thread appeared since the check.
    ForkServerDisabled {
        /// Why, e.g. a failing preload or a second thread.
        reason: String,
        /// The names of the threads besides the main one (`/proc/self/task/*/comm`).
        threads: Vec<String>,
    },
    /// A `fork` request was carried out: the shim runs, and reports on the status socket.
    Forked {
        /// The request's ID.
        id: u64,
    },
    /// A `fork` request failed; the task falls back.
    ForkFailed {
        /// The request's ID.
        id: u64,
        /// Why.
        reason: String,
    },
    /// From the shim, on the status socket: the forked process runs in its own process group
    /// (its ID is the PID), so the agent may signal and watch the group.
    ProcessStarted {
        /// The process's PID, which is also its process group's ID.
        pid: i32,
    },
    /// From the shim, on the status socket: the forked process ended. The shim reaps it when
    /// the agent closes its end, so the PID isn't reused before.
    ProcessExited {
        /// The exit code, if it exited.
        code: Option<i32>,
        /// The signal that ended it, if one did.
        signal: Option<i32>,
        /// Whether it dumped core (ended by a signal).
        core_dumped: bool,
        /// What it used, with the children it reaped; `None` if the shim couldn't read it.
        usage: Option<ProcessUsage>,
    },
}

/// What a process used, with the children it reaped: its parent's view at its exit (`rusage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProcessUsage {
    /// CPU time in user mode, in microseconds.
    pub user_cpu_micros: u64,
    /// CPU time in the kernel, in microseconds.
    pub system_cpu_micros: u64,
    /// The peak resident set size, in bytes. For a forked process it includes the pages it
    /// shares with the fork server (the watchdog's PSS doesn't); `None` for a spawned process
    /// (its memory is gone before the agent reads its zombie).
    pub peak_rss_bytes: Option<u64>,
    /// Page faults that needed I/O.
    pub major_faults: u64,
    /// Times it gave up the CPU (waiting for I/O or a lock), with its threads and reaped
    /// children; `None` for a spawned process (its zombie's `/proc` has the leader thread's
    /// only).
    pub voluntary_context_switches: Option<u64>,
    /// Times it was preempted; `None` as for `voluntary_context_switches`.
    pub involuntary_context_switches: Option<u64>,
}

/// A parse job: the files of one root to import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParseRequest {
    /// The root's name, e.g. `etl-repo`.
    pub root: String,
    /// The root's directory.
    pub root_dir: String,
    /// The files to import, relative to the root, in order.
    pub files: Vec<String>,
    /// Directories put first on `sys.path` (every DAG root).
    pub sys_path: Vec<String>,
}

/// Where a DAG file is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DagFile {
    /// The DAG root's name.
    pub root: String,
    /// The root's directory on this worker.
    pub root_dir: String,
    /// The file's path relative to the root.
    pub path: String,
}

/// The run information a task gets (`dag_ctx` and the `SCHEDR_*` variables).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunInfo {
    /// The DAG's ID.
    pub dag_id: String,
    /// The run's ID (a `UUIDv7`).
    pub run_id: String,
    /// The run's kind.
    pub kind: crate::states::RunKind,
    /// The moment the run is for.
    pub planned_for: jiff::Timestamp,
    /// The start of the run's window.
    pub since: jiff::Timestamp,
    /// The attempt's try number, from 1.
    pub try_number: u32,
    /// The instance of a mapped task.
    pub map_index: Option<u32>,
}

/// One task attempt for the runner to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskRequest {
    /// The DAG file to import.
    pub dag_file: DagFile,
    /// The DAG's ID (the file may define several DAGs).
    pub dag_id: String,
    /// The task as stored in DAG JSON (its arguments and settings).
    pub task: crate::task::Task,
    /// The upstream outputs the task uses, by task ID, as stored JSON.
    pub inputs: std::collections::BTreeMap<String, serde_json::Value>,
    /// The upstream tasks' outcomes, by task ID: a name, or `null` for a skipped one (its
    /// references stand for `None`).
    #[serde(default)]
    pub outcomes: std::collections::BTreeMap<String, serde_json::Value>,
    /// The run information.
    pub run: RunInfo,
    /// Directories put first on `sys.path` (every DAG root).
    pub sys_path: Vec<String>,
}

/// What the fork server preloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkServerRequest {
    /// The modules to import, in order (none for the bare fork server).
    pub preload: Vec<String>,
}

/// A request to the fork server: start a runner process.
///
/// Four descriptors come with it (`SCM_RIGHTS`, on the message's first bytes), in order: the
/// process's end of its channel, its stdout, its stderr, and the shim's end of the status
/// socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkRequest {
    /// The request's ID, repeated in the answer.
    pub id: u64,
    /// What the process runs (`parse` or `task`); it shakes hands on its channel as a spawned
    /// runner does.
    pub mode: RunnerMode,
    /// The process's whole environment.
    pub env: std::collections::BTreeMap<String, String>,
    /// The process's working directory.
    pub cwd: String,
}

/// A message from the agent to the runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentMessage {
    /// The handshake succeeded.
    Welcome,
    /// The handshake failed; the runner exits.
    Refused {
        /// Why, e.g. a version mismatch.
        reason: String,
    },
    /// A parse job.
    Parse(ParseRequest),
    /// A task attempt.
    Task(Box<TaskRequest>),
    /// The fork server's preload list (its mode's request).
    ForkServer(ForkServerRequest),
    /// Start a runner process (to the fork server).
    Fork(ForkRequest),
}

/// Whether the agent accepts a runner: the versions must be equal (the binary and the
/// Python package are released together, see PROJECT.md, Packaging and releases).
///
/// # Errors
///
/// Returns the reason to send in [`AgentMessage::Refused`].
pub fn check_runner_version(agent_version: &str, runner_version: &str) -> Result<(), String> {
    if agent_version == runner_version {
        Ok(())
    } else {
        Err(format!(
            "the runner's schedr package is {runner_version}, the agent is {agent_version}: \
             install schedr=={agent_version} in the interpreter's environment"
        ))
    }
}
