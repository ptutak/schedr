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

/// What the runner was started to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMode {
    /// Import DAG files and report the DAGs they define.
    Parse,
    /// Run one task attempt.
    Task,
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
    },
    /// Every requested file was handled.
    ParseFinished,
    /// The task's function returned; its output is encoded and within the size limit.
    TaskSucceeded {
        /// The output as stored JSON (tagged extra types).
        output: serde_json::Value,
        /// The outcome's name (`SUCCESS` until outcome types arrive, milestone M4).
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
    /// The run information.
    pub run: RunInfo,
    /// Directories put first on `sys.path` (every DAG root).
    pub sys_path: Vec<String>,
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
