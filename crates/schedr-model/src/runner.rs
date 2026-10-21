//! Messages between the worker agent and the Python runner (`python -m schedr.runner`).
//!
//! They travel over the Unix socket named by `SCHEDR_AGENT_SOCKET`, each one as a 4-byte
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
    /// Run one task attempt (milestone 1.7).
    Task,
}

/// Why a DAG file couldn't be imported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ImportError {
    /// The exception's class name, e.g. `ModuleNotFoundError`; `Timeout` when the agent
    /// stopped a file that took too long.
    pub exception: String,
    /// The exception's message.
    pub message: String,
    /// The last lines of the traceback (empty for a timeout).
    pub traceback: String,
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
        error: Option<ImportError>,
    },
    /// Every requested file was handled.
    ParseFinished,
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
