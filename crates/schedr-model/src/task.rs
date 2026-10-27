//! Tasks in DAG JSON: their kind, what they run, and their resolved settings.
//!
//! Settings are stored **resolved** (the Python API applies `TaskDefaults` and group
//! defaults), so the master never needs to know where a value came from.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::TaskId;
use crate::types::TypeDesc;
use crate::value::{Reference, Template, Value};

#[cfg(test)]
mod tests;

/// The queue a task goes to when it names none.
pub const DEFAULT_QUEUE: &str = "default";

/// The default output size limit in bytes (`[outputs] default_max_size`, 1 MiB).
pub const DEFAULT_MAX_OUTPUT_SIZE: u64 = 1024 * 1024;

/// The default delay between attempts in milliseconds (5 minutes).
pub const DEFAULT_RETRY_DELAY_MS: u64 = 5 * 60 * 1000;

/// A task's settings, resolved from its own arguments and the defaults that apply to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskSettings {
    /// How many times a failed attempt is retried.
    pub retries: u32,
    /// The delay before each retry, in milliseconds.
    pub retry_delay_ms: u64,
    /// The attempt's timeout in milliseconds; `None` for no timeout.
    pub timeout_ms: Option<u64>,
    /// The queue the task goes to.
    pub queue: String,
    /// The output size limit in bytes.
    pub max_output_size: u64,
    /// A Python task: fork it from the preloaded fork server (`false`: only from the bare
    /// one, for code that misbehaves next to the preloaded libraries). Not for other tasks.
    #[serde(default = "yes")]
    pub preload: bool,
    /// A Python task: always a new process, never forked (for code that misbehaves after a
    /// fork). Not for other tasks.
    #[serde(default)]
    pub fresh_process: bool,
}

const fn yes() -> bool {
    true
}

impl Default for TaskSettings {
    /// The settings of a task that sets nothing and has no defaults: no retries, a 5 minute
    /// retry delay, no timeout, the default queue, a 1 MiB output limit, forked from the
    /// preloaded fork server where there is one.
    fn default() -> Self {
        Self {
            retries: 0,
            retry_delay_ms: DEFAULT_RETRY_DELAY_MS,
            timeout_ms: None,
            queue: DEFAULT_QUEUE.to_owned(),
            max_output_size: DEFAULT_MAX_OUTPUT_SIZE,
            preload: true,
            fresh_process: false,
        }
    }
}

/// What a `command` task runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandLine {
    /// A string run by a shell; interpolated values are shell-quoted unless `raw`.
    Shell(Template),
    /// A program and its arguments, run without a shell; each element is one argument.
    Argv(Vec<Template>),
}

impl CommandLine {
    /// The templates of this command line, in order.
    #[must_use]
    pub fn templates(&self) -> Vec<&Template> {
        match self {
            Self::Shell(t) => vec![t],
            Self::Argv(args) => args.iter().collect(),
        }
    }
}

/// The kind of a task and what it runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskKind {
    /// A `@task` function, found by task ID when the runner imports the DAG file.
    Python {
        /// The arguments by parameter name.
        #[serde(default)]
        arguments: BTreeMap<String, Value>,
    },
    /// An inline command, run by the agent.
    Command {
        /// The command line.
        command: CommandLine,
        /// Extra environment variables for the process.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        env: BTreeMap<String, Template>,
    },
    /// An `EmptyOperator`: completed by the master, no process.
    Empty,
}

/// A task of a DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Task {
    /// The task's ID, unique within its DAG.
    pub id: TaskId,
    /// Its kind and what it runs.
    #[serde(flatten)]
    pub kind: TaskKind,
    /// Its resolved settings.
    pub settings: TaskSettings,
    /// The type of its output (`process_result` for commands, `none` for empty tasks).
    pub output_type: TypeDesc,
}

impl Task {
    /// Every reference the task uses (arguments, command line, environment), in order.
    #[must_use]
    pub fn references(&self) -> Vec<&Reference> {
        match &self.kind {
            TaskKind::Python { arguments } => {
                arguments.values().flat_map(Value::references).collect()
            }
            TaskKind::Command { command, env } => command
                .templates()
                .into_iter()
                .chain(env.values())
                .flat_map(Template::references)
                .collect(),
            TaskKind::Empty => Vec::new(),
        }
    }
}
