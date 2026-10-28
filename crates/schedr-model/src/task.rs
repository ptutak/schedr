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
    /// When it runs, from its upstream edges (its trigger rule).
    #[serde(default, skip_serializing_if = "RunIf::is_default")]
    pub run_if: RunIf,
}

/// A task's trigger rule: what its upstream edges must have done for it to run (see
/// PROJECT.md, Outcomes, failures and joins). An edge not taken counts as skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunIf {
    /// Every edge that wasn't skipped succeeded, and at least one did: skipped when all were
    /// skipped, `upstream_failed` when one failed.
    #[default]
    NoneFailed,
    /// Every edge succeeded: `upstream_failed` when one failed, otherwise skipped when one
    /// was skipped.
    AllSucceeded,
}

impl RunIf {
    /// Whether it is the default, [`RunIf::NoneFailed`].
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::NoneFailed
    }
}

/// One of a task's outcomes: its name, and for a `command` task the exit code meaning it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutcomeDecl {
    /// The name (an `Outcome` enum member's, a class's, or `None`).
    pub name: String,
    /// A `command` task's exit code for it; none for other tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// The outcome of a task without outcomes of its own (`DefaultOutcome.SUCCESS`).
pub const DEFAULT_OUTCOME: &str = "SUCCESS";

/// A task's outcomes when it declares none: `SUCCESS`, exit code 0 for a command.
#[must_use]
pub fn default_outcomes() -> Vec<OutcomeDecl> {
    vec![OutcomeDecl {
        name: DEFAULT_OUTCOME.to_owned(),
        exit_code: Some(0),
    }]
}

fn is_default_outcomes(outcomes: &Vec<OutcomeDecl>) -> bool {
    *outcomes == default_outcomes()
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
            run_if: RunIf::NoneFailed,
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
        /// A `@branch` task: its output is the list of the downstream task IDs it chose, and
        /// its outgoing edges are selection edges.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        branch: bool,
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
    /// The outcomes it may end with (see PROJECT.md, Task results and outcomes).
    #[serde(
        default = "default_outcomes",
        skip_serializing_if = "is_default_outcomes"
    )]
    pub outcomes: Vec<OutcomeDecl>,
}

impl Task {
    /// The outcome a `command` task's exit code means, if it declared it.
    #[must_use]
    pub fn outcome_of_exit(&self, code: i32) -> Option<&str> {
        self.outcomes
            .iter()
            .find(|o| o.exit_code == Some(code))
            .map(|o| o.name.as_str())
    }
}

impl Task {
    /// Every reference the task uses (arguments, command line, environment), in order.
    #[must_use]
    pub fn references(&self) -> Vec<&Reference> {
        match &self.kind {
            TaskKind::Python { arguments, .. } => {
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
