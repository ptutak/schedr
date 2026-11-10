//! Tasks in DAG JSON: their kind, what they run, and their resolved settings.
//!
//! Settings are stored **resolved** (the Python API applies `StepDefaults` and group
//! defaults), so the master never needs to know where a value came from.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::TaskId;
use crate::secret::{SecretDecl, SecretName};
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

/// How many times a failed cleanup is retried by default (decision 217).
pub const DEFAULT_CLEANUP_RETRIES: u32 = 3;

/// The default delay between a cleanup's tries in milliseconds (decision 217).
pub const DEFAULT_CLEANUP_RETRY_DELAY_MS: u64 = 30 * 1000;

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
    /// At most this many of its instances running at once, across all runs (decided from
    /// the DAG's current version, decision 87A); `None` for no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_active: Option<u32>,
    /// At most this many of its instances running at once within one run (from the run's own
    /// version); `None` for no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_active_per_run: Option<u32>,
    /// Its own memory budget in bytes (`memory=`, decision 436), instead of the worker's share:
    /// the watchdog kills it above; taken only by a worker whose pod limit, less its reserve,
    /// holds it (decision 437). `None`: the worker's budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 16_777_216))]
    pub memory_bytes: Option<u64>,
    /// Also capped hard at `memory_bytes` (`hard_memory_limit=True`, decision 438): the
    /// process's `RLIMIT_DATA`, so an allocation past it fails inside the task.
    #[serde(default, skip_serializing_if = "is_false")]
    pub hard_memory_limit: bool,
}

/// A task's trigger rule: what its upstream edges must have done for it to run (see
/// archive/PROJECT.md, Outcomes, failures and joins). An edge not taken counts as skipped.
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

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's skip_serializing_if takes &T"
)]
const fn is_false(value: &bool) -> bool {
    !*value
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
            max_active: None,
            max_active_per_run: None,
            memory_bytes: None,
            hard_memory_limit: false,
        }
    }
}

/// What a `command` task runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandLine {
    /// A string run by a shell; it takes no references (decision 99): values go in `env`.
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

/// How a wait step waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SensorMode {
    /// `@sensor`: its function does the waiting.
    Loop,
    /// `@sensor.poke`: its function is called every `every_ms`.
    Poke,
}

/// The longest `lag` of a [`RunOf`], either way: 10 years (3650 days), so a planned moment plus
/// or minus it stays a timestamp.
pub const MAX_LAG_MS: i64 = 3_650 * 24 * 3_600_000;

/// Which run of another DAG a step waits for, and of it which task (decision 353).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunOf {
    /// The DAG.
    pub dag: crate::DagId,
    /// Its task; `None`: the whole run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
    /// With it, the run whose `planned_for` is the waiting run's minus this many milliseconds;
    /// without it, the next run to end after the step starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_ms: Option<i64>,
}

/// An event posted through the API (decision 354), by name and, for a step waiting in a run,
/// key: `EventNamed("payment-completed", key=dag_ctx.params.order_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventNamed {
    /// Its name.
    pub name: crate::EventName,
    /// Its key, a literal or resolved for the run (its params, its own event's key or ID, an
    /// upstream output): only an event with this key matches. `None`: any of the name; a
    /// trigger takes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<crate::value::Value>,
}

/// What an `@sensor(wait_for=...)` / `@trigger(wait_for=...)` waits for, and the parameter its
/// function is given it in (its first): a run of another DAG (`RunOf`, a `RunResult`) or an
/// event posted (`EventNamed`, an `Event`, decision 354); exactly one of the two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SensorWaitFor {
    /// The run, and of it which task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_of: Option<RunOf>,
    /// The event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<EventNamed>,
    /// The function's parameter given the run.
    pub param: String,
    /// `@sensor(wait_for=..., timeout=...)`: how long one call may take, in milliseconds; one
    /// over it fails the attempt (its retries get the same run). `None`: what is left of
    /// `max_wait`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// What a [`SensorWaitFor`] waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitSource<'a> {
    /// A run of another DAG.
    RunOf(&'a RunOf),
    /// An event posted.
    Event(&'a EventNamed),
}

impl SensorWaitFor {
    /// What it waits for; `None` for neither or both (refused when the DAG is validated).
    #[must_use]
    pub const fn source(&self) -> Option<WaitSource<'_>> {
        match (&self.run_of, &self.event) {
            (Some(run_of), None) => Some(WaitSource::RunOf(run_of)),
            (None, Some(event)) => Some(WaitSource::Event(event)),
            _ => None,
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
    /// A wait step (`@sensor`, `@sensor.poke`): found by task ID, as a `Python` task, and ending
    /// with its value or with the `TimedOut` outcome.
    Sensor {
        /// The arguments by parameter name.
        #[serde(default)]
        arguments: BTreeMap<String, Value>,
        /// How it waits.
        mode: SensorMode,
        /// How often to call it (`Poke`), in milliseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        every_ms: Option<u64>,
        /// How long it waits, in milliseconds; `None`: the worker's `[sensors] default_max_wait`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_wait_ms: Option<u64>,
        /// `@sensor(wait_for=RunOf(...))` (decision 353): called once a matching run of another
        /// DAG has ended, with it as `param`, rather than at once.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wait_for: Option<SensorWaitFor>,
        /// `@trigger.poke(every=..., batch=...)` (decision 352): what its pokes return is
        /// gathered for this many milliseconds from the first item, then sent as one event.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch_ms: Option<u64>,
    },
    /// `first_of` / `at_least` (decision 347): ended by the master, no process and no slot, as
    /// soon as enough of its upstream edges are taken. Its output is `{"$value": ..., "winner":
    /// ...}`: the winner's output and ID, or for `at_least` lists of them in the order they ended.
    FirstOf {
        /// `at_least`'s count; `None`: `first_of`, ending with one edge.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at_least: Option<u32>,
    },
    /// `wait_for(RunOf(...))` (decision 353): waits for a run of another DAG, or one of its
    /// tasks, ended by the master with no process and no slot. Its value is that run's ID; it
    /// ends `Failed` when the run (or task) ended otherwise, and `TimedOut` after `max_wait`.
    WaitFor {
        /// Which run.
        run_of: RunOf,
        /// How long it waits, in milliseconds; `None`: the master's `[sensors] default_max_wait`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_wait_ms: Option<u64>,
    },
    /// `wait_for(EventNamed(...))` (decision 354): waits for an event posted through the API,
    /// ended by the master with no process and no slot. Its value is the event (`key`, `data`,
    /// `at`); `TimedOut` after `max_wait`.
    WaitForEvent {
        /// Which event.
        event: EventNamed,
        /// How long it waits, in milliseconds; `None`: the master's `[sensors] default_max_wait`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_wait_ms: Option<u64>,
    },
    /// `TriggerDagOperator` (decisions 357-362, 365, 378-381): starts a `chained` run of another
    /// DAG, carried out by the master with no process and no slot. Its value is the started
    /// run's ID; with `wait` it ends when that run has: `Failed` when the run didn't succeed,
    /// `TimedOut` after `max_wait`.
    TriggerDag {
        /// The DAG to start a run of.
        dag: crate::DagId,
        /// The run's parameters, over the target's defaults: literals or references, resolved by
        /// the master and checked against the target's schema when the task runs.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        params: BTreeMap<String, Value>,
        /// The run's `planned_for`, a datetime or a reference; `None`: the parent run's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        planned_for: Option<Value>,
        /// Whether it waits for the run to end (the default) or ends once the run is created.
        #[serde(default = "default_wait")]
        wait: bool,
        /// How long it waits, in milliseconds; `None`: the master's `[sensors] default_max_wait`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_wait_ms: Option<u64>,
    },
    /// An inline command, run by the agent.
    Command {
        /// The command line.
        command: CommandLine,
        /// Extra environment variables for the process.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        env: BTreeMap<String, Template>,
        /// Connections given as variables and files (decision 184:
        /// `connections={"DB": warehouse}`), by prefix: `DB_URL`, `DB_PGPASSFILE`, ... Each
        /// is also in the task's `connections`.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        connection_env: BTreeMap<String, String>,
        /// Its part in a resource (M4.8d): a `CommandResource`'s setup, or a resource's
        /// `cleanup_command`; `None` for any other command.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resource: Option<ResourceRole>,
    },
    /// A script declared in a `schedr.toml` (M4.9): run by the agent with the declared
    /// interpreter, from its script root at the version the master expects. A standalone job's
    /// task.
    Script {
        /// The declaration: `<root>:<schedr.toml's path in the root>#<name>`.
        job: String,
        /// The script, relative to the script root.
        script: String,
        /// The interpreter: an `[interpreters]` entry of the worker that runs it.
        interpreter: String,
        /// Its arguments, each one argument (never a shell string).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<Template>,
        /// Extra environment variables for the process.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        env: BTreeMap<String, Template>,
        /// Where it runs, relative to the script root (`schedr.toml`'s directory by default).
        working_dir: String,
        /// Its part in a resource (`ScriptResource`, `cleanup_script`), if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resource: Option<ResourceRole>,
    },
    /// An `EmptyOperator`: completed by the master, no process.
    Empty,
}

/// `TriggerDagOperator`'s `wait` when not given: it waits.
const fn default_wait() -> bool {
    true
}

/// A command's or a script's part in a resource, for `schedr task-ctx resource set|get`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceRole {
    /// A `CommandResource`'s or `ScriptResource`'s setup: its output is the value it reports with `resource set`,
    /// of the task's `output_type` (`none`: it reports nothing), not its `ProcessResult`.
    Setup,
    /// A resource's `cleanup_command` or `cleanup_script`: it reads the setup's value with
    /// `resource get`.
    Cleanup {
        /// The resource's setup task.
        setup: TaskId,
    },
}

/// What a task's cleanup runs: of the same kind as the task (see archive/PROJECT.md, Per-task
/// cleanup).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CleanupRun {
    /// The function given to `@task(cleanup=...)`, found with the task's when the runner
    /// imports the DAG file.
    Python,
    /// A command, with the task's environment.
    Command(CommandLine),
    /// A script of the task's script root (`[jobs.<name>.cleanup]`), with the task's
    /// environment and working directory.
    Script {
        /// The script, relative to the script root.
        script: String,
        /// The interpreter: an `[interpreters]` entry of the worker that runs it.
        interpreter: String,
        /// Its arguments, each one argument (never a shell string).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<Template>,
    },
}

/// A task's cleanup: `finally` for each of its attempts (see archive/PROJECT.md, Per-task cleanup).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskCleanup {
    /// What it runs.
    pub run: CleanupRun,
    /// How long one try may run, in milliseconds; `None`: `[worker] cleanup_timeout`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// How many times it is retried after a failed try, on the same worker.
    pub retries: u32,
    /// The delay before each retry, in milliseconds.
    pub retry_delay_ms: u64,
    /// The handle types it takes: `None` any, none listed none (a function without a `handles`
    /// parameter would never see them).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handles: Option<Vec<String>>,
    /// Whether it may run on another worker when its own is lost (`cleanup_anywhere`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub anywhere: bool,
}

/// What a mapped task's instances vary over (decision 69): one instance per element.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Expand {
    /// `.expand(name=values, ...)`: each argument a list (a literal or a reference to a list),
    /// in the order written; the instances are their cartesian product, the first argument
    /// varying slowest (decision 79).
    Product(Vec<Mapped>),
    /// `.expand_kwargs(values)`: a list of dicts, each one instance's arguments.
    Kwargs(Value),
}

/// One mapped argument of `.expand(...)`: its name and the list it maps over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Mapped {
    /// The parameter's name.
    pub name: String,
    /// Its list: a literal, or a reference to a list.
    pub value: Value,
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
    /// The outcomes it may end with (see archive/PROJECT.md, Task results and outcomes).
    #[serde(
        default = "default_outcomes",
        skip_serializing_if = "is_default_outcomes"
    )]
    pub outcomes: Vec<OutcomeDecl>,
    /// The group it belongs to (the innermost), if any (decision 64).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// What its instances vary over, for a mapped task (`.expand(...)`); its `arguments` are
    /// then the fixed ones (`.partial(...)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expand: Option<Expand>,
    /// The secrets it declares (`secrets=`): read on demand, never put in its environment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretDecl>,
    /// The connections it opens (`connections=`, decision 179), by name: only these, opened
    /// before it starts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<String>,
    /// Its cleanup, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<TaskCleanup>,
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
    /// Its cleanup's command line, if it has one.
    #[must_use]
    pub fn cleanup_command(&self) -> Option<&CommandLine> {
        match self.cleanup.as_ref().map(|c| &c.run) {
            Some(CleanupRun::Command(command)) => Some(command),
            Some(CleanupRun::Python | CleanupRun::Script { .. }) | None => None,
        }
    }

    /// Its cleanup script's arguments, if it has one.
    #[must_use]
    pub fn cleanup_script_args(&self) -> &[Template] {
        match self.cleanup.as_ref().map(|c| &c.run) {
            Some(CleanupRun::Script { args, .. }) => args,
            _ => &[],
        }
    }

    /// Every reference the task uses (arguments, command line, environment, what it is
    /// mapped over), in order.
    #[must_use]
    pub fn references(&self) -> Vec<&Reference> {
        let mut found = self.argument_references();
        match &self.expand {
            Some(Expand::Product(over)) => {
                found.extend(over.iter().flat_map(|m| m.value.references()));
            }
            Some(Expand::Kwargs(over)) => found.extend(over.references()),
            None => {}
        }
        found
    }

    /// The secrets its references name, in order (each once).
    #[must_use]
    pub fn secret_references(&self) -> Vec<&SecretName> {
        let mut found: Vec<&SecretName> = Vec::new();
        for reference in self.references() {
            if let Reference::Secret(name) = reference
                && !found.contains(&name)
            {
                found.push(name);
            }
        }
        found
    }

    /// The references of its arguments, command line and environment, in order: all but what
    /// it is mapped over.
    #[must_use]
    pub fn argument_references(&self) -> Vec<&Reference> {
        match &self.kind {
            TaskKind::Python { arguments, .. } => {
                arguments.values().flat_map(Value::references).collect()
            }
            TaskKind::Sensor {
                arguments,
                wait_for,
                ..
            } => arguments
                .values()
                .chain(
                    wait_for
                        .iter()
                        .filter_map(|w| w.event.as_ref())
                        .filter_map(|e| e.key.as_ref()),
                )
                .flat_map(Value::references)
                .collect(),
            TaskKind::Command { command, env, .. } => command
                .templates()
                .into_iter()
                .chain(
                    self.cleanup_command()
                        .into_iter()
                        .flat_map(CommandLine::templates),
                )
                .chain(env.values())
                .flat_map(Template::references)
                .collect(),
            TaskKind::Script { args, env, .. } => args
                .iter()
                .chain(self.cleanup_script_args())
                .chain(env.values())
                .flat_map(Template::references)
                .collect(),
            TaskKind::WaitForEvent { event, .. } => {
                event.key.iter().flat_map(Value::references).collect()
            }
            TaskKind::TriggerDag {
                params,
                planned_for,
                ..
            } => params
                .values()
                .chain(planned_for)
                .flat_map(Value::references)
                .collect(),
            TaskKind::Empty | TaskKind::FirstOf { .. } | TaskKind::WaitFor { .. } => Vec::new(),
        }
    }
}
