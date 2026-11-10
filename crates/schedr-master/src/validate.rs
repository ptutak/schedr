//! The master's checks of uploaded parse results.
//!
//! The Python API already refuses wrong DAGs at parse time; the master checks again what it
//! relies on to schedule (it never runs user code, so DAG JSON is its only view of a DAG): the
//! version that produced it, where it is, its trigger, task IDs, edges, cycles and the
//! references between tasks. A file with an invalid DAG is treated as a file that failed to
//! import: its DAGs keep their last good version (see archive/PROJECT.md, DAG code delivery).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::task::{CleanupRun, Task, WaitSource};
use schedr_model::value::DagCtxField;
use schedr_model::{
    CommandLine, Dag, DagId, EdgeKind, Expand, Reference, ResourceRole, SchedrVersion, SecretDecl,
    SensorMode, TaskId, TaskKind, Template, TemplatePart, TypeDesc, Value,
};

use crate::fire_times::FireTimes;
use crate::mapping;

#[cfg(test)]
mod tests;

/// The exception name of the import error recorded for a file with an invalid DAG.
pub const INVALID_DAG_EXCEPTION: &str = schedr_model::runner::INVALID_DAG_EXCEPTION;

/// The exception name of the import error recorded for a path outside its root.
pub const INVALID_PATH_EXCEPTION: &str = schedr_model::runner::INVALID_PATH_EXCEPTION;

/// What is wrong with a DAG parsed from `root`:`path`; empty when it is valid.
#[must_use]
pub fn dag_problems(dag: &Dag, root: &str, path: &str, master: SchedrVersion) -> Vec<String> {
    let mut problems = Vec::new();
    if !master.can_read(dag.produced_by) {
        problems.push(format!(
            "produced by schedr {}, which this master (schedr {master}) can't read",
            dag.produced_by
        ));
    }
    if dag.location.root != root || dag.location.path != path {
        problems.push(format!(
            "located at {}:{} in its DAG JSON, but parsed from {root}:{path}",
            dag.location.root, dag.location.path
        ));
    }
    if let Err(error) = FireTimes::new(&dag.trigger) {
        problems.push(format!("trigger: {error}"));
    }
    if dag.max_active_runs == Some(0) {
        problems.push("max_active_runs must be at least 1".to_owned());
    }
    if dag.max_active_tasks == Some(0) {
        problems.push("max_active_tasks must be at least 1".to_owned());
    }
    for task in &dag.tasks {
        for (name, limit) in [
            ("max_active", task.settings.max_active),
            ("max_active_per_run", task.settings.max_active_per_run),
        ] {
            if limit == Some(0) {
                problems.push(format!("task `{}`: {name} must be at least 1", task.id));
            }
        }
    }
    if let (Some(start), Some(end)) = (dag.trigger.start(), dag.trigger.end())
        && start > end
    {
        problems.push(format!("start ({start}) is after end ({end})"));
    }
    let mut seen = BTreeSet::new();
    for task in &dag.tasks {
        if !seen.insert(&task.id) {
            problems.push(format!("task `{}` is defined more than once", task.id));
        }
    }
    if let Some(params) = &dag.params {
        // A chained run takes its operator's params: no field needs a default.
        let scheduled = !matches!(
            dag.trigger,
            schedr_model::Trigger::None | schedr_model::Trigger::Chained { .. }
        );
        problems.extend(crate::params::problems(params, scheduled));
    }
    group_problems(dag, &mut problems);
    expand_problems(dag, &mut problems);
    param_problems(dag, &mut problems);
    shell_problems(dag, &mut problems);
    secret_problems(dag, &mut problems);
    event_problems(dag, &mut problems);
    edge_problems(dag, &seen, &mut problems);
    reference_problems(dag, &seen, &mut problems);
    first_of_problems(dag, &mut problems);
    resource_problems(dag, &seen, &mut problems);
    role_problems(dag, &mut problems);
    if let Some(cycle) = cycle(dag, &seen) {
        let names: Vec<String> = cycle.iter().map(|id| format!("`{id}`")).collect();
        problems.push(format!(
            "the tasks {} depend on each other in a cycle",
            names.join(", ")
        ));
    }
    problems
}

/// A `trigger=RunOf(...)` (decision 353) starts runs after another DAG's, never before them and
/// never after its own (a DAG starting itself would run for ever).
fn run_of_trigger_problems(
    dag: &Dag,
    run_of: &schedr_model::task::RunOf,
    problems: &mut Vec<String>,
) {
    if run_of.dag == dag.id {
        problems.push(format!(
            "DAG `{}`: its trigger is a run of itself, which would start it for ever",
            dag.id
        ));
    }
    if let Some(why) = lag_problem(run_of) {
        problems.push(format!("DAG `{}`: {why}", dag.id));
    }
    if run_of.lag_ms.is_some_and(|lag| lag < 0) {
        problems.push(format!(
            "DAG `{}`: a RunOf trigger's `lag` can't be negative: a run can't be planned \
             before the one starting it",
            dag.id
        ));
    }
}

/// A `trigger=Chained(by=[...])` (decision 398) names at least one DAG, and not itself (a
/// DAG can't start a run of its own).
fn chained_trigger_problems(dag: &Dag, by: &[DagId], problems: &mut Vec<String>) {
    if by.is_empty() {
        problems.push(format!(
            "DAG `{}`: `Chained(by=[])` names no DAG to start it; `Chained()` is any DAG",
            dag.id
        ));
    }
    if by.contains(&dag.id) {
        problems.push(format!(
            "DAG `{}`: `Chained(by=...)` names the DAG itself, which can't start its own runs",
            dag.id
        ));
    }
}

/// A `RunOf`'s lag over [`schedr_model::task::MAX_LAG_MS`] either way.
fn lag_problem(run_of: &schedr_model::task::RunOf) -> Option<String> {
    run_of
        .lag_ms
        .filter(|lag| lag.unsigned_abs() > schedr_model::task::MAX_LAG_MS.unsigned_abs())
        .map(|lag| format!("RunOf's `lag` is at most 10 years either way, got {lag} ms"))
}

/// A trigger is a sensor with literal arguments, named unlike the DAG's tasks; only where one
/// starts the runs is there an event to read.
fn event_problems(dag: &Dag, problems: &mut Vec<String>) {
    let schedr_model::Trigger::Event {
        task,
        restart_if_idle_ms,
        run_retries,
        ..
    } = &dag.trigger
    else {
        if let schedr_model::Trigger::RunOf { run_of } = &dag.trigger {
            run_of_trigger_problems(dag, run_of, problems);
            return;
        }
        if let schedr_model::Trigger::Chained { by: Some(by) } = &dag.trigger {
            chained_trigger_problems(dag, by, problems);
        }
        if let schedr_model::Trigger::EventNamed { event } = &dag.trigger {
            if event.key.is_some() {
                problems.push(format!(
                    "DAG `{}`: an EventNamed trigger takes no `key`: it starts a run per event \
                     of its name, each keyed by its own",
                    dag.id
                ));
            }
            return;
        }
        for task in &dag.tasks {
            let reads = task.references().into_iter().any(|reference| {
                matches!(
                    reference,
                    Reference::DagCtx {
                        field: DagCtxField::EventKey
                            | DagCtxField::EventData
                            | DagCtxField::EventAt
                    }
                )
            });
            if reads {
                problems.push(format!(
                    "task `{}` uses dag_ctx.event, but no event trigger starts the DAG's runs",
                    task.id
                ));
            }
        }
        return;
    };
    let id = &task.id;
    if !matches!(task.kind, TaskKind::Sensor { .. }) {
        problems.push(format!("trigger `{id}`: its function must be a sensor's"));
    }
    if !task.references().is_empty() {
        problems.push(format!(
            "trigger `{id}`: its arguments must be literals, as no run exists yet"
        ));
    }
    if dag.tasks.iter().any(|other| &other.id == id) {
        problems.push(format!("trigger `{id}` has the name of a task of the DAG"));
    }
    if *restart_if_idle_ms == Some(0) {
        problems.push(format!(
            "trigger `{id}`: restart_if_idle must be more than zero"
        ));
    }
    if let TaskKind::Sensor {
        mode,
        batch_ms: Some(batch),
        ..
    } = &task.kind
    {
        batch_problems(id, *mode, *batch, problems);
    }
    trigger_wait_for_problems(
        dag,
        task,
        restart_if_idle_ms.is_some(),
        *run_retries,
        problems,
    );
    for declared in &task.secrets {
        if let Err(error) = declared.check() {
            problems.push(format!("trigger `{id}`: secrets=: {error}"));
        }
    }
    for name in &task.connections {
        if let Err(error) = TaskId::new(name) {
            problems.push(format!("trigger `{id}`: connections=: `{name}`: {error}"));
        }
    }
}

/// A shell command takes no placeholders (decision 99): a value in shell source can run as
/// code whatever the quoting, so values go in as arguments or environment variables.
fn shell_problems(dag: &Dag, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        if let TaskKind::Command {
            command: CommandLine::Shell(template),
            ..
        } = &task.kind
            && template.0.iter().any(|p| matches!(p, TemplatePart::Ref(_)))
        {
            problems.push(format!(
                "task `{}`: a shell command takes no placeholders (the shell would read a value \
                 as code): pass values as arguments or in `env`",
                task.id
            ));
        }
    }
}

/// A cleanup of another kind than its task: run as it is, it would run the task again.
fn cleanup_problem(task: &Task) -> Option<String> {
    let id = &task.id;
    let run = &task.cleanup.as_ref()?.run;
    match (&task.kind, run) {
        (TaskKind::Sensor { .. }, _) => Some(format!(
            "task `{id}`: a wait step has no cleanup; use `finally` in its function"
        )),
        (TaskKind::Python { .. }, CleanupRun::Python)
        | (TaskKind::Command { .. }, CleanupRun::Command(_))
        | (TaskKind::Script { .. }, CleanupRun::Script { .. }) => None,
        (TaskKind::Python { .. }, CleanupRun::Command(_) | CleanupRun::Script { .. }) => Some(
            format!("task `{id}`: a Python task's cleanup is a function, not a command"),
        ),
        (TaskKind::Command { .. }, CleanupRun::Python | CleanupRun::Script { .. }) => Some(
            format!("task `{id}`: a command task's cleanup is a command, not `python`"),
        ),
        (TaskKind::Empty, _) => Some(format!(
            "task `{id}`: an empty task has nothing to clean up"
        )),
        (TaskKind::FirstOf { .. }, _) => {
            Some(format!("task `{id}`: first_of has nothing to clean up"))
        }
        (TaskKind::WaitFor { .. } | TaskKind::WaitForEvent { .. }, _) => {
            Some(format!("task `{id}`: wait_for has nothing to clean up"))
        }
        (TaskKind::TriggerDag { .. }, _) => Some(format!(
            "task `{id}`: a TriggerDagOperator has nothing to clean up"
        )),
        (TaskKind::Script { .. }, _) => {
            Some(format!("task `{id}`: a script's cleanup is a script"))
        }
    }
}

/// Secrets (see archive/PROJECT.md, Secret storage): each name well formed, none in a command line
/// (it would show in the process list) nor mapped over, a whole secret only where a value
/// goes (an argument), not in text.
fn secret_problems(dag: &Dag, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        let id = &task.id;
        for name in task.secret_references() {
            if let Err(error) = name.check() {
                problems.push(format!("task `{id}`: {error}"));
            }
        }
        problems.extend(cleanup_problem(task));
        for declared in &task.secrets {
            if let Err(error) = declared.check() {
                problems.push(format!("task `{id}`: secrets=: {error}"));
            }
        }
        for name in &task.connections {
            if let Err(error) = TaskId::new(name) {
                problems.push(format!("task `{id}`: connections=: `{name}`: {error}"));
            }
        }
        problems.extend(batch_in_a_step(task));
        if let TaskKind::TriggerDag { dag: target, .. } = &task.kind
            && target == &dag.id
        {
            problems.push(format!(
                "task `{id}`: a TriggerDagOperator can't start a run of its own DAG `{target}` (a \
                 cycle)"
            ));
        }
        let mut in_text = kind_problems(task, problems);
        let mapped: Vec<&Value> = match &task.expand {
            Some(Expand::Product(over)) => over.iter().map(|m| &m.value).collect(),
            Some(Expand::Kwargs(over)) => vec![over],
            None => Vec::new(),
        };
        // What it maps over is written into its instances' arguments too.
        in_text.extend(mapped.iter().flat_map(|value| value_templates(value)));
        for template in in_text {
            for reference in template.references() {
                if let Reference::Secret(name) = reference
                    && name.key.is_none()
                {
                    problems.push(format!(
                        "task `{id}`: `{name}` is a whole secret (every key), which can't be \
                         written into text: name its key, `path#key`"
                    ));
                }
            }
        }
        if mapped
            .iter()
            .any(|v| matches!(v, Value::Ref(Reference::Secret(_))))
        {
            problems.push(format!(
                "task `{id}` is mapped over a secret: the number of instances can't depend on it"
            ));
        }
    }
}

/// The problems of the task's own kind (its command line, connections, arguments); the
/// templates it writes secrets into as text.
fn kind_problems<'a>(task: &'a Task, problems: &mut Vec<String>) -> Vec<&'a Template> {
    let id = &task.id;
    let mut in_text: Vec<&Template> = Vec::new();
    match &task.kind {
        TaskKind::Command {
            command,
            env,
            connection_env,
            ..
        } => {
            for (prefix, name) in connection_env {
                if let Err(error) = schedr_model::connection::check_prefix(prefix) {
                    problems.push(format!("task `{id}`: connections=: {error}"));
                }
                if !task.connections.contains(name) {
                    problems.push(format!(
                        "task `{id}`: connections=: `{prefix}` names `{name}`, which the \
                         task doesn't declare"
                    ));
                }
            }
            let cleanup = task.cleanup_command().into_iter();
            for template in command
                .templates()
                .into_iter()
                .chain(cleanup.flat_map(CommandLine::templates))
            {
                for reference in template.references() {
                    if let Reference::Secret(name) = reference {
                        problems.push(format!(
                            "task `{id}`: `{name}` in the command line (anyone on the \
                             machine could read it in the process list): pass it in `env`, \
                             or declare it in `secrets=` and read it with `schedr task-ctx \
                             secret get`"
                        ));
                    }
                }
            }
            in_text.extend(env.values());
        }
        TaskKind::Script { args, env, .. } => {
            for reference in args
                .iter()
                .chain(task.cleanup_script_args())
                .flat_map(Template::references)
            {
                if let Reference::Secret(name) = reference {
                    problems.push(format!(
                        "task `{id}`: `{name}` in the arguments (anyone on the machine could \
                         read it in the process list): pass it in `env`, or declare it in \
                         `secrets` and read it with `schedr task-ctx secret get`"
                    ));
                }
            }
            in_text.extend(env.values());
        }
        TaskKind::Python { arguments, .. } => {
            for value in arguments.values() {
                in_text.extend(value_templates(value));
            }
        }
        TaskKind::Sensor { arguments, .. } => {
            for value in arguments.values() {
                in_text.extend(value_templates(value));
            }
            sensor_problems(task, problems);
        }
        TaskKind::WaitFor {
            max_wait_ms,
            run_of,
        } => wait_for_problems(task, run_of, *max_wait_ms, problems),
        TaskKind::WaitForEvent { event, max_wait_ms } => {
            wait_for_event_problems(task, event, *max_wait_ms, problems);
        }
        TaskKind::TriggerDag {
            params,
            max_wait_ms,
            wait,
            planned_for,
            ..
        } => trigger_dag_problems(
            task,
            (*max_wait_ms, *wait),
            params,
            planned_for.as_ref(),
            problems,
        ),
        TaskKind::Empty | TaskKind::FirstOf { .. } => {}
    }
    in_text
}

/// What an `@sensor(wait_for=RunOf(...))` (decision 353) can't be: a poke (it is called per
/// run, never on a timer; the user's, option A), given its run's parameter as an argument too, or
/// mapped.
fn sensor_wait_for_problems(
    task: &Task,
    mode: SensorMode,
    arguments: &BTreeMap<String, Value>,
    wait_for: &schedr_model::task::SensorWaitFor,
    problems: &mut Vec<String>,
) {
    let id = &task.id;
    if mode == SensorMode::Poke {
        problems.push(format!(
            "task `{id}`: a @sensor.poke can't take `wait_for=`: it is called once per matching \
             run, never on a timer (a poke can read the run from the API itself)"
        ));
    }
    if arguments.contains_key(&wait_for.param) {
        problems.push(format!(
            "task `{id}`: `{}` is given the run it waits for, so it takes no argument",
            wait_for.param
        ));
    }
    match wait_for.source() {
        Some(WaitSource::RunOf(run_of)) => {
            if let Some(why) = lag_problem(run_of) {
                problems.push(format!("task `{id}`: {why}"));
            }
        }
        Some(WaitSource::Event(event)) => {
            if let Some(why) = event.key.as_ref().and_then(event_key_problem) {
                problems.push(format!("task `{id}`: its event's key {why}"));
            }
        }
        None => problems.push(format!(
            "task `{id}`: `wait_for` is a RunOf or an EventNamed, one of them"
        )),
    }
    if wait_for.timeout_ms == Some(0) {
        problems.push(format!(
            "task `{id}`: a call's `timeout` must be above zero"
        ));
    }
    if mapping::is_mapped(task) {
        problems.push(format!(
            "task `{id}`: a @sensor with `wait_for=` can't be mapped"
        ));
    }
}

/// What a wait step (`@sensor`, `@sensor.poke`) can't be.
fn sensor_problems(task: &Task, problems: &mut Vec<String>) {
    let TaskKind::Sensor {
        arguments,
        mode,
        every_ms,
        max_wait_ms,
        wait_for,
        ..
    } = &task.kind
    else {
        return;
    };
    let id = &task.id;
    if let Some(wait_for) = wait_for {
        sensor_wait_for_problems(task, *mode, arguments, wait_for, problems);
    }
    match (mode, every_ms) {
        (SensorMode::Poke, None | Some(0)) => {
            problems.push(format!(
                "task `{id}`: a @sensor.poke needs `every` above zero"
            ));
        }
        (SensorMode::Loop, Some(_)) => {
            problems.push(format!("task `{id}`: only a @sensor.poke has `every`"));
        }
        _ => {}
    }
    if *max_wait_ms == Some(0) {
        problems.push(format!(
            "task `{id}`: a wait step's `max_wait` must be above zero"
        ));
    }
}

/// A DAG's task with `batch` (decision 352): it is a trigger's, a wait step ending with one value.
fn batch_in_a_step(task: &Task) -> Option<String> {
    matches!(
        task.kind,
        TaskKind::Sensor {
            batch_ms: Some(_),
            ..
        }
    )
    .then(|| {
        format!(
            "task `{}`: `batch` is for an @trigger.poke, which starts runs: a wait step ends \
             with one value",
            task.id
        )
    })
}

/// What an `@trigger.poke(batch=...)` (decision 352) can't be: anything but a poke, or a window
/// of nothing. (A poke is never restarted as idle, so `restart_if_idle` can't cut a window.)
fn batch_problems(id: &TaskId, mode: SensorMode, batch: u64, problems: &mut Vec<String>) {
    if mode != SensorMode::Poke {
        problems.push(format!(
            "trigger `{id}`: `batch` is for an @trigger.poke: an @trigger batches by yielding \
             one event holding the group"
        ));
    }
    if batch == 0 {
        problems.push(format!("trigger `{id}`: `batch` must be more than zero"));
    }
}

/// What an `@trigger(wait_for=RunOf(...))` (decision 353) can't be: anything but a function
/// called once per run (no timer, no idle restart), given its run as an argument too, or reading
/// its own DAG's runs; `run_retries` is for it alone.
fn trigger_wait_for_problems(
    dag: &Dag,
    task: &Task,
    idle_restart: bool,
    run_retries: Option<u32>,
    problems: &mut Vec<String>,
) {
    let id = &task.id;
    let TaskKind::Sensor {
        mode,
        arguments,
        wait_for: Some(wait_for),
        ..
    } = &task.kind
    else {
        if run_retries.is_some() {
            problems.push(format!(
                "trigger `{id}`: `run_retries` is for a trigger with `wait_for=`"
            ));
        }
        return;
    };
    if *mode != SensorMode::Loop {
        problems.push(format!(
            "trigger `{id}`: with `wait_for=` it is called once per run, never on a timer"
        ));
    }
    if idle_restart {
        problems.push(format!(
            "trigger `{id}`: with `wait_for=` it is called once per run: nothing to restart \
             when idle"
        ));
    }
    if arguments.contains_key(&wait_for.param) {
        problems.push(format!(
            "trigger `{id}`: `{}` is given the run it waits for, so it takes no argument",
            wait_for.param
        ));
    }
    match wait_for.source() {
        Some(WaitSource::RunOf(run_of)) => {
            if run_of.dag == dag.id {
                problems.push(format!(
                    "trigger `{id}`: it waits for runs of its own DAG, which would start it for \
                     ever"
                ));
            }
            if let Some(why) = lag_problem(run_of) {
                problems.push(format!("trigger `{id}`: {why}"));
            }
            if run_of.lag_ms.is_some_and(|lag| lag < 0) {
                problems.push(format!(
                    "trigger `{id}`: its RunOf's `lag` can't be negative: a run can't be planned \
                     before the one starting it"
                ));
            }
        }
        Some(WaitSource::Event(event)) => {
            if event.key.is_some() {
                problems.push(format!(
                    "trigger `{id}`: an EventNamed it waits for takes no `key`: it is called for \
                     every event of its name"
                ));
            }
        }
        None => problems.push(format!(
            "trigger `{id}`: `wait_for` is a RunOf or an EventNamed, one of them"
        )),
    }
    if wait_for.timeout_ms == Some(0) {
        problems.push(format!(
            "trigger `{id}`: a call's `timeout` must be above zero"
        ));
    }
}

/// What a `wait_for` step (decision 353) can't be.
fn wait_for_problems(
    task: &Task,
    run_of: &schedr_model::task::RunOf,
    max_wait_ms: Option<u64>,
    problems: &mut Vec<String>,
) {
    let id = &task.id;
    if let Some(why) = lag_problem(run_of) {
        problems.push(format!("task `{id}`: {why}"));
    }
    if max_wait_ms == Some(0) {
        problems.push(format!(
            "task `{id}`: wait_for's `max_wait` must be above zero"
        ));
    }
    if mapping::is_mapped(task) {
        problems.push(format!("task `{id}`: wait_for can't be mapped"));
    }
}

/// What a `TriggerDagOperator` (decisions 358-362) can't be (mapped, see
/// `trigger_dag_expand_problems`): waiting for no time, given `max_wait` without waiting, planned for a literal that
/// isn't a time, or given what the master can't work out (it starts the run, not a runner): a
/// text template or a secret.
fn trigger_dag_problems(
    task: &Task,
    (max_wait_ms, wait): (Option<u64>, bool),
    params: &BTreeMap<String, Value>,
    planned_for: Option<&Value>,
    problems: &mut Vec<String>,
) {
    let id = &task.id;
    let named = params
        .iter()
        .map(|(key, value)| (format!("`params[{key:?}]`"), value))
        .chain(planned_for.map(|value| ("`planned_for`".to_owned(), value)));
    for (name, value) in named {
        if let Some(what) = not_for_the_master(value) {
            problems.push(format!(
                "task `{id}`: {name} can't be {what}: the master starts the run and reads no \
                 secret and renders no template; pass the value from an upstream task"
            ));
        }
    }
    if max_wait_ms == Some(0) {
        problems.push(format!("task `{id}`: `max_wait` must be above zero"));
    }
    if max_wait_ms.is_some() && !wait {
        problems.push(format!(
            "task `{id}`: `max_wait` is for `wait=True`; with `wait=False` it ends once the run \
             is created"
        ));
    }
    if let Some(Value::Literal(literal)) = planned_for
        && literal
            .get("$datetime")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|text| text.parse::<jiff::Timestamp>().is_err())
    {
        problems.push(format!(
            "task `{id}`: `planned_for` is a datetime with an offset or a reference, not \
             {literal}"
        ));
    }
}

/// What in `value` the master can't work out, if anything: a text template or a secret.
fn not_for_the_master(value: &Value) -> Option<&'static str> {
    match value {
        Value::Template(_) => Some("a text template"),
        Value::Ref(schedr_model::value::Reference::Secret(_)) => Some("a secret"),
        Value::List(items) => items.iter().find_map(not_for_the_master),
        Value::Map(entries) => entries.values().find_map(not_for_the_master),
        Value::Literal(_) | Value::Ref(_) => None,
    }
}

/// What a `wait_for(EventNamed(...))` (decision 354) can't be: mapped, waiting for no time, or
/// keyed by what isn't a string the master can work out for the run (a literal string, the
/// run's params, its ID, its DAG, its own event's key, an upstream output).
fn wait_for_event_problems(
    task: &Task,
    event: &schedr_model::task::EventNamed,
    max_wait_ms: Option<u64>,
    problems: &mut Vec<String>,
) {
    let id = &task.id;
    if max_wait_ms == Some(0) {
        problems.push(format!(
            "task `{id}`: wait_for's `max_wait` must be above zero"
        ));
    }
    if mapping::is_mapped(task) {
        problems.push(format!("task `{id}`: wait_for can't be mapped"));
    }
    if let Some(key) = &event.key
        && let Some(why) = event_key_problem(key)
    {
        problems.push(format!("task `{id}`: EventNamed's `key` {why}"));
    }
}

/// Why `key` can't be an `EventNamed`'s key, if it can't: a literal string, or one reference the
/// master resolves for the run (decision 354).
fn event_key_problem(key: &Value) -> Option<&'static str> {
    use schedr_model::value::{DagCtxField, Reference};
    match key {
        Value::Literal(serde_json::Value::String(text)) if text.is_empty() => {
            Some("can't be empty")
        }
        Value::Literal(serde_json::Value::String(_)) => None,
        Value::Literal(_) => Some("is a string"),
        Value::Ref(reference) => match reference {
            Reference::Param { .. }
            | Reference::Output { .. }
            | Reference::DagCtx {
                field: DagCtxField::RunId | DagCtxField::DagId | DagCtxField::EventKey,
            } => None,
            Reference::Secret(_) => Some("can't be a secret: it is stored and shown"),
            Reference::Outcome { .. } | Reference::DagCtx { .. } => Some(
                "is a literal string, the run's params, `dag_ctx.run.id`, `dag_ctx.dag_id`, \
                 `dag_ctx.event.key` or an upstream output",
            ),
        },
        _ => Some(
            "is a literal string, the run's params, `dag_ctx.run.id`, `dag_ctx.dag_id`, \
             `dag_ctx.event.key` or an upstream output",
        ),
    }
}

/// The templates inside a value, at any depth.
fn value_templates(value: &Value) -> Vec<&Template> {
    match value {
        Value::Template(template) => vec![template],
        Value::List(items) => items.iter().flat_map(value_templates).collect(),
        Value::Map(entries) => entries.values().flat_map(value_templates).collect(),
        Value::Literal(_) | Value::Ref(_) => Vec::new(),
    }
}

/// The secret storages a DAG names that the worker that parsed it hasn't configured (the
/// default one, unnamed, isn't checked).
#[must_use]
pub fn storage_problems(dag: &Dag, storages: &BTreeSet<String>) -> Vec<String> {
    let mut problems = Vec::new();
    for task in &dag.tasks {
        let named = task
            .secret_references()
            .into_iter()
            .filter_map(|name| name.storage.as_deref())
            .chain(task.secrets.iter().filter_map(SecretDecl::storage));
        let mut seen = BTreeSet::new();
        for storage in named {
            if !storages.contains(storage) && seen.insert(storage) {
                let known = if storages.is_empty() {
                    "none".to_owned()
                } else {
                    storages
                        .iter()
                        .map(|s| format!("`{s}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                problems.push(format!(
                    "task `{}`: no secret storage `{storage}` on the worker that parsed it \
                     (configured: {known}; `[secret_storage.{storage}]` in the workers' \
                     configuration)",
                    task.id
                ));
            }
        }
    }
    problems
}

/// Mapped tasks (decision 69): a Python task only, not a `@branch`, with something to vary
/// over, never an argument both fixed and mapped, or mapped twice.
fn expand_problems(dag: &Dag, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        let Some(expand) = &task.expand else {
            continue;
        };
        if let TaskKind::TriggerDag {
            params,
            planned_for,
            ..
        } = &task.kind
        {
            let fixed = (!params.is_empty(), planned_for.is_some());
            trigger_dag_expand_problems(task, expand, fixed, problems);
            continue;
        }
        let TaskKind::Python {
            arguments, branch, ..
        } = &task.kind
        else {
            // Of the operators, only a TriggerDagOperator (decision 406).
            problems.push(format!(
                "task `{}`: only a @task function or a TriggerDagOperator can be mapped \
                 (`.expand()`)",
                task.id
            ));
            continue;
        };
        if *branch {
            problems.push(format!(
                "task `{}`: a @branch task can't be mapped (`.expand()`)",
                task.id
            ));
        }
        if let Expand::Kwargs(over) = expand {
            let fixed: std::collections::BTreeSet<&str> = kwargs_names(over)
                .into_iter()
                .flatten()
                .filter(|name| arguments.contains_key(*name))
                .collect();
            for name in fixed {
                problems.push(format!(
                    "task `{}`: argument `{name}` is both fixed (`.partial()`) and mapped \
                     (`.expand_kwargs()`)",
                    task.id
                ));
            }
        }
        if let Expand::Product(over) = expand {
            if over.is_empty() {
                problems.push(format!("task `{}`: `.expand()` names no argument", task.id));
            }
            let mut seen = std::collections::BTreeSet::new();
            for name in over.iter().map(|m| m.name.as_str()) {
                if arguments.contains_key(name) {
                    problems.push(format!(
                        "task `{}`: argument `{name}` is both fixed (`.partial()`) and mapped \
                         (`.expand()`)",
                        task.id
                    ));
                }
                if !seen.insert(name) {
                    problems.push(format!(
                        "task `{}`: argument `{name}` is mapped twice (`.expand()`)",
                        task.id
                    ));
                }
            }
        }
    }
}

/// The names each dict of an `.expand_kwargs()` list written in the DAG gives (an upstream
/// output's are unknown: none).
fn kwargs_names(over: &Value) -> Vec<Vec<&str>> {
    match over {
        Value::Literal(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(serde_json::Value::as_object)
            .map(|item| item.keys().map(String::as_str).collect())
            .collect(),
        Value::List(items) => items
            .iter()
            .filter_map(|item| match item {
                Value::Map(entries) => Some(entries.keys().map(String::as_str).collect()),
                Value::Literal(serde_json::Value::Object(entries)) => {
                    Some(entries.keys().map(String::as_str).collect())
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// A mapped `TriggerDagOperator` (decision 407): it maps `params` or `planned_for`, each once,
/// neither also fixed (`fixed`: whether `params` and `planned_for` are), over what the master can
/// work out (no template, no secret).
fn trigger_dag_expand_problems(
    task: &Task,
    expand: &Expand,
    (params_fixed, planned_fixed): (bool, bool),
    problems: &mut Vec<String>,
) {
    let id = &task.id;
    let (names, values, how): (Vec<&str>, Vec<&Value>, &str) = match expand {
        Expand::Product(over) => {
            if over.is_empty() {
                problems.push(format!("task `{id}`: `.expand()` names no argument"));
            }
            let mut seen = std::collections::BTreeSet::new();
            for name in over.iter().map(|m| m.name.as_str()) {
                if !seen.insert(name) {
                    problems.push(format!(
                        "task `{id}`: argument `{name}` is mapped twice (`.expand()`)"
                    ));
                }
            }
            (
                over.iter().map(|m| m.name.as_str()).collect(),
                over.iter().map(|m| &m.value).collect(),
                "`.expand()`",
            )
        }
        Expand::Kwargs(over) => {
            let mut names: Vec<&str> = kwargs_names(over).into_iter().flatten().collect();
            names.sort_unstable();
            names.dedup();
            (names, vec![over], "`.expand_kwargs()`")
        }
    };
    for name in names {
        let fixed = match name {
            "params" => params_fixed,
            "planned_for" => planned_fixed,
            _ => {
                problems.push(format!(
                    "task `{id}`: a TriggerDagOperator maps `params` or `planned_for`, not \
                     `{name}` ({how})"
                ));
                continue;
            }
        };
        if fixed {
            problems.push(format!(
                "task `{id}`: argument `{name}` is both fixed (`.partial()`) and mapped ({how})"
            ));
        }
    }
    for value in values {
        if let Some(what) = not_for_the_master(value) {
            problems.push(format!(
                "task `{id}`: what it maps over can't hold {what}: the master starts the runs \
                 and reads no secret and renders no template; pass the values from an upstream \
                 task"
            ));
        }
    }
}

/// `dag_ctx.params` references (decision 138): the DAG declares params, each path exists in
/// their type, and none is mapped over (the number of instances would depend on it).
fn param_problems(dag: &Dag, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        for reference in task.references() {
            let Reference::Param { path } = reference else {
                continue;
            };
            let label = param_label(path);
            let Some(params) = &dag.params else {
                problems.push(format!(
                    "task `{}` uses `{label}`, but the DAG declares no params (`Dag(params=...)`)",
                    task.id
                ));
                continue;
            };
            let mut ty = Some(&params.ty);
            for step in path {
                ty = ty.and_then(|t| match step {
                    schedr_model::PathElem::Field(name) => t.field(name),
                    schedr_model::PathElem::Index(i) => t.element(*i),
                });
            }
            if ty.is_none() {
                problems.push(format!(
                    "task `{}` uses `{label}`, which the DAG's params don't have",
                    task.id
                ));
            }
        }
        let mapped: Vec<&Value> = match &task.expand {
            Some(Expand::Product(over)) => over.iter().map(|m| &m.value).collect(),
            Some(Expand::Kwargs(over)) => vec![over],
            None => Vec::new(),
        };
        if mapped
            .iter()
            .any(|v| matches!(v, Value::Ref(Reference::Param { .. })))
        {
            problems.push(format!(
                "task `{}` is mapped over the run's params: the number of instances can't \
                 depend on them",
                task.id
            ));
        }
    }
}

/// `dag_ctx.params` with a path, as written in Python.
fn param_label(path: &[schedr_model::PathElem]) -> String {
    let mut label = "dag_ctx.params".to_owned();
    for step in path {
        match step {
            schedr_model::PathElem::Field(name) => {
                label.push('.');
                label.push_str(name);
            }
            schedr_model::PathElem::Index(i) => {
                label.push('[');
                label.push_str(&i.to_string());
                label.push(']');
            }
        }
    }
    label
}

/// Groups (decision 64): unique, each parent a group of the DAG, no group inside itself, and
/// each task's group declared.
fn group_problems(dag: &Dag, problems: &mut Vec<String>) {
    let mut parents: BTreeMap<&str, Option<&str>> = BTreeMap::new();
    for group in &dag.groups {
        if parents
            .insert(group.id.as_str(), group.parent.as_deref())
            .is_some()
        {
            problems.push(format!("group `{}` is defined more than once", group.id));
        }
    }
    for group in &dag.groups {
        if let Some(parent) = &group.parent
            && !parents.contains_key(parent.as_str())
        {
            problems.push(format!(
                "group `{}`: its parent `{parent}` isn't a group of the DAG",
                group.id
            ));
        }
        // Up the chain of parents: back to this group is a cycle.
        let mut at = group.parent.as_deref();
        for _ in 0..parents.len() {
            match at {
                Some(id) if id == group.id => {
                    problems.push(format!("group `{}` is nested inside itself", group.id));
                    break;
                }
                Some(id) => at = parents.get(id).copied().flatten(),
                None => break,
            }
        }
    }
    for task in &dag.tasks {
        if let Some(group) = &task.group
            && !parents.contains_key(group.as_str())
        {
            problems.push(format!(
                "task `{}`: its group `{group}` isn't a group of the DAG",
                task.id
            ));
        }
    }
}

fn edge_problems(dag: &Dag, tasks: &BTreeSet<&TaskId>, problems: &mut Vec<String>) {
    let branches: BTreeSet<&TaskId> = dag
        .tasks
        .iter()
        .filter(|task| matches!(task.kind, TaskKind::Python { branch: true, .. }))
        .map(|task| &task.id)
        .collect();
    let group_of = |id: &TaskId| dag.task(id).and_then(|t| t.group.as_deref());
    // An order edge out of the group a branch is in: made by a dependency on a group that
    // ends with the branch, taken once it ran, whatever it chose (decision 94).
    let out_of_its_group = |from: &TaskId, to: &TaskId| {
        group_of(from).is_some_and(|group| {
            group_of(to).is_none_or(|g| g != group && !g.starts_with(&format!("{group}.")))
        })
    };
    for edge in &dag.edges {
        for end in [&edge.from, &edge.to] {
            if !tasks.contains(end) {
                problems.push(format!(
                    "edge `{}` -> `{}`: no task `{end}`",
                    edge.from, edge.to
                ));
            }
        }
        if edge.from == edge.to {
            problems.push(format!("task `{}` depends on itself", edge.from));
        }
        let from_branch = branches.contains(&edge.from);
        if edge.kind == EdgeKind::Selection && !from_branch {
            problems.push(format!(
                "edge `{}` -> `{}`: a selection edge, but `{}` isn't a `@branch` task",
                edge.from, edge.to, edge.from
            ));
        } else if edge.kind != EdgeKind::Selection
            && from_branch
            && !(edge.kind == EdgeKind::Order && out_of_its_group(&edge.from, &edge.to))
        {
            problems.push(format!(
                "edge `{}` -> `{}`: of kind `{}`, but every edge from the `@branch` task `{}` must \
                 be a selection edge (or an order edge out of its group, decision 94)",
                edge.from,
                edge.to,
                edge_kind_name(edge.kind),
                edge.from
            ));
        }
    }
}

/// An edge kind as DAG JSON writes it.
const fn edge_kind_name(kind: EdgeKind) -> &'static str {
    match kind {
        EdgeKind::Data => "data",
        EdgeKind::Order => "order",
        EdgeKind::Selection => "selection",
        EdgeKind::Requirement => "requirement",
    }
}

/// Each resource's parts and users are tasks of the DAG; a task is one resource's part at most.
fn resource_problems(dag: &Dag, tasks: &BTreeSet<&TaskId>, problems: &mut Vec<String>) {
    let mut parts = BTreeSet::new();
    for resource in &dag.resources {
        for part in [&resource.setup, &resource.cleanup] {
            if !parts.insert(part) {
                problems.push(format!("task `{part}` is a part of two resources"));
            }
        }
        for task in [&resource.setup, &resource.cleanup]
            .into_iter()
            .chain(&resource.users)
        {
            if !tasks.contains(task) {
                problems.push(format!(
                    "resource `{}`: `{task}` isn't a task of the DAG",
                    resource.setup
                ));
            }
        }
        if resource.users.contains(&resource.setup) || resource.users.contains(&resource.cleanup) {
            problems.push(format!(
                "resource `{}`: a part of it can't be its user",
                resource.setup
            ));
        }
        if !dag.edges.iter().any(|e| {
            e.from == resource.setup
                && e.to == resource.cleanup
                && matches!(e.kind, EdgeKind::Data | EdgeKind::Requirement)
        }) {
            problems.push(format!(
                "resource `{}`: no data or requirement edge to its cleanup `{}`",
                resource.setup, resource.cleanup
            ));
        }
        // The cleanup waits for `users`: the edges and the list must say the same.
        let joined: BTreeSet<&TaskId> = dag
            .edges
            .iter()
            .filter(|e| {
                e.from == resource.setup
                    && e.to != resource.cleanup
                    && matches!(e.kind, EdgeKind::Data | EdgeKind::Requirement)
            })
            .map(|e| &e.to)
            .collect();
        let listed: BTreeSet<&TaskId> = resource
            .users
            .iter()
            .filter(|user| **user != resource.setup && **user != resource.cleanup)
            .collect();
        for task in joined.symmetric_difference(&listed) {
            problems.push(format!(
                "resource `{}`: its users and the edges from it differ on `{task}`",
                resource.setup
            ));
        }
        if let Some(setup) = dag.task(&resource.setup)
            && let TaskKind::Command { resource: None, .. } = &setup.kind
        {
            problems.push(format!(
                "resource `{}`: a command setup must report its value (a `CommandResource`)",
                resource.setup
            ));
        }
        for part in [&resource.setup, &resource.cleanup] {
            if dag.task(part).is_some_and(|t| t.expand.is_some()) {
                problems.push(format!(
                    "resource `{}`: `{part}` can't be mapped",
                    resource.setup
                ));
            }
        }
    }
}

/// A command's resource role must match the DAG's resources: what `resource set|get` allows
/// and what the master sends the cleanup follow from it.
fn role_problems(dag: &Dag, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        let TaskKind::Command {
            resource: Some(role),
            ..
        } = &task.kind
        else {
            continue;
        };
        let id = &task.id;
        match role {
            ResourceRole::Setup => {
                if !dag.resources.iter().any(|r| &r.setup == id) {
                    problems.push(format!(
                        "task `{id}`: a resource setup, but no resource has it as its setup"
                    ));
                }
                if task.output_type == TypeDesc::ProcessResult {
                    problems.push(format!(
                        "task `{id}`: a resource's value can't be a command's result"
                    ));
                }
            }
            ResourceRole::Cleanup { setup } => {
                if !dag
                    .resources
                    .iter()
                    .any(|r| &r.cleanup == id && &r.setup == setup)
                {
                    problems.push(format!(
                        "task `{id}`: the cleanup of `{setup}`, but it isn't that resource's \
                         cleanup"
                    ));
                }
            }
        }
    }
}

/// What a `first_of` or `at_least` needs (decision 347): options to choose from, each a whole
/// task's one value, and a count they can reach.
fn first_of_problems(dag: &Dag, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        let TaskKind::FirstOf { at_least } = task.kind else {
            continue;
        };
        let id = &task.id;
        let options: Vec<&TaskId> = dag
            .edges_into(id)
            .filter(|e| e.kind != EdgeKind::Requirement)
            .map(|e| &e.from)
            .collect();
        if dag.edges_into(id).any(|e| e.kind == EdgeKind::Order) {
            problems.push(format!(
                "task `{id}`: nothing can be ordered before first_of: what it waits for are the \
                 tasks it was given"
            ));
        }
        let distinct: BTreeSet<&TaskId> = options.iter().copied().collect();
        if distinct.len() < 2 || distinct.len() != options.len() {
            problems.push(format!(
                "task `{id}`: first_of needs at least two different tasks to choose from"
            ));
        }
        for option in &distinct {
            if dag.task(option).is_some_and(mapping::is_mapped) {
                problems.push(format!(
                    "task `{id}`: `{option}` is mapped, so it has no one value to choose"
                ));
            }
        }
        if at_least
            .is_some_and(|n| n == 0 || usize::try_from(n).unwrap_or(usize::MAX) > options.len())
        {
            problems.push(format!(
                "task `{id}`: at_least can't be reached with {} tasks to choose from",
                options.len()
            ));
        }
        if mapping::is_mapped(task) {
            problems.push(format!("task `{id}`: first_of can't be mapped"));
        }
    }
}

fn reference_problems(dag: &Dag, tasks: &BTreeSet<&TaskId>, problems: &mut Vec<String>) {
    for task in &dag.tasks {
        let mut reported = BTreeSet::new();
        for reference in task.references() {
            let Reference::Output { task: upstream, .. } = reference else {
                continue;
            };
            if !reported.insert(upstream) {
                continue;
            }
            if !tasks.contains(upstream) {
                problems.push(format!(
                    "task `{}` uses the output of `{upstream}`, which isn't a task of the DAG",
                    task.id
                ));
            } else if !dag.edges.iter().any(|e| {
                // A branch's output reaches its targets along their selection edges.
                matches!(e.kind, EdgeKind::Data | EdgeKind::Selection)
                    && &e.from == upstream
                    && e.to == task.id
            }) {
                problems.push(format!(
                    "task `{}` uses the output of `{upstream}` without a data edge from it",
                    task.id
                ));
            }
        }
    }
}

/// The tasks on or behind a cycle, in ID order (Kahn's algorithm leaves them over).
fn cycle<'a>(dag: &'a Dag, tasks: &BTreeSet<&'a TaskId>) -> Option<Vec<&'a TaskId>> {
    let mut incoming: BTreeMap<&TaskId, usize> = tasks.iter().map(|id| (*id, 0)).collect();
    // A cleanup waits for its resource's users, with no edge saying so.
    let waits = dag
        .resources
        .iter()
        .flat_map(|r| r.users.iter().map(move |user| (user, &r.cleanup)));
    let edges: BTreeSet<(&TaskId, &TaskId)> = dag
        .edges
        .iter()
        .map(|e| (&e.from, &e.to))
        .chain(waits)
        .filter(|(from, to)| tasks.contains(from) && tasks.contains(to))
        .collect();
    for (_, to) in &edges {
        *incoming.entry(to).or_default() += 1;
    }
    let mut ready: Vec<&TaskId> = incoming
        .iter()
        .filter(|(_, n)| **n == 0)
        .map(|(id, _)| *id)
        .collect();
    while let Some(id) = ready.pop() {
        incoming.remove(id);
        for (_, to) in edges.iter().filter(|(from, _)| *from == id) {
            if let Some(n) = incoming.get_mut(to) {
                *n -= 1;
                if *n == 0 {
                    ready.push(to);
                }
            }
        }
    }
    (!incoming.is_empty()).then(|| incoming.into_keys().collect())
}

/// What a parse's files are checked against.
#[derive(Debug, Clone, Copy)]
pub struct Checks<'a> {
    /// The master's schedr version: it must read the DAGs' JSON.
    pub master: SchedrVersion,
    /// The master's message limit: a task whose output can't fit it is refused (B33).
    pub max_message_size: usize,
    /// The secret storages of the worker that parsed them: a DAG naming another is refused;
    /// none when unknown (not checked).
    pub secret_storages: Option<&'a BTreeSet<String>>,
    /// The master's notification settings: a DAG's `notify=` is checked against them; none
    /// when unknown (not checked).
    pub notifications: Option<&'a crate::notifications::Notifications>,
}

impl Checks<'_> {
    /// Checks without secret storages.
    #[must_use]
    pub const fn new(master: SchedrVersion, max_message_size: usize) -> Self {
        Self {
            master,
            max_message_size,
            secret_storages: None,
            notifications: None,
        }
    }
}

/// Checks the files of a parse of `root`: a file whose path isn't inside the root, or that
/// defines an invalid DAG, gets an import error listing the problems and no DAGs.
#[must_use]
pub fn validate_files(root: &str, files: Vec<ParsedFile>, checks: &Checks<'_>) -> Vec<ParsedFile> {
    files
        .into_iter()
        .map(|file| validate_file(root, file, checks))
        .collect()
}

/// The connections a root's DAGs declare checked against the root's own (decision 179): a
/// file declaring one that isn't defined (or is defined twice) fails, as the attempt would.
/// Only over a whole root's files: one file alone doesn't see its siblings' connections.
#[must_use]
pub fn connection_problems(files: Vec<ParsedFile>) -> Vec<ParsedFile> {
    let registry = schedr_model::connection::Registry::of(&files);
    files
        .into_iter()
        .map(|mut file| {
            let problems: Vec<String> = file
                .dags
                .iter()
                .flat_map(|dag| {
                    registry
                        .problems(dag)
                        .into_iter()
                        .map(|p| format!("DAG `{}`: {p}", dag.id))
                })
                .collect();
            if !problems.is_empty() {
                file.dags.clear();
                file.error = Some(ErrorSummary {
                    exception: INVALID_DAG_EXCEPTION.to_owned(),
                    message: problems.join("\n"),
                    traceback: String::new(),
                });
            }
            file
        })
        .collect()
}

/// Each `TriggerDagOperator` checked against its target, over a whole root's files, for
/// `schedr dags check` only (decision 398): a target in the root that doesn't let its DAG start
/// it (not `trigger=Chained()`, or `Chained(by=...)` not naming it) makes the operator's file
/// fail; a target no file defines is a warning (it may be in a root not parsed yet, decision
/// 379). The master doesn't: one DAG's edit would stop another's file loading; its task fails
/// when it runs instead.
#[must_use]
pub fn target_problems(files: Vec<ParsedFile>) -> Vec<ParsedFile> {
    let triggers: BTreeMap<DagId, schedr_model::Trigger> = files
        .iter()
        .flat_map(|f| f.dags.iter().map(|d| (d.id.clone(), d.trigger.clone())))
        .collect();
    files
        .into_iter()
        .map(|mut file| {
            let mut problems = Vec::new();
            for dag in &file.dags {
                for task in &dag.tasks {
                    let TaskKind::TriggerDag { dag: target, .. } = &task.kind else {
                        continue;
                    };
                    let at = format!("DAG `{}`, task `{}`", dag.id, task.id);
                    match triggers.get(target) {
                        None => file.warnings.push(format!(
                            "{at}: no DAG `{target}` in this root; if it is in another, it runs \
                             once that root is parsed, else the task fails"
                        )),
                        Some(trigger) if !trigger.chained_from(&dag.id) => problems.push(format!(
                            "{at}: DAG `{target}` isn't started by this DAG: a TriggerDagOperator \
                             starts only a DAG with `trigger=Chained()` (or `Chained(by=[...])` \
                             naming `{}`)",
                            dag.id
                        )),
                        Some(_) => {}
                    }
                }
            }
            if !problems.is_empty() {
                file.dags.clear();
                file.error = Some(ErrorSummary {
                    exception: INVALID_DAG_EXCEPTION.to_owned(),
                    message: problems.join("\n"),
                    traceback: String::new(),
                });
            }
            file
        })
        .collect()
}

/// Room a worker's `Finished` needs besides the output (ids, the summary, framing).
const RESULT_OVERHEAD: u64 = 64 * 1024;

/// The tasks whose output can't reach the master: a result past its message limit
/// (`[grpc] max_message_size`) would end the worker's stream (B33). A command's
/// output holds stdout and stderr, each up to its `max_output_size`.
#[must_use]
pub fn output_problems(dag: &Dag, max_message_size: usize) -> Vec<String> {
    let limit = u64::try_from(max_message_size).unwrap_or(u64::MAX);
    // A trigger's events go the same way, a batch filled to its limit (Grok's review).
    let trigger = match &dag.trigger {
        schedr_model::Trigger::Event { task, .. } => Some(task.as_ref()),
        _ => None,
    };
    dag.tasks
        .iter()
        .chain(trigger)
        .filter_map(|task| {
            let streams = match task.kind {
                TaskKind::Empty
                | TaskKind::FirstOf { .. }
                | TaskKind::WaitFor { .. }
                | TaskKind::WaitForEvent { .. }
                | TaskKind::TriggerDag { .. } => {
                    return None;
                }
                TaskKind::Command { .. } | TaskKind::Script { .. } => 2,
                TaskKind::Python { .. } | TaskKind::Sensor { .. } => 1,
            };
            let size = task.settings.max_output_size;
            let needed = size.saturating_mul(streams).saturating_add(RESULT_OVERHEAD);
            (needed > limit).then(|| {
                let each = if streams == 2 {
                    " (stdout and stderr each)"
                } else {
                    ""
                };
                format!(
                    "task `{}`: max_output_size {}{each} can't reach the master, which accepts \
                     results up to {} ([grpc] max_message_size, less 64 KiB for the \
                     rest of the result): lower max_output_size, or raise the setting",
                    task.id,
                    shown_size(size),
                    shown_size(limit.saturating_sub(RESULT_OVERHEAD) / streams),
                )
            })
        })
        .collect()
}

/// A size as people read it: whole MiB or KiB when exact, else bytes.
fn shown_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= KIB && bytes.is_multiple_of(KIB) {
        format!("{} KiB", bytes / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn validate_file(root: &str, mut file: ParsedFile, checks: &Checks<'_>) -> ParsedFile {
    if !inside_root(&file.path) {
        // Its connections too: a test imports a connection's file by its path.
        file.dags.clear();
        file.connections.clear();
        file.error = Some(ErrorSummary {
            exception: INVALID_PATH_EXCEPTION.to_owned(),
            message: format!("`{}` isn't a relative path inside the root", file.path),
            traceback: String::new(),
        });
        return file;
    }
    if file.error.is_some() {
        // Failed to import: nothing of it counts, whatever it reported.
        file.dags.clear();
        file.connections.clear();
        return file;
    }
    let mut problems: Vec<String> = Vec::new();
    file.connections.retain(|def| {
        let size = serde_json::to_vec(def).map_or(usize::MAX, |json| json.len());
        if size <= schedr_model::connection::MAX_CONNECTION_SIZE {
            return true;
        }
        problems.push(format!(
            "connection `{}`: its values take {size} bytes, over the 1 MiB a connection may \
             hold (they travel with each task opening it); give bigger data to the task",
            def.name
        ));
        false
    });
    problems.extend(file.dags.iter().flat_map(|dag| {
        let mut problems = dag_problems(dag, root, &file.path, checks.master);
        problems.extend(output_problems(dag, checks.max_message_size));
        if let Some(storages) = checks.secret_storages {
            problems.extend(storage_problems(dag, storages));
        }
        if let Some(notifications) = checks.notifications {
            problems.extend(notifications.problems(dag));
        }
        problems
            .into_iter()
            .map(move |p| format!("DAG `{}`: {p}", dag.id))
    }));
    if !problems.is_empty() {
        file.dags.clear();
        file.error = Some(ErrorSummary {
            exception: INVALID_DAG_EXCEPTION.to_owned(),
            message: problems.join("\n"),
            traceback: String::new(),
        });
    }
    file
}

fn inside_root(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && Path::new(path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}
