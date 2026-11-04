//! The master's checks of uploaded parse results.
//!
//! The Python API already refuses wrong DAGs at parse time; the master checks again what it
//! relies on to schedule (it never runs user code, so DAG JSON is its only view of a DAG): the
//! version that produced it, where it is, its trigger, task IDs, edges, cycles and the
//! references between tasks. A file with an invalid DAG is treated as a file that failed to
//! import: its DAGs keep their last good version (see PROJECT.md, DAG code delivery).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::task::{CleanupRun, Task};
use schedr_model::{
    CommandLine, Dag, EdgeKind, Expand, Reference, SchedrVersion, SecretDecl, TaskId, TaskKind,
    Template, TemplatePart, Value,
};

use crate::fire_times::FireTimes;

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
        let scheduled = !matches!(dag.trigger, schedr_model::Trigger::None);
        problems.extend(crate::params::problems(params, scheduled));
    }
    group_problems(dag, &mut problems);
    expand_problems(dag, &mut problems);
    param_problems(dag, &mut problems);
    shell_problems(dag, &mut problems);
    secret_problems(dag, &mut problems);
    edge_problems(dag, &seen, &mut problems);
    reference_problems(dag, &seen, &mut problems);
    resource_problems(dag, &seen, &mut problems);
    if let Some(cycle) = cycle(dag, &seen) {
        let names: Vec<String> = cycle.iter().map(|id| format!("`{id}`")).collect();
        problems.push(format!(
            "the tasks {} depend on each other in a cycle",
            names.join(", ")
        ));
    }
    problems
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
        (TaskKind::Python { .. }, CleanupRun::Python)
        | (TaskKind::Command { .. }, CleanupRun::Command(_)) => None,
        (TaskKind::Python { .. }, CleanupRun::Command(_)) => Some(format!(
            "task `{id}`: a Python task's cleanup is a function, not a command"
        )),
        (TaskKind::Command { .. }, CleanupRun::Python) => Some(format!(
            "task `{id}`: a command task's cleanup is a command, not `python`"
        )),
        (TaskKind::Empty, _) => Some(format!(
            "task `{id}`: an empty task has nothing to clean up"
        )),
    }
}

/// Secrets (see PROJECT.md, Secret storage): each name well formed, none in a command line
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
        let mut in_text: Vec<&Template> = Vec::new();
        match &task.kind {
            TaskKind::Command {
                command,
                env,
                connection_env,
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
            TaskKind::Python { arguments, .. } => {
                for value in arguments.values() {
                    in_text.extend(value_templates(value));
                }
            }
            TaskKind::Empty => {}
        }
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
        let TaskKind::Python {
            arguments, branch, ..
        } = &task.kind
        else {
            problems.push(format!(
                "task `{}`: only a @task function can be mapped (`.expand()`)",
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
            // The names a list written in the DAG gives (an upstream output's are unknown).
            let elements: Vec<Vec<&str>> = match over {
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
            };
            let fixed: std::collections::BTreeSet<&str> = elements
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
}

impl Checks<'_> {
    /// Checks without secret storages.
    #[must_use]
    pub const fn new(master: SchedrVersion, max_message_size: usize) -> Self {
        Self {
            master,
            max_message_size,
            secret_storages: None,
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

/// Room a worker's `Finished` needs besides the output (ids, the summary, framing).
const RESULT_OVERHEAD: u64 = 64 * 1024;

/// The tasks whose output can't reach the master: a result past its message limit
/// (`[master] grpc_max_message_size`) would end the worker's stream (B33). A command's
/// output holds stdout and stderr, each up to its `max_output_size`.
#[must_use]
pub fn output_problems(dag: &Dag, max_message_size: usize) -> Vec<String> {
    let limit = u64::try_from(max_message_size).unwrap_or(u64::MAX);
    dag.tasks
        .iter()
        .filter_map(|task| {
            let streams = match task.kind {
                TaskKind::Empty => return None,
                TaskKind::Command { .. } => 2,
                TaskKind::Python { .. } => 1,
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
                     results up to {} ([master] grpc_max_message_size, less 64 KiB for the \
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
