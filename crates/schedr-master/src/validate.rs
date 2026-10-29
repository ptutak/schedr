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
use schedr_model::{Dag, EdgeKind, Reference, SchedrVersion, TaskId, TaskKind};

use crate::fire_times::FireTimes;

#[cfg(test)]
mod tests;

/// The exception name of the import error recorded for a file with an invalid DAG.
pub const INVALID_DAG_EXCEPTION: &str = "InvalidDag";

/// The exception name of the import error recorded for a path outside its root.
pub const INVALID_PATH_EXCEPTION: &str = "InvalidPath";

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
    group_problems(dag, &mut problems);
    edge_problems(dag, &seen, &mut problems);
    reference_problems(dag, &seen, &mut problems);
    if let Some(cycle) = cycle(dag, &seen) {
        let names: Vec<String> = cycle.iter().map(|id| format!("`{id}`")).collect();
        problems.push(format!(
            "the tasks {} depend on each other in a cycle",
            names.join(", ")
        ));
    }
    problems
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
        } else if edge.kind != EdgeKind::Selection && from_branch {
            problems.push(format!(
                "edge `{}` -> `{}`: of kind `{}`, but every edge from the `@branch` task `{}` must \
                 be a selection edge",
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
    let edges: BTreeSet<(&TaskId, &TaskId)> = dag
        .edges
        .iter()
        .filter(|e| tasks.contains(&e.from) && tasks.contains(&e.to))
        .map(|e| (&e.from, &e.to))
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

/// Checks the files of a parse of `root`: a file whose path isn't inside the root, or that
/// defines an invalid DAG, gets an import error listing the problems and no DAGs.
#[must_use]
pub fn validate_files(
    root: &str,
    files: Vec<ParsedFile>,
    master: SchedrVersion,
    max_message_size: usize,
) -> Vec<ParsedFile> {
    files
        .into_iter()
        .map(|file| validate_file(root, file, master, max_message_size))
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

fn validate_file(
    root: &str,
    mut file: ParsedFile,
    master: SchedrVersion,
    max_message_size: usize,
) -> ParsedFile {
    if !inside_root(&file.path) {
        file.dags.clear();
        file.error = Some(ErrorSummary {
            exception: INVALID_PATH_EXCEPTION.to_owned(),
            message: format!("`{}` isn't a relative path inside the root", file.path),
            traceback: String::new(),
        });
        return file;
    }
    if file.error.is_some() {
        file.dags.clear();
        return file;
    }
    let problems: Vec<String> = file
        .dags
        .iter()
        .flat_map(|dag| {
            let mut problems = dag_problems(dag, root, &file.path, master);
            problems.extend(output_problems(dag, max_message_size));
            problems
                .into_iter()
                .map(move |p| format!("DAG `{}`: {p}", dag.id))
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
}

fn inside_root(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && Path::new(path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}
