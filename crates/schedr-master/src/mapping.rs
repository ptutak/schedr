//! Mapped tasks (decisions 69, 70, 74-79): which edges are lanes, the dimensions a mapped
//! task's instances span, an instance's place in each, and the master's resolution of what a
//! task is mapped over.
//!
//! - A mapped task's instances are the cartesian product of its mapped lists, the first
//!   argument written varying slowest (`.expand_kwargs`: one dimension, a list of dicts).
//! - A **lane** is an element-wise data edge: the task is mapped over the whole output of a
//!   mapped upstream task, which it references nowhere else; instance `i` then waits only for
//!   that upstream's instance `i` (70 B). Every other edge into a mapped task counts its
//!   upstream as a whole (70.1).

use std::collections::{BTreeMap, BTreeSet};

use schedr_db::{Database, DbError};
use schedr_model::{
    Dag, Edge, EdgeKind, Expand, PathElem, Reference, Task, TaskId, TaskInstanceState, Value,
};
use serde_json::Value as Json;

#[cfg(test)]
mod tests;

/// Whether `task` is mapped (`.expand(...)`).
#[must_use]
pub const fn is_mapped(task: &Task) -> bool {
    task.expand.is_some()
}

/// One dimension of a mapped task's instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dim<'a> {
    /// A list the master resolves: a literal, or a reference to a list.
    Values(&'a Value),
    /// The whole output of a mapped upstream task, element by element (a lane).
    Lane(&'a TaskId),
}

/// A mapped task's dimensions, in order: each with the parameter it fills (`None` for
/// `.expand_kwargs`, whose elements are dicts of arguments).
#[must_use]
pub fn dims<'a>(dag: &Dag, task: &'a Task) -> Vec<(Option<&'a str>, Dim<'a>)> {
    let lanes = lane_sources(dag, task);
    let dim = |value: &'a Value| match lane_of(value) {
        Some(upstream) if lanes.contains(upstream) => Dim::Lane(upstream),
        _ => Dim::Values(value),
    };
    match &task.expand {
        Some(Expand::Product(over)) => over
            .iter()
            .map(|m| (Some(m.name.as_str()), dim(&m.value)))
            .collect(),
        Some(Expand::Kwargs(over)) => vec![(None, dim(over))],
        None => Vec::new(),
    }
}

/// The upstream task a mapped value takes whole (no path), if it is one reference.
fn lane_of(value: &Value) -> Option<&TaskId> {
    match value {
        Value::Ref(Reference::Output { task, path, .. }) if path.is_empty() => Some(task),
        _ => None,
    }
}

/// The mapped upstream tasks `task` is mapped over element by element (its lanes): each the
/// whole output of a mapped task, referenced nowhere else in `task` (a second reference,
/// `.partial(all=sizes)` or `sizes[0]`, makes it count as a whole: 70.1).
#[must_use]
pub fn lane_sources<'a>(dag: &Dag, task: &'a Task) -> BTreeSet<&'a TaskId> {
    let mapped_values: Vec<&Value> = match &task.expand {
        Some(Expand::Product(over)) => over.iter().map(|m| &m.value).collect(),
        Some(Expand::Kwargs(over)) => vec![over],
        None => return BTreeSet::new(),
    };
    let mut uses: BTreeMap<&TaskId, usize> = BTreeMap::new();
    for reference in task.references() {
        if let Some(upstream) = reference.upstream_task() {
            *uses.entry(upstream).or_default() += 1;
        }
    }
    mapped_values
        .into_iter()
        .filter_map(lane_of)
        .filter(|upstream| uses.get(upstream) == Some(&1))
        .filter(|upstream| dag.task(upstream).is_some_and(is_mapped))
        .collect()
}

/// Whether `edge` is a lane: an element-wise data edge into a mapped task (70 B). An order
/// edge, or a data edge whose upstream is used as a whole, counts its upstream as a whole.
#[must_use]
pub fn is_lane(dag: &Dag, edge: &Edge) -> bool {
    edge.kind == EdgeKind::Data
        && dag
            .task(&edge.to)
            .is_some_and(|task| lane_sources(dag, task).contains(&edge.from))
}

/// Instance `index`'s place in each dimension of `sizes` (the first varying slowest).
#[must_use]
pub fn coordinates(index: usize, sizes: &[usize]) -> Vec<usize> {
    let mut rest = index;
    let mut place = vec![0; sizes.len()];
    for (at, size) in sizes.iter().enumerate().rev() {
        if *size > 0 {
            place[at] = rest % size;
            rest /= size;
        }
    }
    place
}

/// The number of instances of `sizes`; `None` past `usize`.
#[must_use]
pub fn count(sizes: &[usize]) -> Option<usize> {
    sizes
        .iter()
        .try_fold(1_usize, |n, size| n.checked_mul(*size))
}

/// What a mapped value is resolved against: the whole outputs of the tasks it references (a
/// mapped task's as the list of its instances' outputs) and their outcomes (`None`: skipped).
#[derive(Debug, Default)]
pub struct Sources {
    /// Whole outputs by task.
    pub outputs: BTreeMap<TaskId, Json>,
    /// Outcomes by task, of those that succeeded or were skipped.
    pub outcomes: BTreeMap<TaskId, Option<String>>,
}

/// The stored JSON `value` stands for, as the runner would resolve it (`None` for a
/// reference whose edge wasn't taken). Errors say why a value can't be resolved here.
///
/// # Errors
///
/// A message for a reference the master can't resolve (the run's context, a missing output
/// or path).
pub fn resolve(value: &Value, sources: &Sources) -> Result<Json, String> {
    match value {
        Value::Literal(json) => Ok(json.clone()),
        Value::Ref(reference) => resolve_ref(reference, sources),
        Value::List(items) => items
            .iter()
            .map(|item| resolve(item, sources))
            .collect::<Result<Vec<_>, _>>()
            .map(Json::Array),
        Value::Map(entries) => entries
            .iter()
            .map(|(key, item)| Ok((key.clone(), resolve(item, sources)?)))
            .collect::<Result<serde_json::Map<_, _>, String>>()
            .map(Json::Object),
        Value::Template(_) => Err("a text template isn't a list".to_owned()),
    }
}

fn resolve_ref(reference: &Reference, sources: &Sources) -> Result<Json, String> {
    match reference {
        Reference::DagCtx { .. } => Err("the run's context isn't a list".to_owned()),
        Reference::Outcome { task } => Ok(sources
            .outcomes
            .get(task)
            .cloned()
            .flatten()
            .map_or(Json::Null, Json::String)),
        Reference::Output { task, path, on } => {
            if let Some(outcome) = sources.outcomes.get(task) {
                // Skipped, or another outcome than it was narrowed to: `None`.
                let taken = outcome
                    .as_ref()
                    .is_some_and(|o| on.is_empty() || on.contains(o));
                if !taken {
                    return Ok(Json::Null);
                }
            }
            let mut value = sources
                .outputs
                .get(task)
                .ok_or_else(|| format!("the output of `{task}` isn't stored"))?;
            for step in path {
                value = match (step, value) {
                    (PathElem::Field(name), Json::Object(fields)) => fields
                        .get(name)
                        .ok_or_else(|| format!("`{task}`: `{name}` doesn't exist in the output"))?,
                    (PathElem::Index(i), Json::Array(items)) => usize::try_from(*i)
                        .ok()
                        .and_then(|i| items.get(i))
                        .ok_or_else(|| format!("`{task}`: index {i} doesn't exist"))?,
                    (PathElem::Index(i), Json::Object(fields)) => fields
                        .get("$tuple")
                        .and_then(Json::as_array)
                        .and_then(|items| usize::try_from(*i).ok().and_then(|i| items.get(i)))
                        .ok_or_else(|| format!("`{task}`: index {i} doesn't exist"))?,
                    _ => return Err(format!("`{task}`: the path doesn't exist in the output")),
                };
            }
            Ok(value.clone())
        }
    }
}

/// A short description of `json`'s kind, for messages ("null", "a string").
#[must_use]
pub fn kind_of(json: &Json) -> &'static str {
    match json {
        Json::Null => "null",
        Json::Bool(_) => "a boolean",
        Json::Number(_) => "a number",
        Json::String(_) => "a string",
        Json::Array(_) => "a list",
        Json::Object(_) => "an object",
    }
}

/// The whole outputs and outcomes of `tasks` in a run, to resolve mapped values against: a
/// mapped task's output is the list of its instances' outputs, in `map_index` order, `null`
/// for one that didn't succeed (decision 77).
///
/// # Errors
///
/// [`DbError`] if the database fails.
pub async fn sources(
    db: &Database,
    dag: &Dag,
    run_id: &str,
    tasks: &BTreeSet<&TaskId>,
) -> Result<Sources, DbError> {
    let all: Vec<&TaskId> = tasks.iter().copied().collect();
    let (mapped, plain): (Vec<&TaskId>, Vec<&TaskId>) = all
        .iter()
        .partition(|task| dag.task(task).is_some_and(is_mapped));
    let mut sources = Sources {
        outputs: db.outputs_of(run_id, &plain).await?,
        outcomes: db.upstream_outcomes(run_id, &all).await?,
    };
    for task in mapped {
        let list = db
            .instance_results(run_id, task)
            .await?
            .into_iter()
            .map(|r| {
                if r.state == TaskInstanceState::Success {
                    r.output.unwrap_or(Json::Null)
                } else {
                    Json::Null
                }
            })
            .collect();
        sources.outputs.insert(task.clone(), Json::Array(list));
    }
    Ok(sources)
}

/// Mapped lists already resolved, by (run, task, dimension): the instances of one task share
/// them, so assigning N of them resolves each list once, not N times. Kept for one step.
pub type Resolved = BTreeMap<(String, TaskId, usize), Json>;

/// Why a task instance's inputs can't be made.
#[derive(Debug, thiserror::Error)]
pub enum InputsError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// An element `.expand_kwargs()` maps over isn't a dict of arguments: the instance can't
    /// run (it would every time).
    #[error("{0}")]
    NotArguments(String),
}

/// A task instance's inputs, as JSON: the whole outputs of the upstream tasks it uses (not
/// its lanes), and for an instance of a mapped task its own arguments under `$expand`, with
/// the outcome of each taken from a lane under `$expand_outcomes` (decision 76). A lane reads
/// its one upstream instance; a mapped list is resolved once per `resolved`.
///
/// # Errors
///
/// [`InputsError::Db`] if the database fails; [`InputsError::NotArguments`] for an element
/// of `.expand_kwargs()` over a mapped task that isn't a dict (a mapped list's elements are
/// checked when the task expands).
pub async fn inputs(
    db: &Database,
    dag: &Dag,
    run_id: &str,
    (task, map_index): (&Task, i32),
    resolved: &mut Resolved,
) -> Result<Json, InputsError> {
    let lanes = lane_sources(dag, task);
    let used: BTreeSet<&TaskId> = task
        .references()
        .into_iter()
        .filter_map(Reference::upstream_task)
        .filter(|upstream| !lanes.contains(upstream))
        .collect();
    let sources = sources(db, dag, run_id, &used).await?;
    let mut inputs: serde_json::Map<String, Json> = sources
        .outputs
        .iter()
        .map(|(task, output)| (task.to_string(), output.clone()))
        .collect();
    let Ok(index) = usize::try_from(map_index) else {
        return Ok(Json::Object(inputs));
    };
    let sizes = db.map_sizes(run_id, &task.id).await?.unwrap_or_default();
    let place = coordinates(index, &sizes);
    let mut expand = serde_json::Map::new();
    let mut outcomes = serde_json::Map::new();
    for (d, ((name, dim), at)) in dims(dag, task).into_iter().zip(place).enumerate() {
        let lane = match dim {
            Dim::Lane(upstream) => Some(upstream),
            Dim::Values(_) => None,
        };
        let (element, outcome) = match dim {
            Dim::Values(value) => {
                let list = resolved
                    .entry((run_id.to_owned(), task.id.clone(), d))
                    .or_insert_with(|| {
                        resolve(value, &sources).unwrap_or_else(|error| {
                            tracing::warn!(
                                task_id = %task.id, %error, "a mapped value no longer resolves"
                            );
                            Json::Null
                        })
                    });
                (list.get(at).cloned().unwrap_or(Json::Null), None)
            }
            Dim::Lane(upstream) => {
                let place = i32::try_from(at).unwrap_or(i32::MAX);
                let result = db.instance_result(run_id, upstream, place).await?;
                let output = result.as_ref().and_then(|r| r.output.clone());
                (output.unwrap_or(Json::Null), result.and_then(|r| r.outcome))
            }
        };
        match name {
            Some(name) => {
                if let Some(outcome) = outcome {
                    outcomes.insert(name.to_owned(), Json::String(outcome));
                }
                expand.insert(name.to_owned(), element);
            }
            // `.expand_kwargs()`: the element is a dict of arguments.
            None => match (element, lane) {
                (Json::Object(arguments), _) => expand.extend(arguments),
                (element, Some(upstream)) => {
                    return Err(InputsError::NotArguments(format!(
                        "element {at} of `{upstream}` isn't a dict of arguments for \
                         `.expand_kwargs()`: {element}"
                    )));
                }
                // A mapped list was checked when the task expanded.
                (_, None) => {}
            },
        }
    }
    inputs.insert("$expand".to_owned(), Json::Object(expand));
    inputs.insert("$expand_outcomes".to_owned(), Json::Object(outcomes));
    Ok(Json::Object(inputs))
}
