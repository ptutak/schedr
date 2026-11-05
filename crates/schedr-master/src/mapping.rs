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
    Dag, Edge, EdgeKind, Expand, PathElem, Reference, ResourceRole, Task, TaskId,
    TaskInstanceState, TaskKind, Value,
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

/// The mapped upstream task a dimension narrows element by element though it counts as a
/// whole (used elsewhere in the task, so not a lane), with the outcomes taken:
/// `.partial(all=results).expand(rows=results.on(Rows))`. Its instance whose upstream
/// instance ended with another outcome, or was skipped, is skipped, as on a lane (decision
/// 119).
#[must_use]
pub fn narrowed_whole<'a>(dag: &Dag, dim: &Dim<'a>) -> Option<(&'a TaskId, &'a [String])> {
    match dim {
        Dim::Values(Value::Ref(Reference::Output { task, path, on }))
            if path.is_empty() && !on.is_empty() && dag.task(task).is_some_and(is_mapped) =>
        {
            Some((task, on))
        }
        _ => None,
    }
}

/// Whether `edge`'s outcomes are taken per element ([`narrowed_whole`]), not on the whole
/// upstream task, whose own outcome is only that its instances ended.
#[must_use]
pub fn narrows_per_element(dag: &Dag, edge: &Edge) -> bool {
    edge.kind == EdgeKind::Data
        && !edge.on.is_empty()
        && dag.task(&edge.to).is_some_and(|task| {
            dims(dag, task).iter().any(|(_, dim)| {
                narrowed_whole(dag, dim)
                    .is_some_and(|(up, on)| *up == edge.from && on == edge.on.as_slice())
            })
        })
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
    /// The mapped tasks among them: a whole mapped task's outcome only says its instances
    /// ended, so `.on(...)` over its output narrows its elements one by one (decision 119),
    /// never the list.
    pub mapped: BTreeSet<TaskId>,
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

/// Whether a value is left to its instance's runner to build: it holds a template or a
/// `dag_ctx` reference, which the master doesn't render (decision 118), the run's parameters
/// included, or a secret, which the master never sees.
#[must_use]
pub fn built_by_runner(value: &Value) -> bool {
    match value {
        Value::Template(_)
        | Value::Ref(Reference::DagCtx { .. } | Reference::Param { .. } | Reference::Secret(_)) => {
            true
        }
        Value::List(items) => items.iter().any(built_by_runner),
        Value::Map(entries) => entries.values().any(built_by_runner),
        Value::Literal(_) | Value::Ref(_) => false,
    }
}

/// What a mapped value stands for, as [`resolve`], except that the elements of a list
/// written in the file that its runners build ([`built_by_runner`]) are `null` here (in a
/// dict, its entries the runners build): each reaches its instance in its stored form
/// (decision 118), and the list's length is known.
///
/// # Errors
///
/// As [`resolve`].
pub fn resolve_mapped(value: &Value, sources: &Sources) -> Result<Json, String> {
    match value {
        Value::List(items) => items
            .iter()
            .map(|item| match item {
                // A dict keeps its keys, for `.expand_kwargs()`: only its entries the runner
                // builds are `null`.
                Value::Map(entries) => entries
                    .iter()
                    .map(|(key, entry)| {
                        let entry = if built_by_runner(entry) {
                            Json::Null
                        } else {
                            resolve(entry, sources)?
                        };
                        Ok((key.clone(), entry))
                    })
                    .collect::<Result<serde_json::Map<_, _>, String>>()
                    .map(Json::Object),
                item if built_by_runner(item) => Ok(Json::Null),
                item => resolve(item, sources),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Json::Array),
        _ => resolve(value, sources),
    }
}

fn resolve_ref(reference: &Reference, sources: &Sources) -> Result<Json, String> {
    match reference {
        Reference::DagCtx { .. } => Err("the run's context isn't a list".to_owned()),
        // Refused when the DAG is validated: the number of instances would depend on them.
        Reference::Param { .. } => Err("a run's parameters can't be mapped over".to_owned()),
        Reference::Secret(_) => Err("a secret can't be mapped over".to_owned()),
        Reference::Outcome { task } => Ok(sources
            .outcomes
            .get(task)
            .cloned()
            .flatten()
            .map_or(Json::Null, Json::String)),
        Reference::Output { task, path, on } => {
            if let Some(outcome) = sources.outcomes.get(task) {
                // Skipped, or another outcome than it was narrowed to: `None` (a mapped task:
                // only skipped, its `.on(...)` being per element).
                let on: &[String] = if sources.mapped.contains(task) {
                    &[]
                } else {
                    on
                };
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
                    (PathElem::Field(name), Json::Object(fields)) => match fields.get("$dict") {
                        // A dict shaped like a tag is stored escaped (B66).
                        Some(Json::Object(inner)) if fields.len() == 1 => inner,
                        _ => fields,
                    }
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
/// for one that didn't succeed (decision 77). `known` gives an instance's state when the
/// caller knows it ahead of the database (a result being applied, its state not stored yet,
/// B78): it is used instead of the stored one.
///
/// # Errors
///
/// [`DbError`] if the database fails.
pub async fn sources(
    db: &Database,
    dag: &Dag,
    run_id: &str,
    tasks: &BTreeSet<&TaskId>,
    known: impl Fn(&TaskId, i32) -> Option<TaskInstanceState>,
) -> Result<Sources, DbError> {
    let all: Vec<&TaskId> = tasks.iter().copied().collect();
    let (mapped, plain): (Vec<&TaskId>, Vec<&TaskId>) = all
        .iter()
        .partition(|task| dag.task(task).is_some_and(is_mapped));
    let mut sources = Sources {
        outputs: db.outputs_of(run_id, &plain).await?,
        outcomes: db.upstream_outcomes(run_id, &all).await?,
        mapped: BTreeSet::new(),
    };
    for task in mapped {
        let list = db
            .instance_results(run_id, task)
            .await?
            .into_iter()
            .map(|r| {
                let state = known(task, r.map_index).unwrap_or(r.state);
                if state == TaskInstanceState::Success {
                    r.output.unwrap_or(Json::Null)
                } else {
                    Json::Null
                }
            })
            .collect();
        sources.outputs.insert(task.clone(), Json::Array(list));
        sources.mapped.insert(task.clone());
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

/// The setup whose value a resource's command or script cleanup reads (`schedr task-ctx resource get`),
/// whatever its templates name.
fn resource_setup(task: &Task) -> Option<&TaskId> {
    match &task.kind {
        TaskKind::Command {
            resource: Some(ResourceRole::Cleanup { setup }),
            ..
        }
        | TaskKind::Script {
            resource: Some(ResourceRole::Cleanup { setup }),
            ..
        } => Some(setup),
        _ => None,
    }
}

/// A task instance's inputs, as JSON: the whole outputs of the upstream tasks it uses (not
/// its lanes), and for an instance of a mapped task its own arguments under `$expand`, with
/// the outcome of each taken from a lane under `$expand_outcomes` (decision 76), and those its
/// runner builds in their stored form under `$expand_values` (decision 118). A lane reads
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
    let upstream = |references: Vec<&Reference>| -> BTreeSet<TaskId> {
        references
            .into_iter()
            .filter_map(Reference::upstream_task)
            .filter(|upstream| !lanes.contains(upstream))
            .cloned()
            .collect()
    };
    // Whole outputs only for the arguments' output references: what the task is mapped over
    // reaches each instance as its element, under `$expand`, and an outcome reference is
    // answered from the outcomes, so neither is sent again whole (B83).
    let dims = dims(dag, task);
    // An element its runner builds (decision 118) needs the outputs its template references,
    // as an argument does.
    let runner_built = dims.iter().filter_map(|(_, dim)| match dim {
        Dim::Values(Value::List(items)) => Some(items.iter().filter(|i| built_by_runner(i))),
        _ => None,
    });
    let mut whole = upstream(
        task.argument_references()
            .into_iter()
            .chain(runner_built.flatten().flat_map(Value::references))
            .filter(|r| matches!(r, Reference::Output { .. }))
            .collect(),
    );
    whole.extend(resource_setup(task).cloned());
    // The mapped values' sources only while their resolved lists aren't known yet.
    let unresolved = dims.iter().enumerate().any(|(d, (_, dim))| {
        matches!(dim, Dim::Values(_))
            && !resolved.contains_key(&(run_id.to_owned(), task.id.clone(), d))
    });
    let used = if unresolved && map_index >= 0 {
        upstream(task.references())
    } else {
        whole.clone()
    };
    let used: BTreeSet<&TaskId> = used.iter().collect();
    let sources = sources(db, dag, run_id, &used, |_, _| None).await?;
    let mut inputs: serde_json::Map<String, Json> = sources
        .outputs
        .iter()
        .filter(|(task, _)| whole.contains(task))
        .map(|(task, output)| (task.to_string(), output.clone()))
        .collect();
    let Ok(index) = usize::try_from(map_index) else {
        return Ok(Json::Object(inputs));
    };
    let sizes = db.map_sizes(run_id, &task.id).await?.unwrap_or_default();
    let place = coordinates(index, &sizes);
    let mut arguments = InstanceArguments::default();
    for (d, ((name, dim), at)) in dims.into_iter().zip(place).enumerate() {
        let lane = match dim {
            Dim::Lane(upstream) => Some(upstream),
            Dim::Values(_) => None,
        };
        let (element, outcome) = match dim {
            Dim::Values(value) => {
                let list = resolved
                    .entry((run_id.to_owned(), task.id.clone(), d))
                    .or_insert_with(|| {
                        resolve_mapped(value, &sources).unwrap_or_else(|error| {
                            tracing::warn!(
                                task_id = %task.id, %error, "a mapped value no longer resolves"
                            );
                            Json::Null
                        })
                    });
                let element = list.get(at).cloned().unwrap_or(Json::Null);
                // The whole output of a mapped task used whole: the element is its instance's
                // output, decoded as that instance's outcome, as on a lane.
                let outcome = match lane_of(value) {
                    Some(upstream) if dag.task(upstream).is_some_and(is_mapped) => {
                        let place = i32::try_from(at).unwrap_or(i32::MAX);
                        db.instance_result(run_id, upstream, place)
                            .await?
                            .and_then(|r| r.outcome)
                    }
                    _ => None,
                };
                (element, outcome)
            }
            Dim::Lane(upstream) => {
                let place = i32::try_from(at).unwrap_or(i32::MAX);
                let result = db.instance_result(run_id, upstream, place).await?;
                let output = result.as_ref().and_then(|r| r.output.clone());
                (output.unwrap_or(Json::Null), result.and_then(|r| r.outcome))
            }
        };
        // An element its runner builds, in its stored form (decision 118).
        let runner_element = match dim {
            Dim::Values(Value::List(items)) => items.get(at).filter(|i| built_by_runner(i)),
            _ => None,
        };
        let element = Element {
            name,
            value: element,
            outcome,
            stored: runner_element,
        };
        arguments.place(element, at, lane)?;
    }
    arguments.into_inputs(&mut inputs);
    Ok(Json::Object(inputs))
}

/// One dimension's element for an instance.
struct Element<'a> {
    /// The parameter it fills; `None` for `.expand_kwargs()`.
    name: Option<&'a str>,
    /// What it resolved to.
    value: Json,
    /// The outcome of its lane's upstream instance.
    outcome: Option<String>,
    /// Written with a template or the run's context: built by the runner (decision 118).
    stored: Option<&'a Value>,
}

/// An instance's own arguments: `$expand`, `$expand_outcomes` and `$expand_values`.
#[derive(Default)]
struct InstanceArguments {
    expand: serde_json::Map<String, Json>,
    stored: serde_json::Map<String, Json>,
    outcomes: serde_json::Map<String, Json>,
}

impl InstanceArguments {
    /// Places element `at` of a dimension (`lane`: its mapped upstream task, if a lane).
    fn place(
        &mut self,
        element: Element<'_>,
        at: usize,
        lane: Option<&TaskId>,
    ) -> Result<(), InputsError> {
        let Element {
            name,
            value,
            outcome,
            stored,
        } = element;
        match name {
            Some(name) => {
                if let Some(outcome) = outcome {
                    self.outcomes.insert(name.to_owned(), Json::String(outcome));
                }
                match stored {
                    Some(item) => {
                        let item = serde_json::to_value(item).unwrap_or(Json::Null);
                        self.stored.insert(name.to_owned(), item);
                    }
                    None => {
                        self.expand.insert(name.to_owned(), value);
                    }
                }
            }
            // `.expand_kwargs()`: the element is a dict of arguments.
            None => match (value, lane) {
                (Json::Object(mut arguments), _) => {
                    // Arguments written with a template or the run's context are built by the
                    // runner (decision 118).
                    if let Some(Value::Map(entries)) = stored {
                        for (key, entry) in entries.iter().filter(|(_, e)| built_by_runner(e)) {
                            arguments.remove(key);
                            let entry = serde_json::to_value(entry).unwrap_or(Json::Null);
                            self.stored.insert(key.clone(), entry);
                        }
                    }
                    self.expand.extend(arguments);
                }
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
        Ok(())
    }

    fn into_inputs(self, inputs: &mut serde_json::Map<String, Json>) {
        inputs.insert("$expand".to_owned(), Json::Object(self.expand));
        inputs.insert("$expand_outcomes".to_owned(), Json::Object(self.outcomes));
        if !self.stored.is_empty() {
            inputs.insert("$expand_values".to_owned(), Json::Object(self.stored));
        }
    }
}
