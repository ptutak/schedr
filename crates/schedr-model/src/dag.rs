//! A DAG as stored in DAG JSON: what the Python runner sends after parsing a DAG file.

use jiff::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::{DagId, TaskId};
use crate::task::Task;
use crate::trigger::Trigger;
use crate::version::SchedrVersion;

#[cfg(test)]
mod tests;

/// Where a DAG is defined: a DAG root and a file path relative to it.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct DagLocation {
    /// The root's name (see PROJECT.md, DAG roots).
    pub root: String,
    /// The file's path relative to the root, with `/` separators.
    pub path: String,
}

/// Why one task depends on another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    /// The downstream task takes the upstream task's output (or a part of it).
    Data,
    /// An order-only dependency (`>>`), no data passed.
    Order,
    /// From a `@branch` task: taken when the branch chose the downstream task.
    Selection,
    /// From a resource's setup to a user (`uses=`): it can stop the user, never make it run.
    Requirement,
}

/// A dependency: `to` runs after `from`, when `from` ended with one of the outcomes `on`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    /// The upstream task.
    pub from: TaskId,
    /// The downstream task.
    pub to: TaskId,
    /// Why the dependency exists.
    pub kind: EdgeKind,
    /// The upstream outcomes the edge is taken on (`.on(...)`); empty for any outcome. An edge
    /// not taken counts as skipped in the downstream task's trigger rule.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<String>,
}

impl Edge {
    /// Whether the edge is taken by an upstream task that succeeded with `outcome`.
    #[must_use]
    pub fn taken_on(&self, outcome: &str) -> bool {
        self.on.is_empty() || self.on.iter().any(|on| on == outcome)
    }
}

/// A resource: one thing of two parts, each a task of the DAG, both always there (decision 259).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Resource {
    /// The task that creates it; its ID is the resource's.
    pub setup: TaskId,
    /// The task that undoes it, once every user is done.
    pub cleanup: TaskId,
    /// The tasks that need it.
    pub users: Vec<TaskId>,
}

/// A DAG: its identity, trigger, tasks and dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Dag {
    /// The schedr version whose runner produced this JSON.
    pub produced_by: SchedrVersion,
    /// The DAG's ID.
    pub id: DagId,
    /// Where it is defined.
    pub location: DagLocation,
    /// A description shown in the UI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tags shown in the UI and used for filtering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// What starts its runs (besides people and other DAGs).
    pub trigger: Trigger,
    /// `since` of the run on the DAG's first fire time (see PROJECT.md, Runs).
    pub initial_since: Option<Timestamp>,
    /// At most this many runs at a time; `None` for no limit.
    pub max_active_runs: Option<u32>,
    /// At most this many of its tasks running at once, across all its runs (each instance of
    /// a mapped task counts); `None` for no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_active_tasks: Option<u32>,
    /// The tasks, in declaration order.
    pub tasks: Vec<Task>,
    /// The dependencies between them.
    pub edges: Vec<Edge>,
    /// The task groups (decision 64), for the UI and the API; the master schedules tasks only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<Group>,
    /// The run parameters it takes (`Dag(params=...)`); none: it takes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
    /// Its resources.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<Resource>,
}

/// The run parameters a DAG takes (`Dag(params=SomeDataclass)`, decision 138).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Params {
    /// The declared class's type: a dataclass or a Pydantic model. Values are stored by it,
    /// as outputs are (tagged).
    #[serde(rename = "type")]
    pub ty: crate::TypeDesc,
    /// A JSON Schema of the class, as JSON values (a date as `format: date`): what the master
    /// checks a trigger's values against, and what the UI's form is built from.
    #[schemars(with = "serde_json::Map<String, serde_json::Value>")]
    pub schema: serde_json::Value,
    /// The fields' defaults, as JSON: the values of a scheduled run, and of the fields a
    /// trigger leaves out.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub defaults: serde_json::Map<String, serde_json::Value>,
}

/// A group of tasks (`with group("load"):`): a name for some of a DAG's tasks, drawn as a box
/// around them in the UI. It has no state and no rule of its own (decision 56A).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Group {
    /// The group's ID, unique within its DAG (`load`, `load.eu` for a nested one).
    pub id: String,
    /// The group it is nested in, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
}

impl Dag {
    /// The task with the given ID.
    #[must_use]
    pub fn task(&self, id: &TaskId) -> Option<&Task> {
        self.tasks.iter().find(|t| &t.id == id)
    }

    /// The edges into `id`, in order (one upstream task may have several, on different
    /// outcomes).
    pub fn edges_into<'a>(&'a self, id: &'a TaskId) -> impl Iterator<Item = &'a Edge> + 'a {
        self.edges.iter().filter(move |e| &e.to == id)
    }

    /// The tasks `id` directly depends on, in edge order, without repeats.
    #[must_use]
    pub fn upstream_of(&self, id: &TaskId) -> Vec<&TaskId> {
        let mut upstream: Vec<&TaskId> = Vec::new();
        for edge in self.edges.iter().filter(|e| &e.to == id) {
            if !upstream.contains(&&edge.from) {
                upstream.push(&edge.from);
            }
        }
        upstream
    }

    /// The tasks that directly depend on `id`, in edge order, without repeats.
    #[must_use]
    pub fn downstream_of(&self, id: &TaskId) -> Vec<&TaskId> {
        let mut downstream: Vec<&TaskId> = Vec::new();
        for edge in self.edges.iter().filter(|e| &e.from == id) {
            if !downstream.contains(&&edge.to) {
                downstream.push(&edge.to);
            }
        }
        downstream
    }
}
