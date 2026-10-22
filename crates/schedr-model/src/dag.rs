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
}

/// A dependency: `to` runs after `from`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    /// The upstream task.
    pub from: TaskId,
    /// The downstream task.
    pub to: TaskId,
    /// Why the dependency exists.
    pub kind: EdgeKind,
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
    /// The tasks, in declaration order.
    pub tasks: Vec<Task>,
    /// The dependencies between them.
    pub edges: Vec<Edge>,
}

impl Dag {
    /// The task with the given ID.
    #[must_use]
    pub fn task(&self, id: &TaskId) -> Option<&Task> {
        self.tasks.iter().find(|t| &t.id == id)
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
