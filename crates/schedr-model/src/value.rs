//! Values in DAG JSON: references, templates and task arguments.
//!
//! Python records everything the DAG body declared as plain data: literal values, references
//! to upstream outputs or the run's context, and t-strings as literal text plus references
//! (see PROJECT.md, Airflow compatibility, Scope: Templating). The agent and the runner fill
//! them in at run time.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::TaskId;

#[cfg(test)]
mod tests;

/// One step of a path into an output: a field or key, or a list / tuple index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PathElem {
    /// A dataclass field or dictionary key.
    Field(String),
    /// A list or tuple index.
    Index(u64),
}

/// A field of the run's context, `dag_ctx.run.*` and `dag_ctx.dag_id`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DagCtxField {
    /// `dag_ctx.run.id`.
    RunId,
    /// `dag_ctx.run.kind`.
    RunKind,
    /// `dag_ctx.run.planned_for`.
    PlannedFor,
    /// `dag_ctx.run.since`.
    Since,
    /// `dag_ctx.dag_id`.
    DagId,
}

/// A reference to data known only at run time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Reference {
    /// An upstream task's output, or a part of it.
    Output {
        /// The upstream task.
        task: TaskId,
        /// The path into the output; empty for the whole output.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        path: Vec<PathElem>,
        /// The outcomes it was narrowed to (`.on(...)`); empty for any. On another outcome,
        /// or a skipped upstream task, it stands for `null`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        on: Vec<String>,
    },
    /// An upstream task's outcome itself (`.outcome`), as its name.
    Outcome {
        /// The upstream task.
        task: TaskId,
    },
    /// A field of the run's context.
    DagCtx {
        /// Which field.
        field: DagCtxField,
    },
}

impl Reference {
    /// The upstream task this reference depends on, if any.
    #[must_use]
    pub fn upstream_task(&self) -> Option<&TaskId> {
        match self {
            Self::Output { task, .. } | Self::Outcome { task } => Some(task),
            Self::DagCtx { .. } => None,
        }
    }
}

/// A reference inside a template, with its optional format spec (e.g. `%Y-%m-%d`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TemplateRef {
    /// What is filled in.
    #[serde(flatten)]
    pub reference: Reference,
    /// The t-string format spec, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// One part of a template: literal text, or a reference filled in at run time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TemplatePart {
    /// Literal text.
    Text(String),
    /// A reference, rendered at run time.
    Ref(TemplateRef),
}

/// A t-string as data: its parts in order.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct Template(pub Vec<TemplatePart>);

impl Template {
    /// A template that is only literal text.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self(vec![TemplatePart::Text(text.into())])
    }

    /// The references in this template, in order.
    pub fn references(&self) -> impl Iterator<Item = &Reference> {
        self.0.iter().filter_map(|part| match part {
            TemplatePart::Text(_) => None,
            TemplatePart::Ref(r) => Some(&r.reference),
        })
    }
}

/// A task argument: a literal, a reference, a template, or a container of values (so
/// references inside lists and dictionaries stay visible).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Value {
    /// A JSON value as encoded by the Python API (with tagged extra types such as
    /// `{"$datetime": "..."}`), passed as is.
    Literal(serde_json::Value),
    /// A reference, passed with its type (a `date` stays a `date`).
    Ref(Reference),
    /// A t-string, rendered to a string.
    Template(Template),
    /// A list whose elements may contain references.
    List(Vec<Value>),
    /// A dictionary (string keys) whose values may contain references.
    Map(BTreeMap<String, Value>),
}

impl Value {
    /// Every reference in this value, at any depth, in order.
    #[must_use]
    pub fn references(&self) -> Vec<&Reference> {
        let mut found = Vec::new();
        self.collect_references(&mut found);
        found
    }

    /// Appends the references in this value to `found`.
    fn collect_references<'a>(&'a self, found: &mut Vec<&'a Reference>) {
        match self {
            Self::Literal(_) => {}
            Self::Ref(r) => found.push(r),
            Self::Template(t) => found.extend(t.references()),
            Self::List(items) => items.iter().for_each(|v| v.collect_references(found)),
            Self::Map(entries) => entries.values().for_each(|v| v.collect_references(found)),
        }
    }
}
