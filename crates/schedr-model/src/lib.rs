//! Shared model of schedr: DAG JSON, run and task states, and the JSON Schema generated from them.
//!
//! These Rust types are the reference for every shape that crosses a process boundary (see
//! PROJECT.md, Rust/Python integration: shared model and schemas). The Python package writes
//! DAG JSON by hand-written code that is validated against the schema generated from here.

pub mod beat;
pub mod commands;
pub mod dag;
pub mod ids;
pub mod logs;
pub mod metric_doc;
pub mod runner;
pub mod schema;
pub mod states;
pub mod task;
pub mod trigger;
pub mod types;
pub mod value;
pub mod version;

pub use dag::{Dag, DagLocation, Edge, EdgeKind, Group};
pub use ids::{DagId, IdError, TaskId};
pub use states::{AttemptStatus, RetryRule, RunKind, RunState, TaskInstanceState, WorkerState};
pub use task::{
    CommandLine, DEFAULT_OUTCOME, Expand, OutcomeDecl, RunIf, Task, TaskKind, TaskSettings,
    default_outcomes,
};
pub use trigger::Trigger;
pub use types::{Field, TypeDesc};
pub use value::{DagCtxField, PathElem, Reference, Template, TemplatePart, TemplateRef, Value};
pub use version::{SchedrVersion, VersionError};
