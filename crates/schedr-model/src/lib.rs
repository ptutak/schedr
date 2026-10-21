//! Shared model of schedr: DAG JSON, run and task states, and the JSON Schema generated from them.
//!
//! These Rust types are the reference for every shape that crosses a process boundary (see
//! PROJECT.md, Rust/Python integration: shared model and schemas). The Python package writes
//! DAG JSON by hand-written code that is validated against the schema generated from here.

pub mod dag;
pub mod ids;
pub mod schedule;
pub mod schema;
pub mod states;
pub mod task;
pub mod types;
pub mod value;
pub mod version;

pub use dag::{Dag, DagLocation, Edge, EdgeKind};
pub use ids::{DagId, IdError, TaskId};
pub use schedule::Schedule;
pub use states::{AttemptStatus, RetryRule, RunKind, RunState, TaskInstanceState};
pub use task::{CommandLine, Task, TaskKind, TaskSettings};
pub use types::{Field, TypeDesc};
pub use value::{DagCtxField, PathElem, Reference, Template, TemplatePart, TemplateRef, Value};
pub use version::{SchedrVersion, VersionError};
