//! The JSON Schemas generated from the model, committed to the repository and checked by CI
//! (see PROJECT.md, Rust/Python integration: shared model and schemas).

use schemars::{Schema, schema_for};

use crate::dag::Dag;

#[cfg(test)]
mod tests;

/// The JSON Schema of DAG JSON (one [`Dag`]).
#[must_use]
pub fn dag_schema() -> Schema {
    schema_for!(Dag)
}
