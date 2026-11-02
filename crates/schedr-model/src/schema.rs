//! The JSON Schemas generated from the model, committed to the repository and checked by CI
//! (see PROJECT.md, Rust/Python integration: shared model and schemas).

use schemars::{Schema, schema_for};

use crate::ctx::{CtxAnswer, CtxRequest};
use crate::dag::Dag;
use crate::runner::{AgentMessage, RunnerMessage};

#[cfg(test)]
mod tests;

/// The JSON Schema of DAG JSON (one [`Dag`]).
#[must_use]
pub fn dag_schema() -> Schema {
    schema_for!(Dag)
}

/// The JSON Schema of the agent <-> runner messages: one message in either direction.
#[must_use]
pub fn runner_schema() -> Schema {
    schema_for!(RunnerChannelMessage)
}

/// The JSON Schema of a command task's context channel: one message in either direction.
#[must_use]
pub fn ctx_schema() -> Schema {
    schema_for!(CtxChannelMessage)
}

/// Any message on a context connection (only used to generate one schema for both).
#[derive(schemars::JsonSchema)]
#[serde(untagged)]
#[expect(dead_code, reason = "only its schema is used")]
enum CtxChannelMessage {
    /// From the task's process.
    Request(CtxRequest),
    /// From the agent.
    Answer(CtxAnswer),
}

/// Any message on the agent <-> runner channel (only used to generate one schema for both).
#[derive(schemars::JsonSchema)]
#[serde(untagged)]
#[expect(dead_code, reason = "only its schema is used")]
enum RunnerChannelMessage {
    /// From the runner.
    Runner(RunnerMessage),
    /// From the agent.
    Agent(AgentMessage),
}
