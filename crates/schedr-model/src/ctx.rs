//! Messages on a command task's context channel: `schedr dag-ctx` and `schedr.script` asking
//! the agent for the run's parameters and information (decisions 143-148).
//!
//! A command task inherits one end of a datagram socket pair (`SCHEDR_CTX_FD`). Each call
//! makes a fresh stream socket pair, sends one end to the agent over it (`SCM_RIGHTS`, one
//! datagram) and talks on the other: a connection of its own, which no other process can
//! join. On that connection the framing is the runner channel's (a 4-byte big-endian length,
//! then UTF-8 JSON tagged by `type`), capped at [`MAX_CTX_MESSAGE_SIZE`]. The caller says
//! [`CtxRequest::Hello`] first, with its protocol ([`CTX_PROTOCOL`]); the agent answers
//! [`CtxAnswer::Welcome`] or [`CtxAnswer::Refused`], then one answer per request until the
//! caller closes it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::states::RunKind;

#[cfg(test)]
mod tests;

/// The environment variable with a command task's descriptor of its context channel.
pub const CTX_FD_ENV: &str = "SCHEDR_CTX_FD";

/// The context channel's protocol (decision 148): the agent accepts a caller speaking the same
/// one, whatever its schedr version. Within a protocol, changes are only additions (a new
/// request, a new field), and either side ignores fields it doesn't know; anything else is a
/// new protocol, a new value of an enumerated field included (a run kind: an older caller
/// can't read it). `hello`'s shape never changes, so a refusal can always be told.
pub const CTX_PROTOCOL: u32 = 1;

/// The largest message on a context connection, in bytes (params are at most 64 KiB as JSON,
/// decision 135; tags and escaping can grow them).
pub const MAX_CTX_MESSAGE_SIZE: u32 = 1024 * 1024;

/// A request from a process of the task to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CtxRequest {
    /// The first message: the protocol the caller speaks ([`CTX_PROTOCOL`]), which must be
    /// the agent's, and its schedr version (for the messages).
    Hello {
        /// The protocol.
        protocol: u32,
        /// The `schedr` binary's or Python package's version.
        schedr_version: String,
    },
    /// The run's parameters.
    ParamsGet,
    /// The run information.
    RunGet,
    /// A secret the task declares (`secrets=`), or one under a declared prefix.
    SecretGet {
        /// Which secret.
        secret: crate::secret::SecretName,
    },
    /// A request to schedr's REST API (`schedr task-ctx api`, M4.11h), run by the master as
    /// the task.
    ApiCall {
        /// `GET`, `POST`, ...
        method: String,
        /// The path and its query, e.g. `/api/v1/events?name=paid`.
        path: String,
        /// The body, JSON; `None` for none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<serde_json::Value>,
        /// Its `Idempotency-Key`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Registers a cleanup handle (`schedr task-ctx cleanup add`).
    HandleAdd {
        /// The handle.
        handle: crate::runner::CleanupHandle,
    },
    /// The attempt's handles, in registration order (`schedr task-ctx cleanup list`).
    HandleList,
    /// A connection's values, rendered (decision 174): any connection of the task's root.
    ConnectionGet {
        /// The connection's name.
        name: String,
    },
    /// A connection as a command gets it (decisions 184, 190): its files written in the
    /// attempt's folder, and its variables (`task-ctx connection NAME --env PREFIX`).
    ConnectionEnvGet {
        /// The connection's name.
        name: String,
        /// The variables' prefix (`DB` gives `DB_URL`).
        prefix: String,
    },
    /// Reports the value of the resource a `CommandResource` setup creates (`schedr task-ctx
    /// resource set`), as JSON; checked against its declared type at once.
    ResourceSet {
        /// The value.
        value: serde_json::Value,
    },
    /// The value of the resource a command cleanup cleans up (`schedr task-ctx resource get`).
    ResourceGet,
}

/// The agent's answer to a [`CtxRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CtxAnswer {
    /// The handshake succeeded.
    Welcome,
    /// The handshake failed (another protocol); the agent closes the connection.
    Refused {
        /// Why.
        reason: String,
    },
    /// The run's parameters, as stored (tagged, decision 138); `{}` when the DAG takes none.
    Params {
        /// The values.
        #[schemars(with = "serde_json::Map<String, serde_json::Value>")]
        params: serde_json::Value,
    },
    /// The run information.
    Run(CtxRun),
    /// The value of the secret asked for: a key's value as a string, a whole secret as an
    /// object of key to value.
    Secret {
        /// The value.
        value: serde_json::Value,
    },
    /// Why the secret asked for can't be read; the connection stays open.
    SecretFailed {
        /// The error kind (`secret_unavailable`, `secret_denied`, `secret_not_found`).
        kind: String,
        /// What went wrong, naming the secret (never a value).
        reason: String,
    },
    /// What the API answered an [`CtxRequest::ApiCall`].
    ApiAnswer {
        /// The HTTP status.
        status: u16,
        /// The body, JSON (`{"error": ...}` for a refusal).
        body: serde_json::Value,
        /// What the task is told of its request: a secret in it masked. Printed to its log.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    /// The handle is registered.
    HandleAdded,
    /// Why the handle isn't registered; the connection stays open.
    HandleRefused {
        /// What went wrong.
        reason: String,
    },
    /// The attempt's handles, in registration order.
    Handles {
        /// The handles.
        handles: Vec<crate::runner::CleanupHandle>,
    },
    /// The resource's value is reported.
    ResourceReported,
    /// The resource's value, as stored JSON (tagged).
    Resource {
        /// The value.
        value: serde_json::Value,
    },
    /// Why the resource's value can't be reported or read; the connection stays open.
    ResourceRefused {
        /// What went wrong (never the value).
        reason: String,
    },
    /// The values of the connection asked for, by parameter, as stored JSON (tagged).
    Connection {
        /// The values.
        values: std::collections::BTreeMap<String, serde_json::Value>,
    },
    /// The variables of the connection asked for, by name (`DB_URL`), its files written.
    ConnectionEnv {
        /// The variables.
        variables: std::collections::BTreeMap<String, String>,
    },
    /// Why the connection asked for can't be opened; the connection stays open.
    ConnectionFailed {
        /// The error kind (`connection_not_found`, `render_failed`, or a secret's).
        kind: String,
        /// What went wrong, naming the connection (never a value).
        reason: String,
    },
    /// A request the agent can't answer (one it doesn't know, out of order); the connection
    /// is closed after it.
    Error {
        /// Why.
        reason: String,
    },
}

/// The run information a task's processes can read (`schedr dag-ctx run get`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CtxRun {
    /// The DAG's ID.
    pub dag_id: String,
    /// The run's ID (a `UUIDv7`).
    pub id: String,
    /// The run's kind.
    pub kind: RunKind,
    /// The moment the run is for.
    pub planned_for: jiff::Timestamp,
    /// The start of the run's window.
    pub since: jiff::Timestamp,
}
