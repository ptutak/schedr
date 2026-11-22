//! The messages a plugin speaks (decision note 17): only those, each as the worker writes it, so
//! the crate stands on its own. A message's fields this crate doesn't name are kept as raw JSON
//! where a plugin might want them (`rest`), and ignored otherwise, so a newer worker's additions
//! don't break an older plugin.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The runner protocol version a plugin speaks: in its `hello` and its `--describe`.
pub const PROTOCOL: u32 = 1;

/// The most a message's JSON may take, either way.
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// A message from the plugin to the worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginMessage {
    /// The first message: in `extension` mode, naming the protocol.
    Hello {
        /// The plugin's own version, not checked.
        schedr_version: String,
        /// Always `extension`.
        mode: String,
        /// [`PROTOCOL`].
        protocol: u32,
        /// Its metrics' declarations, raw JSON (name, kind, help, unit, labels, buckets): the
        /// worker keeps them per extension and version, its config's winning.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        metrics: Vec<Value>,
        /// Its page's panels, raw JSON (title, metric, chart, row, cell; `point`, `combine`,
        /// `group_by`, `limit`: decision 516).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        panels: Vec<Value>,
    },
    /// A secret the task declared; answered [`WorkerMessage::Secret`] or
    /// [`WorkerMessage::SecretFailed`].
    SecretGet {
        /// Which secret.
        secret: SecretName,
    },
    /// A connection the task declared; answered [`WorkerMessage::Connection`] or
    /// [`WorkerMessage::ConnectionFailed`].
    ConnectionGet {
        /// The connection's name.
        name: String,
    },
    /// A cleanup handle; answered [`WorkerMessage::HandleAdded`] or
    /// [`WorkerMessage::HandleRefused`].
    HandleAdd {
        /// The handle.
        handle: Handle,
    },
    /// A call to schedr's REST API, made by the master as the task; answered
    /// [`WorkerMessage::ApiAnswer`].
    ApiCall {
        /// `GET`, `POST`, ...
        method: String,
        /// The path and its query.
        path: String,
        /// The JSON body, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<Value>,
        /// Its `Idempotency-Key`, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// The task (or cleanup) succeeded with this output.
    TaskSucceeded {
        /// The output, JSON.
        output: Value,
        /// `SUCCESS`.
        outcome: String,
    },
    /// Where the task is now (decision 513): kept with the attempt, with its time. No answer.
    Status {
        /// A short name for the phase, e.g. `pulling image`.
        phase: String,
        /// More about it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// How far along, 0 to 1, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        progress: Option<f64>,
    },
    /// A link to what the task started elsewhere; a second with the same label replaces it.
    /// Only `http` and `https` URLs are kept. No answer.
    Link {
        /// What it links to.
        label: String,
        /// Its URL.
        url: String,
    },
    /// Why the task is being stopped, asked after SIGTERM; answered
    /// [`WorkerMessage::StopReason`]. The worker never sends an unasked message.
    StopReasonGet,
    /// Points of the extension's declared metrics. No answer: one matching no declaration is
    /// dropped by the worker with a warning.
    Metrics {
        /// The points.
        points: Vec<MetricPoint>,
    },
    /// The task (or cleanup) failed.
    TaskFailed {
        /// What went wrong.
        error: ErrorSummary,
        /// A failure schedr recognises (`output_too_large`); `None` for the plugin's own.
        kind: Option<String>,
    },
}

/// A message from the worker to the plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    /// The handshake succeeded.
    Welcome,
    /// The handshake failed; the plugin exits.
    Refused {
        /// Why.
        reason: String,
    },
    /// The attempt: the task, or its cleanup (`cleanup` set).
    ExtensionTask(Box<ExtensionTask>),
    /// A secret's value.
    Secret {
        /// A key's value, or a whole secret's object.
        value: Value,
    },
    /// Why a secret can't be read.
    SecretFailed {
        /// `secret_unavailable`, `secret_denied`, `secret_not_found`.
        kind: String,
        /// What went wrong.
        message: String,
    },
    /// A connection's values.
    Connection {
        /// By parameter.
        values: BTreeMap<String, Value>,
    },
    /// Why a connection can't be opened.
    ConnectionFailed {
        /// Its kind.
        kind: String,
        /// What went wrong.
        message: String,
    },
    /// The answer to [`PluginMessage::StopReasonGet`]: why the task is being stopped, `None`
    /// while it isn't.
    StopReason {
        /// Why: `cancel`, `timeout`, `drain`, `master_lost`.
        reason: Option<StopKind>,
        /// How long until SIGKILL, in milliseconds (0 while it isn't stopping).
        grace_ms: u64,
    },
    /// The handle is registered.
    HandleAdded,
    /// Why the handle isn't registered.
    HandleRefused {
        /// What went wrong.
        message: String,
    },
    /// What the API answered.
    ApiAnswer {
        /// The HTTP status.
        status: u16,
        /// The JSON body.
        body: Value,
        /// What the task is told of its request, printed to its log.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
}

/// An `extension_task` request: what a plugin needs of it, the rest raw.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExtensionTask {
    /// The DAG's ID.
    pub dag_id: String,
    /// The task.
    pub task: Task,
    /// The run's information (`run_id`, `try_number`, `planned_for`, ...).
    pub run: Value,
    /// The secrets the worker fetched for it.
    #[serde(default)]
    pub secrets: Vec<SecretValue>,
    /// Set for a cleanup: the handles the attempt registered, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<Vec<Handle>>,
    /// The request's other fields, as sent.
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// The task of an [`ExtensionTask`]: its kind `extension`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Task {
    /// The task's ID.
    pub id: String,
    /// `extension`.
    pub kind: String,
    /// The extension's name.
    pub name: String,
    /// The version it is written for.
    pub version: String,
    /// What it asks the plugin to do.
    #[serde(default)]
    pub payload: Value,
    /// Its settings: `max_output_size` read here, the rest raw.
    pub settings: Settings,
    /// The task's other fields, as sent.
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// A task's settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Settings {
    /// The most its output may take, in bytes.
    pub max_output_size: u64,
    /// The other settings, as sent.
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// A secret's name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SecretName {
    /// The storage; `None` for the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    /// The path.
    pub path: String,
    /// The key; `None` for the whole secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// A secret the worker fetched for the attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SecretValue {
    /// Which.
    pub name: SecretName,
    /// Its value.
    pub value: Value,
}

/// One point of a declared metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct MetricPoint {
    /// The declared metric.
    pub name: String,
    /// Its labels' values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// A counter's increase, a gauge's value, a histogram's observation.
    pub value: f64,
}

/// Why the worker stops a plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StopKind {
    /// A user cancelled the task.
    Cancel,
    /// The task's timeout passed.
    Timeout,
    /// The worker drains: the task is retried elsewhere.
    Drain,
    /// The worker lost the master: the task may run, or be cleaned up, elsewhere.
    MasterLost,
}

/// A cleanup handle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Handle {
    /// Its type's name.
    pub handle_type: String,
    /// Its value, JSON.
    pub value: Value,
}

/// Why a task failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ErrorSummary {
    /// The error's type, e.g. `ExtensionError`.
    pub exception: String,
    /// What went wrong.
    pub message: String,
    /// Where, if known.
    pub traceback: String,
}

/// Any message on a plugin's channel, either way: what `schema/extension.schema.json` describes
/// (M5.2i4). Only for the schema.
#[cfg(feature = "schema")]
#[derive(schemars::JsonSchema)]
#[serde(untagged)]
#[expect(dead_code, reason = "only its schema is used")]
enum ChannelMessage {
    /// From the plugin.
    Plugin(PluginMessage),
    /// From the worker.
    Worker(WorkerMessage),
}

/// The JSON Schema of the plugin protocol's messages, either way (M5.2i4).
#[cfg(feature = "schema")]
#[must_use]
pub fn schema() -> schemars::Schema {
    schemars::schema_for!(ChannelMessage)
}
