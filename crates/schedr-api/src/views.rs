//! The JSON shapes of the REST API: what it returns and what it accepts.
//!
//! They are the API's own types, apart from the database's and the model's, so the API
//! changes only on purpose (within `/api/v1`, additively). Times are UTC, RFC 3339; kinds and
//! states are their model names (clients treat them as open: new values may appear).

use jiff::Timestamp;
use schedr_db::commands::CommandRecord;
use schedr_db::dags::StoredDag;
use schedr_db::reads::{AttemptRecord, InstanceRecord, RunRecord};
use schedr_db::workers::WorkerRecord;
use schedr_model::DagLocation;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[cfg(test)]
mod tests;

/// An error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ErrorBody {
    /// What went wrong.
    pub error: String,
}

/// Where a DAG is defined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LocationView {
    /// The DAG root.
    pub root: String,
    /// The file, relative to the root.
    pub path: String,
}

impl From<&DagLocation> for LocationView {
    fn from(location: &DagLocation) -> Self {
        Self {
            root: location.root.clone(),
            path: location.path.clone(),
        }
    }
}

/// A DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DagView {
    /// Its ID.
    pub dag_id: String,
    /// Whether the scheduler is stopped for it.
    pub paused: bool,
    /// Where its current version is defined (none for a DAG only ever seen in conflict).
    pub location: Option<LocationView>,
    /// Every place defining it, while it is in conflict (then it can't run).
    pub conflict: Option<Vec<LocationView>>,
    /// Its trigger as in DAG JSON.
    #[schema(value_type = Option<Object>)]
    pub trigger: Option<serde_json::Value>,
    /// Its description.
    pub description: Option<String>,
    /// Its tags.
    pub tags: Vec<String>,
}

impl From<&StoredDag> for DagView {
    fn from(dag: &StoredDag) -> Self {
        let current = dag.current.as_ref().map(|c| &c.dag);
        Self {
            dag_id: dag.dag_id.to_string(),
            paused: dag.paused,
            location: current.map(|d| (&d.location).into()),
            conflict: dag
                .conflict
                .as_ref()
                .map(|places| places.iter().map(Into::into).collect()),
            trigger: current.and_then(|d| serde_json::to_value(&d.trigger).ok()),
            description: current.and_then(|d| d.description.clone()),
            tags: current.map(|d| d.tags.clone()).unwrap_or_default(),
        }
    }
}

/// The DAGs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DagList {
    /// Every DAG not removed, in ID order.
    pub dags: Vec<DagView>,
}

/// A run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunView {
    /// Its ID (a UUIDv7).
    pub run_id: String,
    /// The DAG.
    pub dag_id: String,
    /// `scheduled`, `manual`, `backfill`, `chained` or `event`.
    pub kind: String,
    /// `queued`, `running`, `success`, `failed` or `cancelled`.
    pub state: String,
    /// The moment it is for.
    pub planned_for: Timestamp,
    /// The start of its window, `(since, planned_for]`.
    pub since: Timestamp,
    /// When it was created.
    pub triggered_at: Timestamp,
    /// `scheduler`, a user name, the parent run, or `trigger`.
    pub triggered_by: String,
    /// When it started running.
    pub started_at: Option<Timestamp>,
    /// When it finished.
    pub ended_at: Option<Timestamp>,
}

impl From<&RunRecord> for RunView {
    fn from(run: &RunRecord) -> Self {
        Self {
            run_id: run.id.clone(),
            dag_id: run.dag_id.to_string(),
            kind: run.kind.to_string(),
            state: run.state.to_string(),
            planned_for: run.planned_for,
            since: run.since,
            triggered_at: run.triggered_at,
            triggered_by: run.triggered_by.clone(),
            started_at: run.started_at,
            ended_at: run.ended_at,
        }
    }
}

/// A DAG's runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunList {
    /// Newest `planned_for` first.
    pub runs: Vec<RunView>,
}

/// A task of a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TaskView {
    /// The task's ID.
    pub task_id: String,
    /// Its state (`scheduled`, `queued`, `running`, `success`, ...).
    pub state: String,
    /// Its outcome, on success.
    pub outcome: Option<String>,
    /// Attempts made.
    pub try_number: u32,
    /// Why it waits or can't be scheduled.
    pub reason: Option<String>,
    /// When it last changed.
    pub updated_at: Timestamp,
}

impl From<&InstanceRecord> for TaskView {
    fn from(instance: &InstanceRecord) -> Self {
        Self {
            task_id: instance.task_id.to_string(),
            state: instance.state.to_string(),
            outcome: instance.outcome.clone(),
            try_number: instance.try_number,
            reason: instance.queued_reason.clone(),
            updated_at: instance.updated_at,
        }
    }
}

/// A run with its tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunDetail {
    /// The run.
    pub run: RunView,
    /// Its tasks, in task ID order.
    pub tasks: Vec<TaskView>,
    /// The code hashes its attempts ran with, in the order first used. More than one: its
    /// tasks ran on different code (it changed during the run).
    pub code_hashes: Vec<String>,
}

/// An attempt of a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttemptView {
    /// Its ID.
    pub attempt_id: String,
    /// Its try number.
    pub try_number: u32,
    /// The worker it ran on.
    pub worker_id: Option<String>,
    /// How it ended (`success`, `failed`, `timed_out`, ...); none while it runs.
    pub status: Option<String>,
    /// The process's exit code.
    pub exit_code: Option<i32>,
    /// The error kind schedr recognised.
    pub error_kind: Option<String>,
    /// The structured error summary (exception, message, traceback).
    #[schema(value_type = Option<Object>)]
    pub error_summary: Option<serde_json::Value>,
    /// When it was assigned.
    pub assigned_at: Timestamp,
    /// When the worker started it.
    pub started_at: Option<Timestamp>,
    /// When it ended.
    pub ended_at: Option<Timestamp>,
    /// How long it ran, measured by the worker (the timestamps come from the master's wall
    /// clock, so don't subtract them); none while it runs or for attempts ended by the master.
    pub duration_ms: Option<u64>,
}

impl From<&AttemptRecord> for AttemptView {
    fn from(attempt: &AttemptRecord) -> Self {
        Self {
            attempt_id: attempt.id.clone(),
            try_number: attempt.try_number,
            worker_id: attempt.worker_id.clone(),
            status: attempt.status.map(|s| s.to_string()),
            exit_code: attempt.exit_code,
            error_kind: attempt.error_kind.clone(),
            error_summary: attempt
                .error_summary
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok()),
            assigned_at: attempt.assigned_at,
            started_at: attempt.started_at,
            ended_at: attempt.ended_at,
            duration_ms: attempt.duration_ms,
        }
    }
}

/// A task's attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AttemptList {
    /// Oldest first.
    pub attempts: Vec<AttemptView>,
}

/// A command: an action the master applies, then marks `applied` or `rejected`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CommandView {
    /// Its ID; poll `GET /api/v1/commands/{command_id}` for the decision.
    pub command_id: String,
    /// `trigger_run`, `cancel_run`, `pause_dag` or `unpause_dag`.
    pub kind: String,
    /// `pending`, `applied` or `rejected`.
    pub status: String,
    /// Why it was rejected.
    pub reason: Option<String>,
    /// What it produced when applied, e.g. `{"run_id": ...}` for a trigger.
    #[schema(value_type = Option<Object>)]
    pub result: Option<serde_json::Value>,
    /// Who asked for it.
    pub requested_by: String,
    /// When.
    pub requested_at: Timestamp,
    /// When the master decided it.
    pub decided_at: Option<Timestamp>,
}

impl From<&CommandRecord> for CommandView {
    fn from(command: &CommandRecord) -> Self {
        Self {
            command_id: command.id.clone(),
            kind: command.kind.clone(),
            status: serde_json::to_value(command.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default(),
            reason: command.reason.clone(),
            result: command.result.clone(),
            requested_by: command.requested_by.clone(),
            requested_at: command.requested_at,
            decided_at: command.decided_at,
        }
    }
}

/// A manual run to start.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TriggerRequest {
    /// The moment the run is for (`--for`); now if not given. Must not be in the future.
    #[serde(default)]
    pub planned_for: Option<Timestamp>,
}

/// Pausing a DAG.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PauseRequest {
    /// Also cancel its queued and running runs.
    #[serde(default)]
    pub cancel_runs: bool,
}

/// Which runs to list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct RunsQuery {
    /// Only the runs for this moment (`--for`).
    #[serde(default, rename = "for")]
    #[param(rename = "for", value_type = Option<String>, format = DateTime)]
    pub planned_for: Option<Timestamp>,
    /// At most this many (default 50, at most 500).
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Which part of a log to read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct LogQuery {
    /// The first byte wanted (0 by default): the `next` of the previous part, to follow a log.
    #[serde(default)]
    pub from: Option<u64>,
    /// At most this many bytes (1 MiB by default; between 4 bytes, one character, and that).
    #[serde(default)]
    pub limit: Option<u64>,
}

/// A part of an attempt's log, read from the worker that ran it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LogChunk {
    /// The attempt.
    pub attempt_id: String,
    /// Its try number.
    pub try_number: u32,
    /// The worker it ran on (and that answered).
    pub worker_id: String,
    /// The first byte of `content`.
    pub from: u64,
    /// The byte after `content`: where the next part starts.
    pub next: u64,
    /// The log's size when it was read.
    pub size: u64,
    /// Whether the attempt still runs (its log may grow).
    pub running: bool,
    /// The log's text (invalid UTF-8 replaced by `U+FFFD`).
    pub content: String,
}

/// A worker in the registry (kept after it is gone, so its logs can be found).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerView {
    /// Its ID.
    pub worker_id: String,
    /// `active`, `draining`, `disconnected`, `offline` or `lost`.
    pub state: String,
    /// The incarnation of its agent process it last registered with.
    pub incarnation_id: String,
    /// The queues it serves.
    pub queues: Vec<String>,
    /// Its task slots.
    pub slots: u32,
    /// Its attempts still running.
    pub running: u32,
    /// Its schedr version.
    pub schedr_version: String,
    /// Its protocol version (`major.minor`).
    pub protocol_version: String,
    /// Where its log endpoint listens (where a gone worker's logs were served).
    pub log_endpoint: String,
    /// When it last registered.
    pub registered_at: Timestamp,
    /// The last sign of life the master recorded (at most once per heartbeat interval).
    pub last_heartbeat_at: Timestamp,
    /// When it went offline or was lost.
    pub offline_at: Option<Timestamp>,
    /// Drained for maintenance (`schedr workers drain`): no new tasks until resumed.
    pub drained: bool,
}

impl From<&WorkerRecord> for WorkerView {
    fn from(worker: &WorkerRecord) -> Self {
        Self {
            worker_id: worker.id.clone(),
            state: worker.state.to_string(),
            incarnation_id: worker.incarnation.clone(),
            queues: worker.queues.clone(),
            slots: worker.slots,
            running: worker.running,
            schedr_version: worker.schedr_version.clone(),
            protocol_version: worker.protocol_version.clone(),
            log_endpoint: worker.log_endpoint.clone(),
            registered_at: worker.registered_at,
            last_heartbeat_at: worker.last_heartbeat_at,
            offline_at: worker.offline_at,
            drained: worker.drained,
        }
    }
}

/// The workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerList {
    /// Every worker the master has seen, in ID order.
    pub workers: Vec<WorkerView>,
}
