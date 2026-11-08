//! The JSON shapes of the REST API: what it returns and what it accepts.
//!
//! They are the API's own types, apart from the database's and the model's, so the API
//! changes only on purpose (within `/api/v1`, additively). Times are UTC, RFC 3339; kinds and
//! states are their model names (clients treat them as open: new values may appear).

use std::collections::HashMap;

use jiff::Timestamp;
use schedr_config::masked::mask_text;
use schedr_db::commands::CommandRecord;
use schedr_db::dags::StoredDag;
use schedr_db::reads::{AttemptRecord, InstanceRecord, RunRecord};
use schedr_db::runs::Held;
use schedr_db::workers::WorkerRecord;
use schedr_model::{DagId, DagLocation, RunState};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

mod form;
#[cfg(test)]
mod tests;

pub use form::{ParamFieldView, ParamKind};

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
    /// Why it is held, while its file fails to import in its root's current code (decision
    /// 72): the scheduler creates no runs of it and manual triggers are refused until it
    /// imports again. Not set for a timeout or a runner that died (the file is parsed again).
    pub held: Option<HeldView>,
    /// The concurrency limits its current version sets, each with how many run now (decision
    /// 96): only on one DAG's view (`GET /api/v1/dags/{dag_id}`), not in the list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub limits: Vec<LimitView>,
    /// The run parameters its current version takes, as the UI's trigger form shows them
    /// (decision 149): only on one DAG's view, not in the list; none when it takes none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParamFieldView>,
    /// What is wrong with its current version that shows only when a run reaches it: a
    /// connection a task declares that its root's current files don't define, or define twice
    /// (decision 179). Only on one DAG's view, not in the list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// A concurrency limit and how many of what it limits run now: open attempts (assigned, not
/// yet started, running, cleaning up), as the master counts them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LimitView {
    /// `max_active_tasks`, `max_active` or `max_active_per_run`.
    pub limit: String,
    /// The task it is set on; none for `max_active_tasks` (the DAG's).
    pub task_id: Option<String>,
    /// Its value.
    pub value: u32,
    /// How many run now: the DAG's tasks, the task's instances across runs, or, for
    /// `max_active_per_run`, the most in any one run.
    pub running: u32,
}

impl LimitView {
    /// The limits `dag` sets, counted from `counts` (`(run, task, open attempts)` of its runs).
    #[must_use]
    pub fn of(
        dag: &schedr_model::Dag,
        counts: &[(String, schedr_model::TaskId, u32)],
    ) -> Vec<Self> {
        let mut limits = Vec::new();
        if let Some(value) = dag.max_active_tasks {
            limits.push(Self {
                limit: "max_active_tasks".to_owned(),
                task_id: None,
                value,
                running: counts.iter().map(|c| c.2).sum(),
            });
        }
        for task in &dag.tasks {
            let of_task = || counts.iter().filter(|c| c.1 == task.id).map(|c| c.2);
            if let Some(value) = task.settings.max_active {
                limits.push(Self {
                    limit: "max_active".to_owned(),
                    task_id: Some(task.id.to_string()),
                    value,
                    running: of_task().sum(),
                });
            }
            if let Some(value) = task.settings.max_active_per_run {
                limits.push(Self {
                    limit: "max_active_per_run".to_owned(),
                    task_id: Some(task.id.to_string()),
                    value,
                    running: of_task().max().unwrap_or(0),
                });
            }
        }
        limits
    }
}

/// Why a DAG is held: its file's import error. The error is the **file's**, so every DAG of
/// the file is held (decision 72.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HeldView {
    /// What happened and what to do, for people: that the file failed (not only this DAG),
    /// why, and how to fix it.
    pub reason: String,
    /// The file, relative to its root.
    pub path: String,
    /// The error's exception class name, e.g. `ModuleNotFoundError`.
    pub exception: String,
    /// The error's message.
    pub message: String,
}

impl DagView {
    /// The view of `dag`, held if its file is among `held` ([`Held::by_file`] of the current
    /// import errors).
    #[must_use]
    pub fn of(dag: &StoredDag, held: &HashMap<(String, String), Held>) -> Self {
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
            held: current.and_then(|d| Held::of(d, held)).map(|h| HeldView {
                reason: h.reason(dag.dag_id.as_str()),
                path: h.path,
                exception: h.exception,
                message: mask_text(&h.message),
            }),
            limits: Vec::new(),
            params: Vec::new(),
            warnings: Vec::new(),
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
    /// A `success` that learnt of a failure after its end, shown yellow (decision 309): why,
    /// `late_cleanup_failed` for now.
    pub late_failure: Option<String>,
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
            late_failure: run.late_failure.clone(),
        }
    }
}

/// Runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunList {
    /// Newest first: a DAG's by `planned_for`; the list over every DAG by `triggered_at`, then
    /// run ID.
    pub runs: Vec<RunView>,
}

/// A task of a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TaskView {
    /// The task's ID.
    pub task_id: String,
    /// Which instance of a mapped task, from 0; -1 for an unmapped task, and for a mapped
    /// task as a whole (decision 80).
    #[serde(default = "unmapped")]
    pub map_index: i32,
    /// A mapped task as a whole: how many instances it has (0 before it expands, or for an
    /// empty list); `null` for an unmapped task and for an instance.
    #[serde(default)]
    pub instances: Option<usize>,
    /// Its state (`scheduled`, `queued`, `running`, `success`, ...).
    pub state: String,
    /// Its outcome, on success.
    pub outcome: Option<String>,
    /// Attempts made.
    pub try_number: u32,
    /// Why it waits or can't be scheduled, or why a mapped task failed without an attempt
    /// (too many instances, a mapped value that isn't a list).
    pub reason: Option<String>,
    /// How its cleanups ended (`ok`, `failed`, `worker_lost`, `not_needed`): the last, unless
    /// one failed; none for a task without a cleanup, or before its first one ends.
    #[serde(default)]
    pub cleanup: Option<String>,
    /// When it last changed.
    pub updated_at: Timestamp,
}

/// The `map_index` of a task that isn't mapped (read from an answer that leaves it out).
const fn unmapped() -> i32 {
    -1
}

impl TaskView {
    /// The view of `instance`; `instances` for a mapped task as a whole.
    #[must_use]
    pub fn of(instance: &InstanceRecord, instances: Option<usize>) -> Self {
        Self {
            task_id: instance.task_id.to_string(),
            map_index: instance.map_index,
            instances,
            state: instance.state.to_string(),
            outcome: instance.outcome.clone(),
            try_number: instance.try_number,
            reason: instance
                .reason
                .clone()
                .or_else(|| instance.queued_reason.clone()),
            cleanup: instance.cleanup_result.map(|r| r.to_string()),
            updated_at: instance.updated_at,
        }
    }
}

/// Which instance of a mapped task (decisions 81, 82).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct InstanceQuery {
    /// The instance, from 0 (a mapped task only). For an output, none gives the mapped task's
    /// whole list; for a cancel, the whole task; attempts, logs and a retry need it.
    #[serde(default)]
    pub map_index: Option<i32>,
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
    /// Its parameters as JSON, masked like a task's output (decision 134); `{}` for a DAG
    /// that takes none.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub params: serde_json::Value,
    /// Its resources, in the DAG's order (decision 275: each one's state from its two tasks).
    #[serde(default)]
    pub resources: Vec<ResourceView>,
}

/// A run's resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResourceList {
    /// In the DAG's order.
    pub resources: Vec<ResourceView>,
}

/// A wait step switched off for getting its host killed too many times in a row (decision 338).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SensorView {
    /// Its DAG.
    pub dag_id: String,
    /// The wait step.
    pub sensor: String,
    /// Its host kills in a row.
    pub strikes: u32,
    /// When it was switched off (RFC 3339).
    pub switched_off_at: String,
}

/// The wait steps switched off.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SensorList {
    /// By DAG and task.
    pub sensors: Vec<SensorView>,
}

/// A resource of a run: its setup, its cleanup and its users.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResourceView {
    /// Its name: its setup's task ID.
    pub setup: String,
    /// Its cleanup's task ID.
    pub cleanup: String,
    /// The tasks that use it, in the DAG's order.
    pub users: Vec<String>,
    /// Whether what follows its group waits for its cleanup.
    pub blocking: bool,
    /// `setting_up`, `waiting_for_users`, `cleaning_up`, `cleaned`, `cleanup_failed`,
    /// `cleanup_cancelled` (cancelled by hand: what the setup made may be left) or `not_needed`.
    pub state: String,
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
    /// The peak memory of its processes, sampled by the worker (about once a second); none
    /// when it wasn't measured.
    pub peak_memory_bytes: Option<u64>,
    /// How the worker started it: `spawn` (a new process: an interpreter for a Python task,
    /// the command for a command task) or `fork` (from the fork server); none until the
    /// worker reported its start.
    pub start_method: Option<String>,
    /// What its process used, with the children it reaped (its parent's view at its exit);
    /// none while it runs, when the worker couldn't read it, or for attempts ended by the
    /// master.
    pub usage: Option<UsageView>,
    /// Whether its process dumped core (ended by a signal).
    pub core_dumped: bool,
    /// Its cleanup; none for a task without one.
    #[serde(default)]
    pub cleanup: Option<CleanupView>,
}

/// An attempt's cleanup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CleanupView {
    /// How it ended (`ok`, `failed`, `worker_lost`, `not_needed`); none until it has.
    pub result: Option<String>,
    /// The tries it made.
    pub tries: Option<u32>,
    /// How long its tries ran, measured by the worker.
    pub duration_ms: Option<u64>,
    /// The error kind of its last failed try.
    pub error_kind: Option<String>,
    /// The structured error summary of its last failed try.
    #[schema(value_type = Option<Object>)]
    pub error_summary: Option<serde_json::Value>,
    /// How many times it moved to another worker (`cleanup_anywhere`).
    pub moves: u32,
    /// The handles the attempt registered (`{"handle_type": ..., "value": ...}`), masked, in
    /// order.
    #[schema(value_type = Vec<Object>)]
    pub handles: Vec<serde_json::Value>,
}

/// What an attempt's process used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageView {
    /// CPU time in user mode, in microseconds.
    pub user_cpu_micros: u64,
    /// CPU time in the kernel, in microseconds.
    pub system_cpu_micros: u64,
    /// The peak resident set size, in bytes; for a forked process it includes the pages it
    /// shares with the fork server (`peak_memory_bytes`, the watchdog's PSS, doesn't); none
    /// for a spawned process (its memory is gone when the worker reads it).
    pub peak_rss_bytes: Option<u64>,
    /// Page faults that needed I/O.
    pub major_faults: u64,
    /// Times it gave up the CPU (waiting for I/O or a lock), with its threads and reaped
    /// children; none for a spawned process (only its leader thread's could be read).
    pub voluntary_context_switches: Option<u64>,
    /// Times it was preempted; none as for `voluntary_context_switches`.
    pub involuntary_context_switches: Option<u64>,
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
                .and_then(|s| serde_json::from_str(s).ok())
                .map(|mut summary| {
                    schedr_config::masked::mask(&mut summary);
                    summary
                }),
            assigned_at: attempt.assigned_at,
            started_at: attempt.started_at,
            ended_at: attempt.ended_at,
            duration_ms: attempt.duration_ms,
            peak_memory_bytes: attempt.peak_memory_bytes,
            start_method: attempt.start_method.clone(),
            usage: attempt.usage.map(|u| UsageView {
                user_cpu_micros: u.user_cpu_micros,
                system_cpu_micros: u.system_cpu_micros,
                peak_rss_bytes: u.peak_rss_bytes,
                major_faults: u.major_faults,
                voluntary_context_switches: u.voluntary_context_switches,
                involuntary_context_switches: u.involuntary_context_switches,
            }),
            core_dumped: attempt.core_dumped,
            cleanup: (!attempt.cleanup.is_empty()).then(|| CleanupView {
                result: attempt.cleanup.result.map(|r| r.to_string()),
                tries: attempt.cleanup.tries,
                duration_ms: attempt.cleanup.duration_ms,
                error_kind: attempt.cleanup.error_kind.clone(),
                error_summary: attempt
                    .cleanup
                    .error_summary
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .map(|mut summary| {
                        schedr_config::masked::mask(&mut summary);
                        summary
                    }),
                moves: attempt.cleanup.moves,
                handles: attempt
                    .cleanup
                    .handles
                    .iter()
                    .map(|h| shown_handle(h))
                    .collect(),
            }),
        }
    }
}

/// A stored handle as shown: masked as a task's data, by its field names and texts (the agent
/// masked only the secrets the attempt read); one that doesn't parse is shown as `{"unreadable": <text>}`,
/// masked, rather than left out.
fn shown_handle(stored: &str) -> serde_json::Value {
    let mut value = serde_json::from_str(stored)
        .unwrap_or_else(|_| serde_json::json!({ "unreadable": mask_text(stored) }));
    schedr_config::masked::mask_data(&mut value);
    value
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
            // A reason may quote a value given (a run's parameter, decision 134).
            reason: command
                .reason
                .as_deref()
                .map(schedr_config::masked::mask_text),
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
    /// Values for the run's parameters, as JSON (`{"region": "us"}`), over the DAG's
    /// defaults; checked by the master against the DAG's schema (decision 133).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub params: Option<serde_json::Map<String, serde_json::Value>>,
    /// For a DAG a trigger starts: the key of an event it found, to run again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    /// Or an event given here: its key. It is never stored as an event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_key: Option<String>,
    /// And its data, as JSON of any kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_data: Option<serde_json::Value>,
    /// And when it happened; the run's moment otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_at: Option<Timestamp>,
}

/// A DAG's trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TriggerView {
    /// Its DAG.
    pub dag_id: String,
    /// Its function.
    pub trigger: String,
    /// `running`, `waiting` (for a free watcher slot or to start again), `stopped` (see
    /// `stopped_why`), `switched_off` or `paused` (its DAG's, or outside its `start` and `end`).
    pub state: String,
    /// The worker running it.
    pub worker: Option<String>,
    /// The last key acted on: what it starts from.
    pub bookmark: Option<String>,
    /// Why it isn't running although its DAG wants it: `back_pressure` or `returned`.
    pub stopped_why: Option<String>,
    /// Its errors in a row.
    pub failures: u32,
    /// Its last error.
    pub last_error: Option<String>,
    /// Not started again before this moment.
    pub not_before: Option<Timestamp>,
    /// When this run of it started.
    pub started_at: Option<Timestamp>,
    /// When it last found something.
    pub last_event_at: Option<Timestamp>,
    /// When a poke's call last ended.
    pub last_poked_at: Option<Timestamp>,
    /// When it was last started again for finding nothing for its `restart_if_idle`.
    pub last_restart_at: Option<Timestamp>,
    /// `@trigger(wait_for=...)` (decision 353): the run of the other DAG it is called for and
    /// hasn't ended well with yet; `schedr triggers skip` passes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub called_for: Option<String>,
}

/// The triggers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TriggerList {
    /// By DAG.
    pub triggers: Vec<TriggerView>,
}

/// An event a trigger found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EventView {
    /// Its key.
    pub key: String,
    /// Its data, JSON of any kind.
    pub data: serde_json::Value,
    /// When it happened, if it said.
    pub at: Option<Timestamp>,
    /// When the master got it.
    pub received_at: Timestamp,
}

/// A trigger's events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EventList {
    /// Newest received first.
    pub events: Vec<EventView>,
}

/// Connections to check on a worker (`schedr connections check`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CheckConnectionsRequest {
    /// The root; every root with connections if not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    /// One connection; all of the root's if not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The worker to run it on; one the master picks if not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
}

/// A connection to test on a worker (`schedr connections test`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TestConnectionRequest {
    /// Its root; the one root defining it if not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    /// The worker to run it on; one the master picks if not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// How long it may take, in milliseconds: `[webserver] connection_test_timeout` if not given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
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

/// Which runs to list, over every DAG.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct AllRunsQuery {
    /// Only this DAG's runs.
    #[serde(default)]
    #[param(value_type = Option<String>)]
    pub dag: Option<DagId>,
    /// Only runs in this state: `queued`, `running`, `success`, `failed` or `cancelled`.
    #[serde(default)]
    #[param(value_type = Option<String>)]
    pub state: Option<RunState>,
    /// Only runs triggered before this moment: the last listed run's `triggered_at`, for the
    /// next page (with `before_run`).
    #[serde(default)]
    #[param(value_type = Option<String>, format = DateTime)]
    pub before: Option<Timestamp>,
    /// With `before`: the last listed run's ID, so runs triggered at that same moment (a
    /// scheduler round triggers its runs together) are listed after it, by ID, not skipped.
    #[serde(default)]
    pub before_run: Option<String>,
    /// At most this many (default 50, at most 500).
    #[serde(default)]
    pub limit: Option<u32>,
}

/// A task's stored output in a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OutputView {
    /// The outcome the task finished with.
    pub outcome: String,
    /// The value it returned, masked (decision 131: its texts' secrets, and a field whose
    /// name has a word naming a secret); downstream tasks get it as stored.
    pub value: serde_json::Value,
}

/// The commands about a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CommandList {
    /// Oldest first.
    pub commands: Vec<CommandView>,
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
    /// The instance of a mapped task, from 0 (required for one).
    #[serde(default)]
    pub map_index: Option<i32>,
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
    /// The first byte of `content`: the one asked for, or past it, at the start of the next
    /// line, when that one is inside a line.
    pub from: u64,
    /// Where the next part starts: the byte after `content`, or past it, after the part of
    /// a line longer than a part that isn't shown.
    pub next: u64,
    /// The log's size when it was read.
    pub size: u64,
    /// Whether the attempt still runs (its log may grow).
    pub running: bool,
    /// The log's text, whole lines only (a running attempt's last line once it ends), with
    /// its secrets masked: a password in an address, a value after a name like `password=`
    /// (invalid UTF-8 replaced by `U+FFFD`). A line longer than the part asked for is shown
    /// to its last whitespace in it, then `… (the rest of this line is not shown: it is over
    /// N bytes)`, its rest passed over.
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
    /// What is wrong with it now, as it last reported (e.g. a disabled fork server).
    #[serde(default)]
    pub warnings: Vec<WarningView>,
    /// The master it registered with last (a master's ID, as on `GET /api/v1/deployment`):
    /// kept while it is disconnected; none from an older agent's registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_id: Option<String>,
    /// Its slots for wait steps.
    #[serde(default)]
    pub watcher_slots: u32,
    /// The processes running its wait steps, as its last heartbeat said (decision 343), and
    /// code sets whose hosts were lost.
    #[serde(default)]
    pub watcher_hosts: Vec<WatcherHostView>,
}

/// A process running a worker's wait steps, or a code set whose hosts were lost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WatcherHostView {
    /// The code it runs: its folders' names, the copies' hashes.
    pub code: String,
    /// The wait steps on it (none: no host runs this code now).
    pub wait_steps: u32,
    /// Its process's threads, the wait steps' among them.
    pub threads: u32,
    /// Those a wait step is tied to: running its `to_thread` calls, or started by it. The rest
    /// of the process, a library's pools among them, can't be told apart.
    pub tied_threads: u32,
    /// The memory it uses.
    pub memory_bytes: u64,
    /// Hosts of this code lost since the agent started: crashes and memory kills.
    pub hosts_lost: u64,
}

/// Something wrong with a worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WarningView {
    /// What kind: `fork_server_disabled`.
    pub kind: String,
    /// The interpreter it concerns; empty if none.
    pub interpreter: String,
    /// What it means for tasks.
    pub message: String,
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
            warnings: worker
                .warnings
                .iter()
                .map(|w| WarningView {
                    kind: w.kind.clone(),
                    interpreter: w.interpreter.clone(),
                    message: mask_text(&w.message),
                })
                .collect(),
            master_id: None,
            watcher_slots: worker.watcher_slots,
            watcher_hosts: worker
                .watcher_hosts
                .iter()
                .map(|host| WatcherHostView {
                    code: host.code.clone(),
                    wait_steps: host.wait_steps,
                    threads: host.threads,
                    tied_threads: host.tied_threads,
                    memory_bytes: host.memory_bytes,
                    hosts_lost: host.hosts_lost,
                })
                .collect(),
        }
    }
}

/// The deployment this webserver serves, as nodes (decisions 121-123): its own, its masters
/// (leader and standbys) and its workers, each with its effective configuration, masked by the
/// node itself before it left its process (no secret in clear).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DeploymentView {
    /// The webserver answering (one of the deployment's, if it has several).
    pub webserver: NodeView,
    /// The masters heard from lately: the leader (the lease's holder) first.
    pub masters: Vec<MasterView>,
    /// Every worker the master has seen, gone ones included.
    pub workers: Vec<WorkerNodeView>,
    /// Who this request was made as, as the webserver sees it (`anonymous` without
    /// authentication; the person the web UI passed on, from the UI).
    pub user: String,
}

/// A process of the deployment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct NodeView {
    /// The process (shared by a master and a worker in standalone).
    pub process_id: String,
    /// Its host.
    pub host: String,
    /// Its schedr version.
    pub schedr_version: String,
    /// Its effective configuration, masked.
    #[schema(value_type = Object)]
    pub config: serde_json::Value,
}

/// A problem a node has now (decision 129): shown on its node, yellow for a warning, red for
/// an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProblemView {
    /// An error; a warning otherwise.
    pub error: bool,
    /// What it is, for people.
    pub message: String,
}

impl From<schedr_db::nodes::NodeProblem> for ProblemView {
    fn from(problem: schedr_db::nodes::NodeProblem) -> Self {
        Self {
            error: problem.error,
            message: mask_text(&problem.message),
        }
    }
}

/// A master, leading or standing by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MasterView {
    /// Its ID: the lease holder it takes the lease as.
    pub master_id: String,
    /// `leader` (it holds the lease) or `standby`.
    pub role: String,
    /// Where it runs, and its configuration.
    #[serde(flatten)]
    pub node: NodeView,
    /// When it started.
    pub started_at: Timestamp,
    /// When it was last heard from (it writes its row every 10 s).
    pub heard_at: Timestamp,
    /// Its problems as it last wrote them: its lease renewed late, a round failing, its
    /// command notifications failing.
    #[serde(default)]
    pub problems: Vec<ProblemView>,
}

/// A worker as a node of the deployment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkerNodeView {
    /// Its ID.
    pub worker_id: String,
    /// `active`, `draining`, `disconnected`, `offline` or `lost`.
    pub state: String,
    /// Its schedr version.
    pub schedr_version: String,
    /// The master it registered with last; none from an older agent.
    pub master_id: Option<String>,
    /// The process it runs in; none from an older agent.
    pub process_id: Option<String>,
    /// Its effective configuration, masked; none from an older agent.
    #[schema(value_type = Option<Object>)]
    pub config: Option<serde_json::Value>,
    /// The queues it serves.
    #[serde(default)]
    pub queues: Vec<String>,
    /// Its problems now: lost or offline (errors), what it reports wrong, the import errors
    /// of files it parsed (warnings).
    #[serde(default)]
    pub problems: Vec<ProblemView>,
}

impl From<schedr_db::nodes::MasterRecord> for MasterView {
    fn from(master: schedr_db::nodes::MasterRecord) -> Self {
        Self {
            master_id: master.master_id,
            role: if master.leading { "leader" } else { "standby" }.to_owned(),
            node: NodeView {
                process_id: master.process_id,
                host: master.host,
                schedr_version: master.schedr_version,
                config: master.config,
            },
            started_at: master.started_at,
            heard_at: master.heard_at,
            problems: master.problems.into_iter().map(ProblemView::from).collect(),
        }
    }
}

/// A file of a DAG root's current code that failed to import (decision 66).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ImportErrorView {
    /// The DAG root.
    pub root: String,
    /// The file, relative to the root.
    pub path: String,
    /// The root's content hash that was parsed.
    pub content_hash: String,
    /// When it was parsed.
    pub parsed_at: Timestamp,
    /// The worker that parsed it.
    pub worker_id: String,
    /// The master that stored the parse; none when not known (stored before it was kept).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_id: Option<String>,
    /// That worker's schedr version.
    pub schedr_version: String,
    /// The exception's class name (`ParseTimeout`, `RunnerDied` or `ParseResultTooLarge` when
    /// the worker stopped the file; `InvalidDag` when the master refused a DAG).
    pub exception: String,
    /// The message.
    pub message: String,
    /// The traceback's last lines.
    pub traceback: String,
}

impl From<&schedr_db::dags::ImportErrorRecord> for ImportErrorView {
    fn from(record: &schedr_db::dags::ImportErrorRecord) -> Self {
        Self {
            root: record.root.clone(),
            path: record.path.clone(),
            content_hash: record.content_hash.clone(),
            parsed_at: record.parsed_at,
            worker_id: record.worker_id.clone(),
            master_id: record.master_id.clone(),
            schedr_version: record.schedr_version.clone(),
            exception: record.error.exception.clone(),
            message: mask_text(&record.error.message),
            traceback: mask_text(&record.error.traceback),
        }
    }
}

/// The import errors of each DAG root's current code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ImportErrorList {
    /// By root, then path.
    pub errors: Vec<ImportErrorView>,
}

/// Each DAG root's connections, in its current code (decision 211): what the UI lists, never
/// a value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionList {
    /// By root.
    pub roots: Vec<RootConnections>,
}

/// One root's connections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RootConnections {
    /// The DAG root.
    pub root: String,
    /// By name: those its files define, and those its tasks declare without one.
    pub connections: Vec<ConnectionInfo>,
}

/// A connection, as its root's current parse records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionInfo {
    /// Its name.
    pub name: String,
    /// A typed connection's type (`http`, `ssh`, `mongo`, `sql`, `generic`); none for a
    /// `@connection` function, or one that can't be used.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The file defining it, relative to the root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// Why it can't be used: defined twice, invalid, or defined nowhere (a task declares it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
    /// The DAGs whose tasks declare it, by DAG.
    #[serde(default)]
    pub used_by: Vec<ConnectionUse>,
}

/// The tasks of one DAG declaring a connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionUse {
    /// The DAG.
    pub dag_id: String,
    /// Its tasks declaring it, in the DAG's order.
    pub tasks: Vec<String>,
}

impl ConnectionList {
    /// The list of each root's `registries` (root to its current parse's), with the tasks of
    /// `dags` (their current versions, those not removed) declaring each.
    #[must_use]
    pub fn of(
        registries: &std::collections::BTreeMap<String, schedr_model::connection::Registry>,
        dags: &[StoredDag],
    ) -> Self {
        use std::collections::BTreeMap;
        // Root, then connection, then DAG: its tasks.
        let mut uses: BTreeMap<&str, BTreeMap<&str, BTreeMap<String, Vec<String>>>> =
            BTreeMap::new();
        for dag in dags.iter().filter(|d| !d.removed) {
            let Some(current) = &dag.current else {
                continue;
            };
            let root = current.dag.location.root.as_str();
            for task in &current.dag.tasks {
                for name in &task.connections {
                    uses.entry(root)
                        .or_default()
                        .entry(name.as_str())
                        .or_default()
                        .entry(dag.dag_id.to_string())
                        .or_default()
                        .push(task.id.to_string());
                }
            }
        }
        let empty = schedr_model::connection::Registry::default();
        let mut roots: Vec<&str> = registries.keys().map(String::as_str).collect();
        roots.extend(uses.keys().filter(|r| !registries.contains_key(**r)));
        roots.sort_unstable();
        roots.dedup();
        let mut list = Self::default();
        for root in roots {
            let registry = registries.get(root).unwrap_or(&empty);
            let declared = uses.get(root);
            let mut names: Vec<&str> = registry.names().map(String::as_str).collect();
            names.extend(declared.into_iter().flat_map(|d| d.keys().copied()));
            names.sort_unstable();
            names.dedup();
            if names.is_empty() {
                continue;
            }
            let connections = names
                .into_iter()
                .map(|name| {
                    let found = registry.get(name);
                    ConnectionInfo {
                        name: name.to_owned(),
                        kind: found
                            .as_ref()
                            .ok()
                            .and_then(|def| def.kind)
                            .map(connection_type),
                        file: registry.files.get(name).cloned(),
                        refused: found.err(),
                        used_by: declared
                            .and_then(|d| d.get(name))
                            .map(|by_dag| {
                                by_dag
                                    .iter()
                                    .map(|(dag_id, tasks)| ConnectionUse {
                                        dag_id: dag_id.clone(),
                                        tasks: tasks.clone(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }
                })
                .collect();
            list.roots.push(RootConnections {
                root: root.to_owned(),
                connections,
            });
        }
        list
    }
}

/// A connection type as the DAG JSON writes it.
fn connection_type(kind: schedr_model::connection::ConnectionType) -> String {
    use schedr_model::connection::ConnectionType;
    match kind {
        ConnectionType::Http => "http",
        ConnectionType::Ssh => "ssh",
        ConnectionType::Mongo => "mongo",
        ConnectionType::Sql => "sql",
        ConnectionType::Generic => "generic",
    }
    .to_owned()
}

/// The workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerList {
    /// Every worker the master has seen, in ID order.
    pub workers: Vec<WorkerView>,
}
