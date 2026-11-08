//! Commands: actions a person asks for (UI, CLI, API), written by the webserver as command
//! rows and applied by the master, which stays the only writer of scheduling state (see
//! PROJECT.md, Web UI and CLI: how the webserver gets state).

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::ids::{DagId, TaskId};

#[cfg(test)]
mod tests;

/// The event a manual run of an event-triggered DAG brings. It is never stored as an event: a key
/// given here would make the real event a repeat later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AskedEvent {
    /// One the trigger found, by its key.
    Stored {
        /// Its key.
        key: String,
    },
    /// One given here.
    Given {
        /// Its key.
        key: String,
        /// Its data.
        data: serde_json::Value,
        /// When it happened; the run's moment otherwise.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<Timestamp>,
    },
}

/// An action asked for by a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    /// Start a manual run, for `planned_for` (`--for`) or now.
    TriggerRun {
        /// The DAG.
        dag_id: DagId,
        /// The moment the run is for; now if not given.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        planned_for: Option<Timestamp>,
        /// Values for the run's parameters, as JSON, over the DAG's defaults (decision 138).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        params: Option<serde_json::Map<String, serde_json::Value>>,
        /// The event it brings, for a DAG a trigger starts.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event: Option<AskedEvent>,
    },
    /// Start a DAG's trigger again from its bookmark, whatever stopped it (`schedr triggers restart`).
    RestartTrigger {
        /// Its DAG.
        dag_id: DagId,
    },
    /// Pass the run an `@trigger(wait_for=...)` keeps failing on, so it is called for the next
    /// (`schedr triggers skip`, decision 353); refused while a call of it runs.
    SkipTriggerRun {
        /// Its DAG.
        dag_id: DagId,
    },
    /// Cancel a run (queued or running).
    CancelRun {
        /// The run.
        run_id: String,
    },
    /// Stop the scheduler from creating runs of a DAG; optionally cancel its runs in progress.
    PauseDag {
        /// The DAG.
        dag_id: DagId,
        /// Also cancel its queued and running runs.
        #[serde(default)]
        cancel_runs: bool,
    },
    /// Let the scheduler create runs of a DAG again.
    UnpauseDag {
        /// The DAG.
        dag_id: DagId,
    },
    /// Switch a wait step back on (`schedr sensors switch-on`, decision 338).
    SwitchOnSensor {
        /// Its DAG.
        dag_id: DagId,
        /// The wait step.
        sensor: TaskId,
    },
    /// Cancel one task of a run (`schedr tasks cancel`): stopped if it runs, cancelled at
    /// once if it hasn't started; the tasks downstream of it are cancelled.
    CancelTask {
        /// The run.
        run_id: String,
        /// The task.
        task_id: TaskId,
        /// One instance of a mapped task; none: the whole task (decision 82).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        map_index: Option<i32>,
    },
    /// Queue an `unschedulable` task of an unfinished run again (`schedr tasks retry`).
    RetryTask {
        /// The run.
        run_id: String,
        /// The task.
        task_id: TaskId,
        /// The instance of a mapped task (decision 82).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        map_index: Option<i32>,
    },
    /// Drain a worker for maintenance (`schedr workers drain`): no new tasks, its running ones
    /// finish; kept across restarts until resumed.
    DrainWorker {
        /// The worker.
        worker_id: String,
    },
    /// Let a drained worker take tasks again (`schedr workers resume`).
    ResumeWorker {
        /// The worker.
        worker_id: String,
    },
    /// Render connections on a worker (`schedr connections check`, decisions 174, 193): every
    /// secret present or missing, never a value. Each root on a worker having its current
    /// code; the command's result fills in as the workers answer.
    CheckConnections {
        /// The root; none: every root with connections.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        root: Option<String>,
        /// One connection; none: all of the root's.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// The worker to run it on; none: one the master picks.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker: Option<String>,
    },
    /// Open a connection on a worker and run its check (`schedr connections test`, decisions
    /// 193, 198-200): beside the tasks, no slot, one at a time per worker.
    TestConnection {
        /// The connection.
        name: String,
        /// Its root; none: the one root defining it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        root: Option<String>,
        /// The worker to run it on; none: one the master picks.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker: Option<String>,
        /// How long it may take (decision 200: 60 s unless given), in milliseconds.
        timeout_ms: u64,
    },
}

/// The longest a connection test may be given (`--timeout`): an hour.
pub const MAX_CONNECTION_TEST_MS: u64 = 3_600_000;

/// How long a worker gives a connections check (its secrets read, each connection rendered),
/// waiting for its turn included; a test is given its own timeout.
pub const CONNECTION_CHECK_TIMEOUT_MS: u64 = 30_000;

/// How long past its time the master waits for a connections job's answer before calling it
/// lost (a test's process being stopped, the answer on its way).
pub const CONNECTION_ANSWER_MARGIN_MS: u64 = 30_000;

/// The result of a connections command, as stored with it: one job per root, filled in as
/// the workers answer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsResult {
    /// A job per root.
    pub jobs: Vec<ConnectionJobResult>,
}

impl ConnectionsResult {
    /// Whether every job has an answer (or never will).
    #[must_use]
    pub fn finished(&self) -> bool {
        self.jobs.iter().all(|job| job.state != JobState::Running)
    }
}

/// One root's job: the worker it went to and, once it answered, each connection's report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionJobResult {
    /// The root.
    pub root: String,
    /// The worker running it.
    pub worker: String,
    /// Where it stands.
    pub state: JobState,
    /// Why it has no answer (`lost`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Each connection's report, once done.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<ConnectionReport>,
}

/// Where a connections job stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Sent to its worker, no answer yet.
    Running,
    /// Answered.
    Done,
    /// No answer came: its worker went away or restarted, the master restarted, or the
    /// answer didn't come in time.
    Lost,
}

/// What a worker found about one connection: never a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionReport {
    /// The connection.
    pub name: String,
    /// Whether it rendered (a check) or opened and passed its check (a test).
    pub ok: bool,
    /// Each secret it names, and whether the worker's storage has it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretReport>,
    /// What went wrong, masked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How long a test took, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

/// A secret a connection names, as the worker found it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretReport {
    /// The secret, as a DAG writes it (`secrets['etl/db']['password']`).
    pub secret: String,
    /// What the storage said.
    pub state: SecretState,
    /// Why it can't be read, for a `denied` or `unavailable` one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// What a storage said of a secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SecretState {
    /// It is there.
    Present,
    /// It isn't, and its default stands in (decision 176).
    Default,
    /// It isn't.
    Missing,
    /// The worker may not read it.
    Denied,
    /// The storage didn't answer.
    Unavailable,
}

impl Command {
    /// The command's kind, as stored in its row (`trigger_run`, `cancel_run`, ...).
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::TriggerRun { .. } => "trigger_run",
            Self::CancelRun { .. } => "cancel_run",
            Self::PauseDag { .. } => "pause_dag",
            Self::UnpauseDag { .. } => "unpause_dag",
            Self::SwitchOnSensor { .. } => "switch_on_sensor",
            Self::RestartTrigger { .. } => "restart_trigger",
            Self::SkipTriggerRun { .. } => "skip_trigger_run",
            Self::CancelTask { .. } => "cancel_task",
            Self::RetryTask { .. } => "retry_task",
            Self::DrainWorker { .. } => "drain_worker",
            Self::ResumeWorker { .. } => "resume_worker",
            Self::CheckConnections { .. } => "check_connections",
            Self::TestConnection { .. } => "test_connection",
        }
    }
}

/// Where a command stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    /// Written, not yet applied by the master.
    Pending,
    /// Applied.
    Applied,
    /// Not applied, with a reason.
    Rejected,
}
