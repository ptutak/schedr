//! Messages between the worker agent and the Python runner (`python -m schedr.runner`).
//!
//! They travel over the runner's inherited socket (`SCHEDR_AGENT_FD`), each one as a 4-byte
//! big-endian length followed by that many bytes of UTF-8 JSON (see PROJECT.md, Rust/Python
//! integration: the agent <-> runner channel). Every message is an object tagged by `type`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::dag::Dag;

#[cfg(test)]
mod tests;

/// The largest message either side accepts, in bytes (the cap on inputs per task, 64 MiB).
pub const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024;

/// The error kind of a task its DAG file no longer defines (the runner's
/// `task_not_found`).
pub const TASK_NOT_FOUND: &str = "task_not_found";

/// The error kind of a connection a task opens that its root's registry doesn't have
/// (decision 174): not defined in the DAG folder, defined twice, or invalid.
pub const CONNECTION_NOT_FOUND: &str = "connection_not_found";

/// The error kind of a connection a task opens without declaring it in `connections=`
/// (decision 179).
pub const CONNECTION_DENIED: &str = "connection_denied";

/// The error kind of an attempt whose DAG file is missing although the worker has the root at
/// the hash the master expects (the newest code): the file is gone, not on its way.
pub const DAG_FILE_MISSING: &str = "dag_file_missing";

/// The error kind of a wait step that held its host's event loop past `[watchers] block_limit`.
pub const BLOCKED_EVENT_LOOP: &str = "blocked_event_loop";

/// The error kind of a wait step switched off for getting its host killed (decision 338).
pub const SWITCHED_OFF: &str = "switched_off";

/// The error kind of a wait step whose host ended with no wait step to blame.
pub const WATCHER_HOST_LOST: &str = "watcher_host_lost";

/// What the runner was started to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunnerMode {
    /// Import DAG files and report the DAGs they define.
    Parse,
    /// Run one task attempt.
    Task,
    /// Preload libraries, then fork a process per request (Linux only).
    ForkServer,
    /// Run the wait steps of one code set as coroutines, long-lived (decision 320).
    Watchers,
}

/// A structured error summary: why a DAG file couldn't be imported, or why a task failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ErrorSummary {
    /// The exception's class name, e.g. `ModuleNotFoundError`; `ParseTimeout` or
    /// `RunnerDied` when the agent stopped a file that took too long or lost its runner.
    pub exception: String,
    /// The exception's message.
    pub message: String,
    /// The last lines of the traceback (empty for a timeout).
    pub traceback: String,
}

/// A declaration's `[cleanup]`: what its script runs, and only the settings its `schedr.toml`
/// gives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScriptCleanupDeclaration {
    /// The script, relative to the script root.
    pub script: String,
    /// The interpreter's name.
    pub interpreter: String,
    /// Its arguments (templates).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<crate::value::Template>,
    /// `timeout`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// `retries`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
    /// `retry_delay`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay_ms: Option<u64>,
    /// `anywhere`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anywhere: Option<bool>,
    /// The job's `cleanup_handles`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handles: Option<Vec<String>>,
}

/// A declared script, as the agent answers a `ScriptOperator` (decision 293): what its task
/// runs, and only the settings its `schedr.toml` gives (the DAG's defaults apply to the
/// others).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScriptDeclaration {
    /// Its reference, `<root>:<path>#<name>` (an absolute one resolved to its root).
    pub job: String,
    /// The script, relative to the script root.
    pub script: String,
    /// The interpreter's name.
    pub interpreter: String,
    /// Its arguments, as the task carries them (templates).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<crate::value::Template>,
    /// Its environment variables, as the task carries them (templates).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub env: std::collections::BTreeMap<String, crate::value::Template>,
    /// The secrets it declares.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<crate::secret::SecretDecl>,
    /// Where it runs, relative to the script root.
    pub working_dir: String,
    /// `retries`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
    /// `retry_delay`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay_ms: Option<u64>,
    /// `timeout`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// `queue`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// `max_output_size`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_size: Option<u64>,
    /// `[outcomes]`, if given: each name with its exit code.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outcomes: Vec<crate::task::OutcomeDecl>,
    /// `[cleanup]`, if given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<ScriptCleanupDeclaration>,
}

/// The exception name of the import error the agent records for a file that took longer than
/// the parse timeout.
pub const PARSE_TIMEOUT_EXCEPTION: &str = "ParseTimeout";

/// The exception name of the import error the agent records for a file whose runner died.
pub const RUNNER_DIED_EXCEPTION: &str = "RunnerDied";

/// The exception name of the import error the agent records for a file whose parse result
/// (all its DAGs' JSON together) is larger than the master accepts (B35).
pub const PARSE_RESULT_TOO_LARGE_EXCEPTION: &str = "ParseResultTooLarge";

/// The exception name of the import error the master records for a file that imported but
/// defines a DAG its checks refuse.
pub const INVALID_DAG_EXCEPTION: &str = "InvalidDag";

/// The exception name of the import error the master records for a file whose path isn't
/// inside its root.
pub const INVALID_PATH_EXCEPTION: &str = "InvalidPath";

impl ErrorSummary {
    /// Whether it is a **transient** import error (decision 72.2): the agent stopped the file
    /// (a timeout, a runner that died), so the code itself may be fine. Such a file is parsed
    /// again, and doesn't hold its DAGs.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        self.exception == PARSE_TIMEOUT_EXCEPTION || self.exception == RUNNER_DIED_EXCEPTION
    }

    /// The JSON text of a summary schedr itself wrote (no traceback), e.g. when the agent
    /// lost the runner or the master lost the worker.
    #[must_use]
    pub fn json(exception: &str, message: &str) -> String {
        let summary = Self {
            exception: exception.to_owned(),
            message: message.to_owned(),
            traceback: String::new(),
        };
        serde_json::to_string(&summary).unwrap_or_default()
    }
}

/// One file of a parse job: the DAGs it defines, or why it couldn't be imported. The agent
/// builds it from `file_parsed`, uploads it to the master, which stores it as is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedFile {
    /// The path relative to the root.
    pub path: String,
    /// The DAGs it defines (none for a helper module or a failed import).
    pub dags: Vec<Dag>,
    /// The connections it defines itself (decision 174: the root's registry).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connections: Vec<crate::connection::ConnectionDef>,
    /// Why importing it failed, if it did.
    pub error: Option<ErrorSummary>,
    /// The Python warnings importing it raised (`Category: message`), for `schedr dags
    /// check`; neither uploaded to the master nor stored.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// The script roots its `ScriptOperator`s asked the agent about while it was imported,
    /// found or not (decision 304): the master tells every worker which script roots each DAG
    /// root uses, from its current parse.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub script_roots: Vec<String>,
}

/// A process's output stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

/// A message from the runner to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunnerMessage {
    /// The first message: who is speaking and what for.
    Hello {
        /// The installed `schedr` package version; it must equal the agent's.
        schedr_version: String,
        /// The mode the runner was started in.
        mode: RunnerMode,
    },
    /// The runner starts importing a file (the agent times each file).
    FileStarted {
        /// The file's path relative to the root.
        path: String,
    },
    /// A file was imported, or failed to import.
    FileParsed {
        /// The file's path relative to the root.
        path: String,
        /// The DAGs it defines (none for a helper module).
        dags: Vec<Dag>,
        /// The connections it defines itself (not those of modules it imports).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        connections: Vec<crate::connection::ConnectionDef>,
        /// Why importing it failed, if it did; then `dags` is empty.
        error: Option<ErrorSummary>,
        /// The Python warnings importing it raised (`Category: message`), e.g. a
        /// `TypingHint`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
    },
    /// Every requested file was handled.
    ParseFinished,
    /// The task's DAG file is imported and its arguments built; its function is called next
    /// (the agent times the start from it). Not sent when that fails.
    TaskImported,
    /// The task's function returned; its output is encoded and within the size limit.
    TaskSucceeded {
        /// The output as stored JSON (tagged extra types).
        output: serde_json::Value,
        /// The outcome's name: `SUCCESS`, an outcome union member's class name, or an `Outcome` member's name.
        outcome: String,
    },
    /// A running task reads a secret it didn't get with the request: one under a declared
    /// prefix (`task_ctx.secret(...)`); the agent answers [`AgentMessage::Secret`] or
    /// [`AgentMessage::SecretFailed`].
    SecretGet {
        /// Which secret.
        secret: crate::secret::SecretName,
    },
    /// A DAG file being parsed has a `ScriptOperator` (decision 293): the declaration it
    /// refers to, `<root>:<path>#<name>` or `/abs/path/schedr.toml#<name>`; the agent answers
    /// [`AgentMessage::ScriptDeclaration`] or [`AgentMessage::ScriptNotFound`].
    ScriptDeclaration {
        /// The reference as written.
        reference: String,
    },
    /// A running task opens a connection it didn't get with the request
    /// (`with name():`); the agent answers [`AgentMessage::Connection`] or
    /// [`AgentMessage::ConnectionFailed`].
    ConnectionGet {
        /// The connection's name.
        name: String,
    },
    /// A request to schedr's REST API (`schedr.api`, M4.11h): the agent has the master run it as
    /// the task; it answers [`AgentMessage::ApiAnswer`].
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
    /// A running task registers a cleanup handle (`cleanup_handle(x)`); the agent answers
    /// [`AgentMessage::HandleAdded`] or [`AgentMessage::HandleRefused`].
    HandleAdd {
        /// The handle.
        handle: CleanupHandle,
    },
    /// The task failed: an exception, or an output that can't be stored.
    TaskFailed {
        /// What went wrong.
        error: ErrorSummary,
        /// A failure schedr recognises, e.g. `output_not_serializable`; `None` for an
        /// exception from the task's own code.
        kind: Option<String>,
    },
    /// A watcher host's event loop has been held past its limit: it can only be killed.
    LoopBlocked {
        /// The watcher holding it; `None` if no watcher is to blame.
        watcher: Option<u64>,
        /// Where the loop's thread is.
        stack: String,
    },
    /// What a trigger found (inside a [`Self::WatcherMessage`]); it goes on when told
    /// [`AgentMessage::EventStored`].
    TriggerEvent {
        /// Its key, unique in its DAG.
        key: String,
        /// Its data, encoded as an output is.
        #[schemars(with = "serde_json::Value")]
        data: serde_json::Value,
        /// When it happened, if it said.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<jiff::Timestamp>,
    },
    /// A poke trigger's call ended; `error` is why it failed. Nothing answers it.
    TriggerPoked {
        /// Why the call failed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// A watcher host is there, its event loop held or not, and how it is doing.
    HostAlive {
        /// Its process's threads.
        threads: u32,
        /// Those a wait step is tied to: running its `to_thread` calls, or started by it.
        tied_threads: u32,
        /// `to_thread` calls it stopped so far, each of which may have left shared state half
        /// updated.
        thread_stops: u64,
        /// How long its event loop last took to get to a call, in milliseconds.
        loop_lag_ms: u64,
    },
    /// A thread of this watcher didn't stop in its grace time: its host ends after its result
    /// (inside a [`Self::WatcherMessage`]).
    WatcherStuck,
    /// Of one watcher, in a watcher host: what a task's runner would send.
    WatcherMessage {
        /// The watcher's ID.
        watcher: u64,
        /// The message: never a `watcher_message`.
        message: Box<RunnerMessage>,
    },
    /// What a watcher printed: its attempt's log (inside a [`Self::WatcherMessage`]).
    WatcherLog {
        /// Which of its streams.
        stream: OutputStream,
        /// The text.
        text: String,
    },
    /// A watcher ended by its cancel (inside a [`Self::WatcherMessage`]).
    WatcherCancelled,
    /// A poke stopped between calls, as asked: its next one is on new code (decision 335).
    WatcherMoved,
    /// The fork server preloaded its libraries and is single-threaded: it takes `fork`
    /// requests.
    ForkServerReady,
    /// The fork server can't be used: tasks fall back (to the bare fork server, then to
    /// spawn). It answers every later `fork` with `fork_failed` until the agent closes the
    /// channel (exiting with a request unread would reset the connection and could lose this
    /// message). Also the answer to a `fork` when a thread appeared since the check.
    ForkServerDisabled {
        /// Why, e.g. a failing preload or a second thread.
        reason: String,
        /// The names of the threads besides the main one (`/proc/self/task/*/comm`).
        threads: Vec<String>,
    },
    /// A `fork` request was carried out: the shim runs, and reports on the status socket.
    Forked {
        /// The request's ID.
        id: u64,
    },
    /// A `fork` request failed; the task falls back.
    ForkFailed {
        /// The request's ID.
        id: u64,
        /// Why.
        reason: String,
    },
    /// From the shim, on the status socket: the forked process runs in its own process group
    /// (its ID is the PID), so the agent may signal and watch the group.
    ProcessStarted {
        /// The process's PID, which is also its process group's ID.
        pid: i32,
    },
    /// From the shim, on the status socket: the forked process ended. The shim reaps it when
    /// the agent closes its end, so the PID isn't reused before.
    ProcessExited {
        /// The exit code, if it exited.
        code: Option<i32>,
        /// The signal that ended it, if one did.
        signal: Option<i32>,
        /// Whether it dumped core (ended by a signal).
        core_dumped: bool,
        /// What it used, with the children it reaped; `None` if the shim couldn't read it.
        usage: Option<ProcessUsage>,
    },
}

/// What a process used, with the children it reaped: its parent's view at its exit (`rusage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProcessUsage {
    /// CPU time in user mode, in microseconds.
    pub user_cpu_micros: u64,
    /// CPU time in the kernel, in microseconds.
    pub system_cpu_micros: u64,
    /// The peak resident set size, in bytes. For a forked process it includes the pages it
    /// shares with the fork server (the watchdog's PSS doesn't); `None` for a spawned process
    /// (its memory is gone before the agent reads its zombie).
    pub peak_rss_bytes: Option<u64>,
    /// Page faults that needed I/O.
    pub major_faults: u64,
    /// Times it gave up the CPU (waiting for I/O or a lock), with its threads and reaped
    /// children; `None` for a spawned process (its zombie's `/proc` has the leader thread's
    /// only).
    pub voluntary_context_switches: Option<u64>,
    /// Times it was preempted; `None` as for `voluntary_context_switches`.
    pub involuntary_context_switches: Option<u64>,
}

/// A parse job: the files of one root to import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParseRequest {
    /// The root's name, e.g. `etl-repo`.
    pub root: String,
    /// The root's directory.
    pub root_dir: String,
    /// The files to import, relative to the root, in order.
    pub files: Vec<String>,
    /// Directories put first on `sys.path` (every DAG root).
    pub sys_path: Vec<String>,
}

/// Where a DAG file is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DagFile {
    /// The DAG root's name.
    pub root: String,
    /// The root's directory on this worker.
    pub root_dir: String,
    /// The file's path relative to the root.
    pub path: String,
}

/// The run information a task gets (`dag_ctx` and the `SCHEDR_*` variables).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunInfo {
    /// The DAG's ID.
    pub dag_id: String,
    /// The run's ID (a `UUIDv7`).
    pub run_id: String,
    /// The run's kind.
    pub kind: crate::states::RunKind,
    /// The moment the run is for.
    pub planned_for: jiff::Timestamp,
    /// The start of the run's window.
    pub since: jiff::Timestamp,
    /// The attempt's try number, from 1.
    pub try_number: u32,
    /// The instance of a mapped task.
    pub map_index: Option<u32>,
    /// The run's parameters, as stored JSON (decision 138); `{}` when the DAG takes none.
    #[serde(default = "no_params")]
    #[schemars(with = "serde_json::Map<String, serde_json::Value>")]
    pub params: serde_json::Value,
    /// The event that started it, for a run of an event-triggered DAG.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<crate::trigger::RunEvent>,
}

/// A run of a DAG without parameters.
fn no_params() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// One task attempt for the runner to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskRequest {
    /// The DAG file to import.
    pub dag_file: DagFile,
    /// The DAG's ID (the file may define several DAGs).
    pub dag_id: String,
    /// The task as stored in DAG JSON (its arguments and settings).
    pub task: crate::task::Task,
    /// The upstream outputs the task uses, by task ID, as stored JSON.
    pub inputs: std::collections::BTreeMap<String, serde_json::Value>,
    /// The upstream tasks' outcomes, by task ID: a name, or `null` for a skipped one (its
    /// references stand for `None`).
    #[serde(default)]
    pub outcomes: std::collections::BTreeMap<String, serde_json::Value>,
    /// The run information.
    pub run: RunInfo,
    /// Directories put first on `sys.path` (every DAG root).
    pub sys_path: Vec<String>,
    /// The secrets fetched for the attempt (those its arguments name and those it declares,
    /// prefixes aside): filled into its arguments and read by `task_ctx.secret(...)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretValue>,
    /// Set for `schedr connections test` (decision 174): no task is run; the file is
    /// imported, and this connection opened and checked (decision 198).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_test: Option<String>,
    /// Set for a cleanup: the task's cleanup function is called with the task's inputs and
    /// these handles, in registration order, instead of the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<Vec<CleanupHandle>>,
    /// Set for a trigger: `task` is its function, which yields events until it is stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TriggerStart>,
}

/// How a trigger starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TriggerStart {
    /// The last key acted on: what its function gets as `after`.
    pub after: Option<String>,
}

/// A handle registered by an attempt: what its cleanup needs that only existed in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CleanupHandle {
    /// Its type's name (a dataclass's name, or `schedr task-ctx cleanup add`'s first argument).
    pub handle_type: String,
    /// Its fields, as stored JSON.
    pub value: serde_json::Value,
}

/// A secret's value, fetched by the agent for an attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretValue {
    /// Which secret.
    pub name: crate::secret::SecretName,
    /// A key's value as a string; a whole secret as an object of key to value.
    pub value: serde_json::Value,
}

/// What the fork server preloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkServerRequest {
    /// The modules to import, in order (none for the bare fork server).
    pub preload: Vec<String>,
}

/// A request to the fork server: start a runner process.
///
/// Four descriptors come with it (`SCM_RIGHTS`, on the message's first bytes), in order: the
/// process's end of its channel, its stdout, its stderr, and the shim's end of the status
/// socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkRequest {
    /// The request's ID, repeated in the answer.
    pub id: u64,
    /// What the process runs (`parse` or `task`); it shakes hands on its channel as a spawned
    /// runner does.
    pub mode: RunnerMode,
    /// The process's whole environment.
    pub env: std::collections::BTreeMap<String, String>,
    /// The process's working directory.
    pub cwd: String,
}

/// A watcher host's request: what it holds for its whole life (decision 320).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WatchersRequest {
    /// Directories put first on `sys.path` (every DAG root's copy).
    pub sys_path: Vec<String>,
    /// How long a watcher may hold the event loop (`[watchers] block_limit`).
    pub block_limit_ms: u64,
    /// How long importing a DAG file may hold it (`[dags] parse_file_timeout`).
    pub import_limit_ms: u64,
}

/// A message from the agent to the runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentMessage {
    /// The handshake succeeded.
    Welcome,
    /// The handshake failed; the runner exits.
    Refused {
        /// Why, e.g. a version mismatch.
        reason: String,
    },
    /// A parse job.
    Parse(ParseRequest),
    /// A task attempt.
    Task(Box<TaskRequest>),
    /// The fork server's preload list (its mode's request).
    ForkServer(ForkServerRequest),
    /// A watcher host's code set (its mode's request).
    Watchers(WatchersRequest),
    /// Start a wait step in a watcher host.
    StartWatcher {
        /// The watcher's ID, unique in the host.
        watcher: u64,
        /// What to run.
        task: Box<TaskRequest>,
        /// How long a thread it left running has to end before the host does (decision 337).
        cancel_grace_ms: u64,
        /// A poke moving here from older code (decision 335): it says `task_imported` when it
        /// could start, or `task_failed`, and makes no call until a [`Self::GoWatcher`].
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        moved_in: bool,
    },
    /// Stop a poke between its calls: `watcher_moved`, unless it ended first.
    MoveWatcher {
        /// The watcher's ID.
        watcher: u64,
    },
    /// Let a poke that moved here make its first call.
    GoWatcher {
        /// The watcher's ID.
        watcher: u64,
        /// What is left of its `max_wait`.
        max_wait_ms: Option<u64>,
    },
    /// Cancel a watcher: `CancelledError` at its next `await`.
    CancelWatcher {
        /// The watcher's ID.
        watcher: u64,
    },
    /// An answer to one watcher, in a watcher host.
    ToWatcher {
        /// The watcher's ID.
        watcher: u64,
        /// What a task's runner would get.
        message: Box<AgentMessage>,
    },
    /// The master has what a trigger found: on to the next.
    EventStored,
    /// Start a runner process (to the fork server).
    Fork(ForkRequest),
    /// The declaration a [`RunnerMessage::ScriptDeclaration`] asked for, checked.
    ScriptDeclaration(Box<ScriptDeclaration>),
    /// Why the declaration a [`RunnerMessage::ScriptDeclaration`] asked for can't be used:
    /// its script root isn't on this worker (decision 295), its file or job isn't there, or
    /// the file has problems; the DAG file's import error.
    ScriptNotFound {
        /// What is wrong.
        message: String,
    },
    /// The value of the secret a [`RunnerMessage::SecretGet`] asked for.
    Secret {
        /// A key's value as a string; a whole secret as an object of key to value.
        value: serde_json::Value,
    },
    /// What the API answered a [`RunnerMessage::ApiCall`].
    ApiAnswer {
        /// The HTTP status.
        status: u16,
        /// The body, JSON (`{"error": ...}` for a refusal).
        body: serde_json::Value,
        /// What the task is told of its request: a secret in it masked. Printed to its log.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    /// The handle of a [`RunnerMessage::HandleAdd`] is registered.
    HandleAdded,
    /// Why the handle of a [`RunnerMessage::HandleAdd`] isn't registered.
    HandleRefused {
        /// What went wrong.
        message: String,
    },
    /// Why the secret a [`RunnerMessage::SecretGet`] asked for can't be read.
    SecretFailed {
        /// The error kind (`secret_unavailable`, `secret_denied`, `secret_not_found`).
        kind: String,
        /// What went wrong, naming the secret (never a value).
        message: String,
    },
    /// The values of the connection a [`RunnerMessage::ConnectionGet`] asked for, rendered.
    Connection {
        /// Each parameter's value, as stored JSON.
        values: std::collections::BTreeMap<String, serde_json::Value>,
    },
    /// Why the connection a [`RunnerMessage::ConnectionGet`] asked for can't be opened.
    ConnectionFailed {
        /// The error kind (`connection_not_found`, or a secret's).
        kind: String,
        /// What went wrong, naming the connection (never a value).
        message: String,
    },
}

/// Whether the agent accepts a runner: the versions must be equal (the binary and the
/// Python package are released together, see PROJECT.md, Packaging and releases). The
/// agent's Cargo version is compared as the Python package writes it: a release candidate
/// `0.1.0-rc1` is the package's `0.1.0rc1` (decision 163).
///
/// # Errors
///
/// Returns the reason to send in [`AgentMessage::Refused`].
pub fn check_runner_version(agent_version: &str, runner_version: &str) -> Result<(), String> {
    let agent_version = python_version(agent_version);
    if agent_version == runner_version {
        Ok(())
    } else {
        Err(format!(
            "the runner's schedr package is {runner_version}, the agent is {agent_version}: \
             install schedr=={agent_version} in the interpreter's environment"
        ))
    }
}

/// A Cargo version as the Python package's metadata writes it (PEP 440, as maturin
/// normalizes it): `X.Y.Z-rcN` is `X.Y.ZrcN`; a release is unchanged. The release workflow
/// allows no other pre-release form (`scripts/release.py`).
#[must_use]
pub fn python_version(cargo_version: &str) -> String {
    match cargo_version.split_once("-rc") {
        Some((release, number))
            if !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()) =>
        {
            format!("{release}rc{number}")
        }
        _ => cargo_version.to_owned(),
    }
}
