//! The configuration sections and their defaults.
//!
//! Only the keys the first milestones need exist so far; sections grow additively. Unknown
//! keys are an error, so a typo never passes silently.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::roots::{PathList, Root};
use crate::units::{
    DurationText, MemoryReserve, SizeText, deserialize_duration, deserialize_mode_text,
    deserialize_optional_size, deserialize_size, serialize_duration, serialize_optional_size,
    serialize_size,
};

#[cfg(test)]
mod tests;

/// The interpreter name used for parsing DAGs and running Python tasks unless configured.
pub const DEFAULT_DAG_INTERPRETER: &str = "python";

/// The executable behind the built-in `python` interpreter when `[interpreters]` doesn't
/// define one.
pub const BUILTIN_PYTHON: &str = "python3";

/// The start of the environment variables that set a timing of [`Timings`]:
/// `SCHEDR_HIDDEN__MASTER__TICK_INTERVAL=100ms`. Not configuration, and unsupported: the tests
/// need them, and anyone may experiment at their own risk. The name keeps them from being set by
/// accident; a file can't set them, and each one set is warned about.
pub const HIDDEN_PREFIX: &str = "SCHEDR_HIDDEN__";

/// The timings that aren't a user's to choose, each tuned, tied to others, or what the cluster's
/// correctness depends on (a wrong value can run tasks twice, or lose a healthy worker): fixed,
/// not configuration. Only `SCHEDR_HIDDEN__*` changes them, which is what the tests do, to run
/// in seconds; anyone else does so at their own risk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timings {
    /// How often the master's loop runs (scheduler, commands, dispatching).
    pub tick_interval: Duration,
    /// How often workers send `Heartbeat` (the master's setting, sent to every worker).
    pub heartbeat_interval: Duration,
    /// A worker silent for this long is lost.
    pub lease_timeout: Duration,
    /// How long no alive worker must report a script root before DAG roots using it are no
    /// longer withdrawn for it (decision 303).
    pub script_root_grace: Duration,
    /// How far before the newest event an `EventNamed` trigger acted on it looks again, for one
    /// stored late (decision 354: a post's time is taken before it commits, and webservers'
    /// clocks differ); the keys its DAG has are skipped. Longer than a `query_timeout` and the
    /// webservers' clock skew.
    pub event_lookback: Duration,
    /// How long the master's lease lasts (one master per database: a second one waits as a
    /// standby, and takes over at most this long after the first dies); renewed every third
    /// of it. At least three `query_timeout`s, shorter than `loop_stall_timeout`.
    pub lease_period: Duration,
    /// After a commit cut off by the time limit or a lost connection, how long the master reads
    /// back whether it was stored (holding its lock: rounds and worker messages wait). Not seen
    /// by then: taken as not stored.
    pub read_back_window: Duration,
    /// How often it reads back within `read_back_window`.
    pub read_back_interval: Duration,
    /// The longest a query (or waiting for a connection) may take; a write's transaction, a
    /// few of these. Shorter than `loop_stall_timeout`, so one hung query doesn't look
    /// like a stuck loop, and at most a third of `lease_period`.
    pub query_timeout: Duration,
    /// The master's liveness fails when its scheduling loop hasn't started an iteration for
    /// this long.
    pub loop_stall_timeout: Duration,
    /// How long a task whose assignment fails on its own (its data won't read, the database
    /// refuses it) is left out of the rounds, so the others go on, before it is tried again.
    pub set_aside: Duration,
    /// The least an `@sensor(wait_for=...)`'s call gets once it is handed its run, though its
    /// `max_wait` is nearly or already out: the run ended within it (decision 353).
    pub handed_call_min: Duration,
    /// How long a lost worker's cleanup waits, on top of the cancel grace, before it moves: a
    /// worker only cut off has stopped the task by then (decision 256).
    pub cleanup_move_wait: Duration,
    /// How often a master whose listening for commands failed to start (the database too slow
    /// to check for a pooler, or unreachable) tries again; it polls meanwhile.
    pub listen_retry: Duration,
    /// A DAG that had a run in progress turns quiet this long after the last one (a DAG found
    /// idle otherwise is quiet at once), unless a fire time is within `quiet_dag_wake_ahead`:
    /// rounds then skip it until something wakes it.
    pub quiet_dag_after: Duration,
    /// How long before its next fire time a quiet DAG is woken (it is busy from then on).
    pub quiet_dag_wake_ahead: Duration,
    /// Every quiet DAG is visited within this long, spread over the rounds (each visits a
    /// slice, those visited longest ago), and every DAG's ID and version are checked against
    /// memory this often: about the longest a missed wake can delay a DAG.
    pub quiet_dag_full_pass_interval: Duration,
    /// How often the web UI's live pages are told to read themselves again when the
    /// database's changes can't be followed (`[database] change_notifications = false`, or no
    /// listening connection).
    pub refresh_interval: Duration,
    /// How long connecting to a worker may take, to fetch a task's log. Raise it on a slow
    /// network between the webserver and the workers.
    pub worker_connect_timeout: Duration,
    /// From a drain's SIGKILL of the tasks still running to the end of
    /// `termination_grace_period`: time to report them interrupted and deregister before the
    /// pod is killed (with `0s`, their results may not reach the master, and they end `lost`).
    pub drain_report_period: Duration,
    /// How often the watchdog samples each task's memory.
    pub memory_check_interval: Duration,
    /// How long a peer has to complete the TLS handshake. Raise it for slow clients; lower it to
    /// drop stalled connections sooner.
    pub handshake_timeout: Duration,
    /// How long a stopped process's output may take to reach its log (a process it started in a
    /// session of its own can hold the pipes open).
    pub output_drain_timeout: Duration,
    /// How long a fork server may take to answer a request; then it is restarted, and the task
    /// spawned.
    pub fork_answer_timeout: Duration,
    /// The first wait before a fork server is restarted; it doubles up to `fork_backoff_max`.
    pub fork_backoff_first: Duration,
    /// The longest wait before a fork server is restarted.
    pub fork_backoff_max: Duration,
    /// A fork server up for this long starts its restarts' waits over.
    pub fork_backoff_reset_after: Duration,
    /// How often old task logs are swept.
    pub log_sweep_interval: Duration,
    /// How often an orphan found at start is looked at while it stops.
    pub orphan_poll: Duration,
    /// How long an orphan may take to go after SIGKILL.
    pub orphan_kill_wait: Duration,
    /// How long an orphan found at start gets from SIGTERM to SIGKILL.
    pub orphan_grace: Duration,
    /// The first wait after the master connection drops; it doubles up to
    /// `reconnect_backoff_max`, with jitter.
    pub reconnect_backoff_first: Duration,
    /// The longest wait between tries to reach the master.
    pub reconnect_backoff_max: Duration,
    /// How often the agent's loop turns at least, however quiet. Shorter than
    /// `loop_stall_timeout`, or a quiet agent looks stuck.
    pub worker_alive_interval: Duration,
    /// How long a stopping worker waits for the master to take its deregistering.
    pub deregister_wait: Duration,
    /// How often a drain looks at what is left.
    pub drain_tick: Duration,
    /// How long a drained worker waits for the master to acknowledge its last results; never
    /// past half the report period before the grace period ends, left to deregister and exit.
    pub drain_ack_wait: Duration,
    /// How often a stopped task's process group is looked at until it is gone.
    pub stop_poll: Duration,
    /// How long a connection test's process gets from SIGTERM to SIGKILL.
    pub connection_job_cancel_grace: Duration,
    /// A Vault token's renewal never comes sooner than this.
    pub vault_min_renew_wait: Duration,
    /// How far back a worker looks for its tightest master-time sample (decision 447).
    pub master_sample_window: Duration,
    /// How far apart the master's wall clock and a worker's may be, beyond the round trip, before
    /// the worker warns (decision 449, B232).
    pub clock_skew_warning: Duration,
    /// How often a Vault session with nothing to renew is looked at again.
    pub vault_renew_check: Duration,
    /// How soon a failed Vault renewal is tried again, while the token lasts.
    pub vault_renew_retry: Duration,
    /// At most one extra round every this long, under a stream of finished tasks.
    pub round_gap: Duration,
    /// After notifications failed, how long until they are tried again; polling goes on
    /// meanwhile.
    pub notification_retry: Duration,
    /// How often the master's loop turns at least while it waits. Shorter than
    /// `loop_stall_timeout`, or a waiting master looks stuck.
    pub master_alive_interval: Duration,
    /// How often the master's gauges are read from the database.
    pub gauge_refresh: Duration,
    /// How often the master's sender looks for notifications to deliver.
    pub outbox_poll: Duration,
    /// The first wait before a notification that failed is tried again; it doubles up to
    /// `outbox_retry_max`.
    pub outbox_retry_first: Duration,
    /// The longest wait before a notification that failed is tried again.
    pub outbox_retry_max: Duration,
    /// How often the master's retention job deletes what `[retention]` no longer keeps
    /// (decision 432).
    pub retention_interval: Duration,
    /// The first wait before a failed parse job is tried again; it doubles up to
    /// `parse_retry_max`.
    pub parse_retry_first: Duration,
    /// The longest wait before a failed parse job is tried again.
    pub parse_retry_max: Duration,
    /// How often a master writes its row of the deployment's nodes.
    pub node_heartbeat: Duration,
    /// How often changes are read where they aren't announced (`SQLite`).
    pub changes_poll_interval: Duration,
    /// How often the webserver looks at whether the database announces its changes.
    pub events_recheck: Duration,
    /// How often an event stream gets a comment, keeping idle connections open through proxies.
    pub events_keep_alive: Duration,
    /// How soon following the database's changes is tried again after a failure.
    pub events_retry: Duration,
    /// How often the users and token files are looked at for a change. A removed user or a
    /// changed password keeps working this long: raising it delays revocation.
    pub auth_recheck: Duration,
    /// How long a password check that passed is remembered, instead of running bcrypt again. A
    /// change of the users file forgets them all.
    pub auth_remembered: Duration,
    /// How long running requests get once the server stops; then they are cut.
    pub requests_grace: Duration,
    /// How often a webserver started before the migration looks again.
    pub schema_retry: Duration,
    /// How often a page's stream gets a `ping`, and the log tail a comment.
    pub ui_keep_alive: Duration,
    /// A page's stream sends at most one `change` this often, per deployment.
    pub ui_coalesce: Duration,
    /// How often the log tail reads the log.
    pub ui_tail_interval: Duration,
    /// How long the log tail keeps trying a log out of reach before it says so and stops.
    pub ui_tail_give_up: Duration,
    /// The first wait before a relay opens its stream again; it doubles up to `relay_retry_max`.
    pub ui_relay_retry_first: Duration,
    /// The longest wait between a relay's tries.
    pub ui_relay_retry_max: Duration,
    /// Once a standalone's master failed, how long its worker gets to stop.
    pub worker_stop_after_failure: Duration,
    /// How long a listener waits after a failed accept (too many open files), instead of
    /// spinning.
    pub accept_retry: Duration,
    /// The wait before a failed trigger starts again; it doubles with each failure in a row.
    pub trigger_restart_backoff: Duration,
    /// The most it waits.
    pub trigger_restart_backoff_max: Duration,
    /// The `SCHEDR_HIDDEN__*` variables set, each warned about when starting.
    pub overridden: Vec<String>,
}

impl Default for Timings {
    /// The values every deployment runs with.
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(10),
            lease_timeout: Duration::from_secs(60),
            script_root_grace: Duration::from_mins(5),
            event_lookback: Duration::from_mins(1),
            lease_period: Duration::from_secs(30),
            read_back_window: Duration::from_secs(3),
            read_back_interval: Duration::from_millis(500),
            query_timeout: Duration::from_secs(10),
            loop_stall_timeout: Duration::from_secs(120),
            set_aside: Duration::from_mins(1),
            handed_call_min: Duration::from_mins(1),
            cleanup_move_wait: Duration::from_secs(10),
            listen_retry: Duration::from_mins(30),
            quiet_dag_after: Duration::from_mins(1),
            quiet_dag_wake_ahead: Duration::from_mins(1),
            quiet_dag_full_pass_interval: Duration::from_mins(1),
            refresh_interval: Duration::from_secs(15),
            worker_connect_timeout: Duration::from_secs(5),
            drain_report_period: Duration::from_secs(2),
            memory_check_interval: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(10),
            output_drain_timeout: Duration::from_secs(5),
            fork_answer_timeout: Duration::from_secs(30),
            fork_backoff_first: Duration::from_secs(1),
            fork_backoff_max: Duration::from_mins(5),
            fork_backoff_reset_after: Duration::from_mins(10),
            log_sweep_interval: Duration::from_hours(1),
            orphan_poll: Duration::from_millis(50),
            orphan_kill_wait: Duration::from_secs(5),
            orphan_grace: Duration::from_secs(10),
            reconnect_backoff_first: Duration::from_millis(200),
            reconnect_backoff_max: Duration::from_secs(30),
            worker_alive_interval: Duration::from_secs(5),
            deregister_wait: Duration::from_secs(5),
            drain_tick: Duration::from_millis(100),
            drain_ack_wait: Duration::from_secs(5),
            stop_poll: Duration::from_millis(100),
            connection_job_cancel_grace: Duration::from_secs(5),
            vault_min_renew_wait: Duration::from_secs(1),
            master_sample_window: Duration::from_mins(10),
            clock_skew_warning: Duration::from_secs(2),
            vault_renew_check: Duration::from_mins(1),
            vault_renew_retry: Duration::from_secs(10),
            round_gap: Duration::from_millis(50),
            notification_retry: Duration::from_secs(30),
            master_alive_interval: Duration::from_secs(5),
            gauge_refresh: Duration::from_secs(15),
            outbox_poll: Duration::from_secs(5),
            outbox_retry_first: Duration::from_secs(10),
            outbox_retry_max: Duration::from_secs(3600),
            retention_interval: Duration::from_hours(1),
            parse_retry_first: Duration::from_secs(30),
            parse_retry_max: Duration::from_mins(10),
            node_heartbeat: Duration::from_secs(10),
            changes_poll_interval: Duration::from_secs(1),
            events_recheck: Duration::from_mins(1),
            events_keep_alive: Duration::from_secs(15),
            events_retry: Duration::from_secs(1),
            auth_recheck: Duration::from_secs(5),
            auth_remembered: Duration::from_mins(5),
            requests_grace: Duration::from_secs(3),
            schema_retry: Duration::from_secs(5),
            ui_keep_alive: Duration::from_secs(15),
            ui_coalesce: Duration::from_secs(1),
            ui_tail_interval: Duration::from_secs(1),
            ui_tail_give_up: Duration::from_secs(30),
            ui_relay_retry_first: Duration::from_secs(1),
            ui_relay_retry_max: Duration::from_secs(30),
            worker_stop_after_failure: Duration::from_secs(10),
            accept_retry: Duration::from_millis(100),
            trigger_restart_backoff: Duration::from_secs(5),
            trigger_restart_backoff_max: Duration::from_mins(5),
            overridden: Vec::new(),
        }
    }
}

/// `[watchers]`: the worker's watcher hosts, which run wait steps.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WatchersSection {
    /// How many wait steps a worker runs at once, apart from `[worker] slots`. 0: none.
    pub slots: u32,
    /// How long a wait step may hold its host's event loop before reaching an `await`. Past it
    /// the host is killed: the wait step fails with `blocked_event_loop`, and the others on the
    /// host start again without using a retry.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub block_limit: Duration,
    /// The memory all of a worker's hosts may use together. Past it the largest is killed: its
    /// wait steps end `lost`, each using a retry.
    #[serde(
        deserialize_with = "deserialize_size",
        serialize_with = "serialize_size"
    )]
    #[schemars(with = "SizeText")]
    pub memory_budget: u64,
    /// After how many times in a row a wait step gets its host killed it is switched off: its
    /// attempts then fail at once, until it succeeds on new code or `schedr sensors switch-on`.
    /// 0: never.
    pub switch_off_after: u32,
    /// After how many calls in a row that each ran past its `every` a sensor's poke fails
    /// (`PokeTooSlowError`). A call may run on past `every` to three times it, then it is
    /// cancelled and fails at once. 1 (or 0): at the first.
    pub poke_overruns: u32,
}

impl Default for WatchersSection {
    fn default() -> Self {
        Self {
            slots: 500,
            block_limit: Duration::from_secs(30),
            memory_budget: 1024 * 1024 * 1024,
            switch_off_after: 3,
            poke_overruns: 3,
        }
    }
}

/// `[triggers]`: event triggers (`@trigger`, `@trigger.poke`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TriggersSection {
    /// How many queued event runs a DAG may have before its trigger is stopped. It starts again
    /// from its bookmark once they have all started: the events wait in the source, not in
    /// schedr's database.
    pub max_queued_events: u32,
    /// How many errors in a row make a trigger `failing`.
    pub failing_after: u32,
}

impl Default for TriggersSection {
    fn default() -> Self {
        Self {
            max_queued_events: 1000,
            failing_after: 5,
        }
    }
}

/// `[sensors]`: wait steps (`@sensor`, `@sensor.poke`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SensorsSection {
    /// How long a wait step without a `max_wait` of its own waits, before it ends with the
    /// `TimedOut` outcome.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub default_max_wait: Duration,
}

impl Default for SensorsSection {
    fn default() -> Self {
        Self {
            default_max_wait: Duration::from_hours(24),
        }
    }
}

/// The whole configuration, after loading and validation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// `[dags]`: where DAG files are and how they are parsed.
    pub dags: DagsSection,
    /// `[scripts]`: where plain scripts are.
    pub scripts: ScriptsSection,
    /// `[interpreters]`: interpreter names and their executables.
    #[schemars(with = "BTreeMap<String, InterpreterRepr>")]
    pub interpreters: BTreeMap<String, Interpreter>,
    /// `[log]`: schedr's own logs.
    pub log: LogSection,
    /// `[ops]`: the ops port (health and metrics).
    pub ops: OpsSection,
    /// `[database]`: the state database.
    pub database: DatabaseSection,
    /// `[master]`: the master's port and timing.
    pub master: MasterSection,
    /// `[mapping]`: mapped tasks (`.expand()`).
    pub mapping: MappingSection,
    /// `[backfill]`: backfills (`schedr runs backfill`).
    pub backfill: BackfillSection,
    /// `[webserver]`: the REST API (and later the UI).
    pub webserver: WebserverSection,
    /// `[ui]`: the web UI (`schedr ui`, and the one `schedr standalone` serves).
    pub ui: UiSection,
    /// `[worker]`: this worker's identity, capacity and data directory.
    pub worker: WorkerSection,
    /// `[sensors]`: wait steps (`@sensor`, `@sensor.poke`).
    pub watchers: WatchersSection,
    /// `[sensors]`.
    pub sensors: SensorsSection,
    /// Event triggers.
    pub triggers: TriggersSection,
    /// `[grpc]`: the link between the master and its workers, both ends.
    pub grpc: GrpcSection,
    /// `[events]`: events posted through the API (decision 354).
    pub events: EventsSection,
    /// `[notifications]`: what the master sends when runs and tasks end (decisions 408-429).
    pub notifications: NotificationsSection,
    /// `[retention]`: how long the database keeps runs, outputs, events and commands
    /// (decisions 430-435).
    pub retention: RetentionSection,
    /// `[tls]`: mTLS on the internal links (master ↔ worker, webserver → worker logs).
    pub tls: TlsSection,
    /// `[secret_storage]`: where a worker's tasks get their secrets (workers only).
    pub secret_storage: SecretStorageSection,
    /// The DAG roots, resolved from `dags.path` and `dags.roots` when loading.
    #[serde(skip)]
    pub dag_roots: Vec<Root>,
    /// The script roots, resolved from `scripts.path` and `scripts.roots` when loading.
    #[serde(skip)]
    pub script_roots: Vec<Root>,
    /// How the `python` interpreter was chosen when `[interpreters]` doesn't define it: set to
    /// [`BUILTIN_PYTHON_ORIGIN`] when loading, and by the binary when it looks for a better
    /// one (e.g. `project venv`); `None` when `python` is configured.
    #[serde(skip)]
    pub builtin_python: Option<String>,
    /// The fixed timings, as `SCHEDR_HIDDEN__*` left them.
    #[serde(skip)]
    pub timings: Timings,
    /// The fixed limits, as `SCHEDR_HIDDEN__*` left them.
    #[serde(skip)]
    pub limits: HiddenLimits,
}

/// Fixed limits, like [`Timings`] not configuration: only `SCHEDR_HIDDEN__*` changes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenLimits {
    /// The most records of one kind (wait step hosts, warnings) the master keeps of what a
    /// worker reports (`MASTER__WORKER_REPORT_RECORDS`, B183): an honest agent sends a few.
    pub worker_report_records: usize,
    /// The most characters of each text in them the master keeps
    /// (`MASTER__WORKER_REPORT_TEXT`).
    pub worker_report_text: usize,
    /// The most bytes a notification's body may take once filled
    /// (`MASTER__NOTIFICATION_BODY`): a template using big values many times is refused.
    pub notification_body: usize,
    /// The most runs the retention job deletes in one transaction (`MASTER__RETENTION_BATCH`,
    /// decision 432): one run can hold thousands of rows, and a transaction holds its locks.
    pub retention_batch: usize,
    /// The most messages of one worker the master handles in one turn of its lock
    /// (`MASTER__MESSAGES_PER_TURN`, M4B.3): its `Started`s and `Finished`s stored together.
    pub messages_per_turn: usize,
    /// The `SCHEDR_HIDDEN__*` variables that changed one.
    pub overridden: Vec<String>,
}

impl Default for HiddenLimits {
    /// 256 records of each kind, 4096 characters of each text, 1 MiB a notification, 100 runs
    /// a retention transaction, 40 messages a turn.
    fn default() -> Self {
        Self {
            worker_report_records: 256,
            worker_report_text: 4096,
            notification_body: 1 << 20,
            retention_batch: 100,
            messages_per_turn: 40,
            overridden: Vec::new(),
        }
    }
}

/// The JSON Schema of the configuration file: every key with its doc comment, type and
/// default (the configuration reference in the documentation is generated from it).
#[must_use]
pub fn config_schema() -> schemars::Schema {
    schemars::schema_for!(Config)
}

/// How the built-in `python` interpreter was chosen when nothing looked further.
pub const BUILTIN_PYTHON_ORIGIN: &str = "built-in default";

/// `[dags]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DagsSection {
    /// The DAG directories (`SCHEDR_DAG_PATH`).
    pub path: PathList,
    /// Root renames: name → one of the `path` entries.
    pub roots: BTreeMap<String, PathBuf>,
    /// The `[interpreters]` entry used for parsing and for Python tasks.
    pub interpreter: String,
    /// How often the roots are scanned for changes.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub poll_interval: Duration,
    /// How long a root must stay unchanged before its new content hash is computed.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub settle_time: Duration,
    /// How long one DAG file may take to import before the agent stops its runner.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub parse_file_timeout: Duration,
    /// How many DAG files are parsed at once, each in a process of its own. Capped at the cores
    /// the agent may use; 1 parses them one at a time.
    pub parse_concurrency: u32,
}

impl Default for DagsSection {
    /// No DAG directories, the `python` interpreter, polling every 30 s with a 5 s settle time,
    /// 60 s per file to import.
    fn default() -> Self {
        Self {
            path: PathList::default(),
            roots: BTreeMap::new(),
            interpreter: DEFAULT_DAG_INTERPRETER.to_owned(),
            poll_interval: Duration::from_secs(30),
            settle_time: Duration::from_secs(5),
            parse_file_timeout: Duration::from_secs(60),
            parse_concurrency: 4,
        }
    }
}

/// `[scripts]`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ScriptsSection {
    /// The script directories (`SCHEDR_SCRIPT_PATH`).
    pub path: PathList,
    /// Root renames: name → one of the `path` entries.
    pub roots: BTreeMap<String, PathBuf>,
}

/// An `[interpreters]` entry: a plain path, or a table with fork server settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(from = "InterpreterRepr", into = "InterpreterRepr")]
pub struct Interpreter {
    /// The executable.
    pub path: PathBuf,
    /// Whether Python tasks start from a fork server (M2.5; off by default).
    pub fork_server: bool,
    /// The libraries the fork server preloads.
    pub preload: Vec<String>,
}

impl Interpreter {
    /// An interpreter given by its path only (spawn, no preloading).
    #[must_use]
    pub fn path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            fork_server: false,
            preload: Vec::new(),
        }
    }
}

/// How an interpreter is written in TOML.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
enum InterpreterRepr {
    /// `python = "/usr/bin/python3"`.
    Path(PathBuf),
    /// `[interpreters.etl-venv]` with `path`, `fork_server`, `preload`.
    Table(InterpreterTable),
}

/// The table form of an interpreter.
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InterpreterTable {
    /// The executable.
    path: PathBuf,
    /// Whether Python tasks start from a fork server (M2.5; off by default).
    #[serde(default)]
    fork_server: bool,
    /// The libraries the fork server preloads.
    #[serde(default)]
    preload: Vec<String>,
}

impl From<Interpreter> for InterpreterRepr {
    /// The plain path when that says it all, else the table.
    fn from(interpreter: Interpreter) -> Self {
        if !interpreter.fork_server && interpreter.preload.is_empty() {
            Self::Path(interpreter.path)
        } else {
            Self::Table(InterpreterTable {
                path: interpreter.path,
                fork_server: interpreter.fork_server,
                preload: interpreter.preload,
            })
        }
    }
}

impl From<InterpreterRepr> for Interpreter {
    fn from(repr: InterpreterRepr) -> Self {
        match repr {
            InterpreterRepr::Path(path) => Self::path(path),
            InterpreterRepr::Table(t) => Self {
                path: t.path,
                fork_server: t.fork_server,
                preload: t.preload,
            },
        }
    }
}

/// `[log] format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// JSON when stdout isn't a terminal, readable text when it is.
    #[default]
    Auto,
    /// Always JSON.
    Json,
    /// Always readable text.
    Text,
}

/// `[log]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LogSection {
    /// The output format.
    pub format: LogFormat,
    /// The level, with per-module filters in the `RUST_LOG` style.
    pub level: String,
}

impl Default for LogSection {
    /// `auto` format at level `info`.
    fn default() -> Self {
        Self {
            format: LogFormat::Auto,
            level: "info".to_owned(),
        }
    }
}

/// `[database]`.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseSection {
    /// The connection URL (`sqlite:...` or `postgres://...`). A URL with a password must come
    /// from `SCHEDR_CONFIG__DATABASE__URL`, never the file (see archive/PROJECT.md, Configuration:
    /// secrets).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Whether the master listens for new commands on Postgres (`LISTEN`), applying them at
    /// once rather than at its next round. A connection pooler in transaction mode (`PgBouncer`
    /// or similar) can't carry it: the master checks the connection it listens through, and
    /// behind such a pooler it warns, turns notifications off and polls. Off: polling only;
    /// the webserver doesn't notify either.
    pub notifications: bool,
    /// A connection for the master's listening only, straight to Postgres past a pooler, so
    /// commands stay immediate behind one; the main URL when not set. With a password, it must
    /// come from `SCHEDR_CONFIG__DATABASE__NOTIFICATIONS_URL`, never the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notifications_url: Option<String>,
    /// Whether the writes announce what changed, for the web UI's live pages (triggers in the
    /// database; about 2-3% of the master's throughput under heavy load, measured by stress
    /// scenarios 15 and 15b). Applied by `schedr db migrate` and `--migrate`, not by a running
    /// master. Off: the pages refresh every `refresh_interval` instead.
    pub change_notifications: bool,
}

impl Default for DatabaseSection {
    /// No URL, queries limited to 10 s, notifications on (through the main URL), changes
    /// announced.
    fn default() -> Self {
        Self {
            url: None,
            notifications: true,
            notifications_url: None,
            change_notifications: true,
        }
    }
}

/// Whether a URL has a password: in its user-info part (`scheme://user:password@host`) or as
/// a query parameter naming one (`?password=...`, any case, the name percent-decoded; B203).
#[must_use]
pub fn url_has_password(url: &str) -> bool {
    // As its client reads it: `postgres:\n//u:p@db` is `postgres://u:p@db` (B216's review).
    let url = crate::masked::url_as_read(url);
    let Some((_, rest)) = url.split_once("://") else {
        return false;
    };
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let authority = rest.split('/').next().unwrap_or_default();
    let in_user_info = authority
        .rsplit_once('@')
        .is_some_and(|(user_info, _)| user_info.contains(':'));
    let query = query.split('#').next().unwrap_or_default();
    in_user_info
        || query.split('&').any(|pair| {
            pair.split_once('=').is_some_and(|(name, _)| {
                crate::masked::percent_decoded(name)
                    .to_ascii_lowercase()
                    .ends_with("password")
            })
        })
}

/// `[ops]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OpsSection {
    /// The address the ops port listens on (all interfaces by default; `schedr standalone`
    /// uses its `--listen` address, else 127.0.0.1).
    pub bind: IpAddr,
    /// The plain-HTTP port for `/healthz`, `/readyz` and `/metrics`.
    pub port: u16,
}

impl Default for OpsSection {
    /// Port 8090 of every interface and a 2 minute loop stall timeout.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8090,
        }
    }
}

/// `[mapping]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct MappingSection {
    /// The most instances a mapped task may have in a run; more fail the task, saying so
    /// (like Airflow's `max_map_length`). At least 1.
    pub max_instances: u32,
}

impl Default for MappingSection {
    /// 1024 instances.
    fn default() -> Self {
        Self {
            max_instances: 1024,
        }
    }
}

/// `[backfill]`: backfills, a run per fire time of a range (`schedr runs backfill`, decisions
/// 363, 400-404).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct BackfillSection {
    /// The most fire times one backfill's range may have, those already run included (so
    /// it is also the most runs it creates); a bigger range is refused, saying to split it.
    /// At least 1.
    pub max_runs: u32,
}

impl Default for BackfillSection {
    /// 1000 runs: a year of a daily DAG, a month of an hourly one.
    fn default() -> Self {
        Self { max_runs: 1000 }
    }
}

/// `[retention]`: how long the database keeps what runs left behind (decisions 430-435). The
/// master deletes what is older every hour, in batches; never an unfinished run, nor a DAG's
/// newest scheduled run, its newest run, or the last that ended `success` or `failed`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionSection {
    /// Finished runs older than this (by their end) are deleted with their tasks, attempts and
    /// outputs; `"forever"` keeps them.
    pub runs: Age,
    /// A finished run's outputs older than this are deleted, its tasks and their outcomes
    /// kept; the same as `runs` by default. Longer than `runs` changes nothing.
    pub outputs: Option<Age>,
    /// Events triggers found and events posted through the API older than this are deleted;
    /// the same as `runs` by default.
    pub events: Option<Age>,
    /// Commands (the audit trail) decided longer ago than this are deleted, whatever their
    /// runs' age.
    pub commands: Age,
}

impl Default for RetentionSection {
    /// 90 days of runs (their outputs and events the same), 365 days of commands.
    fn default() -> Self {
        Self {
            runs: Age::For(Duration::from_hours(90 * 24)),
            outputs: None,
            events: None,
            commands: Age::For(Duration::from_hours(365 * 24)),
        }
    }
}

impl RetentionSection {
    /// How long outputs are kept: `outputs`, else `runs`.
    #[must_use]
    pub fn outputs(&self) -> Age {
        self.outputs.unwrap_or(self.runs)
    }

    /// How long events are kept: `events`, else `runs`.
    #[must_use]
    pub fn events(&self) -> Age {
        self.events.unwrap_or(self.runs)
    }
}

/// How long something is kept: a duration (`"90d"`) or `"forever"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Age {
    /// Deleted once older than this.
    For(Duration),
    /// Never deleted.
    Forever,
}

impl Serialize for Age {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::For(duration) => serialize_duration(duration, s),
            Self::Forever => s.serialize_str("forever"),
        }
    }
}

impl<'de> Deserialize<'de> for Age {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        if text == "forever" {
            return Ok(Self::Forever);
        }
        let duration = crate::units::parse_duration(&text).map_err(serde::de::Error::custom)?;
        if duration.is_zero() {
            return Err(serde::de::Error::custom(
                "a zero age would delete everything; \"forever\" keeps it",
            ));
        }
        Ok(Self::For(duration))
    }
}

impl JsonSchema for Age {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Age".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A duration such as `90d` or `12h`, or `forever`.",
            "examples": ["90d", "forever"]
        })
    }
}

/// `[notifications]`: messages the master sends to channels when runs and tasks end, and
/// those tasks send themselves (decisions 408-429).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationsSection {
    /// The UI's address (`https://schedr.example.com`), the base of the links in messages;
    /// none: messages carry no links.
    pub ui_url: Option<String>,
    /// The events a channel named without events is told of (`RUN_FAILED`, ...).
    pub default_events: Vec<String>,
    /// How long the master keeps retrying a notification it can't deliver; then it is logged
    /// as an error, counted and dropped.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub give_up_after: Duration,
    /// The limit for one delivery (a POST); past it, the delivery failed and is retried.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub timeout: Duration,
    /// The most notifications one task attempt may send itself (`send`); past it, `send`
    /// fails. At least 1.
    pub max_per_attempt: u32,
    /// The URLs a DAG's own webhook (`WebhookChannel`) may post to: its URL must start with
    /// one of them (`https://relay.example.com/`); empty: DAGs define none (decision 425).
    pub dag_urls: Vec<String>,
    /// A PEM file of CAs trusted for notifications besides the system's (a private CA, such
    /// as `OpenShift`'s service CA).
    pub ca_file: Option<PathBuf>,
    /// The server's channels, by name (`[notifications.channels.ops-slack]`): open to every
    /// DAG by its name.
    pub channels: BTreeMap<String, ChannelConfig>,
    /// `[notifications.smtp]`: the mail server every email channel sends through.
    pub smtp: SmtpSection,
}

/// `[notifications.smtp]`: the mail server email channels send through (decisions 411, 426).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SmtpSection {
    /// The server's host; none: no email channel can be configured.
    pub host: Option<String>,
    /// Its port: 587 (submission, STARTTLS) by default; 465 for `tls = "tls"`.
    pub port: u16,
    /// How the connection is secured: `starttls` (upgrade, required), `tls` (TLS from the
    /// start) or `none` (plain text: a local relay only).
    pub tls: SmtpTls,
    /// The user to log in as; none: no login.
    pub username: Option<String>,
    /// Its password: only from `SCHEDR_CONFIG__NOTIFICATIONS__SMTP__PASSWORD`, never the
    /// file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// The sender (`schedr <schedr@example.com>`).
    pub from: Option<String>,
}

impl std::fmt::Debug for DatabaseSection {
    /// The URLs with their passwords masked (B206).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let masked = |url: &Option<String>| url.as_deref().map(crate::masked::mask_url);
        f.debug_struct("DatabaseSection")
            .field("url", &masked(&self.url))
            .field("notifications", &self.notifications)
            .field("notifications_url", &masked(&self.notifications_url))
            .field("change_notifications", &self.change_notifications)
            .finish()
    }
}

impl std::fmt::Debug for ChannelConfig {
    /// Without its URL, which may be a capability (Slack's), or its body (B206).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelConfig")
            .field("kind", &self.kind)
            .field("url", &self.url.as_ref().map(|_| "****"))
            .field("headers_env", &self.headers_env)
            .field("body", &self.body.as_ref().map(|_| "****"))
            .field("to", &self.to)
            .finish()
    }
}

impl std::fmt::Debug for SmtpSection {
    /// Without the password (Codex's review of M4.13b).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpSection")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "****"))
            .field("from", &self.from)
            .finish()
    }
}

impl Default for SmtpSection {
    /// No server; port 587 with STARTTLS.
    fn default() -> Self {
        Self {
            host: None,
            port: 587,
            tls: SmtpTls::Starttls,
            username: None,
            password: None,
            from: None,
        }
    }
}

/// How an SMTP connection is secured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SmtpTls {
    /// A plain connection upgraded with STARTTLS, which the server must offer.
    Starttls,
    /// TLS from the start (port 465).
    Tls,
    /// No TLS: for a relay on the same host or network only.
    None,
}

impl Default for NotificationsSection {
    /// No links, `RUN_FAILED`, giving up after 24 h, 10 s a delivery, 100 sends an attempt,
    /// no DAG webhooks, no channels.
    fn default() -> Self {
        Self {
            ui_url: None,
            default_events: vec!["RUN_FAILED".to_owned()],
            give_up_after: Duration::from_hours(24),
            timeout: Duration::from_secs(10),
            max_per_attempt: 100,
            dag_urls: Vec::new(),
            ca_file: None,
            channels: BTreeMap::new(),
            smtp: SmtpSection::default(),
        }
    }
}

/// A server channel (`[notifications.channels.<name>]`, decision 424): its `type`, and the keys
/// that type takes (the master refuses another type's).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelConfig {
    /// `webhook` (an HTTP POST of JSON) or `email`.
    #[serde(rename = "type")]
    pub kind: ChannelKind,
    /// A webhook's URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// A webhook's headers whose values are read from environment variables, header to
    /// variable (`{ Authorization = "OPS_HOOK_TOKEN" }`); a secret is never written in the file.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers_env: BTreeMap<String, String>,
    /// A webhook's JSON, with `{name}` placeholders in its strings (`{{` for a brace); none:
    /// schedr's fixed JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Map<String, serde_json::Value>>")]
    pub body: Option<serde_json::Value>,
    /// An email channel's recipients.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
}

/// A server channel's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    /// An HTTP POST of JSON (decisions 412, 417, 427).
    Webhook,
    /// An email, through `[notifications.smtp]` (decision 426).
    Email,
}

/// `[master]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct MasterSection {
    /// The address `schedr master` listens on (all interfaces by default; `schedr standalone`
    /// always listens on 127.0.0.1).
    pub bind: IpAddr,
    /// The gRPC port workers connect to.
    pub grpc_port: u16,
    /// The time between SIGTERM and SIGKILL when an attempt is cancelled.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub cancel_grace: Duration,
    /// How long a task's code may be missing on every eligible worker (git-sync not there
    /// yet) before the task is `unschedulable`.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub code_wait: Duration,
    /// How long a worker has for a parse job before it goes to another worker (or the same one
    /// again, if no other has that code). Raise it for DAG folders so large that parsing one
    /// takes longer; each file still has `[dags] parse_file_timeout`.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub parse_job_timeout: Duration,
    /// Whether rounds visit only busy DAGs and the quiet ones they wake (a run queued or
    /// ended, a command, a new version, a fire time coming up), instead of every DAG every
    /// round. `false` visits every DAG every round.
    pub quiet_dags: bool,
}

impl Default for MasterSection {
    /// All interfaces, port 8070, messages up to 64 MiB, 30 s grace, 10 min for missing code,
    /// 10 min for a parse job; a
    /// task set aside for 60 s, listening tried again every 30 min; quiet DAGs on, quiet 60 s
    /// after a run, woken 60 s ahead, every DAG visited within 60 s.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            grpc_port: 8070,
            cancel_grace: Duration::from_secs(30),
            parse_job_timeout: Duration::from_mins(10),
            code_wait: Duration::from_mins(10),
            quiet_dags: true,
        }
    }
}

/// `[webserver]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WebserverSection {
    /// The address `schedr webserver` listens on (all interfaces by default; `schedr
    /// standalone` always listens on 127.0.0.1).
    pub bind: IpAddr,
    /// The HTTP port of the REST API (and later the UI).
    pub port: u16,
    /// The port serving the whole API to `schedr ui` alone, its calls there passing the user's
    /// name on (decision 36): in `[tls] mode = "app"` over mTLS (its certificate must name
    /// `[tls] ui_name`); in `mesh` mode only with `ui_port_in_mesh`. Elsewhere the UI proves
    /// itself by `ui_token_file`.
    pub ui_port: u16,
    /// In `[tls] mode = "mesh"`: serve `ui_port` in plain HTTP, trusting the mesh to let only
    /// `schedr ui` reach it (Istio's `AuthorizationPolicy` admitting only its account: decision
    /// 40A), as the mesh is trusted with the master's and workers' links. Nothing in schedr
    /// guards that port then: without such a policy, whoever reaches it could name any user.
    pub ui_port_in_mesh: bool,
    /// A file holding the token `schedr ui` presents (`X-Schedr-Ui-Token`) to prove its calls,
    /// where there is no mTLS: `mesh` mode, and standalone (decision 36); mounted from a
    /// Secret. None: no caller is trusted that way.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_token_file: Option<PathBuf>,
    /// How long a whole request to a worker may take, to fetch a task's log. Raise it when
    /// large logs come slowly; a user waits this long at most for a worker that hangs.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub worker_request_timeout: Duration,
    /// How long a connection test may take when the request doesn't say. Raise it for
    /// connections slow to open; `schedr connections test --timeout` sets it for one test.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub connection_test_timeout: Duration,
    /// `[webserver.auth]`: who the user is.
    pub auth: AuthSection,
}

impl Default for WebserverSection {
    /// All interfaces, port 8080, live pages refreshed every 15 s when changes can't be
    /// followed.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8080,
            worker_request_timeout: Duration::from_secs(30),
            connection_test_timeout: Duration::from_mins(1),
            ui_port: 8083,
            ui_port_in_mesh: false,
            ui_token_file: None,
            auth: AuthSection::default(),
        }
    }
}

/// `[ui]`: the web UI. `schedr ui` is a role of its own that talks only to webservers' REST
/// APIs, one per deployment in `deployments` (one the common case); `schedr standalone` serves
/// the UI too, next to its API, over its own deployment (`deployments` is not used there).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UiSection {
    /// The address `schedr ui` listens on (all interfaces by default).
    pub bind: IpAddr,
    /// The HTTP port of `schedr ui`.
    pub port: u16,
    /// The deployments `schedr ui` shows, in this order: each a name (shown, and in the UI's
    /// URLs) and its webserver's URL.
    pub deployments: Vec<UiDeployment>,
    /// The documentation site served at `/docs` (the image's); served only when the directory
    /// exists, `""` for none.
    pub docs_dir: PathBuf,
    /// `[ui.auth]`: who the person using the UI is (as `[webserver.auth]`).
    pub auth: AuthSection,
}

impl Default for UiSection {
    /// All interfaces, port 8082, no deployments, the image's documentation.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8082,
            deployments: Vec::new(),
            docs_dir: PathBuf::from("/usr/share/schedr/docs"),
            auth: AuthSection::default(),
        }
    }
}

/// A deployment `schedr ui` shows (`[[ui.deployments]]`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UiDeployment {
    /// Its name: letters, digits, `-` and `_` (it is in the UI's URLs).
    pub name: String,
    /// Its webserver's URL, e.g. `http://schedr-webserver.etl.svc:8080`; with
    /// `https://`, its `[webserver] ui_port` over mTLS (this role's `[tls]` certificate).
    pub url: String,
    /// A file holding the token this UI presents to it (its `[webserver] ui_token_file`'s),
    /// where there is no mTLS: a webserver in `mesh` mode, a standalone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
}

/// `[webserver.auth]` and `[ui.auth]`: who the user is (see archive/PROJECT.md, Authentication: as
/// little as possible in schedr). The name is recorded on commands, never used to allow or
/// deny anything; there are no roles.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AuthSection {
    /// How the user is known.
    pub mode: AuthMode,
    /// `none`: a header naming the user, recorded if present (e.g. `x-forwarded-user`, set
    /// by a mesh from its token); `proxy_header`: the header the SSO proxy sets, required
    /// (`x-forwarded-user` when not set).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_header: Option<String>,
    /// `basic`: a file of `name:bcrypt-hash` lines (mounted from a Secret; never in the
    /// configuration itself).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users_file: Option<PathBuf>,
    /// `basic` beyond a loopback address: TLS is ended by a proxy in front (passwords would
    /// otherwise cross the network in the clear: refused without it).
    pub behind_tls_proxy: bool,
    /// `proxy_header` beyond a loopback address: everything reaching it comes through the
    /// proxy (anything reaching it around the proxy could name any user: refused without it).
    pub allow_remote: bool,
}

/// `[webserver.auth] mode`, `[ui.auth] mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// Whoever reaches it: access is decided in front (a mesh, the namespace).
    #[default]
    None,
    /// An SSO proxy in front logs users in and names them in a header.
    ProxyHeader,
    /// HTTP Basic against `users_file`.
    Basic,
}

/// `[worker]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerSection {
    /// The worker's ID, stable across restarts; the host name when not set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Task slots.
    pub slots: u32,
    /// The queues served; none serves `default`.
    pub queues: Vec<String>,
    /// Where task logs, the journal (`journal/<id>`; keep it across restarts), the copies of
    /// the DAG roots' code that parse jobs and tasks import from (`code/<id>`, one or two per
    /// root, read-only) and (in standalone) the SQLite database go; `$XDG_DATA_HOME/schedr`
    /// (else `~/.local/share/schedr`) when not set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
    /// Where commands' connection files go while their attempts run (`.pgpass`, SSH keys:
    /// secrets as files their tools read), in a folder per worker; the worker's user's own,
    /// mode 0700, or the worker refuses to start. In memory when not set: under `/dev/shm`
    /// where it exists, else `connections/` in `data_dir`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_dir: Option<PathBuf>,
    /// Agent environment variables passed to tasks besides the allow-list.
    pub pass_env: Vec<String>,
    /// The master's gRPC URL, e.g. `https://schedr-master:8070` (`http://` with `[tls] mode =
    /// "mesh"`); required by `schedr worker`
    /// (`schedr standalone` connects to its own master).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub master_url: Option<String>,
    /// How long running tasks get when the worker is asked to stop (SIGTERM): match the
    /// pod's `terminationGracePeriodSeconds`.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub termination_grace_period: Duration,
    /// For a task still running when a drain's time is up: from its SIGTERM to its SIGKILL
    /// (the drain's own, instead of `[master] cancel_grace`). The running tasks get
    /// `termination_grace_period` minus this and `drain_report_period`, whose sum must not
    /// exceed it.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub drain_terminate_period: Duration,
    /// The address the log endpoint listens on (all interfaces by default; `schedr
    /// standalone` always listens on 127.0.0.1).
    pub log_bind: IpAddr,
    /// The log endpoint's port (0: a free one).
    pub log_port: u16,
    /// The host the webserver reaches the log endpoint at, sent to the master (e.g. the pod's
    /// DNS name through a headless Service); the host name when not set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_address: Option<String>,
    /// Task logs older than this are deleted.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub log_retention: Duration,
    /// Beyond this many bytes of logs the oldest are deleted; no limit when not set.
    #[serde(
        deserialize_with = "deserialize_optional_size",
        serialize_with = "serialize_optional_size",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SizeText>")]
    pub log_max_size: Option<u64>,
    /// The mode of the log files the worker creates (task, attempt, parse and watcher-host
    /// logs), in octal: owner-only `"0600"` by default, as the journal and connection files. A
    /// log shipper reading them under another uid needs them opened on purpose: `"0640"` for
    /// one in the worker's group. At most `"0644"`, and the owner always reads and writes.
    #[serde(deserialize_with = "deserialize_mode_text")]
    pub log_mode: String,
    /// Each task's memory budget; when not set, (the pod's limit − 10 %) ÷ slots.
    #[serde(
        deserialize_with = "deserialize_optional_size",
        serialize_with = "serialize_optional_size",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SizeText>")]
    pub memory_budget: Option<u64>,
    /// The part of the pod's memory limit kept out of task budgets (for schedr's own process):
    /// a percentage (`25%`) or a size (`1GiB`); when not set, 10 % for `schedr worker` and 25 %
    /// for `schedr standalone` (whose process also runs the master and the API). Not used when
    /// `memory_budget` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_reserve: Option<MemoryReserve>,
    /// How long a started Python runner has to say hello. Raise it where the interpreter is
    /// slow to start (cold storage, a loaded machine); a runner that can't start is noticed
    /// this much later.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub runner_start_timeout: Duration,
    /// How long a fork server may take to import its preloaded libraries. Raise it for large
    /// preload lists or slow storage; until it is up, tasks are spawned instead.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub fork_server_preload_timeout: Duration,
    /// How long one try of a cleanup may run, for a task that doesn't set `cleanup_timeout`.
    /// The attempt holds its slot meanwhile: raise it if your cleanups wait on slow systems,
    /// lower it to free slots sooner.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub cleanup_timeout: Duration,
    /// How long one try of a resource's cleanup may run, for one that doesn't set `timeout`.
    /// It ends a cleanup hung on a system that never answers, which can't be cancelled with
    /// its run and holds the run (and its `max_active_runs` slot) until it ends. Raise it for
    /// teardowns that take longer (a cluster's deletion), lower it to notice a hang sooner.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub resource_cleanup_timeout: Duration,
    /// How long a fork server whose preloaded library fails to import is retried (with
    /// backoff, from the first failure), for what isn't ready when the pod starts (a
    /// sidecar's network, a mount); `0s`: not retried. Threads after preloading and a version
    /// mismatch are never retried (they don't change until the image does).
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub fork_server_import_retry: Duration,
}

impl Default for WorkerSection {
    /// The host name, 2 slots, the `default` queue, the default data directory, 30 s of grace
    /// (Kubernetes' default) of which the last 5 s stop and report what still runs (3 s to
    /// SIGKILL, 2 s to report), the log endpoint on port 8081 of every interface, logs kept for
    /// 30 days and mode 0600, the memory budget from the pod's limit, sampled every second, 30 s for a runner
    /// to start, 5 minutes for a fork server to preload and for a cleanup's try, 30 minutes for
    /// a resource cleanup's try, a fork server's failing import retried for 20 minutes.
    fn default() -> Self {
        Self {
            id: None,
            slots: 2,
            queues: Vec::new(),
            data_dir: None,
            connection_dir: None,
            pass_env: Vec::new(),
            termination_grace_period: Duration::from_secs(30),
            drain_terminate_period: Duration::from_secs(3),
            master_url: None,
            log_bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            log_port: 8081,
            log_address: None,
            log_retention: Duration::from_hours(24 * 30),
            log_max_size: None,
            log_mode: "0600".to_owned(),
            memory_budget: None,
            memory_reserve: None,
            runner_start_timeout: Duration::from_secs(30),
            fork_server_preload_timeout: Duration::from_mins(5),
            cleanup_timeout: Duration::from_mins(5),
            resource_cleanup_timeout: Duration::from_mins(30),
            fork_server_import_retry: Duration::from_mins(20),
        }
    }
}

/// `[tls] mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    /// schedr's own mTLS on every internal link.
    #[default]
    App,
    /// Plain traffic: a service mesh (e.g. Istio) provides mTLS between the pods.
    Mesh,
}

/// `[events]`: events posted through the API (`POST /api/v1/events`, decision 354).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EventsSection {
    /// The largest event's data, as JSON; a bigger one is refused (`413`).
    #[serde(
        deserialize_with = "deserialize_size",
        serialize_with = "serialize_size"
    )]
    #[schemars(with = "SizeText")]
    pub max_size: u64,
}

impl Default for EventsSection {
    fn default() -> Self {
        Self {
            max_size: 1024 * 1024,
        }
    }
}

/// `[grpc]`: the link between the master and its workers. Set the same on both: each end reads
/// its own.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct GrpcSection {
    /// The largest message either end takes from the other: a root's parse upload (every DAG it
    /// defines, in one message) or a `Finished` with its output, to the master; an assignment
    /// carrying another DAG's run result (`wait_for=`), to a worker. A larger upload is refused
    /// and the worker logs it; a larger message on the stream ends it; a run result too big for
    /// it fails the call that would carry it. From 4 MiB (gRPC's default) to 1 GiB.
    #[serde(
        deserialize_with = "deserialize_size",
        serialize_with = "serialize_size"
    )]
    #[schemars(with = "SizeText")]
    pub max_message_size: u64,
}

impl Default for GrpcSection {
    fn default() -> Self {
        Self {
            max_message_size: 64 * 1024 * 1024,
        }
    }
}

/// `[tls]`: certificates are PEM files (mounted from Kubernetes Secrets), read again when they
/// change.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TlsSection {
    /// Whether schedr does mTLS itself (`app`) or leaves it to a mesh (`mesh`).
    pub mode: TlsMode,
    /// The CA certificates peers' certificates must be signed by.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<PathBuf>,
    /// This role's certificate (chain).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert: Option<PathBuf>,
    /// This role's private key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<PathBuf>,
    /// The name the webserver expects in workers' certificates, whatever address it reaches
    /// them at.
    pub worker_name: String,
    /// The name a webserver expects in `schedr ui`'s certificate on its `[webserver] ui_port`
    /// (decision 36: only a caller so proven may pass a user's name on).
    pub ui_name: String,
    /// How often the files are read again for a change.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub reload_interval: Duration,
}

impl Default for TlsSection {
    /// `app` mode, no files yet, workers named `schedr-worker`, the UI `schedr-ui`, files read
    /// every 30 s, 10 s for a handshake.
    fn default() -> Self {
        Self {
            mode: TlsMode::App,
            ca: None,
            cert: None,
            key: None,
            worker_name: "schedr-worker".to_owned(),
            ui_name: "schedr-ui".to_owned(),
            reload_interval: Duration::from_secs(30),
        }
    }
}

/// `[secret_storage]`: where a worker's tasks get their secrets (only workers fetch them; see
/// archive/PROJECT.md, Secret storage). Each storage is a table of its own, named by its key
/// (`[secret_storage.vault]`); standalone always has the `db` storage besides them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default)]
pub struct SecretStorageSection {
    /// The storage `secrets['...']` means: one configured below, or `db` in standalone. Unset:
    /// the only storage configured, else `db` in standalone (none on a worker).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Values shorter than this many characters aren't masked in logs and outputs (masking
    /// `1` would mangle them); the task's log says which.
    pub min_mask_length: usize,
    /// The storages, by name.
    #[serde(flatten)]
    pub storages: BTreeMap<String, SecretStorageConfig>,
}

impl Default for SecretStorageSection {
    /// No storages, values from 5 characters masked.
    fn default() -> Self {
        Self {
            default: None,
            min_mask_length: 5,
            storages: BTreeMap::new(),
        }
    }
}

/// A secret storage's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SecretStorageType {
    /// The `secrets` table of schedr's database, encrypted under `SCHEDR_SECRET_KEY`
    /// (standalone only).
    Db,
    /// Vault's KV v2 (`HashiCorp` Vault).
    Vault,
}

/// `[secret_storage.<name>]`: one secret storage. Its `Debug` hides the token.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretStorageConfig {
    /// Its kind: `db` or `vault`.
    #[serde(rename = "type")]
    pub kind: SecretStorageType,
    /// Vault's address, e.g. `https://vault.example.com:8200` (`vault`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// The KV v2 mount (`vault`; default `secret`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount: Option<String>,
    /// The Vault Enterprise namespace (`vault`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// A PEM file of the CA certificates Vault's certificate is checked against, instead of
    /// the system's (`vault`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert: Option<PathBuf>,
    /// How the agent logs in to Vault (`vault`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<VaultAuthConfig>,
    /// The Vault token for `auth = { method = "token" }`: only from
    /// `SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__TOKEN`, never the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// How long one request to Vault may take (`vault`). Raise it for a slow or distant Vault;
    /// a task reading a secret waits this long at most for one that hangs.
    #[serde(
        default = "default_vault_request_timeout",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub request_timeout: Duration,
    /// How long connecting to Vault may take (`vault`). Raise it on a slow network.
    #[serde(
        default = "default_vault_connect_timeout",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub connect_timeout: Duration,
    /// How long a fetched value is kept in the agent's memory (never on disk); `0s` fetches
    /// every time.
    #[serde(
        default = "default_cache_ttl",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub cache_ttl: Duration,
    /// Path prefixes whose values aren't masked (e.g. `["variables/"]`: settings, not
    /// credentials).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unmasked_prefixes: Vec<String>,
}

impl std::fmt::Debug for SecretStorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretStorageConfig")
            .field("kind", &self.kind)
            .field("address", &self.address)
            .field("mount", &self.mount)
            .field("namespace", &self.namespace)
            .field("ca_cert", &self.ca_cert)
            .field("auth", &self.auth)
            .field("token", &self.token.as_ref().map(|_| crate::masked::MASK))
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("cache_ttl", &self.cache_ttl)
            .field("unmasked_prefixes", &self.unmasked_prefixes)
            .finish()
    }
}

/// How the agent logs in to Vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VaultAuthMethod {
    /// A token from the environment (`SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__TOKEN`): local
    /// development.
    Token,
    /// Vault's Kubernetes auth method, with the pod's service-account token.
    Kubernetes,
}

/// `auth = { ... }` of a Vault storage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VaultAuthConfig {
    /// `token` or `kubernetes`.
    pub method: VaultAuthMethod,
    /// The Vault role (`kubernetes`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// The auth method's mount (`kubernetes`; default `kubernetes`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mount: Option<String>,
    /// The service-account token's file (`kubernetes`; default
    /// `/var/run/secrets/kubernetes.io/serviceaccount/token`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
}

const fn default_vault_request_timeout() -> Duration {
    Duration::from_secs(30)
}

const fn default_vault_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

const fn default_cache_ttl() -> Duration {
    Duration::from_secs(60)
}

/// `[worker] log_mode` as a mode: octal, within 0644, with the owner's read and write; `None`
/// for one that isn't.
#[must_use]
pub fn log_mode(text: &str) -> Option<u32> {
    let digits = text.strip_prefix("0o").unwrap_or(text);
    if digits.is_empty() || digits.len() > 4 {
        return None;
    }
    let mode = u32::from_str_radix(digits, 8).ok()?;
    (mode & !0o644 == 0 && mode & 0o600 == 0o600).then_some(mode)
}
