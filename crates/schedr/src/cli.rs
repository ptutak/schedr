//! The command-line interface: the server roles, `db`, the user CLI and the in-task CLI
//! (see archive/PROJECT.md, Web UI and CLI; In-task CLI).

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[cfg(test)]
mod tests;

/// The `schedr` command line.
#[derive(Debug, Parser)]
#[command(
    name = "schedr",
    version,
    about = "A job scheduler for Python scripts, Bash and DAGs."
)]
pub struct Cli {
    /// The configuration file (otherwise `SCHEDR_CONFIG_FILE`, then /etc/schedr/config.toml).
    #[arg(long, global = true, value_name = "PATH")]
    pub config: Option<PathBuf>,
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The top-level commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the master: scheduling loop, state in the database, internal API for workers.
    Master {
        /// Migrate the database at start, holding the master's lease, when its schema isn't up
        /// to date, instead of refusing it: for a deployment that can't migrate as a step of
        /// its own (e.g. an init container without a mesh's sidecar).
        #[arg(long)]
        migrate: bool,
    },
    /// Run a worker agent.
    Worker,
    /// Run the webserver: the REST API used by the web UI and the CLI.
    Webserver,
    /// Run the web UI over webservers' APIs (`[ui] deployments`).
    Ui,
    /// Run every role in one process (SQLite or Postgres): local development, or one pod in
    /// production.
    Standalone {
        /// The address the REST API listens on (default 127.0.0.1); e.g. 0.0.0.0 in a pod
        /// behind a Service. The master's gRPC stays on loopback.
        #[arg(long, value_name = "ADDRESS")]
        listen: Option<std::net::IpAddr>,
        /// Migrate the database at start (as `schedr db migrate` does) instead of refusing a
        /// schema that isn't up to date: for a laptop. In production, migrate as a step of its
        /// own (the Helm chart's init container).
        #[arg(long)]
        migrate: bool,
        /// Serve only the API on its port, not the web UI: one deployment of a `schedr ui`
        /// elsewhere (a federation of standalones).
        #[arg(long)]
        no_ui: bool,
    },
    /// Manage the database schema.
    Db {
        /// The database command.
        #[command(subcommand)]
        command: DbCommand,
    },
    /// Set, list, read and export the secrets of standalone's `db` storage (straight in the
    /// database, as `db` is; values encrypted under `SCHEDR_SECRET_KEY`).
    Secrets {
        /// The secrets command.
        #[command(subcommand)]
        command: SecretsCommand,
    },
    /// List, pause, check and test DAGs.
    Dags {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The DAG command.
        #[command(subcommand)]
        command: DagsCommand,
    },
    /// Trigger, list, show, cancel and backfill runs.
    Runs {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The run command.
        #[command(subcommand)]
        command: RunsCommand,
    },
    /// Cancel or retry single tasks of a run, or read their attempts, logs and outputs.
    Tasks {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The task command.
        #[command(subcommand)]
        command: TasksCommand,
    },
    /// Check a deployment's connections, or test one, on a worker.
    Connections {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The connections command.
        #[command(subcommand)]
        command: ConnectionsCommand,
    },
    /// Wait steps switched off for getting their host killed.
    Sensors {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The sensor command.
        #[command(subcommand)]
        command: SensorsCommand,
    },
    /// Event triggers: see them, and start them again.
    Triggers {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The sensor command.
        #[command(subcommand)]
        command: TriggersCommand,
    },
    /// Events posted for `EventNamed` triggers and waiting steps (decision 354).
    Events {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The events command.
        #[command(subcommand)]
        command: EventsCommand,
    },
    /// A run's resources.
    Resources {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The resource command.
        #[command(subcommand)]
        command: ResourcesCommand,
    },
    /// Drain and resume workers.
    Workers {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The worker command.
        #[command(subcommand)]
        command: WorkersCommand,
    },
    /// Inside a running task: cleanup handles, resource values, declared secrets.
    TaskCtx {
        /// The task context command.
        #[command(subcommand)]
        command: TaskCtxCommand,
    },
    /// Inside a running task: the run's parameters and run information.
    DagCtx {
        /// The DAG context command.
        #[command(subcommand)]
        command: DagCtxCommand,
    },
    /// Internal, started by a worker: caps the process's data (`RLIMIT_DATA`) and runs the
    /// task's program in its place (`hard_memory_limit=True`, decisions 50, 438).
    #[command(hide = true)]
    TaskExec {
        /// The cap, in bytes.
        #[arg(long)]
        data_limit: u64,
        /// The program and its arguments, after `--`.
        #[arg(last = true, required = true, value_parser = clap::value_parser!(std::ffi::OsString))]
        command: Vec<std::ffi::OsString>,
    },
}

/// `schedr db ...`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum DbCommand {
    /// Apply the migrations not applied yet, holding the master's lease (refused while a
    /// master holds it).
    Migrate {
        /// Wait until no master holds the lease (one stopped, or dead and its lease expiring)
        /// instead of refusing.
        #[arg(long)]
        wait: bool,
    },
    /// Show whether the schema fits this version of schedr.
    Status,
}

/// `schedr secrets ...`: the `db` secret storage (see archive/PROJECT.md, Secret storage). Only
/// `get` prints a value.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum SecretsCommand {
    /// Set keys of the secret at PATH (made if there is none); its other keys are kept.
    Set {
        /// The secret's path, e.g. `etl/db`.
        path: String,
        /// `key=value`; `key=@FILE` for a file's content (exactly), `key=-` for standard input
        /// (one trailing newline dropped). A value starting with `@` is set with `-`.
        #[arg(required = true, value_name = "KEY=VALUE")]
        values: Vec<String>,
    },
    /// List the secrets' paths and key names (never the values).
    List {
        /// Only the paths starting with it, e.g. `etl/`.
        prefix: Option<String>,
    },
    /// Print one value (`path#key`), or a whole secret as a JSON object (`path`).
    Get {
        /// `path#key` or `path`.
        secret: String,
    },
    /// Delete the secret at PATH, all its keys.
    Delete {
        /// The secret's path.
        path: String,
    },
    /// Write every secret for Vault: `DIR/<path>.json` each (mode 0600), loaded with `vault
    /// kv put secret/<path> @DIR/<path>.json`. DIR must be empty or missing.
    Export {
        /// The directory to write (made if missing).
        #[arg(long, value_name = "DIR")]
        to: PathBuf,
    },
    /// Print a new random key for `SCHEDR_SECRET_KEY`.
    NewKey,
}

/// How the user CLI reaches the webserver (credentials are only sent, never validated here).
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct ClientArgs {
    /// The webserver's URL.
    #[arg(
        long,
        env = "SCHEDR_SERVER",
        default_value = "http://localhost:8080",
        global = true
    )]
    pub server: String,
    /// A bearer token (e.g. from Keycloak).
    #[arg(long, env = "SCHEDR_TOKEN", global = true, hide_env_values = true)]
    pub token: Option<String>,
    /// A user name for HTTP Basic (the password comes from `SCHEDR_PASSWORD`).
    #[arg(long, env = "SCHEDR_USER", global = true)]
    pub user: Option<String>,
    /// Print JSON instead of human-readable output.
    #[arg(long, global = true)]
    pub json: bool,
}

/// `schedr dags ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
#[expect(
    clippy::large_enum_variant,
    reason = "parsed once, at the start of a command"
)]
pub enum DagsCommand {
    /// List DAGs.
    List,
    /// Stop the scheduler creating runs of a DAG.
    Pause {
        /// The DAG's ID.
        dag: String,
    },
    /// Let the scheduler create runs of a DAG again.
    Unpause {
        /// The DAG's ID.
        dag: String,
    },
    /// Parse DAG files and `schedr.toml` files locally and report errors.
    Check {
        /// A file or directory.
        path: PathBuf,
        /// Fail if an active worker at this server runs an older schedr (major.minor) than this
        /// one.
        #[arg(long, value_name = "SERVER_URL")]
        against: Option<String>,
    },
    /// Run one real run of a DAG locally and report each task (no server).
    Test {
        /// The DAG's ID.
        dag: String,
        /// The moment the run is for (UTC without an offset).
        #[arg(long = "for", value_name = "TIME")]
        planned_for: Option<String>,
        /// A run parameter: `key=value` or `key:=json`.
        #[arg(short = 'p', long = "param", value_name = "FIELD")]
        params: Vec<String>,
        /// All run parameters as one JSON object.
        #[arg(long = "params", value_name = "JSON", conflicts_with = "params")]
        params_json: Option<String>,
        /// A secret value for the run: `path#key=value`.
        #[arg(long = "secret", value_name = "SECRET")]
        secrets: Vec<String>,
        /// For a DAG a trigger starts: the event's key.
        #[arg(long, value_name = "KEY")]
        event_key: Option<String>,
        /// And its data: JSON, or `@file.json`.
        #[arg(long, value_name = "JSON", requires = "event_key")]
        event_data: Option<String>,
        /// And when it happened (as `--for`); the run's moment otherwise.
        #[arg(long, value_name = "TIME", requires = "event_key")]
        event_at: Option<String>,
        /// A connection's values for the run, given instead of its own: `name={"param":
        /// value}` (stored JSON); its function runs with them.
        #[arg(long = "connection", value_name = "CONNECTION")]
        connections: Vec<String>,
        /// Run only this task and its upstream tasks.
        #[arg(long, value_name = "TASK_ID")]
        task: Option<String>,
        /// The DAG files: a directory (a DAG root) or one file; default: the configured DAG
        /// roots (`SCHEDR_DAG_PATH`), else the current directory.
        #[arg(long, value_name = "PATH")]
        path: Option<PathBuf>,
    },
}

/// `schedr runs ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum RunsCommand {
    /// Trigger a manual run.
    Trigger {
        /// The DAG's ID.
        dag: String,
        /// The moment the run is for (UTC without an offset); default: now.
        #[arg(long = "for", value_name = "TIME")]
        planned_for: Option<String>,
        /// A run parameter: `key=value` or `key:=json`.
        #[arg(short = 'p', long = "param", value_name = "FIELD")]
        params: Vec<String>,
        /// All run parameters as one JSON object.
        #[arg(long = "params", value_name = "JSON", conflicts_with = "params")]
        params_json: Option<String>,
        /// For a DAG a trigger starts: run again an event it found, by key.
        #[arg(long, value_name = "KEY", conflicts_with_all = ["event_key", "event_data", "event_at"])]
        event: Option<String>,
        /// Or give an event: its key. It is never stored as an event.
        #[arg(long, value_name = "KEY")]
        event_key: Option<String>,
        /// And its data: JSON, or `@file.json`.
        #[arg(long, value_name = "JSON", requires = "event_key")]
        event_data: Option<String>,
        /// And when it happened (as `--for`); the run's moment otherwise.
        #[arg(long, value_name = "TIME", requires = "event_key")]
        event_at: Option<String>,
    },
    /// List the runs of a DAG.
    List {
        /// The DAG's ID.
        dag: String,
        /// Only the runs for this moment.
        #[arg(long = "for", value_name = "TIME")]
        planned_for: Option<String>,
    },
    /// Show a run and its tasks (a mapped task's instances as `fetch[0]`, `fetch[1]`, ...).
    Show {
        /// The run's ID.
        run_id: String,
    },
    /// Cancel a run, or what's left of a backfill.
    #[command(group(clap::ArgGroup::new("target").required(true).args(["run_id", "backfill"])))]
    Cancel {
        /// The run's ID.
        run_id: Option<String>,
        /// Cancel the remaining runs of this backfill instead.
        #[arg(long, value_name = "BACKFILL_ID")]
        backfill: Option<String>,
    },
    /// Create one run per fire time in a range.
    Backfill {
        /// The DAG's ID.
        dag: String,
        /// The range's first moment (inclusive; UTC without an offset).
        #[arg(long, value_name = "TIME")]
        from: String,
        /// Its last moment (inclusive); default: now, and a later one ends at now.
        #[arg(long, value_name = "TIME")]
        to: Option<String>,
        /// A run for every fire time, also those with a successful, queued or running run.
        #[arg(long)]
        all: bool,
        /// A run parameter, for every run: `key=value` or `key:=json`.
        #[arg(short = 'p', long = "param", value_name = "FIELD")]
        params: Vec<String>,
        /// All run parameters as one JSON object.
        #[arg(long = "params", value_name = "JSON", conflicts_with = "params")]
        params_json: Option<String>,
        /// Only count the fire times and the runs it would create.
        #[arg(long)]
        dry_run: bool,
    },
}

/// The task a `schedr tasks` command acts on.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct TaskTarget {
    /// The run's ID.
    pub run_id: String,
    /// The task's ID.
    pub task_id: String,
    /// The instance of a mapped task.
    #[arg(long, value_name = "N")]
    pub map_index: Option<u32>,
}

/// `schedr tasks logs ...`.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct LogsArgs {
    /// The task.
    #[command(flatten)]
    pub target: TaskTarget,
    /// The attempt's try number (the latest by default).
    #[arg(long = "try", value_name = "N")]
    pub try_number: Option<u32>,
    /// Keep printing the log while the task runs.
    #[arg(long, short)]
    pub follow: bool,
}

/// `schedr tasks ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum TasksCommand {
    /// Cancel one running task.
    Cancel(TaskTarget),
    /// Retry an `unschedulable` task of an unfinished run.
    Retry(TaskTarget),
    /// Print an attempt's log, read from the worker that ran it.
    Logs(LogsArgs),
    /// List a task's attempts (of one instance, for a mapped task).
    Attempts(TaskTarget),
    /// Print a task's output (a mapped task's gathered list, or one instance's).
    Output(TaskTarget),
}

/// `schedr connections ...` (decisions 174, 193, 198-200): run on a worker, through the
/// master.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ConnectionsCommand {
    /// Render every connection of a root (all roots if none is given) with the worker's
    /// storages: each secret present or missing, never a value. Fails when one isn't.
    Check {
        /// The root; every root with connections if not given.
        root: Option<String>,
        /// Only this connection.
        #[arg(long)]
        name: Option<String>,
        /// The worker to run it on; one having the root's current code otherwise.
        #[arg(long)]
        worker: Option<String>,
    },
    /// Open a connection on a worker and run its check (or open and close it, without one).
    /// Fails when it doesn't work.
    Test {
        /// The connection.
        name: String,
        /// Its root, when several define one by that name.
        #[arg(long)]
        root: Option<String>,
        /// The worker to run it on; one having the root's current code otherwise.
        #[arg(long)]
        worker: Option<String>,
        /// How long it may take (`30s`, `2m`); `[webserver] connection_test_timeout` if not
        /// given.
        #[arg(long, value_parser = test_timeout)]
        timeout: Option<std::time::Duration>,
    },
}

fn test_timeout(text: &str) -> Result<std::time::Duration, String> {
    let timeout = schedr_config::units::parse_duration(text).map_err(|e| e.to_string())?;
    let most = std::time::Duration::from_millis(schedr_model::commands::MAX_CONNECTION_TEST_MS);
    if timeout.is_zero() || timeout > most {
        return Err(format!("1 ms to {} s", most.as_secs()));
    }
    Ok(timeout)
}

/// `schedr triggers ...` (decision 340's reasons: they are not tasks).
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum TriggersCommand {
    /// List every DAG's trigger.
    List,
    /// Show a DAG's trigger.
    Show {
        /// Its DAG.
        dag: String,
    },
    /// List what a DAG's trigger found, newest first.
    Events {
        /// Its DAG.
        dag: String,
        /// How many at most.
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
    /// Start a DAG's trigger again from its bookmark, whatever stopped it.
    Restart {
        /// Its DAG.
        dag: String,
    },
    /// Pass the run an `@trigger(wait_for=...)` keeps failing on: it is called for the next.
    Skip {
        /// Its DAG.
        dag: String,
    },
    /// Switch a DAG's trigger back on, forgetting its strikes.
    SwitchOn {
        /// Its DAG.
        dag: String,
    },
}

/// `schedr events ...` (decision 354).
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum EventsCommand {
    /// Post an event: a trigger on its name starts a run per key, a step waiting for it in a
    /// run goes on.
    Post {
        /// Its name, written as a DAG ID is.
        name: String,
        /// Its key.
        #[arg(long)]
        key: String,
        /// Its data: JSON, or `@file.json`.
        #[arg(long, value_name = "JSON")]
        data: Option<String>,
        /// When it happened (UTC without an offset); now otherwise.
        #[arg(long, value_name = "TIME")]
        at: Option<String>,
    },
    /// List the events posted, newest first.
    List {
        /// Only those of this name.
        #[arg(long)]
        name: Option<String>,
        /// How many at most.
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
}

/// `schedr sensors ...` (decision 338).
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum SensorsCommand {
    /// List the wait steps switched off.
    SwitchedOff,
    /// Switch a wait step back on, forgetting its strikes.
    SwitchOn {
        /// Its DAG.
        dag: String,
        /// The wait step's task ID.
        sensor: String,
    },
}

/// `schedr resources ...` (decision 340).
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ResourcesCommand {
    /// List a run's resources.
    List {
        /// The run.
        run: String,
    },
}

/// `schedr workers ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum WorkersCommand {
    /// List the workers the master has seen, with their state (gone ones included).
    List,
    /// Drain a worker for maintenance: it finishes its tasks and stays idle.
    Drain {
        /// The worker's ID.
        worker: String,
    },
    /// Let a drained worker take tasks again.
    Resume {
        /// The worker's ID.
        worker: String,
    },
}

/// A `--field NAME` option of the in-task CLI: print raw values instead of JSON.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct FieldArg {
    /// Print this field's raw values, one per line.
    #[arg(long, value_name = "NAME")]
    pub field: Option<String>,
}

/// `schedr task-ctx ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum TaskCtxCommand {
    /// Cleanup handles of the running attempt.
    Cleanup {
        /// The cleanup command.
        #[command(subcommand)]
        command: CleanupCommand,
    },
    /// The value of the resource this setup creates, or this cleanup cleans up.
    Resource {
        /// The resource command.
        #[command(subcommand)]
        command: ResourceCommand,
    },
    /// Secrets declared in the task's `secrets=`.
    Secret {
        /// The secret command.
        #[command(subcommand)]
        command: SecretCommand,
    },
    /// A connection the task declares (`connections=`): its values, its secrets filled in.
    Connection(ConnectionArgs),
    /// A request to schedr's REST API, made as this task (open to tasks: `POST` and `GET
    /// /api/v1/events`); prints the answer's JSON, fails on an error status.
    Api {
        /// `GET`, `POST`, ...
        method: String,
        /// The path and its query, e.g. `/api/v1/events?name=paid`.
        path: String,
        /// The body, JSON.
        #[arg(long, value_name = "JSON")]
        data: Option<String>,
        /// Sent again with the same key, the request is the one made the first time.
        #[arg(long, value_name = "KEY")]
        idempotency_key: Option<String>,
    },
    /// Events (`EventNamed`), posted and listed as this task.
    Events {
        /// The events command.
        #[command(subcommand)]
        command: TaskEventsCommand,
    },
    /// Sends a message to a notification channel of the task's DAG (or a server one); the
    /// master fills the channel's body and delivers it later.
    Notify {
        /// The channel's name.
        channel: String,
        /// Its text (the `text` placeholder; schedr's summary otherwise).
        #[arg(long)]
        text: Option<String>,
        /// Its data (the `data` placeholder): JSON, or `@file.json`.
        #[arg(long, value_name = "JSON")]
        data: Option<String>,
    },
}

/// `schedr task-ctx events ...`: as `schedr events`, made as the task.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum TaskEventsCommand {
    /// Post an event; prints it as stored, as one line of JSON.
    Post {
        /// Its name, written as a DAG ID is.
        name: String,
        /// Its key: a trigger starts one run per key, a waiting step matches its own.
        #[arg(long)]
        key: String,
        /// Its data: JSON, or `@file.json`.
        #[arg(long, value_name = "JSON")]
        data: Option<String>,
        /// When it happened (UTC without an offset); now otherwise.
        #[arg(long, value_name = "TIME")]
        at: Option<String>,
        /// Sent again with the same key (a retried task), it is the event stored the first
        /// time.
        #[arg(long, value_name = "KEY")]
        idempotency_key: Option<String>,
    },
    /// List the events posted, newest first, one line of JSON each.
    List {
        /// Only those of this name.
        #[arg(long)]
        name: Option<String>,
        /// How many at most (500 at most).
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
}

/// `schedr task-ctx cleanup ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum CleanupCommand {
    /// Register a handle (`key=value`, `key:=json`), before the work it cleans up after.
    Add {
        /// The handle's type name, e.g. `ExportJob`.
        handle_type: String,
        /// The handle's fields.
        fields: Vec<String>,
        /// The whole handle as one JSON object.
        #[arg(long, value_name = "JSON")]
        json: Option<String>,
    },
    /// List the handles, in a cleanup.
    List {
        /// Only handles of this type.
        handle_type: Option<String>,
        /// How to print them.
        #[command(flatten)]
        field: FieldArg,
    },
}

/// `schedr task-ctx resource ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ResourceCommand {
    /// Report the resource's value, in its setup.
    Set {
        /// The value's fields (`key=value`, `key:=json`).
        fields: Vec<String>,
        /// The whole value as JSON, of any type (`--json 7`).
        #[arg(long, value_name = "JSON")]
        json: Option<String>,
    },
    /// Read the resource's value, in its cleanup.
    Get {
        /// How to print it.
        #[command(flatten)]
        field: FieldArg,
    },
}

/// `schedr task-ctx secret ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum SecretCommand {
    /// Print a declared secret: a key's value as it is, with no trailing newline; a whole
    /// secret (`path` without `#key`) as one line of JSON.
    Get {
        /// `path#key`, or `path` for the whole secret.
        secret: String,
        /// The secret storage, if not the default one.
        #[arg(long, value_name = "NAME")]
        storage: Option<String>,
    },
}

/// `schedr task-ctx connection NAME [--env PREFIX | --json]`.
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct ConnectionArgs {
    /// The connection's name (its function's).
    pub name: String,
    /// Print `export PREFIX_PARAM='value'` lines for `eval`, one per parameter (the
    /// parameter's name in capitals; a value that isn't text as JSON).
    #[arg(long, value_name = "PREFIX")]
    pub env: Option<String>,
    /// Print the values as one line of JSON (the default).
    #[arg(long, conflicts_with = "env")]
    pub json: bool,
}

/// `schedr dag-ctx ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum DagCtxCommand {
    /// The run's parameters.
    Params {
        /// The parameters command.
        #[command(subcommand)]
        command: GetCommand,
    },
    /// The run information (`id`, `kind`, `planned_for`, `since`, ...).
    Run {
        /// The run information command.
        #[command(subcommand)]
        command: GetCommand,
    },
}

/// A `get` subcommand.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum GetCommand {
    /// Print the values as JSON, or one field's raw value.
    Get {
        /// How to print them.
        #[command(flatten)]
        field: FieldArg,
    },
}

impl Command {
    /// The milestone in which this command starts working (see archive/PROJECT.md, Roadmap), or
    /// `None` if it works already.
    #[must_use]
    pub const fn milestone(&self) -> Option<&'static str> {
        match self {
            Self::Db { .. }
            | Self::Secrets { .. }
            | Self::Dags { .. }
            | Self::Runs { .. }
            | Self::Standalone { .. }
            | Self::Master { .. }
            | Self::Worker
            | Self::Webserver
            | Self::Ui
            | Self::Tasks { .. }
            | Self::Workers { .. }
            | Self::Sensors { .. }
            | Self::Triggers { .. }
            | Self::Events { .. }
            | Self::Resources { .. }
            | Self::Connections { .. }
            | Self::DagCtx { .. }
            | Self::TaskExec { .. }
            | Self::TaskCtx { .. } => None,
        }
    }

    /// The command's name as typed, e.g. `master` or `runs`, for messages.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Master { .. } => "master",
            Self::Worker => "worker",
            Self::Webserver => "webserver",
            Self::Ui => "ui",
            Self::Standalone { .. } => "standalone",
            Self::Db { .. } => "db",
            Self::Secrets { .. } => "secrets",
            Self::Dags { .. } => "dags",
            Self::Runs { .. } => "runs",
            Self::Tasks { .. } => "tasks",
            Self::Workers { .. } => "workers",
            Self::Sensors { .. } => "sensors",
            Self::Triggers { .. } => "triggers",
            Self::Events { .. } => "events",
            Self::Resources { .. } => "resources",
            Self::Connections { .. } => "connections",
            Self::TaskCtx { .. } => "task-ctx",
            Self::DagCtx { .. } => "dag-ctx",
            Self::TaskExec { .. } => "task-exec",
        }
    }
}
