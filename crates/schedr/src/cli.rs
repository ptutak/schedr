//! The command-line interface: the server roles, `db`, the user CLI and the in-task CLI
//! (see PROJECT.md, Web UI and CLI; In-task CLI).

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
    Master,
    /// Run a worker agent.
    Worker,
    /// Run the webserver: web UI and the API used by the UI and CLI.
    Webserver,
    /// Run every role in one process with SQLite (local development, or one pod for testing).
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
    },
    /// Manage the database schema.
    Db {
        /// The database command.
        #[command(subcommand)]
        command: DbCommand,
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
    /// Trigger, list, cancel and backfill runs.
    Runs {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The run command.
        #[command(subcommand)]
        command: RunsCommand,
    },
    /// Cancel or retry single tasks of a run, or read their logs.
    Tasks {
        /// How to reach the server.
        #[command(flatten)]
        client: ClientArgs,
        /// The task command.
        #[command(subcommand)]
        command: TasksCommand,
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
    /// Parse DAG files and `schedr.toml` files locally and report errors (no server).
    Check {
        /// A file or directory.
        path: PathBuf,
        /// Also warn when the DAGs need a newer schedr than the server's workers run.
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
        /// A secret value for the run: `path#key=value`.
        #[arg(long = "secret", value_name = "SECRET")]
        secrets: Vec<String>,
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
    },
    /// List the runs of a DAG.
    List {
        /// The DAG's ID.
        dag: String,
        /// Only the runs for this moment.
        #[arg(long = "for", value_name = "TIME")]
        planned_for: Option<String>,
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
        /// The first fire time (inclusive; UTC without an offset).
        #[arg(long, value_name = "TIME")]
        from: String,
        /// The last fire time (inclusive); default: now.
        #[arg(long, value_name = "TIME")]
        to: Option<String>,
        /// Also fire times that already have a successful run.
        #[arg(long)]
        all: bool,
        /// A run parameter: `key=value` or `key:=json`.
        #[arg(short = 'p', long = "param", value_name = "FIELD")]
        params: Vec<String>,
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
        /// The whole value as one JSON object.
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
    /// Print a declared secret (no trailing newline).
    Get {
        /// `path#key`.
        secret: String,
        /// The secret storage, if not the default one.
        #[arg(long, value_name = "NAME")]
        storage: Option<String>,
    },
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
    /// The milestone in which this command starts working (see PROJECT.md, Roadmap), or
    /// `None` if it works already.
    #[must_use]
    pub const fn milestone(&self) -> Option<&'static str> {
        match self {
            Self::Db { .. }
            | Self::Dags { .. }
            | Self::Runs { .. }
            | Self::Standalone { .. }
            | Self::Master
            | Self::Worker
            | Self::Webserver
            | Self::Tasks { .. }
            | Self::Workers { .. } => None,
            Self::TaskCtx { .. } | Self::DagCtx { .. } => Some("M4"),
        }
    }

    /// The command's name as typed, e.g. `master` or `runs`, for messages.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Worker => "worker",
            Self::Webserver => "webserver",
            Self::Standalone { .. } => "standalone",
            Self::Db { .. } => "db",
            Self::Dags { .. } => "dags",
            Self::Runs { .. } => "runs",
            Self::Tasks { .. } => "tasks",
            Self::Workers { .. } => "workers",
            Self::TaskCtx { .. } => "task-ctx",
            Self::DagCtx { .. } => "dag-ctx",
        }
    }
}
