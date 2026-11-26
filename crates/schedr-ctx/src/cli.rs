//! The in-task CLI's commands (`schedr task-ctx ...`, `schedr dag-ctx ...`), as parsed: the same
//! for `schedr` and the small `schedr-ctx` program (M5.3c).

use clap::{Args, Subcommand};

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
    /// Says where the task is now (decision 519): its phase, shown on the task with its time.
    Status {
        /// Its phase (`pulling`, `running`, ...).
        phase: String,
        /// A line about it.
        #[arg(long)]
        message: Option<String>,
        /// How far along it is, from 0 to 1.
        #[arg(long, value_name = "FRACTION")]
        progress: Option<String>,
    },
    /// A link shown on the task (decision 519): `http` or `https`.
    Link {
        /// Its label.
        label: String,
        /// Its address.
        url: String,
    },
    /// The extension's own: only in a shell plugin (`schedr extension shell`, M5.2i6); a command
    /// task has none of them (DN27).
    Extension {
        /// The extension command.
        #[command(subcommand)]
        command: ExtensionCtxCommand,
    },
    /// Writes a structured log line to stderr (decision 514): shown in the Logs tab with its
    /// level and fields, as a Python task's `logging` is.
    Log {
        /// What it says.
        message: String,
        /// `debug`, `info`, `warning`, `error` or `critical`.
        #[arg(long, default_value = "info")]
        level: String,
        /// A field, `name=value`: a JSON value, or the text as it is. Repeatable.
        #[arg(long = "field", value_name = "NAME=VALUE")]
        fields: Vec<String>,
    },
}

/// `schedr task-ctx extension ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ExtensionCtxCommand {
    /// A point of the extension's declared metric: only in a shell plugin (decision 519, DN27).
    Metric {
        /// The metric.
        name: String,
        /// Its value: for a counter what it grew by, for a gauge its value, for a histogram
        /// one observation.
        #[arg(allow_negative_numbers = true)]
        value: String,
        /// A label, `name=value`. Repeatable.
        #[arg(long = "label", value_name = "NAME=VALUE")]
        labels: Vec<String>,
    },
    /// One of the extension's declared notification events (M5.2i8): only in a shell plugin;
    /// told to the channels of the attempt's DAG subscribed to `EXTENSION_EVENT`.
    Notify {
        /// The declared event (`[extensions.<name>] notifications`).
        event: String,
        /// What happened (the `text` placeholder; schedr's summary otherwise).
        #[arg(long)]
        text: Option<String>,
        /// More about it (the `data` placeholder): JSON, or `@file.json`.
        #[arg(long, value_name = "JSON")]
        data: Option<String>,
    },
    /// Calls the extension's service (decision 518): only in a shell plugin. Prints the
    /// service's result as JSON; exits 1 with its error.
    Call {
        /// What is asked for.
        method: String,
        /// Its arguments: JSON, or `@file.json`.
        #[arg(long, value_name = "JSON")]
        args: Option<String>,
    },
    /// Why the task is being stopped, after `SIGTERM`: only in a shell plugin (decision 519, DN27).
    /// Prints `{"reason": ..., "seconds_left": ...}`; the reason is `cancel`, `timeout`,
    /// `drain`, `master_lost`, or null while it isn't stopped.
    StopReason {
        /// How to print it.
        #[command(flatten)]
        field: FieldArg,
    },
    /// The task's output.
    Output {
        /// The output command.
        #[command(subcommand)]
        command: OutputCommand,
    },
}

/// `schedr task-ctx extension output ...`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum OutputCommand {
    /// Sets the task's output: JSON, or `@file.json`; set again, the last one counts. None set,
    /// it is null.
    Set {
        /// The output.
        #[arg(value_name = "JSON", allow_hyphen_values = true)]
        value: String,
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
