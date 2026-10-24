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
    DurationText, MemoryReserve, SizeText, deserialize_duration, deserialize_optional_size,
    serialize_duration, serialize_optional_size,
};

#[cfg(test)]
mod tests;

/// The interpreter name used for parsing DAGs and running Python tasks unless configured.
pub const DEFAULT_DAG_INTERPRETER: &str = "python";

/// The executable behind the built-in `python` interpreter when `[interpreters]` doesn't
/// define one.
pub const BUILTIN_PYTHON: &str = "python3";

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
    /// `[webserver]`: the REST API (and later the UI).
    pub webserver: WebserverSection,
    /// `[worker]`: this worker's identity, capacity and data directory.
    pub worker: WorkerSection,
    /// `[tls]`: mTLS on the internal links (master ↔ worker, webserver → worker logs).
    pub tls: TlsSection,
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseSection {
    /// The connection URL (`sqlite:...` or `postgres://...`). A URL with a password must come
    /// from `SCHEDR_CONFIG__DATABASE__URL`, never the file (see PROJECT.md, Configuration:
    /// secrets).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// The longest a query (or waiting for a connection) may take; a write's transaction, a
    /// few of these. Shorter than `[ops] loop_stall_timeout`, so one hung query doesn't look
    /// like a stuck loop, and at most a third of `[master] lease_period`.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub query_timeout: Duration,
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
}

impl Default for DatabaseSection {
    /// No URL, queries limited to 10 s, notifications on (through the main URL).
    fn default() -> Self {
        Self {
            url: None,
            query_timeout: Duration::from_secs(10),
            notifications: true,
            notifications_url: None,
        }
    }
}

/// Whether a URL has a password in its user-info part (`scheme://user:password@host`).
#[must_use]
pub fn url_has_password(url: &str) -> bool {
    let Some((_, rest)) = url.split_once("://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    authority
        .rsplit_once('@')
        .is_some_and(|(user_info, _)| user_info.contains(':'))
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
    /// The master's liveness fails when its scheduling loop hasn't started an iteration for
    /// this long.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub loop_stall_timeout: Duration,
}

impl Default for OpsSection {
    /// Port 8090 of every interface and a 2 minute loop stall timeout.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8090,
            loop_stall_timeout: Duration::from_secs(120),
        }
    }
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
    /// How often the master's loop runs (scheduler, commands, dispatching).
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub tick_interval: Duration,
    /// How often workers send `Heartbeat` (the master's setting, sent to every worker).
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub heartbeat_interval: Duration,
    /// A worker silent for this long is lost.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub lease_timeout: Duration,
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
    /// How long the master's lease lasts (one master per database: a second one waits as a
    /// standby, and takes over at most this long after the first dies); renewed every third
    /// of it. At least three `[database] query_timeout`s, shorter than `[ops]
    /// loop_stall_timeout`.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub lease_period: Duration,
}

impl Default for MasterSection {
    /// All interfaces, port 8070, a tick every second, heartbeats every 10 s, lost after
    /// 60 s, 30 s grace, 10 min for missing code, a 30 s lease.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            grpc_port: 8070,
            tick_interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(10),
            lease_timeout: Duration::from_secs(60),
            cancel_grace: Duration::from_secs(30),
            code_wait: Duration::from_mins(10),
            lease_period: Duration::from_secs(30),
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
    /// `[webserver.auth]`: who the user is.
    pub auth: WebserverAuthSection,
}

impl Default for WebserverSection {
    /// All interfaces, port 8080.
    fn default() -> Self {
        Self {
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 8080,
            auth: WebserverAuthSection::default(),
        }
    }
}

/// `[webserver.auth]`: who the user is (only the identity so far; the modes come with the UI,
/// milestone M3).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WebserverAuthSection {
    /// The header naming the user, recorded on commands (e.g. `x-forwarded-user`); never used
    /// to allow or deny anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_header: Option<String>,
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
    /// Where task logs, the journal (`journal/<id>`; keep it across restarts) and (in
    /// standalone) the SQLite database go; `$XDG_DATA_HOME/schedr` (else
    /// `~/.local/share/schedr`) when not set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
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
    /// From a drain's SIGKILL of the tasks still running to the end of
    /// `termination_grace_period`: time to report them interrupted and deregister before the
    /// pod is killed (with `0s`, their results may not reach the master, and they end `lost`).
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub drain_report_period: Duration,
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
    /// How often the watchdog samples each task's memory.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub memory_check_interval: Duration,
}

impl Default for WorkerSection {
    /// The host name, 2 slots, the `default` queue, the default data directory, 30 s of grace
    /// (Kubernetes' default) of which the last 5 s stop and report what still runs (3 s to
    /// SIGKILL, 2 s to report), the log endpoint on port 8081 of every interface, logs kept for
    /// 30 days, the memory budget from the pod's limit, sampled every second.
    fn default() -> Self {
        Self {
            id: None,
            slots: 2,
            queues: Vec::new(),
            data_dir: None,
            pass_env: Vec::new(),
            termination_grace_period: Duration::from_secs(30),
            drain_terminate_period: Duration::from_secs(3),
            drain_report_period: Duration::from_secs(2),
            master_url: None,
            log_bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            log_port: 8081,
            log_address: None,
            log_retention: Duration::from_hours(24 * 30),
            log_max_size: None,
            memory_budget: None,
            memory_reserve: None,
            memory_check_interval: Duration::from_secs(1),
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
    /// How often the files are read again for a change.
    #[serde(
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    #[schemars(with = "DurationText")]
    pub reload_interval: Duration,
}

impl Default for TlsSection {
    /// `app` mode, no files yet, workers named `schedr-worker`, files read every 30 s.
    fn default() -> Self {
        Self {
            mode: TlsMode::App,
            ca: None,
            cert: None,
            key: None,
            worker_name: "schedr-worker".to_owned(),
            reload_interval: Duration::from_secs(30),
        }
    }
}
