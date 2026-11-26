//! Loading the configuration: layering the sources, parsing, and validating.

use std::collections::BTreeMap;
use std::path::PathBuf;

use figment::error::{Actual, Kind};
use figment::providers::{Format, Toml};
use figment::value::{Dict, Map};
use figment::{Figment, Metadata, Profile, Provider};

use crate::config::{
    BUILTIN_PYTHON, BUILTIN_PYTHON_ORIGIN, Config, DEFAULT_DAG_INTERPRETER, HIDDEN_PREFIX,
    HiddenLimits, Interpreter, Timings, url_has_password,
};
use crate::roots::{RootError, resolve_roots};
use crate::sources::{
    FileSource, Override, SourceError, env_provider, file_metadata, override_provider,
};
use crate::units::{format_duration, format_size, parse_duration};

/// The smallest `[grpc] max_message_size`: gRPC's default, which workers rely on.
const MIN_MESSAGE_SIZE: u64 = 4 * 1024 * 1024;

/// The largest `[grpc] max_message_size`.
const MAX_MESSAGE_SIZE: u64 = 1024 * 1024 * 1024;

#[cfg(test)]
mod tests;

/// Why the configuration can't be used; schedr refuses to start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// A value can't be read: bad TOML, an unknown key, a wrong type. The message names the
    /// key and where the value came from.
    #[error("{0}")]
    Invalid(String),
    /// A `SCHEDR_HIDDEN__*` variable that isn't a timing or a limit, or not a value of its kind.
    #[error("{name}: {reason}")]
    BadHiddenTiming {
        /// The variable.
        name: String,
        /// What is wrong with it.
        reason: String,
    },
    /// An environment variable can't be used.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// The roots are invalid (nested or repeated entries, duplicate names, bad renames).
    #[error(transparent)]
    Roots(#[from] RootError),
    /// A secret (e.g. a database URL with a password) is in the configuration file.
    #[error(
        "`{key}` contains a password: set it with the environment variable `{env}`, never in the configuration file"
    )]
    SecretInFile {
        /// The configuration key.
        key: &'static str,
        /// The environment variable to use instead.
        env: &'static str,
    },
    /// A timing that can't work at zero is set to it.
    #[error("{key} must be longer than 0s")]
    ZeroTiming {
        /// The key, with its section.
        key: String,
    },
    /// The drain's terminate and report periods don't fit in the termination grace period.
    #[error(
        "[worker] drain_terminate_period ({terminate_period}) + drain_report_period \
         ({report_period}) must not exceed [worker] termination_grace_period ({grace}): \
         shorten them, or lengthen the grace period (and the pod's \
         terminationGracePeriodSeconds with it)"
    )]
    DrainPeriods {
        /// `drain_terminate_period`, as written.
        terminate_period: String,
        /// `drain_report_period`, as written.
        report_period: String,
        /// `termination_grace_period`, as written.
        grace: String,
    },
    /// `[grpc] max_message_size` is out of its range.
    #[error(
        "[grpc] max_message_size ({size}) must be from 4MiB (gRPC's default, which \
         workers rely on) to 1GiB"
    )]
    MessageSize {
        /// The size, as written.
        size: String,
    },
    /// `[secret_storage]` can't be used: a bad storage name, or a `default` not configured
    /// (the message names the table).
    #[error("{0}")]
    SecretStorage(String),
    /// An `[extensions.<name>]` table is invalid (the message names it).
    #[error("{0}")]
    Extensions(String),
    /// `[backfill] max_runs` is zero.
    #[error("[backfill] max_runs must be at least 1")]
    NoBackfillRuns,
    /// `[notifications] max_per_attempt` is zero.
    #[error("[notifications] max_per_attempt must be at least 1")]
    NoNotificationsPerAttempt,
    /// `[mapping] max_instances` is zero.
    #[error("[mapping] max_instances must be at least 1")]
    NoInstances,
    /// `[worker] log_mode` isn't a mode logs may have.
    #[error(
        "[worker] log_mode `{mode}` isn't a mode for logs: octal, the owner reading and writing, \
         at most 0644 (e.g. \"0600\", \"0640\")"
    )]
    LogMode {
        /// What was given.
        mode: String,
    },
    /// `[dags] parse_concurrency` is zero.
    #[error("[dags] parse_concurrency must be at least 1")]
    NoParseConcurrency,
    /// The DAG interpreter isn't an `[interpreters]` entry.
    #[error("[dags] interpreter `{name}` is not defined in [interpreters] (defined: {defined})")]
    UnknownInterpreter {
        /// The configured name.
        name: String,
        /// The defined names, comma-separated.
        defined: String,
    },
}

impl From<figment::Error> for ConfigError {
    /// Each error with the value it found left out, its type kept: the value may be a secret
    /// (B201: a numeric password given to a string field).
    fn from(error: figment::Error) -> Self {
        let messages: Vec<String> = error
            .into_iter()
            .map(|mut e| {
                e.kind = match e.kind {
                    Kind::InvalidType(found, expected) => {
                        Kind::InvalidType(unquoted(&found), expected)
                    }
                    Kind::InvalidValue(found, expected) => {
                        Kind::InvalidValue(unquoted(&found), expected)
                    }
                    Kind::Message(message) => Kind::Message(without_excerpt(&message)),
                    kind => kind,
                };
                e.to_string()
            })
            .collect();
        Self::Invalid(messages.join("\n"))
    }
}

/// A parser's message without its excerpt of the file (`2 | password = "...`, and the `|`
/// lines around it): the line quoted may hold a secret, mistyped (B201's review). Where it is
/// (line and column) and what is wrong stay.
fn without_excerpt(message: &str) -> String {
    let excerpt = |line: &str| {
        let line = line.trim_start();
        let after_number = line
            .trim_start_matches(|c: char| c.is_ascii_digit())
            .trim_start();
        after_number.starts_with('|')
    };
    message
        .lines()
        .filter(|line| !excerpt(line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// What a value was, without the value: `an unsigned int`, not ``unsigned int `731946285` ``.
fn unquoted(found: &Actual) -> Actual {
    let kind = match found {
        Actual::Bool(_) => "a bool",
        Actual::Unsigned(_) => "an unsigned int",
        Actual::Signed(_) => "a signed int",
        Actual::Float(_) => "a float",
        Actual::Char(_) => "a char",
        Actual::Str(_) => "a string",
        Actual::Bytes(_) => "bytes",
        other => return other.clone(),
    };
    Actual::Other(kind.to_owned())
}

/// The configuration file as a provider whose metadata names the file.
struct FileProvider<'a>(FileSource<'a>);

impl Provider for FileProvider<'_> {
    fn metadata(&self) -> Metadata {
        file_metadata(self.0.path)
    }

    fn data(&self) -> Result<Map<Profile, Dict>, figment::Error> {
        Toml::string(self.0.contents).data()
    }
}

/// Loads the configuration from the file's contents (if any), the environment and the
/// command-line overrides; later sources win (flags > environment > file).
///
/// Validation resolves the DAG and script roots and checks that the DAG interpreter is
/// defined. If `[interpreters]` doesn't define `python`, it is the built-in `python3`.
///
/// # Errors
///
/// Returns a [`ConfigError`] for anything that would make schedr refuse to start.
pub fn load(
    file: Option<FileSource<'_>>,
    env: &BTreeMap<String, String>,
    overrides: &[Override],
) -> Result<Config, ConfigError> {
    let mut figment = Figment::new();
    if let Some(file) = file {
        figment = figment.merge(FileProvider(file));
    }
    figment = figment
        .merge(env_provider(env)?)
        .merge(override_provider(overrides));
    let mut config: Config = figment.extract()?;
    config.timings = hidden_timings(env)?;
    check_lease(&config.timings)?;
    check_metrics_final(&config.timings)?;
    config.limits = hidden_limits(env)?;
    check_secrets_in_file(&figment, &config)?;
    let worker = &config.worker;
    let drain_end = worker
        .drain_terminate_period
        .checked_add(config.timings.drain_report_period);
    if drain_end.is_none_or(|end| end > worker.termination_grace_period) {
        return Err(ConfigError::DrainPeriods {
            terminate_period: format_duration(worker.drain_terminate_period),
            report_period: format_duration(config.timings.drain_report_period),
            grace: format_duration(worker.termination_grace_period),
        });
    }
    if !(MIN_MESSAGE_SIZE..=MAX_MESSAGE_SIZE).contains(&config.grpc.max_message_size) {
        return Err(ConfigError::MessageSize {
            size: format_size(config.grpc.max_message_size),
        });
    }
    if crate::config::log_mode(&worker.log_mode).is_none() {
        return Err(ConfigError::LogMode {
            mode: worker.log_mode.clone(),
        });
    }
    if config.dags.parse_concurrency == 0 {
        return Err(ConfigError::NoParseConcurrency);
    }
    if config.mapping.max_instances == 0 {
        return Err(ConfigError::NoInstances);
    }
    if config.backfill.max_runs == 0 {
        return Err(ConfigError::NoBackfillRuns);
    }
    check_not_zero(&config)?;
    check_secret_storage(&config.secret_storage)?;
    check_extensions(&config.extensions)?;
    for name in config.secret_storage.storages.keys() {
        // As every credential: from the environment, never the file.
        if from_file(&figment, &format!("secret_storage.{name}.token")) {
            return Err(ConfigError::SecretStorage(format!(
                "[secret_storage.{name}] token: a token isn't written in the file; set \
                 SCHEDR_CONFIG__SECRET_STORAGE__{}__TOKEN",
                name.to_ascii_uppercase()
            )));
        }
    }
    if !config.interpreters.contains_key(DEFAULT_DAG_INTERPRETER) {
        config.interpreters.insert(
            DEFAULT_DAG_INTERPRETER.to_owned(),
            Interpreter::path(PathBuf::from(BUILTIN_PYTHON)),
        );
        config.builtin_python = Some(BUILTIN_PYTHON_ORIGIN.to_owned());
    }
    if !config.interpreters.contains_key(&config.dags.interpreter) {
        let defined: Vec<&str> = config.interpreters.keys().map(String::as_str).collect();
        return Err(ConfigError::UnknownInterpreter {
            name: config.dags.interpreter.clone(),
            defined: defined.join(", "),
        });
    }
    // A list given on the command line is named after its flag in messages.
    let list = |key: &str, alias: &'static str, flag: &'static str| {
        if overrides.iter().any(|o| o.key == key) {
            flag
        } else {
            alias
        }
    };
    let (dag_list, script_list) = (
        list("dags.path", "SCHEDR_DAG_PATH", "--dag-path"),
        list("scripts.path", "SCHEDR_SCRIPT_PATH", "--script-path"),
    );
    config.dag_roots = resolve_roots(dag_list, "dags", &config.dags.path, &config.dags.roots)?;
    config.script_roots = resolve_roots(
        script_list,
        "scripts",
        &config.scripts.path,
        &config.scripts.roots,
    )?;
    crate::roots::check_apart(script_list, &config.dag_roots, &config.script_roots)?;
    Ok(config)
}

/// Whether `key`'s value came from the configuration file.
fn from_file(figment: &Figment, key: &str) -> bool {
    figment
        .find_metadata(key)
        .is_some_and(|m| m.source.as_ref().is_some_and(|s| s.file_path().is_some()))
}

/// Refuses a credential written in the file: those come from the environment only.
fn check_secrets_in_file(figment: &Figment, config: &Config) -> Result<(), ConfigError> {
    for (key, env, url) in [
        (
            "database.url",
            "SCHEDR_CONFIG__DATABASE__URL",
            &config.database.url,
        ),
        (
            "database.notifications_url",
            "SCHEDR_CONFIG__DATABASE__NOTIFICATIONS_URL",
            &config.database.notifications_url,
        ),
    ] {
        if from_file(figment, key) && url.as_deref().is_some_and(url_has_password) {
            return Err(ConfigError::SecretInFile { key, env });
        }
    }
    if from_file(figment, "notifications.smtp.password") {
        return Err(ConfigError::SecretInFile {
            key: "notifications.smtp.password",
            env: "SCHEDR_CONFIG__NOTIFICATIONS__SMTP__PASSWORD",
        });
    }
    Ok(())
}

/// The timings a zero would break everything with: every task timed out at once, a panicking
/// interval, or every log deleted. Where zero means something (no wait, no retry) it stays allowed.
fn check_not_zero(config: &Config) -> Result<(), ConfigError> {
    if config.notifications.max_per_attempt == 0 {
        return Err(ConfigError::NoNotificationsPerAttempt);
    }
    let fixed = [
        ("[dags] poll_interval", config.dags.poll_interval),
        ("[notifications] timeout", config.notifications.timeout),
        ("[dags] parse_file_timeout", config.dags.parse_file_timeout),
        (
            "[master] parse_job_timeout",
            config.master.parse_job_timeout,
        ),
        (
            "[webserver] worker_request_timeout",
            config.webserver.worker_request_timeout,
        ),
        ("[worker] log_retention", config.worker.log_retention),
        (
            "[worker] runner_start_timeout",
            config.worker.runner_start_timeout,
        ),
        (
            "[worker] fork_server_preload_timeout",
            config.worker.fork_server_preload_timeout,
        ),
        ("[worker] cleanup_timeout", config.cleanup_timeout()),
        (
            "[sensors] default_max_wait",
            config.sensors.default_max_wait,
        ),
        (
            "[worker] resource_cleanup_timeout",
            config.worker.resource_cleanup_timeout,
        ),
    ]
    .map(|(key, timing)| (key.to_owned(), timing));
    let storages = config
        .secret_storage
        .storages
        .iter()
        .flat_map(|(name, storage)| {
            [
                ("request_timeout", storage.request_timeout),
                ("connect_timeout", storage.connect_timeout),
            ]
            .map(|(key, timing)| (format!("[secret_storage.{name}] {key}"), timing))
        });
    match fixed
        .into_iter()
        .chain(storages)
        .find(|(_, timing)| timing.is_zero())
    {
        Some((key, _)) => Err(ConfigError::ZeroTiming { key }),
        None => Ok(()),
    }
}

/// The `SCHEDR_HIDDEN__*` variables of [`HiddenLimits`], read by [`hidden_limits`].
const HIDDEN_LIMITS: [&str; 43] = [
    "MASTER__WORKER_REPORT_RECORDS",
    "MASTER__WORKER_REPORT_TEXT",
    "MASTER__NOTIFICATION_BODY",
    "MASTER__RETENTION_BATCH",
    "MASTER__MESSAGES_PER_TURN",
    "WEBSERVER__METRICS_CACHE_KEYS",
    "WEBSERVER__METRICS_CACHE_BUCKETS",
    "WEBSERVER__MEMORY_PEAKS_RUNS",
    "MASTER__ASSIGNMENTS_PER_ROUND",
    "MASTER__EXPIRIES_PER_ROUND",
    "MASTER__WAITING_REASONS_PER_ROUND",
    "MASTER__PENDING_REASONS_PER_ROUND",
    "MASTER__COMMANDS_PER_ROUND",
    "MASTER__SCHEDULED_RUNS_PER_ROUND",
    "MASTER__STARTED_INSTANCES_PER_ROUND",
    "MASTER__RETRIES_PER_ROUND",
    "MASTER__RUNS_PER_PLANNING_PASS",
    "MASTER__RUN_OF_RUNS_PER_PASS",
    "MASTER__FAILED_GROUP_MESSAGES_PER_TURN",
    "MASTER__WORKER_STREAM_BUFFER",
    "MASTER__NOTIFICATIONS_PER_PASS",
    "MASTER__SLOW_EQUALS_FACTOR",
    "WORKER__RESULTS_PER_JOURNAL_GROUP",
    "WORKER__STARTS_PER_JOURNAL_GROUP",
    "WORKER__RESULT_RESENDS",
    "WORKER__RESULT_RESEND_GAP_MAX",
    "WORKER__UNACKED_MESSAGES_KEPT",
    "WORKER__UNSENT_CONNECTION_ANSWERS",
    "WORKER__TASK_CONNECTIONS_OPEN",
    "WORKER__WATCHER_LOG_BACKLOG",
    "WEBSERVER__EVENTS_BACKLOG",
    "UI__EVENTS_BACKLOG",
    "WEBSERVER__AUTH_REMEMBERED_MAX",
    "DATABASE__CHANGES_POLL_ROWS",
    "DATABASE__DAG_VERSIONS_CACHED",
    "TLS__HANDSHAKES_AT_ONCE",
    "WORKER__EXTENSION_REPORTS_PER_SECOND",
    "WORKER__EXTENSION_REPORTS_AT_ONCE",
    "WORKER__EXTENSION_EVENTS_PENDING",
    "MASTER__EXTENSION_EVENTS_KEPT",
    "MASTER__EXTENSION_QUEUE",
    "MASTER__EXTENSION_EVENTS_PER_FLUSH",
    "MASTER__TRIGGER_RUNS_KEPT",
];

/// The hidden waiting queue's factor (decision 509): a fraction from 0 to 4, not a whole number.
const QUEUE_FACTOR: &str = "WORKER__QUEUE_FACTOR";

/// The largest waiting queue's factor: four times the slots.
const MOST_QUEUE_FACTOR_HUNDREDTHS: u32 = 400;

/// `text` as a fraction from 0 to 4 with at most two decimals (`0.5`, `2`, `1.25`), in
/// hundredths.
fn queue_factor(text: &str) -> Option<u32> {
    let (whole, part) = text.trim().split_once('.').unwrap_or((text.trim(), ""));
    if whole.is_empty()
        || part.len() > 2
        || !(whole.chars().chain(part.chars())).all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let whole: u32 = whole.parse().ok()?;
    let part: u32 = if part.is_empty() {
        0
    } else {
        format!("{part:0<2}").parse().ok()?
    };
    let hundredths = whole.checked_mul(100)?.checked_add(part)?;
    (hundredths <= MOST_QUEUE_FACTOR_HUNDREDTHS).then_some(hundredths)
}

/// The fixed limits, with what `SCHEDR_HIDDEN__*` variables set: whole numbers above zero (the
/// waiting queue's factor a fraction from 0 to 4).
fn hidden_limits(env: &BTreeMap<String, String>) -> Result<HiddenLimits, ConfigError> {
    let mut limits = HiddenLimits::default();
    for (name, text) in env {
        if name.strip_prefix(HIDDEN_PREFIX) == Some(QUEUE_FACTOR) {
            limits.queue_factor_hundredths =
                queue_factor(text).ok_or_else(|| ConfigError::BadHiddenTiming {
                    name: name.clone(),
                    reason: format!(
                        "must be a number from 0 to 4 with at most two decimals, got `{text}`"
                    ),
                })?;
            limits.overridden.push(name.clone());
            continue;
        }
        let limit = match name.strip_prefix(HIDDEN_PREFIX) {
            Some("MASTER__WORKER_REPORT_RECORDS") => &mut limits.worker_report_records,
            Some("MASTER__WORKER_REPORT_TEXT") => &mut limits.worker_report_text,
            Some("MASTER__NOTIFICATION_BODY") => &mut limits.notification_body,
            Some("MASTER__RETENTION_BATCH") => &mut limits.retention_batch,
            Some("MASTER__MESSAGES_PER_TURN") => &mut limits.messages_per_turn,
            Some("WEBSERVER__METRICS_CACHE_KEYS") => &mut limits.metrics_cache_keys,
            Some("WEBSERVER__METRICS_CACHE_BUCKETS") => &mut limits.metrics_cache_buckets,
            Some("WEBSERVER__MEMORY_PEAKS_RUNS") => &mut limits.memory_peaks_runs,
            Some("MASTER__ASSIGNMENTS_PER_ROUND") => &mut limits.assignments_per_round,
            Some("MASTER__EXPIRIES_PER_ROUND") => &mut limits.expiries_per_round,
            Some("MASTER__WAITING_REASONS_PER_ROUND") => &mut limits.waiting_reasons_per_round,
            Some("MASTER__PENDING_REASONS_PER_ROUND") => &mut limits.pending_reasons_per_round,
            Some("MASTER__COMMANDS_PER_ROUND") => &mut limits.commands_per_round,
            Some("MASTER__SCHEDULED_RUNS_PER_ROUND") => &mut limits.scheduled_runs_per_round,
            Some("MASTER__STARTED_INSTANCES_PER_ROUND") => &mut limits.started_instances_per_round,
            Some("MASTER__RETRIES_PER_ROUND") => &mut limits.retries_per_round,
            Some("MASTER__RUNS_PER_PLANNING_PASS") => &mut limits.runs_per_planning_pass,
            Some("MASTER__RUN_OF_RUNS_PER_PASS") => &mut limits.run_of_runs_per_pass,
            Some("MASTER__FAILED_GROUP_MESSAGES_PER_TURN") => {
                &mut limits.failed_group_messages_per_turn
            }
            Some("MASTER__WORKER_STREAM_BUFFER") => &mut limits.worker_stream_buffer,
            Some("MASTER__NOTIFICATIONS_PER_PASS") => &mut limits.notifications_per_pass,
            Some("MASTER__SLOW_EQUALS_FACTOR") => &mut limits.slow_equals_factor,
            Some("WORKER__RESULTS_PER_JOURNAL_GROUP") => &mut limits.results_per_journal_group,
            Some("WORKER__STARTS_PER_JOURNAL_GROUP") => &mut limits.starts_per_journal_group,
            Some("WORKER__RESULT_RESENDS") => &mut limits.result_resends,
            Some("WORKER__RESULT_RESEND_GAP_MAX") => &mut limits.result_resend_gap_max,
            Some("WORKER__UNACKED_MESSAGES_KEPT") => &mut limits.unacked_messages_kept,
            Some("WORKER__UNSENT_CONNECTION_ANSWERS") => &mut limits.unsent_connection_answers,
            Some("WORKER__TASK_CONNECTIONS_OPEN") => &mut limits.task_connections_open,
            Some("WORKER__WATCHER_LOG_BACKLOG") => &mut limits.watcher_log_backlog,
            Some("WORKER__EXTENSION_REPORTS_PER_SECOND") => {
                &mut limits.extension_reports_per_second
            }
            Some("WORKER__EXTENSION_REPORTS_AT_ONCE") => &mut limits.extension_reports_at_once,
            Some("WORKER__EXTENSION_EVENTS_PENDING") => &mut limits.extension_events_pending,
            Some("MASTER__EXTENSION_EVENTS_KEPT") => &mut limits.extension_events_kept,
            Some("MASTER__TRIGGER_RUNS_KEPT") => &mut limits.trigger_runs_kept,
            Some("MASTER__EXTENSION_QUEUE") => &mut limits.extension_queue,
            Some("MASTER__EXTENSION_EVENTS_PER_FLUSH") => &mut limits.extension_events_per_flush,
            Some("WEBSERVER__EVENTS_BACKLOG") => &mut limits.webserver_events_backlog,
            Some("UI__EVENTS_BACKLOG") => &mut limits.ui_events_backlog,
            Some("WEBSERVER__AUTH_REMEMBERED_MAX") => &mut limits.auth_remembered_max,
            Some("DATABASE__CHANGES_POLL_ROWS") => &mut limits.changes_poll_rows,
            Some("DATABASE__DAG_VERSIONS_CACHED") => &mut limits.dag_versions_cached,
            Some("TLS__HANDSHAKES_AT_ONCE") => &mut limits.tls_handshakes_at_once,
            _ => continue,
        };
        *limit = text
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=MOST_HIDDEN_LIMIT).contains(n))
            .ok_or_else(|| ConfigError::BadHiddenTiming {
                name: name.clone(),
                reason: format!(
                    "must be a whole number from 1 to {MOST_HIDDEN_LIMIT}, got `{text}`"
                ),
            })?;
        limits.overridden.push(name.clone());
    }
    Ok(limits)
}

/// The timing a hidden variable's `<SECTION>__<NAME>` names.
#[allow(clippy::too_many_lines)] // a line a timing
fn timing_named<'a>(timings: &'a mut Timings, key: &str) -> Option<&'a mut std::time::Duration> {
    let timing = match key {
        "MASTER__TICK_INTERVAL" => &mut timings.tick_interval,
        "MASTER__HEARTBEAT_INTERVAL" => &mut timings.heartbeat_interval,
        "MASTER__LEASE_TIMEOUT" => &mut timings.lease_timeout,
        "MASTER__SCRIPT_ROOT_GRACE" => &mut timings.script_root_grace,
        "MASTER__EVENT_LOOKBACK" => &mut timings.event_lookback,
        "MASTER__LEASE_PERIOD" => &mut timings.lease_period,
        "MASTER__READ_BACK_WINDOW" => &mut timings.read_back_window,
        "MASTER__READ_BACK_INTERVAL" => &mut timings.read_back_interval,
        "DATABASE__QUERY_TIMEOUT" => &mut timings.query_timeout,
        "OPS__LOOP_STALL_TIMEOUT" => &mut timings.loop_stall_timeout,
        "MASTER__SET_ASIDE" => &mut timings.set_aside,
        "MASTER__HANDED_CALL_MIN" => &mut timings.handed_call_min,
        "MASTER__CLEANUP_MOVE_WAIT" => &mut timings.cleanup_move_wait,
        "MASTER__EXTENSION_FLUSH_INTERVAL" => &mut timings.extension_flush_interval,
        "MASTER__EXTENSION_WRITE_TIMEOUT" => &mut timings.extension_write_timeout,
        "MASTER__LISTEN_RETRY" => &mut timings.listen_retry,
        "WEBSERVER__REFRESH_INTERVAL" => &mut timings.refresh_interval,
        "WEBSERVER__WORKER_CONNECT_TIMEOUT" => &mut timings.worker_connect_timeout,
        "WORKER__DRAIN_REPORT_PERIOD" => &mut timings.drain_report_period,
        "WORKER__MEMORY_CHECK_INTERVAL" => &mut timings.memory_check_interval,
        "TLS__HANDSHAKE_TIMEOUT" => &mut timings.handshake_timeout,
        "WORKER__OUTPUT_DRAIN_TIMEOUT" => &mut timings.output_drain_timeout,
        "WORKER__FORK_ANSWER_TIMEOUT" => &mut timings.fork_answer_timeout,
        "WORKER__FORK_BACKOFF_FIRST" => &mut timings.fork_backoff_first,
        "WORKER__FORK_BACKOFF_MAX" => &mut timings.fork_backoff_max,
        "WORKER__FORK_BACKOFF_RESET_AFTER" => &mut timings.fork_backoff_reset_after,
        "WORKER__SERVICE_BACKOFF_FIRST" => &mut timings.service_backoff_first,
        "WORKER__SERVICE_BACKOFF_MAX" => &mut timings.service_backoff_max,
        "WORKER__LOG_SWEEP_INTERVAL" => &mut timings.log_sweep_interval,
        "WORKER__ORPHAN_POLL" => &mut timings.orphan_poll,
        "WORKER__ORPHAN_KILL_WAIT" => &mut timings.orphan_kill_wait,
        "WORKER__ORPHAN_GRACE" => &mut timings.orphan_grace,
        "WORKER__RECONNECT_BACKOFF_FIRST" => &mut timings.reconnect_backoff_first,
        "WORKER__RECONNECT_BACKOFF_MAX" => &mut timings.reconnect_backoff_max,
        "WORKER__ALIVE_INTERVAL" => &mut timings.worker_alive_interval,
        "WORKER__DEREGISTER_WAIT" => &mut timings.deregister_wait,
        "WORKER__DRAIN_TICK" => &mut timings.drain_tick,
        "WORKER__QUEUE_CHECK_INTERVAL" => &mut timings.queue_check_interval,
        "WORKER__EXTENSION_FIGURES_INTERVAL" => &mut timings.extension_figures_interval,
        "WORKER__DRAIN_ACK_WAIT" => &mut timings.drain_ack_wait,
        "WORKER__STOP_POLL" => &mut timings.stop_poll,
        "WORKER__CONNECTION_JOB_CANCEL_GRACE" => &mut timings.connection_job_cancel_grace,
        "WORKER__VAULT_MIN_RENEW_WAIT" => &mut timings.vault_min_renew_wait,
        "WORKER__MASTER_SAMPLE_WINDOW" => &mut timings.master_sample_window,
        "WORKER__CLOCK_SKEW_WARNING" => &mut timings.clock_skew_warning,
        "WORKER__VAULT_RENEW_CHECK" => &mut timings.vault_renew_check,
        "WORKER__VAULT_RENEW_RETRY" => &mut timings.vault_renew_retry,
        "MASTER__ROUND_GAP" => &mut timings.round_gap,
        "MASTER__TASK_ROUND_GAP" => &mut timings.task_round_gap,
        "MASTER__NOTIFICATION_RETRY" => &mut timings.notification_retry,
        "MASTER__ALIVE_INTERVAL" => &mut timings.master_alive_interval,
        "MASTER__GAUGE_REFRESH" => &mut timings.gauge_refresh,
        "MASTER__OUTBOX_POLL" => &mut timings.outbox_poll,
        "MASTER__SLOW_TICK_LOG" => &mut timings.slow_tick_log,
        "MASTER__OUTBOX_RETRY_FIRST" => &mut timings.outbox_retry_first,
        "MASTER__OUTBOX_RETRY_MAX" => &mut timings.outbox_retry_max,
        "MASTER__RETENTION_INTERVAL" => &mut timings.retention_interval,
        "MASTER__SAMPLE_INTERVAL" => &mut timings.sample_interval,
        "MASTER__SAMPLES_KEPT" => &mut timings.samples_kept,
        "MASTER__ROLLUPS_KEPT" => &mut timings.rollups_kept,
        "MASTER__PARSE_RETRY_FIRST" => &mut timings.parse_retry_first,
        "MASTER__PARSE_RETRY_MAX" => &mut timings.parse_retry_max,
        "MASTER__NODE_HEARTBEAT" => &mut timings.node_heartbeat,
        "DATABASE__CHANGES_POLL_INTERVAL" => &mut timings.changes_poll_interval,
        "WEBSERVER__EVENTS_RECHECK" => &mut timings.events_recheck,
        "WEBSERVER__EVENTS_KEEP_ALIVE" => &mut timings.events_keep_alive,
        "WEBSERVER__EVENTS_RETRY" => &mut timings.events_retry,
        "WEBSERVER__AUTH_RECHECK" => &mut timings.auth_recheck,
        "WEBSERVER__AUTH_REMEMBERED" => &mut timings.auth_remembered,
        "WEBSERVER__REQUESTS_GRACE" => &mut timings.requests_grace,
        "WEBSERVER__SCHEMA_RETRY" => &mut timings.schema_retry,
        "UI__KEEP_ALIVE" => &mut timings.ui_keep_alive,
        "UI__COALESCE" => &mut timings.ui_coalesce,
        "UI__TAIL_INTERVAL" => &mut timings.ui_tail_interval,
        "UI__TAIL_GIVE_UP" => &mut timings.ui_tail_give_up,
        "UI__SLOW_READ" => &mut timings.ui_slow_read,
        "UI__SLOW_READ_WAIT" => &mut timings.ui_slow_read_wait,
        "UI__RELAY_RETRY_FIRST" => &mut timings.ui_relay_retry_first,
        "UI__RELAY_RETRY_MAX" => &mut timings.ui_relay_retry_max,
        "STANDALONE__WORKER_STOP_AFTER_FAILURE" => &mut timings.worker_stop_after_failure,
        "TLS__ACCEPT_RETRY" => &mut timings.accept_retry,
        "MASTER__TRIGGER_RESTART_BACKOFF" => &mut timings.trigger_restart_backoff,
        "MASTER__TRIGGER_RESTART_BACKOFF_MAX" => &mut timings.trigger_restart_backoff_max,
        "WEBSERVER__METRICS_FINAL_AFTER" => &mut timings.metrics_final_after,
        "MASTER__SAMPLE_ROLLUP_GRACE" => &mut timings.sample_rollup_grace,
        "WEBSERVER__MEMORY_PEAKS_WINDOW" => &mut timings.memory_peaks_window,
        "EVENTS__FUTURE_TOLERANCE" => &mut timings.event_future_tolerance,
        "MASTER__FOR_FUTURE_TOLERANCE" => &mut timings.for_future_tolerance,
        "MASTER__SLOW_EQUALS_FLOOR" => &mut timings.slow_equals_floor,
        "MASTER__CODE_RETRY_DELAY" => &mut timings.code_retry_delay,
        "MASTER__NODE_ROW_KEPT" => &mut timings.node_row_kept,
        "WORKER__REGISTER_TIMEOUT" => &mut timings.register_timeout,
        "MASTER__UNOFFERED_WAIT" => &mut timings.unoffered_wait,
        "WORKER__UNANSWERED_TIMEOUT" => &mut timings.unanswered_timeout,
        "CLIENT__STREAM_IDLE" => &mut timings.client_stream_idle,
        "WORKER__MASTER_CONNECT_TIMEOUT" => &mut timings.master_connect_timeout,
        "CLIENT__CONNECT_TIMEOUT" => &mut timings.client_connect_timeout,
        "CLIENT__REQUEST_TIMEOUT" => &mut timings.client_request_timeout,
        "CLIENT__COMMAND_WAIT" => &mut timings.client_command_wait,
        "DAGS__TEST_RUN_TIMEOUT" => &mut timings.dags_test_run_timeout,
        _ => return None,
    };
    Some(timing)
}

/// Hidden timings no longer used, accepted and ignored so that a deployment setting one still
/// starts: the quiet DAGs' memory, replaced by the stored next fire times (decision 485).
const RETIRED_TIMINGS: [&str; 3] = [
    "MASTER__QUIET_DAG_AFTER",
    "MASTER__QUIET_DAG_WAKE_AHEAD",
    "MASTER__QUIET_DAG_FULL_PASS_INTERVAL",
];

/// The highest a hidden limit takes (Codex's review of M5.1p): far above any default, and
/// below what tokio's channels and semaphores refuse with a panic.
pub const MOST_HIDDEN_LIMIT: usize = 1 << 24;

/// The longest a hidden timing takes, ten years (Codex's review of M5.1p): a deadline from now
/// stays a time.
pub const LONGEST_HIDDEN_TIMING: std::time::Duration = std::time::Duration::from_hours(3650 * 24);

/// The fixed timings, with what `SCHEDR_HIDDEN__*` variables set.
fn hidden_timings(env: &BTreeMap<String, String>) -> Result<Timings, ConfigError> {
    let mut timings = Timings::default();
    for (name, text) in env {
        let Some(key) = name.strip_prefix(HIDDEN_PREFIX) else {
            continue;
        };
        if HIDDEN_LIMITS.contains(&key) || key == QUEUE_FACTOR || RETIRED_TIMINGS.contains(&key) {
            continue;
        }
        let Some(timing) = timing_named(&mut timings, key) else {
            return Err(ConfigError::BadHiddenTiming {
                name: name.clone(),
                reason: "no such timing".to_owned(),
            });
        };
        *timing = parse_duration(text).map_err(|e| ConfigError::BadHiddenTiming {
            name: name.clone(),
            reason: e.to_string(),
        })?;
        if timing.is_zero() {
            return Err(ConfigError::BadHiddenTiming {
                name: name.clone(),
                reason: "must be longer than zero".to_owned(),
            });
        }
        if *timing > LONGEST_HIDDEN_TIMING {
            return Err(ConfigError::BadHiddenTiming {
                name: name.clone(),
                reason: format!("must be at most 3650 days, got `{text}`"),
            });
        }
        timings.overridden.push(name.clone());
    }
    Ok(timings)
}

/// A Metrics bucket final only after its samples' rollup: a webserver keeping one sooner would
/// never show a sample stored within the rollup's grace (decision 492).
///
/// And the minute samples kept past a window's rollup (Codex's review of M5.1p): a window's
/// first minute, 15 minutes and the grace before it is rolled up, would be deleted unrolled.
fn check_metrics_final(timings: &Timings) -> Result<(), ConfigError> {
    let rolled_after = timings
        .sample_rollup_grace
        .saturating_add(std::time::Duration::from_mins(15));
    if rolled_after >= timings.samples_kept {
        return Err(ConfigError::BadHiddenTiming {
            name: format!("{HIDDEN_PREFIX}MASTER__SAMPLE_ROLLUP_GRACE"),
            reason: format!(
                "with the 15-minute window, must be shorter than \
                 `{HIDDEN_PREFIX}MASTER__SAMPLES_KEPT` ({:?}), got {:?}",
                timings.samples_kept, timings.sample_rollup_grace
            ),
        });
    }
    if timings.sample_rollup_grace >= timings.metrics_final_after {
        return Err(ConfigError::BadHiddenTiming {
            name: format!("{HIDDEN_PREFIX}MASTER__SAMPLE_ROLLUP_GRACE"),
            reason: format!(
                "must be shorter than `{HIDDEN_PREFIX}WEBSERVER__METRICS_FINAL_AFTER` ({:?}), got {:?}",
                timings.metrics_final_after, timings.sample_rollup_grace
            ),
        });
    }
    Ok(())
}

/// The lease at least four heartbeats: under one, a worker is lost between two beats; with
/// four, a few beats held up or lost in a row don't lose it, and its cutoff, read from a
/// heartbeat sent up to one before, keeps most of the lease.
fn check_lease(timings: &Timings) -> Result<(), ConfigError> {
    if timings.lease_timeout < timings.heartbeat_interval.saturating_mul(4) {
        return Err(ConfigError::BadHiddenTiming {
            name: format!("{HIDDEN_PREFIX}MASTER__LEASE_TIMEOUT"),
            reason: format!(
                "must be at least four times `{HIDDEN_PREFIX}MASTER__HEARTBEAT_INTERVAL` ({:?}), got {:?}",
                timings.heartbeat_interval, timings.lease_timeout
            ),
        });
    }
    // A deposed master answers its workers until its lead runs out, its lease period less a
    // query limit; a worker follows a lower lease epoch only after a lease without word from
    // the higher one, so that lead must end first (decision 444, Grok's review).
    let lead = timings.lease_period.saturating_sub(timings.query_timeout);
    if lead >= timings.lease_timeout {
        return Err(ConfigError::BadHiddenTiming {
            name: format!("{HIDDEN_PREFIX}MASTER__LEASE_PERIOD"),
            reason: format!(
                "less `{HIDDEN_PREFIX}DATABASE__QUERY_TIMEOUT` ({:?}), it must be shorter than \
                 `{HIDDEN_PREFIX}MASTER__LEASE_TIMEOUT` ({:?}): a deposed master leads that \
                 long, got {:?}",
                timings.query_timeout, timings.lease_timeout, timings.lease_period
            ),
        });
    }
    Ok(())
}

/// The secret storage name every standalone has (`[secret_storage]`'s `db`).
pub const DB_STORAGE: &str = "db";

/// A storage's fields for its type: Vault's only for `vault`, and what its auth method
/// needs.
fn check_storage_fields(
    name: &str,
    storage: &crate::config::SecretStorageConfig,
) -> Result<(), ConfigError> {
    use crate::config::{SecretStorageType, VaultAuthMethod};
    let problem = |what: String| {
        Err(ConfigError::SecretStorage(format!(
            "[secret_storage.{name}] {what}"
        )))
    };
    let vault_fields = [
        ("address", storage.address.is_some()),
        ("mount", storage.mount.is_some()),
        ("namespace", storage.namespace.is_some()),
        ("ca_cert", storage.ca_cert.is_some()),
        ("auth", storage.auth.is_some()),
        ("token", storage.token.is_some()),
    ];
    if storage.kind == SecretStorageType::Db {
        if let Some((field, _)) = vault_fields.iter().find(|(_, set)| *set) {
            return problem(format!("{field}: only a `type = \"vault\"` storage has it"));
        }
        return Ok(());
    }
    let Some(address) = &storage.address else {
        return problem("needs `address`, e.g. \"https://vault.example.com:8200\"".to_owned());
    };
    if !(address.starts_with("https://") || address.starts_with("http://")) {
        return problem(format!("address `{address}`: an http:// or https:// URL"));
    }
    if let Some(mount) = &storage.mount
        && mount
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return problem(format!(
            "mount `{mount}`: a path such as `secret` or `team/kv`"
        ));
    }
    let Some(auth) = &storage.auth else {
        return problem(
            "needs `auth`: { method = \"kubernetes\", role = \"...\" } or { method = \"token\" }"
                .to_owned(),
        );
    };
    let env = format!(
        "SCHEDR_CONFIG__SECRET_STORAGE__{}__TOKEN",
        name.to_ascii_uppercase()
    );
    match auth.method {
        VaultAuthMethod::Token => {
            if storage.token.as_deref().is_none_or(str::is_empty) {
                return problem(format!("auth method `token` needs the token in {env}"));
            }
            if auth.role.is_some() || auth.mount.is_some() || auth.token_file.is_some() {
                return problem("auth method `token` has no role, mount or token_file".to_owned());
            }
        }
        VaultAuthMethod::Kubernetes => {
            if auth.role.as_deref().is_none_or(str::is_empty) {
                return problem("auth method `kubernetes` needs `role`".to_owned());
            }
            if storage.token.is_some() {
                return problem(format!("auth method `kubernetes` takes no token ({env})"));
            }
        }
    }
    Ok(())
}

/// `[secret_storage]`'s names and `default`. Which storages a role may have (`db` in
/// standalone only) is the role's to check.
/// Each plugin's name, command and versions (decision 510).
fn check_extensions(section: &crate::config::ExtensionsSection) -> Result<(), ConfigError> {
    for (name, plugin) in &section.plugins {
        schedr_model::task::check_extension_name(name)
            .map_err(|error| ConfigError::Extensions(format!("[extensions.{name}]: {error}")))?;
        // A relative path would be found from the attempt's working directory, the DAG root,
        // which holds the DAG author's files (Grok's review of M5.2i): absolute, or a bare name
        // found on `PATH`.
        let program = plugin.command.first().map_or("", String::as_str);
        if program.is_empty() || (program.contains('/') && !program.starts_with('/')) {
            return Err(ConfigError::Extensions(format!(
                "[extensions.{name}] command: the plugin's program, an absolute path or a name \
                 found on PATH, then its arguments, e.g. [\"/opt/acme/schedr-ext-{name}\"]"
            )));
        }
        if plugin.versions.is_empty() {
            return Err(ConfigError::Extensions(format!(
                "[extensions.{name}] versions: the versions it serves, e.g. [\"1.0\"]"
            )));
        }
        for version in &plugin.versions {
            schedr_model::task::check_extension_version(version)
                .map_err(|error| ConfigError::Extensions(format!("[extensions.{name}] {error}")))?;
        }
        // Its panels may draw the built-in service metrics it collects: none without a service
        // (agy's review: one turned off passed here, then the master refused all its figures).
        let builtins = if plugin.service.is_empty() {
            Vec::new()
        } else {
            plugin.builtin_service_metrics.kept()
        };
        schedr_model::extension::check_own_declarations(
            &schedr_model::extension::Declarations {
                metrics: plugin.metrics.clone(),
                panels: plugin.panels.clone(),
                notifications: plugin.notifications.clone(),
            },
            &builtins,
        )
        .map_err(|error| ConfigError::Extensions(format!("[extensions.{name}] {error}")))?;
        if plugin.max_series == 0 {
            return Err(ConfigError::Extensions(format!(
                "[extensions.{name}] max_series: at least 1"
            )));
        }
        check_service(name, plugin)?;
        if let crate::config::BuiltinServiceMetrics::Named(named) = &plugin.builtin_service_metrics
            && let Some(unknown) = named
                .iter()
                .find(|n| !schedr_model::extension::BUILTIN_SERVICE_METRICS.contains(&n.as_str()))
        {
            return Err(ConfigError::Extensions(format!(
                "[extensions.{name}] builtin_service_metrics: `{unknown}` isn't one; true, false, \
                 or some of {:?}",
                schedr_model::extension::BUILTIN_SERVICE_METRICS
            )));
        }
    }
    Ok(())
}

/// `[extensions.<name>]`'s service keys (decision 518): a program as `command`'s, a ready
/// timeout and a settle window that aren't zero, some memory, declared secrets that are names.
fn check_service(name: &str, plugin: &crate::config::ExtensionPlugin) -> Result<(), ConfigError> {
    let bad = |message: String| {
        Err(ConfigError::Extensions(format!(
            "[extensions.{name}] {message}"
        )))
    };
    let set = !plugin.service.is_empty();
    if !set {
        if !plugin.service_secrets.is_empty() {
            return bad("service_secrets: given without a service".to_owned());
        }
        return Ok(());
    }
    let program = plugin.service.first().map_or("", String::as_str);
    if program.is_empty() || (program.contains('/') && !program.starts_with('/')) {
        return bad(format!(
            "service: the service's program, an absolute path or a name found on PATH, then its \
             arguments, e.g. [\"/opt/acme/schedr-ext-{name}\", \"--service\"]"
        ));
    }
    if plugin.service_ready_timeout.is_zero() {
        return bad("service_ready_timeout: longer than 0s (e.g. \"60s\")".to_owned());
    }
    if plugin.service_accept_timeout.is_zero() {
        return bad("service_accept_timeout: longer than 0s (e.g. \"10s\")".to_owned());
    }
    if plugin.service_settle.is_zero() {
        return bad("service_settle: longer than 0s (e.g. \"24h\")".to_owned());
    }
    if plugin.service_memory < 16 * 1024 * 1024 {
        return bad("service_memory: at least 16MiB (e.g. \"256MiB\")".to_owned());
    }
    for secret in &plugin.service_secrets {
        if secret.default.is_some() {
            return bad(format!(
                "service_secrets: `{secret}` has a default; a secret is named only"
            ));
        }
        secret.check().map_err(|error| {
            ConfigError::Extensions(format!("[extensions.{name}] service_secrets: {error}"))
        })?;
    }
    Ok(())
}

fn check_secret_storage(section: &crate::config::SecretStorageSection) -> Result<(), ConfigError> {
    for name in section.storages.keys() {
        let valid = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            // Its environment variables (`SCHEDR_CONFIG__SECRET_STORAGE__<NAME>__TOKEN`)
            // would split there.
            && !name.contains("__");
        if !valid || name == "default" {
            return Err(ConfigError::SecretStorage(format!(
                "[secret_storage] storage name `{name}`: letters, digits, `_` and `-` only, no `__`, and not `default` \
                 (DAGs write it `secrets.{name}[...]`)"
            )));
        }
    }
    for (name, storage) in &section.storages {
        check_storage_fields(name, storage)?;
        // A prefix matches the start of a path: `etl` would also be `etl-other/...`, and an
        // empty one every path (Grok's finding in M4.6c's security review).
        if let Some(bad) = storage
            .unmasked_prefixes
            .iter()
            .find(|p| p.len() < 2 || !p.ends_with('/') || p.starts_with('/'))
        {
            return Err(ConfigError::SecretStorage(format!(
                "[secret_storage.{name}] unmasked_prefixes: `{bad}` isn't a path prefix ending \
                 with `/`, e.g. `variables/`"
            )));
        }
    }
    // Decision 172: which one `secrets['...']` means is said, never guessed, once there is a
    // choice (adding a second storage mustn't move unnamed secrets elsewhere).
    if section.default.is_none() && section.storages.len() > 1 {
        let configured: Vec<&str> = section.storages.keys().map(String::as_str).collect();
        return Err(ConfigError::SecretStorage(format!(
            "[secret_storage] several storages ({}) and no `default`: set [secret_storage] default to the one \
             `secrets['...']` means",
            configured.join(", ")
        )));
    }
    if let Some(default) = &section.default
        && default != DB_STORAGE
        && !section.storages.contains_key(default)
    {
        let configured: Vec<&str> = section.storages.keys().map(String::as_str).collect();
        return Err(ConfigError::SecretStorage(format!(
            "[secret_storage] default `{default}` isn't a configured storage (configured: {}; `db` in \
             standalone)",
            if configured.is_empty() {
                "none".to_owned()
            } else {
                configured.join(", ")
            }
        )));
    }
    Ok(())
}
