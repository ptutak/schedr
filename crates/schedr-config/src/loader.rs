//! Loading the configuration: layering the sources, parsing, and validating.

use std::collections::BTreeMap;
use std::path::PathBuf;

use figment::providers::{Format, Toml};
use figment::value::{Dict, Map};
use figment::{Figment, Metadata, Profile, Provider};

use crate::config::{
    BUILTIN_PYTHON, BUILTIN_PYTHON_ORIGIN, Config, DEFAULT_DAG_INTERPRETER, HIDDEN_PREFIX,
    Interpreter, Timings, url_has_password,
};
use crate::roots::{RootError, resolve_roots};
use crate::sources::{
    FileSource, Override, SourceError, env_provider, file_metadata, override_provider,
};
use crate::units::{format_duration, format_size, parse_duration};

/// The smallest `[master] grpc_max_message_size`: gRPC's default, which workers rely on.
const MIN_MESSAGE_SIZE: u64 = 4 * 1024 * 1024;

/// The largest `[master] grpc_max_message_size`.
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
    /// A `SCHEDR_HIDDEN__*` variable that isn't a timing, or not a duration.
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
    /// `[master] grpc_max_message_size` is out of its range.
    #[error(
        "[master] grpc_max_message_size ({size}) must be from 4MiB (gRPC's default, which \
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
    /// `[mapping] max_instances` is zero.
    #[error("[mapping] max_instances must be at least 1")]
    NoInstances,
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
    fn from(error: figment::Error) -> Self {
        let messages: Vec<String> = error.into_iter().map(|e| e.to_string()).collect();
        Self::Invalid(messages.join("\n"))
    }
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
    let from_file = |key: &str| {
        figment
            .find_metadata(key)
            .is_some_and(|m| m.source.as_ref().is_some_and(|s| s.file_path().is_some()))
    };
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
        if from_file(key) && url.as_deref().is_some_and(url_has_password) {
            return Err(ConfigError::SecretInFile { key, env });
        }
    }
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
    if !(MIN_MESSAGE_SIZE..=MAX_MESSAGE_SIZE).contains(&config.master.grpc_max_message_size) {
        return Err(ConfigError::MessageSize {
            size: format_size(config.master.grpc_max_message_size),
        });
    }
    if config.mapping.max_instances == 0 {
        return Err(ConfigError::NoInstances);
    }
    check_not_zero(&config)?;
    check_secret_storage(&config.secret_storage)?;
    for name in config.secret_storage.storages.keys() {
        // As every credential: from the environment, never the file.
        if from_file(&format!("secret_storage.{name}.token")) {
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
    config.dag_roots = resolve_roots(
        "SCHEDR_DAG_PATH",
        "dags",
        &config.dags.path,
        &config.dags.roots,
    )?;
    config.script_roots = resolve_roots(
        "SCHEDR_SCRIPT_PATH",
        "scripts",
        &config.scripts.path,
        &config.scripts.roots,
    )?;
    crate::roots::check_apart(&config.dag_roots, &config.script_roots)?;
    Ok(config)
}

/// The timings a zero would break everything with: every task timed out at once, a panicking
/// interval, or every log deleted. Where zero means something (no wait, no retry) it stays allowed.
fn check_not_zero(config: &Config) -> Result<(), ConfigError> {
    let fixed = [
        ("[dags] poll_interval", config.dags.poll_interval),
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
        ("[worker] cleanup_timeout", config.worker.cleanup_timeout),
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

/// The fixed timings, with what `SCHEDR_HIDDEN__*` variables set.
fn hidden_timings(env: &BTreeMap<String, String>) -> Result<Timings, ConfigError> {
    let mut timings = Timings::default();
    for (name, text) in env {
        let Some(key) = name.strip_prefix(HIDDEN_PREFIX) else {
            continue;
        };
        let timing = match key {
            "MASTER__TICK_INTERVAL" => &mut timings.tick_interval,
            "MASTER__HEARTBEAT_INTERVAL" => &mut timings.heartbeat_interval,
            "MASTER__LEASE_TIMEOUT" => &mut timings.lease_timeout,
            "MASTER__SCRIPT_ROOT_GRACE" => &mut timings.script_root_grace,
            "MASTER__LEASE_PERIOD" => &mut timings.lease_period,
            "MASTER__READ_BACK_WINDOW" => &mut timings.read_back_window,
            "MASTER__READ_BACK_INTERVAL" => &mut timings.read_back_interval,
            "DATABASE__QUERY_TIMEOUT" => &mut timings.query_timeout,
            "OPS__LOOP_STALL_TIMEOUT" => &mut timings.loop_stall_timeout,
            "MASTER__SET_ASIDE" => &mut timings.set_aside,
            "MASTER__CLEANUP_MOVE_WAIT" => &mut timings.cleanup_move_wait,
            "MASTER__LISTEN_RETRY" => &mut timings.listen_retry,
            "MASTER__QUIET_DAG_AFTER" => &mut timings.quiet_dag_after,
            "MASTER__QUIET_DAG_WAKE_AHEAD" => &mut timings.quiet_dag_wake_ahead,
            "MASTER__QUIET_DAG_FULL_PASS_INTERVAL" => &mut timings.quiet_dag_full_pass_interval,
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
            "WORKER__LOG_SWEEP_INTERVAL" => &mut timings.log_sweep_interval,
            "WORKER__ORPHAN_POLL" => &mut timings.orphan_poll,
            "WORKER__ORPHAN_KILL_WAIT" => &mut timings.orphan_kill_wait,
            "WORKER__ORPHAN_GRACE" => &mut timings.orphan_grace,
            "WORKER__RECONNECT_BACKOFF_FIRST" => &mut timings.reconnect_backoff_first,
            "WORKER__RECONNECT_BACKOFF_MAX" => &mut timings.reconnect_backoff_max,
            "WORKER__ALIVE_INTERVAL" => &mut timings.worker_alive_interval,
            "WORKER__DEREGISTER_WAIT" => &mut timings.deregister_wait,
            "WORKER__DRAIN_TICK" => &mut timings.drain_tick,
            "WORKER__DRAIN_ACK_WAIT" => &mut timings.drain_ack_wait,
            "WORKER__STOP_POLL" => &mut timings.stop_poll,
            "WORKER__CONNECTION_JOB_CANCEL_GRACE" => &mut timings.connection_job_cancel_grace,
            "WORKER__VAULT_MIN_RENEW_WAIT" => &mut timings.vault_min_renew_wait,
            "WORKER__VAULT_RENEW_CHECK" => &mut timings.vault_renew_check,
            "WORKER__VAULT_RENEW_RETRY" => &mut timings.vault_renew_retry,
            "MASTER__ROUND_GAP" => &mut timings.round_gap,
            "MASTER__NOTIFICATION_RETRY" => &mut timings.notification_retry,
            "MASTER__ALIVE_INTERVAL" => &mut timings.master_alive_interval,
            "MASTER__GAUGE_REFRESH" => &mut timings.gauge_refresh,
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
            "UI__RELAY_RETRY_FIRST" => &mut timings.ui_relay_retry_first,
            "UI__RELAY_RETRY_MAX" => &mut timings.ui_relay_retry_max,
            "STANDALONE__WORKER_STOP_AFTER_FAILURE" => &mut timings.worker_stop_after_failure,
            "TLS__ACCEPT_RETRY" => &mut timings.accept_retry,
            _ => {
                return Err(ConfigError::BadHiddenTiming {
                    name: name.clone(),
                    reason: "no such timing".to_owned(),
                });
            }
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
        timings.overridden.push(name.clone());
    }
    Ok(timings)
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
