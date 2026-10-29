//! Loading the configuration: layering the sources, parsing, and validating.

use std::collections::BTreeMap;
use std::path::PathBuf;

use figment::providers::{Format, Toml};
use figment::value::{Dict, Map};
use figment::{Figment, Metadata, Profile, Provider};

use crate::config::{
    BUILTIN_PYTHON, BUILTIN_PYTHON_ORIGIN, Config, DEFAULT_DAG_INTERPRETER, Interpreter,
    url_has_password,
};
use crate::roots::{RootError, resolve_roots};
use crate::sources::{
    FileSource, Override, SourceError, env_provider, file_metadata, override_provider,
};
use crate::units::{format_duration, format_size};

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
        .checked_add(worker.drain_report_period);
    if drain_end.is_none_or(|end| end > worker.termination_grace_period) {
        return Err(ConfigError::DrainPeriods {
            terminate_period: format_duration(worker.drain_terminate_period),
            report_period: format_duration(worker.drain_report_period),
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
    Ok(config)
}
