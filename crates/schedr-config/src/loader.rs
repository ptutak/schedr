//! Loading the configuration: layering the sources, parsing, and validating.

use std::collections::BTreeMap;
use std::path::PathBuf;

use figment::providers::{Format, Toml};
use figment::value::{Dict, Map};
use figment::{Figment, Metadata, Profile, Provider};

use crate::config::{BUILTIN_PYTHON, Config, DEFAULT_DAG_INTERPRETER, Interpreter};
use crate::roots::{RootError, resolve_roots};
use crate::sources::{
    FileSource, Override, SourceError, env_provider, file_metadata, override_provider,
};

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
    config
        .interpreters
        .entry(DEFAULT_DAG_INTERPRETER.to_owned())
        .or_insert_with(|| Interpreter::path(PathBuf::from(BUILTIN_PYTHON)));
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
