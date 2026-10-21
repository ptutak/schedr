//! Where configuration comes from: the file, `SCHEDR_CONFIG__*` environment variables, the
//! short aliases, and command-line flags (see PROJECT.md, Configuration).
//!
//! Every source is a `figment` provider whose metadata names the exact origin of a value
//! (the file, `SCHEDR_CONFIG__OPS__PORT`, `SCHEDR_DAG_PATH`, `--ops-port`), so error messages
//! say where a bad value came from.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use figment::value::{Dict, Map, Value};
use figment::{Error, Metadata, Profile, Provider, Source};

#[cfg(test)]
mod tests;

/// The prefix of environment variables that mirror the configuration file.
pub const ENV_PREFIX: &str = "SCHEDR_CONFIG__";

/// The environment variable naming the configuration file.
pub const CONFIG_FILE_ENV: &str = "SCHEDR_CONFIG_FILE";

/// The configuration file read when neither `--config` nor `SCHEDR_CONFIG_FILE` names one.
pub const DEFAULT_CONFIG_FILE: &str = "/etc/schedr/config.toml";

/// Short aliases: environment variable → configuration key.
pub const ALIASES: [(&str, &str); 2] = [
    ("SCHEDR_DAG_PATH", "dags.path"),
    ("SCHEDR_SCRIPT_PATH", "scripts.path"),
];

/// Which configuration file to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFile {
    /// Named by `--config` or `SCHEDR_CONFIG_FILE`: it must exist.
    Explicit(PathBuf),
    /// The default location: read if it exists, otherwise defaults and environment only.
    Default(PathBuf),
}

impl ConfigFile {
    /// The file's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        match self {
            Self::Explicit(p) | Self::Default(p) => p,
        }
    }

    /// Whether a missing file is an error.
    #[must_use]
    pub const fn is_required(&self) -> bool {
        matches!(self, Self::Explicit(_))
    }
}

/// Chooses the configuration file: the `--config` flag, then `SCHEDR_CONFIG_FILE`, then
/// [`DEFAULT_CONFIG_FILE`].
#[must_use]
pub fn config_file(flag: Option<&Path>, env: &BTreeMap<String, String>) -> ConfigFile {
    match (flag, env.get(CONFIG_FILE_ENV)) {
        (Some(path), _) => ConfigFile::Explicit(path.to_owned()),
        (None, Some(path)) if !path.is_empty() => ConfigFile::Explicit(path.into()),
        _ => ConfigFile::Default(DEFAULT_CONFIG_FILE.into()),
    }
}

/// The configuration file's contents, with its path for messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSource<'a> {
    /// Where the contents were read from.
    pub path: &'a Path,
    /// The TOML text.
    pub contents: &'a str,
}

/// A configuration value given on the command line, e.g. `--ops-port 9100`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Override {
    /// The configuration key, dotted (`ops.port`).
    pub key: String,
    /// The value, parsed like an environment variable's.
    pub value: String,
    /// The flag it came from, for messages (`--ops-port`).
    pub flag: String,
}

/// Why an environment variable can't be used as configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    /// A `SCHEDR_CONFIG__` variable with an empty key part (`SCHEDR_CONFIG__DAGS____PATH`).
    #[error("environment variable `{0}` is not a configuration key (empty part between `__`)")]
    InvalidEnvKey(String),
    /// An alias and the full key are both set.
    #[error("`{alias}` and `{var}` set the same key `{key}`; set only one")]
    AliasConflict {
        /// The alias, e.g. `SCHEDR_DAG_PATH`.
        alias: &'static str,
        /// The full variable, e.g. `SCHEDR_CONFIG__DAGS__PATH`.
        var: String,
        /// The key both set.
        key: &'static str,
    },
}

/// The configuration key of a `SCHEDR_CONFIG__` variable (`SCHEDR_CONFIG__DAGS__POLL_INTERVAL`
/// → `dags.poll_interval`), or `None` for other variables.
///
/// # Errors
///
/// Returns [`SourceError::InvalidEnvKey`] if the key has an empty part.
pub fn env_key(var: &str) -> Result<Option<String>, SourceError> {
    let Some(rest) = var.strip_prefix(ENV_PREFIX) else {
        return Ok(None);
    };
    let parts: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
    if parts.iter().any(String::is_empty) {
        return Err(SourceError::InvalidEnvKey(var.to_owned()));
    }
    Ok(Some(parts.join(".")))
}

/// One value from the environment or the command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    key: String,
    value: String,
    origin: String,
}

/// A provider of key/value entries, each with its own origin for messages.
#[derive(Debug, Clone)]
pub(crate) struct Entries {
    name: &'static str,
    entries: Vec<Entry>,
}

impl Provider for Entries {
    fn metadata(&self) -> Metadata {
        let origins: BTreeMap<String, String> = self
            .entries
            .iter()
            .map(|e| (e.key.clone(), e.origin.clone()))
            .collect();
        Metadata::named(self.name).interpolater(move |_: &Profile, keys: &[&str]| {
            let key = keys.join(".");
            origins.get(&key).cloned().unwrap_or(key)
        })
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        let mut dict = Dict::new();
        for entry in &self.entries {
            let value: Value = entry
                .value
                .parse()
                .expect("parsing a figment value never fails");
            if let Some(nested) = figment::util::nest(&entry.key, value).into_dict() {
                dict = merge(dict, nested);
            }
        }
        Ok(Profile::Default.collect(dict))
    }
}

/// Deep-merges `b` into `a` (`b` wins).
fn merge(mut a: Dict, b: Dict) -> Dict {
    for (key, value) in b {
        let merged = match (a.remove(&key), value) {
            (Some(Value::Dict(_, old)), Value::Dict(tag, new)) => Value::Dict(tag, merge(old, new)),
            (_, new) => new,
        };
        a.insert(key, merged);
    }
    a
}

/// The provider for `SCHEDR_CONFIG__*` variables and the short aliases.
///
/// # Errors
///
/// Returns a [`SourceError`] for a malformed `SCHEDR_CONFIG__` variable, or when an alias and
/// its full variable are both set.
pub(crate) fn env_provider(env: &BTreeMap<String, String>) -> Result<Entries, SourceError> {
    let mut entries = Vec::new();
    for (var, value) in env {
        if let Some(key) = env_key(var)? {
            entries.push(Entry {
                key,
                value: value.clone(),
                origin: var.clone(),
            });
        }
    }
    for (alias, key) in ALIASES {
        if let Some(value) = env.get(alias) {
            if let Some(full) = entries.iter().find(|e| e.key == key) {
                return Err(SourceError::AliasConflict {
                    alias,
                    var: full.origin.clone(),
                    key,
                });
            }
            entries.push(Entry {
                key: key.to_owned(),
                value: value.clone(),
                origin: alias.to_owned(),
            });
        }
    }
    Ok(Entries {
        name: "environment variable",
        entries,
    })
}

/// The provider for command-line overrides.
pub(crate) fn override_provider(overrides: &[Override]) -> Entries {
    let entries = overrides
        .iter()
        .map(|o| Entry {
            key: o.key.clone(),
            value: o.value.clone(),
            origin: o.flag.clone(),
        })
        .collect();
    Entries {
        name: "command-line flag",
        entries,
    }
}

/// The metadata of the configuration file, naming its path in messages.
pub(crate) fn file_metadata(path: &Path) -> Metadata {
    Metadata::from("configuration file", Source::File(path.to_owned()))
}
