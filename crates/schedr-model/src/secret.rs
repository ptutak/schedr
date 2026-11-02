//! Secrets in DAG JSON: which secret a reference names, and the secrets a task declares
//! (see PROJECT.md, Secret storage).
//!
//! A secret is `path#key` in a storage, like a Vault KV v2 secret's key; `path` alone is the
//! whole secret (every key). The storage is named, or left out for the default one. Only the
//! names travel: the agent that runs a task fetches the values, never the master.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// A secret in a storage: `path#key`, or `path` for the whole secret.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct SecretName {
    /// The storage it comes from; none for the default one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    /// The secret's path in the storage (`etl/db`).
    pub path: String,
    /// The key in the secret (`password`); none for the whole secret, an object of key to
    /// value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

impl SecretName {
    /// A secret of the default storage.
    #[must_use]
    pub fn new(path: impl Into<String>, key: Option<&str>) -> Self {
        Self {
            storage: None,
            path: path.into(),
            key: key.map(str::to_owned),
        }
    }

    /// `path#key`, or `path` for the whole secret.
    #[must_use]
    pub fn spec(&self) -> String {
        match &self.key {
            Some(key) => format!("{}#{key}", self.path),
            None => self.path.clone(),
        }
    }

    /// What is wrong with it, if anything: its storage name, path and key.
    ///
    /// # Errors
    ///
    /// Returns a message naming the secret as written (`secrets['etl//db#password']`).
    pub fn check(&self) -> Result<(), String> {
        let problem = |reason: &str| Err(format!("`{self}`: {reason}"));
        if let Some(storage) = &self.storage
            && let Err(reason) = check_storage_name(storage)
        {
            return problem(&reason);
        }
        if self.path.len() > 1 && self.path.ends_with('/') {
            return problem("a path doesn't end with `/` (a prefix is `SecretPrefix(...)`)");
        }
        if let Err(reason) = check_path(&self.path, "path") {
            return problem(&reason);
        }
        if let Some(key) = &self.key {
            if key.is_empty() {
                return problem("the key after `#` is empty");
            }
            if key.contains('#') || key.contains('\0') {
                return problem("a key contains neither `#` nor NUL");
            }
        }
        Ok(())
    }
}

impl fmt::Display for SecretName {
    /// As written in Python: `secrets['etl/db#password']`, `secrets.other['etl/db']`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.storage {
            Some(storage) if !attribute(storage) => {
                let (storage, spec) = (py_str(storage), py_str(&self.spec()));
                write!(f, "secrets.storage({storage})[{spec}]")
            }
            Some(storage) => write!(f, "secrets.{storage}[{}]", py_str(&self.spec())),
            None => write!(f, "secrets[{}]", py_str(&self.spec())),
        }
    }
}

/// Python's keywords: a storage so named can't be written `secrets.<name>`.
const PYTHON_KEYWORDS: [&str; 35] = [
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
    "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
    "with", "yield",
];

/// Whether `secrets.<storage>` reaches the storage (as Python's `storage_label` decides): a
/// Python name, not a keyword, not `_` first (refused there), not `storage` (the method).
fn attribute(storage: &str) -> bool {
    !storage.is_empty()
        && !storage.starts_with(|c: char| c.is_ascii_digit() || c == '_')
        && storage
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && storage != "storage"
        && !PYTHON_KEYWORDS.contains(&storage)
}

/// A string as Python's `repr` writes it: in single quotes, unless only double quotes avoid
/// escaping one.
fn py_str(text: &str) -> String {
    use std::fmt::Write as _;
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if c.is_control() => {
                let _ = write!(out, "\\x{:02x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// A secret a task declares in `secrets=`: one it may read on demand (`schedr task-ctx
/// secret get`, `task_ctx.secret(...)`), fetched before its process starts, or a prefix
/// covering every secret under it, fetched when read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SecretDecl {
    /// One secret, or a whole one (every key).
    Secret(SecretName),
    /// Every secret under a prefix (`SecretPrefix('connections/')`), at any depth.
    Prefix {
        /// The storage; none for the default one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        storage: Option<String>,
        /// The prefix, ending in `/`.
        prefix: String,
    },
}

impl SecretDecl {
    /// What is wrong with it, if anything.
    ///
    /// # Errors
    ///
    /// Returns a message naming it as written.
    pub fn check(&self) -> Result<(), String> {
        match self {
            Self::Secret(name) => name.check(),
            Self::Prefix { storage, prefix } => {
                let problem = |reason: &str| Err(format!("`{self}`: {reason}"));
                if let Some(storage) = storage
                    && let Err(reason) = check_storage_name(storage)
                {
                    return problem(&reason);
                }
                let Some(inner) = prefix.strip_suffix('/') else {
                    return problem("a prefix ends with `/`");
                };
                if let Err(reason) = check_path(inner, "prefix") {
                    return problem(&reason);
                }
                Ok(())
            }
        }
    }

    /// Whether it covers `name`, a secret the task reads (the storages as written: none for
    /// the default one).
    #[must_use]
    pub fn covers(&self, name: &SecretName) -> bool {
        match self {
            Self::Secret(declared) => {
                declared.storage == name.storage
                    && declared.path == name.path
                    && (declared.key.is_none() || declared.key == name.key)
            }
            Self::Prefix { storage, prefix } => {
                *storage == name.storage && name.path.starts_with(prefix.as_str())
            }
        }
    }

    /// The storage it names; none for the default one.
    #[must_use]
    pub fn storage(&self) -> Option<&str> {
        match self {
            Self::Secret(name) => name.storage.as_deref(),
            Self::Prefix { storage, .. } => storage.as_deref(),
        }
    }
}

impl fmt::Display for SecretDecl {
    /// As written in Python: the secret, or `SecretPrefix('connections/')`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Secret(name) => name.fmt(f),
            Self::Prefix {
                storage: Some(storage),
                prefix,
            } => write!(
                f,
                "SecretPrefix({}, storage={})",
                py_str(prefix),
                py_str(storage)
            ),
            Self::Prefix {
                storage: None,
                prefix,
            } => write!(f, "SecretPrefix({})", py_str(prefix)),
        }
    }
}

/// Checks a storage's name: the key of its `[secret_storage.<name>]` table.
///
/// # Errors
///
/// Returns why it can't be one.
pub fn check_storage_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("the storage's name is empty".to_owned());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "storage `{name}`: a name has only letters, digits, `_` and `-`"
        ));
    }
    if name == "default" {
        return Err("`default` names the default storage's setting, not a storage".to_owned());
    }
    Ok(())
}

/// Checks a path (or a prefix without its last `/`): `/`-separated, no empty part, no `#`.
fn check_path(path: &str, what: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err(format!("the {what} is empty"));
    }
    if path.contains('#') {
        return Err(format!("a {what} contains no `#` (it separates the key)"));
    }
    if path.contains('\0') {
        return Err(format!("a {what} contains no NUL"));
    }
    if path.split('/').any(str::is_empty) {
        return Err(format!(
            "a {what} has no empty part (no leading `/`, no `//`)"
        ));
    }
    Ok(())
}
