//! Connections (PROJECT.md, Connections; decisions 53 and 174): what the parser records of a
//! `@connection` function, so the agent can render it for a task by name.
//!
//! A connection's values may name secrets; the record holds the names (as in DAG JSON),
//! never a value. Its name is unique in a DAG root.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ids::TaskId;
use crate::secret::SecretName;
use crate::value::{Reference, Value};

#[cfg(test)]
mod tests;

/// A connection as the runner reports it when parsing (`file_parsed`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionDef {
    /// Its name: the function's, or a typed connection's first argument (an identifier,
    /// unique in the root).
    pub name: String,
    /// A typed connection's type (decision 174); none for a `@connection` function.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<ConnectionType>,
    /// Its parameters' values, by parameter: literals, secrets, and t-strings of them.
    pub params: BTreeMap<String, Value>,
}

/// The built-in connection types (decision 174): each a fixed shape, given to a command as
/// variables and files of its own (M4.7b2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionType {
    /// An HTTP API (httpx).
    Http,
    /// An SSH host (paramiko).
    Ssh,
    /// A MongoDB database (pymongo).
    Mongo,
    /// A Postgres or MySQL database, by URL (psycopg 3, `PyMySQL`).
    Sql,
    /// Keys of one's own.
    Generic,
}

impl ConnectionDef {
    /// Checks what a runner can't be trusted to have checked: a name like a task ID, and
    /// only secrets referenced (a connection is defined outside any DAG).
    ///
    /// # Errors
    ///
    /// What is wrong, naming the connection.
    pub fn check(&self) -> Result<(), String> {
        TaskId::new(&self.name).map_err(|e| format!("connection `{}`: {e}", self.name))?;
        for (param, value) in &self.params {
            for reference in value.references() {
                match reference {
                    Reference::Secret(name) => name
                        .check()
                        .map_err(|e| format!("connection `{}`, `{param}`: {e}", self.name))?,
                    _ => {
                        return Err(format!(
                            "connection `{}`, `{param}`: only secrets can be referenced",
                            self.name
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// The secrets its values name, in order (each once).
    #[must_use]
    pub fn secrets(&self) -> Vec<&SecretName> {
        let mut found: Vec<&SecretName> = Vec::new();
        for reference in self.params.values().flat_map(Value::references) {
            if let Reference::Secret(name) = reference
                && !found.contains(&name)
            {
                found.push(name);
            }
        }
        found
    }
}

/// The most a connection's recorded values may take, as JSON: they travel with each
/// assignment of a task that opens it, within the worker's 4 MiB message. A file defining a
/// bigger one fails to import.
pub const MAX_CONNECTION_SIZE: usize = 1024 * 1024;

/// A root's connections (decision 174), from its current parse: what the agent renders by
/// name for the root's tasks.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Registry {
    /// The connections, by name.
    #[serde(default)]
    pub connections: BTreeMap<String, ConnectionDef>,
    /// Names that can't be used, with why: defined in more than one file, or invalid.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub refused: BTreeMap<String, String>,
    /// The file defining each connection, relative to the root: a test imports it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
}

impl Registry {
    /// The registry of a root's parsed files (in any order). A file that failed to import
    /// records none; one the master refused after it imported (a DAG of it invalid) keeps its
    /// connections: one bad DAG doesn't fail the tasks of other files using them.
    #[must_use]
    pub fn of(files: &[crate::runner::ParsedFile]) -> Self {
        let mut places: BTreeMap<&str, Vec<(&str, &ConnectionDef)>> = BTreeMap::new();
        for file in files {
            for def in &file.connections {
                places
                    .entry(def.name.as_str())
                    .or_default()
                    .push((file.path.as_str(), def));
            }
        }
        let mut registry = Self::default();
        for (name, mut defined) in places {
            defined.sort_by_key(|(path, _)| *path);
            if let [(path, def)] = defined.as_slice() {
                match def.check() {
                    Ok(()) => {
                        registry.connections.insert(name.to_owned(), (*def).clone());
                        registry.files.insert(name.to_owned(), (*path).to_owned());
                    }
                    Err(why) => {
                        registry.refused.insert(name.to_owned(), why);
                    }
                }
            } else {
                let paths: Vec<&str> = defined.iter().map(|(path, _)| *path).collect();
                registry.refused.insert(
                    name.to_owned(),
                    format!(
                        "connection `{name}` is defined in more than one file ({}): rename one",
                        paths.join(", ")
                    ),
                );
            }
        }
        registry
    }

    /// Every connection's name, the refused ones' after.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.connections.keys().chain(self.refused.keys())
    }

    /// Only the connections named in `names`, and why those refused are: what one task
    /// declares, sent with its assignment (the agent serves no other).
    #[must_use]
    pub fn only(&self, names: &[String]) -> Self {
        let mut subset = Self::default();
        for name in names {
            if let Some(def) = self.connections.get(name) {
                subset.connections.insert(name.clone(), def.clone());
                if let Some(path) = self.files.get(name) {
                    subset.files.insert(name.clone(), path.clone());
                }
            } else if let Some(why) = self.refused.get(name) {
                subset.refused.insert(name.clone(), why.clone());
            }
        }
        subset
    }

    /// What is wrong with the connections `dag`'s tasks declare (decision 179), one line
    /// each: `task `load`: connections=: <why>`.
    #[must_use]
    pub fn problems(&self, dag: &crate::Dag) -> Vec<String> {
        let trigger = match &dag.trigger {
            crate::Trigger::Event { task, .. } => Some(task.as_ref()),
            _ => None,
        };
        dag.tasks
            .iter()
            .chain(trigger)
            .flat_map(|task| {
                task.connections.iter().filter_map(move |name| {
                    self.get(name)
                        .err()
                        .map(|why| format!("task `{}`: connections=: {why}", task.id))
                })
            })
            .collect()
    }

    /// The connection named `name`.
    ///
    /// # Errors
    ///
    /// Why it can't be used: unknown, defined twice, or invalid.
    pub fn get(&self, name: &str) -> Result<&ConnectionDef, String> {
        if let Some(def) = self.connections.get(name) {
            return Ok(def);
        }
        Err(self.refused.get(name).cloned().unwrap_or_else(|| {
            format!(
                "connection `{name}` isn't defined in the DAG folder (a `@connection` function \
                 in a file of the DAG root that imports)"
            )
        }))
    }
}

/// A connection's prefix in a command's `connections={"DB": ...}` (decision 184): the start
/// of an environment variable's name, not schedr's own.
///
/// # Errors
///
/// What is wrong with it.
pub fn check_prefix(prefix: &str) -> Result<(), String> {
    let mut chars = prefix.chars();
    let starts = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if !starts || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "`{prefix}`: a prefix is letters, digits and `_`, not starting with a digit"
        ));
    }
    if prefix.to_ascii_uppercase().starts_with("SCHEDR") {
        return Err(format!(
            "`{prefix}`: prefixes starting with SCHEDR are schedr's own"
        ));
    }
    Ok(())
}
