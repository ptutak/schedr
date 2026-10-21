//! Identifiers: DAG IDs and task IDs.
//!
//! Both are validated when created, so a value of these types is always well formed. They
//! serialize as plain strings.

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// The longest allowed identifier, in bytes.
pub const MAX_ID_LEN: usize = 250;

/// Characters allowed in a DAG ID, as shown in error messages.
const DAG_ID_ALLOWED: &str = "ASCII letters, digits and `_ . - : / #`";

/// Characters allowed in a task ID, as shown in error messages.
const TASK_ID_ALLOWED: &str = "ASCII letters, digits and `_ . -`";

/// Why a string isn't a valid identifier.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// The identifier is empty.
    #[error("{kind} is empty")]
    Empty {
        /// Which kind of identifier ("DAG ID", "task ID").
        kind: &'static str,
    },
    /// The identifier is longer than [`MAX_ID_LEN`] bytes.
    #[error("{kind} `{id}` is longer than {MAX_ID_LEN} bytes")]
    TooLong {
        /// Which kind of identifier.
        kind: &'static str,
        /// The rejected identifier.
        id: String,
    },
    /// The identifier contains a character that isn't allowed.
    #[error("{kind} `{id}` contains `{ch}`; allowed are {allowed}")]
    InvalidChar {
        /// Which kind of identifier.
        kind: &'static str,
        /// The rejected identifier.
        id: String,
        /// The first character that isn't allowed.
        ch: char,
        /// A description of the allowed characters.
        allowed: &'static str,
    },
    /// A task ID has an empty part around a `.` (group separator).
    #[error("task ID `{id}` has an empty group or task name around `.`")]
    EmptyPart {
        /// The rejected identifier.
        id: String,
    },
}

/// Checks length and characters shared by every identifier kind.
fn check_chars(
    kind: &'static str,
    id: &str,
    allowed: &'static str,
    is_allowed: fn(char) -> bool,
) -> Result<(), IdError> {
    if id.is_empty() {
        return Err(IdError::Empty { kind });
    }
    if id.len() > MAX_ID_LEN {
        return Err(IdError::TooLong {
            kind,
            id: id.to_owned(),
        });
    }
    if let Some(ch) = id.chars().find(|&c| !is_allowed(c)) {
        return Err(IdError::InvalidChar {
            kind,
            id: id.to_owned(),
            ch,
            allowed,
        });
    }
    Ok(())
}

/// The ID of a DAG: a native DAG's explicit ID, or a standalone job's reference
/// (`etl-repo:team_a/schedr.toml#report_daily`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DagId(String);

impl DagId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    ///
    /// Returns an [`IdError`] if `id` is empty, too long or contains a character that isn't
    /// allowed.
    pub fn new(id: impl Into<String>) -> Result<Self, IdError> {
        let id = id.into();
        check_chars("DAG ID", &id, DAG_ID_ALLOWED, |c| {
            c.is_ascii_alphanumeric() || "_.-:/#".contains(c)
        })?;
        Ok(Self(id))
    }

    /// The ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The ID of a task within its DAG, `group.task` for tasks in groups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TaskId(String);

impl TaskId {
    /// Validates `id` and wraps it.
    ///
    /// # Errors
    ///
    /// Returns an [`IdError`] if `id` is empty, too long, contains a character that isn't
    /// allowed, or has an empty part around a `.`.
    pub fn new(id: impl Into<String>) -> Result<Self, IdError> {
        let id = id.into();
        check_chars("task ID", &id, TASK_ID_ALLOWED, |c| {
            c.is_ascii_alphanumeric() || "_.-".contains(c)
        })?;
        if id.split('.').any(str::is_empty) {
            return Err(IdError::EmptyPart { id });
        }
        Ok(Self(id))
    }

    /// The ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Implements the string conversions and the JSON Schema shared by both identifier types.
macro_rules! id_conversions {
    ($ty:ident, $pattern:literal) => {
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $ty {
            type Err = IdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $ty {
            type Error = IdError;

            fn try_from(s: String) -> Result<Self, Self::Error> {
                Self::new(s)
            }
        }

        impl From<$ty> for String {
            fn from(id: $ty) -> Self {
                id.0
            }
        }

        impl JsonSchema for $ty {
            fn schema_name() -> Cow<'static, str> {
                stringify!($ty).into()
            }

            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!({
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_ID_LEN,
                    "pattern": $pattern,
                })
            }
        }
    };
}

id_conversions!(DagId, r"^[A-Za-z0-9_.:/#-]+$");
id_conversions!(TaskId, r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$");
