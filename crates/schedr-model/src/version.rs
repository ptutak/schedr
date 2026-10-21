//! The schedr version that produced a piece of stored data, and which versions can read it.
//!
//! Within a major version, changes to DAG JSON (and the protocol, the API, stored outputs) are
//! additive only, so a reader accepts data of the same major and an equal or lower minor
//! version (see PROJECT.md, Upgrades and compatibility).

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// A schedr version as `major.minor`; the patch level never changes compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SchedrVersion {
    /// The major version: data of another major version is never read.
    pub major: u32,
    /// The minor version: a reader reads data of an equal or lower minor version.
    pub minor: u32,
}

/// Why a string isn't a valid schedr version.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a schedr version (expected `major.minor` or `major.minor.patch`)")]
pub struct VersionError(pub String);

impl SchedrVersion {
    /// Creates a version from its parts.
    #[must_use]
    pub const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// The version of this build of schedr.
    ///
    /// # Panics
    ///
    /// Never in practice: the crate version is checked by a unit test.
    #[must_use]
    pub fn current() -> Self {
        env!("CARGO_PKG_VERSION")
            .parse()
            .expect("the crate version is a valid schedr version")
    }

    /// Whether this version can read data produced by `produced`: the same major and an equal
    /// or lower minor version.
    #[must_use]
    pub const fn can_read(self, produced: Self) -> bool {
        self.major == produced.major && produced.minor <= self.minor
    }
}

impl fmt::Display for SchedrVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl FromStr for SchedrVersion {
    type Err = VersionError;

    /// Parses `major.minor` or `major.minor.patch` (the patch level, including any
    /// pre-release suffix, is ignored).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let error = || VersionError(s.to_owned());
        let mut parts = s.split('.');
        let major = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(error)?;
        let minor = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(error)?;
        match (parts.next(), parts.next()) {
            (None, None) => {}
            (Some(patch), None) if !patch.is_empty() => {}
            _ => return Err(error()),
        }
        Ok(Self { major, minor })
    }
}

impl TryFrom<String> for SchedrVersion {
    type Error = VersionError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<SchedrVersion> for String {
    fn from(v: SchedrVersion) -> Self {
        v.to_string()
    }
}

impl JsonSchema for SchedrVersion {
    fn schema_name() -> Cow<'static, str> {
        "SchedrVersion".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": r"^[0-9]+\.[0-9]+$",
            "description": "A schedr version as `major.minor`.",
        })
    }
}
