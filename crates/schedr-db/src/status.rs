//! Whether a database's schema fits this build of schedr.
//!
//! `schedr db migrate` applies the migrations; the master refuses to start against an
//! outdated schema (see PROJECT.md, State storage). A database migrated by a **newer** schedr
//! is still usable: within a major version schema changes are additive, so an older master or
//! webserver keeps working during a rolling upgrade (see Upgrades and compatibility).

#[cfg(test)]
mod tests;

/// How the applied migrations compare with the ones built into this binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaStatus {
    /// No migration has been applied: run `schedr db migrate`.
    Uninitialized,
    /// Exactly the migrations of this binary are applied.
    UpToDate,
    /// Migrations of this binary are missing: run `schedr db migrate`.
    Outdated {
        /// The versions not applied yet.
        missing: Vec<i64>,
    },
    /// The database has migrations this binary doesn't know (a newer schedr migrated it).
    Newer {
        /// The versions this binary doesn't know.
        unknown: Vec<i64>,
    },
}

impl SchemaStatus {
    /// Compares the versions built into the binary with the versions applied to the database.
    #[must_use]
    pub fn compare(embedded: &[i64], applied: &[i64]) -> Self {
        if applied.is_empty() && !embedded.is_empty() {
            return Self::Uninitialized;
        }
        let missing: Vec<i64> = embedded
            .iter()
            .copied()
            .filter(|v| !applied.contains(v))
            .collect();
        if !missing.is_empty() {
            return Self::Outdated { missing };
        }
        let unknown: Vec<i64> = applied
            .iter()
            .copied()
            .filter(|v| !embedded.contains(v))
            .collect();
        if unknown.is_empty() {
            Self::UpToDate
        } else {
            Self::Newer { unknown }
        }
    }

    /// Whether schedr can run against this schema: up to date, or migrated by a newer schedr.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::UpToDate | Self::Newer { .. })
    }
}
