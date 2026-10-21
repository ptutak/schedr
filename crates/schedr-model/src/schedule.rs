//! A DAG's schedule, as stored in DAG JSON.
//!
//! Cron expressions and time zone names are validated by the master when it stores a DAG;
//! here they are plain data (see PROJECT.md, Native API: schedules are typed objects).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// When the scheduler creates runs of a DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Schedule {
    /// No scheduled runs: manual runs only.
    None,
    /// A cron expression (5 or 6 fields, or a preset such as `@daily`) evaluated in a time zone.
    Cron {
        /// The cron expression.
        expression: String,
        /// An IANA time zone name, e.g. `Europe/Warsaw`; `UTC` by default in the Python API.
        timezone: String,
    },
    /// A fixed interval measured in real time (UTC), unaffected by daylight saving.
    Every {
        /// The interval in milliseconds.
        interval_ms: u64,
    },
}

impl Schedule {
    /// Whether the scheduler never creates runs: only people (or other DAGs) do.
    #[must_use]
    pub const fn is_manual_only(&self) -> bool {
        matches!(self, Self::None)
    }
}
