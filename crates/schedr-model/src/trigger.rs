//! A DAG's trigger, as stored in DAG JSON: what starts its runs.
//!
//! Cron expressions and time zone names are validated by the master when it stores a DAG;
//! here they are plain data (see PROJECT.md, Native API: triggers are typed objects).

use jiff::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// What starts runs of a DAG besides people (and other DAGs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trigger {
    /// Nothing: manual runs only.
    None,
    /// A cron expression (5 or 6 fields, or a preset such as `@daily`) evaluated in a time zone.
    Cron {
        /// The cron expression.
        expression: String,
        /// An IANA time zone name, e.g. `Europe/Warsaw`; `UTC` by default in the Python API.
        timezone: String,
        /// Whether missed fire times get runs in order (`true`) or only the most recent
        /// (`false`).
        catchup: bool,
    },
    /// A fixed interval measured in real time (UTC), unaffected by daylight saving.
    Every {
        /// The interval in milliseconds.
        interval_ms: u64,
        /// Whether missed fire times get runs in order (`true`) or only the most recent
        /// (`false`).
        catchup: bool,
    },
    /// A single fire time (it has no previous fire time).
    Once {
        /// The fire time.
        at: Timestamp,
    },
}

impl Trigger {
    /// Whether the scheduler never creates runs: only people (or other DAGs) do.
    #[must_use]
    pub const fn is_manual_only(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Whether missed fire times get runs in order; `false` for triggers without `catchup`
    /// (a single fire time has nothing to catch up on).
    #[must_use]
    pub const fn catchup(&self) -> bool {
        match self {
            Self::Cron { catchup, .. } | Self::Every { catchup, .. } => *catchup,
            Self::None | Self::Once { .. } => false,
        }
    }
}
