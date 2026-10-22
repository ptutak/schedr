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
        /// The first run it creates is for the first fire time at or after it; without it,
        /// the moment the master first stored the DAG.
        #[serde(default)]
        start: Option<Timestamp>,
        /// The last run it creates is for the last fire time at or before it.
        #[serde(default)]
        end: Option<Timestamp>,
    },
    /// A fixed interval measured in real time (UTC), unaffected by daylight saving.
    Every {
        /// The interval in milliseconds; fire times are whole intervals from `start`, or from
        /// the Unix epoch without one.
        interval_ms: u64,
        /// Whether missed fire times get runs in order (`true`) or only the most recent
        /// (`false`).
        catchup: bool,
        /// The first run it creates is for the first fire time at or after it; without it,
        /// the moment the master first stored the DAG.
        #[serde(default)]
        start: Option<Timestamp>,
        /// The last run it creates is for the last fire time at or before it.
        #[serde(default)]
        end: Option<Timestamp>,
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

    /// Where the runs it creates start (see the `start` fields); `None` for triggers without
    /// one.
    #[must_use]
    pub const fn start(&self) -> Option<Timestamp> {
        match self {
            Self::Cron { start, .. } | Self::Every { start, .. } => *start,
            Self::None | Self::Once { .. } => None,
        }
    }

    /// Where the runs it creates end (see the `end` fields); `None` for triggers without one.
    #[must_use]
    pub const fn end(&self) -> Option<Timestamp> {
        match self {
            Self::Cron { end, .. } | Self::Every { end, .. } => *end,
            Self::None | Self::Once { .. } => None,
        }
    }
}
