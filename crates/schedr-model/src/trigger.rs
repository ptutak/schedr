//! A DAG's trigger, as stored in DAG JSON: what starts its runs.
//!
//! Cron expressions and time zone names are validated by the master when it stores a DAG;
//! here they are plain data (see archive/PROJECT.md, Native API: triggers are typed objects).

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
    /// A `@trigger` function, which runs in a worker's watcher host and yields events: each new
    /// one starts a run.
    Event {
        /// Its function as a task of kind `sensor`: its literal arguments, whether it is called
        /// every so often, its queue, its secrets and connections. It isn't in the graph.
        task: Box<crate::task::Task>,
        /// Started again from its bookmark when no event came for this long; `None`: never.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        restart_if_idle_ms: Option<u64>,
        /// It runs from this moment on.
        #[serde(default)]
        start: Option<Timestamp>,
        /// It stops at this moment.
        #[serde(default)]
        end: Option<Timestamp>,
        /// `@trigger(wait_for=RunOf(...), run_retries=N)` (decision 353): a run its function
        /// raised on is tried again this many times, then passed, recorded; `None`: tried until
        /// `schedr triggers skip` passes it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_retries: Option<u32>,
    },
    /// `trigger=RunOf(...)` (decision 353): a run per run of another DAG (or its task) that
    /// succeeded while this one is live, planned for that run's `planned_for` plus `lag`, or for
    /// the moment the master saw it.
    RunOf {
        /// Which runs.
        run_of: crate::task::RunOf,
    },
    /// `trigger=EventNamed(...)` (decision 354): a run per event of that name posted through the
    /// API while this DAG is live, keyed by the event's key, planned for its `at`.
    EventNamed {
        /// Which events: its name (a trigger takes no key).
        event: crate::task::EventNamed,
    },
}

/// The longest key an event may have, in bytes: it is stored with every event and sent back as a
/// trigger's `after`. Python says it first ([`RunEvent`]'s data is limited by the trigger's
/// `max_output_size`).
pub const MAX_EVENT_KEY: usize = 1024;

/// Whether a terminal or a page would act on `c` instead of showing it: a control character,
/// a line or paragraph separator, or a bidirectional override or isolate (Grok's review of
/// M4.11e: a key with U+2028 broke `schedr events list`'s table).
#[must_use]
pub const fn acts_on_display(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{2028}' | '\u{2029}' | '\u{200E}' | '\u{200F}' | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
        )
}

/// Whether a text can be an event's key.
///
/// # Errors
///
/// What is wrong with it.
pub fn check_event_key(key: &str) -> Result<(), String> {
    if key.is_empty() || key.len() > MAX_EVENT_KEY || key.chars().any(acts_on_display) {
        return Err(format!(
            "an event's key has 1 to {MAX_EVENT_KEY} bytes and no control, line-separator or \
             bidirectional characters, got {} bytes",
            key.len()
        ));
    }
    Ok(())
}

/// What a `@trigger` found, as stored and given to the run it starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunEvent {
    /// Its key, unique in its DAG.
    pub key: String,
    /// Its data, as stored JSON.
    #[schemars(with = "serde_json::Value")]
    pub data: serde_json::Value,
    /// When it happened, if it said; the run is planned for this moment.
    #[serde(default)]
    pub at: Option<Timestamp>,
}

impl Trigger {
    /// Whether the scheduler never creates runs: only people (or other DAGs) do.
    #[must_use]
    pub const fn is_manual_only(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Whether events start its runs: it has no fire times.
    #[must_use]
    pub const fn is_event(&self) -> bool {
        matches!(self, Self::Event { .. })
    }

    /// Whether its runs bring an event: a `@trigger`'s, or the run of another DAG that started
    /// it (`RunOf`).
    #[must_use]
    pub const fn brings_event(&self) -> bool {
        matches!(
            self,
            Self::Event { .. } | Self::RunOf { .. } | Self::EventNamed { .. }
        )
    }

    /// Whether missed fire times get runs in order; `false` for triggers without `catchup`
    /// (a single fire time has nothing to catch up on).
    #[must_use]
    pub const fn catchup(&self) -> bool {
        match self {
            Self::Cron { catchup, .. } | Self::Every { catchup, .. } => *catchup,
            Self::None
            | Self::Once { .. }
            | Self::Event { .. }
            | Self::RunOf { .. }
            | Self::EventNamed { .. } => false,
        }
    }

    /// Where the runs it creates start (see the `start` fields); `None` for triggers without
    /// one.
    #[must_use]
    pub const fn start(&self) -> Option<Timestamp> {
        match self {
            Self::Cron { start, .. } | Self::Every { start, .. } | Self::Event { start, .. } => {
                *start
            }
            Self::None | Self::Once { .. } | Self::RunOf { .. } | Self::EventNamed { .. } => None,
        }
    }

    /// Where the runs it creates end (see the `end` fields); `None` for triggers without one.
    #[must_use]
    pub const fn end(&self) -> Option<Timestamp> {
        match self {
            Self::Cron { end, .. } | Self::Every { end, .. } | Self::Event { end, .. } => *end,
            Self::None | Self::Once { .. } | Self::RunOf { .. } | Self::EventNamed { .. } => None,
        }
    }
}
