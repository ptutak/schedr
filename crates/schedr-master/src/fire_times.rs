//! Fire times of a DAG's time trigger, with schedr's daylight-saving rules.
//!
//! A cron expression is matched against **wall-clock times** in its time zone (croner parses
//! and matches it); each match becomes real moments by these rules (see PROJECT.md, Native API:
//! daylight-saving changes):
//!
//! - **Gap** (spring forward, the time doesn't exist): the fire time moves to the first moment
//!   after the gap; several matches in one gap collapse into that one fire time.
//! - **Overlap** (fall back, the time happens twice): with a **fixed hour field** (`30 2 * * *`)
//!   it fires once, at the first occurrence; with a wildcard, step, range or list in the hour
//!   field (`*/15 * * * *`) it fires at each occurrence.
//!
//! `Every` is measured in real time from its anchor (`start`, or the Unix epoch without one);
//! `Once` has one fire time; no trigger has none.

use jiff::civil::DateTime;
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{SignedDuration, Timestamp};
use schedr_model::Trigger;

#[cfg(test)]
mod tests;

/// How far around a moment wall-clock matches are looked at: more than any daylight-saving
/// shift, so the matches of both sides of an overlap are compared.
const MARGIN: SignedDuration = SignedDuration::from_hours(3);

/// A time trigger that can't be evaluated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FireTimesError {
    /// The cron expression doesn't parse.
    #[error("cron expression `{expression}`: {reason}")]
    Cron {
        /// The expression.
        expression: String,
        /// Why it doesn't parse.
        reason: String,
    },
    /// The time zone isn't known.
    #[error("unknown time zone `{0}`")]
    TimeZone(String),
    /// An `Every` interval of zero.
    #[error("the interval of an Every trigger must be positive")]
    ZeroInterval,
}

/// The fire times of a time trigger.
#[derive(Debug, Clone)]
pub enum FireTimes {
    /// No trigger: no fire times.
    None,
    /// A cron expression in a time zone.
    Cron(Box<CronTimes>),
    /// A fixed interval from an anchor.
    Every {
        /// A fire time the others are whole intervals from.
        anchor: Timestamp,
        /// The interval in nanoseconds (positive).
        interval_ns: i128,
    },
    /// One fire time.
    Once(Timestamp),
}

/// A parsed cron trigger.
#[derive(Debug, Clone)]
pub struct CronTimes {
    cron: croner::Cron,
    tz: TimeZone,
    hour_fixed: bool,
}

impl FireTimes {
    /// The fire times of `trigger`; an `Every` trigger counts from its `start` when given.
    ///
    /// # Errors
    ///
    /// [`FireTimesError`] for a cron expression that doesn't parse, an unknown time zone or a
    /// zero interval.
    pub fn new(trigger: &Trigger) -> Result<Self, FireTimesError> {
        match trigger {
            Trigger::None => Ok(Self::None),
            Trigger::Once { at } => Ok(Self::Once(*at)),
            Trigger::Every {
                interval_ms, start, ..
            } => {
                if *interval_ms == 0 {
                    return Err(FireTimesError::ZeroInterval);
                }
                Ok(Self::Every {
                    anchor: start.unwrap_or(Timestamp::UNIX_EPOCH),
                    interval_ns: i128::from(*interval_ms) * 1_000_000,
                })
            }
            Trigger::Cron {
                expression,
                timezone,
                ..
            } => Ok(Self::Cron(Box::new(CronTimes::new(expression, timezone)?))),
        }
    }

    /// The first fire time strictly after `t`.
    #[must_use]
    pub fn next_after(&self, t: Timestamp) -> Option<Timestamp> {
        match self {
            Self::None => None,
            Self::Once(at) => (*at > t).then_some(*at),
            Self::Every {
                anchor,
                interval_ns,
            } => {
                let k = (t.as_nanosecond() - anchor.as_nanosecond()).div_euclid(*interval_ns) + 1;
                Timestamp::from_nanosecond(anchor.as_nanosecond() + k * interval_ns).ok()
            }
            Self::Cron(cron) => cron.next_after(t),
        }
    }

    /// The last fire time strictly before `t`.
    #[must_use]
    pub fn previous_before(&self, t: Timestamp) -> Option<Timestamp> {
        match self {
            Self::None => None,
            Self::Once(at) => (*at < t).then_some(*at),
            Self::Every {
                anchor,
                interval_ns,
            } => {
                let offset = t.as_nanosecond() - anchor.as_nanosecond();
                let k = (offset - 1).div_euclid(*interval_ns);
                Timestamp::from_nanosecond(anchor.as_nanosecond() + k * interval_ns).ok()
            }
            Self::Cron(cron) => cron.previous_before(t),
        }
    }

    /// The first fire time at or after `t`.
    #[must_use]
    pub fn next_at_or_after(&self, t: Timestamp) -> Option<Timestamp> {
        if self.is_fire_time(t) {
            Some(t)
        } else {
            self.next_after(t)
        }
    }

    /// The last fire time at or before `t`.
    #[must_use]
    pub fn last_at_or_before(&self, t: Timestamp) -> Option<Timestamp> {
        if self.is_fire_time(t) {
            Some(t)
        } else {
            self.previous_before(t)
        }
    }

    /// Whether `t` is a fire time.
    #[must_use]
    pub fn is_fire_time(&self, t: Timestamp) -> bool {
        t.checked_sub(SignedDuration::from_nanos(1))
            .ok()
            .and_then(|before| self.next_after(before))
            == Some(t)
    }
}

impl CronTimes {
    /// Parses a cron expression (5 fields, 6 with seconds first, or a preset) in a time zone.
    ///
    /// # Errors
    ///
    /// [`FireTimesError::Cron`] or [`FireTimesError::TimeZone`].
    pub fn new(expression: &str, timezone: &str) -> Result<Self, FireTimesError> {
        let tz =
            TimeZone::get(timezone).map_err(|_| FireTimesError::TimeZone(timezone.to_owned()))?;
        let cron = croner::parser::CronParser::builder()
            .seconds(croner::parser::Seconds::Optional)
            .year(croner::parser::Year::Disallowed)
            .build()
            .parse(expression.trim())
            .map_err(|e| FireTimesError::Cron {
                expression: expression.to_owned(),
                reason: e.to_string(),
            })?;
        Ok(Self {
            cron,
            tz,
            hour_fixed: hour_field_is_fixed(expression),
        })
    }

    fn next_after(&self, t: Timestamp) -> Option<Timestamp> {
        let mut civil = self.civil(t).checked_sub(MARGIN).ok()?;
        let mut best: Option<Timestamp> = None;
        while let Ok(found) = self.cron.find_next_occurrence(&civil, false) {
            if best.is_some_and(|b| {
                self.civil(b)
                    .checked_add(MARGIN)
                    .is_ok_and(|limit| found > limit)
            }) {
                break;
            }
            for moment in self.moments(found) {
                if moment > t && best.is_none_or(|b| moment < b) {
                    best = Some(moment);
                }
            }
            civil = found;
        }
        best
    }

    fn previous_before(&self, t: Timestamp) -> Option<Timestamp> {
        let mut civil = self.civil(t).checked_add(MARGIN).ok()?;
        let mut best: Option<Timestamp> = None;
        while let Ok(found) = self.cron.find_previous_occurrence(&civil, false) {
            if best.is_some_and(|b| {
                self.civil(b)
                    .checked_sub(MARGIN)
                    .is_ok_and(|limit| found < limit)
            }) {
                break;
            }
            for moment in self.moments(found) {
                if moment < t && best.is_none_or(|b| moment > b) {
                    best = Some(moment);
                }
            }
            civil = found;
        }
        best
    }

    fn civil(&self, t: Timestamp) -> DateTime {
        t.to_zoned(self.tz.clone()).datetime()
    }

    /// The real moments of a matching wall-clock time, by the daylight-saving rules.
    fn moments(&self, civil: DateTime) -> Vec<Timestamp> {
        match self.tz.to_ambiguous_timestamp(civil).offset() {
            AmbiguousOffset::Unambiguous { offset } => {
                offset.to_timestamp(civil).ok().into_iter().collect()
            }
            AmbiguousOffset::Gap { after, .. } => after
                .to_timestamp(civil)
                .ok()
                .and_then(|before_gap| self.tz.following(before_gap).next())
                .map(|transition| transition.timestamp())
                .into_iter()
                .collect(),
            AmbiguousOffset::Fold { before, after } => {
                let first = before.to_timestamp(civil).ok();
                let second = after.to_timestamp(civil).ok().filter(|_| !self.hour_fixed);
                first.into_iter().chain(second).collect()
            }
        }
    }
}

/// Whether the hour field is one fixed hour (presets other than `@hourly` are).
fn hour_field_is_fixed(expression: &str) -> bool {
    let text = expression.trim();
    if text.starts_with('@') {
        return text != "@hourly";
    }
    let fields: Vec<&str> = text.split_whitespace().collect();
    let hour = if fields.len() == 6 {
        fields[2]
    } else {
        fields.get(1).copied().unwrap_or("")
    };
    !hour.is_empty() && hour.bytes().all(|b| b.is_ascii_digit())
}
