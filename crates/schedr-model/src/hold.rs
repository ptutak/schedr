//! A hold of an attempt (decision 535, B304): the task's own run, or one worker's hold of its
//! moved cleanup. Every message a worker sends or is sent about an attempt names one hold.
//!
//! On the wire, and on the worker (its journal, its logs, its maps), a hold is one string: the
//! attempt's ID for the task's own run (hold 0), `<attempt>_<n>` for the moved cleanup's `n`th
//! holder. A UUID holds no `_`, so the two never meet, and a worker keeps two holds of one
//! attempt apart as two attempts. The master parses it once where a message comes in
//! ([`HoldId::parse`]) and writes it once where one goes out (`Display`).

use std::fmt;

/// One hold of an attempt: which attempt, and which of its holders.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HoldId {
    attempt: String,
    holder: u32,
}

impl HoldId {
    /// The task's own run of `attempt` (hold 0).
    #[must_use]
    pub fn own(attempt: &str) -> Self {
        Self {
            attempt: attempt.to_owned(),
            holder: 0,
        }
    }

    /// `attempt`'s hold `holder` (0: the task's own run).
    #[must_use]
    pub fn new(attempt: &str, holder: u32) -> Self {
        Self {
            attempt: attempt.to_owned(),
            holder,
        }
    }

    /// The hold a worker names: `<attempt>_<n>` for a moved cleanup's holder `n`, else the
    /// attempt's own run. A suffix that isn't a positive number is part of the ID: such an
    /// attempt is unknown, as any other.
    #[must_use]
    pub fn parse(wire: &str) -> Self {
        // One spelling (agy's fifth review of B304): only ASCII digits, no leading zero, so
        // the hold written back is `wire` itself, and a string and a typed comparison agree.
        if let Some((attempt, n)) = wire.rsplit_once('_')
            && !attempt.is_empty()
            && !n.is_empty()
            && n.bytes().all(|b| b.is_ascii_digit())
            && !n.starts_with('0')
            && let Ok(holder) = n.parse::<u32>()
        {
            return Self::new(attempt, holder);
        }
        Self::own(wire)
    }

    /// Whether `wire` names an attempt as the master writes it: its UUID,
    /// or a hold of it. The one check every edge taking an ID from a worker or a request makes
    /// (a log's name, a report's attempt).
    #[must_use]
    pub fn is_valid(wire: &str) -> bool {
        let hold = Self::parse(wire);
        is_uuid(hold.attempt())
    }

    /// The attempt.
    #[must_use]
    pub fn attempt(&self) -> &str {
        &self.attempt
    }

    /// Which holder: 0 for the task's own run.
    #[must_use]
    pub const fn holder(&self) -> u32 {
        self.holder
    }

    /// Whether it is the task's own run, which ran the task (a moved cleanup's holder ran only
    /// the cleanup).
    #[must_use]
    pub const fn ran_the_task(&self) -> bool {
        self.holder == 0
    }
}

impl fmt::Display for HoldId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.holder == 0 {
            f.write_str(&self.attempt)
        } else {
            write!(f, "{}_{}", self.attempt, self.holder)
        }
    }
}

#[cfg(test)]
mod tests;

/// A UUID as text: 36 characters, hex digits with `-` at 8, 13, 18 and 23.
fn is_uuid(text: &str) -> bool {
    text.len() == 36
        && text.bytes().enumerate().all(|(i, b)| {
            matches!(i, 8 | 13 | 18 | 23) == (b == b'-') && (b == b'-' || b.is_ascii_hexdigit())
        })
}
