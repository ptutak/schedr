//! Master time (B225, B228): the one clock every liveness decision of the cluster reads.
//!
//! The master is handed the wall clock (`now`) for what people see and what the database
//! keeps: schedules, the times shown, retention. Whether a worker is lost, an assignment has
//! waited too long, a parse or connection job overran, or a moved cleanup may start is decided
//! on master time instead: the master's monotonic clock, which never steps. It is a
//! [`Timestamp`] on a timeline of its own (the wall clock when the process first read it, plus
//! the monotonic time since), so the code comparing times stays as it was; such times are never
//! stored as wall-clock ones, nor compared with them. Workers read it from the clock every
//! master message carries (`MasterClock`).
//!
//! A test hands the master its own times and these follow them ([`as_given`]), so a test moves
//! master time by the `now` it passes.

use std::sync::OnceLock;

use jiff::{SignedDuration, Timestamp};

/// How the master reads master time, from the wall-clock time it was handed.
pub type Steady = fn(Timestamp) -> Timestamp;

/// Master time as the time handed (tests: they move it with the `now` they pass).
#[must_use]
pub const fn as_given(now: Timestamp) -> Timestamp {
    now
}

/// Master time from the monotonic clock (production): the wall clock at the process's first
/// reading, plus the monotonic time since. `now` is ignored.
#[must_use]
pub fn monotonic(_now: Timestamp) -> Timestamp {
    static ORIGIN: OnceLock<(Timestamp, std::time::Instant)> = OnceLock::new();
    let (wall, mono) = *ORIGIN.get_or_init(|| (Timestamp::now(), std::time::Instant::now()));
    let since = SignedDuration::try_from(mono.elapsed()).unwrap_or(SignedDuration::MAX);
    wall.checked_add(since).unwrap_or(Timestamp::MAX)
}

/// This master process's timeline: master times of another process aren't comparable.
#[must_use]
pub fn timeline() -> u64 {
    static TIMELINE: OnceLock<u64> = OnceLock::new();
    *TIMELINE.get_or_init(|| {
        // The low half of a UUIDv7, its random part: unique enough among a deployment's masters.
        let bits = uuid::Uuid::now_v7().as_u128();
        u64::try_from(bits & u128::from(u64::MAX))
            .unwrap_or(1)
            .max(1)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_monotonic_clock_never_goes_back_and_given_times_pass_through() {
        let first = monotonic(Timestamp::MIN);
        let second = monotonic(Timestamp::MAX);
        assert!(second >= first);
        let given: Timestamp = "2026-11-11T03:00:00Z".parse().unwrap();
        assert_eq!(as_given(given), given);
        assert_eq!(timeline(), timeline());
        assert_ne!(timeline(), 0);
    }
}
