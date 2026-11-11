//! What a worker knows of master time (B225, B228): the leading master's monotonic clock, on
//! which every deadline the master and its workers must agree on is set.
//!
//! Every master message carries the master's clock (`MasterClock`): its timeline, master time
//! when it was sent, and the latest heartbeat the master had heard on this stream. That
//! heartbeat (or the `Register`, number 0) was sent before the message, so master time
//! `master_ms` happened after the worker sent it: master time `X` is reached no earlier than
//! the send time plus `X - master_ms` (Cristian's bound), less what the two clocks' rates may
//! differ by. The worker keeps the tightest such sample and decides by it, never by its own idea
//! of when the master sent something, and never by a wall clock.
//!
//! The worker's own clock is the one that keeps counting through a suspend (`CLOCK_BOOTTIME`):
//! one that stopped would make it late. A pause that stops even that one (a frozen VM) shows
//! as its wall clock running ahead of it, one machine's two clocks compared: the worker then
//! counts any deadline as passed.

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use schedr_proto::pb;

/// How much the master's and the worker's clocks may differ in rate, per thousand: Linux slews
/// a clock by at most 500 ppm, so two of them by at most 1000 ppm.
const DRIFT_PER_MILLE: u32 = 1;

/// The wall clock running this much ahead of the boot clock since the sample, plus
/// [`PAUSE_PER_MILLE`] of the time since, means a pause the boot clock didn't count.
const PAUSE_SLACK: Duration = Duration::from_secs(1);

/// The part of the time since the sample the two clocks may drift apart by, per thousand.
const PAUSE_PER_MILLE: u32 = 2;

/// At most so many heartbeats wait to be heard of (the oldest dropped first): a session that
/// silent has ended.
const SENT_KEPT: usize = 10_000;

/// The worker's clocks at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Local {
    /// Its boot clock (counts through suspend).
    pub boot: Duration,
    /// Its wall clock: only to see a pause the boot clock didn't count.
    pub wall: SystemTime,
}

impl Local {
    /// The clocks now.
    #[must_use]
    pub fn now() -> Self {
        Self {
            boot: boot_now(),
            wall: SystemTime::now(),
        }
    }
}

/// The boot clock: time since boot, suspend included.
#[must_use]
pub fn boot_now() -> Duration {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
    Duration::new(
        u64::try_from(now.tv_sec).unwrap_or(0),
        u32::try_from(now.tv_nsec).unwrap_or(0),
    )
}

/// Whether the worker was paused between `then` and `now` without its boot clock counting it:
/// its wall clock ran ahead of it by more than the two may drift apart.
fn paused(then: Local, now: Local) -> bool {
    let since = now.boot.saturating_sub(then.boot);
    let walked = now.wall.duration_since(then.wall).unwrap_or_default();
    walked > since + PAUSE_SLACK + since / 1000 * PAUSE_PER_MILLE
}

/// A master message's clock paired with when the worker sent what it had heard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sample {
    timeline: u64,
    sent: Local,
    master_ms: i64,
}

impl Sample {
    /// Whether this sample bounds every master time after it at least as late as `older`
    /// does (Grok's review of D442): its bound for `X` is `sent + (X - master_ms)` less the
    /// drift allowance, so it is when `sent - older.sent >= (master_ms - older.master_ms)`
    /// less that allowance, whatever `X`. An equally tight later sample so replaces an old one,
    /// and the allowance counts from a recent sample, not from the session's first.
    fn at_least_as_tight_as(&self, older: &Self) -> bool {
        let sent = i128::try_from(self.sent.boot.as_nanos()).unwrap_or(i128::MAX)
            - i128::try_from(older.sent.boot.as_nanos()).unwrap_or(i128::MAX);
        let master = (i128::from(self.master_ms) - i128::from(older.master_ms)) * 1_000_000;
        let allowed = master - master / 1000 * i128::from(DRIFT_PER_MILLE);
        sent >= allowed
    }
}

/// Master time as this worker knows it, for one session.
#[derive(Debug, Default)]
pub struct MasterTime {
    /// What it sent and the master hasn't shown it heard: number, when sent.
    sent: VecDeque<(u64, Local)>,
    /// The next heartbeat's number.
    next: u64,
    /// The tightest sample of the master's current timeline.
    best: Option<Sample>,
}

impl MasterTime {
    /// A session opens: its `Register` (number 0) leaves after now. Returns now on the agent's
    /// own timer clock, the session's opening.
    pub fn open(&mut self) -> tokio::time::Instant {
        let opened = tokio::time::Instant::now();
        *self = Self {
            sent: VecDeque::from([(0, Local::now())]),
            next: 1,
            best: None,
        };
        opened
    }

    /// A heartbeat leaves after `at`: its number.
    pub fn sent(&mut self, at: Local) -> u64 {
        let sequence = self.next;
        self.next += 1;
        if self.sent.len() == SENT_KEPT {
            self.sent.pop_front();
        }
        self.sent.push_back((sequence, at));
        sequence
    }

    /// A master message's clock: a sample if it names what this session sent.
    pub fn heard(&mut self, clock: &pb::MasterClock) {
        if self
            .best
            .is_some_and(|best| best.timeline != clock.timeline)
        {
            self.best = None;
        }
        // Those before it won't be named again: the master's messages come in order.
        while self
            .sent
            .front()
            .is_some_and(|(number, _)| *number < clock.heard_sequence)
        {
            self.sent.pop_front();
        }
        let Some(&(number, sent)) = self.sent.front() else {
            return;
        };
        if number != clock.heard_sequence {
            return;
        }
        let sample = Sample {
            timeline: clock.timeline,
            sent,
            master_ms: clock.master_ms,
        };
        // A pause since the best sample (the wall clock ran ahead of the boot clock) leaves
        // it unusable: the new one replaces it, tighter or not.
        if self
            .best
            .is_none_or(|best| sample.at_least_as_tight_as(&best) || paused(best.sent, sent))
        {
            self.best = Some(sample);
        }
    }

    /// Whether master time `by_ms` may have been reached at `now`, `reserve` before it: no
    /// sample, a pause the boot clock didn't count, or the bound past.
    #[must_use]
    pub fn past(&self, by_ms: i64, reserve: Duration, now: Local) -> bool {
        let Some(best) = self.best else {
            return true;
        };
        if paused(best.sent, now) {
            return true;
        }
        let Ok(ahead) = u64::try_from(by_ms - best.master_ms) else {
            return true;
        };
        let ahead = Duration::from_millis(ahead);
        let ahead = ahead.saturating_sub(ahead / 1000 * DRIFT_PER_MILLE);
        (best.sent.boot + ahead).saturating_sub(reserve) <= now.boot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(boot_ms: u64) -> Local {
        Local {
            boot: Duration::from_millis(boot_ms),
            wall: SystemTime::UNIX_EPOCH + Duration::from_millis(boot_ms),
        }
    }

    fn clock(timeline: u64, master_ms: i64, heard: u64) -> pb::MasterClock {
        pb::MasterClock {
            timeline,
            master_ms,
            heard_sequence: heard,
            wall_ms: 0,
        }
    }

    fn opened(register_at: u64) -> MasterTime {
        MasterTime {
            sent: VecDeque::from([(0, at(register_at))]),
            next: 1,
            best: None,
        }
    }

    #[test]
    fn a_deadline_counts_from_the_send_of_what_the_master_had_heard() {
        // Register sent at 1000 ms; the master stamped 50 000 on its answer.
        let mut time = opened(1000);
        assert!(time.past(60_000, Duration::ZERO, at(1001)), "no sample yet");
        time.heard(&clock(7, 50_000, 0));
        // Master time 60 000 is no earlier than 1000 + 10 000, less 0.1 %: 10 990.
        assert!(!time.past(60_000, Duration::ZERO, at(10_989)));
        assert!(time.past(60_000, Duration::ZERO, at(10_990)));
        // Two seconds kept for `Started`.
        assert!(time.past(60_000, Duration::from_secs(2), at(8_990)));
        // A deadline already behind the sample: passed.
        assert!(time.past(49_000, Duration::ZERO, at(1001)));
    }

    #[test]
    fn the_tightest_sample_counts_and_one_naming_nothing_sent_changes_nothing() {
        let mut time = opened(0);
        let one = time.sent(at(1000));
        let two = time.sent(at(2000));
        assert_eq!((one, two), (1, 2));
        // Heard 1 at master 10 000: lead 1000 - 10 000 = -9000 ms.
        time.heard(&clock(7, 10_000, 1));
        // Heard 2 at master 12 500 (a slow answer): lead -10 500, looser: kept the first.
        time.heard(&clock(7, 12_500, 2));
        assert!(!time.past(20_000, Duration::ZERO, at(10_989)));
        assert!(time.past(20_000, Duration::ZERO, at(10_990)));
        // A number never sent, or one heard already: nothing.
        time.heard(&clock(7, 1, 9));
        time.heard(&clock(7, 1, 1));
        assert!(!time.past(20_000, Duration::ZERO, at(10_989)));
    }

    #[test]
    fn another_timeline_drops_what_was_known() {
        let mut time = opened(0);
        time.heard(&clock(7, 10_000, 0));
        let beat = time.sent(at(1000));
        // A new master: its first answer names a heartbeat of this stream.
        time.heard(&clock(8, 3_000, beat));
        // From the new sample only: 1000 + (5000 - 3000), less 0.1 %.
        assert!(!time.past(5_000, Duration::ZERO, at(2_997)));
        assert!(time.past(5_000, Duration::ZERO, at(2_998)));
    }

    #[test]
    fn a_pause_the_boot_clock_didnt_count_makes_every_deadline_passed() {
        let mut time = opened(1000);
        time.heard(&clock(7, 0, 0));
        // 2 s on the boot clock, 10 s on the wall clock: frozen meanwhile.
        let paused = Local {
            boot: Duration::from_millis(3000),
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(11),
        };
        assert!(time.past(60_000, Duration::ZERO, paused));
        // The same within the slack: running.
        let running = Local {
            boot: Duration::from_millis(3000),
            wall: SystemTime::UNIX_EPOCH + Duration::from_millis(3_900),
        };
        assert!(!time.past(60_000, Duration::ZERO, running));
    }

    #[test]
    fn after_a_pause_the_next_sample_replaces_the_old_one() {
        let mut time = opened(1000);
        time.heard(&clock(7, 0, 0));
        // Frozen: 2 s on the boot clock, 10 s on the wall clock; then a heartbeat, answered.
        let after = Local {
            boot: Duration::from_millis(3000),
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(11),
        };
        let beat = time.sent(after);
        // Master time moved 10 s meanwhile: a looser sample than the first, yet it counts.
        time.heard(&clock(7, 10_000, beat));
        let later = Local {
            boot: Duration::from_millis(3500),
            wall: SystemTime::UNIX_EPOCH + Duration::from_millis(11_500),
        };
        // Due at master time 12 000: no earlier than 3000 + 2000, less 0.1 %.
        assert!(!time.past(12_000, Duration::ZERO, later));
        assert!(time.past(
            12_000,
            Duration::ZERO,
            Local {
                boot: Duration::from_millis(4998),
                wall: SystemTime::UNIX_EPOCH + Duration::from_millis(12_998),
            }
        ));
    }

    #[test]
    fn an_equally_tight_later_sample_replaces_an_hours_old_one() {
        // Grok's review: kept for 40 000 s, the old sample's drift allowance (0.1 % of the
        // span) ate a whole lease.
        let mut time = opened(0);
        time.heard(&clock(7, 0, 0));
        let hours = 40_000_000;
        let deadline = hours + 60_000;
        let now = at(u64::try_from(hours).unwrap());
        assert!(
            time.past(deadline, Duration::from_secs(20), now),
            "the old one alone"
        );
        let beat = time.sent(now);
        time.heard(&clock(7, hours, beat));
        assert!(!time.past(deadline, Duration::from_secs(20), now));
        // A looser later one doesn't replace it: 1 s slower than the clocks' rates allow.
        let mut time = opened(0);
        time.heard(&clock(7, 0, 0));
        let beat = time.sent(at(10_000));
        time.heard(&clock(7, 11_000, beat));
        assert!(!time.past(20_000, Duration::ZERO, at(19_979)));
        assert!(time.past(20_000, Duration::ZERO, at(19_980)));
    }

    #[test]
    fn the_boot_clock_reads() {
        let first = boot_now();
        assert!(boot_now() >= first);
    }
}
