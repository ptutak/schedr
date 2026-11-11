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
//! The tightest sample is taken over a window (decision 447: 10 minutes by default): an older
//! one, however tight, drifts its 0.1 % from further back.
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

/// How far back the tightest sample is looked for (decision 447,
/// `SCHEDR_HIDDEN__WORKER__MASTER_SAMPLE_WINDOW`).
pub const DEFAULT_SAMPLE_WINDOW: Duration = Duration::from_mins(10);

/// The wall-clock difference from the master, beyond the round trip, that a worker warns
/// about (decision 449, B232; `SCHEDR_HIDDEN__WORKER__CLOCK_SKEW_WARNING`).
pub const DEFAULT_SKEW_WARNING: Duration = Duration::from_secs(2);

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

/// The boot clock: time since boot, suspend included. Linux's `CLOCK_BOOTTIME`; elsewhere
/// (macOS) `CLOCK_MONOTONIC`, which counts a suspend there too.
#[must_use]
pub fn boot_now() -> Duration {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let clock = rustix::time::ClockId::Boottime;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let clock = rustix::time::ClockId::Monotonic;
    let now = rustix::time::clock_gettime(clock);
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

/// A master message's clock paired with when the worker sent what it had heard, and when the
/// message arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sample {
    timeline: u64,
    sent: Local,
    master_ms: i64,
    arrived: Local,
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

/// How far `master_ms` (Unix milliseconds) lies outside `from..=to`: zero inside it.
fn wall_skew(master_ms: i64, from: SystemTime, to: SystemTime) -> Duration {
    let master = SystemTime::UNIX_EPOCH
        + Duration::from_millis(u64::try_from(master_ms).unwrap_or_default());
    master
        .duration_since(to)
        .or_else(|_| from.duration_since(master))
        .unwrap_or_default()
}

/// The latest master message's stamp: its master time and wall clock, and when it arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastStamp {
    /// Master time on it.
    pub master_ms: i64,
    /// The master's wall clock on it, Unix milliseconds.
    pub wall_ms: i64,
    /// When it arrived.
    pub arrived: Local,
}

/// What the best sample says now (for metrics, decision 448).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleReading {
    /// How long ago its heartbeat was sent.
    pub age: Duration,
    /// Its heartbeat's round trip: sent to the stamped answer's arrival.
    pub round_trip: Duration,
}

/// Master time as this worker knows it, for one session.
#[derive(Debug)]
pub struct MasterTime {
    /// What it sent and the master hasn't shown it heard: number, when sent.
    sent: VecDeque<(u64, Local)>,
    /// The next heartbeat's number.
    next: u64,
    /// Samples of the master's current timeline over the window, each looser than the one
    /// before it and later: the first is the tightest (a monotonic queue).
    samples: VecDeque<Sample>,
    /// How far back the tightest sample is looked for.
    window: Duration,
    /// The latest master message's stamp.
    last: Option<LastStamp>,
}

impl Default for MasterTime {
    fn default() -> Self {
        Self::with_window(DEFAULT_SAMPLE_WINDOW)
    }
}

impl MasterTime {
    /// Nothing known yet; the tightest sample looked for over `window`.
    #[must_use]
    pub fn with_window(window: Duration) -> Self {
        Self {
            sent: VecDeque::new(),
            next: 0,
            samples: VecDeque::new(),
            window,
            last: None,
        }
    }

    /// A session opens: its `Register` (number 0) leaves after now. Returns now on the agent's
    /// own timer clock, the session's opening.
    pub fn open(&mut self) -> tokio::time::Instant {
        let opened = tokio::time::Instant::now();
        *self = Self {
            sent: VecDeque::from([(0, Local::now())]),
            next: 1,
            ..Self::with_window(self.window)
        };
        opened
    }

    /// The tightest sample of the window.
    fn best(&self) -> Option<Sample> {
        self.samples.front().copied()
    }

    /// The latest master message's stamp.
    #[must_use]
    pub const fn last(&self) -> Option<LastStamp> {
        self.last
    }

    /// The best sample's age and round trip at `now`; `None` without one.
    #[must_use]
    pub fn reading(&self, now: Local) -> Option<SampleReading> {
        let best = self.best()?;
        Some(SampleReading {
            age: now.boot.saturating_sub(best.sent.boot),
            round_trip: best.arrived.boot.saturating_sub(best.sent.boot),
        })
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

    /// A master message's clock, arrived at `now`: a sample if it names what this session
    /// sent. For a sample, how far the master's wall clock on it lies outside this worker's
    /// over the round trip, the heartbeat's send to now (decision 449: for a warning only).
    pub fn heard(&mut self, clock: &pb::MasterClock, now: Local) -> Option<Duration> {
        self.last = Some(LastStamp {
            master_ms: clock.master_ms,
            wall_ms: clock.wall_ms,
            arrived: now,
        });
        if self
            .best()
            .is_some_and(|best| best.timeline != clock.timeline)
        {
            self.samples.clear();
        }
        // Those before it won't be named again: the master's messages come in order.
        while self
            .sent
            .front()
            .is_some_and(|(number, _)| *number < clock.heard_sequence)
        {
            self.sent.pop_front();
        }
        let &(number, sent) = self.sent.front()?;
        if number != clock.heard_sequence {
            return None;
        }
        let sample = Sample {
            timeline: clock.timeline,
            sent,
            master_ms: clock.master_ms,
            arrived: now,
        };
        // A pause since the samples (the wall clock ran ahead of the boot clock) leaves them
        // unusable: the new one replaces them, tighter or not.
        if self.best().is_some_and(|best| paused(best.sent, sent)) {
            self.samples.clear();
        }
        // Those it is at least as tight as are never the tightest again; those older than the
        // window are past it (the newest always stays).
        while self
            .samples
            .back()
            .is_some_and(|last| sample.at_least_as_tight_as(last))
        {
            self.samples.pop_back();
        }
        self.samples.push_back(sample);
        while self
            .samples
            .front()
            .is_some_and(|first| sent.boot.saturating_sub(first.sent.boot) > self.window)
        {
            self.samples.pop_front();
        }
        Some(wall_skew(clock.wall_ms, sent.wall, now.wall))
    }

    /// How long from `now` until master time may have passed the latest stamp's plus `lease`
    /// (decision 445: the cutoff's other bound); `None` without a stamp and a sample to read it
    /// by.
    #[must_use]
    pub fn cutoff_in(&self, lease: Duration, now: Local) -> Option<Duration> {
        let last = self.last?;
        self.best()?;
        let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
        Some(self.until(last.master_ms.saturating_add(lease_ms), now))
    }

    /// How long from `now` until master time `by_ms` may be reached, by the same bound as
    /// [`Self::past`]: zero when it may be already (no sample, a pause, the bound past).
    #[must_use]
    pub fn until(&self, by_ms: i64, now: Local) -> Duration {
        self.reached_at(by_ms, now)
            .map_or(Duration::ZERO, |at| at.saturating_sub(now.boot))
    }

    /// On the boot clock, the earliest master time `by_ms` may be reached; `None` when it may
    /// be already at `now`.
    fn reached_at(&self, by_ms: i64, now: Local) -> Option<Duration> {
        let best = self.best()?;
        if paused(best.sent, now) {
            return None;
        }
        // The stamp is master time rounded down to the millisecond: up to 1 ms later in fact.
        let ahead = Duration::from_millis(u64::try_from(by_ms - best.master_ms - 1).ok()?);
        Some(best.sent.boot + ahead.saturating_sub(ahead / 1000 * DRIFT_PER_MILLE))
    }

    /// Whether master time `by_ms` may have been reached at `now`, `reserve` before it: no
    /// sample, a pause the boot clock didn't count, or the bound past.
    #[must_use]
    pub fn past(&self, by_ms: i64, reserve: Duration, now: Local) -> bool {
        self.slack(by_ms, reserve, now).is_none()
    }

    /// How long before master time `by_ms`, `reserve` kept, there is still at `now`: `None` once
    /// [`Self::past`] (for the metrics too, decision 448).
    #[must_use]
    pub fn slack(&self, by_ms: i64, reserve: Duration, now: Local) -> Option<Duration> {
        let at = self.reached_at(by_ms, now)?.saturating_sub(reserve);
        at.checked_sub(now.boot).filter(|left| !left.is_zero())
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
            lease_epoch: 0,
        }
    }

    fn opened(register_at: u64) -> MasterTime {
        MasterTime {
            sent: VecDeque::from([(0, at(register_at))]),
            next: 1,
            ..MasterTime::default()
        }
    }

    #[test]
    fn a_deadline_counts_from_the_send_of_what_the_master_had_heard() {
        // Register sent at 1000 ms; the master stamped 50 000 on its answer.
        let mut time = opened(1000);
        assert!(time.past(60_000, Duration::ZERO, at(1001)), "no sample yet");
        time.heard(&clock(7, 50_000, 0), at(0));
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
        time.heard(&clock(7, 10_000, 1), at(0));
        // Heard 2 at master 12 500 (a slow answer): lead -10 500, looser: kept the first.
        time.heard(&clock(7, 12_500, 2), at(0));
        assert!(!time.past(20_000, Duration::ZERO, at(10_989)));
        assert!(time.past(20_000, Duration::ZERO, at(10_990)));
        // A number never sent, or one heard already: nothing.
        time.heard(&clock(7, 1, 9), at(0));
        time.heard(&clock(7, 1, 1), at(0));
        assert!(!time.past(20_000, Duration::ZERO, at(10_989)));
    }

    #[test]
    fn another_timeline_drops_what_was_known() {
        let mut time = opened(0);
        time.heard(&clock(7, 10_000, 0), at(0));
        let beat = time.sent(at(1000));
        // A new master: its first answer names a heartbeat of this stream.
        time.heard(&clock(8, 3_000, beat), at(0));
        // From the new sample only: 1000 + (5000 - 3000), less 0.1 %.
        assert!(!time.past(5_000, Duration::ZERO, at(2_997)));
        assert!(time.past(5_000, Duration::ZERO, at(2_998)));
    }

    #[test]
    fn a_pause_the_boot_clock_didnt_count_makes_every_deadline_passed() {
        let mut time = opened(1000);
        time.heard(&clock(7, 0, 0), at(0));
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
        time.heard(&clock(7, 0, 0), at(0));
        // Frozen: 2 s on the boot clock, 10 s on the wall clock; then a heartbeat, answered.
        let after = Local {
            boot: Duration::from_millis(3000),
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(11),
        };
        let beat = time.sent(after);
        // Master time moved 10 s meanwhile: a looser sample than the first, yet it counts.
        time.heard(&clock(7, 10_000, beat), at(0));
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
        time.heard(&clock(7, 0, 0), at(0));
        let hours = 40_000_000;
        let deadline = hours + 60_000;
        let now = at(u64::try_from(hours).unwrap());
        assert!(
            time.past(deadline, Duration::from_secs(20), now),
            "the old one alone"
        );
        let beat = time.sent(now);
        time.heard(&clock(7, hours, beat), at(0));
        assert!(!time.past(deadline, Duration::from_secs(20), now));
        // A looser later one doesn't replace it: 1 s slower than the clocks' rates allow.
        let mut time = opened(0);
        time.heard(&clock(7, 0, 0), at(0));
        let beat = time.sent(at(10_000));
        time.heard(&clock(7, 11_000, beat), at(0));
        assert!(!time.past(20_000, Duration::ZERO, at(19_979)));
        assert!(time.past(20_000, Duration::ZERO, at(19_980)));
    }

    #[test]
    fn the_boot_clock_reads() {
        let first = boot_now();
        assert!(boot_now() >= first);
    }
}

#[cfg(test)]
mod window_tests {
    use super::*;

    fn at(boot_ms: u64) -> Local {
        Local {
            boot: Duration::from_millis(boot_ms),
            wall: SystemTime::UNIX_EPOCH + Duration::from_millis(boot_ms),
        }
    }

    fn clock(master_ms: i64, heard: u64) -> pb::MasterClock {
        pb::MasterClock {
            timeline: 7,
            master_ms,
            heard_sequence: heard,
            wall_ms: master_ms,
            lease_epoch: 0,
        }
    }

    #[test]
    fn the_tightest_sample_is_taken_over_the_window() {
        // Decision 447: a 10 s window. The first sample is tight (lead -10 000), the next
        // looser (a slow answer); once the first is older than the window, the looser counts.
        let mut time = MasterTime::with_window(Duration::from_secs(10));
        time.open();
        let one = time.sent(at(1_000));
        time.heard(&clock(11_000, one), at(1_010));
        let two = time.sent(at(5_000));
        time.heard(&clock(16_000, two), at(5_600));
        // Master time 30 000 by the first: 1000 + 19 000, less the stamp's rounding (1 ms) and
        // 0.1 %: 19 980.001.
        assert_eq!(
            time.until(30_000, at(9_000)),
            Duration::from_micros(10_980_001)
        );
        let reading = time.reading(at(9_000)).unwrap();
        assert_eq!(
            (reading.age, reading.round_trip),
            (Duration::from_secs(8), Duration::from_millis(10))
        );
        // 12 s on: the first is out of the window; the second (5000 + 14 000, less 0.1 %)
        // counts, the third being looser still.
        let three = time.sent(at(12_000));
        time.heard(&clock(24_000, three), at(12_900));
        assert_eq!(
            time.until(30_000, at(9_000)),
            Duration::from_micros(9_985_001)
        );
        assert!(time.slack(30_000, Duration::ZERO, at(18_986)).is_none());
        assert!(time.slack(30_000, Duration::ZERO, at(18_985)).is_some());
        assert_eq!(time.last().unwrap().wall_ms, 24_000);
    }

    #[test]
    fn the_cutoff_counts_from_the_last_stamp_on_master_time() {
        // Decision 445: Register sent at 1000, answered at master 50 000; a later message stamped
        // 55 000 arrives at 9000 (held on its way). The lease (10 s) after master time 55 000 may
        // be reached at 1000 + 15 000, less 1 ms (the stamp's rounding) and 0.1 %: 15 984.001,
        // sooner than 9000 + 10 000 by receipt.
        let mut time = MasterTime::with_window(Duration::from_secs(600));
        assert_eq!(
            time.cutoff_in(Duration::from_secs(10), at(0)),
            None,
            "nothing heard"
        );
        time.sent = VecDeque::from([(0, at(1_000))]);
        time.next = 1;
        time.heard(&clock(50_000, 0), at(1_100));
        time.heard(&clock(55_000, 0), at(9_000));
        assert_eq!(
            time.cutoff_in(Duration::from_secs(10), at(9_000)),
            Some(Duration::from_micros(6_984_001))
        );
        assert_eq!(
            time.cutoff_in(Duration::from_secs(10), at(16_000)),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn the_masters_wall_clock_is_compared_over_the_round_trip() {
        // Decision 449: sent at wall 1 s, answered at 1.5 s; the master's wall clock on it
        // anywhere between is no skew, 3.5 s is 2 s ahead, 0.2 s is 0.8 s behind.
        let skew = |wall_ms| {
            let mut time = MasterTime::default();
            time.sent(at(1_000));
            time.heard(
                &pb::MasterClock {
                    wall_ms,
                    ..clock(5_000, 0)
                },
                at(1_500),
            )
        };
        assert_eq!(skew(1_200), Some(Duration::ZERO));
        assert_eq!(skew(3_500), Some(Duration::from_secs(2)));
        assert_eq!(skew(200), Some(Duration::from_millis(800)));
        let mut time = MasterTime::default();
        assert_eq!(
            time.heard(&clock(5_000, 3), at(1_500)),
            None,
            "not a sample"
        );
    }

    #[test]
    fn a_tighter_sample_drops_the_looser_ones_before_it() {
        let mut time = MasterTime::with_window(Duration::from_secs(600));
        time.open();
        let one = time.sent(at(1_000));
        time.heard(&clock(12_000, one), at(1_500));
        let two = time.sent(at(2_000));
        time.heard(&clock(12_000, two), at(2_001));
        // The second (lead -10 000) is tighter than the first (-11 000): it alone remains.
        assert_eq!(time.samples.len(), 1);
        assert_eq!(
            time.until(13_000, at(2_000)),
            Duration::from_micros(998_001)
        );
    }
}
