//! A deterministic simulator of master time (decisions 446 and 447): a worker's real
//! [`MasterTime`] against a master on a virtual clock, over random rate drift (within the
//! 0.1 % allowed), delays each way, answers lost, assignments delivered late, and stalls that
//! even the boot clock doesn't count (a frozen VM). It checks the rules a worker keeps:
//!
//! 1. **No start after expiry:** an assignment the worker starts is started at least its
//!    reserve before its deadline in master time; the master expires an unstarted one two
//!    heartbeats past that deadline (decision 450), so a `Started` sent at once is in time.
//! 2. **No attempt open forever:** every assignment is decided (started or rejected `TOO_LATE`)
//!    when it arrives; one never started the master expires (its side: `dispatch` tests).
//! 3. **No retry within a replaced incarnation's hold:** a worker cut off from the master stops
//!    by master time last stamp + lease; the master ends its attempts no earlier than a lease
//!    after it marks them, which is after its last stamp to that worker (decision 443).
//! 4. **A moved cleanup never beside its old worker:** a cleanup moves only past that hold and
//!    the move wait, so rule 3 covers it.
//!
//! Times are in nanoseconds of a real, ideal clock; master time runs on it exactly (the
//! reference), the worker's boot clock at its drift and not at all through a stall.
//!
//! A known limit, kept: a stall the boot clock misses shows only once the wall clock has run
//! 1 s (plus 0.2 % of the time since the sample) ahead of it, so that a wall-clock step isn't
//! taken for one; a shorter stall goes unseen and leaves the worker that much late. With a
//! stall in a scenario the rules are checked with that allowance.

use std::time::{Duration, SystemTime};

use proptest::prelude::*;
use schedr_agent::master_time::{Local, MasterTime};
use schedr_proto::pb;

const MS: u64 = 1_000_000;

/// The worker's clocks: drift in parts per million, stalls as (start, length).
#[derive(Debug, Clone)]
struct Worker {
    ppm: i64,
    stalls: Vec<(u64, u64)>,
}

impl Worker {
    /// Real time `t` that isn't in a stall: in one, its end (the worker acts only then).
    fn awake(&self, t: u64) -> u64 {
        self.stalls
            .iter()
            .find(|&&(start, len)| t >= start && t < start + len)
            .map_or(t, |&(start, len)| start + len)
    }

    /// The boot clock at real time `t`.
    fn boot(&self, t: u64) -> Duration {
        let stalled: u64 = self
            .stalls
            .iter()
            .map(|&(start, len)| t.saturating_sub(start).min(len))
            .sum();
        let active = i128::from(t - stalled);
        let boot = active + active * i128::from(self.ppm) / 1_000_000;
        Duration::from_nanos(u64::try_from(boot).unwrap())
    }

    /// Both clocks at `t`: the wall clock counts stalls.
    fn local(&self, t: u64) -> Local {
        Local {
            boot: self.boot(t),
            wall: SystemTime::UNIX_EPOCH + Duration::from_hours(500_000) + Duration::from_nanos(t),
        }
    }

    /// The earliest real time at or after `from` the boot clock reads `boot`.
    fn when_boot(&self, from: u64, boot: Duration) -> u64 {
        let (mut low, mut high) = (from, from + 10_000_000 * MS);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.boot(mid) >= boot {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        low
    }

    /// Whether a stall starts in `from..to`.
    fn stall_between(&self, from: u64, to: u64) -> bool {
        self.stalls
            .iter()
            .any(|&(start, _)| start >= from && start < to)
    }
}

/// One heartbeat's fate: its delay to the master, its answer's back (`None`: lost), and the
/// assignment the answer is, if any (its age when sent, its lease).
#[derive(Debug, Clone)]
struct Beat {
    up_ms: u64,
    down_ms: Option<u64>,
    assignment: Option<(u64, u64)>,
}

fn beat() -> impl Strategy<Value = Beat> {
    // Mostly quick, now and then very slow either way.
    let delay = prop_oneof![8 => 0..50u64, 2 => 0..5_000u64];
    (
        delay.clone(),
        prop::option::weighted(0.8, delay),
        prop::option::weighted(0.3, (0..30_000u64, 1_000..60_000u64)),
    )
        .prop_map(|(up_ms, down_ms, assignment)| Beat {
            up_ms,
            down_ms,
            assignment,
        })
}

#[derive(Debug, Clone)]
struct Scenario {
    worker: Worker,
    heartbeat_ms: u64,
    window_ms: u64,
    lease_ms: u64,
    master_origin_ms: u64,
    beats: Vec<Beat>,
}

fn scenario() -> impl Strategy<Value = Scenario> {
    (
        -990..=990i64,
        // Gaps before each stall and their lengths: never overlapping, never empty.
        prop::collection::vec((0..200_000u64, 1..20_000u64), 0..3),
        20..2_000u64,
        1_000..600_000u64,
        1_000..60_000u64,
        0..1_000_000_000u64,
        prop::collection::vec(beat(), 2..150),
    )
        .prop_map(
            |(ppm, stalls, heartbeat_ms, window_ms, lease_ms, master_origin_ms, beats)| {
                let mut end = 0;
                let stalls = stalls
                    .into_iter()
                    .map(|(gap, len)| {
                        let start = end + gap * MS;
                        end = start + len * MS;
                        (start, len * MS)
                    })
                    .collect();
                Scenario {
                    worker: Worker { ppm, stalls },
                    heartbeat_ms,
                    window_ms,
                    lease_ms,
                    master_origin_ms,
                    beats,
                }
            },
        )
}

/// What happens, in real time order: a send, or an answer's arrival.
#[derive(Debug)]
enum Event {
    Send(u64),
    Answer {
        clock: pb::MasterClock,
        /// The real time master time was at the stamp.
        stamped: u64,
        /// The assignment's deadline in master time, milliseconds.
        start_by_ms: Option<i64>,
    },
}

/// Runs a scenario, checking the rules at every event; `Err` names the one broken.
fn run(s: &Scenario) -> Result<(), String> {
    let master_ns = |t: u64| s.master_origin_ms * MS + t;
    let stamp_ms = |t: u64| i64::try_from(master_ns(t) / MS).unwrap();
    let heartbeat = s.heartbeat_ms * MS;
    let reserve = Duration::from_nanos(2 * heartbeat);
    let lease = Duration::from_millis(s.lease_ms);
    // The unseen-stall allowance at real time `t`: none before the first stall began.
    let unseen = |t: u64| {
        if s.worker.stalls.first().is_some_and(|&(start, _)| start < t) {
            1_000 * MS + t / 500
        } else {
            0
        }
    };
    let mut events: Vec<(u64, Event)> = Vec::new();
    let (mut at_master, mut back) = (0, 0);
    for (number, beat) in s.beats.iter().enumerate() {
        let number = u64::try_from(number).unwrap();
        let sent = s.worker.awake(number * heartbeat);
        events.push((sent, Event::Send(number)));
        // One stream: in order both ways.
        at_master = (sent + beat.up_ms * MS).max(at_master);
        let Some(down_ms) = beat.down_ms else {
            continue;
        };
        back = s.worker.awake((at_master + down_ms * MS).max(back));
        let start_by_ms = beat.assignment.map(|(age_ms, lease_ms)| {
            let created = master_ns(at_master).saturating_sub(age_ms * MS);
            i64::try_from((created + lease_ms * MS) / MS).unwrap()
        });
        let clock = pb::MasterClock {
            timeline: 7,
            master_ms: stamp_ms(at_master),
            heard_sequence: number,
            wall_ms: 0,
            lease_epoch: 1,
        };
        events.push((
            back,
            Event::Answer {
                clock,
                stamped: at_master,
                start_by_ms,
            },
        ));
    }
    // Sends before answers at the same instant: an answer never names a beat not sent yet.
    events.sort_by_key(|(t, e)| (*t, matches!(e, Event::Answer { .. })));
    let mut time = MasterTime::with_window(Duration::from_millis(s.window_ms));
    let mut last_stamped = None;
    for (t, event) in events {
        let now = s.worker.local(t);
        match event {
            Event::Send(number) => {
                let sequence = time.sent(now);
                if sequence != number {
                    return Err(format!("beat {number} numbered {sequence}"));
                }
            }
            Event::Answer {
                clock,
                stamped,
                start_by_ms,
            } => {
                time.heard(&clock, now);
                last_stamped = Some(stamped);
                // Rule 1: started only its reserve before the deadline.
                if let Some(by) = start_by_ms
                    && time.slack(by, reserve, now).is_some()
                {
                    let deadline = u64::try_from(by).unwrap() * MS;
                    if master_ns(t) + 2 * heartbeat > deadline + unseen(t) {
                        return Err(format!(
                            "started at master {} ns, deadline {deadline} less the reserve",
                            master_ns(t)
                        ));
                    }
                }
            }
        }
        // Rule 3: while not cut off, master time is within a lease of the latest stamp; and
        // the cutoff it counts down to (no stall before it) is too.
        let Some(stamped) = last_stamped else {
            continue;
        };
        let limit = master_ns(stamped) + s.lease_ms * MS + unseen(t);
        // Zero left: cut off already (a message arriving past its lease doesn't count, B162).
        if let Some(left) = time.cutoff_in(lease, now)
            && !left.is_zero()
        {
            if master_ns(t) > limit {
                return Err(format!(
                    "not cut off at master {} ns, past {limit}",
                    master_ns(t)
                ));
            }
            let cutoff = s.worker.when_boot(t, now.boot + left);
            if !s.worker.stall_between(t, cutoff)
                && master_ns(cutoff) > limit + unseen(cutoff) - unseen(t)
            {
                return Err(format!(
                    "cut off at master {} ns, past {limit}",
                    master_ns(cutoff)
                ));
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn a_worker_keeps_master_time_rules_whatever_the_delays_drift_and_stalls(s in scenario()) {
        prop_assert_eq!(run(&s), Ok(()), "{:?}", s);
    }
}

#[test]
fn a_stall_the_boot_clock_missed_counts_every_deadline_as_passed() {
    // A fixed case: a 30 s freeze between a beat and its answer; the answer's assignment,
    // due 10 s after it was sent, is refused, and the worker is cut off at once.
    let s = Scenario {
        worker: Worker {
            ppm: 0,
            stalls: vec![(150 * MS, 30_000 * MS)],
        },
        heartbeat_ms: 100,
        window_ms: 600_000,
        lease_ms: 10_000,
        master_origin_ms: 5_000,
        beats: vec![
            Beat {
                up_ms: 1,
                down_ms: Some(1),
                assignment: None,
            },
            Beat {
                up_ms: 10,
                down_ms: Some(100),
                assignment: Some((0, 10_000)),
            },
        ],
    };
    assert_eq!(run(&s), Ok(()));
}
