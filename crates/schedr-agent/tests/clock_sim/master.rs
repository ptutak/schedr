//! The master's side of the simulator (decision 446, R1.0b): the real [`MasterCore`] over an
//! in-memory database and two workers, each with the real [`MasterTime`] on a drifting clock,
//! on one virtual clock, through streams that delay, hold up and break, workers cut off one
//! way or both, workers replaced by a new incarnation while the old one may run on, and masters
//! restarted or taken over by another while the deposed one still answers heartbeats for a
//! while (decision 444). Between them sits what the service does on a stream: messages
//! stamped with master time and the heartbeats heard, heartbeats answered at once, arrivals
//! noted before the core handles them.
//!
//! The DAG has two tasks with retries, run side by side: one with a `cleanup_anywhere`
//! cleanup, which a worker cut off stops (decision 252), and an ordinary one, which it keeps
//! running, so that one may run again elsewhere once the master counts its attempt `lost` (at
//! least once). The rules, checked as it runs and at its end:
//!
//! 1. **No start after expiry:** when a worker starts an attempt, the master hasn't ended it.
//! 2. **No attempt open forever:** once the faults are over and the workers have been
//!    connected a while, every attempt and run has ended.
//! 3. **No retry within a replaced incarnation's hold**, and
//! 4. **a moved cleanup never beside its old worker:** no two processes of one task instance
//!    (its attempts, a moved cleanup) run at the same time, on any worker or incarnation; the
//!    ordinary task's only beside an attempt the master ended `lost`.
//!
//! Each safeguard is needed: switched off ([`Weakened`]), scenarios aimed at it break a rule.
//!
//! A scenario runs with one of two sets of timings, both on a 1 s heartbeat: a lease of ten
//! heartbeats and a cancel grace of two ([`Timings::SCALED`], what the aimed scenarios are
//! tuned to), or the defaults' ratios (a lease of six, a cancel grace of three:
//! [`Timings::DEFAULTS`]).
//!
//! Known limits, kept: master time is the ideal clock (the worker's side checks drift against
//! it in [`super::run`]); workers drift but don't stall (a stall the boot clock misses leaves
//! a worker late, a limit [`super::run`] checks with its allowance); a stream holds a message
//! up at most 3 s outside a cut (decision 450 counts four heartbeat intervals for a `Started`
//! on its way); a deposed master only answers heartbeats (its writes would fail on the lease).

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use proptest::prelude::*;
use schedr_agent::master_time::{MasterTime, lock};
use schedr_db::Database;
use schedr_db::reads::RunFilter;
use schedr_master::core::{MasterCore, Settings};
use schedr_master::dispatch::{Outgoing, Weakened};
use schedr_model::RunState;
use schedr_model::runner::ParsedFile;
use schedr_proto::PROTOCOL_VERSION;
use schedr_proto::convert::parsed_file_to_proto;
use schedr_proto::pb::{self, master_message, worker_message};
use serde_json::json;

use super::{MS, Worker};

/// Times in milliseconds of real time (master time runs on it exactly).
const HEARTBEAT: u64 = 1_000;

/// The master's timings a scenario runs with, in milliseconds (the heartbeat is [`HEARTBEAT`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    pub lease: u64,
    pub cancel_grace: u64,
    pub move_wait: u64,
    pub takeover_wait: u64,
}

impl Timings {
    /// A lease of ten heartbeats, a grace of two: what the aimed scenarios are tuned to.
    pub const SCALED: Self = Self {
        lease: 10_000,
        cancel_grace: 2_000,
        move_wait: 3_000,
        takeover_wait: 4_000,
    };
    /// The defaults' ratios to a 10 s heartbeat (lease 60 s, cancel grace 30 s, move wait
    /// 10 s, takeover wait 30 s less the 10 s query timeout), scaled to [`HEARTBEAT`].
    pub const DEFAULTS: Self = Self {
        lease: 6_000,
        cancel_grace: 3_000,
        move_wait: 1_000,
        takeover_wait: 2_000,
    };
}
/// A stream held up this long breaks, both sides seeing it (TCP's retries given up).
const KEEPALIVE: u64 = 15_000;
/// The master's rounds; the workers' alive checks.
const TICK: u64 = 1_000;
const ALIVE: u64 = 250;
/// Runs are triggered and faults happen before this; the run ends at [`END`].
const FAULTS_END: u64 = 60_000;
const END: u64 = 400_000;
/// How often, past the faults, it checks whether everything has ended.
const SETTLE_CHECK: u64 = 5_000;
const TRIGGER_EVERY: u64 = 5_000;
const SLOTS: usize = 4;
/// The DAG's task without a cleanup: a worker cut off keeps running it (decision 252).
const ORDINARY: &str = "load";
const WORKERS: [&str; 2] = ["w1", "w2"];

/// Which way a cut holds messages up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Way {
    /// Worker to master.
    Up,
    /// Master to worker.
    Down,
    Both,
}

#[derive(Debug, Clone)]
pub enum Fault {
    /// The worker's link holds messages up one way or both for `len` ms.
    Cut { worker: usize, len: u64, way: Way },
    /// The worker's link slows for `len` ms: each message up takes `up` ms more, each down
    /// `down` more (a congested link, a reader behind).
    Lag {
        worker: usize,
        len: u64,
        up: u64,
        down: u64,
    },
    /// The worker's stream breaks (both sides see it).
    Drop { worker: usize },
    /// A new incarnation of the worker starts `gap` ms later; the old one dies, or closes its
    /// stream and runs on cut off for good (a pod replaced, its process still running).
    Replace {
        worker: usize,
        old_lives: bool,
        gap: u64,
    },
    /// The master is replaced: the deposed one answers heartbeats for `deposed` ms more, the
    /// new one leads `gap` ms after (a takeover, or a restart).
    Master { deposed: u64, gap: u64 },
}

#[derive(Debug, Clone)]
pub struct Scenario {
    pub timings: Timings,
    pub ppm: [i64; 2],
    pub seed: u64,
    pub faults: Vec<(u64, Fault)>,
}

fn fault() -> impl Strategy<Value = Fault> {
    let way = prop_oneof![Just(Way::Up), Just(Way::Down), Just(Way::Both)];
    prop_oneof![
        4 => (0..2usize, 500..40_000u64, way)
            .prop_map(|(worker, len, way)| Fault::Cut { worker, len, way }),
        // Up below decision 450's four heartbeat intervals for a `Started` on its way.
        // Often near the edges: an assignment arriving just before the worker's reserve, its
        // `Started` held up almost as long as decision 450 allows.
        3 => (
            0..2usize,
            1_000..30_000u64,
            prop_oneof![0..3_900u64, 2_500..3_900u64],
            prop_oneof![0..9_000u64, 6_500..8_200u64],
        )
            .prop_map(|(worker, len, up, down)| Fault::Lag { worker, len, up, down }),
        2 => (0..2usize).prop_map(|worker| Fault::Drop { worker }),
        2 => (0..2usize, any::<bool>(), 0..10_000u64)
            .prop_map(|(worker, old_lives, gap)| Fault::Replace { worker, old_lives, gap }),
        1 => (0..=4_000u64, 0..5_000u64)
            .prop_map(|(deposed, gap)| Fault::Master { deposed, gap }),
    ]
}

pub fn scenario() -> impl Strategy<Value = Scenario> {
    (
        prop_oneof![Just(Timings::SCALED), Just(Timings::DEFAULTS)],
        -990..=990i64,
        -990..=990i64,
        any::<u64>(),
        prop::collection::vec((5_000..FAULTS_END, fault()), 0..6),
    )
        .prop_map(|(timings, a, b, seed, mut faults)| {
            faults.sort_by_key(|(at, _)| *at);
            Scenario {
                timings,
                ppm: [a, b],
                seed,
                faults,
            }
        })
}

/// Scenarios aimed at one safeguard: what it is there for, often (the mutation tests; with
/// the safeguard on, they are among the scenarios every rule must hold in).
pub fn aimed_at(safeguard: Weakened) -> BoxedStrategy<Scenario> {
    let faults: BoxedStrategy<Vec<(u64, Fault)>> = match safeguard {
        // What the echo is there for (B240): a run's first assignment whose fate the master
        // can't see at once. Its `Started` held up a moment (a lag up), its stream broken
        // right after it was sent (the same process registers again), or its worker replaced
        // with the old process running on.
        Weakened::Echo => {
            let start = |run: u64, after: u64| 1_000 + run * TRIGGER_EVERY + after;
            let late_start = (
                0..8u64,
                0..300u64,
                300..3_000u64,
                2_000..6_000u64,
                0..2usize,
            )
                .prop_map(move |(run, early, up, len, worker)| {
                    let at = start(run, 0).saturating_sub(early);
                    let down = 0;
                    vec![(
                        at,
                        Fault::Lag {
                            worker,
                            len,
                            up,
                            down,
                        },
                    )]
                });
            let dropped = (0..8u64, 0..200u64, 0..2usize).prop_map(move |(run, after, worker)| {
                vec![(start(run, after), Fault::Drop { worker })]
            });
            let replaced = (0..8u64, 0..200u64, 0..2usize, 0..2_000u64).prop_map(
                move |(run, after, worker, gap)| {
                    let old_lives = true;
                    vec![(
                        start(run, after),
                        Fault::Replace {
                            worker,
                            old_lives,
                            gap,
                        },
                    )]
                },
            );
            prop_oneof![late_start, dropped, replaced].boxed()
        }
        // A worker replaced, the old process running on, the new one soon back.
        Weakened::Hold => prop::collection::vec(
            (2_000..40_000u64, 0..2usize, 0..4_000u64).prop_map(|(at, worker, gap)| {
                let old_lives = true;
                (
                    at,
                    Fault::Replace {
                        worker,
                        old_lives,
                        gap,
                    },
                )
            }),
            1..3,
        )
        .boxed(),
        // A worker cut off long enough to be lost, its cleanup moved.
        Weakened::MoveWait => prop::collection::vec(
            (2_000..40_000u64, 0..2usize, 12_000..30_000u64).prop_map(|(at, worker, len)| {
                let way = Way::Both;
                (at, Fault::Cut { worker, len, way })
            }),
            1..3,
        )
        .boxed(),
    };
    (-990..=990i64, -990..=990i64, any::<u64>(), faults)
        .prop_map(|(a, b, seed, mut faults)| {
            faults.sort_by_key(|(at, _)| *at);
            Scenario {
                timings: Timings::SCALED,
                ppm: [a, b],
                seed,
                faults,
            }
        })
        .boxed()
}

/// Every kind of scenario: random faults, and those aimed at each safeguard.
pub fn any_scenario() -> impl Strategy<Value = Scenario> {
    prop_oneof![
        3 => scenario(),
        1 => aimed_at(Weakened::Echo),
        1 => aimed_at(Weakened::Hold),
        1 => aimed_at(Weakened::MoveWait),
    ]
}

/// `SplitMix64`: the scenario's seed picks every delay and duration.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    /// A message's time on the wire: mostly quick, now and then up to 3 s.
    fn delay(&mut self) -> u64 {
        if self.below(10) < 8 {
            self.below(50)
        } else {
            self.below(3_000)
        }
    }
}

/// A task instance: its run, task and map index.
type Key = (String, String, i32);

#[derive(Debug)]
enum Event {
    Tick,
    Trigger,
    Alive(usize),
    Heartbeat(usize, u64),
    ProcEnd(usize, String),
    AtMaster {
        inst: usize,
        sid: u64,
        message: Box<worker_message::Message>,
        /// Its stamp, as the agent's sender puts it on as it queues it (B240).
        clock: pb::WorkerClock,
    },
    AtWorker {
        inst: usize,
        sid: u64,
        message: Box<pb::MasterMessage>,
    },
    Refused(usize, u64),
    /// The master's side of a stream ends.
    MasterSeesEnd(usize, u64),
    /// The worker's side.
    WorkerSeesEnd(usize, u64),
    /// A stream held up past [`KEEPALIVE`].
    Break(usize),
    Fault(Fault),
    Incarnation(usize),
    MasterUp,
    DeposedEnds,
}

/// A process a worker runs: an attempt's task, or a moved cleanup.
#[derive(Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent facts: stopped by the cutoff and then killed, say"
)]
struct Proc {
    key: Key,
    /// Its task has no cleanup: not stopped at the cutoff.
    ordinary: bool,
    moved: bool,
    ends_at: u64,
    /// Stopped: by the cutoff (its cleanup then waits for the master), or killed.
    stopping: bool,
    killed: bool,
    /// Its task ended while cut off: its cleanup waits for the master's answer.
    held: bool,
    interval: usize,
}

/// A worker process (an incarnation).
struct Instance {
    worker: usize,
    incarnation: u32,
    clock: Worker,
    alive: bool,
    /// [from, to): its link holds messages up that way.
    cuts: Vec<(u64, u64, Way)>,
    /// [from, to): its messages take this much longer up, and down.
    lags: Vec<(u64, u64, u64, u64)>,
    session: Option<u64>,
    connecting: Option<u64>,
    next_try: u64,
    backoff: u64,
    time: MasterTime,
    lease: Option<Duration>,
    /// The boot clock at the master's last word (or a stream's opening).
    heard_boot: Duration,
    /// When master time may pass its cutoff, in real time (carried over a reconnect).
    cutoff_at: Option<u64>,
    cut_off: bool,
    procs: BTreeMap<String, Proc>,
    unacked: BTreeMap<String, pb::Finished>,
}

impl Instance {
    fn new(worker: usize, incarnation: u32, ppm: i64) -> Self {
        Self {
            worker,
            incarnation,
            clock: Worker {
                ppm,
                stalls: Vec::new(),
            },
            alive: true,
            cuts: Vec::new(),
            lags: Vec::new(),
            session: None,
            connecting: None,
            next_try: 0,
            backoff: 500,
            time: MasterTime::with_window(Duration::from_mins(10)),
            lease: None,
            heard_boot: Duration::ZERO,
            cutoff_at: None,
            cut_off: false,
            procs: BTreeMap::new(),
            unacked: BTreeMap::new(),
        }
    }

    /// Until when its link holds a message up at `t` going `up` (or down), if it does.
    fn held_until(&self, t: u64, up: bool) -> Option<u64> {
        self.cuts
            .iter()
            .filter(|&&(from, to, way)| {
                t >= from
                    && t < to
                    && match way {
                        Way::Both => true,
                        Way::Up => up,
                        Way::Down => !up,
                    }
            })
            .map(|&(_, to, _)| to)
            .max()
    }

    /// How much longer a message sent at `t` takes, `up` or down.
    fn lag(&self, t: u64, up: bool) -> u64 {
        self.lags
            .iter()
            .filter(|&&(from, to, _, _)| t >= from && t < to)
            .map(|&(_, _, u, d)| if up { u } else { d })
            .max()
            .unwrap_or(0)
    }

    fn stream(&self) -> Option<u64> {
        self.session.or(self.connecting)
    }
}

/// When one process of a task instance ran.
#[derive(Debug)]
struct Interval {
    key: Key,
    attempt: String,
    inst: usize,
    start: u64,
    end: Option<u64>,
}

struct Sim {
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<(u64, u64)>>,
    events: BTreeMap<u64, Event>,
    rng: Rng,
    timings: Timings,
    db: Database,
    core: Option<MasterCore>,
    weakened: Option<Weakened>,
    epoch: i64,
    /// Each worker's current stream at the master, and the instance on it.
    current: BTreeMap<usize, (u64, usize)>,
    /// The latest worker message heard on each stream.
    heard: BTreeMap<u64, u64>,
    /// The last number the master used on each stream (B240).
    numbers: BTreeMap<u64, u64>,
    /// Each stream's ID in the master's `Arrivals`.
    stream_ids: BTreeMap<u64, u64>,
    /// A deposed master's streams, which it answers heartbeats on until then, with its epoch.
    deposed: Option<(u64, i64, BTreeMap<u64, usize>)>,
    instances: Vec<Instance>,
    ppm: [i64; 2],
    next_sid: u64,
    /// The last delivery on each stream each way: in order.
    fifo: BTreeMap<(u64, bool), u64>,
    intervals: Vec<Interval>,
    failure: Option<String>,
}

fn ts(ms: u64) -> Timestamp {
    let origin: Timestamp = "2026-10-21T08:00:00Z".parse().unwrap();
    origin
        .checked_add(SignedDuration::from_millis(i64::try_from(ms).unwrap()))
        .unwrap()
}

fn settings(t: Timings) -> Settings {
    Settings {
        heartbeat_interval: Duration::from_millis(HEARTBEAT),
        lease_timeout: Duration::from_millis(t.lease),
        takeover_wait: Duration::from_millis(t.takeover_wait),
        cancel_grace: Duration::from_millis(t.cancel_grace),
        cleanup_move_wait: Duration::from_millis(t.move_wait),
        set_aside: Duration::from_secs(2),
        ..Settings::default()
    }
}

fn dag_file() -> ParsedFile {
    serde_json::from_value(json!({
        "path": "sync.py",
        "dags": [{
            "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
            "trigger": {"kind": "none"}, "initial_since": null,
            "max_active_runs": 2,
            "tasks": [{
                "id": "extract", "kind": "python",
                "settings": {
                    "retries": 3, "retry_delay_ms": 0, "timeout_ms": null,
                    "queue": "default", "max_output_size": 1_048_576
                },
                "output_type": {"type": "none"},
                "cleanup": {"run": "python", "retries": 0, "retry_delay_ms": 0, "anywhere": true}
            }, {
                "id": ORDINARY, "kind": "python",
                "settings": {
                    "retries": 3, "retry_delay_ms": 0, "timeout_ms": null,
                    "queue": "default", "max_output_size": 1_048_576
                },
                "output_type": {"type": "none"}
            }],
            "edges": []
        }],
        "error": null
    }))
    .unwrap()
}

fn root() -> pb::RootHash {
    pb::RootHash {
        kind: pb::RootKind::Dag.into(),
        root: "etl".to_owned(),
        content_hash: "h1".to_owned(),
    }
}

fn register(worker: usize, incarnation: u32, journal: Vec<pb::JournalEntry>) -> pb::Register {
    pb::Register {
        running_triggers: Vec::new(),
        watcher_slots: 0,
        worker_id: WORKERS[worker].to_owned(),
        incarnation_id: format!("i{incarnation}"),
        protocol: Some(PROTOCOL_VERSION),
        schedr_version: "1.0.0".to_owned(),
        queues: vec![],
        slots: u32::try_from(SLOTS).unwrap(),
        log_endpoint: "127.0.0.1:8081".to_owned(),
        roots: vec![root()],
        secret_storages: vec![],
        journal,
        process_id: format!("p-{incarnation}"),
        config_json: r#"{"worker":{"slots":4}}"#.to_owned(),
    }
}

fn finished(attempt_id: &str, status: pb::AttemptStatus, cleaned: bool) -> pb::Finished {
    pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: status.into(),
        cleanup: cleaned.then(|| pb::CleanupResult {
            status: pb::CleanupStatus::Ok.into(),
            tries: 1,
            ..pb::CleanupResult::default()
        }),
        ..pb::Finished::default()
    }
}

/// A master with DAG `sync` stored, no worker connected.
async fn master(timings: Timings, weakened: Option<Weakened>) -> (Database, MasterCore) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let mut core = MasterCore::new(db.clone(), settings(timings));
    if let Some(safeguard) = weakened {
        core.weaken(safeguard);
    }
    let (_, out) = core.register(&register(0, 0, vec![]), ts(0)).await.unwrap();
    let Some(master_message::Message::AssignParse(job)) = &out[0].message.message else {
        panic!("expected AssignParse: {out:?}")
    };
    let upload = pb::ParseResult {
        parse_job_id: job.parse_job_id.clone(),
        worker_id: "w1".to_owned(),
        root: Some(root()),
        schedr_version: "1.0.0".to_owned(),
        files: vec![parsed_file_to_proto(&dag_file())],
    };
    core.upload(upload, ts(0)).await.unwrap();
    core.disconnected("w1", ts(0)).await.unwrap();
    (db, core)
}

impl Sim {
    fn at(&mut self, t: u64, event: Event) {
        self.seq += 1;
        self.events.insert(self.seq, event);
        self.queue.push(Reverse((t, self.seq)));
    }

    fn fail(&mut self, why: &str) {
        if self.failure.is_none() {
            self.failure = Some(format!("at {} ms: {why}", self.now));
        }
    }

    fn local(&self, inst: usize) -> schedr_agent::master_time::Local {
        self.instances[inst].clock.local(self.now * MS)
    }

    /// The real time (ms, rounded up) at which `inst`'s boot clock reads `boot`.
    fn when_boot(&self, inst: usize, boot: Duration) -> u64 {
        self.instances[inst]
            .clock
            .when_boot(self.now * MS, boot)
            .div_ceil(MS)
    }

    /// A message on `sid`, `up` to the master or down: held up by a cut, in order, lost if
    /// the stream is gone when it gets there (checked on arrival).
    fn send(&mut self, inst: usize, sid: u64, up: bool, event: Event) {
        let from = match self.instances[inst].held_until(self.now, up) {
            Some(u64::MAX) => return,
            Some(until) => until,
            None => self.now,
        };
        let last = self.fifo.get(&(sid, up)).copied().unwrap_or(0);
        let lag = self.instances[inst].lag(self.now, up);
        let at = (from + lag + self.rng.delay()).max(last);
        self.fifo.insert((sid, up), at);
        self.at(at, event);
    }

    fn up(&mut self, inst: usize, message: worker_message::Message) {
        if let Some(sid) = self.instances[inst].session {
            self.up_on(inst, sid, message);
        }
    }

    /// Sends `message` up stream `sid`, stamped from the instance's outbox as the agent's
    /// sender does (B240).
    fn up_on(&mut self, inst: usize, sid: u64, message: worker_message::Message) {
        let local = self.local(inst);
        let clock = lock(&self.instances[inst].time.outbox()).stamp(local);
        self.send(
            inst,
            sid,
            true,
            Event::AtMaster {
                inst,
                sid,
                message: Box::new(message),
                clock,
            },
        );
    }

    fn stamp(&self, sid: u64, epoch: i64) -> pb::MasterClock {
        let ms = ts(self.now).as_millisecond();
        pb::MasterClock {
            timeline: u64::try_from(epoch).unwrap() + 1,
            master_ms: ms,
            heard_sequence: self.heard.get(&sid).copied().unwrap_or(0),
            wall_ms: ms,
            lease_epoch: epoch,
            sequence: 0,
        }
    }

    /// Sends `message` down stream `sid`, numbered as the service numbers it: its number.
    fn down(&mut self, inst: usize, sid: u64, message: master_message::Message, epoch: i64) -> u64 {
        let number = {
            let last = self.numbers.entry(sid).or_default();
            *last += 1;
            *last
        };
        let mut clock = self.stamp(sid, epoch);
        clock.sequence = number;
        let message = pb::MasterMessage {
            message: Some(message),
            clock: Some(clock),
        };
        let message = Box::new(message);
        self.send(inst, sid, false, Event::AtWorker { inst, sid, message });
        number
    }

    fn deliver(&mut self, out: Vec<Outgoing>) {
        for o in out {
            let worker = WORKERS.iter().position(|w| *w == o.worker_id).unwrap();
            let Some(&(sid, inst)) = self.current.get(&worker) else {
                continue;
            };
            if let Some(message) = o.message.message {
                let assigned = match &message {
                    master_message::Message::AssignTask(assign) => Some(assign.attempt_id.clone()),
                    _ => None,
                };
                let number = self.down(inst, sid, message, self.epoch);
                if let (Some(attempt_id), Some(core), Some(&stream)) =
                    (assigned, &self.core, self.stream_ids.get(&sid))
                {
                    core.arrivals()
                        .assigned(WORKERS[worker], stream, &attempt_id, number);
                }
            }
        }
    }

    /// The master's side of `worker`'s stream `sid` ended.
    async fn master_end(&mut self, worker: usize, sid: u64) {
        if self.current.get(&worker).map(|c| c.0) != Some(sid) {
            return;
        }
        self.current.remove(&worker);
        self.heard.remove(&sid);
        if let Some(core) = &mut self.core
            && let Err(error) = core.disconnected(WORKERS[worker], ts(self.now)).await
        {
            self.fail(&format!("disconnect: {error}"));
        }
    }

    /// The worker's side of its stream ends (it tries again after its backoff).
    fn worker_end(&mut self, inst: usize, sid: u64) {
        let lease = self.timings.lease;
        let i = &mut self.instances[inst];
        if i.stream() != Some(sid) {
            return;
        }
        i.session = None;
        i.connecting = None;
        i.next_try = self.now + i.backoff;
        i.backoff = (i.backoff * 2).min(lease / 2);
    }

    /// The worker closes its stream: the master sees it once the close gets there.
    fn worker_closes(&mut self, inst: usize) {
        let Some(sid) = self.instances[inst].stream() else {
            return;
        };
        self.worker_end(inst, sid);
        let from = match self.instances[inst].held_until(self.now, true) {
            Some(until) => until.min(self.now + KEEPALIVE),
            None => self.now,
        };
        self.closed(inst, sid, from);
    }

    /// `inst` closed stream `sid` at `from`: the master sees it after what was sent before
    /// (one stream, in order).
    fn closed(&mut self, inst: usize, sid: u64, from: u64) {
        let worker = self.instances[inst].worker;
        let last = self.fifo.get(&(sid, true)).copied().unwrap_or(0);
        let at = (from + self.rng.delay()).max(last);
        self.at(at, Event::MasterSeesEnd(worker, sid));
    }

    /// No two processes of one task instance at once (rules 3 and 4).
    fn check_alone(&mut self, key: &Key, attempt: &str) {
        let now = self.now;
        let beside = self
            .intervals
            .iter()
            .find(|i| i.key == *key && i.end.is_none_or(|end| end > now))
            .map(|i| format!("{} on instance {} since {} ms", i.attempt, i.inst, i.start));
        if let Some(beside) = beside {
            self.fail(&format!("{attempt} of {key:?} starts beside {beside}"));
        }
    }

    /// An ordinary task may run beside an earlier attempt only if the master ended that one
    /// `lost` (its worker lost or replaced, which keeps running it: at least once, decision
    /// 252); beside one taken back as never started (`assignment_expired`), or still open, it
    /// runs twice.
    async fn check_alone_ordinary(&mut self, key: &Key, attempt: &str) {
        let now = self.now;
        let beside: Vec<(String, usize, u64)> = self
            .intervals
            .iter()
            .filter(|i| i.key == *key && i.end.is_none_or(|end| end > now))
            .map(|i| (i.attempt.clone(), i.inst, i.start))
            .collect();
        for (other, inst, start) in beside {
            let status = self
                .db
                .attempt(&other)
                .await
                .ok()
                .flatten()
                .and_then(|a| a.status);
            if status != Some(schedr_model::AttemptStatus::Lost) {
                self.fail(&format!(
                    "{attempt} of {key:?} starts beside {other} on instance {inst} since {start} \
                     ms, ended {status:?}"
                ));
            }
        }
    }

    fn end_interval(&mut self, interval: usize) {
        if self.intervals[interval].end.is_none() {
            self.intervals[interval].end = Some(self.now);
        }
    }

    async fn run(&mut self, scenario: &Scenario) {
        for (at, fault) in &scenario.faults {
            self.at(*at, Event::Fault(fault.clone()));
        }
        let mut t = 1_000;
        while t < FAULTS_END {
            self.at(t, Event::Trigger);
            t += TRIGGER_EVERY;
        }
        self.at(0, Event::Tick);
        for inst in 0..self.instances.len() {
            self.at(0, Event::Alive(inst));
        }
        let mut settling = FAULTS_END + SETTLE_CHECK;
        while let Some(Reverse((t, seq))) = self.queue.pop() {
            if t > END || self.failure.is_some() {
                break;
            }
            // Past the faults, ended once nothing is left open (else rule 2 at the end).
            if t >= settling {
                settling += SETTLE_CHECK;
                if self.open().await.is_empty() {
                    break;
                }
            }
            self.now = t;
            let event = self.events.remove(&seq).unwrap();
            self.handle(event).await;
        }
    }

    async fn handle(&mut self, event: Event) {
        match event {
            Event::Tick => {
                if let Some(core) = &mut self.core {
                    match core.tick(ts(self.now)).await {
                        Ok(out) => self.deliver(out),
                        Err(error) => self.fail(&format!("tick: {error}")),
                    }
                }
                self.at(self.now + TICK, Event::Tick);
            }
            Event::Trigger => {
                let command = schedr_model::commands::Command::TriggerRun {
                    event: None,
                    params: None,
                    dag_id: schedr_model::DagId::new("sync").unwrap(),
                    planned_for: None,
                };
                self.db
                    .insert_command(&command, "anna", ts(self.now))
                    .await
                    .unwrap();
            }
            Event::Alive(inst) => self.alive(inst),
            Event::Heartbeat(inst, sid) => self.heartbeat(inst, sid),
            Event::ProcEnd(inst, attempt) => self.proc_end(inst, &attempt),
            Event::AtMaster {
                inst,
                sid,
                message,
                clock,
            } => self.at_master(inst, sid, *message, clock).await,
            Event::AtWorker { inst, sid, message } => self.at_worker(inst, sid, *message).await,
            Event::Refused(inst, sid) | Event::WorkerSeesEnd(inst, sid) => {
                self.worker_end(inst, sid);
            }
            Event::MasterSeesEnd(worker, sid) => self.master_end(worker, sid).await,
            Event::Break(inst) => {
                if let Some(sid) = self.instances[inst].stream() {
                    self.worker_end(inst, sid);
                    let worker = self.instances[inst].worker;
                    // The master sees the end after what the stream sent before it, one due
                    // this millisecond too: a `Register` on its way is answered, then ended
                    // (else it stayed live).
                    let last = self.fifo.get(&(sid, true)).copied().unwrap_or(0);
                    self.at(last.max(self.now), Event::MasterSeesEnd(worker, sid));
                }
            }
            Event::Fault(fault) => self.fault(&fault),
            Event::Incarnation(worker) => {
                let incarnation = self
                    .instances
                    .iter()
                    .filter(|i| i.worker == worker)
                    .map(|i| i.incarnation)
                    .max()
                    .unwrap_or(0)
                    + 1;
                self.instances
                    .push(Instance::new(worker, incarnation, self.ppm[worker]));
                let inst = self.instances.len() - 1;
                self.instances[inst].next_try = self.now;
                self.at(self.now, Event::Alive(inst));
            }
            Event::MasterUp => {
                let mut core = MasterCore::new(self.db.clone(), settings(self.timings));
                if let Some(safeguard) = self.weakened {
                    core.weaken(safeguard);
                }
                if let Err(error) = core.restore(ts(self.now)).await {
                    self.fail(&format!("restore: {error}"));
                }
                self.core = Some(core);
            }
            Event::DeposedEnds => {
                if let Some((_, _, streams)) = self.deposed.take() {
                    for (sid, inst) in streams {
                        let at = self.now + self.rng.delay();
                        self.at(at, Event::WorkerSeesEnd(inst, sid));
                    }
                }
            }
        }
    }

    /// The current, live instance of `worker`.
    fn instance_of(&self, worker: usize) -> Option<usize> {
        self.instances
            .iter()
            .rposition(|i| i.worker == worker && i.alive && i.cuts.iter().all(|c| c.1 != u64::MAX))
    }

    fn fault(&mut self, fault: &Fault) {
        match *fault {
            Fault::Cut { worker, len, way } => {
                let Some(inst) = self.instance_of(worker) else {
                    return;
                };
                self.instances[inst]
                    .cuts
                    .push((self.now, self.now + len, way));
                if len > KEEPALIVE {
                    self.at(self.now + KEEPALIVE, Event::Break(inst));
                }
            }
            Fault::Lag {
                worker,
                len,
                up,
                down,
            } => {
                if let Some(inst) = self.instance_of(worker) {
                    self.instances[inst]
                        .lags
                        .push((self.now, self.now + len, up, down));
                }
            }
            Fault::Drop { worker } => {
                if let Some(inst) = self.instance_of(worker) {
                    self.at(self.now, Event::Break(inst));
                }
            }
            Fault::Replace {
                worker,
                old_lives,
                gap,
            } => {
                let Some(inst) = self.instance_of(worker) else {
                    return;
                };
                if old_lives {
                    // Its stream closed, the master hears of it at once; the process runs on
                    // with no link, until its cutoff stops what it must.
                    if let Some(sid) = self.instances[inst].stream() {
                        self.closed(inst, sid, self.now);
                    }
                    self.instances[inst]
                        .cuts
                        .push((self.now, u64::MAX, Way::Both));
                } else {
                    let i = &mut self.instances[inst];
                    i.alive = false;
                    let intervals: Vec<usize> = i.procs.values().map(|p| p.interval).collect();
                    let sid = i.stream();
                    for interval in intervals {
                        self.end_interval(interval);
                    }
                    if let Some(sid) = sid {
                        self.closed(inst, sid, self.now);
                    }
                }
                self.at(self.now + gap, Event::Incarnation(worker));
            }
            Fault::Master { deposed, gap } => {
                if self.core.is_none() {
                    return;
                }
                self.core = None;
                let streams: BTreeMap<u64, usize> = self
                    .current
                    .values()
                    .map(|&(sid, inst)| (sid, inst))
                    .collect();
                self.current.clear();
                self.deposed = Some((self.now + deposed, self.epoch, streams));
                self.epoch += 1;
                self.at(self.now + deposed, Event::DeposedEnds);
                self.at(self.now + gap, Event::MasterUp);
            }
        }
    }

    /// A worker's alive check: its cutoff (it stops its tasks and closes its stream), and a
    /// connection tried again.
    fn alive(&mut self, inst: usize) {
        if !self.instances[inst].alive {
            return;
        }
        let local = self.local(inst);
        let i = &self.instances[inst];
        let silent = i
            .lease
            .is_some_and(|lease| local.boot >= i.heard_boot + lease)
            && i.stream().is_some();
        let past = i.cutoff_at.is_some_and(|at| self.now >= at);
        if !i.cut_off && (silent || past) {
            self.cut_off(inst);
        }
        if silent || (past && self.instances[inst].session.is_some()) {
            self.worker_closes(inst);
        }
        let i = &self.instances[inst];
        if i.stream().is_none() && self.now >= i.next_try {
            self.connect(inst);
        }
        self.at(self.now + ALIVE, Event::Alive(inst));
    }

    /// No word from the master: the tasks whose cleanup may move are stopped (decision 252).
    fn cut_off(&mut self, inst: usize) {
        self.instances[inst].cut_off = true;
        let ids: Vec<String> = self.instances[inst]
            .procs
            .iter()
            .filter(|(_, p)| !p.ordinary && !p.moved && !p.stopping && !p.held)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let ends_at = self.now + self.stop_time();
            let proc = self.instances[inst].procs.get_mut(&id).unwrap();
            proc.stopping = true;
            proc.ends_at = ends_at;
            self.at(ends_at, Event::ProcEnd(inst, id));
        }
    }

    /// How long a process takes to stop: half the time the whole cancel grace (one that
    /// ignores SIGTERM, killed at its end), else less.
    fn stop_time(&mut self) -> u64 {
        if self.rng.below(2) == 0 {
            self.timings.cancel_grace
        } else {
            self.rng.below(self.timings.cancel_grace)
        }
    }

    fn connect(&mut self, inst: usize) {
        let local = self.local(inst);
        let lease = self.timings.lease;
        let i = &mut self.instances[inst];
        if i.held_until(self.now, true).is_some() || i.held_until(self.now, false).is_some() {
            i.next_try = self.now + i.backoff;
            i.backoff = (i.backoff * 2).min(lease / 2);
            return;
        }
        self.next_sid += 1;
        let sid = self.next_sid;
        i.connecting = Some(sid);
        i.time = MasterTime::with_window(Duration::from_mins(10));
        i.heard_boot = local.boot;
        let mut journal: Vec<pb::JournalEntry> = i
            .procs
            .keys()
            .map(|id| pb::JournalEntry {
                attempt_id: id.clone(),
                state: pb::JournalState::Running.into(),
                handles: 0,
            })
            .collect();
        journal.extend(i.unacked.keys().map(|id| pb::JournalEntry {
            attempt_id: id.clone(),
            state: pb::JournalState::Finished.into(),
            handles: 0,
        }));
        let message = worker_message::Message::Register(register(i.worker, i.incarnation, journal));
        self.up_on(inst, sid, message);
    }

    fn heartbeat(&mut self, inst: usize, sid: u64) {
        let i = &self.instances[inst];
        if !i.alive || i.session != Some(sid) {
            return;
        }
        let local = self.local(inst);
        let i = &self.instances[inst];
        let beat = pb::Heartbeat {
            free_slots: u32::try_from(SLOTS.saturating_sub(i.procs.len())).unwrap(),
            running_attempt_ids: i.procs.keys().cloned().collect(),
            ..pb::Heartbeat::default()
        };
        self.up_on(inst, sid, worker_message::Message::Heartbeat(beat));
        let next = self.when_boot(inst, local.boot + Duration::from_millis(HEARTBEAT));
        self.at(next, Event::Heartbeat(inst, sid));
    }

    fn proc_end(&mut self, inst: usize, attempt: &str) {
        let Some(proc) = self.instances[inst].procs.get(attempt) else {
            return;
        };
        if proc.ends_at != self.now || proc.held || !self.instances[inst].alive {
            return;
        }
        let (interval, killed, moved, ordinary) =
            (proc.interval, proc.killed, proc.moved, proc.ordinary);
        self.end_interval(interval);
        let i = &mut self.instances[inst];
        if killed {
            i.procs.remove(attempt);
            return;
        }
        if !moved && !ordinary && i.cut_off {
            // Its cleanup waits for the master's answer (decision 254).
            i.procs.get_mut(attempt).unwrap().held = true;
            return;
        }
        i.procs.remove(attempt);
        let status = if moved {
            pb::AttemptStatus::Lost
        } else {
            pb::AttemptStatus::Success
        };
        let result = finished(attempt, status, !ordinary);
        i.unacked.insert(attempt.to_owned(), result.clone());
        self.up(inst, worker_message::Message::Finished(result));
    }

    async fn at_master(
        &mut self,
        inst: usize,
        sid: u64,
        message: worker_message::Message,
        clock: pb::WorkerClock,
    ) {
        let worker = self.instances[inst].worker;
        let id = WORKERS[worker];
        if let Some((until, epoch, streams)) = &self.deposed
            && self.now < *until
            && streams.contains_key(&sid)
        {
            let epoch = *epoch;
            let heard = self.heard.entry(sid).or_default();
            *heard = (*heard).max(clock.sequence);
            if let worker_message::Message::Heartbeat(_) = &message {
                let ack = master_message::Message::HeartbeatAck(pb::HeartbeatAck {});
                self.down(inst, sid, ack, epoch);
            }
            return;
        }
        if let worker_message::Message::Register(register) = message {
            self.at_register(inst, sid, register).await;
            return;
        }
        if self.current.get(&worker) != Some(&(sid, inst)) {
            return;
        }
        let Some(core) = &mut self.core else {
            return;
        };
        let arrivals = core.arrivals();
        arrivals.arrived(id, ts(self.now));
        // As the service's reader: every message's number heard, its echo read (B240).
        let heard = self.heard.entry(sid).or_default();
        *heard = (*heard).max(clock.sequence);
        if let Some(&stream) = self.stream_ids.get(&sid) {
            arrivals.read(id, stream, clock.heard_sequence);
        }
        match &message {
            worker_message::Message::Heartbeat(_) => {
                let ack = master_message::Message::HeartbeatAck(pb::HeartbeatAck {});
                self.down(inst, sid, ack, self.epoch);
            }
            worker_message::Message::Started(pb::Started { attempt_id, .. })
            | worker_message::Message::Finished(pb::Finished { attempt_id, .. }) => {
                arrivals.start_arrived(id, attempt_id);
                arrivals.answered(id, attempt_id);
            }
            worker_message::Message::Rejected(pb::Rejected { attempt_id, .. }) => {
                arrivals.answered(id, attempt_id);
            }
            _ => {}
        }
        let core = self.core.as_mut().unwrap();
        match core.handle(id, message, ts(self.now)).await {
            Ok(out) => self.deliver(out),
            Err(error) if error.is_data_fault() => self.fail(&format!("refused: {error}")),
            // The stream ends: the worker registers again.
            Err(_) => {
                self.master_end(worker, sid).await;
                let at = self.now + self.rng.delay();
                self.at(at, Event::WorkerSeesEnd(inst, sid));
            }
        }
    }

    async fn at_register(&mut self, inst: usize, sid: u64, register: pb::Register) {
        let worker = self.instances[inst].worker;
        let id = WORKERS[worker];
        let Some(core) = &mut self.core else {
            self.send(inst, sid, false, Event::Refused(inst, sid));
            return;
        };
        let arrivals = core.arrivals();
        arrivals.arrived(id, ts(self.now));
        let added: Vec<String> = register
            .journal
            .iter()
            .filter(|e| arrivals.start_known(id, &e.attempt_id))
            .map(|e| e.attempt_id.clone())
            .collect();
        match core.register(&register, ts(self.now)).await {
            Ok((registered, out)) => {
                arrivals.forget_starts(id);
                self.stream_ids.insert(sid, arrivals.new_stream(id));
                self.current.insert(worker, (sid, inst));
                self.heard.insert(sid, 0);
                let registered = master_message::Message::Registered(registered);
                self.down(inst, sid, registered, self.epoch);
                self.deliver(out);
            }
            Err(schedr_master::core::CoreError::Refused(_)) => {
                for attempt_id in &added {
                    arrivals.start_handled(attempt_id);
                }
                self.send(inst, sid, false, Event::Refused(inst, sid));
            }
            Err(error) => self.fail(&format!("register: {error}")),
        }
    }

    async fn at_worker(&mut self, inst: usize, sid: u64, message: pb::MasterMessage) {
        let i = &self.instances[inst];
        if !i.alive || i.stream() != Some(sid) {
            return;
        }
        let local = self.local(inst);
        // Past the lease, a message doesn't count as the master's word (B162, B163).
        if i.lease
            .is_some_and(|lease| local.boot >= i.heard_boot + lease)
        {
            if !i.cut_off {
                self.cut_off(inst);
            }
            self.worker_closes(inst);
            return;
        }
        let i = &mut self.instances[inst];
        if let Some(clock) = &message.clock {
            i.time.heard(clock, local);
        }
        let number = message.clock.as_ref().map_or(0, |clock| clock.sequence);
        i.heard_boot = local.boot;
        let Some(message) = message.message else {
            return;
        };
        if let master_message::Message::Registered(registered) = &message {
            i.session = Some(sid);
            i.connecting = None;
            i.backoff = 500;
            i.cut_off = false;
            i.lease = registered
                .timing
                .as_ref()
                .map(|t| Duration::from_millis(t.lease_timeout_ms));
            let next = self.when_boot(inst, local.boot + Duration::from_millis(HEARTBEAT));
            self.at(next, Event::Heartbeat(inst, sid));
        }
        let i = &mut self.instances[inst];
        if let Some(lease) = i.lease
            && let Some(left) = i.time.cutoff_in(lease, local)
        {
            let at = self.when_boot(inst, local.boot + left);
            self.instances[inst].cutoff_at = Some(at);
        }
        if self.instances[inst].session != Some(sid) {
            return;
        }
        match message {
            master_message::Message::Registered(registered) => {
                for decision in registered.decisions {
                    self.decide(inst, &decision);
                }
            }
            master_message::Message::AssignTask(assign) => self.assign(inst, assign).await,
            master_message::Message::CancelAttempt(cancel) => {
                self.kill(inst, &cancel.attempt_id);
            }
            master_message::Message::FinishedAck(ack) => {
                self.instances[inst].unacked.remove(&ack.attempt_id);
            }
            _ => {}
        }
        // Handled: what it sent is queued, before anything naming it (B240).
        lock(&self.instances[inst].time.outbox()).handled(number);
    }

    fn kill(&mut self, inst: usize, attempt: &str) {
        let ends_at = self.now + self.stop_time();
        let Some(proc) = self.instances[inst].procs.get_mut(attempt) else {
            return;
        };
        proc.killed = true;
        if proc.held {
            self.instances[inst].procs.remove(attempt);
        } else if !proc.stopping {
            proc.stopping = true;
            proc.ends_at = ends_at;
            self.at(ends_at, Event::ProcEnd(inst, attempt.to_owned()));
        }
    }

    fn decide(&mut self, inst: usize, decision: &pb::JournalDecision) {
        let id = decision.attempt_id.as_str();
        match pb::AttemptDecision::try_from(decision.decision) {
            Ok(pb::AttemptDecision::Continue) => {
                if let Some(proc) = self.instances[inst].procs.get(id)
                    && proc.held
                {
                    // Its cleanup runs here, now: alone (rule 4).
                    let key = proc.key.clone();
                    self.instances[inst].procs.remove(id);
                    self.check_alone(&key, id);
                    let result = finished(id, pb::AttemptStatus::Lost, true);
                    self.instances[inst]
                        .unacked
                        .insert(id.to_owned(), result.clone());
                    self.up(inst, worker_message::Message::Finished(result));
                } else if let Some(result) = self.instances[inst].unacked.get(id).cloned() {
                    self.up(inst, worker_message::Message::Finished(result));
                }
            }
            Ok(pb::AttemptDecision::Ack) => {
                self.instances[inst].unacked.remove(id);
            }
            _ => {
                self.instances[inst].unacked.remove(id);
                self.kill(inst, id);
            }
        }
    }

    async fn assign(&mut self, inst: usize, assign: pb::AssignTask) {
        let id = assign.attempt_id.clone();
        // Already here, running or its result not acknowledged: ignored, as the agent does (a
        // moved cleanup given back to the worker whose result is on its way).
        let here = &self.instances[inst];
        if here.procs.contains_key(&id) || here.unacked.contains_key(&id) {
            return;
        }
        let i = &self.instances[inst];
        let reason = if i.procs.len() >= SLOTS || i.cut_off {
            Some(pb::RejectReason::NoFreeSlot)
        } else {
            None
        };
        if let Some(reason) = reason {
            let rejected = pb::Rejected {
                attempt_id: id,
                reason: reason.into(),
                detail: String::new(),
            };
            self.up(inst, worker_message::Message::Rejected(rejected));
            return;
        }
        // Rule 1: the master hasn't ended what starts.
        match self.db.attempt(&id).await {
            Ok(Some(attempt)) if attempt.status.is_some() => {
                self.fail(&format!(
                    "{id} starts, ended by the master as {:?}",
                    attempt.status
                ));
            }
            Ok(_) => {}
            Err(error) => self.fail(&format!("attempt {id}: {error}")),
        }
        let run_id = assign
            .run
            .as_ref()
            .map(|r| r.run_id.clone())
            .unwrap_or_default();
        let key = (run_id, assign.task_id.clone(), assign.map_index);
        let ordinary = assign.task_id == ORDINARY;
        if ordinary {
            self.check_alone_ordinary(&key, &id).await;
        } else {
            self.check_alone(&key, &id);
        }
        let moved = assign.cleanup_only.is_some();
        let length = if moved {
            500 + self.rng.below(1_500)
        } else {
            3_000 + self.rng.below(17_000)
        };
        self.intervals.push(Interval {
            key: key.clone(),
            attempt: id.clone(),
            inst,
            start: self.now,
            end: None,
        });
        let proc = Proc {
            key,
            ordinary,
            moved,
            ends_at: self.now + length,
            stopping: false,
            killed: false,
            held: false,
            interval: self.intervals.len() - 1,
        };
        self.instances[inst].procs.insert(id.clone(), proc);
        self.at(self.now + length, Event::ProcEnd(inst, id.clone()));
        let started = pb::Started {
            attempt_id: id,
            start_method: pb::StartMethod::Spawn.into(),
        };
        self.up(inst, worker_message::Message::Started(started));
    }

    /// What hasn't ended: attempts, then runs.
    async fn open(&self) -> Vec<String> {
        let mut open: Vec<String> = self
            .db
            .open_attempts(None)
            .await
            .unwrap()
            .into_iter()
            .map(|a| format!("attempt {} on {:?}", a.id, a.worker_id))
            .collect();
        let filter = RunFilter {
            limit: 1_000,
            ..RunFilter::default()
        };
        open.extend(
            self.db
                .runs(&filter)
                .await
                .unwrap()
                .into_iter()
                .filter(|r| matches!(r.state, RunState::Queued | RunState::Running))
                .map(|r| format!("run {}", r.id)),
        );
        open
    }

    /// Rule 2, once everything has settled.
    async fn settled(&mut self) {
        let open = self.open().await;
        if !open.is_empty() {
            self.fail(&format!("open at the end: {open:?}"));
        }
    }
}

/// Runs `scenario` with `weakened` switched off; `Err` names the rule broken.
pub fn simulate(scenario: &Scenario, weakened: Option<Weakened>) -> Result<(), String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (db, core) = master(scenario.timings, weakened).await;
            let mut sim = Sim {
                now: 0,
                seq: 0,
                queue: BinaryHeap::new(),
                events: BTreeMap::new(),
                rng: Rng(scenario.seed),
                timings: scenario.timings,
                db,
                core: Some(core),
                weakened,
                epoch: 1,
                current: BTreeMap::new(),
                heard: BTreeMap::new(),
                numbers: BTreeMap::new(),
                stream_ids: BTreeMap::new(),
                deposed: None,
                instances: vec![
                    Instance::new(0, 1, scenario.ppm[0]),
                    Instance::new(1, 1, scenario.ppm[1]),
                ],
                ppm: scenario.ppm,
                next_sid: 0,
                fifo: BTreeMap::new(),
                intervals: Vec::new(),
                failure: None,
            };
            sim.run(scenario).await;
            if sim.failure.is_none() {
                sim.settled().await;
            }
            sim.failure.map_or(Ok(()), Err)
        })
}
