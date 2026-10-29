//! Quiet DAGs (see PROJECT.md, "Quiet DAGs"): which DAGs a scheduler round visits.
//!
//! **Busy** DAGs (a run in progress, a fire time coming up, or active not long ago) are visited
//! every round, as before. A DAG with nothing in progress for `quiet_dag_after` (or found idle
//! when it wasn't busy), and no fire time within `quiet_dag_wake_ahead`, turns **quiet**: rounds
//! skip it until it is woken, by its
//! timer (its next fire time less `quiet_dag_wake_ahead`) or by a mark from what changes it (a
//! run queued or ended, a command, a new version).
//!
//! The safety net is spread over the rounds: each round also **sweeps** a slice of the quiet
//! DAGs, those visited longest ago, sized so that every one is visited within
//! `quiet_dag_full_pass_interval`; and once per interval the round **checks** every active
//! DAG's ID and version against memory (a DAG not known, or of another version, is visited). A
//! missed mark delays a DAG by at most about that interval, never loses a run, and no round
//! costs what visiting every DAG costs.
//!
//! The state lives in the leading master's memory only: a new term starts with one round
//! visiting every DAG. A visit finding a DAG idle, with no fire time near, makes it quiet at
//! once, whatever woke it; only a DAG seen active stays busy for `quiet_dag_after`. Visiting a DAG that didn't need it creates nothing (a
//! scheduled run is unique per `planned_for`), so a wrong guess costs time, never runs.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_model::DagId;

use crate::metrics::MasterMetrics;

#[cfg(test)]
mod tests;

/// The `[master] quiet_dag*` settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuietSettings {
    /// Whether rounds skip quiet DAGs; `false` visits every DAG every round.
    pub enabled: bool,
    /// Nothing in progress this long (and no fire time within `wake_ahead`): quiet.
    pub after: Duration,
    /// How long before its next fire time a quiet DAG is woken.
    pub wake_ahead: Duration,
    /// Every DAG is visited within this long: the sweep's period, and the ID check's.
    pub full_pass_interval: Duration,
}

impl Default for QuietSettings {
    /// On; quiet after 60 s, woken 60 s ahead, every DAG visited within 60 s.
    fn default() -> Self {
        Self {
            enabled: true,
            after: Duration::from_mins(1),
            wake_ahead: Duration::from_mins(1),
            full_pass_interval: Duration::from_mins(1),
        }
    }
}

/// Why a quiet DAG was visited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WakeReason {
    /// Its next fire time is within `quiet_dag_wake_ahead`.
    Timer,
    /// A command about it (trigger, pause, unpause).
    Command,
    /// A run of it was stored queued (manual, chained, event, backfill).
    RunQueued,
    /// A run of it ended (a queued one may take its place).
    RunEnded,
    /// A new version of it, or it came back.
    NewVersion,
    /// The sweep or the ID check, which found it had something to do: a mark was missed.
    Sweep,
}

impl WakeReason {
    /// The name in metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timer => "timer",
            Self::Command => "command",
            Self::RunQueued => "run_queued",
            Self::RunEnded => "run_ended",
            Self::NewVersion => "new_version",
            Self::Sweep => "sweep",
        }
    }
}

/// Marks waking DAGs, shared by the parts of the master that change them (cheap to clone;
/// clones share the marks). The first reason a DAG is marked with in a round is kept.
#[derive(Debug, Clone, Default)]
pub struct Wakes(Arc<Mutex<HashMap<DagId, WakeReason>>>);

impl Wakes {
    /// Marks `dag` to be visited in the next round.
    pub fn wake(&self, dag: &DagId, reason: WakeReason) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(dag.clone())
            .or_insert(reason);
    }

    /// The marks so far, cleared.
    pub(crate) fn take(&self) -> HashMap<DagId, WakeReason> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// What a round visits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Visit {
    /// Every DAG.
    All,
    /// These DAGs only (sorted).
    Some(Vec<DagId>),
}

/// The DAGs a round visits, and whether it checks every active DAG's ID and version first
/// ([`QuietDags::checked`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Round {
    /// What it visits.
    pub visit: Visit,
    /// Whether the ID check is due.
    pub check: bool,
}

/// What a visit found of a DAG, for its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Visited<'a> {
    /// Its current version.
    pub version: &'a str,
    /// A run is in progress, or the round created or started one.
    pub active: bool,
    /// It is paused, or held (its fire times create nothing).
    pub paused: bool,
    /// Why it is held, when its file failed (decision 72): [`schedr_db::runs::Held::reason`].
    pub held: Option<&'a str>,
    /// Its next fire time after now, if it has one.
    pub next_fire: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Busy { active_at: Timestamp },
    Quiet { wake_at: Option<Timestamp> },
}

/// A round's changes to the cursors, kept until it is done.
#[derive(Debug)]
struct Pending {
    round_at: Timestamp,
    checked_at: Option<Timestamp>,
    sweep_credit: u128,
    marks: HashMap<DagId, WakeReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    state: State,
    version: String,
    visited_at: Timestamp,
}

/// Busy and quiet DAGs, and the marks waking them.
#[derive(Debug)]
pub struct QuietDags {
    settings: QuietSettings,
    dags: HashMap<DagId, Entry>,
    /// The DAGs held now (decision 72), to log a hold when it starts and when it lifts.
    held: HashSet<DagId>,
    /// Why each quiet DAG of the current round was visited.
    woken: HashMap<DagId, WakeReason>,
    /// When the last round was (`None` before the first: it visits every DAG).
    last_round: Option<Timestamp>,
    /// When the ID check last ran.
    checked_at: Option<Timestamp>,
    /// The sweep's share not taken yet, in DAG-milliseconds: what is carried to the next
    /// round (a DAG is due per interval's milliseconds of it).
    sweep_credit: u128,
    /// What the current round changes once it is done ([`Self::round_done`]); dropped, its
    /// marks put back, if it fails ([`Self::round_failed`]).
    pending: Option<Pending>,
    wakes: Wakes,
    metrics: MasterMetrics,
}

impl QuietDags {
    /// No DAG known yet: the first round visits every DAG.
    #[must_use]
    pub fn new(settings: QuietSettings, wakes: Wakes) -> Self {
        Self {
            settings,
            dags: HashMap::new(),
            held: HashSet::new(),
            woken: HashMap::new(),
            last_round: None,
            checked_at: None,
            sweep_credit: 0,
            pending: None,
            wakes,
            metrics: MasterMetrics::default(),
        }
    }

    /// Whether rounds skip quiet DAGs (`[master] quiet_dags`).
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.settings.enabled
    }

    /// Counts wakes and busy / quiet DAGs in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.metrics = metrics;
    }

    /// The DAGs the round at `now` visits: every DAG when off or in a term's first round;
    /// else the busy ones, the quiet ones whose timer is due, the marked ones and the sweep's
    /// slice.
    pub fn round(&mut self, now: Timestamp) -> Round {
        let marks = self.wakes.take();
        self.woken.clear();
        let mut pending = Pending {
            round_at: now,
            checked_at: self.checked_at,
            sweep_credit: self.sweep_credit,
            marks,
        };
        let Some(last_round) = self.last_round.filter(|_| self.settings.enabled) else {
            pending.checked_at = Some(now);
            self.pending = Some(pending);
            return Round {
                visit: Visit::All,
                check: false,
            };
        };
        if now < last_round {
            // The clock stepped back: what was stamped in the future is "now", or the sweep
            // would keep re-choosing what it visits from now on (stamped earlier) over it.
            for entry in self.dags.values_mut() {
                entry.visited_at = entry.visited_at.min(now);
            }
            pending.checked_at = pending.checked_at.map(|at| at.min(now));
        }
        let interval = signed(self.settings.full_pass_interval);
        let check = pending
            .checked_at
            .is_none_or(|at| now.duration_since(at) >= interval);
        if check {
            pending.checked_at = Some(now);
        }
        let mut visit: HashSet<DagId> = HashSet::new();
        let mut quiet: Vec<(Timestamp, &DagId)> = Vec::new();
        for (dag, entry) in &self.dags {
            match entry.state {
                State::Busy { .. } => {
                    visit.insert(dag.clone());
                }
                State::Quiet { wake_at } => {
                    if let Some(reason) = pending.marks.get(dag) {
                        self.woken.insert(dag.clone(), *reason);
                        visit.insert(dag.clone());
                    } else if wake_at.is_some_and(|at| at <= now) {
                        self.woken.insert(dag.clone(), WakeReason::Timer);
                        visit.insert(dag.clone());
                    } else {
                        quiet.push((entry.visited_at, dag));
                    }
                }
            }
        }
        // The sweep: the quiet DAGs visited longest ago, as many as the time since the last
        // round is of the interval (all of them within it, however many rounds there are).
        let (slice, credit) = sweep_size(
            quiet.len(),
            pending.sweep_credit,
            now.duration_since(last_round),
            interval,
        );
        pending.sweep_credit = credit;
        quiet.sort();
        for (_, dag) in quiet.into_iter().take(slice) {
            self.woken.insert(dag.clone(), WakeReason::Sweep);
            visit.insert(dag.clone());
        }
        visit.extend(pending.marks.keys().cloned());
        let mut visit: Vec<DagId> = visit.into_iter().collect();
        visit.sort();
        self.pending = Some(pending);
        Round {
            visit: Visit::Some(visit),
            check,
        }
    }

    /// The round failed (the database): nothing it would have changed is kept, and its marks
    /// are put back, so the next round is as this one would have been (a term's first round
    /// again visits every DAG).
    pub fn round_failed(&mut self) {
        if let Some(pending) = self.pending.take() {
            for (dag, reason) in pending.marks {
                self.wakes.wake(&dag, reason);
            }
        }
        self.woken.clear();
    }

    /// The ID check: given every active DAG's ID and current version, forgets the DAGs not
    /// among them and returns those to visit as well: not known, or of another version than
    /// known (a missed mark). The quiet ones among them count as swept.
    pub fn checked(&mut self, active: &[(DagId, String)]) -> Vec<DagId> {
        let ids: HashSet<&DagId> = active.iter().map(|(dag, _)| dag).collect();
        self.dags.retain(|dag, _| ids.contains(dag));
        let mut visit = Vec::new();
        for (dag, version) in active {
            match self.dags.get(dag) {
                Some(entry) if entry.version == *version => {}
                known => {
                    if matches!(
                        known,
                        Some(Entry {
                            state: State::Quiet { .. },
                            ..
                        })
                    ) {
                        self.woken.entry(dag.clone()).or_insert(WakeReason::Sweep);
                    }
                    visit.push(dag.clone());
                }
            }
        }
        visit
    }

    /// Notes what the round at `now` found of `dag`.
    pub fn visited(&mut self, dag: &DagId, found: Visited<'_>, now: Timestamp) {
        self.note_held(dag, found.held);
        if !self.settings.enabled {
            return;
        }
        let woken = self.woken.get(dag).copied();
        if let Some(reason) = woken
            && (reason != WakeReason::Sweep || found.active)
        {
            self.metrics.dag_woken(reason.as_str());
        }
        let busy_now = State::Busy { active_at: now };
        let state = match self.dags.get(dag).map(|entry| entry.state) {
            _ if found.active => busy_now,
            // Unknown (new, or every DAG in a new term) or quiet, and found with nothing to
            // do: quiet at once, unless a fire time is near. Only a DAG seen active stays busy
            // for `quiet_dag_after`: a master's start, a deploy changing every DAG or a mass
            // unpause costs one round visiting them, not a minute of such rounds.
            None | Some(State::Quiet { .. }) => self.quiet(found, now).unwrap_or(busy_now),
            Some(State::Busy { active_at }) => {
                let idle = now.duration_since(active_at) >= signed(self.settings.after);
                let busy = State::Busy { active_at };
                if idle {
                    self.quiet(found, now).unwrap_or(busy)
                } else {
                    busy
                }
            }
        };
        let entry = Entry {
            state,
            version: found.version.to_owned(),
            visited_at: now,
        };
        self.dags.insert(dag.clone(), entry);
    }

    /// Logs a DAG's hold when it starts and when it lifts (not every round).
    fn note_held(&mut self, dag: &DagId, held: Option<&str>) {
        match held {
            Some(why) if self.held.insert(dag.clone()) => tracing::warn!(
                dag_id = dag.as_str(),
                why,
                "DAG held (its file failed); no run is created or started until the file is fixed"
            ),
            None if self.held.remove(dag) => tracing::info!(
                dag_id = dag.as_str(),
                "DAG no longer held: its file imports again; missed fire times follow its \
                 catchup setting"
            ),
            _ => {}
        }
    }

    /// After a round: forgets the visited DAGs that can't run any more (removed, in
    /// conflict), `seen` being those that can, and counts busy, quiet and held DAGs.
    pub fn round_done(&mut self, visited: &Visit, seen: &HashSet<DagId>) {
        if let Some(pending) = self.pending.take() {
            self.last_round = Some(pending.round_at);
            self.checked_at = pending.checked_at;
            self.sweep_credit = pending.sweep_credit;
        }
        match visited {
            Visit::All => {
                self.dags.retain(|dag, _| seen.contains(dag));
                self.held.retain(|dag| seen.contains(dag));
            }
            Visit::Some(dags) => {
                for dag in dags.iter().filter(|dag| !seen.contains(*dag)) {
                    self.dags.remove(dag);
                    self.held.remove(dag);
                }
            }
        }
        let quiet = self
            .dags
            .values()
            .filter(|entry| matches!(entry.state, State::Quiet { .. }))
            .count();
        let busy = if self.settings.enabled {
            self.dags.len() - quiet
        } else {
            seen.len()
        };
        self.metrics.set_dags(busy, quiet, self.held.len());
    }

    /// The quiet state of a DAG with nothing in progress, unless a fire time is within
    /// `quiet_dag_wake_ahead` (then it stays busy): woken that long before its next fire
    /// time, or only by a mark (or the sweep) when paused or without one.
    fn quiet(&self, found: Visited<'_>, now: Timestamp) -> Option<State> {
        let ahead = signed(self.settings.wake_ahead);
        let fire_far = found
            .next_fire
            .is_none_or(|fire| fire.duration_since(now) > ahead);
        (found.paused || fire_far).then(|| State::Quiet {
            wake_at: if found.paused {
                None
            } else {
                found
                    .next_fire
                    .and_then(|fire| fire.checked_sub(ahead).ok())
            },
        })
    }
}

/// How many of `quiet` DAGs a round `since` the last one sweeps, so that all are swept
/// within `interval`, and the credit carried to the next round (in DAG-milliseconds): the
/// round adds `quiet` × `since` to `credit` and takes one DAG per `interval` of it, exactly
/// (600 DAGs, 1 s rounds, 60 s: 10 a round; one DAG: once in 60 rounds). None when the clock
/// stepped back; all of them once the interval has passed.
fn sweep_size(
    quiet: usize,
    credit: u128,
    since: SignedDuration,
    interval: SignedDuration,
) -> (usize, u128) {
    let millis = |d: SignedDuration| u128::try_from(d.as_millis()).unwrap_or(0);
    let (quiet_count, interval) = (quiet as u128, millis(interval));
    if quiet == 0 {
        return (0, 0);
    }
    if interval == 0 {
        return (quiet, 0);
    }
    let since = millis(since).min(interval);
    let credit = (credit + quiet_count * since).min(quiet_count * interval);
    let slice = usize::try_from(credit / interval)
        .unwrap_or(quiet)
        .min(quiet);
    (slice, credit % interval)
}

fn signed(duration: Duration) -> SignedDuration {
    SignedDuration::try_from(duration).unwrap_or(SignedDuration::MAX)
}
