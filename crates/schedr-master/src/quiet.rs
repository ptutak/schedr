//! Which DAGs a scheduler round visits (decision 485; first "Quiet DAGs", archive/PROJECT.md).
//!
//! A round visits the scheduled DAGs whose stored next fire time (`dag_triggers.next_fire_at`)
//! has come or isn't known, the DAGs with queued runs, and those a mark woke (a command, a run
//! queued or ended, a new version): the database says which, through the round's probe, and
//! nothing about a DAG is kept in memory to decide it. The rest are **quiet**: not visited.
//! A term's first round visits every DAG, and every round does with `[master] quiet_dags` off.
//! Visiting a DAG that didn't need it creates nothing (a scheduled run is unique per
//! `planned_for`), so a needless visit costs time, never runs.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use schedr_model::DagId;

use crate::metrics::MasterMetrics;

#[cfg(test)]
mod tests;

/// The `[master] quiet_dags` setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuietSettings {
    /// Whether rounds visit only the DAGs with something due; `false` visits every DAG every
    /// round.
    pub enabled: bool,
}

impl Default for QuietSettings {
    /// On.
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Why a DAG was visited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WakeReason {
    /// Its stored next fire time has come.
    Timer,
    /// Its next fire time isn't known yet (a new version, a switch changed).
    Unknown,
    /// A command about it (trigger, pause, unpause).
    Command,
    /// A run of it is queued (manual, chained, event, backfill).
    RunQueued,
    /// A run of it ended (a queued one may take its place).
    RunEnded,
    /// A new version of it, or it came back.
    NewVersion,
}

impl WakeReason {
    /// The name in metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timer => "timer",
            Self::Unknown => "unknown",
            Self::Command => "command",
            Self::RunQueued => "run_queued",
            Self::RunEnded => "run_ended",
            Self::NewVersion => "new_version",
        }
    }
}

/// The marks from what changes a DAG, taken by the next round (shared by clones).
#[derive(Debug, Clone, Default)]
pub struct Wakes(Arc<Mutex<HashMap<DagId, WakeReason>>>);

impl Wakes {
    /// Marks `dag` for the next round (the first reason kept).
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

/// What a visit found of a DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Visited<'a> {
    /// Why it is held, when its file failed (decision 72): [`schedr_db::runs::Held::reason`].
    pub held: Option<&'a str>,
}

/// The DAGs a round visits, and what is logged and counted of them.
#[derive(Debug)]
pub struct QuietDags {
    settings: QuietSettings,
    /// The DAGs that can run, as last seen: for the count of quiet ones.
    known: HashSet<DagId>,
    /// The DAGs held now (decision 72), to log a hold when it starts and when it lifts.
    held: HashSet<DagId>,
    /// Why each DAG of the current round was visited.
    woken: HashMap<DagId, WakeReason>,
    /// Whether this term's first round (every DAG) is done.
    first_done: bool,
    /// The current round's marks: put back if it fails.
    marks: Option<HashMap<DagId, WakeReason>>,
    wakes: Wakes,
    metrics: MasterMetrics,
}

impl QuietDags {
    /// No round done yet: the first visits every DAG.
    #[must_use]
    pub fn new(settings: QuietSettings, wakes: Wakes) -> Self {
        Self {
            settings,
            known: HashSet::new(),
            held: HashSet::new(),
            woken: HashMap::new(),
            first_done: false,
            marks: None,
            wakes,
            metrics: MasterMetrics::default(),
        }
    }

    /// Counts busy and quiet DAGs, and wakes, in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.metrics = metrics;
    }

    /// Whether rounds visit only the DAGs with something due (`[master] quiet_dags`).
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.settings.enabled
    }

    /// What the round visits: every DAG when off or in a term's first round; else `due` (the
    /// DAGs the database found due, with why) and the marked ones.
    pub fn round(&mut self, due: Vec<(DagId, WakeReason)>) -> Visit {
        let marks = self.wakes.take();
        self.woken.clear();
        if !self.settings.enabled || !self.first_done {
            self.marks = Some(marks);
            return Visit::All;
        }
        for (dag, reason) in due {
            self.woken.entry(dag).or_insert(reason);
        }
        for (dag, reason) in &marks {
            self.woken.entry(dag.clone()).or_insert(*reason);
        }
        self.marks = Some(marks);
        let mut visit: Vec<DagId> = self.woken.keys().cloned().collect();
        visit.sort();
        Visit::Some(visit)
    }

    /// The round failed (the database): its marks are put back, so the next round is as this
    /// one would have been (a term's first round again visits every DAG).
    pub fn round_failed(&mut self) {
        if let Some(marks) = self.marks.take() {
            for (dag, reason) in marks {
                self.wakes.wake(&dag, reason);
            }
        }
        self.woken.clear();
    }

    /// Notes what the round found of `dag`.
    pub fn visited(&mut self, dag: &DagId, found: Visited<'_>) {
        self.note_held(dag, found.held);
        self.known.insert(dag.clone());
        if let Some(reason) = self.woken.get(dag) {
            self.metrics.dag_woken(reason.as_str());
        }
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
    /// conflict), `seen` being those that can, and counts the visited (busy), the others
    /// (quiet) and the held DAGs.
    pub fn round_done(&mut self, visited: &Visit, seen: &HashSet<DagId>) {
        self.marks = None;
        self.first_done = true;
        let busy = match visited {
            Visit::All => {
                self.known.retain(|dag| seen.contains(dag));
                self.held.retain(|dag| seen.contains(dag));
                seen.len()
            }
            Visit::Some(dags) => {
                for dag in dags.iter().filter(|dag| !seen.contains(*dag)) {
                    self.known.remove(dag);
                    self.held.remove(dag);
                }
                seen.len()
            }
        };
        let quiet = if self.settings.enabled {
            self.known.len().saturating_sub(busy)
        } else {
            0
        };
        self.metrics.set_dags(busy, quiet, self.held.len());
    }
}
