//! The workers the master knows, their queues and slots, and the choice of a worker for a
//! task (see PROJECT.md, Task assignment).
//!
//! **Spread:** among the active workers serving the task's queue with a free slot, the one
//! with the most free slots; ties go to the least recently assigned, then the worker ID. A
//! worker with no queues configured serves the `default` queue. Slots count the attempts
//! assigned to the worker and not yet finished.
//!
//! **Eligible** workers also have the task's root at its current hash (only the newest code
//! runs) and a schedr version at least that of the parse the task's DAG version came from
//! (see PROJECT.md, Code versions and change detection, and Upgrades and compatibility).

use std::collections::{BTreeMap, BTreeSet};

use jiff::Timestamp;
use schedr_model::SchedrVersion;

#[cfg(test)]
mod tests;

/// The queue of tasks with no queue, served by workers with no queues configured.
pub const DEFAULT_QUEUE: &str = "default";

/// A worker's state as the master sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Takes new tasks.
    Active,
    /// Finishing its tasks; takes no new ones.
    Draining,
}

/// A registered worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    /// Its ID.
    pub id: String,
    /// The incarnation of its agent process.
    pub incarnation: String,
    /// The queues it serves.
    pub queues: BTreeSet<String>,
    /// Its task slots.
    pub slots: u32,
    /// Its state.
    pub state: WorkerState,
    /// The attempts assigned to it and not finished.
    pub attempts: BTreeSet<String>,
    /// When it was last given a task.
    pub last_assigned: Option<Timestamp>,
    /// Its schedr version, when known.
    pub version: Option<SchedrVersion>,
    /// The content hash it last reported for each root.
    pub roots: BTreeMap<String, String>,
    /// Drained for maintenance (`schedr workers drain`): no new tasks until resumed.
    pub drained: bool,
}

/// What a task needs of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Needs<'a> {
    /// Its queue.
    pub queue: &'a str,
    /// Its root and the hashes the worker must have it at (the root's current code, or newer
    /// code whose parse is on its way), when known.
    pub code: Option<(&'a str, &'a BTreeSet<String>)>,
    /// The lowest schedr version that runs it, when known.
    pub version: Option<SchedrVersion>,
}

impl<'a> Needs<'a> {
    /// Only a worker serving `queue`.
    #[must_use]
    pub const fn queue(queue: &'a str) -> Self {
        Self {
            queue,
            code: None,
            version: None,
        }
    }
}

impl Worker {
    /// Whether its schedr version runs a task of a parse of `version` (an unknown version on
    /// either side does).
    #[must_use]
    pub fn runs(&self, version: Option<SchedrVersion>) -> bool {
        match (self.version, version) {
            (Some(mine), Some(needed)) => mine >= needed,
            _ => true,
        }
    }

    /// Whether it has `root` at one of `hashes` (anything, when no code is asked for).
    #[must_use]
    pub fn has_code(&self, code: Option<(&str, &BTreeSet<String>)>) -> bool {
        code.is_none_or(|(root, hashes)| self.roots.get(root).is_some_and(|h| hashes.contains(h)))
    }

    /// Whether it serves `queue`.
    #[must_use]
    pub fn serves(&self, queue: &str) -> bool {
        self.queues.contains(queue)
    }

    /// Its free slots.
    #[must_use]
    pub fn free_slots(&self) -> u32 {
        self.slots
            .saturating_sub(u32::try_from(self.attempts.len()).unwrap_or(u32::MAX))
    }
}

/// The registered workers.
#[derive(Debug, Clone, Default)]
pub struct Workers {
    workers: BTreeMap<String, Worker>,
}

impl Workers {
    /// No workers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a worker (or its new incarnation, which starts with no attempts). Returns
    /// whether the set of workers changed (a new worker or a restart), which gives
    /// `unschedulable` tasks another chance.
    pub fn register(&mut self, id: &str, incarnation: &str, queues: &[String], slots: u32) -> bool {
        let queues: BTreeSet<String> = if queues.is_empty() {
            BTreeSet::from([DEFAULT_QUEUE.to_owned()])
        } else {
            queues.iter().cloned().collect()
        };
        if let Some(known) = self.workers.get_mut(id)
            && known.incarnation == incarnation
        {
            known.queues = queues;
            known.slots = slots;
            known.state = WorkerState::Active;
            // It reports its roots again right after.
            known.roots.clear();
            return false;
        }
        self.workers.insert(
            id.to_owned(),
            Worker {
                id: id.to_owned(),
                incarnation: incarnation.to_owned(),
                queues,
                slots,
                state: WorkerState::Active,
                attempts: BTreeSet::new(),
                last_assigned: None,
                version: None,
                roots: BTreeMap::new(),
                drained: false,
            },
        );
        true
    }

    /// Drains a worker for maintenance, or resumes it.
    pub fn set_drained(&mut self, id: &str, drained: bool) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.drained = drained;
        }
    }

    /// Records a worker's schedr version.
    pub fn set_version(&mut self, id: &str, version: Option<SchedrVersion>) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.version = version;
        }
    }

    /// Records the content hash a worker reported for a root.
    pub fn report_root(&mut self, id: &str, root: &str, hash: &str) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.roots.insert(root.to_owned(), hash.to_owned());
        }
    }

    /// The worker stops taking new tasks.
    pub fn draining(&mut self, id: &str) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.state = WorkerState::Draining;
        }
    }

    /// Forgets a worker (deregistered or lost); returns it.
    pub fn remove(&mut self, id: &str) -> Option<Worker> {
        self.workers.remove(id)
    }

    /// A worker by ID.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&Worker> {
        self.workers.get(id)
    }

    /// The active workers serving `queue` (not draining, not drained).
    pub fn serving<'a>(&'a self, queue: &'a str) -> impl Iterator<Item = &'a Worker> + 'a {
        self.workers
            .values()
            .filter(move |w| w.state == WorkerState::Active && !w.drained && w.serves(queue))
    }

    /// Whether an active worker drained for maintenance serves `queue`.
    #[must_use]
    pub fn drained_serving(&self, queue: &str) -> bool {
        self.workers
            .values()
            .any(|w| w.state == WorkerState::Active && w.drained && w.serves(queue))
    }

    /// The active workers that can run a task with `needs`: serving its queue, with its code
    /// and a recent enough version.
    pub fn eligible<'a>(&'a self, needs: Needs<'a>) -> impl Iterator<Item = &'a Worker> + 'a {
        self.serving(needs.queue)
            .filter(move |w| w.runs(needs.version) && w.has_code(needs.code))
    }

    /// The worker to give a task with `needs` to, leaving out `excluded`; `None` when no
    /// eligible worker has a free slot.
    #[must_use]
    pub fn pick<'a>(&'a self, needs: Needs<'a>, excluded: &BTreeSet<String>) -> Option<&'a str> {
        self.eligible(needs)
            .filter(|w| w.free_slots() > 0 && !excluded.contains(&w.id))
            .min_by(|a, b| {
                b.free_slots()
                    .cmp(&a.free_slots())
                    .then(a.last_assigned.cmp(&b.last_assigned))
                    .then(a.id.cmp(&b.id))
            })
            .map(|w| w.id.as_str())
    }

    /// Takes a slot of `worker` for `attempt`.
    pub fn reserve(&mut self, worker: &str, attempt: &str, now: Timestamp) {
        if let Some(w) = self.workers.get_mut(worker) {
            w.attempts.insert(attempt.to_owned());
            w.last_assigned = Some(now);
        }
    }

    /// Frees the slot of `attempt`, wherever it is.
    pub fn release(&mut self, attempt: &str) {
        for worker in self.workers.values_mut() {
            worker.attempts.remove(attempt);
        }
    }
}
