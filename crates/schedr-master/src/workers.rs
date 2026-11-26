//! The workers the master knows, their queues and slots, and the choice of a worker for a
//! task (see archive/PROJECT.md, Task assignment).
//!
//! **Spread:** among the active workers serving the task's queue with a free slot, the one
//! with the most free slots; ties go to the least recently assigned, then the worker ID. A
//! worker with no queues configured serves the `default` queue. Slots count the attempts
//! assigned to the worker and not yet finished.
//!
//! **Eligible** workers also have the task's root at its current hash (only the newest code
//! runs) and a schedr version at least that of the parse the task's DAG version came from
//! (see archive/PROJECT.md, Code versions and change detection, and Upgrades and compatibility).

use std::collections::{BTreeMap, BTreeSet};

use jiff::Timestamp;
use schedr_model::SchedrVersion;

#[cfg(test)]
mod tests;

/// The queue of tasks with no queue, served by workers with no queues configured.
pub const DEFAULT_QUEUE: &str = "default";

/// A worker's watcher slots until it says (`[watchers] slots`).
pub const DEFAULT_WATCHER_SLOTS: u32 = 500;

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
    /// How many assigned tasks past `slots` it keeps waiting (its queue, decision 509).
    pub queue_depth: u32,
    /// Its slots for wait steps.
    pub watcher_slots: u32,
    /// The extensions it offers, each with its versions (`Register.extensions`, decision 527):
    /// by name, so a check allocates nothing (agy's review of M5.3d: looked up per worker per
    /// waiting task, each round).
    pub extensions: BTreeMap<String, BTreeSet<String>>,
    /// Its state.
    pub state: WorkerState,
    /// The attempts assigned to it and not finished, on its task slots.
    pub attempts: BTreeSet<String>,
    /// The wait steps assigned to it and not finished, on its watcher slots.
    pub wait_steps: BTreeSet<String>,
    /// Those of them that are pokes: a drain moves them at once.
    pub pokes: BTreeSet<String>,
    /// When it was last given a task.
    pub last_assigned: Option<Timestamp>,
    /// Its schedr version, when known.
    pub version: Option<SchedrVersion>,
    /// The content hash it last reported for each root.
    pub roots: BTreeMap<String, String>,
    /// Drained for maintenance (`schedr workers drain`): no new tasks until resumed.
    pub drained: bool,
    /// Whether its stream is up: a connections job goes only to a worker that is.
    pub connected: bool,
    /// How many times each of its extensions' services has become ready, as its last
    /// heartbeat said (DN37): its state, a lost heartbeat losing nothing.
    pub service_readies: BTreeMap<String, u64>,
    /// Each root's content hash it last reported, kept over a reconnect (whose `roots` start
    /// empty): new code is a hash other than this one (B285).
    pub code_seen: BTreeMap<String, String>,
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
    /// An extension task's extension and version: only a worker offering it (decision 527).
    pub extension: Option<(&'a str, &'a str)>,
    /// A wait step: it takes a watcher slot.
    pub wait_step: bool,
    /// It may wait in a worker's queue past its slots (decision 509): an ordinary task's
    /// assignment, not one given back from a queue.
    pub may_queue: bool,
}

impl<'a> Needs<'a> {
    /// Only a worker serving `queue`.
    #[must_use]
    pub const fn queue(queue: &'a str) -> Self {
        Self {
            queue,
            code: None,
            version: None,
            extension: None,
            wait_step: false,
            may_queue: false,
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

    /// Whether it offers `extension` at its version (anything, when none is asked for).
    #[must_use]
    pub fn offers(&self, extension: Option<(&str, &str)>) -> bool {
        extension.is_none_or(|(name, version)| {
            self.extensions
                .get(name)
                .is_some_and(|versions| versions.contains(version))
        })
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

    /// Its free watcher slots.
    #[must_use]
    pub fn free_watcher_slots(&self) -> u32 {
        self.watcher_slots
            .saturating_sub(u32::try_from(self.wait_steps.len()).unwrap_or(u32::MAX))
    }

    fn free_for(&self, needs: Needs<'_>) -> u32 {
        if needs.wait_step {
            self.free_watcher_slots()
        } else if needs.may_queue {
            // Its free slots and its queue's room (decision 509).
            self.slots
                .saturating_add(self.queue_depth)
                .saturating_sub(u32::try_from(self.attempts.len()).unwrap_or(u32::MAX))
        } else {
            self.free_slots()
        }
    }

    /// Its free slots for `needs`, not counting its queue: what [`Workers::pick`] fills first.
    fn free_now(&self, needs: Needs<'_>) -> u32 {
        if needs.wait_step {
            self.free_watcher_slots()
        } else {
            self.free_slots()
        }
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

    /// Whether `id` is registered at `incarnation` already.
    #[must_use]
    pub fn known_as(&self, id: &str, incarnation: &str) -> bool {
        self.workers
            .get(id)
            .is_some_and(|worker| worker.incarnation == incarnation)
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
            known.connected = true;
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
                queue_depth: 0,
                watcher_slots: DEFAULT_WATCHER_SLOTS,
                extensions: BTreeMap::new(),
                state: WorkerState::Active,
                attempts: BTreeSet::new(),
                wait_steps: BTreeSet::new(),
                pokes: BTreeSet::new(),
                last_assigned: None,
                version: None,
                roots: BTreeMap::new(),
                drained: false,
                connected: true,
                service_readies: BTreeMap::new(),
                code_seen: BTreeMap::new(),
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

    /// A worker's stream ended (it may come back, see [`Self::register`]).
    pub fn disconnected(&mut self, id: &str) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.connected = false;
        }
    }

    /// Records a worker's schedr version.
    pub fn set_version(&mut self, id: &str, version: Option<SchedrVersion>) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.version = version;
        }
    }

    /// Records the content hash a worker reported for a root: whether it is new code for it,
    /// a hash other than the one it last reported, a reconnect's report of the same not (B285:
    /// its "code missing" refusals no longer hold).
    pub fn report_root(&mut self, id: &str, root: &str, hash: &str) -> bool {
        self.workers.get_mut(id).is_some_and(|worker| {
            worker.roots.insert(root.to_owned(), hash.to_owned());
            worker
                .code_seen
                .insert(root.to_owned(), hash.to_owned())
                .as_deref()
                != Some(hash)
        })
    }

    /// Records how many times each of a worker's services has become ready, as its heartbeat
    /// said (DN37): only those, so the map stays within one heartbeat's limits (agy's review).
    /// A count never goes back within an incarnation: a lower one (a heartbeat handled after a
    /// newer one) keeps the higher. Whether any count rose.
    pub fn report_services(&mut self, id: &str, readies: BTreeMap<String, u64>) -> bool {
        let Some(worker) = self.workers.get_mut(id) else {
            return false;
        };
        let mut rose = false;
        let known = std::mem::take(&mut worker.service_readies);
        worker.service_readies = readies
            .into_iter()
            .map(|(extension, count)| {
                let before = known.get(&extension).copied().unwrap_or(0);
                rose |= count > before;
                (extension, count.max(before))
            })
            .collect();
        rose
    }

    /// Whether `id`'s service of `extension` has become ready since a refusal that saw it ready
    /// `refused` times (B284): told by a count past it, whatever order the two were handled in.
    #[must_use]
    pub fn service_ready_since(&self, id: &str, extension: &str, refused: u64) -> bool {
        self.workers.get(id).is_some_and(|worker| {
            worker
                .service_readies
                .get(extension)
                .is_some_and(|count| *count > refused)
        })
    }

    /// A worker withdrew a root (decision 303): it no longer has it.
    pub fn withdraw_root(&mut self, id: &str, root: &str) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.roots.remove(root);
            worker.code_seen.remove(root);
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

    /// Whether an active worker drained for maintenance serves `queue` offering `extension`.
    #[must_use]
    pub fn drained_offering(&self, queue: &str, extension: Option<(&str, &str)>) -> bool {
        self.workers.values().any(|w| {
            w.state == WorkerState::Active && w.drained && w.serves(queue) && w.offers(extension)
        })
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
        self.serving(needs.queue).filter(move |w| {
            w.runs(needs.version) && w.has_code(needs.code) && w.offers(needs.extension)
        })
    }

    /// The worker to give a task with `needs` to, leaving out `excluded`; `None` when no
    /// eligible worker has a free slot. Only a connected one: an assignment to a worker without
    /// a stream would be dropped and wait for its return or loss (Grok's review of B240); one
    /// disconnected still counts as eligible, so the task waits rather than being unschedulable.
    #[must_use]
    pub fn pick<'a>(&'a self, needs: Needs<'a>, excluded: &BTreeSet<String>) -> Option<&'a str> {
        self.eligible(needs)
            .filter(|w| w.connected && w.free_for(needs) > 0 && !excluded.contains(&w.id))
            .min_by(|a, b| {
                // Free slots first; then, for a task that may queue, the most room left in a
                // queue (decision 509).
                b.free_now(needs)
                    .cmp(&a.free_now(needs))
                    .then(b.free_for(needs).cmp(&a.free_for(needs)))
                    .then(a.last_assigned.cmp(&b.last_assigned))
                    .then(a.id.cmp(&b.id))
            })
            .map(|w| w.id.as_str())
    }

    /// `id` offers `extensions` (its registration's), all of them in place of what it offered.
    pub fn set_extensions(&mut self, id: &str, extensions: BTreeSet<(String, String)>) {
        if let Some(worker) = self.workers.get_mut(id) {
            let mut by_name: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for (name, version) in extensions {
                by_name.entry(name).or_default().insert(version);
            }
            worker.extensions = by_name;
        }
    }

    /// Every registered worker.
    pub fn all(&self) -> impl Iterator<Item = &Worker> {
        self.workers.values()
    }

    /// Takes a slot of `worker` for `attempt`.
    pub fn reserve(&mut self, worker: &str, attempt: &str, now: Timestamp) {
        if let Some(w) = self.workers.get_mut(worker) {
            w.attempts.insert(attempt.to_owned());
            w.last_assigned = Some(now);
        }
    }

    /// Takes a watcher slot of `worker` for `attempt`.
    pub fn reserve_wait_step(&mut self, worker: &str, attempt: &str, poke: bool, now: Timestamp) {
        if let Some(w) = self.workers.get_mut(worker) {
            w.wait_steps.insert(attempt.to_owned());
            if poke {
                w.pokes.insert(attempt.to_owned());
            }
            w.last_assigned = Some(now);
        }
    }

    /// Sets how many tasks past its slots a worker keeps waiting (decision 509).
    pub fn set_queue_depth(&mut self, id: &str, depth: u32) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.queue_depth = depth;
        }
    }

    /// Sets a worker's watcher slots.
    pub fn set_watcher_slots(&mut self, id: &str, slots: u32) {
        if let Some(worker) = self.workers.get_mut(id) {
            worker.watcher_slots = slots;
        }
    }

    /// Whether a worker other than `worker` holds `attempt` (a task's slot, a wait step's or a
    /// trigger's).
    #[must_use]
    pub fn held_by_another(&self, worker: &str, attempt: &str) -> bool {
        self.workers.values().any(|w| {
            w.id != worker
                && (w.attempts.contains(attempt)
                    || w.wait_steps.contains(attempt)
                    || w.pokes.contains(attempt))
        })
    }

    /// Frees the slot of `attempt`, wherever it is.
    pub fn release(&mut self, attempt: &str) {
        for worker in self.workers.values_mut() {
            worker.attempts.remove(attempt);
            worker.wait_steps.remove(attempt);
            worker.pokes.remove(attempt);
        }
    }
}
