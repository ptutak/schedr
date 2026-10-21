//! The workers the master knows, their queues and slots, and the choice of a worker for a
//! task (see PROJECT.md, Task assignment).
//!
//! **Spread:** among the active workers serving the task's queue with a free slot, the one
//! with the most free slots; ties go to the least recently assigned, then the worker ID. A
//! worker with no queues configured serves the `default` queue. Slots count the attempts
//! assigned to the worker and not yet finished.

use std::collections::{BTreeMap, BTreeSet};

use jiff::Timestamp;

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
}

impl Worker {
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
            },
        );
        true
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

    /// The active workers serving `queue`.
    pub fn serving<'a>(&'a self, queue: &'a str) -> impl Iterator<Item = &'a Worker> + 'a {
        self.workers
            .values()
            .filter(move |w| w.state == WorkerState::Active && w.serves(queue))
    }

    /// The worker to give a task of `queue` to, leaving out `excluded`; `None` when no
    /// eligible worker has a free slot.
    #[must_use]
    pub fn pick<'a>(&'a self, queue: &'a str, excluded: &BTreeSet<String>) -> Option<&'a str> {
        self.serving(queue)
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
