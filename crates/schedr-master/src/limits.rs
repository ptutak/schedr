//! Concurrency limits (see archive/PROJECT.md, Native API: Concurrency limits, decisions 87-91):
//! `max_active_tasks` on a DAG, `max_active` and `max_active_per_run` on a task.
//!
//! The master counts the open attempts (assigned and not yet started, running, cleaning up) in
//! memory, by DAG, by task and by a run's task (88A): read from the database once when it
//! starts dispatching (one master holds the lease, its writes fenced), then kept as it assigns
//! and as attempts end. Each instance of a mapped task counts (89). A task is checked as it
//! is assigned; one whose limit is reached stays `queued`, saying which limit and its value.
//!
//! The limits across runs (`max_active_tasks`, `max_active`) are the DAG's current version's,
//! `max_active_per_run` the run's own version's (87A).

use std::collections::BTreeMap;

use schedr_db::attempts::{ActiveAttempt, ReadyInstance};
use schedr_model::{Dag, DagId, TaskId};

#[cfg(test)]
mod tests;

/// The open attempts, counted for the limits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Active {
    /// Whether they were read from the database (until then nothing is counted).
    loaded: bool,
    /// Each open attempt and what it counts toward.
    attempts: BTreeMap<String, Holder>,
    /// Open attempts per DAG, and per task of it.
    dags: BTreeMap<DagId, Counted>,
    /// Open attempts per run, by task.
    runs: BTreeMap<String, BTreeMap<TaskId, u32>>,
}

/// What an open attempt counts toward.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Holder {
    dag: DagId,
    run: String,
    task: TaskId,
}

/// A DAG's open attempts: all of them, and by task.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Counted {
    all: u32,
    tasks: BTreeMap<TaskId, u32>,
}

/// A limit reached: which one and its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reached {
    /// The limit's name, also the waiting metric's reason (decision 91).
    pub name: &'static str,
    /// Its value.
    pub value: u32,
}

impl Reached {
    /// The reason shown with the waiting task: "waiting: `max_active_per_run` (10) reached".
    #[must_use]
    pub fn reason(&self) -> String {
        format!("waiting: `{}` ({}) reached", self.name, self.value)
    }
}

impl Active {
    /// Nothing counted, not read yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the open attempts were read (see [`Self::load`]).
    #[must_use]
    pub const fn is_loaded(&self) -> bool {
        self.loaded
    }

    /// Counts exactly `attempts` (the database's open attempts), replacing what was counted.
    pub fn load(&mut self, attempts: Vec<ActiveAttempt>) {
        *self = Self::default();
        for a in attempts {
            self.add(&a.attempt_id, a.dag_id, a.run_id, a.task_id);
        }
        self.loaded = true;
    }

    /// Counts an attempt just assigned (once: again changes nothing).
    pub fn add(&mut self, attempt_id: &str, dag_id: DagId, run_id: String, task_id: TaskId) {
        if self.attempts.contains_key(attempt_id) {
            return;
        }
        let dag = self.dags.entry(dag_id.clone()).or_default();
        dag.all += 1;
        *dag.tasks.entry(task_id.clone()).or_default() += 1;
        *self
            .runs
            .entry(run_id.clone())
            .or_default()
            .entry(task_id.clone())
            .or_default() += 1;
        self.attempts.insert(
            attempt_id.to_owned(),
            Holder {
                dag: dag_id,
                run: run_id,
                task: task_id,
            },
        );
    }

    /// Counts `attempt_id`, just assigned for `ready` (see [`Self::add`]).
    pub fn assigned(&mut self, attempt_id: &str, ready: &ReadyInstance) {
        let (dag, run, task) = (&ready.dag.id, &ready.run_id, &ready.task_id);
        self.add(attempt_id, dag.clone(), run.clone(), task.clone());
    }

    /// An attempt ended or was taken back: it no longer counts (one not counted: nothing).
    pub fn remove(&mut self, attempt_id: &str) {
        let Some(holder) = self.attempts.remove(attempt_id) else {
            return;
        };
        if let Some(dag) = self.dags.get_mut(&holder.dag) {
            dag.all = dag.all.saturating_sub(1);
            decrement(&mut dag.tasks, &holder.task);
            if dag.all == 0 {
                self.dags.remove(&holder.dag);
            }
        }
        if let Some(run) = self.runs.get_mut(&holder.run) {
            decrement(run, &holder.task);
            if run.is_empty() {
                self.runs.remove(&holder.run);
            }
        }
    }

    /// The DAG's open attempts.
    #[must_use]
    pub fn of_dag(&self, dag_id: &DagId) -> u32 {
        self.dags.get(dag_id).map_or(0, |d| d.all)
    }

    /// The task's open attempts across the DAG's runs.
    #[must_use]
    pub fn of_task(&self, dag_id: &DagId, task_id: &TaskId) -> u32 {
        self.dags
            .get(dag_id)
            .and_then(|d| d.tasks.get(task_id))
            .copied()
            .unwrap_or(0)
    }

    /// The task's open attempts within one run.
    #[must_use]
    pub fn of_run_task(&self, run_id: &str, task_id: &TaskId) -> u32 {
        self.runs
            .get(run_id)
            .and_then(|r| r.get(task_id))
            .copied()
            .unwrap_or(0)
    }

    /// The open attempts counted, by ID (for the fault-injection harness's check).
    #[cfg(test)]
    pub(crate) fn attempt_ids(&self) -> std::collections::BTreeSet<&str> {
        self.attempts.keys().map(String::as_str).collect()
    }

    /// The first limit that one more instance of `task_id` in run `run_id` would go over, if
    /// any: `max_active_tasks` and `max_active` from `current` (the DAG's current version),
    /// `max_active_per_run` from `own` (the run's). A task the current version no longer has
    /// keeps its own version's `max_active`.
    #[must_use]
    pub fn reached(
        &self,
        (current, own): (&Dag, &Dag),
        run_id: &str,
        task_id: &TaskId,
    ) -> Option<Reached> {
        let dag_id = &own.id;
        let reached = |name, limit: Option<u32>, count: u32| {
            limit
                .filter(|&value| count >= value)
                .map(|value| Reached { name, value })
        };
        // The DAG's cap first, without looking the task up: with it reached, every ready task
        // of the DAG is held back at once (Codex's review of M4.4: two searches of the DAG's
        // tasks for each of them, every round).
        if let Some(cap) = reached(
            "max_active_tasks",
            current.max_active_tasks,
            self.of_dag(dag_id),
        ) {
            return Some(cap);
        }
        let task_limit = current
            .task(task_id)
            .or_else(|| own.task(task_id))
            .and_then(|t| t.settings.max_active);
        if let Some(task) = reached("max_active", task_limit, self.of_task(dag_id, task_id)) {
            return Some(task);
        }
        let per_run = own
            .task(task_id)
            .and_then(|t| t.settings.max_active_per_run);
        reached(
            "max_active_per_run",
            per_run,
            self.of_run_task(run_id, task_id),
        )
    }
}

/// One fewer for `key`, gone at zero.
fn decrement(counts: &mut BTreeMap<TaskId, u32>, key: &TaskId) {
    if let Some(n) = counts.get_mut(key) {
        *n = n.saturating_sub(1);
        if *n == 0 {
            counts.remove(key);
        }
    }
}
