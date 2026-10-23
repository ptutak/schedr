//! The master's metrics (see PROJECT.md, Observability), registered in the process's registry
//! and served on the ops port.
//!
//! Counters and histograms are recorded where things happen; the gauges taken from the
//! database (runs, tasks, workers by state) are refreshed by the master's loop every
//! [`GAUGE_REFRESH`], so a scrape never touches the database or the master's lock. Labels are
//! bounded: states, kinds, reasons and `dag_id`, never a run, task or attempt ID.

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use schedr_db::reads::StateCounts;

#[cfg(test)]
mod tests;

/// How often the gauges taken from the database are refreshed.
pub const GAUGE_REFRESH: Duration = Duration::from_secs(15);

/// A state (of runs, tasks or workers) and, for runs, their kind.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StateLabels {
    /// The state.
    pub state: String,
}

/// Runs by state and kind.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RunLabels {
    /// The run's state.
    pub state: String,
    /// The run's kind.
    pub kind: String,
}

/// Finished runs by DAG and result.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResultLabels {
    /// The DAG.
    pub dag_id: String,
    /// The run's result.
    pub result: String,
}

/// A reason (a rejection's, a waiting task's).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReasonLabels {
    /// The reason.
    pub reason: String,
}

/// An attempt's status.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabels {
    /// The status.
    pub status: String,
}

/// A command's kind and the master's decision.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CommandLabels {
    /// The command's kind.
    pub kind: String,
    /// `applied` or `rejected`.
    pub decision: String,
}

/// The master's metrics (cheap to clone; clones share them).
#[derive(Debug, Clone)]
pub struct MasterMetrics {
    /// How long each tick of the loop took.
    pub tick_duration: Histogram,
    /// Runs by state and kind.
    pub runs: Family<RunLabels, Gauge>,
    /// Runs finished, by DAG and result.
    pub runs_finished: Family<ResultLabels, Counter>,
    /// Task instances of unfinished runs, by state.
    pub tasks: Family<StateLabels, Gauge>,
    /// Ready tasks no worker could take at the last assignment pass, by reason.
    pub tasks_waiting: Family<ReasonLabels, Gauge>,
    /// Attempts assigned to workers.
    pub assignments: Counter,
    /// Assignments workers rejected, by reason.
    pub rejections: Family<ReasonLabels, Counter>,
    /// Attempts ended, by status.
    pub attempts_ended: Family<StatusLabels, Counter>,
    /// Parse jobs created.
    pub parse_jobs_started: Counter,
    /// Parse jobs finished (their parse stored).
    pub parse_jobs_finished: Counter,
    /// How long parse jobs took, from creation to the stored parse.
    pub parse_duration: Histogram,
    /// Commands decided, by kind and decision.
    pub commands: Family<CommandLabels, Counter>,
    /// How long commands waited for their decision.
    pub command_latency: Histogram,
    /// Workers in the registry, by state.
    pub workers: Family<StateLabels, Gauge>,
    /// The label sets the gauges have had: one that is gone reads 0 rather than vanishing.
    seen: Arc<Mutex<Seen>>,
}

#[derive(Debug, Default)]
struct Seen {
    runs: HashSet<RunLabels>,
    tasks: HashSet<StateLabels>,
    workers: HashSet<StateLabels>,
    waiting: HashSet<ReasonLabels>,
}

impl Default for MasterMetrics {
    fn default() -> Self {
        Self {
            // 1 ms to about 16 s.
            tick_duration: Histogram::new(exponential_buckets(0.001, 2.0, 15)),
            runs: Family::default(),
            runs_finished: Family::default(),
            tasks: Family::default(),
            tasks_waiting: Family::default(),
            assignments: Counter::default(),
            rejections: Family::default(),
            attempts_ended: Family::default(),
            parse_jobs_started: Counter::default(),
            parse_jobs_finished: Counter::default(),
            // 0.5 s to about 1 h.
            parse_duration: Histogram::new(exponential_buckets(0.5, 2.0, 13)),
            commands: Family::default(),
            // 10 ms to about 2 min.
            command_latency: Histogram::new(exponential_buckets(0.01, 2.0, 14)),
            workers: Family::default(),
            seen: Arc::default(),
        }
    }
}

impl MasterMetrics {
    /// Registers every metric in `registry` (names get the `schedr_` prefix there).
    pub fn register(&self, registry: &mut Registry) {
        let r = registry;
        r.register(
            "master_tick_duration_seconds",
            "How long each tick of the master's loop took",
            self.tick_duration.clone(),
        );
        r.register("runs", "Runs by state and kind", self.runs.clone());
        r.register(
            "runs_finished",
            "Runs finished, by DAG and result",
            self.runs_finished.clone(),
        );
        r.register(
            "tasks",
            "Task instances of unfinished runs, by state",
            self.tasks.clone(),
        );
        r.register(
            "tasks_waiting",
            "Ready tasks no worker could take at the last assignment pass, by reason",
            self.tasks_waiting.clone(),
        );
        r.register(
            "assignments",
            "Attempts assigned to workers",
            self.assignments.clone(),
        );
        r.register(
            "rejections",
            "Assignments workers rejected, by reason",
            self.rejections.clone(),
        );
        r.register(
            "attempts_ended",
            "Attempts ended, by status",
            self.attempts_ended.clone(),
        );
        r.register(
            "parse_jobs_started",
            "Parse jobs created",
            self.parse_jobs_started.clone(),
        );
        r.register(
            "parse_jobs_finished",
            "Parse jobs finished",
            self.parse_jobs_finished.clone(),
        );
        r.register(
            "parse_duration_seconds",
            "How long parse jobs took, from creation to the stored parse",
            self.parse_duration.clone(),
        );
        r.register(
            "commands",
            "Commands decided, by kind and decision",
            self.commands.clone(),
        );
        r.register(
            "command_latency_seconds",
            "How long commands waited for the master's decision",
            self.command_latency.clone(),
        );
        r.register(
            "workers",
            "Workers in the registry, by state",
            self.workers.clone(),
        );
    }

    /// Sets the gauges taken from the database.
    pub fn set_counts(&self, counts: &StateCounts) {
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let runs = counts.runs.iter().map(|(state, kind, count)| {
            let labels = RunLabels {
                state: state.clone(),
                kind: kind.clone(),
            };
            (labels, *count)
        });
        set_gauges(&self.runs, &mut seen.runs, runs);
        set_gauges(&self.tasks, &mut seen.tasks, states(&counts.tasks));
        set_gauges(&self.workers, &mut seen.workers, states(&counts.workers));
    }

    /// Sets the waiting tasks found by an assignment pass, by reason.
    pub fn set_waiting(&self, waiting: &[(&'static str, i64)]) {
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let waiting = waiting.iter().map(|(reason, count)| {
            let labels = ReasonLabels {
                reason: (*reason).to_owned(),
            };
            (labels, *count)
        });
        set_gauges(&self.tasks_waiting, &mut seen.waiting, waiting);
    }

    /// Counts one ended attempt.
    pub fn attempt_ended(&self, status: &impl ToString) {
        self.attempts_ended
            .get_or_create(&StatusLabels {
                status: status.to_string(),
            })
            .inc();
    }

    /// Counts one finished run.
    pub fn run_finished(&self, dag_id: &str, result: &impl ToString) {
        self.runs_finished
            .get_or_create(&ResultLabels {
                dag_id: dag_id.to_owned(),
                result: result.to_string(),
            })
            .inc();
    }
}

fn states(counts: &[(String, i64)]) -> impl Iterator<Item = (StateLabels, i64)> + '_ {
    counts.iter().map(|(state, count)| {
        let labels = StateLabels {
            state: state.clone(),
        };
        (labels, *count)
    })
}

/// Sets each gauge of `counts`, and those `seen` before but not counted now to 0 (so a state
/// that empties reads 0, and a scrape meanwhile never misses a series).
fn set_gauges<L: Clone + Hash + Eq + EncodeLabelSet + std::fmt::Debug + Send + Sync + 'static>(
    family: &Family<L, Gauge>,
    seen: &mut HashSet<L>,
    counts: impl Iterator<Item = (L, i64)>,
) {
    let counts: Vec<(L, i64)> = counts.collect();
    for labels in seen.iter() {
        if !counts.iter().any(|(counted, _)| counted == labels) {
            family.get_or_create(labels).set(0);
        }
    }
    for (labels, count) in counts {
        family.get_or_create(&labels).set(count);
        seen.insert(labels);
    }
}
