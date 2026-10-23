//! The worker agent's metrics (see PROJECT.md, Observability), registered in the process's
//! registry and served on the ops port. Each is updated where things happen; none costs
//! anything at a scrape.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

#[cfg(test)]
mod tests;

/// An attempt's status.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabels {
    /// The status.
    pub status: String,
}

/// The agent's metrics (cheap to clone; clones share them).
#[derive(Debug, Clone, Default)]
pub struct AgentMetrics {
    /// Task slots.
    pub slots: Gauge,
    /// Task slots in use.
    pub slots_used: Gauge,
    /// Attempts ended here, by status.
    pub attempts: Family<StatusLabels, Counter>,
    /// Tasks and parse runners the memory watchdog killed.
    pub watchdog_kills: Counter,
    /// Attempts in the journal (running, or results not acknowledged).
    pub journal_records: Gauge,
    /// Bytes of task and parse logs kept, as of the last retention sweep.
    pub log_bytes: Gauge,
    /// Bytes of logs deleted by retention.
    pub log_swept_bytes: Counter,
}

impl AgentMetrics {
    /// Registers every metric in `registry` (names get the `schedr_` prefix there).
    pub fn register(&self, registry: &mut Registry) {
        let r = registry;
        r.register("worker_slots", "Task slots", self.slots.clone());
        r.register(
            "worker_slots_used",
            "Task slots in use",
            self.slots_used.clone(),
        );
        r.register(
            "worker_attempts",
            "Attempts ended on this worker, by status",
            self.attempts.clone(),
        );
        r.register(
            "worker_watchdog_kills",
            "Tasks and parse runners killed over their memory budget",
            self.watchdog_kills.clone(),
        );
        r.register(
            "worker_journal_records",
            "Attempts in the journal (running, or results not acknowledged)",
            self.journal_records.clone(),
        );
        r.register(
            "worker_log_bytes",
            "Bytes of logs kept, as of the last retention sweep",
            self.log_bytes.clone(),
        );
        r.register(
            "worker_log_swept_bytes",
            "Bytes of logs deleted by retention",
            self.log_swept_bytes.clone(),
        );
    }

    /// Counts one ended attempt.
    pub fn attempt_ended(&self, status: &impl ToString) {
        self.attempts
            .get_or_create(&StatusLabels {
                status: status.to_string(),
            })
            .inc();
    }
}
