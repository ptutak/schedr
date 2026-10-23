//! The worker agent's metrics (see PROJECT.md, Observability), registered in the process's
//! registry and served on the ops port. Each is updated where things happen; none costs
//! anything at a scrape.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use schedr_model::metric_doc::{MetricDoc, MetricKind};

#[cfg(test)]
mod tests;

const SLOTS: MetricDoc = MetricDoc {
    name: "worker_slots",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Task slots",
};
const SLOTS_USED: MetricDoc = MetricDoc {
    name: "worker_slots_used",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Task slots in use",
};
const ATTEMPTS: MetricDoc = MetricDoc {
    name: "worker_attempts",
    kind: MetricKind::Counter,
    labels: &["status"],
    help: "Attempts ended on this worker, by status",
};
const WATCHDOG_KILLS: MetricDoc = MetricDoc {
    name: "worker_watchdog_kills",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Tasks and parse runners killed over their memory budget",
};
const JOURNAL_RECORDS: MetricDoc = MetricDoc {
    name: "worker_journal_records",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Attempts in the journal (running, or results not acknowledged)",
};
const LOG_BYTES: MetricDoc = MetricDoc {
    name: "worker_log_bytes",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Bytes of logs kept, as of the last retention sweep",
};
const LOG_SWEPT_BYTES: MetricDoc = MetricDoc {
    name: "worker_log_swept_bytes",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Bytes of logs deleted by retention",
};

/// The worker agent's metrics, as registered (under `schedr_`).
pub const METRICS: &[MetricDoc] = &[
    SLOTS,
    SLOTS_USED,
    ATTEMPTS,
    WATCHDOG_KILLS,
    JOURNAL_RECORDS,
    LOG_BYTES,
    LOG_SWEPT_BYTES,
];

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
    /// Registers every metric in `registry` (names get the `schedr_` prefix there), as
    /// [`METRICS`] describes them.
    pub fn register(&self, registry: &mut Registry) {
        let r = registry;
        r.register(SLOTS.name, SLOTS.help, self.slots.clone());
        r.register(SLOTS_USED.name, SLOTS_USED.help, self.slots_used.clone());
        r.register(ATTEMPTS.name, ATTEMPTS.help, self.attempts.clone());
        r.register(
            WATCHDOG_KILLS.name,
            WATCHDOG_KILLS.help,
            self.watchdog_kills.clone(),
        );
        r.register(
            JOURNAL_RECORDS.name,
            JOURNAL_RECORDS.help,
            self.journal_records.clone(),
        );
        r.register(LOG_BYTES.name, LOG_BYTES.help, self.log_bytes.clone());
        r.register(
            LOG_SWEPT_BYTES.name,
            LOG_SWEPT_BYTES.help,
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
