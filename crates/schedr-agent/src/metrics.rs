//! The worker agent's metrics (see PROJECT.md, Observability), registered in the process's
//! registry and served on the ops port. Each is updated where things happen; none costs
//! anything at a scrape.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use schedr_model::metric_doc::{MetricDoc, MetricKind};

use crate::attempt::StartTimes;

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
const TASK_START: MetricDoc = MetricDoc {
    name: "worker_task_start_seconds",
    kind: MetricKind::Histogram,
    labels: &["method", "phase"],
    help: "How long Python tasks took to start, by start method (spawn, fork) and phase \
        (runner: from starting the process to the runner's hello; import: from sending the \
        task to the DAG file imported, not counted when that fails)",
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
const FORK_SERVERS: MetricDoc = MetricDoc {
    name: "worker_fork_servers",
    kind: MetricKind::Gauge,
    labels: &["interpreter", "kind", "state"],
    help: "Fork servers by interpreter, kind (preloaded, bare) and state (up, down, disabled): \
        1 for each server's current state",
};
const FORK_FALLBACKS: MetricDoc = MetricDoc {
    name: "worker_fork_fallbacks",
    kind: MetricKind::Counter,
    labels: &["interpreter", "reason"],
    help: "Python tasks a fork server didn't fork, by interpreter and reason (disabled, down, \
        fork_failed, shim_died); the next fork server, or spawn, starts them",
};
const LOG_SWEPT_BYTES: MetricDoc = MetricDoc {
    name: "worker_log_swept_bytes",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Bytes of logs deleted by retention",
};
const WATCHER_SLOTS: MetricDoc = MetricDoc {
    name: "worker_watcher_slots",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Slots for wait steps",
};
const WATCHER_SLOTS_USED: MetricDoc = MetricDoc {
    name: "worker_watcher_slots_used",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Wait steps running",
};
const WATCHER_HOSTS: MetricDoc = MetricDoc {
    name: "worker_watcher_hosts",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Processes running wait steps, one per code set",
};
const WATCHER_THREADS: MetricDoc = MetricDoc {
    name: "worker_watcher_threads",
    kind: MetricKind::Gauge,
    labels: &["kind"],
    help: "Threads of the hosts, by kind (tied: running a wait step's `to_thread` calls or \
        started by it; untied: the rest of the process, a library's pools among them)",
};
const WATCHER_MEMORY: MetricDoc = MetricDoc {
    name: "worker_watcher_memory_bytes",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Memory the hosts use together, against `[watchers] memory_budget`",
};
const WATCHER_LOOP_LAG: MetricDoc = MetricDoc {
    name: "worker_watcher_loop_lag_seconds",
    kind: MetricKind::Histogram,
    labels: &[],
    help: "How long a host's event loop took to get to a call: long ones are wait steps \
        holding it up, short of `[watchers] block_limit`",
};
const WATCHER_BLOCKED_KILLS: MetricDoc = MetricDoc {
    name: "worker_watcher_blocked_kills",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Hosts killed for one wait step: it held the event loop, a thread of it stayed stuck, \
        or it ignored its cancel",
};
const WATCHER_RESTARTS: MetricDoc = MetricDoc {
    name: "worker_watcher_restarts",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Wait steps started again on a new host, as another one got theirs killed",
};
const WATCHER_HOSTS_LOST: MetricDoc = MetricDoc {
    name: "worker_watcher_hosts_lost",
    kind: MetricKind::Counter,
    labels: &["code"],
    help: "Hosts ended with no wait step to blame (a crash, a memory kill, silence), by the code \
        they ran: their wait steps use a retry",
};
const WATCHER_THREAD_STOPS: MetricDoc = MetricDoc {
    name: "worker_watcher_thread_stops",
    kind: MetricKind::Counter,
    labels: &[],
    help: "`to_thread` calls stopped at their wait step's end, each of which may have left \
        shared state half updated",
};

/// The worker agent's metrics, as registered (under `schedr_`).
pub const METRICS: &[MetricDoc] = &[
    SLOTS,
    SLOTS_USED,
    ATTEMPTS,
    TASK_START,
    WATCHDOG_KILLS,
    JOURNAL_RECORDS,
    LOG_BYTES,
    LOG_SWEPT_BYTES,
    FORK_SERVERS,
    FORK_FALLBACKS,
    WATCHER_SLOTS,
    WATCHER_SLOTS_USED,
    WATCHER_HOSTS,
    WATCHER_THREADS,
    WATCHER_MEMORY,
    WATCHER_LOOP_LAG,
    WATCHER_BLOCKED_KILLS,
    WATCHER_RESTARTS,
    WATCHER_HOSTS_LOST,
    WATCHER_THREAD_STOPS,
];

/// An attempt's status.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabels {
    /// The status.
    pub status: String,
}

/// How a task's process was started, and which part of its start.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StartLabels {
    /// `spawn` or `fork`.
    pub method: String,
    /// `runner` or `import`.
    pub phase: String,
}

/// A fork server and its state.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ForkServerLabels {
    /// The interpreter's name.
    pub interpreter: String,
    /// `preloaded` or `bare`.
    pub kind: String,
    /// `up`, `down` or `disabled`.
    pub state: String,
}

/// Why a fork server didn't fork a task.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ForkFallbackLabels {
    /// The interpreter's name.
    pub interpreter: String,
    /// `disabled`, `down`, `fork_failed` or `shim_died`.
    pub reason: String,
}

/// Which of a host's threads.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ThreadLabels {
    /// `tied` or `untied`.
    pub kind: String,
}

/// The code a host ran.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CodeLabels {
    /// Its folders' names, the copies' hashes.
    pub code: String,
}

/// The wait step hosts' metrics (cheap to clone; clones share them).
#[derive(Debug, Clone)]
pub struct WatcherMetrics {
    /// Slots for wait steps.
    pub slots: Gauge,
    /// Wait steps running.
    pub slots_used: Gauge,
    /// Hosts running.
    pub hosts: Gauge,
    /// Their threads, by kind.
    pub threads: Family<ThreadLabels, Gauge>,
    /// The memory they use together.
    pub memory: Gauge,
    /// How long their event loops took to get to a call.
    pub loop_lag: Histogram,
    /// Hosts killed for one wait step.
    pub blocked_kills: Counter,
    /// Wait steps started again on a new host.
    pub restarts: Counter,
    /// Hosts ended with no wait step to blame, by code.
    pub hosts_lost: Family<CodeLabels, Counter>,
    /// `to_thread` calls stopped.
    pub thread_stops: Counter,
}

impl Default for WatcherMetrics {
    fn default() -> Self {
        Self {
            slots: Gauge::default(),
            slots_used: Gauge::default(),
            hosts: Gauge::default(),
            threads: Family::default(),
            memory: Gauge::default(),
            // 1 ms to about 30 s: `[watchers] block_limit` is 30 s by default.
            loop_lag: Histogram::new(exponential_buckets(0.001, 2.0, 16)),
            blocked_kills: Counter::default(),
            restarts: Counter::default(),
            hosts_lost: Family::default(),
            thread_stops: Counter::default(),
        }
    }
}

impl WatcherMetrics {
    fn register(&self, r: &mut Registry) {
        r.register(WATCHER_SLOTS.name, WATCHER_SLOTS.help, self.slots.clone());
        r.register(
            WATCHER_SLOTS_USED.name,
            WATCHER_SLOTS_USED.help,
            self.slots_used.clone(),
        );
        r.register(WATCHER_HOSTS.name, WATCHER_HOSTS.help, self.hosts.clone());
        r.register(
            WATCHER_THREADS.name,
            WATCHER_THREADS.help,
            self.threads.clone(),
        );
        r.register(
            WATCHER_MEMORY.name,
            WATCHER_MEMORY.help,
            self.memory.clone(),
        );
        r.register(
            WATCHER_LOOP_LAG.name,
            WATCHER_LOOP_LAG.help,
            self.loop_lag.clone(),
        );
        r.register(
            WATCHER_BLOCKED_KILLS.name,
            WATCHER_BLOCKED_KILLS.help,
            self.blocked_kills.clone(),
        );
        r.register(
            WATCHER_RESTARTS.name,
            WATCHER_RESTARTS.help,
            self.restarts.clone(),
        );
        r.register(
            WATCHER_HOSTS_LOST.name,
            WATCHER_HOSTS_LOST.help,
            self.hosts_lost.clone(),
        );
        r.register(
            WATCHER_THREAD_STOPS.name,
            WATCHER_THREAD_STOPS.help,
            self.thread_stops.clone(),
        );
    }

    /// The hosts' threads now.
    pub fn set_threads(&self, tied: u64, untied: u64) {
        for (kind, count) in [("tied", tied), ("untied", untied)] {
            self.threads
                .get_or_create(&ThreadLabels {
                    kind: kind.to_owned(),
                })
                .set(i64::try_from(count).unwrap_or(i64::MAX));
        }
    }
}

type StartHistograms = Family<StartLabels, Histogram, fn() -> Histogram>;

/// The agent's metrics (cheap to clone; clones share them).
#[derive(Debug, Clone)]
pub struct AgentMetrics {
    /// Task slots.
    pub slots: Gauge,
    /// Task slots in use.
    pub slots_used: Gauge,
    /// Attempts ended here, by status.
    pub attempts: Family<StatusLabels, Counter>,
    /// How long Python tasks took to start, by start method and phase.
    pub task_start: StartHistograms,
    /// Tasks and parse runners the memory watchdog killed.
    pub watchdog_kills: Counter,
    /// Attempts in the journal (running, or results not acknowledged).
    pub journal_records: Gauge,
    /// Bytes of task and parse logs kept, as of the last retention sweep.
    pub log_bytes: Gauge,
    /// Bytes of logs deleted by retention.
    pub log_swept_bytes: Counter,
    /// The fork servers' states and fallbacks.
    pub fork: crate::fork::ForkMetrics,
    /// The wait step hosts'.
    pub watchers: WatcherMetrics,
}

impl Default for AgentMetrics {
    fn default() -> Self {
        Self {
            slots: Gauge::default(),
            slots_used: Gauge::default(),
            attempts: Family::default(),
            // 10 ms to about 40 s.
            task_start: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.01, 2.0, 13))
            }),
            watchdog_kills: Counter::default(),
            journal_records: Gauge::default(),
            log_bytes: Gauge::default(),
            log_swept_bytes: Counter::default(),
            fork: crate::fork::ForkMetrics::default(),
            watchers: WatcherMetrics::default(),
        }
    }
}

impl AgentMetrics {
    /// Registers every metric in `registry` (names get the `schedr_` prefix there), as
    /// [`METRICS`] describes them.
    pub fn register(&self, registry: &mut Registry) {
        let r = registry;
        r.register(SLOTS.name, SLOTS.help, self.slots.clone());
        r.register(SLOTS_USED.name, SLOTS_USED.help, self.slots_used.clone());
        r.register(ATTEMPTS.name, ATTEMPTS.help, self.attempts.clone());
        r.register(TASK_START.name, TASK_START.help, self.task_start.clone());
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
        r.register(
            FORK_SERVERS.name,
            FORK_SERVERS.help,
            self.fork.servers.clone(),
        );
        r.register(
            FORK_FALLBACKS.name,
            FORK_FALLBACKS.help,
            self.fork.fallbacks.clone(),
        );
        self.watchers.register(r);
    }

    /// Records how long a Python task started `method` took to start.
    pub fn task_started(&self, method: &str, start: &StartTimes) {
        let phases = [("runner", Some(start.runner)), ("import", start.import)];
        for (phase, took) in phases {
            if let Some(took) = took {
                self.task_start
                    .get_or_create(&StartLabels {
                        method: method.to_owned(),
                        phase: phase.to_owned(),
                    })
                    .observe(took.as_secs_f64());
            }
        }
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
