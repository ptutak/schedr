//! The master's metrics (see archive/PROJECT.md, Observability), registered in the process's
//! registry and served on the ops port.
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
use schedr_model::metric_doc::{MetricDoc, MetricKind};

#[cfg(test)]
mod tests;

/// How often the gauges taken from the database are refreshed.
pub const GAUGE_REFRESH: Duration = Duration::from_secs(15);

const TICK_DURATION: MetricDoc = MetricDoc {
    name: "master_tick_duration_seconds",
    kind: MetricKind::Histogram,
    labels: &[],
    help: "How long each tick of the master's loop took",
};
const CORE_LOCK_WAIT: MetricDoc = MetricDoc {
    name: "master_core_lock_wait_seconds",
    kind: MetricKind::Histogram,
    labels: &["by"],
    help: "How long the master's core lock was waited for, by who waited (turn: a worker's \
        messages; round; register; upload: a parse result; stream: a stream ended or dropped; \
        restore)",
};
const TURN_PART: MetricDoc = MetricDoc {
    name: "master_turn_part_seconds",
    kind: MetricKind::Histogram,
    labels: &["part"],
    help: "How long each part of a worker's turn held the core lock (started: its `Started`s \
        stored; finished: its `Finished`s; rest: heartbeats, warnings and the other messages)",
};
const ROUND_PART: MetricDoc = MetricDoc {
    name: "master_round_part_seconds",
    kind: MetricKind::Histogram,
    labels: &["part"],
    help: "How long each part of a round took, the core lock held (commands: commands applied \
        and their follow-ups; scheduler: the scheduler's pass; triggers; prepare: runs prepared \
        and retries queued; expire: assignments expired and replaced incarnations ended; assign: \
        cleanups, chained runs and assignments; rest: parse jobs expired, workers lost)",
};
const TURN_MESSAGES: MetricDoc = MetricDoc {
    name: "master_turn_messages",
    kind: MetricKind::Histogram,
    labels: &["part"],
    help: "How many messages each part of a worker's turn handled",
};
const RUNS: MetricDoc = MetricDoc {
    name: "runs",
    kind: MetricKind::Gauge,
    labels: &["state", "kind"],
    help: "Runs by state and kind",
};
const RUNS_FINISHED: MetricDoc = MetricDoc {
    name: "runs_finished",
    kind: MetricKind::Counter,
    labels: &["dag_id", "result"],
    help: "Runs finished, by DAG and result",
};
const RUNS_LATE_FAILED: MetricDoc = MetricDoc {
    name: "runs_late_failed",
    kind: MetricKind::Counter,
    labels: &["dag_id", "reason"],
    help: "Runs that ended `success` and learnt of a failure afterwards (a yellow success, \
           decision 309), by DAG and reason (`late_cleanup_failed`)",
};
const NOTIFICATIONS_SENT: MetricDoc = MetricDoc {
    name: "notifications_sent",
    kind: MetricKind::Counter,
    labels: &["channel"],
    help: "Notifications delivered, by channel",
};
const RETENTION_DELETED: MetricDoc = MetricDoc {
    name: "retention_deleted",
    kind: MetricKind::Counter,
    labels: &["kind"],
    help: "Rows the retention job deleted: runs, outputs (runs whose outputs went), events, \
           commands, dags, versions",
};
const NOTIFICATIONS_FAILED: MetricDoc = MetricDoc {
    name: "notifications_failed",
    kind: MetricKind::Counter,
    labels: &["channel"],
    help: "Notifications given up on (`[notifications] give_up_after`), by channel",
};
const TASKS: MetricDoc = MetricDoc {
    name: "tasks",
    kind: MetricKind::Gauge,
    labels: &["state"],
    help: "Task instances of unfinished runs, by state",
};
const TASKS_WAITING: MetricDoc = MetricDoc {
    name: "tasks_waiting",
    kind: MetricKind::Gauge,
    labels: &["reason"],
    help: "Ready tasks not assigned at the last assignment pass, by reason (`next_round`: over \
           the round's limit of assignments; `max_active_tasks`, `max_active`, \
           `max_active_per_run`: a concurrency limit reached)",
};
const ASSIGNMENTS: MetricDoc = MetricDoc {
    name: "assignments",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Attempts assigned to workers",
};
const REJECTIONS: MetricDoc = MetricDoc {
    name: "rejections",
    kind: MetricKind::Counter,
    labels: &["reason"],
    help: "Assignments workers rejected, by reason",
};
const ATTEMPTS_ENDED: MetricDoc = MetricDoc {
    name: "attempts_ended",
    kind: MetricKind::Counter,
    labels: &["status"],
    help: "Attempts ended, by status",
};
const PARSE_JOBS_STARTED: MetricDoc = MetricDoc {
    name: "parse_jobs_started",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Parse jobs created",
};
const PARSE_JOBS_FINISHED: MetricDoc = MetricDoc {
    name: "parse_jobs_finished",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Parse jobs finished",
};
const PARSE_DURATION: MetricDoc = MetricDoc {
    name: "parse_duration_seconds",
    kind: MetricKind::Histogram,
    labels: &[],
    help: "How long parse jobs took, from creation to the stored parse",
};
const COMMANDS: MetricDoc = MetricDoc {
    name: "commands",
    kind: MetricKind::Counter,
    labels: &["kind", "decision"],
    help: "Commands decided, by kind and decision",
};
const COMMAND_LATENCY: MetricDoc = MetricDoc {
    name: "command_latency_seconds",
    kind: MetricKind::Histogram,
    labels: &[],
    help: "How long commands waited for the master's decision",
};
const LEADING: MetricDoc = MetricDoc {
    name: "master_leading",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "1 while this master holds the lease and schedules, 0 while it waits or is paused",
};
const REPLACED_HELD: MetricDoc = MetricDoc {
    name: "master_replaced_attempts_held",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Started attempts of a replaced worker incarnation the master holds for a lease before \
        ending them lost (decision 443): its old process may still run them",
};
const WORKERS: MetricDoc = MetricDoc {
    name: "workers",
    kind: MetricKind::Gauge,
    labels: &["state"],
    help: "Workers in the registry, by state",
};

const DAGS: MetricDoc = MetricDoc {
    name: "master_dags",
    kind: MetricKind::Gauge,
    labels: &["state"],
    help: "DAGs that can run, as of the last scheduler round: busy (it visited them) or quiet \
        (nothing due); held: those whose file fails to import (no runs until it does)",
};
const DAG_WAKES: MetricDoc = MetricDoc {
    name: "master_dag_wakes",
    kind: MetricKind::Counter,
    labels: &["reason"],
    help: "DAGs a scheduler round visited after its term's first, by reason: timer, unknown (no \
        next fire time stored), command, run_queued, run_ended, new_version",
};

const TRIGGERS: MetricDoc = MetricDoc {
    name: "master_triggers",
    kind: MetricKind::Gauge,
    labels: &["state"],
    help: "Triggers of DAGs that want them running, by state: running, waiting (for a free watcher \
        slot or to start again), stopped (back-pressure, a generator that returned) or failing \
        (`[triggers] failing_after` errors in a row)",
};
const TRIGGER_EVENTS: MetricDoc = MetricDoc {
    name: "master_trigger_events",
    kind: MetricKind::Counter,
    labels: &["result"],
    help: "Events received from triggers, by result: new (it started a run) or repeat (its key was \
        seen before, and it started none)",
};
const TRIGGER_BACK_PRESSURE_STOPS: MetricDoc = MetricDoc {
    name: "master_trigger_back_pressure_stops",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Triggers stopped for having `[triggers] max_queued_events` queued runs",
};
const TRIGGER_IDLE_RESTARTS: MetricDoc = MetricDoc {
    name: "master_trigger_idle_restarts",
    kind: MetricKind::Counter,
    labels: &[],
    help: "Triggers started again for finding nothing for their `restart_if_idle`",
};

/// The triggers by state, for the gauge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TriggerCounts {
    /// Running on a worker.
    pub running: usize,
    /// Waiting for a free watcher slot or to start again.
    pub waiting: usize,
    /// Stopped by back-pressure or a generator that returned.
    pub stopped: usize,
    /// With `[triggers] failing_after` errors in a row.
    pub failing: usize,
}

/// A trigger event's label.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EventResultLabels {
    /// `new` or `repeat`.
    pub result: String,
}

/// The master's metrics, as registered (under `schedr_`).
pub const METRICS: &[MetricDoc] = &[
    TRIGGERS,
    TRIGGER_EVENTS,
    TRIGGER_BACK_PRESSURE_STOPS,
    TRIGGER_IDLE_RESTARTS,
    TICK_DURATION,
    CORE_LOCK_WAIT,
    TURN_PART,
    TURN_MESSAGES,
    ROUND_PART,
    RUNS,
    RUNS_FINISHED,
    RUNS_LATE_FAILED,
    NOTIFICATIONS_SENT,
    NOTIFICATIONS_FAILED,
    RETENTION_DELETED,
    TASKS,
    TASKS_WAITING,
    ASSIGNMENTS,
    REJECTIONS,
    ATTEMPTS_ENDED,
    PARSE_JOBS_STARTED,
    PARSE_JOBS_FINISHED,
    PARSE_DURATION,
    COMMANDS,
    COMMAND_LATENCY,
    WORKERS,
    LEADING,
    REPLACED_HELD,
    DAGS,
    DAG_WAKES,
];

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

/// A DAG and why its run turned a yellow success.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct LateLabels {
    /// The DAG.
    pub dag_id: String,
    /// Why (`late_cleanup_failed`).
    pub reason: String,
}

/// What the retention job deleted.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct KindLabels {
    /// `runs`, `outputs`, `events`, `commands`, `dags` or `versions`.
    pub kind: String,
}

/// A notification channel.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ChannelLabels {
    /// Its name.
    pub channel: String,
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

/// Who waited for the core lock.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ByLabels {
    /// `turn`, `round`, `register`, `upload`, `stream` or `restore`.
    pub by: String,
}

/// A part of a worker's turn.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartLabels {
    /// A turn's: `started`, `finished` or `rest`; a round's: see `master_round_part_seconds`.
    pub part: String,
}

type Histograms<L> = Family<L, Histogram, fn() -> Histogram>;

/// The master's metrics (cheap to clone; clones share them).
#[derive(Debug, Clone)]
pub struct MasterMetrics {
    /// How long each tick of the loop took.
    pub tick_duration: Histogram,
    /// How long the core lock was waited for, by who waited.
    pub core_lock_wait: Histograms<ByLabels>,
    /// How long each part of a worker's turn held the core lock.
    pub turn_part: Histograms<PartLabels>,
    /// How many messages each part of a worker's turn handled.
    pub turn_messages: Histograms<PartLabels>,
    /// How long each part of a round took.
    pub round_part: Histograms<PartLabels>,
    /// Runs by state and kind.
    pub runs: Family<RunLabels, Gauge>,
    /// Runs finished, by DAG and result.
    pub runs_finished: Family<ResultLabels, Counter>,
    /// Runs that ended `success` and learnt of a failure afterwards, by DAG and reason.
    pub runs_late_failed: Family<LateLabels, Counter>,
    /// Notifications delivered, by channel.
    pub notifications_sent: Family<ChannelLabels, Counter>,
    /// Notifications given up on, by channel.
    pub notifications_failed: Family<ChannelLabels, Counter>,
    /// What the retention job deleted, by kind.
    pub retention_deleted: Family<KindLabels, Counter>,
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
    /// 1 while this master leads (holds the lease), 0 while it waits or is paused.
    pub leading: Gauge,
    /// Started attempts of replaced incarnations still held (decision 443).
    pub replaced_held: Gauge,
    /// DAGs that can run, by `busy` (the last scheduler round visited it) / `quiet` / `held`.
    pub dags: Family<StateLabels, Gauge>,
    /// DAGs a scheduler round visited after its term's first, by why (decision 485).
    pub dag_wakes: Family<ReasonLabels, Counter>,
    /// Triggers by state.
    pub triggers: Family<StateLabels, Gauge>,
    /// Events received from triggers, by result.
    pub trigger_events: Family<EventResultLabels, Counter>,
    /// Triggers stopped by back-pressure.
    pub trigger_back_pressure_stops: Counter,
    /// Triggers started again for finding nothing for too long.
    pub trigger_idle_restarts: Counter,
    /// The label sets the gauges have had: one that is gone reads 0 rather than vanishing.
    seen: Arc<Mutex<Seen>>,
    /// Counts held while a group of worker messages is in its transaction (M4B.3).
    staged: Arc<Mutex<Option<Vec<Staged>>>>,
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
            // 1 ms to about 16 s, each bucket √2 times the last: fine enough for scenario 14
            // to tell a round's p99 one step worse from two (M2.5.2).
            tick_duration: Histogram::new(exponential_buckets(0.001, std::f64::consts::SQRT_2, 29)),
            // 0.1 ms to about 26 s, each bucket 2 times the last.
            core_lock_wait: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.0001, 2.0, 19))
            }),
            turn_part: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.0001, 2.0, 19))
            }),
            round_part: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.0001, 2.0, 19))
            }),
            // 1 to 128 messages.
            turn_messages: Family::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 2.0, 8))
            }),
            runs: Family::default(),
            runs_finished: Family::default(),
            runs_late_failed: Family::default(),
            notifications_sent: Family::default(),
            notifications_failed: Family::default(),
            retention_deleted: Family::default(),
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
            leading: Gauge::default(),
            replaced_held: Gauge::default(),
            dags: Family::default(),
            dag_wakes: Family::default(),
            triggers: Family::default(),
            trigger_events: Family::default(),
            trigger_back_pressure_stops: Counter::default(),
            trigger_idle_restarts: Counter::default(),
            seen: Arc::default(),
            staged: Arc::default(),
        }
    }
}

impl MasterMetrics {
    /// The core lock's and the turns' metrics (M5.1a).
    fn register_lock(&self, r: &mut Registry) {
        r.register(
            CORE_LOCK_WAIT.name,
            CORE_LOCK_WAIT.help,
            self.core_lock_wait.clone(),
        );
        r.register(TURN_PART.name, TURN_PART.help, self.turn_part.clone());
        r.register(
            TURN_MESSAGES.name,
            TURN_MESSAGES.help,
            self.turn_messages.clone(),
        );
        r.register(ROUND_PART.name, ROUND_PART.help, self.round_part.clone());
    }

    /// Records a wait of `took` for the core lock by `by`.
    pub fn core_lock_waited(&self, by: &str, took: Duration) {
        self.core_lock_wait
            .get_or_create(&ByLabels { by: by.to_owned() })
            .observe(took.as_secs_f64());
    }

    /// Records a part of a round that took `took`.
    pub fn round_part_done(&self, part: &str, took: Duration) {
        self.round_part
            .get_or_create(&PartLabels {
                part: part.to_owned(),
            })
            .observe(took.as_secs_f64());
    }

    /// Records a part of a worker's turn: `messages` handled in `took`.
    pub fn turn_part_done(&self, part: &str, messages: usize, took: Duration) {
        let labels = PartLabels {
            part: part.to_owned(),
        };
        self.turn_part
            .get_or_create(&labels)
            .observe(took.as_secs_f64());
        #[expect(
            clippy::cast_precision_loss,
            reason = "a turn's messages, far below 2^52"
        )]
        self.turn_messages
            .get_or_create(&labels)
            .observe(messages as f64);
    }

    /// Registers every metric in `registry` (names get the `schedr_` prefix there), as
    /// [`METRICS`] describes them.
    pub fn register(&self, registry: &mut Registry) {
        let r = registry;
        r.register(
            TICK_DURATION.name,
            TICK_DURATION.help,
            self.tick_duration.clone(),
        );
        self.register_lock(r);
        r.register(RUNS.name, RUNS.help, self.runs.clone());
        r.register(
            RUNS_FINISHED.name,
            RUNS_FINISHED.help,
            self.runs_finished.clone(),
        );
        r.register(
            RUNS_LATE_FAILED.name,
            RUNS_LATE_FAILED.help,
            self.runs_late_failed.clone(),
        );
        r.register(
            NOTIFICATIONS_SENT.name,
            NOTIFICATIONS_SENT.help,
            self.notifications_sent.clone(),
        );
        r.register(
            NOTIFICATIONS_FAILED.name,
            NOTIFICATIONS_FAILED.help,
            self.notifications_failed.clone(),
        );
        r.register(
            RETENTION_DELETED.name,
            RETENTION_DELETED.help,
            self.retention_deleted.clone(),
        );
        r.register(TASKS.name, TASKS.help, self.tasks.clone());
        r.register(
            TASKS_WAITING.name,
            TASKS_WAITING.help,
            self.tasks_waiting.clone(),
        );
        r.register(ASSIGNMENTS.name, ASSIGNMENTS.help, self.assignments.clone());
        r.register(REJECTIONS.name, REJECTIONS.help, self.rejections.clone());
        r.register(
            ATTEMPTS_ENDED.name,
            ATTEMPTS_ENDED.help,
            self.attempts_ended.clone(),
        );
        r.register(
            PARSE_JOBS_STARTED.name,
            PARSE_JOBS_STARTED.help,
            self.parse_jobs_started.clone(),
        );
        r.register(
            PARSE_JOBS_FINISHED.name,
            PARSE_JOBS_FINISHED.help,
            self.parse_jobs_finished.clone(),
        );
        r.register(
            PARSE_DURATION.name,
            PARSE_DURATION.help,
            self.parse_duration.clone(),
        );
        r.register(COMMANDS.name, COMMANDS.help, self.commands.clone());
        r.register(
            COMMAND_LATENCY.name,
            COMMAND_LATENCY.help,
            self.command_latency.clone(),
        );
        r.register(WORKERS.name, WORKERS.help, self.workers.clone());
        r.register(LEADING.name, LEADING.help, self.leading.clone());
        r.register(
            REPLACED_HELD.name,
            REPLACED_HELD.help,
            self.replaced_held.clone(),
        );
        r.register(DAGS.name, DAGS.help, self.dags.clone());
        r.register(DAG_WAKES.name, DAG_WAKES.help, self.dag_wakes.clone());
        r.register(TRIGGERS.name, TRIGGERS.help, self.triggers.clone());
        r.register(
            TRIGGER_EVENTS.name,
            TRIGGER_EVENTS.help,
            self.trigger_events.clone(),
        );
        r.register(
            TRIGGER_BACK_PRESSURE_STOPS.name,
            TRIGGER_BACK_PRESSURE_STOPS.help,
            self.trigger_back_pressure_stops.clone(),
        );
        r.register(
            TRIGGER_IDLE_RESTARTS.name,
            TRIGGER_IDLE_RESTARTS.help,
            self.trigger_idle_restarts.clone(),
        );
    }

    /// Sets the busy, quiet and held DAGs' counts (held ones count as busy or quiet too).
    pub fn set_dags(&self, busy: usize, quiet: usize, held: usize) {
        for (state, count) in [("busy", busy), ("quiet", quiet), ("held", held)] {
            self.dags
                .get_or_create(&StateLabels {
                    state: state.to_owned(),
                })
                .set(i64::try_from(count).unwrap_or(i64::MAX));
        }
    }

    /// Sets the triggers' gauge.
    pub fn set_triggers(&self, counts: TriggerCounts) {
        for (state, count) in [
            ("running", counts.running),
            ("waiting", counts.waiting),
            ("stopped", counts.stopped),
            ("failing", counts.failing),
        ] {
            self.triggers
                .get_or_create(&StateLabels {
                    state: state.to_owned(),
                })
                .set(i64::try_from(count).unwrap_or(i64::MAX));
        }
    }

    /// Counts an event from a trigger: whether it started a run.
    pub fn trigger_event(&self, new: bool) {
        self.trigger_events
            .get_or_create(&EventResultLabels {
                result: if new { "new" } else { "repeat" }.to_owned(),
            })
            .inc();
    }

    /// Counts a quiet DAG woken for `reason`.
    pub fn dag_woken(&self, reason: &str) {
        self.dag_wakes
            .get_or_create(&ReasonLabels {
                reason: reason.to_owned(),
            })
            .inc();
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
        if self.held(|| Staged::AttemptEnded(status.to_string())) {
            return;
        }
        self.attempts_ended
            .get_or_create(&StatusLabels {
                status: status.to_string(),
            })
            .inc();
    }

    /// Counts one run turned a yellow success (decision 309).
    pub fn run_late_failed(&self, dag_id: &str, reason: &str) {
        if self.held(|| Staged::RunLateFailed(dag_id.to_owned(), reason.to_owned())) {
            return;
        }
        self.runs_late_failed
            .get_or_create(&LateLabels {
                dag_id: dag_id.to_owned(),
                reason: reason.to_owned(),
            })
            .inc();
    }

    /// Counts one notification delivered to `channel`.
    pub fn notification_sent(&self, channel: &str) {
        self.notifications_sent
            .get_or_create(&ChannelLabels {
                channel: channel.to_owned(),
            })
            .inc();
    }

    /// Counts one notification to `channel` given up on.
    pub fn notification_failed(&self, channel: &str) {
        self.notifications_failed
            .get_or_create(&ChannelLabels {
                channel: channel.to_owned(),
            })
            .inc();
    }

    /// Counts `n` rows of `kind` the retention job deleted.
    pub fn retention_deleted(&self, kind: &str, n: u64) {
        if n > 0 {
            self.retention_deleted
                .get_or_create(&KindLabels {
                    kind: kind.to_owned(),
                })
                .inc_by(n);
        }
    }

    /// Counts one finished run.
    pub fn run_finished(&self, dag_id: &str, result: &impl ToString) {
        if self.held(|| Staged::RunFinished(dag_id.to_owned(), result.to_string())) {
            return;
        }
        self.runs_finished
            .get_or_create(&ResultLabels {
                dag_id: dag_id.to_owned(),
                result: result.to_string(),
            })
            .inc();
    }
}

/// A count held while a transaction is open ([`MasterMetrics::stage`]).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Staged {
    AttemptEnded(String),
    RunFinished(String, String),
    RunLateFailed(String, String),
}

impl MasterMetrics {
    /// From now on attempt ends and run results are held instead of counted, until
    /// [`Self::count_staged`] counts them (the transaction committed) or [`Self::drop_staged`]
    /// drops them (it rolled back, and its messages will be handled again: M4B.3, so no
    /// result is counted twice).
    pub fn stage(&self) {
        *self
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Vec::new());
    }

    /// Counts what was held, and stops holding.
    pub fn count_staged(&self) {
        let held = self
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        for staged in held.unwrap_or_default() {
            match staged {
                Staged::AttemptEnded(status) => self.attempt_ended(&status),
                Staged::RunFinished(dag_id, result) => self.run_finished(&dag_id, &result),
                Staged::RunLateFailed(dag_id, reason) => self.run_late_failed(&dag_id, &reason),
            }
        }
    }

    /// Drops what was held, and stops holding.
    pub fn drop_staged(&self) {
        self.staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    /// Holds a count while staging; whether it was held.
    fn held(&self, staged: impl FnOnce() -> Staged) -> bool {
        let mut held = self
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.as_mut().map(|held| held.push(staged())).is_some()
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
