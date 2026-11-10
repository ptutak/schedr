//! The master's protocol logic, apart from any transport: registering workers, their messages,
//! disconnects, the periodic tick, parse uploads and inputs (see archive/PROJECT.md, Master–worker
//! protocol and Worker lifecycle).
//!
//! The gRPC service ([`crate::service`]) only moves messages between workers' streams and
//! this core, so everything here is tested without a network.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::workers::{WatcherHostRecord, WorkerRegistration, WorkerWarning};
use schedr_db::{Database, DbError};
use schedr_model::commands::{
    CONNECTION_ANSWER_MARGIN_MS, ConnectionJobResult, ConnectionReport, ConnectionsResult, JobState,
};
use schedr_model::runner::ErrorSummary;
use schedr_model::{SchedrVersion, WorkerState};
use schedr_proto::convert::parsed_file_from_proto;
use schedr_proto::pb::{self, master_message, worker_message};
use schedr_proto::{PROTOCOL_VERSION, check_worker_version};

use crate::commands::{Applied, CommandError, Commander, parse_connection_job_id};
use crate::discovery::{Discovery, ParseUpload};
use crate::dispatch::{
    DEFAULT_CANCEL_GRACE, DEFAULT_CODE_WAIT, DispatchError, Dispatcher, KeptResults, Outgoing,
};
use crate::execution::ExecutionError;
use crate::metrics::MasterMetrics;
use crate::parse_jobs::{PARSE_JOB_TIMEOUT, ParseAssignment, ParseJobs, ParseKey};
use crate::quiet::{QuietSettings, Wakes};
use crate::scheduler::{Scheduler, SchedulerError};

pub mod task_api;
pub(crate) mod triggers;

#[cfg(test)]
mod tests;

/// How long a script root may be unreported before it counts as absent (decision 303).
pub const DEFAULT_SCRIPT_ROOT_GRACE: Duration = Duration::from_mins(5);

/// The master's timing, sent to every worker in `Registered` so all behave the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// How often workers send `Heartbeat`.
    pub heartbeat_interval: Duration,
    /// A worker silent for this long is lost.
    pub lease_timeout: Duration,
    /// The time between SIGTERM and SIGKILL when an attempt is cancelled.
    pub cancel_grace: Duration,
    /// After how many host kills in a row a wait step is switched off (0: never).
    pub switch_off_after: u32,
    /// Queued event runs of a DAG that stop its trigger.
    pub max_queued_events: u32,
    /// Errors in a row that make a trigger `failing`.
    pub failing_after: u32,
    /// How long a wait step without a `max_wait` waits.
    pub sensor_max_wait: Duration,
    /// How long a worker has for a parse job.
    pub parse_job_timeout: Duration,
    /// How long a task's code may be missing on every eligible worker.
    pub code_wait: Duration,
    /// How long a task whose assignment fails on its own is set aside.
    pub set_aside: Duration,
    /// The least an `@sensor(wait_for=...)`'s call gets once handed its run (decision 353).
    pub handed_call_min: Duration,
    /// How long a lost worker's cleanup waits before it moves, on top of `cancel_grace`.
    pub cleanup_move_wait: Duration,
    /// How long no alive worker must report a script root (counted from the master's start
    /// too) before it counts as absent: DAG roots using it are then no longer withdrawn for it
    /// (decision 303).
    pub script_root_grace: Duration,
    /// How far before the newest event an `EventNamed` trigger acted on it looks again, for one
    /// stored late (`SCHEDR_HIDDEN__MASTER__EVENT_LOOKBACK`, decision 354).
    pub event_lookback: Duration,
    /// How a commit cut off is read back (see [`crate::dispatch::ReadBack`]).
    pub read_back: crate::dispatch::ReadBack,
    /// How often to try listening for commands again after it failed to start.
    pub listen_retry: Duration,
    /// How its loop and its parse jobs are paced.
    pub timings: MasterTimings,
    /// Which DAGs a round visits (`[master] quiet_dag*`).
    pub quiet: QuietSettings,
    /// The largest message a worker may send (`[grpc] max_message_size`).
    pub max_message_size: usize,
    /// The most instances a mapped task may have (`[mapping] max_instances`).
    pub max_instances: usize,
    /// The most fire times a backfill's range may have (`[backfill] max_runs`).
    pub backfill_max_runs: u32,
    /// The most records of one kind, and characters of each text, kept of what a worker
    /// reports (wait step hosts, warnings; B183).
    pub worker_report: ReportLimits,
    /// `[retention]` and its job's timing (decisions 430-435).
    pub retention: crate::retention::RetentionSettings,
}

/// How the master's loop and its parse jobs are paced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MasterTimings {
    /// The wait before a failed trigger starts again; it doubles with each failure in a row.
    pub trigger_restart_backoff: Duration,
    /// The most it waits.
    pub trigger_restart_backoff_max: Duration,
    /// At most one extra round every this long (`round_gap`).
    pub round_gap: Duration,
    /// How long until failed notifications are tried again (`notification_retry`).
    pub notification_retry: Duration,
    /// How often its loop turns at least while it waits (`alive_interval`).
    pub alive: Duration,
    /// How often its gauges are read (`gauge_refresh`).
    pub gauge_refresh: Duration,
    /// The first wait before a failed parse job is tried again (`parse_retry_first`).
    pub parse_retry_first: Duration,
    /// The longest one (`parse_retry_max`).
    pub parse_retry_max: Duration,
    /// How long its listener waits after a failed accept (`accept_retry`).
    pub accept_retry: Duration,
}

impl Default for MasterTimings {
    /// What every master runs with.
    fn default() -> Self {
        Self {
            round_gap: crate::service::ROUND_GAP,
            notification_retry: crate::service::NOTIFICATION_RETRY,
            alive: crate::service::ALIVE_INTERVAL,
            gauge_refresh: crate::metrics::GAUGE_REFRESH,
            parse_retry_first: crate::parse_jobs::FIRST_RETRY_DELAY,
            parse_retry_max: crate::parse_jobs::MAX_RETRY_DELAY,
            accept_retry: Duration::from_millis(100),
            trigger_restart_backoff: Duration::from_secs(5),
            trigger_restart_backoff_max: Duration::from_mins(5),
        }
    }
}

impl Default for Settings {
    /// Heartbeat every 10 s, lost after 60 s, 30 s cancel grace, 10 min per parse job, 10 min
    /// for missing code, messages up to 64 MiB.
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            lease_timeout: Duration::from_secs(60),
            cancel_grace: DEFAULT_CANCEL_GRACE,
            switch_off_after: 3,
            max_queued_events: 1000,
            failing_after: 5,
            sensor_max_wait: Duration::from_hours(24),
            parse_job_timeout: PARSE_JOB_TIMEOUT,
            code_wait: DEFAULT_CODE_WAIT,
            set_aside: crate::dispatch::DEFAULT_SET_ASIDE,
            handed_call_min: crate::dispatch::DEFAULT_HANDED_CALL_MIN,
            cleanup_move_wait: Duration::from_secs(10),
            script_root_grace: DEFAULT_SCRIPT_ROOT_GRACE,
            event_lookback: Duration::from_mins(1),
            read_back: crate::dispatch::ReadBack::default(),
            listen_retry: Duration::from_mins(30),
            timings: MasterTimings::default(),
            quiet: QuietSettings::default(),
            max_message_size: crate::service::DEFAULT_MAX_MESSAGE_SIZE,
            max_instances: crate::execution::DEFAULT_MAX_INSTANCES,
            backfill_max_runs: crate::scheduler::backfill::DEFAULT_MAX_RUNS,
            worker_report: ReportLimits::default(),
            retention: crate::retention::RetentionSettings::default(),
        }
    }
}

/// How much of what a worker reports for people (wait step hosts, warnings) the master keeps
/// (B183): an honest agent sends a few short records; more are dropped, longer texts cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportLimits {
    /// The most records of one kind.
    pub records: usize,
    /// The most characters of each text.
    pub text: usize,
}

impl Default for ReportLimits {
    /// 256 records, 4096 characters (`SCHEDR_HIDDEN__MASTER__WORKER_REPORT_*`).
    fn default() -> Self {
        Self {
            records: 256,
            text: 4096,
        }
    }
}

impl ReportLimits {
    /// Why a registration is over the limits, if it is (B183): what it says is kept and matched
    /// on (its ID, queues), so it is refused rather than cut. Its configuration, shown only,
    /// may be up to as many texts as records.
    fn oversized(self, register: &pb::Register) -> Option<String> {
        let long = |text: &str| text.chars().count() > self.text;
        let texts = [
            ("worker ID", register.worker_id.as_str()),
            ("incarnation", &register.incarnation_id),
            ("schedr version", &register.schedr_version),
            ("log endpoint", &register.log_endpoint),
            ("process ID", &register.process_id),
        ];
        if let Some((what, _)) = texts.iter().find(|(_, text)| long(text)) {
            return Some(format!("its {what} is over {} characters", self.text));
        }
        for (what, list) in [
            ("queues", &register.queues),
            ("secret storages", &register.secret_storages),
        ] {
            if list.len() > self.records {
                return Some(format!("it has over {} {what}", self.records));
            }
            if list.iter().any(|text| long(text)) {
                return Some(format!(
                    "one of its {what} is over {} characters",
                    self.text
                ));
            }
        }
        let config_limit = self.text.saturating_mul(self.records);
        (register.config_json.len() > config_limit)
            .then(|| format!("its configuration is over {config_limit} bytes"))
    }

    /// `text` cut to the limit, at a character.
    fn cut(self, text: String) -> String {
        match text.char_indices().nth(self.text) {
            Some((end, _)) => text[..end].to_owned(),
            None => text,
        }
    }

    /// `records` within the limit, saying in the log when some were dropped.
    fn take<T>(self, worker_id: &str, what: &str, mut records: Vec<T>) -> Vec<T> {
        if records.len() > self.records {
            tracing::warn!(
                worker_id,
                sent = records.len(),
                kept = self.records,
                "worker sent more {what} than the master keeps: the rest are dropped"
            );
            records.truncate(self.records);
        }
        records
    }
}

/// Why the master refused or failed something.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// A registration is refused (the worker is told why and the stream ends).
    #[error("registration refused: {0}")]
    Refused(String),
    /// A message can't be understood.
    #[error("message from worker `{worker_id}`: {reason}")]
    BadMessage {
        /// The worker.
        worker_id: String,
        /// What is wrong.
        reason: String,
    },
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// Dispatching failed.
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    /// Applying commands failed.
    #[error(transparent)]
    Command(#[from] CommandError),
    /// The scheduler failed.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
}

impl CoreError {
    /// Whether the database refused the data a message would store (see
    /// [`DbError::is_data_fault`]): sent again, it is refused again.
    #[must_use]
    pub fn is_data_fault(&self) -> bool {
        match self {
            Self::Db(error)
            | Self::Dispatch(
                DispatchError::Db(error) | DispatchError::Execution(ExecutionError::Db(error)),
            ) => error.is_data_fault(),
            _ => false,
        }
    }
}

/// Whether a `TaskEnded` cut off at its commit stored what it was for: the result kept, or the
/// attempt over (nothing to keep it for).
fn result_landed(attempt: Option<&schedr_db::attempts::StoredAttempt>) -> bool {
    attempt.is_none_or(|a| a.result_kept || a.status.is_some())
}

/// A `Finished` whose result the database refused, as the attempt's failure: kind
/// `result_not_stored` (not retried: the same result would be refused again), what ran kept.
fn not_stored(finished: &pb::Finished, error: &CoreError) -> pb::Finished {
    pb::Finished {
        attempt_id: finished.attempt_id.clone(),
        status: pb::AttemptStatus::Failed.into(),
        exit_code: finished.exit_code,
        signal: finished.signal,
        output_json: Vec::new(),
        outcome: String::new(),
        duration_ms: finished.duration_ms,
        peak_memory_bytes: finished.peak_memory_bytes,
        error_kind: "result_not_stored".to_owned(),
        error_summary_json: ErrorSummary::json(
            "ResultNotStored",
            &format!("the result couldn't be stored: {error}"),
        )
        .into_bytes(),
        // The worker's text, kept, must not be refused again (B127): Postgres stores no NUL.
        code_hash: finished.code_hash.replace('\0', "\u{fffd}"),
        usage: finished.usage,
        core_dumped: finished.core_dumped,
        cleanup: finished.cleanup.clone(),
        wait_step: finished.wait_step,
        struck: finished.struck,
    }
}

/// A connected (or recently disconnected) worker.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Connection {
    incarnation: String,
    schedr_version: String,
    last_seen: Timestamp,
    /// When `last_seen` was last written to the registry (at most once per heartbeat
    /// interval).
    persisted: Timestamp,
    live: bool,
    /// The secret storages it has configured (`Register`): a DAG it parses that names another
    /// is refused. None until it registers with this master (restored from the database).
    secret_storages: Option<BTreeSet<String>>,
    /// What its wait step hosts looked like when last written to the registry.
    watcher_hosts: Vec<pb::WatcherHost>,
}

/// What has arrived from workers before the master handled it (cheap to clone; clones share
/// it). A stream's reader records each message as it is read, before the message waits for the
/// master's lock, so a busy master doesn't count its own delay against workers:
/// - when each worker's latest message arrived: the lease counts from it, so a master stuck in
///   a slow write doesn't declare workers lost whose messages keep arriving (B6);
/// - which attempts' `Started` (or `Finished`) arrived and wait to be handled: they aren't
///   taken back as never started (B11).
#[derive(Debug, Clone, Default)]
pub struct Arrivals(Arc<std::sync::Mutex<Arrived>>);

#[derive(Debug, Default)]
struct Arrived {
    last: BTreeMap<String, Timestamp>,
    /// Attempts whose start arrived and waits to be handled, with their worker.
    started: BTreeMap<String, String>,
}

impl Arrivals {
    fn inner(&self) -> std::sync::MutexGuard<'_, Arrived> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A message from `worker_id` arrived `at`.
    pub fn arrived(&self, worker_id: &str, at: Timestamp) {
        let mut inner = self.inner();
        let last = inner.last.entry(worker_id.to_owned()).or_insert(at);
        *last = (*last).max(at);
    }

    /// When the latest message from `worker_id` arrived, if one has since it was forgotten.
    #[must_use]
    pub fn last(&self, worker_id: &str) -> Option<Timestamp> {
        self.inner().last.get(worker_id).copied()
    }

    /// Forgets `worker_id` (it is gone), and the starts of its that wait: their messages won't
    /// be handled any more.
    pub fn forget(&self, worker_id: &str) {
        let mut inner = self.inner();
        inner.last.remove(worker_id);
        inner.started.retain(|_, worker| worker != worker_id);
    }

    /// A `Started` or `Finished` for `attempt_id` arrived from `worker_id`: the attempt started.
    pub fn start_arrived(&self, worker_id: &str, attempt_id: &str) {
        let _ = self.start_known(worker_id, attempt_id);
    }

    /// As [`Self::start_arrived`]; returns whether the start wasn't known already.
    #[must_use]
    pub fn start_known(&self, worker_id: &str, attempt_id: &str) -> bool {
        self.inner()
            .started
            .insert(attempt_id.to_owned(), worker_id.to_owned())
            .is_none()
    }

    /// Whether a `Started` or `Finished` for `attempt_id` arrived and isn't handled yet.
    #[must_use]
    pub fn start_waiting(&self, attempt_id: &str) -> bool {
        self.inner().started.contains_key(attempt_id)
    }

    /// How many `Started` or `Finished` messages arrived and aren't handled yet.
    #[must_use]
    pub fn starts_waiting(&self) -> usize {
        self.inner().started.len()
    }

    /// Forgets the starts of `worker_id`'s that wait: it registered, and its journal is what
    /// stands now.
    pub fn forget_starts(&self, worker_id: &str) {
        self.inner().started.retain(|_, worker| worker != worker_id);
    }

    /// The message about `attempt_id`'s start was handled (or failed to be).
    pub fn start_handled(&self, attempt_id: &str) {
        self.inner().started.remove(attempt_id);
    }
}

/// The master's state between messages.
#[derive(Debug)]
pub struct MasterCore {
    db: Database,
    settings: Settings,
    discovery: Discovery,
    dispatcher: Dispatcher,
    scheduler: Scheduler,
    commander: Commander,
    connections: BTreeMap<String, Connection>,
    arrivals: Arrivals,
    metrics: MasterMetrics,
    /// Workers resumed whose follow-up (their parse jobs) failed: tried again each round, as
    /// the resume command isn't applied again (B99).
    unresumed: BTreeSet<String>,
    /// The script roots each DAG root uses as last sent to the live workers (decision 304); a
    /// change goes to them all at the next tick (a worker registering gets them in
    /// `Registered`).
    sent_uses: crate::discovery::ScriptUses,
    /// When an alive worker last reported each script root, and when this master started
    /// looking (decision 303's grace).
    script_roots_seen: (BTreeMap<String, Timestamp>, Option<Timestamp>),
    /// The connection jobs sent and not answered, by job ID, with their worker (lost with
    /// it) and when they are given up on (`schedr connections check` / `test`).
    connection_jobs: BTreeMap<String, (String, Timestamp)>,
    /// Triggers told to stop and not heard of since, by instance, with their worker and DAG: each
    /// keeps its watcher slot, as the worker's own count does, and what it ends with still counts
    /// (a strike for getting its host killed on the way).
    stopping_triggers: BTreeMap<String, (String, String)>,
    /// The API tasks call through their agents (M4.11h); none: they are refused.
    task_api: Option<Arc<dyn task_api::TaskApi>>,
    /// `[notifications]`.
    notifications: Arc<crate::notifications::Notifications>,
}

impl MasterCore {
    /// The master over `db`, with no workers yet.
    #[must_use]
    pub fn new(db: Database, settings: Settings) -> Self {
        let arrivals = Arrivals::default();
        // What changes runs or DAGs marks them, so the scheduler wakes them if quiet.
        let wakes = Wakes::default();
        let mut discovery = Discovery::new(
            db.clone(),
            SchedrVersion::current(),
            ParseJobs::new(settings.parse_job_timeout).with_retry(
                settings.timings.parse_retry_first,
                settings.timings.parse_retry_max,
            ),
        )
        .with_max_message_size(settings.max_message_size);
        discovery.set_wakes(wakes.clone());
        let mut dispatcher = Dispatcher::new(db.clone(), settings.cancel_grace)
            .with_switch_off_after(settings.switch_off_after)
            .with_sensor_max_wait(settings.sensor_max_wait)
            .with_max_message_size(settings.max_message_size)
            .with_code_wait(settings.code_wait)
            .with_set_aside(settings.set_aside, settings.read_back)
            .with_handed_call_min(settings.handed_call_min)
            .with_cleanup_move_wait(
                settings
                    .cancel_grace
                    .saturating_add(settings.cleanup_move_wait),
            )
            .with_start_timeout(settings.lease_timeout)
            .with_arrivals(arrivals.clone());
        dispatcher.set_wakes(wakes.clone());
        dispatcher.set_max_instances(settings.max_instances);
        let scheduler = Scheduler::with_quiet(db.clone(), settings.quiet, wakes);
        dispatcher.set_scheduler(scheduler.clone());
        Self {
            discovery,
            dispatcher,
            scheduler,
            commander: Commander::new(db.clone())
                .with_backfill_max_runs(settings.backfill_max_runs),
            connections: BTreeMap::new(),
            arrivals,
            metrics: MasterMetrics::default(),
            unresumed: BTreeSet::new(),
            sent_uses: crate::discovery::ScriptUses::new(),
            script_roots_seen: (BTreeMap::new(), None),
            connection_jobs: BTreeMap::new(),
            stopping_triggers: BTreeMap::new(),
            task_api: None,
            notifications: Arc::default(),
            settings,
            db,
        }
        // One set shared by every part, replaced by the caller's to register them.
        .with_metrics(MasterMetrics::default())
    }

    /// The master sending notifications as `notifications` says (`[notifications]`): what
    /// runs, tasks and triggers queue, and the checks on DAGs' `notify=`.
    #[must_use]
    pub fn with_notifications(
        mut self,
        notifications: Arc<crate::notifications::Notifications>,
    ) -> Self {
        self.dispatcher.set_notifications(notifications.clone());
        self.discovery.set_notifications(notifications.clone());
        self.notifications = notifications;
        self
    }

    /// The master serving tasks' REST requests through `api` (M4.11h).
    #[must_use]
    pub fn with_task_api(mut self, api: Arc<dyn task_api::TaskApi>) -> Self {
        self.task_api = Some(api);
        self
    }

    /// The master recording its metrics in `metrics` (registered by the caller).
    #[must_use]
    pub fn with_metrics(mut self, metrics: MasterMetrics) -> Self {
        self.dispatcher.set_metrics(metrics.clone());
        self.discovery.set_metrics(metrics.clone());
        self.commander.set_metrics(metrics.clone());
        self.scheduler.set_metrics(metrics.clone());
        self.metrics = metrics;
        self
    }

    /// Its metrics.
    #[must_use]
    pub const fn metrics(&self) -> &MasterMetrics {
        &self.metrics
    }

    /// Its database.
    #[must_use]
    pub const fn db(&self) -> &Database {
        &self.db
    }

    /// The master's timing.
    #[must_use]
    pub const fn settings(&self) -> Settings {
        self.settings
    }

    /// The dispatcher (its workers, for status and tests).
    #[must_use]
    pub const fn dispatcher(&self) -> &Dispatcher {
        &self.dispatcher
    }

    /// When each worker's latest message arrived, for the streams' readers to record.
    #[must_use]
    pub fn arrivals(&self) -> Arrivals {
        self.arrivals.clone()
    }

    /// Reads the worker registry back at start: every worker not gone counts as disconnected
    /// from `now`, so its lease starts from zero (master downtime doesn't count against
    /// workers) and one that never comes back is lost, its attempts with it. Returns how many
    /// workers were restored.
    ///
    /// # Errors
    ///
    /// [`CoreError::Db`] if the database fails.
    pub async fn restore(&mut self, now: Timestamp) -> Result<usize, CoreError> {
        let mut restored = 0;
        for worker in self.db.workers().await? {
            if worker.state.is_gone() {
                continue;
            }
            self.db
                .set_worker_state(&worker.id, WorkerState::Disconnected, now)
                .await?;
            self.dispatcher.worker_away(&worker.id);
            self.connections.insert(
                worker.id,
                Connection {
                    incarnation: worker.incarnation,
                    schedr_version: worker.schedr_version,
                    last_seen: now,
                    persisted: now,
                    live: false,
                    secret_storages: None,
                    watcher_hosts: Vec::new(),
                },
            );
            restored += 1;
        }
        // The jobs a previous master sent: their answers would come to it.
        self.db
            .connections_commands_lost("the master restarted before the answer came")
            .await?;
        Ok(restored)
    }

    /// Whether a worker has a live stream.
    #[must_use]
    pub fn is_live(&self, worker_id: &str) -> bool {
        self.connections.get(worker_id).is_some_and(|c| c.live)
    }

    /// A worker registers (the first message on its stream). Returns `Registered`, with a
    /// decision per journal entry, and the parse jobs for its roots.
    ///
    /// # Errors
    ///
    /// [`CoreError::Refused`] for an incompatible protocol version or a worker ID that already
    /// has a live stream; [`CoreError::Db`] / [`CoreError::Dispatch`] if the database fails.
    pub async fn register(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<(pb::Registered, Vec<Outgoing>), CoreError> {
        check_worker_version(PROTOCOL_VERSION, register.protocol)
            .map_err(|e| CoreError::Refused(e.to_string()))?;
        let worker_id = register.worker_id.as_str();
        if worker_id.is_empty() {
            return Err(CoreError::Refused(
                "the worker sent no worker ID".to_owned(),
            ));
        }
        if let Some(why) = self.settings.worker_report.oversized(register) {
            return Err(CoreError::Refused(why));
        }
        if self.is_live(worker_id) {
            return Err(CoreError::Refused(format!(
                "worker `{worker_id}` already has a live stream"
            )));
        }
        let accepted = match self.accept(register, now).await {
            Ok((registered, mut outgoing)) => {
                let triggers = self
                    .triggers_on_register(worker_id, &register.running_triggers, now)
                    .await;
                triggers.map(|triggers| {
                    outgoing.extend(triggers);
                    (registered, outgoing)
                })
            }
            Err(error) => Err(error),
        };
        if accepted.is_err()
            && let Some(connection) = self.connections.get_mut(worker_id)
        {
            // A registration that failed half-way (the database) leaves no live stream behind:
            // the worker's next attempt is accepted, or its lease runs out.
            connection.live = false;
        }
        accepted
    }

    /// Records the registering worker's node for the Deployments page: this master (the lease's
    /// holder it writes as), the worker's process and its configuration, masked again here
    /// (it came from the worker; one that isn't JSON isn't kept).
    async fn record_node(&self, register: &pb::Register) -> Result<(), CoreError> {
        let config = if register.config_json.is_empty() {
            None
        } else {
            match serde_json::from_str::<serde_json::Value>(&register.config_json) {
                Ok(mut config) => {
                    schedr_config::masked::mask(&mut config);
                    Some(config)
                }
                Err(error) => {
                    tracing::warn!(
                        worker_id = register.worker_id,
                        %error,
                        "the worker's configuration isn't JSON; not shown"
                    );
                    None
                }
            }
        };
        let process_id = Some(register.process_id.as_str()).filter(|id| !id.is_empty());
        self.db
            .set_worker_node(
                &register.worker_id,
                self.db.fenced_as(),
                process_id,
                config.as_ref(),
            )
            .await?;
        Ok(())
    }

    /// A worker came back as a new incarnation: its unreported attempts and its jobs are lost.
    async fn incarnation_replaced(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let worker_id = register.worker_id.as_str();
        let reported = register
            .journal
            .iter()
            .map(|entry| entry.attempt_id.as_str())
            .collect();
        let ended = self
            .dispatcher
            .incarnation_replaced(worker_id, &reported, now)
            .await?;
        tracing::warn!(
            worker_id,
            ended,
            "worker restarted (a new incarnation): its unreported attempts are lost \
             (possibly out of memory)"
        );
        // The old incarnation's jobs: none will answer.
        self.connection_jobs_lost(
            worker_id,
            &format!("worker `{worker_id}` restarted before answering"),
        )
        .await
    }

    /// Accepts a registration that passed the checks: reconciles a replaced incarnation,
    /// records the worker, and decides on its journal.
    async fn accept(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<(pb::Registered, Vec<Outgoing>), CoreError> {
        let worker_id = register.worker_id.as_str();
        let replaced = self
            .connections
            .get(worker_id)
            .is_some_and(|known| known.incarnation != register.incarnation_id);
        if replaced {
            self.incarnation_replaced(register, now).await?;
        }
        self.connections.insert(
            worker_id.to_owned(),
            Connection {
                incarnation: register.incarnation_id.clone(),
                schedr_version: register.schedr_version.clone(),
                last_seen: now,
                persisted: now,
                live: true,
                secret_storages: Some(register.secret_storages.iter().cloned().collect()),
                watcher_hosts: Vec::new(),
            },
        );
        let protocol = register
            .protocol
            .as_ref()
            .map(|p| format!("{}.{}", p.major, p.minor))
            .unwrap_or_default();
        // The queues it serves, as stored and shown: none configured is `default`.
        let queues = if register.queues.is_empty() {
            vec![crate::workers::DEFAULT_QUEUE.to_owned()]
        } else {
            register.queues.clone()
        };
        self.db
            .register_worker(
                &WorkerRegistration {
                    id: worker_id,
                    incarnation: &register.incarnation_id,
                    protocol_version: &protocol,
                    schedr_version: &register.schedr_version,
                    queues: &queues,
                    slots: register.slots,
                    watcher_slots: register.watcher_slots,
                    log_endpoint: &register.log_endpoint,
                },
                now,
            )
            .await?;
        self.record_node(register).await?;
        self.dispatcher
            .register(
                worker_id,
                &register.incarnation_id,
                &register.queues,
                register.slots,
                now,
            )
            .await?;
        self.dispatcher
            .set_watcher_slots(worker_id, register.watcher_slots);
        // Failing, discovery's memory goes back as it was: the worker registers again with the
        // same reports, and a switch among them is seen again (B91).
        let memory = self.discovery.memory();
        let reported = self.report_roots(register, now).await;
        if reported.is_err() {
            self.discovery.restore(memory);
            self.sync_code();
        }
        let mut outgoing = reported?;
        let mut decisions = Vec::with_capacity(register.journal.len());
        for entry in &register.journal {
            let (decision, cleanup_from, cancel) =
                self.journal_decision(worker_id, entry, now).await?;
            decisions.push(pb::JournalDecision {
                attempt_id: entry.attempt_id.clone(),
                decision: decision.into(),
                cleanup_from,
            });
            outgoing.extend(cancel);
        }
        let registered = pb::Registered {
            timing: Some(pb::Timing {
                heartbeat_interval_ms: millis(self.settings.heartbeat_interval),
                lease_timeout_ms: millis(self.settings.lease_timeout),
                cancel_grace_ms: millis(self.settings.cancel_grace),
            }),
            decisions,
            max_message_size: u64::try_from(self.settings.max_message_size).unwrap_or(u64::MAX),
            script_uses: root_uses(&self.script_uses_at(now)),
        };
        Ok((registered, outgoing))
    }

    /// A worker is draining: its parsing moves to the others.
    async fn worker_draining(
        &mut self,
        worker_id: &str,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        self.dispatcher.draining(worker_id);
        self.db
            .set_worker_state(worker_id, WorkerState::Draining, now)
            .await?;
        let moved = self.discovery.stop_parsing(worker_id, now);
        Ok(moved.into_iter().map(assign_parse).collect())
    }

    /// A task's process ended and its cleanup starts: the result is kept for a worker lost
    /// meanwhile (decision 229); `Finished` settles the attempt otherwise.
    async fn task_ended(
        &mut self,
        worker_id: &str,
        ended: pb::TaskEnded,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let Some(result) = ended.result else {
            return Ok(());
        };
        let (db, memory) = (self.db.clone(), self.dispatcher.memory());
        let id = result.attempt_id.as_str();
        let handled =
            Box::pin(db.atomically(self.dispatcher.task_ended(worker_id, &result, now))).await;
        self.settle(handled, memory, id, result_landed).await?;
        Ok(())
    }

    /// Any message is a sign of life.
    async fn heard_from(&mut self, worker_id: &str, now: Timestamp) {
        let heartbeat = SignedDuration::try_from(self.settings.heartbeat_interval)
            .unwrap_or(SignedDuration::MAX);
        if let Some(connection) = self.connections.get_mut(worker_id) {
            connection.last_seen = now;
            if now.duration_since(connection.persisted) >= heartbeat {
                connection.persisted = now;
                // A sign of life in the registry is for people: a failed write mustn't cost
                // the message it came with (the next interval writes it again).
                if let Err(error) = self.db.worker_seen(worker_id, now).await {
                    tracing::warn!(worker_id, %error, "worker's last heartbeat not recorded");
                }
            }
        }
    }

    /// For people, like the last heartbeat: a failed write is logged, not the message's failure
    /// (the next heartbeat that differs from what was written writes it).
    async fn heard_of_hosts(&mut self, worker_id: &str, hosts: Vec<pb::WatcherHost>) {
        let Some(connection) = self.connections.get_mut(worker_id) else {
            return;
        };
        // Within the limits before anything keeps it: the database and this cache (B183).
        let limits = self.settings.worker_report;
        let hosts: Vec<pb::WatcherHost> = limits
            .take(worker_id, "wait step hosts", hosts)
            .into_iter()
            .map(|host| pb::WatcherHost {
                code: limits.cut(host.code),
                ..host
            })
            .collect();
        if connection.watcher_hosts == hosts {
            return;
        }
        let records: Vec<WatcherHostRecord> = hosts
            .iter()
            .map(|host| WatcherHostRecord {
                code: host.code.clone(),
                wait_steps: host.wait_steps,
                threads: host.threads,
                tied_threads: host.tied_threads.min(host.threads),
                memory_bytes: host.memory_bytes,
                hosts_lost: host.hosts_lost,
            })
            .collect();
        match self.db.set_worker_hosts(worker_id, &records).await {
            Ok(()) => connection.watcher_hosts = hosts,
            Err(error) => {
                tracing::warn!(worker_id, %error, "worker's wait step hosts not recorded");
            }
        }
    }

    /// Handles an attempt registered.
    async fn handles_added(
        &mut self,
        worker_id: &str,
        added: &pb::HandlesAdded,
    ) -> Result<Vec<Outgoing>, CoreError> {
        match Box::pin(self.dispatcher.handles_added(worker_id, added)).await {
            // Refused for its data, it would end the stream on every reconnect.
            Err(error) if error.is_data_fault() => {
                tracing::error!(worker_id, %error, "handles the database refuses");
            }
            handled => handled?,
        }
        Ok(Vec::new())
    }

    /// A worker's warnings recorded, as many and as long as kept (B183).
    async fn worker_warnings(&self, worker_id: &str, reported: pb::Warnings) {
        let limits = self.settings.worker_report;
        let warnings: Vec<WorkerWarning> = limits
            .take(worker_id, "warnings", reported.warnings)
            .into_iter()
            .map(|w| WorkerWarning {
                kind: limits.cut(w.kind),
                interpreter: limits.cut(w.interpreter),
                message: limits.cut(w.message),
            })
            .collect();
        if let Err(error) = self.db.set_worker_warnings(worker_id, &warnings).await {
            tracing::warn!(worker_id, %error, "worker's warnings not recorded");
        }
    }

    /// A message on a registered worker's stream.
    ///
    /// # Errors
    ///
    /// [`CoreError::BadMessage`] for a message that doesn't fit (a second `Register`, unknown
    /// enum values), or a database failure.
    pub async fn handle(
        &mut self,
        worker_id: &str,
        message: worker_message::Message,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        self.heard_from(worker_id, now).await;
        Ok(match message {
            worker_message::Message::Register(_) => {
                return Err(CoreError::BadMessage {
                    worker_id: worker_id.to_owned(),
                    reason: "`Register` is only the first message".to_owned(),
                });
            }
            worker_message::Message::Heartbeat(beat) => {
                self.heard_of_hosts(worker_id, beat.watcher_hosts).await;
                self.triggers_reported(worker_id, &beat.running_triggers)
                    .await?
            }
            message @ (worker_message::Message::TriggerEvent(_)
            | worker_message::Message::TriggerPoked(_)
            | worker_message::Message::TriggerStopped(_)) => {
                self.trigger_message(worker_id, message, now).await?
            }
            // For people, like the last heartbeat: a failed write is logged, not the message's
            // failure (the worker sends them again after its next registration).
            worker_message::Message::Warnings(reported) => {
                self.worker_warnings(worker_id, reported).await;
                Vec::new()
            }
            // A message's writes are one transaction: stored whole or not at all, with one
            // lease check and one commit (T1, 2.10b).
            worker_message::Message::CodeVersionChanged(changed) => match changed.root {
                Some(root) => self.code_version_changed(worker_id, &root, now).await?,
                None => Vec::new(),
            },
            // Rolled back, their changes to memory are put back too; a commit cut off is read
            // back first (D1): stored, memory stays as the message left it.
            worker_message::Message::Started(started) => {
                let (db, memory) = (self.db.clone(), self.dispatcher.memory());
                let handled =
                    Box::pin(db.atomically(self.dispatcher.started(worker_id, &started, now)))
                        .await;
                let id = started.attempt_id.as_str();
                let settled = self
                    .settle(handled, memory, id, |a| a.is_some_and(|a| a.started))
                    .await;
                // Not stored, the start stays known in memory: the attempt runs, and isn't
                // taken back meanwhile; its `Finished` (or a reconnect's journal) settles it.
                if settled.is_ok() {
                    self.arrivals.start_handled(id);
                }
                match settled? {
                    Some(out) => out.into_iter().collect(),
                    None => Vec::new(),
                }
            }
            worker_message::Message::Rejected(rejected) => {
                let (db, memory) = (self.db.clone(), self.dispatcher.memory());
                let handled =
                    Box::pin(db.atomically(self.dispatcher.rejected(worker_id, &rejected, now)))
                        .await;
                let id = rejected.attempt_id.as_str();
                // Undone, the attempt is deleted.
                self.settle(handled, memory, id, |a| a.is_none()).await?;
                Vec::new()
            }
            worker_message::Message::Finished(finished) => {
                let settled = match self.finished(worker_id, &finished, now).await {
                    // Refused for its data, it would be refused again after every reconnect,
                    // the worker's later results waiting behind it: stored as the attempt's
                    // failure instead, and acknowledged (B106).
                    Err(error) if error.is_data_fault() => {
                        tracing::error!(
                            worker_id,
                            attempt_id = finished.attempt_id,
                            %error,
                            "a result the database refuses is stored as the attempt's failure"
                        );
                        self.finished(worker_id, &not_stored(&finished, &error), now)
                            .await
                    }
                    settled => settled,
                };
                let id = finished.attempt_id.as_str();
                // As for `Started`: not stored, the worker sends it again (B41), and the
                // attempt isn't taken back meanwhile.
                if settled.is_ok() {
                    self.arrivals.start_handled(id);
                }
                vec![settled?.unwrap_or_else(|| crate::dispatch::finished_ack(worker_id, id))]
            }
            worker_message::Message::ApiCall(call) => self.api_call(worker_id, call, now).await?,
            worker_message::Message::HandlesAdded(added) => {
                self.handles_added(worker_id, &added).await?
            }
            worker_message::Message::TaskEnded(ended) => {
                self.task_ended(worker_id, ended, now).await?;
                Vec::new()
            }
            worker_message::Message::Draining(_) => self.worker_draining(worker_id, now).await?,
            worker_message::Message::Deregister(_) => self.deregistered(worker_id, now).await,
            worker_message::Message::ConnectionJobDone(done) => {
                self.connection_job_done(worker_id, done).await?;
                Vec::new()
            }
        })
    }

    /// A worker answered a connections job: its reports into the command's result.
    async fn connection_job_done(
        &mut self,
        worker_id: &str,
        done: pb::ConnectionJobDone,
    ) -> Result<(), CoreError> {
        let mut reports = Vec::with_capacity(done.report_json.len());
        for json in &done.report_json {
            let report: ConnectionReport =
                serde_json::from_slice(json).map_err(|e| CoreError::BadMessage {
                    worker_id: worker_id.to_owned(),
                    reason: format!("a connection report doesn't read: {e}"),
                })?;
            reports.push(report);
        }
        // Only its own worker's answer, and once stored: until then it can still be lost.
        if self
            .connection_jobs
            .get(&done.job_id)
            .is_some_and(|(worker, _)| worker != worker_id)
        {
            return Ok(());
        }
        self.update_connection_job(&done.job_id, worker_id, |job| {
            job.state = JobState::Done;
            job.connections = reports;
        })
        .await?;
        self.connection_jobs.remove(&done.job_id);
        Ok(())
    }

    /// Changes one job of a connections command's stored result, if it is `worker_id`'s and
    /// still running (an answer after the job was given up on is dropped).
    async fn update_connection_job(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut ConnectionJobResult),
    ) -> Result<(), CoreError> {
        let Some((command_id, root)) = parse_connection_job_id(job_id) else {
            return Ok(());
        };
        let Some(record) = self.db.command(command_id).await? else {
            return Ok(());
        };
        let Some(mut result) = record
            .result
            .and_then(|value| serde_json::from_value::<ConnectionsResult>(value).ok())
        else {
            return Ok(());
        };
        let Some(job) = result.jobs.iter_mut().find(|job| {
            job.root == root && job.worker == worker_id && job.state == JobState::Running
        }) else {
            return Ok(());
        };
        update(job);
        let value = serde_json::to_value(&result).unwrap_or_default();
        self.db.set_command_result(command_id, &value).await?;
        Ok(())
    }

    /// The connections jobs of a worker that went away (or restarted, `why`): lost, so their
    /// command finishes.
    async fn connection_jobs_lost(&mut self, worker_id: &str, why: &str) -> Result<(), CoreError> {
        let lost: Vec<String> = self
            .connection_jobs
            .iter()
            .filter(|(_, (worker, _))| worker == worker_id)
            .map(|(job, _)| job.clone())
            .collect();
        for job_id in lost {
            self.connection_job_lost(&job_id, worker_id, why.to_owned())
                .await?;
        }
        Ok(())
    }

    async fn connection_job_lost(
        &mut self,
        job_id: &str,
        worker_id: &str,
        why: String,
    ) -> Result<(), CoreError> {
        self.update_connection_job(job_id, worker_id, |job| {
            job.state = JobState::Lost;
            job.reason = Some(why);
        })
        .await?;
        self.connection_jobs.remove(job_id);
        Ok(())
    }

    /// The connections jobs past their time: lost (their answer, if any, went with a stream
    /// that ended).
    async fn connection_jobs_overdue(&mut self, now: Timestamp) -> Result<(), CoreError> {
        let overdue: Vec<(String, String)> = self
            .connection_jobs
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= now)
            .map(|(job, (worker, _))| (job.clone(), worker.clone()))
            .collect();
        for (job_id, worker_id) in overdue {
            let why = format!("worker `{worker_id}` didn't answer in time");
            self.connection_job_lost(&job_id, &worker_id, why).await?;
        }
        Ok(())
    }

    /// A worker's stream ended: it may reconnect (same incarnation, tasks still running)
    /// within the lease timeout, or it is lost at the next tick after that.
    ///
    /// # Errors
    ///
    /// [`CoreError::Db`] if the registry can't be written.
    pub async fn disconnected(&mut self, worker_id: &str, now: Timestamp) -> Result<(), CoreError> {
        if let Some(connection) = self.connections.get_mut(worker_id)
            && connection.live
        {
            connection.live = false;
            self.dispatcher.worker_disconnected(worker_id);
            self.db
                .set_worker_state(worker_id, WorkerState::Disconnected, now)
                .await?;
        }
        Ok(())
    }

    /// One pass of the master's loop: commands, the scheduler, dispatching, parse jobs past
    /// their timeout, and workers past their lease.
    ///
    /// # Errors
    ///
    /// The first failure; what was done before it stays done, and the next tick goes on.
    pub async fn tick(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, CoreError> {
        let (outgoing, result) = self.tick_partial(now).await;
        result.map(|()| outgoing)
    }

    /// As [`Self::tick`], with the messages built before a failure: each is built after the
    /// write it stands for is stored, so they are delivered whatever comes after (B7: a cancel
    /// dropped with its failed round was sent again only when the worker reconnected).
    pub async fn tick_partial(&mut self, now: Timestamp) -> (Vec<Outgoing>, Result<(), CoreError>) {
        let mut outgoing = Vec::new();
        let result = self.tick_into(&mut outgoing, now).await;
        (outgoing, result)
    }

    async fn tick_into(
        &mut self,
        outgoing: &mut Vec<Outgoing>,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        self.send_uses(outgoing, now);
        let mut applied = Applied::default();
        let commands = self
            .commander
            .apply_into(&self.scheduler, &mut self.dispatcher, &mut applied, now)
            .await;
        outgoing.append(&mut applied.outgoing);
        for job in applied.connection_jobs.drain(..) {
            let deadline = now
                .checked_add(job.allowed + Duration::from_millis(CONNECTION_ANSWER_MARGIN_MS))
                .unwrap_or(now);
            self.connection_jobs
                .insert(job.job_id, (job.worker_id, deadline));
        }
        if let Err(error) = self.connection_jobs_overdue(now).await {
            tracing::warn!(%error, "overdue connection jobs not marked lost yet");
        }
        // The drains and resumes decided before a failing command get their follow-up too:
        // being decided, they won't be applied again. A resume whose follow-up failed is
        // tried again each round until it succeeds, or the worker is drained again (B99).
        let mut follow_ups: Vec<(String, bool)> = std::mem::take(&mut self.unresumed)
            .into_iter()
            .map(|worker_id| (worker_id, false))
            .collect();
        follow_ups.append(&mut applied.drained);
        for (worker_id, drained) in follow_ups {
            if drained {
                self.unresumed.remove(&worker_id);
                let moved = self.discovery.stop_parsing(&worker_id, now);
                outgoing.extend(moved.into_iter().map(assign_parse));
            } else if self
                .dispatcher
                .workers()
                .get(&worker_id)
                .is_none_or(|w| w.state == crate::workers::WorkerState::Active)
            {
                // Not if the agent itself is draining (shutting down).
                match self.discovery.allow_parsing(&worker_id, now).await {
                    Ok(jobs) => outgoing.extend(jobs.into_iter().map(assign_parse)),
                    Err(error) => {
                        // Logged, not failing the round: the rest of it doesn't depend on it.
                        tracing::error!(
                            worker_id,
                            %error,
                            "resumed worker's parse jobs not found: tried again next round"
                        );
                        self.unresumed.insert(worker_id);
                    }
                }
            }
        }
        commands?;
        self.scheduler.pass(now).await?;
        self.trigger_pass(now, outgoing).await?;
        self.dispatcher.step_into(now, outgoing).await?;
        let expired = self.discovery.jobs_mut().expire(now);
        outgoing.extend(expired.into_iter().map(assign_parse));
        let lease =
            SignedDuration::try_from(self.settings.lease_timeout).unwrap_or(SignedDuration::MAX);
        let lost: Vec<String> = self
            .connections
            .iter()
            .filter(|(id, c)| {
                // The later of its last handled message and its last arrived one.
                let seen = self
                    .arrivals
                    .last(id)
                    .map_or(c.last_seen, |a| a.max(c.last_seen));
                now.duration_since(seen) > lease
            })
            .map(|(id, _)| id.clone())
            .collect();
        for worker_id in lost {
            self.forget(&worker_id, WorkerState::Lost, now, outgoing)
                .await?;
        }
        Ok(())
    }

    /// A worker uploads a parse result (unary call).
    ///
    /// # Errors
    ///
    /// [`CoreError::BadMessage`] for a result missing its root or with DAG JSON that doesn't
    /// read; a database failure.
    pub async fn upload(
        &mut self,
        result: pb::ParseResult,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let bad = |reason: String| CoreError::BadMessage {
            worker_id: result.worker_id.clone(),
            reason,
        };
        let root = result
            .root
            .clone()
            .ok_or_else(|| bad("a parse result without its root".to_owned()))?;
        let files = result
            .files
            .iter()
            .cloned()
            .map(parsed_file_from_proto)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| bad(e.to_string()))?;
        self.discovery
            .parse_uploaded(
                ParseUpload {
                    worker_id: result.worker_id.clone(),
                    key: ParseKey {
                        kind: root.kind(),
                        root: root.root,
                        content_hash: root.content_hash,
                        schedr_version: result.schedr_version.clone(),
                    },
                    files,
                    secret_storages: self
                        .connections
                        .get(&result.worker_id)
                        .and_then(|c| c.secret_storages.clone()),
                },
                now,
            )
            .await?;
        self.sync_code();
        Ok(())
    }

    /// An open attempt's inputs as JSON (`FetchInputs`); `None` for an unknown or ended one.
    ///
    /// # Errors
    ///
    /// [`CoreError::Dispatch`] if the database fails.
    pub async fn inputs(&self, attempt_id: &str) -> Result<Option<Vec<u8>>, CoreError> {
        Ok(self.dispatcher.attempt_inputs(attempt_id).await?)
    }

    /// What a worker message's handling comes to: its result when stored; when its commit was
    /// cut off and attempt `id` (or its absence) reads back as `landed` (see
    /// [`crate::dispatch::ReadBack`]), `Ok(None)`: stored after all, memory kept; otherwise
    /// the error, memory put back. A read that fails tells nothing.
    /// A worker's `Finished` stored in one transaction (see [`Self::settle`]).
    async fn finished(
        &mut self,
        worker_id: &str,
        finished: &pb::Finished,
        now: Timestamp,
    ) -> Result<Option<Outgoing>, CoreError> {
        let (db, memory) = (self.db.clone(), self.dispatcher.memory());
        let handled =
            Box::pin(db.atomically(self.dispatcher.finished(worker_id, finished, now))).await;
        self.settle(handled, memory, &finished.attempt_id, |a| {
            a.is_some_and(|a| a.status.is_some())
        })
        .await
    }

    async fn settle<T>(
        &mut self,
        handled: Result<T, DispatchError>,
        memory: crate::dispatch::Memory,
        id: &str,
        landed: impl Fn(Option<&schedr_db::attempts::StoredAttempt>) -> bool,
    ) -> Result<Option<T>, CoreError> {
        match handled {
            Ok(done) => Ok(Some(done)),
            Err(DispatchError::Db(DbError::CommitUnknown))
                if self
                    .settings
                    .read_back
                    .stored(|| async {
                        self.db
                            .attempt(id)
                            .await
                            .is_ok_and(|attempt| landed(attempt.as_ref()))
                    })
                    .await =>
            {
                Ok(None)
            }
            Err(error) => {
                self.dispatcher.undo(memory, &error);
                Err(error.into())
            }
        }
    }

    /// A registered worker's version, maintenance drain and roots: its reports replace what it
    /// reported before. Returns the parse jobs to send.
    async fn report_roots(
        &mut self,
        register: &pb::Register,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let worker_id = register.worker_id.as_str();
        let mut outgoing = Vec::new();
        self.dispatcher
            .set_worker_version(worker_id, register.schedr_version.parse().ok());
        // Drained for maintenance stays drained, whatever restarted (until resumed).
        if self.db.worker_drained(worker_id).await? {
            self.dispatcher.set_drained(worker_id);
            let moved = self.discovery.stop_parsing(worker_id, now);
            outgoing.extend(moved.into_iter().map(assign_parse));
        } else {
            outgoing.extend(
                self.discovery
                    .allow_parsing(worker_id, now)
                    .await?
                    .into_iter()
                    .map(assign_parse),
            );
        }
        // Its reports replace what it reported before: a root it no longer has stops counting.
        let keys: Vec<ParseKey> = register
            .roots
            .iter()
            .filter(|root| parsed_kind(root))
            .map(|root| {
                self.dispatcher
                    .report_root(worker_id, &root.root, &root.content_hash);
                ParseKey {
                    kind: root.kind(),
                    root: root.root.clone(),
                    content_hash: root.content_hash.clone(),
                    schedr_version: register.schedr_version.clone(),
                }
            })
            .collect();
        let jobs = self.discovery.registered(worker_id, keys, now).await?;
        outgoing.extend(jobs.into_iter().map(assign_parse));
        self.sync_code();
        Ok(outgoing)
    }

    /// A worker's `CodeVersionChanged`, its writes one transaction.
    async fn code_version_changed(
        &mut self,
        worker_id: &str,
        root: &pb::RootHash,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let (db, memory) = (self.db.clone(), self.discovery.memory());
        let handled = Box::pin(db.atomically(self.root_reported(worker_id, root, now))).await;
        if handled.is_err() {
            // Rolled back, discovery's memory too: a retry decides afresh, and records the
            // switch again (B91).
            self.discovery.restore(memory);
            self.sync_code();
        }
        handled
    }

    async fn root_reported(
        &mut self,
        worker_id: &str,
        root: &pb::RootHash,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        if !parsed_kind(root) {
            return Ok(Vec::new());
        }
        if root.content_hash.is_empty() {
            // Withdrawn (decision 303).
            self.dispatcher.withdraw_root(worker_id, &root.root);
            self.discovery
                .root_withdrawn(worker_id, &root.root, now)
                .await?;
            self.sync_code();
            return Ok(Vec::new());
        }
        let version = self
            .connections
            .get(worker_id)
            .map(|c| c.schedr_version.clone())
            .unwrap_or_default();
        let key = ParseKey {
            kind: root.kind(),
            root: root.root.clone(),
            content_hash: root.content_hash.clone(),
            schedr_version: version,
        };
        self.dispatcher
            .report_root(worker_id, &root.root, &root.content_hash);
        let assignment = self.discovery.root_reported(worker_id, key, now).await?;
        self.sync_code();
        Ok(assignment.into_iter().map(assign_parse).collect())
    }

    async fn journal_decision(
        &mut self,
        worker_id: &str,
        entry: &pb::JournalEntry,
        now: Timestamp,
    ) -> Result<(pb::AttemptDecision, u32, Option<Outgoing>), CoreError> {
        let Some(mut attempt) = self.db.attempt(&entry.attempt_id).await? else {
            return Ok((pb::AttemptDecision::Kill, 0, None));
        };
        let finished = entry.state == i32::from(pb::JournalState::Finished);
        if attempt.worker_id.is_empty()
            && attempt.status.is_none()
            && self.db.cleanup_moved_from(&attempt.id).await?.as_deref() == Some(worker_id)
        {
            // A cleanup that ended while its worker was away, waiting to move: its
            // `Finished`, sent again, counts and the move is dropped (decision 282). Not
            // adopted: nothing of it runs any more.
            if finished {
                return Ok((pb::AttemptDecision::Continue, 0, None));
            }
            // One not ended, holding handles the master never got: the only worker with all
            // of them gets it back, adopted below, and nothing else cleans them (B169).
            if entry.handles > self.known_handles(&attempt.id).await?
                && self.take_back(&attempt, worker_id).await?
            {
                attempt.worker_id = worker_id.to_owned();
            }
        }
        if attempt.worker_id != worker_id {
            // Its cleanup moved from this worker, which may hold handles the master never got
            // (registered while it was cut off): it cleans those itself, and its result is
            // added to the attempt's (B169). One that ended is sent again for that.
            if self.db.rest_owed_by(&attempt.id).await?.as_deref() == Some(worker_id) {
                if finished {
                    return Ok((pb::AttemptDecision::Continue, 0, None));
                }
                let known = self.known_handles(&attempt.id).await?;
                if entry.handles > known {
                    return Ok((pb::AttemptDecision::CleanRest, known, None));
                }
            }
            return Ok((pb::AttemptDecision::Kill, 0, None));
        }
        let (decision, outgoing) = match (attempt.status, finished) {
            // The master stored its result: the worker can forget it.
            (Some(_), true) => (pb::AttemptDecision::Ack, None),
            // The master ended it (lost, expired) while the worker still runs it.
            (Some(_), false) => (pb::AttemptDecision::Kill, None),
            // Still open: keep running (or send its `Finished` again). A cancel sent while
            // the worker was away was dropped, so it is sent again: the run's or the task's
            // (B43).
            (None, _) => {
                self.dispatcher.adopt(worker_id, &attempt.id, now).await?;
                let cancelling = self
                    .db
                    .run_tasks(&attempt.run_id)
                    .await?
                    .is_some_and(|run| run.cancels_instance(&attempt.instance_id));
                // A poke on a worker drained while it was away missed being moved.
                let moving = !cancelling
                    && self.db.worker_drained(worker_id).await?
                    && self.dispatcher.is_poke(worker_id, &attempt.id);
                let cancel = (cancelling || moving).then(|| Outgoing {
                    worker_id: worker_id.to_owned(),
                    message: pb::MasterMessage {
                        message: Some(master_message::Message::CancelAttempt(pb::CancelAttempt {
                            interrupted: moving,
                            attempt_id: attempt.id.clone(),
                        })),
                    },
                });
                (pb::AttemptDecision::Continue, cancel)
            }
        };
        Ok((decision, 0, outgoing))
    }

    /// Undoes a waiting cleanup's move back to `worker_id` (B169). The task's result the
    /// move kept is the master's own `lost` when the task ran there: dropped, the worker
    /// reports its own.
    async fn take_back(
        &self,
        attempt: &schedr_db::attempts::StoredAttempt,
        worker_id: &str,
    ) -> Result<bool, CoreError> {
        let lost_kept = attempt.cleanup_moves == 1
            && self
                .db
                .task_result(&attempt.id)
                .await?
                .and_then(|kept| <pb::Finished as prost::Message>::decode(kept.as_slice()).ok())
                .is_some_and(|kept| kept.status == i32::from(pb::AttemptStatus::Lost));
        Ok(self
            .db
            .take_back_cleanup(&attempt.id, worker_id, lost_kept)
            .await?)
    }

    /// How many of an attempt's cleanup handles the master has, from the first without a gap.
    async fn known_handles(&self, attempt_id: &str) -> Result<u32, CoreError> {
        Ok(self.db.cleanup_handles_unbroken(attempt_id).await?)
    }

    /// A worker's state and the end of its attempts, in one transaction, memory put back if it
    /// fails.
    async fn store_gone(
        &mut self,
        worker_id: &str,
        state: WorkerState,
        results: KeptResults,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let (db, memory) = (self.db.clone(), self.dispatcher.memory());
        let stored = Box::pin(db.atomically(async {
            self.db.set_worker_state(worker_id, state, now).await?;
            self.dispatcher.worker_lost(worker_id, results, now).await?;
            Ok::<(), CoreError>(())
        }))
        .await;
        if let Err(error) = stored {
            self.dispatcher.restore(memory);
            if matches!(error, CoreError::Db(DbError::CommitUnknown))
                || matches!(&error, CoreError::Dispatch(e) if e.commit_unknown())
            {
                self.dispatcher.recount();
            }
            return Err(error);
        }
        Ok(())
    }

    /// A worker deregistered (`Offline`) or is lost (`Lost`): its attempts end `lost`, its
    /// parse jobs move on, and the registry keeps it listed in that state.
    async fn forget(
        &mut self,
        worker_id: &str,
        state: WorkerState,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        // Its state and the end of its attempts in one transaction, memory put back if it
        // fails, and the connection dropped only once everything below is stored: a failure
        // anywhere is retried whole by the next tick's lease check, which looks only at
        // connections (B42); each step can be done again. (`worker_lost` joins this
        // transaction; the snapshot here covers its memory too.)
        let stored = self
            .store_gone(worker_id, state, KeptResults::Used, now)
            .await;
        match stored {
            // A kept result the database refuses would hold the worker, and those after it,
            // for ever: this time without it.
            Err(error) if error.is_data_fault() => {
                tracing::error!(worker_id, %error, "a kept result the database refuses");
                self.store_gone(worker_id, state, KeptResults::Refused, now)
                    .await?;
            }
            stored => stored?,
        }
        // Moved in memory: sent whatever fails after (B7).
        let moved = self.discovery.jobs_mut().worker_lost(worker_id, now);
        outgoing.extend(moved.into_iter().map(assign_parse));
        self.discovery.worker_gone(worker_id, now).await?;
        self.sync_code();
        self.connections.remove(worker_id);
        self.arrivals.forget(worker_id);
        self.stopping_triggers
            .retain(|_, (worker, _)| worker != worker_id);
        // After what is retried: its failure is logged, the CLI giving up after its timeout.
        if let Err(error) = self
            .connection_jobs_lost(
                worker_id,
                &format!("worker `{worker_id}` went away before answering"),
            )
            .await
        {
            tracing::warn!(worker_id, %error, "a lost worker's connection jobs not marked lost");
        }
        Ok(())
    }

    /// A worker deregistered: forgotten as `Offline`. A failure is retried by the lease check
    /// (the connection stays); the parse jobs already moved go out either way, or they'd wait
    /// for their timeout.
    async fn deregistered(&mut self, worker_id: &str, now: Timestamp) -> Vec<Outgoing> {
        let mut outgoing = Vec::new();
        if let Err(error) = self
            .forget(worker_id, WorkerState::Offline, now, &mut outgoing)
            .await
        {
            tracing::error!(worker_id, %error, "a deregistered worker not forgotten yet");
        }
        outgoing
    }

    /// Tells the dispatcher each root's current code, as discovery decided it.
    fn sync_code(&mut self) {
        let mut parsed = BTreeMap::new();
        for (root, hash, accepted, version) in self.discovery.current_code() {
            self.dispatcher.set_current_code(root, accepted, version);
            parsed.insert(root.to_owned(), hash.to_owned());
        }
        self.dispatcher
            .set_connections(self.discovery.registries(), &parsed);
    }
}

impl MasterCore {
    /// The script roots each DAG root uses, as workers fold them: those no alive worker has
    /// reported for `script_root_grace` (since this master started, for one never reported)
    /// left out. No worker could fold one, so all would withdraw the DAG root and a fix to it
    /// would never be parsed; a reference to it stays an import error (295). The grace keeps a
    /// restart of the workers having it, or of the master, from dropping it meanwhile.
    fn script_uses_at(&mut self, now: Timestamp) -> crate::discovery::ScriptUses {
        let (seen, started) = &mut self.script_roots_seen;
        let started = *started.get_or_insert(now);
        for root in self.discovery.reported_script_roots() {
            seen.insert(root, now);
        }
        let grace = jiff::SignedDuration::try_from(self.settings.script_root_grace)
            .unwrap_or(jiff::SignedDuration::MAX);
        let present = |script: &str| {
            let since = seen.get(script).copied().unwrap_or(started);
            now.duration_since(since) < grace
        };
        self.discovery
            .script_uses()
            .iter()
            .filter_map(|(root, used)| {
                let used: BTreeSet<String> = used.iter().filter(|s| present(s)).cloned().collect();
                (!used.is_empty()).then(|| (root.clone(), used))
            })
            .collect()
    }

    /// Sends every live worker the script roots each DAG root uses when they changed since
    /// they were last sent (decision 304).
    fn send_uses(&mut self, outgoing: &mut Vec<Outgoing>, now: Timestamp) {
        let uses = self.script_uses_at(now);
        if uses == self.sent_uses {
            return;
        }
        self.sent_uses = uses;
        let roots = root_uses(&self.sent_uses);
        for (worker_id, connection) in &self.connections {
            if connection.live {
                outgoing.push(Outgoing {
                    worker_id: worker_id.clone(),
                    message: pb::MasterMessage {
                        message: Some(master_message::Message::ScriptUses(pb::ScriptUses {
                            roots: roots.clone(),
                        })),
                    },
                });
            }
        }
    }
}

/// The script roots each DAG root uses, as the protocol sends them (decision 304).
fn root_uses(uses: &crate::discovery::ScriptUses) -> Vec<pb::RootUses> {
    uses.iter()
        .map(|(root, used)| pb::RootUses {
            root: root.clone(),
            script_roots: used.iter().cloned().collect(),
        })
        .collect()
}

fn assign_parse(assignment: ParseAssignment) -> Outgoing {
    Outgoing {
        worker_id: assignment.worker_id,
        message: pb::MasterMessage {
            message: Some(master_message::Message::AssignParse(pb::AssignParse {
                parse_job_id: assignment.job_id,
                root: Some(pb::RootHash {
                    kind: assignment.key.kind.into(),
                    root: assignment.key.root,
                    content_hash: assignment.key.content_hash,
                }),
            })),
        },
    }
}

/// A root the master has parsed: DAG roots, and script roots for their `schedr.toml` files
/// (M4.9); not one of a kind it doesn't know.
fn parsed_kind(root: &pb::RootHash) -> bool {
    matches!(root.kind(), pb::RootKind::Dag | pb::RootKind::Script)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
