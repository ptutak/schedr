//! The master's gRPC service and its loop: workers' `Control` streams, parse uploads and
//! `FetchInputs`, over [`MasterCore`] (see archive/PROJECT.md, Master–worker protocol).
//!
//! A [`Hub`] holds the core and each live worker's outgoing stream; whatever the core decides
//! to send (assignments, cancels, acknowledgements) is delivered through it, from a stream's
//! messages and from the loop's ticks alike.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use jiff::Timestamp;
use schedr_db::commands::CommandNotifications;
use schedr_db::{Database, DbError};
use schedr_model::beat::Beat;
use schedr_proto::convert::ParseUpload;
use schedr_proto::pb::master_server::{Master, MasterServer};
use schedr_proto::pb::{self, master_message, worker_message};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::core::task_api::ApiCallPlan;
use crate::core::{Arrivals, CoreError, Group, MasterCore, RoundPart};
use crate::dispatch::{AssignReads, Outgoing};
use crate::lease::{Leadership, Renewal};
use crate::metrics::MasterMetrics;
use crate::problems::{ProblemKind, Problems};
use crate::session::{Session, Sessions};

#[cfg(test)]
mod tests;

/// The size of one `FetchInputs` chunk.
pub const INPUT_CHUNK: usize = 1024 * 1024;

/// How many messages wait for a worker before it is taken for one that stopped reading.
pub const STREAM_BUFFER: usize = 1024;

/// The most rounds [`Hub::tick`] runs after its first, each asked for by a read the one before
/// skipped on a stale probe (M5.2b): a round creating a run, then one preparing it, then one
/// assigning it, with room for writes from worker turns in between.
#[cfg(any(test, feature = "testing"))]
const FOLLOW_UP_ROUNDS: usize = 4;

/// Whether the certificate a call came with names worker `worker_id` (decision 520,
/// `[tls] worker_identity = "certificate"`): given the call's extensions (where the role puts
/// its connection's peer) and the ID; an `Err` says why not.
#[derive(Clone)]
pub struct WorkerIdentity(pub Arc<IdentityCheck>);

/// [`WorkerIdentity`]'s function: a call's extensions and the worker ID it names.
pub type IdentityCheck = dyn Fn(&tonic::Extensions, &str) -> Result<(), String> + Send + Sync;

impl std::fmt::Debug for WorkerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerIdentity")
    }
}

/// The core and the workers' outgoing streams.
#[derive(Debug, Clone)]
pub struct Hub {
    core: Arc<Mutex<MasterCore>>,
    /// Control calls dropped before they finished, for tests: a test of the path a worker
    /// leaving takes waits for it instead of sleeping (agy's sixth review of M5.3g).
    #[cfg(test)]
    calls_dropped: Arc<std::sync::atomic::AtomicUsize>,
    /// Each worker's session (its current stream), read without a lock; the core writes it
    /// (M5.3i, decision 531).
    sessions: Sessions,
    /// Set when the master stops: each stream's server lets its sender go, for the stream to
    /// end even when full (B200's review).
    closing: Arc<watch::Sender<bool>>,
    clock: fn() -> Timestamp,
    /// When the loop last turned (the master's liveness).
    beat: Beat,
    metrics: MasterMetrics,
    db: Database,
    /// The longest a gauge refresh may take before it is abandoned.
    refresh_limit: Duration,
    /// The largest message a worker may send (a parse upload's file, a `Finished` with its
    /// output): the core's `Settings::max_message_size`, which `Registered` also announces.
    max_message_size: usize,
    /// The most messages of one worker in a turn of the core's lock
    /// (`SCHEDR_HIDDEN__MASTER__MESSAGES_PER_TURN`, M4B.3).
    messages_per_turn: usize,
    /// The most messages of a failed group handled one by one in a turn (decision 493).
    alone_per_turn: usize,
    /// The workers' queue head ages, for the slow-equals each `HeartbeatAck` carries
    /// (decision 509).
    slow_equals: Arc<crate::slow_equals::SlowEquals>,
    /// Whether the gauges' refresh is counting (B270: it runs in a task of its own).
    refreshing: Arc<AtomicBool>,
    /// The most a round assigns: its upstream is read before the core lock (M5.2cA).
    assignments_per_round: usize,
    /// The workers' results (`Finished`, `TaskEnded`, `Rejected`) a turn received, for the next
    /// round to handle (M5.2cB, decision 505): turns don't move runs forward, rounds do.
    results: Arc<std::sync::Mutex<Vec<QueuedResult>>>,
    /// When the round's slow parts (Commands, Expire, Rest) last ran (M5.2cB4, the user's due
    /// flags): they run again once a tick interval has passed, every other round running only
    /// Results, the Scheduler (it starts queued runs: an event's, the next one a finished run
    /// let start), Prepare and Assign. None: due (at the start, leading again, after a failed
    /// round).
    slow_last: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    /// The Commands part is due at once: a command was announced.
    commands_due: Arc<AtomicBool>,
    /// The messages that wait for a worker before it is taken for one that stopped reading.
    stream_buffer: usize,
    /// When each worker's latest message arrived (the core's).
    arrivals: Arrivals,
    /// How often to try listening for commands again while the loop has no notifications
    /// because starting them failed (see [`Hub::with_listen_retry`]).
    listen_retry: Option<Duration>,
    /// Whether this master leads (holds the lease, see [`crate::lease`]): worker messages,
    /// registrations and uploads wait while it doesn't.
    leading: Arc<watch::Sender<bool>>,
    /// Told when a worker's `Finished` was recorded: a round runs at once instead of at the
    /// next tick, its downstream tasks ready (decision 42B; [`TASK_ROUND_GAP`] after the last).
    finished: Arc<tokio::sync::Notify>,
    /// Told when the trigger loop must look (decision 486): a trigger's stored moment came
    /// (the tick's probe), a named event was announced.
    triggers_due: Arc<tokio::sync::Notify>,
    /// Set when a trigger pass failed: the next tick wakes the loop again, as a failed round
    /// is tried again at the next (Grok's review of M5.1f2).
    triggers_failed: Arc<std::sync::atomic::AtomicBool>,
    /// Its problems now, written with its row (decision 129).
    problems: Problems,
    /// The worker identity check (decision 520); none: any worker certificate is any worker.
    worker_identity: Option<WorkerIdentity>,
    /// How its loop is paced.
    timings: crate::core::MasterTimings,
    /// Master time (see [`crate::clock`]), stamped on every message.
    steady: crate::clock::Steady,
    /// The lease epoch this master leads with (decision 444), on every message's stamp.
    epoch: i64,
}

/// Another master took the lease over: this one's view is stale, and it must end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("another master took the lease over: this one stops (restarted, it waits as a standby)")]
pub struct LeaseLost;

/// What woke the master's loop.
enum Wake {
    Stop,
    Tick,
    Command,
    Finished,
    RoundDue,
    NotificationsFailed(DbError),
    Alive,
    Refresh,
    LeaseLost,
    ListenAgain,
}

/// Clears its flag when dropped: a background job's "running", however the job ends.
struct ClearOnDrop(Arc<std::sync::atomic::AtomicBool>);

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Hub {
    /// A hub over `core`, with the system clock.
    #[must_use]
    pub fn new(core: MasterCore) -> Self {
        Self::with_clock(
            core.with_steady(crate::clock::monotonic)
                .with_wall(Timestamp::now),
            Timestamp::now,
        )
    }

    /// A hub over `core` with another clock (tests).
    #[must_use]
    pub fn with_clock(core: MasterCore, clock: fn() -> Timestamp) -> Self {
        let core_steady = core.steady_clock();
        let sessions = core.sessions();
        Self {
            beat: Beat::default(),
            refresh_limit: DEFAULT_REFRESH_LIMIT,
            max_message_size: core.settings().max_message_size,
            messages_per_turn: core.settings().messages_per_turn.max(1),
            alone_per_turn: core.settings().failed_group_messages_per_turn.max(1),
            refreshing: Arc::new(AtomicBool::new(false)),
            slow_equals: Arc::new(crate::slow_equals::SlowEquals::new(
                core.settings().slow_equals,
            )),
            assignments_per_round: core.assignments_per_round(),
            results: Arc::default(),
            slow_last: Arc::default(),
            commands_due: Arc::new(AtomicBool::new(true)),
            stream_buffer: core.settings().worker_stream_buffer.max(1),
            timings: core.settings().timings,
            metrics: core.metrics().clone(),
            db: core.db().clone(),
            arrivals: core.arrivals(),
            leading: Arc::new(watch::Sender::new(true)),
            finished: Arc::new(tokio::sync::Notify::new()),
            // The dispatcher's: a trigger's state changing in memory wakes the loop too.
            triggers_due: core.dispatcher().trigger_wake(),
            triggers_failed: Arc::default(),
            problems: Problems::default(),
            worker_identity: None,
            listen_retry: None,
            core: Arc::new(Mutex::new(core)),
            sessions,
            closing: Arc::new(watch::channel(false).0),
            #[cfg(test)]
            calls_dropped: Arc::default(),
            steady: core_steady,
            epoch: 0,
            clock,
        }
    }

    /// The hub trying to listen for commands again every `period` while its loop runs without
    /// notifications (starting them failed: the database too slow to check for a pooler, or
    /// unreachable), polling meanwhile; it stops trying once they start, or once they are found
    /// off or behind a pooler.
    #[must_use]
    pub const fn with_listen_retry(mut self, period: Duration) -> Self {
        self.listen_retry = Some(period);
        self
    }

    /// The hub stamping `epoch`, its lease's, on every message (decision 444).
    #[must_use]
    pub const fn with_epoch(mut self, epoch: i64) -> Self {
        self.epoch = epoch;
        self
    }

    /// The hub checking each worker call's certificate names the worker it says it is (decision
    /// 520): a `Register`, a parse upload, an attempt's inputs; none, no check.
    #[must_use]
    pub fn with_worker_identity(mut self, check: Option<WorkerIdentity>) -> Self {
        self.worker_identity = check;
        self
    }

    /// Whether a call with `extensions` may act as worker `worker_id`: always without the
    /// check; with it, only when its certificate names it, refused otherwise (and warned of).
    fn identified(&self, extensions: &tonic::Extensions, worker_id: &str) -> Result<(), Status> {
        let Some(check) = &self.worker_identity else {
            return Ok(());
        };
        (check.0)(extensions, worker_id).map_err(|why| {
            tracing::warn!(worker_id, %why, "a call refused: its certificate isn't this worker's");
            Status::permission_denied(format!("worker `{worker_id}`: {why}"))
        })
    }

    /// The hub reporting its problems to `problems` (its heartbeat writes them).
    #[must_use]
    pub fn with_problems(mut self, problems: Problems) -> Self {
        self.problems = problems;
        self
    }

    /// The hub abandoning a gauge refresh that takes longer than `limit` (it only reads, so
    /// abandoning it is safe; a tick is never abandoned: see archive/PROJECT.md, Observability).
    #[must_use]
    pub const fn with_refresh_limit(mut self, limit: Duration) -> Self {
        self.refresh_limit = limit;
        self
    }

    /// Whether this master leads: false while paused (its lease not renewed) or after another
    /// took it over; readiness follows it (B165).
    #[must_use]
    pub fn leading(&self) -> watch::Receiver<bool> {
        self.leading.subscribe()
    }

    /// When the loop last turned: every tick, and at least every [`ALIVE_INTERVAL`] while it
    /// waits, never while a tick is stuck.
    #[must_use]
    pub fn beat(&self) -> Beat {
        self.beat.clone()
    }

    /// Records whether this master leads, telling the waiting worker messages and the log
    /// when that changes.
    fn set_leading(&self, leads: bool) {
        let changed = self.leading.send_if_modified(|leading| {
            let changed = *leading != leads;
            *leading = leads;
            changed
        });
        self.metrics.leading.set(i64::from(leads));
        if changed && leads {
            // Leading again: the first round runs every part.
            *lock_slow(&self.slow_last) = None;
            tracing::info!("the master's lease is renewed: leading again");
            self.problems.clear(ProblemKind::Lease);
        } else if changed {
            tracing::warn!("the master's lease isn't renewed in time: pausing until it is");
            self.problems.set(
                ProblemKind::Lease,
                false,
                "its lease isn't renewed in time: paused until it is",
            );
        }
    }

    /// Waits until this master leads (at once when it does).
    async fn while_leading(&self) {
        let mut leading = self.leading.subscribe();
        let _ = leading.wait_for(|leads| *leads).await;
    }

    /// Waits while this master doesn't lead, as [`Self::while_leading`]; false when it closes
    /// its streams first (it stops): a call waiting would hold tonic's graceful shutdown
    /// (B219's review).
    async fn leading_unless_closing(&self) -> bool {
        let mut closing = self.closing.subscribe();
        // Closing first: a master stopping while it still leads lets nothing new in (agy's fifth
        // review of M5.3g).
        tokio::select! {
            biased;
            _ = closing.wait_for(|closing| *closing) => false,
            () = self.while_leading() => true,
        }
    }

    /// Delivers the outbox through `sender` while this master leads (decision 410): a pass
    /// every `poll`, at once again after a full one, until `stop` completes. A failing pass is
    /// logged; the next one goes on.
    pub async fn run_sender(
        &self,
        sender: crate::notifications::sender::Sender,
        poll: Duration,
        stop: impl Future<Output = ()>,
    ) {
        tokio::pin!(stop);
        loop {
            tokio::select! {
                () = &mut stop => return,
                () = self.while_leading() => {}
            }
            // Housekeeping: nothing a trigger depends on (decision 486).
            let full = match self
                .db
                .quietly(sender.pass((self.clock)(), || *self.leading.borrow()))
                .await
            {
                Ok(pass) => {
                    pass.sent + pass.retried + pass.dropped
                        >= crate::notifications::sender::BATCH as usize
                }
                Err(error) => {
                    tracing::warn!(%error, "can't deliver notifications; trying again");
                    false
                }
            };
            if full {
                continue;
            }
            tokio::select! {
                () = &mut stop => return,
                () = tokio::time::sleep(poll) => {}
            }
        }
    }

    /// Deletes what `[retention]` no longer keeps while this master leads (decisions
    /// 430-435): a pass at once, then one every `settings.interval`, until `stop` completes. A
    /// failing pass is logged; the next one goes on.
    pub async fn run_retention(
        &self,
        settings: crate::retention::RetentionSettings,
        stop: impl Future<Output = ()>,
    ) {
        let nothing = crate::retention::RetentionSettings {
            interval: settings.interval,
            batch: settings.batch,
            ..Default::default()
        };
        if settings == nothing {
            return;
        }
        tokio::pin!(stop);
        loop {
            tokio::select! {
                () = &mut stop => return,
                () = self.while_leading() => {}
            }
            let leading = || *self.leading.borrow();
            // Housekeeping: what it deletes is past anything a trigger waits for (decision 486).
            match self
                .db
                .quietly(crate::retention::pass(
                    &self.db,
                    &settings,
                    (self.clock)(),
                    leading,
                    &self.metrics,
                ))
                .await
            {
                Ok(pass) if pass.any() => tracing::info!(
                    runs = pass.runs,
                    outputs = pass.outputs,
                    events = pass.events,
                    commands = pass.commands,
                    dags = pass.dags,
                    versions = pass.versions,
                    "retention deleted what [retention] no longer keeps"
                ),
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "retention failed; trying again at its next pass");
                }
            }
            tokio::select! {
                () = &mut stop => return,
                () = tokio::time::sleep(settings.interval) => {}
            }
        }
    }

    /// Samples each process a minute while this master leads (decision 464): its masters and
    /// workers, one row per process (`process_id`, this master's own, gets its webserver role
    /// with `with_webserver`), the rollups and deletes once a window closes, until `stop`
    /// completes. A failing pass is logged; the next one goes on.
    pub async fn run_samples(
        &self,
        settings: crate::samples::SampleSettings,
        (process_id, with_webserver): (String, bool),
        stop: impl Future<Output = ()>,
    ) {
        tokio::pin!(stop);
        let mut sampler = crate::samples::Sampler::default();
        let also_runs: std::collections::BTreeMap<String, String> = if with_webserver {
            std::collections::BTreeMap::from([("webserver".to_owned(), process_id.clone())])
        } else {
            std::collections::BTreeMap::new()
        };
        loop {
            tokio::select! {
                () = &mut stop => return,
                () = self.while_leading() => {}
            }
            let own = (!also_runs.is_empty()).then_some((process_id.as_str(), &also_runs));
            // Housekeeping: nothing a trigger depends on (decision 486).
            if let Err(error) = self
                .db
                .quietly(sampler.pass(&self.db, &settings, (self.clock)(), own))
                .await
            {
                tracing::warn!(%error, "sampling the processes failed; trying again in a minute");
            }
            tokio::select! {
                () = &mut stop => return,
                () = tokio::time::sleep(settings.interval) => {}
            }
        }
    }

    /// Starts the gauges' refresh in a task of its own, its count of every run kept never
    /// holding a round up (B270); one still counting is left to finish.
    fn refresh_in_background(&self) {
        if self.refreshing.swap(true, Ordering::SeqCst) {
            return;
        }
        let hub = self.clone();
        tokio::spawn(async move {
            // Cleared however the refresh ends, a panic too (agy's review of M5.2k): set for
            // good, the gauges would never refresh again.
            let _clear = ClearOnDrop(Arc::clone(&hub.refreshing));
            hub.refresh_gauges().await;
        });
    }

    /// Refreshes the gauges taken from the database; a failure is logged (the gauges keep
    /// their last values).
    pub async fn refresh_gauges(&self) {
        match tokio::time::timeout(self.refresh_limit, self.db.state_counts()).await {
            Ok(Ok(counts)) => self.metrics.set_counts(&counts),
            Ok(Err(error)) => tracing::warn!(%error, "can't count runs, tasks and workers"),
            Err(_) => tracing::warn!("counting runs, tasks and workers took too long; abandoned"),
        }
    }

    /// The core, for status and tests.
    #[must_use]
    pub fn core(&self) -> Arc<Mutex<MasterCore>> {
        Arc::clone(&self.core)
    }

    /// The gRPC service for a tonic server.
    #[must_use]
    pub fn service(&self) -> MasterServer<Self> {
        MasterServer::new(self.clone()).max_decoding_message_size(self.max_message_size)
    }

    /// The core's lock, the wait for it recorded by who asked (`by`: M5.1a's profile).
    async fn core_for(&self, by: &str) -> tokio::sync::MutexGuard<'_, MasterCore> {
        let asked = std::time::Instant::now();
        let core = self.core.lock().await;
        self.metrics.core_lock_waited(by, asked.elapsed());
        core
    }

    /// Reads the worker registry back (see [`MasterCore::restore`]) at the hub's clock, the
    /// one its lease checks use.
    ///
    /// # Errors
    ///
    /// The core's failure.
    pub async fn restore(&self) -> Result<usize, CoreError> {
        let now = (self.clock)();
        self.core_for("restore").await.restore(now).await
    }

    /// One tick of the master's loop, its messages delivered; its probe looks no time ahead.
    /// With the rounds a read skipped on a stale probe asks for (M5.2b), one after the other,
    /// as the loop runs them after its round gap: at most [`FOLLOW_UP_ROUNDS`] more. For tests
    /// only: the loop runs [`Self::tick_ahead`], one round, and its follow-ups after the gap.
    ///
    /// # Errors
    ///
    /// The core's failure (the next tick goes on).
    #[cfg(any(test, feature = "testing"))]
    pub async fn tick(&self) -> Result<(), CoreError> {
        for _ in 0..=FOLLOW_UP_ROUNDS {
            if !self.round(Duration::ZERO, true).await? {
                break;
            }
        }
        Ok(())
    }

    /// One round, as the tests' `tick` runs, its probe looking `ahead` (the loop's tick interval: what comes due
    /// before the next tick is found, so the round's parts share one probe, decision 485),
    /// and one round only: one that skipped a read on a stale probe wakes the loop for another
    /// after the round gap (M5.2b).
    ///
    /// # Errors
    ///
    /// The core's failure (the next tick goes on).
    pub async fn tick_ahead(&self, ahead: Duration) -> Result<(), CoreError> {
        if self.round(ahead, false).await? {
            // As for a stored `Finished`: another round after the gap, for what a write made
            // possible after the probe.
            self.finished.notify_one();
        }
        Ok(())
    }

    /// One round, looking `ahead`: whether it skipped a read on a probe a write had made
    /// stale (M5.2b), so another round should follow. Results, the Scheduler, Prepare and Assign
    /// run in every round; the slow parts (Commands, Expire, Rest) when `full`, or once `ahead`
    /// (the tick interval) has passed since they last ran, Commands also once a command was
    /// announced (M5.2cB4, the user's due flags). A failed round leaves them due.
    async fn round(&self, ahead: Duration, full: bool) -> Result<bool, CoreError> {
        let slow = full || self.slow_due(ahead);
        let commands = self.commands_due.swap(false, Ordering::SeqCst) || slow;
        let done = self.round_parts(ahead, slow, commands).await;
        if done.is_err() {
            if slow {
                *lock_slow(&self.slow_last) = None;
            }
            if commands {
                self.commands_due.store(true, Ordering::SeqCst);
            }
        }
        done
    }

    /// Whether the slow parts are due (a tick interval since they last ran, or never): if so,
    /// they count as run now.
    fn slow_due(&self, every: Duration) -> bool {
        let mut last = lock_slow(&self.slow_last);
        let due = last.is_none_or(|ran| ran.elapsed() >= every);
        if due {
            *last = Some(std::time::Instant::now());
        }
        due
    }

    /// The round's parts (see [`Self::round`]).
    async fn round_parts(
        &self,
        ahead: Duration,
        slow: bool,
        commands: bool,
    ) -> Result<bool, CoreError> {
        // The lock taken for each part on its own, so a worker's turn gets in between them
        // (decision 469), each part at the time it runs. What a part built before a failure is
        // delivered too (B7); a failed part ends the round, as before. The round's reads the
        // probe finds empty are skipped (decision 485): an idle tick is one statement. The probe
        // is taken once a round (M5.2b); a read skipped on it after a write wakes another round.
        let (result, stale) = Box::pin(self.db.in_round_noting((self.clock)(), ahead, async {
            // The trigger pass is the trigger loop's (decision 486): woken when a trigger's
            // stored moment has come, or its last pass failed.
            if self
                .triggers_failed
                .swap(false, std::sync::atomic::Ordering::SeqCst)
                || !self
                    .db
                    .skipped_at(schedr_db::probe::Check::TriggersDue, (self.clock)())
                    .await?
            {
                self.triggers_due.notify_one();
            }
            // The workers' results first: their runs moved forward here (M5.2cB), before any
            // other part (M5.2cB4).
            Box::pin(self.round_results((self.clock)())).await?;
            for part in RoundPart::ALL {
                match part {
                    RoundPart::Triggers => continue,
                    RoundPart::Commands => {
                        if commands {
                            self.commands().await?;
                        }
                        continue;
                    }
                    RoundPart::Expire | RoundPart::Rest if !slow => {
                        continue;
                    }
                    _ => {}
                }
                let mut outgoing = Vec::new();
                let now = (self.clock)();
                // The assign part's reads before the core lock (M5.2cA, decision 501): under
                // it only what the batch decides is checked again, and claimed.
                let reads = if part == RoundPart::Assign {
                    Some(AssignReads::read(&self.db, now, self.assignments_per_round).await?)
                } else {
                    None
                };
                let mut core = self.core_for("round").await;
                if let Some(reads) = reads {
                    core.give_assign_reads(reads);
                }
                let result = core.round_part(part, now, &mut outgoing).await;
                drop(core);
                self.deliver(outgoing);
                result?;
            }
            Ok(())
        }))
        .await;
        result.map(|()| stale)
    }

    /// The round's commands part, the core's lock taken for each command on its own (decision
    /// 483): each command is one transaction, so a worker's turn waits for one at most. Timed
    /// as the part's time under the lock.
    async fn commands(&self) -> Result<(), CoreError> {
        let mut held = std::time::Duration::ZERO;
        let mut outgoing = Vec::new();
        let pending = {
            let mut core = self.core_for("round").await;
            let began = std::time::Instant::now();
            let pending = core.commands_to_apply(&mut outgoing, (self.clock)()).await;
            held += began.elapsed();
            pending
        };
        self.deliver(std::mem::take(&mut outgoing));
        let mut failed = Ok(());
        for command in pending? {
            let applied = {
                let mut core = self.core_for("round").await;
                let began = std::time::Instant::now();
                let applied = core
                    .apply_command(command, &mut outgoing, (self.clock)())
                    .await;
                held += began.elapsed();
                applied
            };
            self.deliver(std::mem::take(&mut outgoing));
            if applied.is_err() {
                failed = applied;
                break;
            }
        }
        {
            let mut core = self.core_for("round").await;
            let began = std::time::Instant::now();
            core.commands_done(&mut outgoing, (self.clock)()).await;
            held += began.elapsed();
        }
        self.deliver(outgoing);
        self.metrics
            .round_part_done(RoundPart::Commands.name(), held);
        failed
    }

    /// Runs the master's loop: a tick every `interval` until `stop` completes, and at once when
    /// a new command is announced (`notifications`, Postgres; notifications that arrived
    /// meanwhile are taken by the same tick, and the tick interval starts again after it). A
    /// failing tick is logged; the next one goes on. When notifications fail (the database is
    /// unreachable), they are tried again after [`NOTIFICATION_RETRY`]; polling goes on.
    pub async fn run(
        &self,
        interval: Duration,
        notifications: Option<CommandNotifications>,
        stop: impl Future<Output = ()>,
    ) {
        let _ = self.run_with(interval, notifications, None, stop).await;
    }

    /// As [`Self::run`], leading by `leadership`'s lease (see [`crate::lease`]): renewed every
    /// third of its period by a task of its own (a long round doesn't hold renewals up); while
    /// it isn't fresh enough the master pauses (no rounds, worker messages wait) and resumes
    /// once renewed; it is given up when `stop` completes. The hub's core must write through
    /// [`Leadership::fenced_db`].
    ///
    /// # Errors
    ///
    /// [`LeaseLost`] when another master took it over.
    pub async fn run_leading(
        &self,
        interval: Duration,
        notifications: Option<CommandNotifications>,
        leadership: Leadership,
        stop: impl Future<Output = ()>,
    ) -> Result<(), LeaseLost> {
        self.run_with(interval, notifications, Some(leadership), stop)
            .await
    }

    /// Tries to listen for commands again: `Some` once decided (listening, or found off or
    /// behind a pooler), `None` to try later.
    async fn listen_again(&self) -> Option<Option<CommandNotifications>> {
        match self.db.command_notifications().await {
            Ok(started) => {
                if started.is_some() {
                    tracing::info!("listening for commands now");
                }
                self.problems.clear(ProblemKind::Notifications);
                Some(started)
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "still can't listen for commands; polling, trying again in {} min",
                    self.listen_retry.unwrap_or_default().as_secs() / 60
                );
                None
            }
        }
    }

    /// Command notifications failed with `error`: logged and reported as a problem; until
    /// the moment returned, the master polls.
    fn notifications_failed(&self, error: &DbError) -> tokio::time::Instant {
        tracing::warn!(
            %error,
            "command notifications failed; polling, trying again in {}s",
            self.timings.notification_retry.as_secs()
        );
        self.problems.set(
            ProblemKind::Notifications,
            false,
            format!("its command notifications failed ({error}): polling"),
        );
        tokio::time::Instant::now() + self.timings.notification_retry
    }

    async fn run_with(
        &self,
        interval: Duration,
        mut notifications: Option<CommandNotifications>,
        leadership: Option<Leadership>,
        stop: impl Future<Output = ()>,
    ) -> Result<(), LeaseLost> {
        let (release, released) = tokio::sync::oneshot::channel();
        let mut keeper = leadership.map(|leadership| {
            self.set_leading(leadership.leads(std::time::Instant::now()));
            let hub = self.clone();
            // The lease's writes are housekeeping (decision 486).
            tokio::spawn(async move {
                let db = hub.db.clone();
                db.quietly(hub.keep_leading(leadership, released)).await;
            })
        });
        // Ends with this loop, however it returns.
        let _triggers = self.start_triggers();
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut alive = tokio::time::interval(self.timings.alive);
        let mut refresh = tokio::time::interval(self.timings.gauge_refresh);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut paused_until: Option<tokio::time::Instant> = None;
        let mut last_round: Option<tokio::time::Instant> = None;
        // A round finished tasks asked for, held back by the gap after the last one: waited for
        // here, so a stop or a lost lease doesn't wait for it.
        let mut round_due: Option<tokio::time::Instant> = None;
        let mut listen_again = self
            .listen_retry
            .filter(|_| notifications.is_none())
            .map(|period| tokio::time::interval_at(tokio::time::Instant::now() + period, period));
        tokio::pin!(stop);
        loop {
            let wake = tokio::select! {
                () = &mut stop => Wake::Stop,
                _ = ticks.tick() => Wake::Tick,
                () = self.finished.notified() => Wake::Finished,
                () = async { tokio::time::sleep_until(round_due.expect("guarded")).await },
                    if round_due.is_some() => Wake::RoundDue,
                listened = next_command(
                    notifications.as_mut(), &mut paused_until, &self.problems, &self.triggers_due,
                ) => {
                    match listened {
                        Listened::Command => Wake::Command,
                        Listened::Failed(error) => Wake::NotificationsFailed(error),
                    }
                }
                _ = alive.tick() => Wake::Alive,
                _ = refresh.tick() => Wake::Refresh,
                _ = async { keeper.as_mut().expect("guarded").await }, if keeper.is_some() => {
                    Wake::LeaseLost
                }
                _ = async { listen_again.as_mut().expect("guarded").tick().await },
                    if listen_again.is_some() => Wake::ListenAgain,
            };
            self.beat.beat();
            let leads = *self.leading.borrow();
            match wake {
                Wake::Stop => {
                    let _ = release.send(());
                    if let Some(keeper) = keeper {
                        // It gives the lease up.
                        let _ = keeper.await;
                    }
                    return Ok(());
                }
                Wake::LeaseLost => return Err(LeaseLost),
                Wake::Alive => continue,
                Wake::ListenAgain => {
                    if let Some(started) = self.listen_again().await {
                        notifications = started;
                        listen_again = None;
                    }
                    continue;
                }
                // Paused: no rounds until the lease is renewed.
                Wake::Tick | Wake::Command | Wake::Finished | Wake::RoundDue if !leads => {
                    round_due = None;
                    continue;
                }
                Wake::Refresh => {
                    self.refresh_in_background();
                    continue;
                }
                Wake::Command => {
                    paused_until = None;
                    self.commands_due.store(true, Ordering::SeqCst);
                    self.command_announced(notifications.as_mut()).await;
                    ticks.reset();
                }
                Wake::NotificationsFailed(error) => {
                    paused_until = Some(self.notifications_failed(&error));
                    continue;
                }
                Wake::Finished => {
                    // At most one round every TASK_ROUND_GAP: under a stream of finishes each
                    // round takes what arrived meanwhile, and the lease's renewals and the
                    // workers' messages get their turns.
                    let due = last_round.map(|ended| ended + self.timings.task_round_gap);
                    if let Some(due) = due.filter(|due| *due > tokio::time::Instant::now()) {
                        round_due = Some(due);
                        continue;
                    }
                    ticks.reset();
                }
                Wake::RoundDue => ticks.reset(),
                Wake::Tick => {}
            }
            self.timed_tick(interval).await;
            round_due = None;
            last_round = Some(tokio::time::Instant::now());
        }
    }

    /// A command was announced: notifications work again, and those already arrived are taken
    /// with it (one round for a burst); an event among them wakes the trigger loop.
    async fn command_announced(&self, notifications: Option<&mut CommandNotifications>) {
        self.problems.clear(ProblemKind::Notifications);
        if let Some(notifications) = notifications
            && notifications.skip_arrived().await
        {
            self.triggers_due.notify_one();
        }
    }

    /// Starts the trigger loop beside the master's loop: it stops when what this returns is
    /// dropped.
    fn start_triggers(&self) -> impl Drop {
        let (stop, stopped) = watch::channel(false);
        let hub = self.clone();
        let task = tokio::spawn(async move { Box::pin(hub.run_triggers(stopped)).await });
        scopeguard_stop(stop, task)
    }

    /// The trigger loop (decisions 485 and 486): the trigger pass at once, then again whenever
    /// a write ended elsewhere in this process, a trigger's stored moment came (the tick's
    /// probe) or a named event was announced, at most once every [`ROUND_GAP`] and only while
    /// this master leads, until `stop` turns true. A pass is skipped by one `EXISTS` when no
    /// trigger it looks after is switched on or running; its own writes wake nothing of it, and
    /// a pass that wrote (a run queued) wakes the round, as a recorded `Finished` does.
    async fn run_triggers(&self, mut stop: watch::Receiver<bool>) {
        let mut last: Option<tokio::time::Instant> = None;
        loop {
            if let Some(last) = last {
                tokio::select! {
                    _ = stop.changed() => return,
                    () = tokio::time::sleep_until(last + self.timings.round_gap) => {}
                }
            }
            if *stop.borrow() {
                return;
            }
            if *self.leading.borrow() {
                Box::pin(self.trigger_pass()).await;
                last = Some(tokio::time::Instant::now());
            }
            loop {
                tokio::select! {
                    _ = stop.changed() => return,
                    () = self.db.changed() => break,
                    () = self.triggers_due.notified() => break,
                    // Steps a post in this process woke are the round's (decision 487): no
                    // pass for them.
                    () = self.db.waits_due() => self.finished.notify_one(),
                    // Leading again after a pause: what changed meanwhile is looked at.
                    () = async {
                        let mut leading = self.leading.subscribe();
                        let _ = leading.wait_for(|leads| *leads).await;
                    }, if !*self.leading.borrow() => break,
                }
            }
        }
    }

    /// One pass of the trigger loop, its messages delivered.
    async fn trigger_pass(&self) {
        let now = (self.clock)();
        match self.db.exists(schedr_db::probe::Check::Triggers, now).await {
            Ok(false) => return,
            Ok(true) => {}
            Err(error) => {
                tracing::warn!(%error, "the trigger pass failed; trying again at the next tick");
                self.triggers_failed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        }
        let before = self.db.writes_ended();
        let mut outgoing = Vec::new();
        let result = self
            .db
            .quietly(async {
                Box::pin(self.core_for("triggers").await.round_part(
                    RoundPart::Triggers,
                    (self.clock)(),
                    &mut outgoing,
                ))
                .await
            })
            .await;
        self.deliver(outgoing);
        if self.db.writes_ended() != before {
            // A run queued, a trigger started or stopped: the round starts what it can.
            self.finished.notify_one();
        }
        if let Err(error) = result {
            tracing::warn!(%error, "the trigger pass failed; trying again at the next tick");
            self.triggers_failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// A round, its duration observed.
    async fn timed_tick(&self, ahead: Duration) {
        let started = std::time::Instant::now();
        match self.tick_ahead(ahead).await {
            Ok(()) => self.problems.clear(ProblemKind::Round),
            Err(error) => {
                tracing::error!(%error, "master tick failed");
                self.problems
                    .set(ProblemKind::Round, true, format!("a round failed: {error}"));
            }
        }
        self.metrics
            .tick_duration
            .observe(started.elapsed().as_secs_f64());
    }

    /// Renews `leadership` every third of its period, pausing the master when its last
    /// renewal is no longer recent enough and resuming it when renewed, until another master
    /// took the lease over (returned then) or `give_up` (the lease given up, a standby takes
    /// over at once).
    async fn keep_leading(
        &self,
        mut leadership: Leadership,
        mut give_up: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut renew = tokio::time::interval(leadership.period() / 3);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        renew.reset();
        loop {
            let until = tokio::time::Instant::from_std(leadership.until());
            let leading = *self.leading.borrow();
            tokio::select! {
                biased;
                _ = &mut give_up => {
                    leadership.release().await;
                    // Not leading from now on: a stream offered while stopping is refused, and
                    // no heartbeat is answered (B200).
                    self.set_leading(false);
                    return;
                }
                _ = renew.tick() => {
                    if leadership.renew(std::time::Instant::now()).await == Renewal::TakenOver {
                        self.set_leading(false);
                        tracing::error!("{LeaseLost}");
                        return;
                    }
                }
                () = tokio::time::sleep_until(until), if leading => {}
            }
            self.set_leading(leadership.leads(std::time::Instant::now()));
        }
    }

    /// Sends messages to their workers' streams; one for a worker without a live stream is
    /// dropped (an assignment then recorded as never sent, so it is taken back without a retry
    /// once the worker registers again or is lost; a cancel is sent again when the worker
    /// comes back and reports the attempt in its journal).
    ///
    /// A send never waits: a worker [`STREAM_BUFFER`] messages behind has stopped reading.
    /// Its stream is dropped and the worker disconnected, so it registers again and its
    /// journal is reconciled; it never holds up the master's rounds, registrations or other
    /// workers' messages, nor its messages pile up.
    ///
    /// Each is stamped with the master's clock as it is sent (B225).
    fn deliver(&self, outgoing: Vec<Outgoing>) {
        let mut stuck = Vec::new();
        for Outgoing {
            worker_id,
            mut message,
        } in outgoing
        {
            let assigned = match &message.message {
                Some(master_message::Message::AssignTask(assign)) => {
                    Some(assign.attempt_id.clone())
                }
                _ => None,
            };
            // An assignment that doesn't leave never started there (Grok's review of B240).
            let unsent = |attempt: Option<&String>| {
                if let Some(attempt_id) = attempt {
                    self.arrivals.unsent(&worker_id, attempt_id);
                }
            };
            let Some(stream) = self.sessions.current(&worker_id) else {
                tracing::warn!(worker_id, "no stream for worker; message dropped");
                unsent(assigned.as_ref());
                continue;
            };
            message.clock = Some(self.stamp(stream.heard()));
            match stream.send(message) {
                Ok(number) => {
                    if let Some(attempt_id) = assigned {
                        self.arrivals
                            .assigned(&worker_id, stream.id(), &attempt_id, number);
                    }
                }
                Err(mpsc::error::TrySendError::Full(())) => {
                    unsent(assigned.as_ref());
                    tracing::error!(
                        worker_id,
                        "worker doesn't read its stream ({} messages waiting): \
                         disconnected",
                        self.stream_buffer
                    );
                    // Gone for every reader at once; the core takes it out (under its lock).
                    stream.mark_stuck();
                    stuck.push((worker_id, stream.id()));
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    unsent(assigned.as_ref());
                    tracing::warn!(worker_id, "worker stream closed; message dropped");
                }
            }
        }
        for (worker_id, id) in stuck {
            // Not here: the caller may hold the core's lock.
            tokio::spawn(self.clone().disconnect_stuck(worker_id, id));
        }
    }

    /// The master's clock to stamp on a message (B225), `heard` the latest heartbeat heard on
    /// the worker's stream.
    fn stamp(&self, heard: u64) -> pb::MasterClock {
        stamp_of((self.clock, self.steady), heard, self.epoch)
    }

    /// Takes out a worker's session found full (stuck, for not being read) and disconnects the
    /// worker, unless it has registered again since (another session, `id` no longer its). (Its
    /// stream's reader stops at its next message, which ends the worker's side.)
    async fn disconnect_stuck(self, worker_id: String, id: u64) {
        let mut core = self.core_for("stream").await;
        if core.sessions_mut().remove(&worker_id, Some(id)).is_none() {
            return;
        }
        if let Err(error) = core.disconnected(&worker_id, (self.clock)()).await {
            tracing::error!(worker_id, %error, "worker disconnect not recorded");
        }
    }

    /// A worker's message handled; a task's API call checked here and run outside the lock (a
    /// request may take a while; M4.11h), its answer delivered when it comes.
    async fn handle(
        &self,
        core: &mut MasterCore,
        (worker_id, in_round): (&str, bool),
        message: worker_message::Message,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let worker_message::Message::ApiCall(call) = message else {
            // The round's: what moves runs forward too (M5.2cB).
            return if in_round {
                core.handle_in_round(worker_id, message, (self.clock)())
                    .await
            } else {
                core.handle(worker_id, message, (self.clock)()).await
            };
        };
        match core.plan_api_call(worker_id, call, (self.clock)()).await? {
            ApiCallPlan::Answered(answer) => Ok(vec![*answer]),
            ApiCallPlan::Run(job) => {
                let hub = self.clone();
                tokio::spawn(async move {
                    let answer = job.run().await;
                    hub.deliver(vec![answer]);
                });
                Ok(Vec::new())
            }
        }
    }

    /// The worker's next message, once this master leads (not while paused: another master
    /// may be leading), unless the master closes its streams first.
    async fn next_to_handle(
        &self,
        queued: &mut mpsc::UnboundedReceiver<worker_message::Message>,
        (closing, replaced): (&mut watch::Receiver<bool>, &mut watch::Receiver<bool>),
    ) -> Next {
        let next = async {
            let Some(message) = queued.recv().await else {
                return Next::Ended;
            };
            self.while_leading().await;
            Next::Message(Box::new(message))
        };
        // Its end first: a replaced stream's messages still queued count for nothing.
        tokio::select! {
            biased;
            _ = closing.wait_for(|closing| *closing) => Next::Closing,
            _ = replaced.wait_for(|replaced| *replaced) => Next::Replaced,
            next = next => next,
        }
    }

    /// One turn of a worker's messages: its `Started`s and results queued for the round, which
    /// handles them (M5.2cB, decision 505); heartbeats and warnings after them, each alone; any
    /// other message ends the stretch (what came before it queued first) and is handled alone,
    /// or queued behind the worker's results still waiting ([`Self::alone_served`]). The core's
    /// lock is taken for each message on its own and the stream checked again each time, so
    /// rounds and other workers' turns get in between (decision 480).
    async fn turn(
        &self,
        (worker_id, stream): (&str, &Arc<Session>),
        turn: Vec<worker_message::Message>,
    ) -> TurnEnd {
        let mut results = Vec::new();
        let mut after = Vec::new();
        let mut messages = turn.into_iter().peekable();
        loop {
            let barrier = match messages.next() {
                Some(message) if is_result(&message) => {
                    results.push(message);
                    continue;
                }
                Some(
                    message @ (worker_message::Message::Heartbeat(_)
                    | worker_message::Message::Warnings(_)),
                ) => {
                    after.push(message);
                    continue;
                }
                other => other,
            };
            self.queue_results(worker_id, stream, &mut results);
            // A trigger's message timed as its own part (M5.2f's scenario: what triggers cost).
            let trigger = barrier.as_ref().is_some_and(is_trigger_message);
            let rest = after.len() + usize::from(barrier.is_some() && !trigger);
            let mut held = std::time::Duration::ZERO;
            for message in after.drain(..) {
                let (end, took) = self.alone_served(worker_id, stream, message).await;
                held += took;
                if !matches!(end, TurnEnd::Goes) {
                    self.metrics.turn_part_done("rest", rest, held);
                    return end;
                }
            }
            let end = match barrier {
                // The trigger messages that follow it, up to the first other one, with it: one
                // transaction for them all (M5.2h).
                Some(message) if trigger => {
                    let mut group = vec![message];
                    while let Some(next) = messages.next_if(is_trigger_message) {
                        group.push(next);
                    }
                    let count = group.len();
                    let (end, took) =
                        Box::pin(self.triggers_served(worker_id, stream, group)).await;
                    self.metrics.turn_part_done("triggers", count, took);
                    Some(end)
                }
                Some(message) => {
                    let (end, took) = self.alone_served(worker_id, stream, message).await;
                    held += took;
                    Some(end)
                }
                None => None,
            };
            if rest > 0 {
                self.metrics.turn_part_done("rest", rest, held);
            }
            match end {
                // The barrier handled: on to the messages after it.
                Some(TurnEnd::Goes) => {}
                Some(end) => return end,
                None => return TurnEnd::Goes,
            }
        }
    }

    /// A turn's results, queued for the next round, which is woken (M5.2cB, decision 505).
    fn queue_results(
        &self,
        worker_id: &str,
        stream: &Arc<Session>,
        results: &mut Vec<worker_message::Message>,
    ) {
        if results.is_empty() {
            return;
        }
        let queued = results.drain(..).map(|message| QueuedResult {
            worker_id: worker_id.to_owned(),
            stream: stream.clone(),
            message: Some(message),
        });
        self.enqueue(|queue| queue.extend(queued));
    }

    /// The one way into the round's queue: `change` applied under the queue's lock, unless the
    /// master closes (nothing more queued: the queue holding a stream's session would keep the
    /// stream open, Codex's review of M5.2c, agy's review of M5.3i). Asked under the queue's
    /// lock, which `close_streams` empties it under after setting it: one or the other. The
    /// round woken. Whether it was queued.
    fn enqueue(&self, change: impl FnOnce(&mut Vec<QueuedResult>)) -> bool {
        let mut queue = self.queued_results();
        if *self.closing.borrow() {
            return false;
        }
        change(&mut queue);
        drop(queue);
        self.finished.notify_one();
        true
    }

    fn queued_results(&self) -> std::sync::MutexGuard<'_, Vec<QueuedResult>> {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether `worker_id` has results waiting for the round: its later messages wait behind
    /// them (M5.2cB), so a `Deregister` never overtakes the `Finished` sent before it. Asked
    /// under the core's lock, which the round takes the queue under.
    fn results_waiting(&self, worker_id: &str) -> bool {
        self.queued_results()
            .iter()
            .any(|r| r.worker_id == worker_id)
    }

    /// The round's results part (M5.2cB, decision 505): the workers' `Started`s and results
    /// queued since the last round, in the order they arrived. Each run of `Started`s and
    /// `Finished`s, whatever their worker, is one transaction ([`MasterCore::handle_results`]);
    /// any other message is handled alone after the ones before it. One from a replaced
    /// stream, or a worker taken for lost, changes nothing (B44): the worker sends it again.
    /// What they send goes out after the lock.
    async fn round_results(&self, now: Timestamp) -> Result<(), CoreError> {
        if self.queued_results().is_empty() {
            return Ok(());
        }
        // The queued `Started`s' and `Finished`s' attempts and runs read before the lock
        // (M5.2cB3): true until the batch writes, only the round moving runs forward. One
        // queued after this is read under the lock.
        let ids: Vec<String> = self
            .queued_results()
            .iter()
            .filter_map(|r| match &r.message {
                Some(worker_message::Message::Started(s)) => Some(s.attempt_id.clone()),
                Some(worker_message::Message::Finished(f)) => Some(f.attempt_id.clone()),
                _ => None,
            })
            .collect();
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        // A failed read changes nothing: the batch reads under the lock instead (Codex's
        // review of M5.2c: a message the read fails on, a malformed ID, would else stop every
        // round before it is taken off the queue).
        let reads = match crate::dispatch::ResultsReads::read(&self.db, &ids).await {
            Ok(reads) => reads,
            Err(error) => {
                tracing::warn!(%error, "the round's results read before the lock failed");
                crate::dispatch::ResultsReads::default()
            }
        };
        // Taken under the lock: a turn finds a worker's results still queued, or handled.
        let mut core = self.core_for("round").await;
        core.give_results_reads(reads);
        let mut queued: VecDeque<QueuedResult> = std::mem::take(&mut *self.queued_results()).into();
        let began = std::time::Instant::now();
        let mut handling = Handling {
            outgoing: Vec::new(),
            alone_left: self.alone_per_turn,
            back: Vec::new(),
            ended: BTreeSet::new(),
        };
        let mut batch: Vec<Stretch> = Vec::new();
        while let Some(QueuedResult {
            worker_id,
            stream,
            message,
        }) = queued.pop_front()
        {
            if !handling.back.is_empty() {
                handling.back.push(QueuedResult {
                    worker_id,
                    stream,
                    message,
                });
                continue;
            }
            // A replaced stream's, or a lost worker's, changes nothing (B44); one that has
            // ended since still counts (it said this before its end). After one of its messages
            // ended its stream this round, nothing more of the worker's counts.
            if handling.ended.contains(&worker_id)
                || !self.queued_served(&core, &worker_id, &stream)
            {
                // Its start, if any, stays: its worker's registration or loss forgets it, and a
                // newer stream's report of the same start isn't this one's to erase.
                continue;
            }
            match message {
                // Its stream ended after the rest: the worker disconnected, in order.
                None => {
                    let ended = std::mem::take(&mut batch);
                    Box::pin(self.results_batch(&mut core, ended, now, &mut handling)).await;
                    if !handling.back.is_empty() {
                        handling.back.push(QueuedResult {
                            worker_id,
                            stream,
                            message: None,
                        });
                    } else if handling.ended.contains(&worker_id) {
                        // Ended by a message of its own in the flush: disconnected already.
                    } else if let Err(error) = core.disconnected(&worker_id, now).await {
                        tracing::error!(worker_id, %error, "worker disconnect not recorded");
                    }
                }
                Some(worker_message::Message::Started(started)) => {
                    stretch_of(&mut batch, &worker_id, &stream)
                        .started
                        .push(started);
                }
                Some(worker_message::Message::Finished(finished)) => {
                    stretch_of(&mut batch, &worker_id, &stream)
                        .finished
                        .push(finished);
                }
                Some(other) => {
                    let ended = std::mem::take(&mut batch);
                    Box::pin(self.results_batch(&mut core, ended, now, &mut handling)).await;
                    self.result_alone(&mut core, (&worker_id, &stream), other, &mut handling)
                        .await;
                }
            }
        }
        Box::pin(self.results_batch(&mut core, batch, now, &mut handling)).await;
        core.drop_results_reads();
        self.metrics.round_part_done("results", began.elapsed());
        if !handling.back.is_empty() {
            // What a failed batch left unhandled: first in the next round, which is woken.
            let back = std::mem::take(&mut handling.back);
            self.enqueue(|queue| {
                let later = std::mem::take(queue);
                queue.extend(back);
                queue.extend(later);
            });
        }
        drop(core);
        self.deliver(handling.outgoing);
        Ok(())
    }

    /// A round's run of `Started`s and `Finished`s in one transaction; failing, each worker's
    /// stretch as a group, then message by message, at most [`ALONE_PER_TURN`] alone a round
    /// (by default): the rest go back, for the next round.
    async fn results_batch(
        &self,
        core: &mut MasterCore,
        batch: Vec<Stretch>,
        now: Timestamp,
        handling: &mut Handling,
    ) {
        if batch.is_empty() {
            return;
        }
        let (streams, groups): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .map(|s| ((s.worker_id.clone(), s.stream), (s.worker_id, s.group)))
            .unzip();
        let failed = match core.handle_results(groups, now).await {
            Ok(out) => {
                handling.outgoing.extend(out);
                return;
            }
            Err(failed) => failed,
        };
        for ((worker_id, group), (_, stream)) in failed.into_iter().zip(streams) {
            if !handling.back.is_empty() {
                handling.put_back(&worker_id, &stream, group.into_messages());
                continue;
            }
            match core.handle_group(&worker_id, group, now).await {
                Ok(out) => handling.outgoing.extend(out),
                Err(group) => {
                    for message in group.into_messages() {
                        self.result_alone(core, (&worker_id, &stream), message, handling)
                            .await;
                    }
                }
            }
        }
    }

    /// One message handled alone in the round, as a turn handles one ([`Self::alone`]): one
    /// that can't be handled ends its worker's stream, so it registers again (B101). Past the
    /// round's share of them, or after one put back, it goes back too.
    async fn result_alone(
        &self,
        core: &mut MasterCore,
        (worker_id, stream): (&str, &Arc<Session>),
        message: worker_message::Message,
        handling: &mut Handling,
    ) {
        // After one of its messages ended its stream, nothing more of the worker's counts
        // (Codex's review of M5.2c: a fallback's later messages, a barrier after a flush).
        if handling.ended.contains(worker_id) {
            return;
        }
        if !handling.back.is_empty() || handling.alone_left == 0 {
            handling.put_back(worker_id, stream, vec![message]);
            return;
        }
        handling.alone_left -= 1;
        // What was read before the lock is no longer true once this writes (Codex's review of
        // M5.2c: a `Deregister` before a batch of the same run's results).
        core.drop_results_reads();
        let mut done = TurnDone::default();
        if !self
            .alone(core, (worker_id, true), message, &mut done)
            .await
            || done.unhandled
        {
            // As a turn's end then: the stream let go and the worker disconnected, so it
            // registers again; what it queued after this is dropped, and sent again (B101).
            core.sessions_mut().remove(worker_id, Some(stream.id()));
            if let Err(error) = core.disconnected(worker_id, (self.clock)()).await {
                tracing::error!(worker_id, %error, "worker disconnect not recorded");
            }
            handling.ended.insert(worker_id.to_owned());
        }
        handling.outgoing.extend(done.outgoing);
    }

    /// Whether a queued message still counts when the round comes to it (under the core's
    /// lock): not once a newer stream of its worker is in place (B44), nor once the worker was
    /// taken for lost (its lease) while it waited, the stream then let go, as a turn would. Its
    /// stream having ended since is no reason: what it said came before its end, which waits
    /// behind it (M5.2cB).
    fn queued_served(&self, core: &MasterCore, worker_id: &str, stream: &Arc<Session>) -> bool {
        let current = self.sessions.current(worker_id);
        // A replaced stream's: another in place, or, the newer one ended too (its end waiting
        // in the queue), another registered after it (Codex's review of M5.2c).
        if current.as_ref().is_some_and(|s| s.id() != stream.id())
            || (current.is_none() && self.arrivals.latest_stream(worker_id) != Some(stream.id()))
        {
            tracing::warn!(worker_id, "a queued message of a replaced stream dropped");
            return false;
        }
        // Not live, it has no session (`MasterCore::not_live`, its loss): nothing to let go.
        if !core.is_live(worker_id) {
            tracing::warn!(
                worker_id,
                "a queued message of a worker taken for lost dropped"
            );
            return false;
        }
        true
    }

    /// A stream's end, under the core's lock: the worker disconnected now, or once the round has
    /// handled what it queued before its end (M5.2cB), so that, a `Deregister`, still counts.
    async fn stream_ended(&self, core: &mut MasterCore, worker_id: &str, stream: &Arc<Session>) {
        // Behind its results, under one look at the queue; closing, nothing queued and nothing
        // written (the master is stopping: its lease given back).
        let mut behind = false;
        let open = self.enqueue(|queue| {
            behind = queue.iter().any(|r| r.worker_id == worker_id);
            if behind {
                queue.push(QueuedResult {
                    worker_id: worker_id.to_owned(),
                    stream: stream.clone(),
                    message: None,
                });
            }
        });
        if !open || behind {
            return;
        }
        if let Err(error) = core.disconnected(worker_id, (self.clock)()).await {
            tracing::error!(worker_id, %error, "worker disconnect not recorded");
        }
    }

    async fn served_core(
        &self,
        worker_id: &str,
        stream: &Arc<Session>,
    ) -> Option<tokio::sync::MutexGuard<'_, MasterCore>> {
        let core = self.core_for("turn").await;
        self.still_served(&core, worker_id, stream).then_some(core)
    }

    /// `message` handled alone, under the core's lock taken for it ([`Self::alone`]), what it
    /// sends delivered after the lock is let go; with how long the lock was held.
    async fn alone_served(
        &self,
        worker_id: &str,
        stream: &Arc<Session>,
        message: worker_message::Message,
    ) -> (TurnEnd, std::time::Duration) {
        let Some(mut core) = self.served_core(worker_id, stream).await else {
            return (TurnEnd::Dropped, std::time::Duration::ZERO);
        };
        // A trigger's message doesn't wait either: nothing of it depends on the worker's
        // results, and a heartbeat carrying pokes each second outran the round's ten a round
        // behind them, the results waiting past their resends (B268).
        if self.results_waiting(worker_id)
            && !matches!(
                message,
                worker_message::Message::Heartbeat(_) | worker_message::Message::Warnings(_)
            )
            && !is_trigger_message(&message)
        {
            drop(core);
            self.queue_results(worker_id, stream, &mut vec![message]);
            return (TurnEnd::Goes, std::time::Duration::ZERO);
        }
        let began = std::time::Instant::now();
        let mut done = TurnDone::default();
        let goes_on = self
            .alone(&mut core, (worker_id, false), message, &mut done)
            .await;
        let took = began.elapsed();
        drop(core);
        self.sent(done.outgoing, done.finished);
        let end = if goes_on && !done.unhandled {
            TurnEnd::Goes
        } else {
            TurnEnd::Unhandled
        };
        (end, took)
    }

    /// A worker's trigger messages that came one after another (M5.2h), one or more: one
    /// transaction for them all under the core's lock ([`MasterCore::handle_triggers`]), where
    /// a message's writes were each their own; when that fails, each on its own as before
    /// ([`Self::alone_served`]). Like any trigger message, they don't wait behind the worker's
    /// results (B268).
    async fn triggers_served(
        &self,
        worker_id: &str,
        stream: &Arc<Session>,
        group: Vec<worker_message::Message>,
    ) -> (TurnEnd, std::time::Duration) {
        let Some(mut core) = self.served_core(worker_id, stream).await else {
            return (TurnEnd::Dropped, std::time::Duration::ZERO);
        };
        let began = std::time::Instant::now();
        let handled = core.handle_triggers(worker_id, group, (self.clock)()).await;
        let mut took = began.elapsed();
        drop(core);
        let group = match handled {
            Ok(outgoing) => {
                self.sent(outgoing, false);
                return (TurnEnd::Goes, took);
            }
            Err(crate::core::TriggersFailed::Again(group)) => group,
            Err(crate::core::TriggersFailed::Unknown(error)) => {
                // As one message alone failing so: not sent again on this stream, so the
                // worker registers again and sends what wasn't acknowledged.
                tracing::error!(
                    worker_id,
                    %error,
                    "a turn's trigger messages may not be stored: its stream is ended, so the \
                     worker registers again"
                );
                return (TurnEnd::Unhandled, took);
            }
        };
        for message in group {
            let (end, one) = self.alone_served(worker_id, stream, message).await;
            took += one;
            if !matches!(end, TurnEnd::Goes) {
                return (end, took);
            }
        }
        (TurnEnd::Goes, took)
    }

    /// Delivers what a transaction of a turn sends, waking a round if it stored a `Finished`.
    fn sent(&self, outgoing: Vec<Outgoing>, finished: bool) {
        if finished {
            self.finished.notify_one();
        }
        self.deliver(outgoing);
    }

    /// One message handled on its own, as every message was before M4B.3; whether the turn
    /// goes on (not: the stream ends).
    async fn alone(
        &self,
        core: &mut MasterCore,
        (worker_id, in_round): (&str, bool),
        message: worker_message::Message,
        done: &mut TurnDone,
    ) -> bool {
        let is_finished = matches!(message, worker_message::Message::Finished(_));
        // A statement cut off at `query_timeout` is tried once more at once, a fresh one (B263:
        // a master frozen in a statement counts the stall on its thaw, and the try again finds
        // the database as it is): a second ends the stream as any database failure does
        // (decision 115). Safe: the cut-off transaction is rolled back, and a message handled
        // again is what a reconnect's resending already does (B101).
        let again = message.clone();
        let handled = match self.handle(core, (worker_id, in_round), message).await {
            Err(error) if error.is_query_timeout() => {
                tracing::warn!(
                    worker_id,
                    %error,
                    "a worker message's statement ran past its limit: tried once more"
                );
                self.handle(core, (worker_id, in_round), again).await
            }
            other => other,
        };
        match handled {
            // A round wakes for a `Finished` recorded, not one dropped or whose handling ended
            // the stream (B191, decision 42).
            Ok(out) => {
                done.finished |= is_finished;
                done.outgoing.extend(out);
                true
            }
            // Refused for its data, it would be refused again after a reconnect, and the
            // worker's later messages with it: dropped, the stream goes on (B106; a `Finished`
            // is stored as the attempt's failure instead).
            Err(error) if error.is_data_fault() => {
                tracing::error!(
                    worker_id,
                    %error,
                    "worker message dropped: the database refuses its data"
                );
                true
            }
            Err(error) => {
                // Not sent again on this stream (a code report is sent once): ended, so the
                // worker registers again and reports afresh (B101).
                tracing::error!(
                    worker_id,
                    %error,
                    "worker message not handled: its stream is ended, so the worker registers \
                     again"
                );
                done.unhandled = true;
                false
            }
        }
    }

    /// Whether `stream` still carries its worker's messages (under the core's lock); not, it
    /// is let go.
    fn still_served(&self, core: &MasterCore, worker_id: &str, stream: &Arc<Session>) -> bool {
        if !self.sessions.is_current(stream) {
            // Replaced by a newer stream of the worker: what this one still says is out of
            // date, and must not change the newer one's state (B44).
            tracing::warn!(worker_id, "messages of a replaced stream dropped");
            return false;
        }
        if !core.is_live(worker_id) {
            // The worker was taken for lost (its lease) while this stream stayed open: ended,
            // so it registers again and its journal is reconciled; meanwhile its messages
            // change nothing. Its session went with its loss (`MasterCore::not_live`).
            tracing::warn!(worker_id, "stream of a worker taken for lost ended");
            return false;
        }
        true
    }

    async fn serve_stream(
        self,
        worker_id: String,
        stream: Arc<Session>,
        inbound: Streaming<pb::WorkerMessage>,
        has_queue: bool,
    ) {
        // Read apart from handling: while the master is stuck behind its lock, the worker's
        // messages still arrive, are recorded and wait in order (B6).
        let (queue, mut queued) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read_stream(
            worker_id.clone(),
            inbound,
            self.arrivals.clone(),
            queue,
            HeartbeatAcks {
                session: Arc::downgrade(&stream),
                stream_id: stream.id(),
                leading: self.leading.subscribe(),
                clocks: (self.clock, self.steady),
                epoch: self.epoch,
                sessions: self.sessions.clone(),
                slow_equals: (Arc::clone(&self.slow_equals), has_queue),
            },
        ));
        let mut closing = self.closing.subscribe();
        let mut replaced = stream.ended();
        loop {
            let first = match self
                .next_to_handle(&mut queued, (&mut closing, &mut replaced))
                .await
            {
                Next::Message(first) => *first,
                Next::Ended => break,
                // The master stopping: this stream's sender let go, its worker not written
                // as disconnected (the lease is given back), as the master's end. Or its
                // worker's newer stream in place: this one's end changes nothing.
                Next::Closing | Next::Replaced => {
                    reader.abort();
                    return;
                }
            };
            // Everything already queued, up to the turn's limit, in one turn of the lock: one
            // message per turn left a worker's messages behind every round in between (B12).
            let turn = fill_turn(first, &mut queued, self.messages_per_turn);
            match Box::pin(self.turn((&worker_id, &stream), turn)).await {
                TurnEnd::Goes => {}
                TurnEnd::Dropped => {
                    reader.abort();
                    return;
                }
                TurnEnd::Unhandled => {
                    reader.abort();
                    break;
                }
            }
        }
        if let Err(error) = reader.await
            && !error.is_cancelled()
        {
            tracing::error!(worker_id, %error, "worker stream reader failed");
        }
        // Only the worker's current stream ending disconnects it: a replaced one's end says
        // nothing of the newer stream (B44). Checked and removed under the core's lock, which
        // a registration holds while it puts its stream in place.
        let mut core = self.core_for("stream").await;
        if core
            .sessions_mut()
            .remove(&worker_id, Some(stream.id()))
            .is_none()
        {
            return;
        }
        self.stream_ended(&mut core, &worker_id, &stream).await;
    }

    /// A newer connection of a live worker, or one whose stream is gone (B267), puts its old
    /// stream aside before registering: it is ended and the worker disconnected (decision 525).
    async fn replace_stream(
        &self,
        core: &mut MasterCore,
        register: &pb::Register,
    ) -> Result<(), Status> {
        // Only a registration the core would accept (agy's review of M5.3g: a refused one tore the
        // live stream down). One look at the streams: present or gone, and the old one taken
        // out. Not while the master stops: its streams emptied then are no worker's gone stream.
        let admissible = core.check(register).is_ok();
        // Refused, not registered after all, while the master stops (agy's third review of
        // M5.3g): its sessions are being closed.
        if !*self.leading.borrow() || *self.closing.borrow() || self.sessions.is_closing() {
            return Err(Status::unavailable(NOT_LEADING));
        }
        let gone = !self.sessions.has(&register.worker_id);
        let replacing =
            admissible && core.is_live(&register.worker_id) && (core.replaces(register) || gone);
        if replacing
            && core
                .sessions_mut()
                .replace_out(&register.worker_id)
                .is_some()
        {
            tracing::warn!(
                worker_id = register.worker_id,
                connection = register.connection,
                "a newer connection of the worker replaces its stream"
            );
        }
        if replacing
            && let Err(error) = core.disconnected(&register.worker_id, (self.clock)()).await
        {
            tracing::error!(worker_id = register.worker_id, %error, "worker disconnect not recorded");
        }
        Ok(())
    }

    /// Registers a worker on its stream, and serves the stream from then on. Not to be cancelled
    /// half-way: it changes the core's state in steps (B171).
    async fn open_stream(
        self,
        register: pb::Register,
        inbound: Streaming<pb::WorkerMessage>,
        caller: oneshot::Sender<()>,
    ) -> Result<Response<ReceiverStream<Result<pb::MasterMessage, Status>>>, Status> {
        // A sign of life as it arrives, before it waits for the lock (B6: a reconnect during
        // a stall), while it waits only (B267: a refused one renews nothing); and the attempts
        // its journal reports have started, so none is taken back for want of a `Started`
        // meanwhile (B50: lost with the old stream). Adopted, they are marked started, after
        // which the reconciliation's decision stands.
        // Its journal's starts are the guard's own: refused or left, it takes them away, never a
        // stream's report of the same start (agy's third review of M5.3g).
        let waiting = self.arrivals.registering(
            &register.worker_id,
            (self.steady)((self.clock)()),
            register
                .journal
                .iter()
                .map(|entry| entry.attempt_id.clone())
                .collect(),
        );
        let leads = self.leading_unless_closing().await;
        let mut core = self.core_for("register").await;
        // Declared after the core's guard, dropped before it on every way out: a round sees a
        // registration's starts while it waits, never after (agy's fourth review of M5.3g).
        let waiting = waiting;
        // Paused while it waited for the lock (behind a round), or stopping: refused as
        // above, what it added gone.
        if !leads || !*self.leading.borrow() || *self.closing.borrow() {
            return Err(Status::unavailable(NOT_LEADING));
        }
        // The worker left while this waited for the lock (its registration's limit, B297): it
        // asks again, perhaps already in line behind this, and registered here would be refused
        // as live. Its starts stay, as at a database failure, while no stream of its is live:
        // its next try reports the same journal, and nothing else protects its attempts
        // meanwhile. Beside a live stream (a newer connection registered), that stream's
        // registration was the whole truth (agy's third review of M5.3g).
        // Kept only for a registration that would have been accepted: none refused (stale, say)
        // keeps anything, and one that would have replaced a live stream keeps its starts as
        // one beside no live stream does (agy's fourth review of M5.3g).
        if caller.is_closed() {
            let gone = !self.sessions.has(&register.worker_id);
            // A worker this master doesn't know keeps nothing: nothing would ever forget it.
            if core.knows(&register.worker_id)
                && core.check(&register).is_ok()
                && (!core.is_live(&register.worker_id) || core.replaces(&register) || gone)
            {
                waiting.keep();
            }
            return Err(Status::cancelled("the worker left before its registration"));
        }
        // A newer connection of the worker replaces its stream: an old one whose end never
        // reached the master (B301), or is still waiting for the lock or behind what it queued for
        // the round (B267). That end, handled after this registration, finds a newer stream and
        // changes nothing (B44); its messages are a replaced stream's from here. An older or equal
        // connection is refused below (decision 525).
        self.replace_stream(&mut core, &register).await?;
        // The worker's old stream, no longer live (a live one refuses this registration), is
        // answered no more from here (decision 443): a replaced incarnation's lease runs out
        // from this registration, the master's last word to it, whatever the old process
        // still sends.
        // (Not live, it has no session: `MasterCore::not_live`.)
        let before = core.before_registration(&register.worker_id);
        let registered = core.register(&register, (self.clock)()).await;
        if core.dispatcher().takes_back() {
            // What it never started is the next round's to take back (M5.2cB).
            self.finished.notify_one();
        }
        // Accepted, its journal is the whole truth, older starts forgotten as the stream is
        // numbered (below). Refused, its starts go with the guard; a start known before (a
        // `Started` that couldn't be stored, its stream still live) stays. A database failure:
        // the worker registers again with the same journal; kept till then (and gone with the
        // worker if it never comes back, by its lease).
        if let Err(error) = &registered
            && !matches!(error, CoreError::Refused(_))
            && core.knows(&register.worker_id)
        {
            waiting.keep();
        }
        let (registered, outgoing) = registered.map_err(|error| match error {
            CoreError::Refused(reason) => Status::failed_precondition(reason),
            other => Status::internal(other.to_string()),
        })?;
        // Room for the registration's own messages (`Registered` and a parse for each new
        // root, all sent before the worker reads any) on top of the usual buffer (B45).
        let own = outgoing
            .iter()
            .filter(|message| message.worker_id == register.worker_id)
            .count();
        let (sender, receiver) = mpsc::channel(self.stream_buffer + 1 + own);
        let first = pb::MasterMessage {
            message: Some(master_message::Message::Registered(registered)),
            clock: Some(self.stamp(0)),
        };
        // The worker's stream from now on, put in place before the lock is let go: a message
        // of the old stream handled after this registration is known to be the old one's
        // (B44).
        // A master stopping meanwhile closes it or refuses it, never leaving it open for its
        // graceful shutdown to wait for (B200's review): `SessionWriter::install` checks the
        // closing under its compare-and-swap, as `Sessions::close_all` sets it first.
        if !*self.leading.borrow() || *self.closing.borrow() || self.sessions.is_closing() {
            // Registered all the same, it would be live with no stream, and its number known:
            // a worker sending none locked out till its lease (agy's fourth review of M5.3g).
            // Undone in memory, as a restart leaves it; the database's word is the next
            // leader's.
            core.registration_undone(&register.worker_id, &before);
            return Err(Status::unavailable(NOT_LEADING));
        }
        // Numbered only now, certain to be put in place, under the core's lock as the
        // registration: an older stream's reader records nothing more (B240), and a
        // registration refused for not leading leaves the live stream current (agy's third
        // review of M5.3g). Its journal is the whole truth: older starts go with it.
        let stream = Arc::new(Session::new(
            &register.worker_id,
            self.arrivals.registered(&register.worker_id),
            sender,
        ));
        stream
            .send(first)
            .map_err(|_| Status::internal("the stream closed at once"))?;
        if core.sessions_mut().install(&stream).is_err() {
            // Closing began meanwhile: as above.
            core.registration_undone(&register.worker_id, &before);
            return Err(Status::unavailable(NOT_LEADING));
        }
        // Its starts the reconciliation's now, under the core's lock.
        drop(waiting);
        drop(core);
        self.deliver(outgoing);
        tokio::spawn(self.clone().serve_stream(
            register.worker_id,
            stream,
            inbound,
            register.queue_depth > 0,
        ));
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    /// Ends every worker's stream with `UNAVAILABLE`, for the master to stop: tonic's graceful
    /// shutdown waits for open calls, which a worker's stream never ends by itself, and each
    /// worker connects again, to the master that leads (B200).
    pub fn close_streams(&self) {
        // Each stream's server lets its sender go: a full one ends once its worker read it.
        self.closing.send_replace(true);
        // Full, a worker isn't reading: its stream ends when its other senders go.
        self.sessions.close_all(&Status::unavailable(STOPPING));
        // What waits for a round holds its stream's sender too: let go, or the stream never
        // ends (Grok's review of M4B.3); the worker sends it again to the next leader.
        drop(std::mem::take(&mut *self.queued_results()));
    }
}

/// A turn from `first`: what is queued after it, up to `limit`.
fn fill_turn(
    first: worker_message::Message,
    queued: &mut mpsc::UnboundedReceiver<worker_message::Message>,
    limit: usize,
) -> Vec<worker_message::Message> {
    let mut turn = vec![first];
    while turn.len() < limit {
        let Some(next) = queued.try_recv().ok() else {
            break;
        };
        turn.push(next);
    }
    turn
}

/// A worker's result waiting for the round (M5.2cB), with the stream it came on.
#[derive(Debug)]
struct QueuedResult {
    worker_id: String,
    stream: Arc<Session>,
    /// The message; none: its stream's end, which waits behind what it said before.
    message: Option<worker_message::Message>,
}

/// A worker's consecutive `Started`s and `Finished`s in a round's batch.
struct Stretch {
    worker_id: String,
    stream: Arc<Session>,
    group: Group,
}

/// The group `worker_id`'s next `Started` or `Finished` joins: that worker's in the batch, or
/// a new one. A batch holds only these, so each worker's keep their order; between workers it
/// doesn't matter (decision 505).
fn stretch_of<'a>(
    batch: &'a mut Vec<Stretch>,
    worker_id: &str,
    stream: &Arc<Session>,
) -> &'a mut Group {
    let at = batch
        .iter()
        .position(|s| s.worker_id == worker_id)
        .unwrap_or_else(|| {
            batch.push(Stretch {
                worker_id: worker_id.to_owned(),
                stream: stream.clone(),
                group: Group::default(),
            });
            batch.len() - 1
        });
    &mut batch[at].group
}

/// What a round's results part has come to.
struct Handling {
    outgoing: Vec<Outgoing>,
    /// How many more it may handle alone.
    alone_left: usize,
    /// What goes back to the queue, for the next round, in order.
    back: Vec<QueuedResult>,
    /// The workers whose stream a message ended this round: their later messages are dropped
    /// (the worker registers again and sends them again), as a turn's were.
    ended: BTreeSet<String>,
}

impl Handling {
    fn put_back(
        &mut self,
        worker_id: &str,
        stream: &Arc<Session>,
        messages: Vec<worker_message::Message>,
    ) {
        self.back
            .extend(messages.into_iter().map(|message| QueuedResult {
                worker_id: worker_id.to_owned(),
                stream: stream.clone(),
                message: Some(message),
            }));
    }
}

/// Whether `message` is a trigger's: an event, a poke's report or a trigger stopped.
fn is_trigger_message(message: &worker_message::Message) -> bool {
    matches!(
        message,
        worker_message::Message::TriggerEvent(_)
            | worker_message::Message::TriggerPoked(_)
            | worker_message::Message::TriggerStopped(_)
    )
}

/// A worker's message the round handles, not its turn (M5.2cB, decision 505): what moves an
/// instance, and so its run, forward; a `Started` with them, which keeps its order; and a
/// `Deregister`, which ends the worker's open attempts.
const fn is_result(message: &worker_message::Message) -> bool {
    matches!(
        message,
        worker_message::Message::Started(_)
            | worker_message::Message::Finished(_)
            | worker_message::Message::TaskEnded(_)
            | worker_message::Message::Rejected(_)
            | worker_message::Message::Deregister(_)
    )
}

/// What a turn came to.
#[derive(Debug, Default)]
struct TurnDone {
    outgoing: Vec<Outgoing>,
    /// A `Finished` was stored: a round wakes.
    finished: bool,
    /// A message wasn't handled: the stream ends.
    unhandled: bool,
}

/// How a turn ended.
enum TurnEnd {
    /// The stream goes on.
    Goes,
    /// The stream no longer carries its worker's messages (replaced, or the worker taken for
    /// lost): let go, the worker not written as disconnected.
    Dropped,
    /// A message wasn't handled: the stream ends, the worker disconnected.
    Unhandled,
}

/// What a worker's stream's server does next.
enum Next {
    /// Handles this message (boxed: a message is far larger than the other cases).
    Message(Box<worker_message::Message>),
    /// The worker's stream ended.
    Ended,
    /// The master closes its streams.
    Closing,
    /// A newer connection of its worker replaced it.
    Replaced,
}

/// Why a worker's stream ends when this master stops.
const STOPPING: &str = "this master is stopping: connect again";

impl Hub {
    /// The refusal of a stream while this master doesn't lead, naming the master that holds
    /// the lease and until when, for whoever reads the worker's log (decision 452); as before
    /// when the database doesn't say within [`LEADER_LOOKUP`].
    async fn not_leading(&self) -> Status {
        let held = tokio::time::timeout(LEADER_LOOKUP, self.db.lease_held()).await;
        let message = match held {
            Ok(Ok(Some((holder, until)))) => {
                format!("{NOT_LEADING}; the lease is held by `{holder}` until {until}")
            }
            Ok(Ok(None)) => format!("{NOT_LEADING}; no master holds the lease now"),
            Ok(Err(_)) | Err(_) => NOT_LEADING.to_owned(),
        };
        Status::unavailable(message)
    }
}

/// The longest a refusal waits to name the leader.
const LEADER_LOOKUP: Duration = Duration::from_secs(1);

/// Why a stream opened while this master doesn't lead is refused.
const NOT_LEADING: &str = "this master isn't leading (its database lease isn't renewed, or another \
     master took it over): connect again";

/// The master's clock to stamp on a message (B225): this process's timeline, master time
/// and the wall clock now, the latest heartbeat heard on the worker's stream, and the lease
/// epoch it leads with (decision 444).
fn stamp_of(
    (wall, steady): (fn() -> Timestamp, crate::clock::Steady),
    heard: u64,
    epoch: i64,
) -> pb::MasterClock {
    let wall = wall();
    pb::MasterClock {
        timeline: crate::clock::timeline(),
        master_ms: steady(wall).as_millisecond(),
        heard_sequence: heard,
        wall_ms: wall.as_millisecond(),
        lease_epoch: epoch,
        // Its number, set as it is queued on its stream (B240).
        sequence: 0,
    }
}

/// Answers a worker's heartbeats as they arrive, before they wait for the lock: a busy master
/// isn't a lost one. Not while paused: another master may be leading, and may count the worker
/// lost. The session is held weakly, for this reader not to keep its stream open.
struct HeartbeatAcks {
    /// Its stream's session: its numbering (B240) and its heartbeats heard (B225).
    session: std::sync::Weak<Session>,
    /// The stream's ID, for [`Arrivals::read`].
    stream_id: u64,
    leading: watch::Receiver<bool>,
    /// The master's wall clock and master time, for the stamp.
    clocks: (fn() -> Timestamp, crate::clock::Steady),
    /// The lease epoch on the stamp.
    epoch: i64,
    /// The workers' sessions: answered only while this is its worker's current one (decision
    /// 443).
    sessions: Sessions,
    /// The workers' queue head ages, shared by every stream (decision 509), and whether this
    /// worker has a queue: one without counts no age (it would always say 0, busy or not).
    slow_equals: (Arc<crate::slow_equals::SlowEquals>, bool),
}

impl HeartbeatAcks {
    /// The message numbered `sequence` arrived: noted (the latest's number goes on the
    /// master's next stamps, B225), and a heartbeat (`Some`: its queue's head age) answered
    /// with it on the answer's stamp, and the cluster's slow-equals (decision 509).
    fn heard(&self, sequence: u64, heartbeat: Option<Duration>) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        session.hear(sequence);
        let heard = session.heard();
        if let Some(head_age) = heartbeat
            && *self.leading.borrow()
            && self.sessions.is_current(&session)
        {
            let (slow_equals, has_queue) = &self.slow_equals;
            let slow = slow_equals.heard(
                session.worker_id(),
                has_queue.then_some(head_age),
                tokio::time::Instant::now(),
            );
            let ack = pb::MasterMessage {
                message: Some(master_message::Message::HeartbeatAck(pb::HeartbeatAck {
                    slow_equals_ms: u64::try_from(slow.as_millis()).unwrap_or(u64::MAX),
                })),
                clock: Some(stamp_of(self.clocks, heard, self.epoch)),
            };
            // Full, the worker isn't reading: the next heartbeat tries again.
            let _ = session.send(ack);
        }
    }

    /// Whether its session is still its worker's current one.
    fn is_current(&self) -> bool {
        self.session
            .upgrade()
            .is_some_and(|session| self.sessions.is_current(&session))
    }

    /// Master time now (a message's arrival).
    fn steady_now(&self) -> Timestamp {
        (self.clocks.1)((self.clocks.0)())
    }
}

/// Reads a worker's stream until it ends: records when each message arrived and queues it
/// for handling.
async fn read_stream(
    worker_id: String,
    mut inbound: Streaming<pb::WorkerMessage>,
    arrivals: Arrivals,
    queue: mpsc::UnboundedSender<worker_message::Message>,
    acks: HeartbeatAcks,
) {
    loop {
        match inbound.message().await {
            Ok(Some(message)) => {
                // Superseded by a newer registration (replaced, or taken out of the hub and
                // registered again): what it still brings is no sign of life and starts
                // nothing (agy's second review of M5.3g).
                // Not current any more (replaced, stuck or taken out): it records nothing, under
                // the sessions' word first and Arrivals' lock then (agy's review of M5.3i).
                if !acks.is_current()
                    || !arrivals.arrived_on(&worker_id, acks.stream_id, acks.steady_now())
                {
                    return;
                }
                // A heartbeat is answered whatever it carries; its numbers count when stamped.
                let heartbeat = match &message.message {
                    Some(worker_message::Message::Heartbeat(beat)) => {
                        Some(Duration::from_millis(beat.queue_head_age_ms))
                    }
                    _ => None,
                };
                acks.heard(message.clock.as_ref().map_or(0, |c| c.sequence), heartbeat);
                if let Some(clock) = &message.clock {
                    arrivals.read(&worker_id, acks.stream_id, clock.heard_sequence);
                }
                if let Some(
                    worker_message::Message::Started(pb::Started { attempt_id, .. })
                    | worker_message::Message::Finished(pb::Finished { attempt_id, .. }),
                ) = &message.message
                {
                    arrivals.start_arrived_on(&worker_id, acks.stream_id, attempt_id);
                    arrivals.answered_on(&worker_id, acks.stream_id, attempt_id);
                }
                if let Some(worker_message::Message::Rejected(pb::Rejected {
                    attempt_id, ..
                })) = &message.message
                {
                    arrivals.answered_on(&worker_id, acks.stream_id, attempt_id);
                }
                if let Some(message) = message.message
                    && let Err(mpsc::error::SendError(unsent)) = queue.send(message)
                {
                    // Never to be handled: its start mustn't keep protecting the attempt.
                    if let worker_message::Message::Started(pb::Started { attempt_id, .. })
                    | worker_message::Message::Finished(pb::Finished { attempt_id, .. }) =
                        &unsent
                    {
                        arrivals.start_handled_on(&worker_id, acks.stream_id, attempt_id);
                    }
                    return;
                }
            }
            Ok(None) => return,
            Err(status) => {
                tracing::warn!(worker_id, %status, "worker stream failed");
                return;
            }
        }
    }
}

#[tonic::async_trait]
impl Master for Hub {
    type ControlStream = ReceiverStream<Result<pb::MasterMessage, Status>>;
    type FetchInputsStream = ReceiverStream<Result<pb::InputChunk, Status>>;

    async fn control(
        &self,
        request: Request<Streaming<pb::WorkerMessage>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        let extensions = request.extensions().clone();
        let mut inbound = request.into_inner();
        // Not waited for once the master stops: a stream that never sends `Register` would
        // keep tonic's graceful shutdown waiting (B219).
        let mut closing = self.closing.subscribe();
        // Closing first, as every wait racing it (agy's sixth review of M5.3g).
        let first = tokio::select! {
            biased;
            _ = closing.wait_for(|closing| *closing) => {
                return Err(Status::unavailable(STOPPING));
            }
            first = inbound.message() => first?,
        }
        .ok_or_else(|| Status::invalid_argument("the stream ended before `Register`"))?;
        let Some(worker_message::Message::Register(register)) = first.message else {
            return Err(Status::invalid_argument(
                "the first message must be `Register`",
            ));
        };
        // Its certificate's worker, before anything is noted of it (decision 520): the stream
        // is then that worker's, every message on it too.
        self.identified(&extensions, &register.worker_id)?;
        // Not while paused: another master may be leading. Refused at once rather than held
        // (B165), before anything is noted of it: the worker connects again, through the
        // Service, to the master that leads.
        if !*self.leading.borrow() {
            return Err(self.not_leading().await);
        }
        // Apart from this call, which ends when the worker resets its stream (giving up on a
        // slow master): stopped half-way, the worker would stay live with no stream behind it,
        // refused from then on (B171). Finished, its stream's end disconnects it as usual.
        let (caller, _waiting) = oneshot::channel();
        #[cfg(test)]
        let _counted = CountOnDrop(Arc::clone(&self.calls_dropped));
        tokio::spawn(self.clone().open_stream(register, inbound, caller))
            .await
            .map_err(|e| Status::internal(format!("the registration failed: {e}")))?
    }

    async fn upload_parse_result(
        &self,
        request: Request<Streaming<pb::ParseResultPart>>,
    ) -> Result<Response<pb::ParseResultAck>, Status> {
        // The header, then a file a message (B35): read whole before the lock is taken.
        let extensions = request.extensions().clone();
        let mut parts = request.into_inner();
        let mut upload = ParseUpload::default();
        // Not waited for once the master stops, as `control`'s `Register` isn't (B219's review).
        let mut closing = self.closing.subscribe();
        loop {
            let part = tokio::select! {
                biased;
                _ = closing.wait_for(|closing| *closing) => {
                    return Err(Status::unavailable(STOPPING));
                }
                part = parts.message() => part?,
            };
            let Some(part) = part else { break };
            upload
                .add(part)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        let result = upload
            .finish()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.identified(&extensions, &result.worker_id)?;
        // Apart from this call, as a registration is: a reset stream doesn't stop it half-way.
        let hub = self.clone();
        let uploaded = tokio::spawn(async move {
            if !hub.leading_unless_closing().await {
                return None;
            }
            Some(
                hub.core_for("upload")
                    .await
                    .upload(result, (hub.clock)())
                    .await,
            )
        })
        .await
        .map_err(|e| Status::internal(format!("the upload failed: {e}")))?
        .ok_or_else(|| Status::unavailable(STOPPING))?;
        match uploaded {
            Ok(()) => Ok(Response::new(pb::ParseResultAck {})),
            Err(error @ CoreError::BadMessage { .. }) => {
                Err(Status::invalid_argument(error.to_string()))
            }
            Err(error) => Err(Status::internal(error.to_string())),
        }
    }

    async fn fetch_inputs(
        &self,
        request: Request<pb::FetchInputsRequest>,
    ) -> Result<Response<Self::FetchInputsStream>, Status> {
        let extensions = request.extensions().clone();
        let attempt_id = request.into_inner().attempt_id;
        // Only the attempt's own worker gets its inputs (decision 520): read without the lock.
        if self.worker_identity.is_some() {
            let attempt = self
                .db
                .attempt(&attempt_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found(format!("no open attempt `{attempt_id}`")))?;
            self.identified(&extensions, &attempt.worker_id)?;
        }
        // Read without the master's lock: a busy master doesn't hold a task's start up (B11).
        let inputs = crate::dispatch::attempt_inputs(&self.db, &attempt_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("no open attempt `{attempt_id}`")))?;
        let chunks: Vec<Vec<u8>> = inputs.chunks(INPUT_CHUNK).map(<[u8]>::to_vec).collect();
        let (sender, receiver) = mpsc::channel(chunks.len().max(1));
        for data in chunks {
            // The channel holds every chunk, so sending never waits.
            let _ = sender.try_send(Ok(pb::InputChunk { data }));
        }
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

/// The most messages of one worker handled in one turn of the master's lock: what is queued
/// gets through together, and a long queue doesn't hold rounds and other workers up for long
/// (at most this many `Finished`, each a few writes; the rest waits for the next turn). 80 from
/// M5.1e's stress runs (decision 484): bursts recorded fastest, steady throughput unchanged.
pub const MESSAGES_PER_TURN: usize = 80;

/// The most messages of a failed group handled one by one in a turn (M4B.3): the rest wait for
/// the next, so a failing group doesn't hold the lock for 40 transactions.
pub const ALONE_PER_TURN: usize = 10;

/// The least time between the trigger loop's passes (decision 486); the rounds results wake
/// have their own, [`TASK_ROUND_GAP`] (M5.2cB4).
pub const ROUND_GAP: Duration = Duration::from_millis(50);

/// The least time between the end of a round and one finished tasks wake (M5.2cB4): such a
/// round runs its slow parts once a tick interval at most, so it is short; under a stream of
/// finishes each takes what arrived meanwhile.
pub const TASK_ROUND_GAP: Duration = Duration::from_millis(10);

/// How long the master waits before listening again after notifications failed.
pub const NOTIFICATION_RETRY: Duration = Duration::from_secs(30);

/// The longest a gauge refresh may take unless set otherwise.
pub const DEFAULT_REFRESH_LIMIT: Duration = Duration::from_secs(30);

/// The largest message from a worker unless set otherwise (`[grpc] max_message_size`'s
/// default; gRPC's own, 4 MiB, is too small for a large root's parse upload).
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 << 20;

/// How often the loop beats while it waits between ticks (liveness, see [`Hub::beat`]).
pub const ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// What waiting for command notifications gave.
#[derive(Debug)]
enum Listened {
    /// A command was announced.
    Command,
    /// Listening failed.
    Failed(DbError),
}

/// The next command notification, never without notifications. Paused after a failure
/// until `paused_until`; once that passes, listening resumes and the failure's problem is
/// cleared (set again if it fails again): a listener that reconnected by itself would
/// otherwise report it for good.
async fn next_command(
    notifications: Option<&mut CommandNotifications>,
    paused_until: &mut Option<tokio::time::Instant>,
    problems: &Problems,
    triggers_due: &tokio::sync::Notify,
) -> Listened {
    if let Some(until) = *paused_until {
        tokio::time::sleep_until(until).await;
        *paused_until = None;
        problems.clear(ProblemKind::Notifications);
    }
    let Some(notifications) = notifications else {
        return std::future::pending().await;
    };
    loop {
        return match notifications.next().await {
            Ok(schedr_db::commands::Notified::Command) => Listened::Command,
            // For the trigger loop (decision 486), and the round only when steps wait for it
            // (decision 487).
            Ok(schedr_db::commands::Notified::Event { triggers, waiters }) => {
                if triggers {
                    triggers_due.notify_one();
                }
                if !waiters {
                    continue;
                }
                Listened::Command
            }
            Err(error) => Listened::Failed(error),
        };
    }
}

/// Stops the trigger loop when dropped: `stop` turned true. A pass under way ends first (cut
/// off halfway, its transaction and the core's memory would be left in between); the loop
/// then returns at its next wait.
fn scopeguard_stop(stop: watch::Sender<bool>, task: tokio::task::JoinHandle<()>) -> impl Drop {
    struct Guard(
        watch::Sender<bool>,
        #[expect(dead_code, reason = "detached on drop")] tokio::task::JoinHandle<()>,
    );
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.send(true);
        }
    }
    Guard(stop, task)
}

fn lock_slow(
    last: &std::sync::Mutex<Option<std::time::Instant>>,
) -> std::sync::MutexGuard<'_, Option<std::time::Instant>> {
    last.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Counts its drop, for [`Hub::calls_dropped`].
#[cfg(test)]
struct CountOnDrop(Arc<std::sync::atomic::AtomicUsize>);

#[cfg(test)]
impl Drop for CountOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
