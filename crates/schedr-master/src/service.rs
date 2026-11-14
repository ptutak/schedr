//! The master's gRPC service and its loop: workers' `Control` streams, parse uploads and
//! `FetchInputs`, over [`MasterCore`] (see archive/PROJECT.md, Master–worker protocol).
//!
//! A [`Hub`] holds the core and each live worker's outgoing stream; whatever the core decides
//! to send (assignments, cancels, acknowledgements) is delivered through it, from a stream's
//! messages and from the loop's ticks alike.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use jiff::Timestamp;
use schedr_db::commands::CommandNotifications;
use schedr_db::{Database, DbError};
use schedr_model::beat::Beat;
use schedr_proto::convert::ParseUpload;
use schedr_proto::pb::master_server::{Master, MasterServer};
use schedr_proto::pb::{self, master_message, worker_message};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::core::task_api::ApiCallPlan;
use crate::core::{Arrivals, CoreError, Group, MasterCore, RoundPart};
use crate::dispatch::Outgoing;
use crate::lease::{Leadership, Renewal};
use crate::metrics::MasterMetrics;
use crate::problems::{ProblemKind, Problems};

#[cfg(test)]
mod tests;

/// The size of one `FetchInputs` chunk.
pub const INPUT_CHUNK: usize = 1024 * 1024;

/// How many messages wait for a worker before it is taken for one that stopped reading.
const STREAM_BUFFER: usize = 1024;

/// A worker's stream of messages. Sending never waits (see [`Hub::deliver`]).
/// A worker's stream to it (B240): each message's `MasterClock.sequence` is taken and the
/// message queued under one lock, by the core's deliveries and the reader's answers alike, so
/// the numbers leave in order; `id` tells it from the worker's other streams.
#[derive(Debug, Clone)]
struct Outbound {
    sender: mpsc::Sender<Result<pb::MasterMessage, Status>>,
    /// The last number used.
    numbers: Arc<std::sync::Mutex<u64>>,
    id: u64,
}

impl Outbound {
    fn new(sender: mpsc::Sender<Result<pb::MasterMessage, Status>>, id: u64) -> Self {
        Self {
            sender,
            numbers: Arc::default(),
            id,
        }
    }

    fn same_channel(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }

    /// Numbers `message` (stamped already) and queues it: its number. Not queued, the number
    /// isn't used, and the message is dropped.
    fn send(&self, message: pb::MasterMessage) -> Result<u64, mpsc::error::TrySendError<()>> {
        numbered_send(&self.numbers, &self.sender, message)
    }

    /// Ends the stream with `status` (unnumbered: no message).
    fn end(&self, status: Status) {
        let _ = self.sender.try_send(Err(status));
    }
}

/// Numbers `message` past the last of `numbers` and queues it on `sender`, under the lock.
fn numbered_send(
    numbers: &std::sync::Mutex<u64>,
    sender: &mpsc::Sender<Result<pb::MasterMessage, Status>>,
    mut message: pb::MasterMessage,
) -> Result<u64, mpsc::error::TrySendError<()>> {
    let mut last = numbers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let number = *last + 1;
    if let Some(clock) = message.clock.as_mut() {
        clock.sequence = number;
    }
    // The unsent message isn't wanted back: only whether the stream is full or closed.
    sender.try_send(Ok(message)).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
        mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(()),
    })?;
    *last = number;
    Ok(number)
}

/// The core and the workers' outgoing streams.
#[derive(Debug, Clone)]
pub struct Hub {
    core: Arc<Mutex<MasterCore>>,
    /// Each worker's current stream. Held only briefly (a send never waits), so the core's
    /// lock, which can be held while waiting for it, never waits on a slow worker; a delivery
    /// goes out whole under it, one after another.
    streams: Arc<Mutex<BTreeMap<String, Outbound>>>,
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
    /// When each worker's latest message arrived (the core's).
    arrivals: Arrivals,
    /// How often to try listening for commands again while the loop has no notifications
    /// because starting them failed (see [`Hub::with_listen_retry`]).
    listen_retry: Option<Duration>,
    /// Whether this master leads (holds the lease, see [`crate::lease`]): worker messages,
    /// registrations and uploads wait while it doesn't.
    leading: Arc<watch::Sender<bool>>,
    /// Told when a worker's `Finished` was recorded: a round runs at once instead of at the
    /// next tick, its downstream tasks ready (decision 42B; [`ROUND_GAP`] after the last).
    finished: Arc<tokio::sync::Notify>,
    /// Told when the trigger loop must look (decision 486): a trigger's stored moment came
    /// (the tick's probe), a named event was announced.
    triggers_due: Arc<tokio::sync::Notify>,
    /// Set when a trigger pass failed: the next tick wakes the loop again, as a failed round
    /// is tried again at the next (Grok's review of M5.1f2).
    triggers_failed: Arc<std::sync::atomic::AtomicBool>,
    /// Its problems now, written with its row (decision 129).
    problems: Problems,
    /// How its loop is paced.
    timings: crate::core::MasterTimings,
    /// Master time (see [`crate::clock`]), stamped on every message.
    steady: crate::clock::Steady,
    /// The lease epoch this master leads with (decision 444), on every message's stamp.
    epoch: i64,
    /// The heartbeats heard on each worker's current stream: the latest's number goes on every
    /// message to it (`MasterClock.heard_sequence`). One count per stream, so a replaced
    /// stream's reader can't put its numbers on the new one's messages.
    heard: Counters,
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

impl Hub {
    /// A hub over `core`, with the system clock.
    #[must_use]
    pub fn new(core: MasterCore) -> Self {
        Self::with_clock(core.with_steady(crate::clock::monotonic), Timestamp::now)
    }

    /// A hub over `core` with another clock (tests).
    #[must_use]
    pub fn with_clock(core: MasterCore, clock: fn() -> Timestamp) -> Self {
        let core_steady = core.steady_clock();
        Self {
            beat: Beat::default(),
            refresh_limit: DEFAULT_REFRESH_LIMIT,
            max_message_size: core.settings().max_message_size,
            messages_per_turn: core.settings().messages_per_turn.max(1),
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
            listen_retry: None,
            core: Arc::new(Mutex::new(core)),
            streams: Arc::new(Mutex::new(BTreeMap::new())),
            closing: Arc::new(watch::channel(false).0),
            steady: core_steady,
            epoch: 0,
            heard: Arc::default(),
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
        tokio::select! {
            () = self.while_leading() => true,
            _ = closing.wait_for(|closing| *closing) => false,
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
    ///
    /// # Errors
    ///
    /// The core's failure (the next tick goes on).
    pub async fn tick(&self) -> Result<(), CoreError> {
        self.tick_ahead(Duration::ZERO).await
    }

    /// As [`Self::tick`], its probe looking `ahead` (the loop's tick interval: what comes due
    /// before the next tick is found, so the round's parts share one probe, decision 485).
    ///
    /// # Errors
    ///
    /// The core's failure (the next tick goes on).
    pub async fn tick_ahead(&self, ahead: Duration) -> Result<(), CoreError> {
        // The lock taken for each part on its own, so a worker's turn gets in between them
        // (decision 469), each part at the time it runs. What a part built before a failure is
        // delivered too (B7); a failed part ends the round, as before. The round's reads the
        // probe finds empty are skipped (decision 485): an idle tick is one statement.
        Box::pin(self.db.in_round((self.clock)(), ahead, async {
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
            for part in RoundPart::ALL {
                if part == RoundPart::Triggers {
                    continue;
                }
                if part == RoundPart::Commands {
                    self.commands().await?;
                    continue;
                }
                let mut outgoing = Vec::new();
                let result = self
                    .core_for("round")
                    .await
                    .round_part(part, (self.clock)(), &mut outgoing)
                    .await;
                self.deliver(outgoing).await;
                result?;
            }
            Ok(())
        }))
        .await
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
        self.deliver(std::mem::take(&mut outgoing)).await;
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
            self.deliver(std::mem::take(&mut outgoing)).await;
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
        self.deliver(outgoing).await;
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
                    self.refresh_gauges().await;
                    continue;
                }
                Wake::Command => {
                    paused_until = None;
                    self.command_announced(notifications.as_mut()).await;
                    ticks.reset();
                }
                Wake::NotificationsFailed(error) => {
                    paused_until = Some(self.notifications_failed(&error));
                    continue;
                }
                Wake::Finished => {
                    // At most one round every ROUND_GAP: under a stream of finishes each
                    // round takes what arrived meanwhile, and the lease's renewals and the
                    // workers' messages get their turns.
                    let due = last_round.map(|ended| ended + self.timings.round_gap);
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
        self.deliver(outgoing).await;
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
    async fn deliver(&self, outgoing: Vec<Outgoing>) {
        let mut stuck = Vec::new();
        let mut streams = self.streams.lock().await;
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
            let Some(stream) = streams.get(&worker_id) else {
                tracing::warn!(worker_id, "no stream for worker; message dropped");
                unsent(assigned.as_ref());
                continue;
            };
            message.clock = Some(self.stamp(self.heard_on(&worker_id)));
            match stream.send(message) {
                Ok(number) => {
                    if let Some(attempt_id) = assigned {
                        self.arrivals
                            .assigned(&worker_id, stream.id, &attempt_id, number);
                    }
                }
                Err(mpsc::error::TrySendError::Full(())) => {
                    unsent(assigned.as_ref());
                    tracing::error!(
                        worker_id,
                        "worker doesn't read its stream ({STREAM_BUFFER} messages waiting): \
                         disconnected"
                    );
                    streams.remove(&worker_id);
                    stuck.push(worker_id);
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    unsent(assigned.as_ref());
                    tracing::warn!(worker_id, "worker stream closed; message dropped");
                }
            }
        }
        drop(streams);
        for worker_id in stuck {
            // Not here: the caller may hold the core's lock.
            tokio::spawn(self.clone().disconnect_stuck(worker_id));
        }
    }

    /// The master's clock to stamp on a message (B225), `heard` the latest heartbeat heard on
    /// the worker's stream.
    fn stamp(&self, heard: u64) -> pb::MasterClock {
        stamp_of((self.clock, self.steady), heard, self.epoch)
    }

    /// The latest heartbeat heard on `worker_id`'s current stream (0: none yet, or no stream).
    fn heard_on(&self, worker_id: &str) -> u64 {
        lock_heard(&self.heard)
            .get(worker_id)
            .map_or(0, |heard| heard.load(Ordering::SeqCst))
    }

    /// Disconnects a worker whose stream was dropped for not being read, unless it has
    /// registered again since. (Its stream's reader stops at its next message, which ends
    /// the worker's side.)
    async fn disconnect_stuck(self, worker_id: String) {
        let mut core = self.core_for("stream").await;
        if self.streams.lock().await.contains_key(&worker_id) {
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
        worker_id: &str,
        message: worker_message::Message,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let worker_message::Message::ApiCall(call) = message else {
            return core.handle(worker_id, message, (self.clock)()).await;
        };
        match core.plan_api_call(worker_id, call, (self.clock)()).await? {
            ApiCallPlan::Answered(answer) => Ok(vec![*answer]),
            ApiCallPlan::Run(job) => {
                let hub = self.clone();
                tokio::spawn(async move {
                    let answer = job.run().await;
                    hub.deliver(vec![answer]).await;
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
        closing: &mut watch::Receiver<bool>,
    ) -> Next {
        let next = async {
            let Some(message) = queued.recv().await else {
                return Next::Ended;
            };
            self.while_leading().await;
            Next::Message(Box::new(message))
        };
        tokio::select! {
            next = next => next,
            _ = closing.wait_for(|closing| *closing) => Next::Closing,
        }
    }

    /// One turn of a worker's messages (M4B.3, decision 397): its `Started`s and `Finished`s as
    /// one group, stored together, heartbeats and warnings after it; any other message ends the
    /// group (it is stored first) and is handled alone. A group that fails is redone one by one,
    /// [`ALONE_PER_TURN`] now and the rest first in the next turn, with what this turn hadn't
    /// reached. The core's lock is taken for each transaction on its own (the `Started` group,
    /// the `Finished` group, each message alone) and its stream checked again each time, so
    /// rounds and other workers' turns get in between (decision 480); what each sends goes out
    /// at once.
    async fn turn(
        &self,
        (worker_id, stream): (&str, &Outbound),
        turn: Vec<Turned>,
        carried: &mut VecDeque<Turned>,
    ) -> TurnEnd {
        let mut group = Group::default();
        let mut after = Vec::new();
        let mut messages = turn.into_iter();
        let mut barrier = None;
        loop {
            let next = messages.next();
            match next {
                Some(Turned::Grouped(worker_message::Message::Started(started))) => {
                    group.started.push(started);
                    continue;
                }
                Some(Turned::Grouped(worker_message::Message::Finished(finished))) => {
                    group.finished.push(finished);
                    continue;
                }
                Some(Turned::Grouped(
                    message @ (worker_message::Message::Heartbeat(_)
                    | worker_message::Message::Warnings(_)),
                )) => {
                    after.push(message);
                    continue;
                }
                Some(Turned::Grouped(message) | Turned::Alone(message)) => barrier = Some(message),
                None => {}
            }
            // The groups so far (`Started`s, then `Finished`s: each kind its own transaction,
            // decision 397), then what waited for them, then the barrier.
            let results = Group {
                started: Vec::new(),
                finished: std::mem::take(&mut group.finished),
            };
            let mut kinds = VecDeque::from([std::mem::take(&mut group), results]);
            while let Some(kind) = kinds.pop_front() {
                if kind.is_empty() {
                    continue;
                }
                let wakes = !kind.finished.is_empty();
                let (part, count) = if kind.started.is_empty() {
                    ("finished", kind.finished.len())
                } else {
                    ("started", kind.started.len())
                };
                let Some(mut core) = self.served_core(worker_id, stream).await else {
                    return TurnEnd::Dropped;
                };
                let began = std::time::Instant::now();
                let handled = core.handle_group(worker_id, kind, (self.clock)()).await;
                self.metrics.turn_part_done(part, count, began.elapsed());
                drop(core);
                match handled {
                    Ok(out) => self.sent(out, wakes).await,
                    Err(failed) => {
                        let mut failed = failed.into_messages().into_iter();
                        for message in failed.by_ref().take(ALONE_PER_TURN) {
                            let (end, _) = self.alone_served(worker_id, stream, message).await;
                            if !matches!(end, TurnEnd::Goes) {
                                return end;
                            }
                        }
                        // The rest in order: a `Finished` never stored before its `Started`.
                        carried.extend(failed.map(Turned::Alone));
                        carried.extend(
                            kinds
                                .into_iter()
                                .flat_map(Group::into_messages)
                                .map(Turned::Grouped),
                        );
                        carried.extend(after.drain(..).map(Turned::Grouped));
                        carried.extend(barrier.take().map(Turned::Alone));
                        carried.extend(messages);
                        return TurnEnd::Goes;
                    }
                }
            }
            let rest = after.len() + usize::from(barrier.is_some());
            let mut held = std::time::Duration::ZERO;
            for message in after.drain(..) {
                let (end, took) = self.alone_served(worker_id, stream, message).await;
                held += took;
                if !matches!(end, TurnEnd::Goes) {
                    self.metrics.turn_part_done("rest", rest, held);
                    return end;
                }
            }
            let end = match barrier.take() {
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

    /// The core, locked, if `stream` still carries its worker's messages (see
    /// [`Self::still_served`]); none when it doesn't, and the stream is let go.
    async fn served_core(
        &self,
        worker_id: &str,
        stream: &Outbound,
    ) -> Option<tokio::sync::MutexGuard<'_, MasterCore>> {
        let core = self.core_for("turn").await;
        self.still_served(&core, worker_id, stream)
            .await
            .then_some(core)
    }

    /// `message` handled alone, under the core's lock taken for it ([`Self::alone`]), what it
    /// sends delivered after the lock is let go; with how long the lock was held.
    async fn alone_served(
        &self,
        worker_id: &str,
        stream: &Outbound,
        message: worker_message::Message,
    ) -> (TurnEnd, std::time::Duration) {
        let Some(mut core) = self.served_core(worker_id, stream).await else {
            return (TurnEnd::Dropped, std::time::Duration::ZERO);
        };
        let began = std::time::Instant::now();
        let mut done = TurnDone::default();
        let goes_on = self.alone(&mut core, worker_id, message, &mut done).await;
        let took = began.elapsed();
        drop(core);
        self.sent(done.outgoing, done.finished).await;
        let end = if goes_on && !done.unhandled {
            TurnEnd::Goes
        } else {
            TurnEnd::Unhandled
        };
        (end, took)
    }

    /// Delivers what a transaction of a turn sends, waking a round if it stored a `Finished`.
    async fn sent(&self, outgoing: Vec<Outgoing>, finished: bool) {
        if finished {
            self.finished.notify_one();
        }
        self.deliver(outgoing).await;
    }

    /// One message handled on its own, as every message was before M4B.3; whether the turn
    /// goes on (not: the stream ends).
    async fn alone(
        &self,
        core: &mut MasterCore,
        worker_id: &str,
        message: worker_message::Message,
        done: &mut TurnDone,
    ) -> bool {
        let is_finished = matches!(message, worker_message::Message::Finished(_));
        match self.handle(core, worker_id, message).await {
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
    async fn still_served(&self, core: &MasterCore, worker_id: &str, stream: &Outbound) -> bool {
        if !self.is_current(worker_id, stream).await {
            // Replaced by a newer stream of the worker: what this one still says is out of
            // date, and must not change the newer one's state (B44).
            tracing::warn!(worker_id, "messages of a replaced stream dropped");
            return false;
        }
        if !core.is_live(worker_id) {
            // The worker was taken for lost (its lease) while this stream stayed open: ended,
            // so it registers again and its journal is reconciled; meanwhile its messages
            // change nothing.
            self.streams.lock().await.remove(worker_id);
            tracing::warn!(worker_id, "stream of a worker taken for lost ended");
            return false;
        }
        true
    }

    async fn serve_stream(
        self,
        worker_id: String,
        stream: Outbound,
        inbound: Streaming<pb::WorkerMessage>,
        heard: Arc<AtomicU64>,
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
                stream: stream.sender.downgrade(),
                numbers: Arc::clone(&stream.numbers),
                stream_id: stream.id,
                leading: self.leading.subscribe(),
                clocks: (self.clock, self.steady),
                heard,
                epoch: self.epoch,
                current: (worker_id.clone(), Arc::clone(&self.heard)),
            },
        ));
        let mut closing = self.closing.subscribe();
        // A failed group's messages not redone in its turn, first in the next (M4B.3).
        let mut carried: VecDeque<Turned> = VecDeque::new();
        loop {
            let first = if let Some(first) = carried.pop_front() {
                // As `next_to_handle`: the master stopping lets the stream go (Grok's review).
                if !self.leading_unless_closing().await {
                    reader.abort();
                    return;
                }
                first
            } else {
                match self.next_to_handle(&mut queued, &mut closing).await {
                    Next::Message(first) => Turned::Grouped(*first),
                    Next::Ended => break,
                    // The master stopping: this stream's sender let go, its worker not written
                    // as disconnected (the lease is given back), as the master's end.
                    Next::Closing => {
                        reader.abort();
                        return;
                    }
                }
            };
            // Everything already queued, up to the turn's limit, in one turn of the lock: one
            // message per turn left a worker's messages behind every round in between (B12).
            let turn = fill_turn(first, &mut carried, &mut queued, self.messages_per_turn);
            match self.turn((&worker_id, &stream), turn, &mut carried).await {
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
        let mut streams = self.streams.lock().await;
        if !streams
            .get(&worker_id)
            .is_some_and(|s| s.same_channel(&stream))
        {
            return;
        }
        streams.remove(&worker_id);
        drop(streams);
        let now = (self.clock)();
        if let Err(error) = core.disconnected(&worker_id, now).await {
            tracing::error!(worker_id, %error, "worker disconnect not recorded");
        }
    }

    /// Registers a worker on its stream, and serves the stream from then on. Not to be cancelled
    /// half-way: it changes the core's state in steps (B171).
    async fn open_stream(
        self,
        register: pb::Register,
        inbound: Streaming<pb::WorkerMessage>,
    ) -> Result<Response<ReceiverStream<Result<pb::MasterMessage, Status>>>, Status> {
        // A sign of life as it arrives, before it waits for the lock (B6: a reconnect during
        // a stall); and the attempts its journal reports have started, so none is taken back
        // for want of a `Started` meanwhile (B50: lost with the old stream). Adopted, they are
        // marked started, after which the reconciliation's decision stands.
        self.arrivals
            .arrived(&register.worker_id, (self.steady)((self.clock)()));
        let added: Vec<&str> = register
            .journal
            .iter()
            .filter(|entry| {
                self.arrivals
                    .start_known(&register.worker_id, &entry.attempt_id)
            })
            .map(|entry| entry.attempt_id.as_str())
            .collect();
        let leads = self.leading_unless_closing().await;
        let mut core = self.core_for("register").await;
        // Paused while it waited for the lock (behind a round), or stopping: refused as
        // above, what it added gone.
        if !leads || !*self.leading.borrow() {
            drop(core);
            for attempt_id in &added {
                self.arrivals.start_handled(attempt_id);
            }
            return Err(Status::unavailable(NOT_LEADING));
        }
        // The worker's old stream, no longer live (a live one refuses this registration), is
        // answered no more from here (decision 443): a replaced incarnation's lease runs out
        // from this registration, the master's last word to it, whatever the old process
        // still sends.
        if !core.is_live(&register.worker_id) {
            lock_heard(&self.heard).remove(&register.worker_id);
        }
        let registered = core.register(&register, (self.clock)()).await;
        match &registered {
            // Its journal is the whole truth now: what it reports is adopted (started) or
            // ended, and a start known of an attempt it doesn't report (a `Started` that
            // couldn't be stored, its process gone with a restart) protects nothing any more:
            // never started, it expires.
            Ok(_) => self.arrivals.forget_starts(&register.worker_id),
            // Never to be registered as it is: what it added goes; a start known before (a
            // `Started` that couldn't be stored, its stream still live) stays.
            Err(CoreError::Refused(_)) => {
                for attempt_id in &added {
                    self.arrivals.start_handled(attempt_id);
                }
            }
            // A database failure: the worker registers again with the same journal; kept
            // till then (and gone with the worker if it never comes back, by its lease).
            Err(_) => {}
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
        let (sender, receiver) = mpsc::channel(STREAM_BUFFER + 1 + own);
        // Under the core's lock, as the registration: what an older stream's reader still
        // records is of another stream (B240).
        let stream = Outbound::new(sender, self.arrivals.new_stream(&register.worker_id));
        // A new count of heartbeats for the new stream: its `Register` is heartbeat 0.
        let heard = Arc::new(AtomicU64::new(0));
        let first = pb::MasterMessage {
            message: Some(master_message::Message::Registered(registered)),
            clock: Some(self.stamp(0)),
        };
        stream
            .send(first)
            .map_err(|_| Status::internal("the stream closed at once"))?;
        // The worker's stream from now on, put in place before the lock is let go: a message
        // of the old stream handled after this registration is known to be the old one's
        // (B44).
        {
            let mut streams = self.streams.lock().await;
            // Checked under the streams' lock: a master stopping meanwhile (no longer leading,
            // then closing its streams under that lock) either closes this one or refuses it,
            // never leaving it open for its graceful shutdown to wait for (B200's review).
            if !*self.leading.borrow() {
                return Err(Status::unavailable(NOT_LEADING));
            }
            streams.insert(register.worker_id.clone(), stream.clone());
            lock_heard(&self.heard).insert(register.worker_id.clone(), Arc::clone(&heard));
        }
        drop(core);
        self.deliver(outgoing).await;
        tokio::spawn(
            self.clone()
                .serve_stream(register.worker_id, stream, inbound, heard),
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    /// Ends every worker's stream with `UNAVAILABLE`, for the master to stop: tonic's graceful
    /// shutdown waits for open calls, which a worker's stream never ends by itself, and each
    /// worker connects again, to the master that leads (B200).
    pub async fn close_streams(&self) {
        // Each stream's server lets its sender go: a full one ends once its worker read it.
        self.closing.send_replace(true);
        let streams = std::mem::take(&mut *self.streams.lock().await);
        for stream in streams.into_values() {
            // Full, the worker isn't reading: its stream ends when its other senders go.
            stream.end(Status::unavailable(STOPPING));
        }
    }

    /// Whether `stream` is still `worker_id`'s stream (not replaced by a newer registration).
    async fn is_current(&self, worker_id: &str, stream: &Outbound) -> bool {
        self.streams
            .lock()
            .await
            .get(worker_id)
            .is_some_and(|s| s.same_channel(stream))
    }
}

/// What a worker's stream's server does next.
/// A turn from `first`: the carried messages first, then what is queued, up to `limit`; a
/// failed group's messages, each alone, at most [`ALONE_PER_TURN`] (Grok's review of M4B.3:
/// the carried ones took a whole turn's).
fn fill_turn(
    first: Turned,
    carried: &mut VecDeque<Turned>,
    queued: &mut mpsc::UnboundedReceiver<worker_message::Message>,
    limit: usize,
) -> Vec<Turned> {
    let mut alone = usize::from(matches!(first, Turned::Alone(_)));
    let mut turn = vec![first];
    while turn.len() < limit {
        if alone >= ALONE_PER_TURN && matches!(carried.front(), Some(Turned::Alone(_))) {
            break;
        }
        let Some(next) = carried
            .pop_front()
            .or_else(|| queued.try_recv().ok().map(Turned::Grouped))
        else {
            break;
        };
        alone += usize::from(matches!(next, Turned::Alone(_)));
        turn.push(next);
    }
    turn
}

/// A message of a turn: grouped with the others like it, or (a failed group's, M4B.3) handled
/// alone.
#[derive(Debug)]
enum Turned {
    Grouped(worker_message::Message),
    Alone(worker_message::Message),
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

enum Next {
    /// Handles this message (boxed: a message is far larger than the other cases).
    Message(Box<worker_message::Message>),
    /// The worker's stream ended.
    Ended,
    /// The master closes its streams.
    Closing,
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

fn lock_heard(
    heard: &std::sync::Mutex<BTreeMap<String, Arc<AtomicU64>>>,
) -> std::sync::MutexGuard<'_, BTreeMap<String, Arc<AtomicU64>>> {
    heard
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Each worker's current stream's heartbeat counter (B225), shared by the hub and the
/// streams' readers.
type Counters = Arc<std::sync::Mutex<BTreeMap<String, Arc<AtomicU64>>>>;

/// Answers a worker's heartbeats as they arrive, before they wait for the lock: a busy master
/// isn't a lost one. Not while paused: another master may be leading, and may count the worker
/// lost. The stream is held weakly, for this reader not to keep it open.
struct HeartbeatAcks {
    stream: mpsc::WeakSender<Result<pb::MasterMessage, Status>>,
    /// The stream's numbering, shared with its [`Outbound`] (B240).
    numbers: Arc<std::sync::Mutex<u64>>,
    /// The stream's ID, for [`Arrivals::read`].
    stream_id: u64,
    leading: watch::Receiver<bool>,
    /// The master's wall clock and master time, for the stamp.
    clocks: (fn() -> Timestamp, crate::clock::Steady),
    /// The heartbeats heard on this stream: the latest's number.
    heard: Arc<AtomicU64>,
    /// The lease epoch on the stamp.
    epoch: i64,
    /// Its worker and the hub's counters of the current streams: answered only while `heard`
    /// is its worker's current one (decision 443).
    current: (String, Counters),
}

impl HeartbeatAcks {
    /// The message numbered `sequence` arrived: noted (the latest's number goes on the
    /// master's next stamps, B225), and a heartbeat answered with it on the answer's stamp.
    fn heard(&self, sequence: u64, heartbeat: bool) {
        let heard = self
            .heard
            .fetch_max(sequence, Ordering::SeqCst)
            .max(sequence);
        if heartbeat
            && *self.leading.borrow()
            && self.is_current()
            && let Some(stream) = self.stream.upgrade()
        {
            let ack = pb::MasterMessage {
                message: Some(master_message::Message::HeartbeatAck(pb::HeartbeatAck {})),
                clock: Some(stamp_of(self.clocks, heard, self.epoch)),
            };
            // Full, the worker isn't reading: the next heartbeat tries again.
            let _ = numbered_send(&self.numbers, &stream, ack);
        }
    }

    /// Whether this is its worker's current stream: one replaced by a registration (a new
    /// incarnation, or the same reconnecting) is answered no more.
    fn is_current(&self) -> bool {
        let (worker_id, streams) = &self.current;
        lock_heard(streams)
            .get(worker_id)
            .is_some_and(|heard| Arc::ptr_eq(heard, &self.heard))
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
                arrivals.arrived(&worker_id, acks.steady_now());
                // A heartbeat is answered whatever it carries; its numbers count when stamped.
                let heartbeat =
                    matches!(message.message, Some(worker_message::Message::Heartbeat(_)));
                acks.heard(message.clock.as_ref().map_or(0, |c| c.sequence), heartbeat);
                if let Some(clock) = &message.clock {
                    arrivals.read(&worker_id, acks.stream_id, clock.heard_sequence);
                }
                if let Some(
                    worker_message::Message::Started(pb::Started { attempt_id, .. })
                    | worker_message::Message::Finished(pb::Finished { attempt_id, .. }),
                ) = &message.message
                {
                    arrivals.start_arrived(&worker_id, attempt_id);
                    arrivals.answered(&worker_id, attempt_id);
                }
                if let Some(worker_message::Message::Rejected(pb::Rejected {
                    attempt_id, ..
                })) = &message.message
                {
                    arrivals.answered(&worker_id, attempt_id);
                }
                if let Some(message) = message.message
                    && let Err(mpsc::error::SendError(unsent)) = queue.send(message)
                {
                    // Never to be handled: its start mustn't keep protecting the attempt.
                    if let worker_message::Message::Started(pb::Started { attempt_id, .. })
                    | worker_message::Message::Finished(pb::Finished { attempt_id, .. }) =
                        &unsent
                    {
                        arrivals.start_handled(attempt_id);
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
        let mut inbound = request.into_inner();
        // Not waited for once the master stops: a stream that never sends `Register` would
        // keep tonic's graceful shutdown waiting (B219).
        let mut closing = self.closing.subscribe();
        let first = tokio::select! {
            first = inbound.message() => first?,
            _ = closing.wait_for(|closing| *closing) => {
                return Err(Status::unavailable(STOPPING));
            }
        }
        .ok_or_else(|| Status::invalid_argument("the stream ended before `Register`"))?;
        let Some(worker_message::Message::Register(register)) = first.message else {
            return Err(Status::invalid_argument(
                "the first message must be `Register`",
            ));
        };
        // Not while paused: another master may be leading. Refused at once rather than held
        // (B165), before anything is noted of it: the worker connects again, through the
        // Service, to the master that leads.
        if !*self.leading.borrow() {
            return Err(self.not_leading().await);
        }
        // Apart from this call, which ends when the worker resets its stream (giving up on a
        // slow master): stopped half-way, the worker would stay live with no stream behind it,
        // refused from then on (B171). Finished, its stream's end disconnects it as usual.
        tokio::spawn(self.clone().open_stream(register, inbound))
            .await
            .map_err(|e| Status::internal(format!("the registration failed: {e}")))?
    }

    async fn upload_parse_result(
        &self,
        request: Request<Streaming<pb::ParseResultPart>>,
    ) -> Result<Response<pb::ParseResultAck>, Status> {
        // The header, then a file a message (B35): read whole before the lock is taken.
        let mut parts = request.into_inner();
        let mut upload = ParseUpload::default();
        // Not waited for once the master stops, as `control`'s `Register` isn't (B219's review).
        let mut closing = self.closing.subscribe();
        loop {
            let part = tokio::select! {
                part = parts.message() => part?,
                _ = closing.wait_for(|closing| *closing) => {
                    return Err(Status::unavailable(STOPPING));
                }
            };
            let Some(part) = part else { break };
            upload
                .add(part)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        let result = upload
            .finish()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
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
        let attempt_id = request.into_inner().attempt_id;
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

/// The least time between the end of a round and one a worker's `Finished` wakes (decision
/// 42B): none when the master was idle, so a finish starts its downstream tasks at once; under
/// a stream of finishes at most 20 rounds a second, each taking what arrived meanwhile.
pub const ROUND_GAP: Duration = Duration::from_millis(50);

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
