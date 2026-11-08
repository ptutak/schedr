//! The master's gRPC service and its loop: workers' `Control` streams, parse uploads and
//! `FetchInputs`, over [`MasterCore`] (see PROJECT.md, Master–worker protocol).
//!
//! A [`Hub`] holds the core and each live worker's outgoing stream; whatever the core decides
//! to send (assignments, cancels, acknowledgements) is delivered through it, from a stream's
//! messages and from the loop's ticks alike.

use std::collections::BTreeMap;
use std::sync::Arc;
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

use crate::core::{Arrivals, CoreError, MasterCore};
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
type Outbound = mpsc::Sender<Result<pb::MasterMessage, Status>>;

/// The core and the workers' outgoing streams.
#[derive(Debug, Clone)]
pub struct Hub {
    core: Arc<Mutex<MasterCore>>,
    /// Each worker's current stream. Held only briefly (a send never waits), so the core's
    /// lock, which can be held while waiting for it, never waits on a slow worker; a delivery
    /// goes out whole under it, one after another.
    streams: Arc<Mutex<BTreeMap<String, Outbound>>>,
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
    /// Its problems now, written with its row (decision 129).
    problems: Problems,
    /// How its loop is paced.
    timings: crate::core::MasterTimings,
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
        Self::with_clock(core, Timestamp::now)
    }

    /// A hub over `core` with another clock (tests).
    #[must_use]
    pub fn with_clock(core: MasterCore, clock: fn() -> Timestamp) -> Self {
        Self {
            beat: Beat::default(),
            refresh_limit: DEFAULT_REFRESH_LIMIT,
            max_message_size: core.settings().max_message_size,
            timings: core.settings().timings,
            metrics: core.metrics().clone(),
            db: core.db().clone(),
            arrivals: core.arrivals(),
            leading: Arc::new(watch::Sender::new(true)),
            finished: Arc::new(tokio::sync::Notify::new()),
            problems: Problems::default(),
            listen_retry: None,
            core: Arc::new(Mutex::new(core)),
            streams: Arc::new(Mutex::new(BTreeMap::new())),
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

    /// The hub reporting its problems to `problems` (its heartbeat writes them).
    #[must_use]
    pub fn with_problems(mut self, problems: Problems) -> Self {
        self.problems = problems;
        self
    }

    /// The hub abandoning a gauge refresh that takes longer than `limit` (it only reads, so
    /// abandoning it is safe; a tick is never abandoned: see PROJECT.md, Observability).
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

    /// Reads the worker registry back (see [`MasterCore::restore`]) at the hub's clock, the
    /// one its lease checks use.
    ///
    /// # Errors
    ///
    /// The core's failure.
    pub async fn restore(&self) -> Result<usize, CoreError> {
        let now = (self.clock)();
        self.core.lock().await.restore(now).await
    }

    /// One tick of the master's loop, its messages delivered.
    ///
    /// # Errors
    ///
    /// The core's failure (the next tick goes on).
    pub async fn tick(&self) -> Result<(), CoreError> {
        // What the round built before a failure is delivered too (B7).
        let (outgoing, result) = self.core.lock().await.tick_partial((self.clock)()).await;
        self.deliver(outgoing).await;
        result
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
            tokio::spawn(async move { hub.keep_leading(leadership, released).await })
        });
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
                listened = next_command(notifications.as_mut(), &mut paused_until, &self.problems) => {
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
                    self.problems.clear(ProblemKind::Notifications);
                    if let Some(notifications) = notifications.as_mut() {
                        notifications.skip_arrived().await;
                    }
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
            self.timed_tick().await;
            round_due = None;
            last_round = Some(tokio::time::Instant::now());
        }
    }

    /// A round, its duration observed.
    async fn timed_tick(&self) {
        let started = std::time::Instant::now();
        match self.tick().await {
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
    /// dropped (an assignment then expires without `Started`; a cancel is sent again when the
    /// worker comes back and reports the attempt in its journal).
    ///
    /// A send never waits: a worker [`STREAM_BUFFER`] messages behind has stopped reading.
    /// Its stream is dropped and the worker disconnected, so it registers again and its
    /// journal is reconciled; it never holds up the master's rounds, registrations or other
    /// workers' messages, nor its messages pile up.
    async fn deliver(&self, outgoing: Vec<Outgoing>) {
        let mut stuck = Vec::new();
        let mut streams = self.streams.lock().await;
        for Outgoing { worker_id, message } in outgoing {
            let Some(stream) = streams.get(&worker_id) else {
                tracing::warn!(worker_id, "no stream for worker; message dropped");
                continue;
            };
            match stream.try_send(Ok(message)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::error!(
                        worker_id,
                        "worker doesn't read its stream ({STREAM_BUFFER} messages waiting): \
                         disconnected"
                    );
                    streams.remove(&worker_id);
                    stuck.push(worker_id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
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

    /// Disconnects a worker whose stream was dropped for not being read, unless it has
    /// registered again since. (Its stream's reader stops at its next message, which ends
    /// the worker's side.)
    async fn disconnect_stuck(self, worker_id: String) {
        let mut core = self.core.lock().await;
        if self.streams.lock().await.contains_key(&worker_id) {
            return;
        }
        if let Err(error) = core.disconnected(&worker_id, (self.clock)()).await {
            tracing::error!(worker_id, %error, "worker disconnect not recorded");
        }
    }

    async fn serve_stream(
        self,
        worker_id: String,
        stream: Outbound,
        inbound: Streaming<pb::WorkerMessage>,
    ) {
        // Read apart from handling: while the master is stuck behind its lock, the worker's
        // messages still arrive, are recorded and wait in order (B6).
        let (queue, mut queued) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read_stream(
            worker_id.clone(),
            inbound,
            self.arrivals.clone(),
            self.clock,
            queue,
            HeartbeatAcks {
                stream: stream.downgrade(),
                leading: self.leading.subscribe(),
            },
        ));
        let mut unhandled = false;
        while let Some(first) = queued.recv().await {
            // Not while paused: another master may be leading.
            self.while_leading().await;
            // Everything already queued, up to MESSAGES_PER_TURN, in one turn of the lock: one
            // message per turn left a worker's messages behind every round in between (B12).
            let mut outgoing = Vec::new();
            {
                let mut core = self.core.lock().await;
                if !self.is_current(&worker_id, &stream).await {
                    // Replaced by a newer stream of the worker: what this one still says is
                    // out of date, and must not change the newer one's state (B44).
                    reader.abort();
                    tracing::warn!(worker_id, "messages of a replaced stream dropped");
                    return;
                }
                if !core.is_live(&worker_id) {
                    // The worker was taken for lost (its lease) while this stream stayed open:
                    // ended, so it registers again and its journal is reconciled; meanwhile
                    // its messages change nothing.
                    reader.abort();
                    self.streams.lock().await.remove(&worker_id);
                    tracing::warn!(worker_id, "stream of a worker taken for lost ended");
                    return;
                }
                let mut next = Some(first);
                let mut handled = 0;
                let mut finished = false;
                while let Some(message) = next.take() {
                    finished |= matches!(message, worker_message::Message::Finished(_));
                    match core.handle(&worker_id, message, (self.clock)()).await {
                        Ok(out) => outgoing.extend(out),
                        // Refused for its data, it would be refused again after a reconnect,
                        // and the worker's later messages with it: dropped, the stream goes on
                        // (B106; a `Finished` is stored as the attempt's failure instead).
                        Err(error) if error.is_data_fault() => {
                            tracing::error!(
                                worker_id,
                                %error,
                                "worker message dropped: the database refuses its data"
                            );
                        }
                        Err(error) => {
                            // Not sent again on this stream (a code report is sent once):
                            // ended, so the worker registers again and reports afresh (B101).
                            tracing::error!(
                                worker_id,
                                %error,
                                "worker message not handled: its stream is ended, so the \
                                 worker registers again"
                            );
                            unhandled = true;
                            break;
                        }
                    }
                    handled += 1;
                    if handled < MESSAGES_PER_TURN {
                        next = queued.try_recv().ok();
                    }
                }
                if finished {
                    self.finished.notify_one();
                }
            }
            self.deliver(outgoing).await;
            if unhandled {
                reader.abort();
                break;
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
        let mut core = self.core.lock().await;
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
        self.arrivals.arrived(&register.worker_id, (self.clock)());
        let added: Vec<&str> = register
            .journal
            .iter()
            .filter(|entry| {
                self.arrivals
                    .start_known(&register.worker_id, &entry.attempt_id)
            })
            .map(|entry| entry.attempt_id.as_str())
            .collect();
        self.while_leading().await;
        let mut core = self.core.lock().await;
        // Paused while it waited for the lock (behind a round): refused as above, what it
        // added gone.
        if !*self.leading.borrow() {
            drop(core);
            for attempt_id in &added {
                self.arrivals.start_handled(attempt_id);
            }
            return Err(Status::unavailable(NOT_LEADING));
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
        let (stream, receiver) = mpsc::channel(STREAM_BUFFER + 1 + own);
        let first = pb::MasterMessage {
            message: Some(master_message::Message::Registered(registered)),
        };
        stream
            .try_send(Ok(first))
            .map_err(|_| Status::internal("the stream closed at once"))?;
        // The worker's stream from now on, put in place before the lock is let go: a message
        // of the old stream handled after this registration is known to be the old one's
        // (B44).
        self.streams
            .lock()
            .await
            .insert(register.worker_id.clone(), stream.clone());
        drop(core);
        self.deliver(outgoing).await;
        tokio::spawn(
            self.clone()
                .serve_stream(register.worker_id, stream, inbound),
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
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

/// Why a stream opened while this master doesn't lead is refused.
const NOT_LEADING: &str = "this master isn't leading (its database lease isn't renewed, or another \
     master took it over): connect again";

/// Answers a worker's heartbeats as they arrive, before they wait for the lock: a busy master
/// isn't a lost one. Not while paused: another master may be leading, and may count the worker
/// lost. The stream is held weakly, for this reader not to keep it open.
struct HeartbeatAcks {
    stream: mpsc::WeakSender<Result<pb::MasterMessage, Status>>,
    leading: watch::Receiver<bool>,
}

impl HeartbeatAcks {
    fn answer(&self) {
        if *self.leading.borrow()
            && let Some(stream) = self.stream.upgrade()
        {
            let ack = master_message::Message::HeartbeatAck(pb::HeartbeatAck {});
            // Full, the worker isn't reading: the next heartbeat tries again.
            let _ = stream.try_send(Ok(pb::MasterMessage { message: Some(ack) }));
        }
    }
}

/// Reads a worker's stream until it ends: records when each message arrived and queues it
/// for handling.
async fn read_stream(
    worker_id: String,
    mut inbound: Streaming<pb::WorkerMessage>,
    arrivals: Arrivals,
    clock: fn() -> Timestamp,
    queue: mpsc::UnboundedSender<worker_message::Message>,
    acks: HeartbeatAcks,
) {
    loop {
        match inbound.message().await {
            Ok(Some(message)) => {
                arrivals.arrived(&worker_id, clock());
                if matches!(message.message, Some(worker_message::Message::Heartbeat(_))) {
                    acks.answer();
                }
                if let Some(
                    worker_message::Message::Started(pb::Started { attempt_id, .. })
                    | worker_message::Message::Finished(pb::Finished { attempt_id, .. }),
                ) = &message.message
                {
                    arrivals.start_arrived(&worker_id, attempt_id);
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
        let first = inbound
            .message()
            .await?
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
            return Err(Status::unavailable(NOT_LEADING));
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
        while let Some(part) = parts.message().await? {
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
            hub.while_leading().await;
            hub.core.lock().await.upload(result, (hub.clock)()).await
        })
        .await
        .map_err(|e| Status::internal(format!("the upload failed: {e}")))?;
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
/// (at most this many `Finished`, each a few writes; the rest waits for the next turn).
pub const MESSAGES_PER_TURN: usize = 10;

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
) -> Listened {
    if let Some(until) = *paused_until {
        tokio::time::sleep_until(until).await;
        *paused_until = None;
        problems.clear(ProblemKind::Notifications);
    }
    match notifications {
        Some(notifications) => match notifications.next().await {
            Ok(()) => Listened::Command,
            Err(error) => Listened::Failed(error),
        },
        None => std::future::pending().await,
    }
}
