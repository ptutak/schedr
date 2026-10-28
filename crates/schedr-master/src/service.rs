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
use schedr_proto::pb::master_server::{Master, MasterServer};
use schedr_proto::pb::{self, master_message, worker_message};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::core::{Arrivals, CoreError, MasterCore};
use crate::dispatch::Outgoing;
use crate::lease::{Leadership, Renewal};
use crate::metrics::{GAUGE_REFRESH, MasterMetrics};

#[cfg(test)]
mod tests;

/// How many messages wait for a worker before the oldest sender waits.
const STREAM_BUFFER: usize = 1024;

/// The size of one `FetchInputs` chunk.
pub const INPUT_CHUNK: usize = 1024 * 1024;

type Outbound = mpsc::Sender<Result<pb::MasterMessage, Status>>;

/// The core and the workers' outgoing streams.
#[derive(Debug, Clone)]
pub struct Hub {
    core: Arc<Mutex<MasterCore>>,
    streams: Arc<Mutex<BTreeMap<String, Outbound>>>,
    clock: fn() -> Timestamp,
    /// When the loop last turned (the master's liveness).
    beat: Beat,
    metrics: MasterMetrics,
    db: Database,
    /// The longest a gauge refresh may take before it is abandoned.
    refresh_limit: Duration,
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
}

/// Another master took the lease over: this one's view is stale, and it must end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("another master took the lease over: this one stops (restarted, it waits as a standby)")]
pub struct LeaseLost;

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
            metrics: core.metrics().clone(),
            db: core.db().clone(),
            arrivals: core.arrivals(),
            leading: Arc::new(watch::Sender::new(true)),
            finished: Arc::new(tokio::sync::Notify::new()),
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

    /// The hub abandoning a gauge refresh that takes longer than `limit` (it only reads, so
    /// abandoning it is safe; a tick is never abandoned: see PROJECT.md, Observability).
    #[must_use]
    pub const fn with_refresh_limit(mut self, limit: Duration) -> Self {
        self.refresh_limit = limit;
        self
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
        } else if changed {
            tracing::warn!("the master's lease isn't renewed in time: pausing until it is");
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
        MasterServer::new(self.clone())
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

    async fn run_with(
        &self,
        interval: Duration,
        mut notifications: Option<CommandNotifications>,
        leadership: Option<Leadership>,
        stop: impl Future<Output = ()>,
    ) -> Result<(), LeaseLost> {
        enum Wake {
            Stop,
            Tick,
            Command,
            Finished,
            NotificationsFailed(DbError),
            Alive,
            Refresh,
            LeaseLost,
            ListenAgain,
        }
        let (release, released) = tokio::sync::oneshot::channel();
        let mut keeper = leadership.map(|leadership| {
            self.set_leading(leadership.leads(std::time::Instant::now()));
            let hub = self.clone();
            tokio::spawn(async move { hub.keep_leading(leadership, released).await })
        });
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut alive = tokio::time::interval(ALIVE_INTERVAL);
        let mut refresh = tokio::time::interval(GAUGE_REFRESH);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut paused_until: Option<tokio::time::Instant> = None;
        let mut last_round: Option<tokio::time::Instant> = None;
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
                result = next_command(notifications.as_mut(), paused_until) => match result {
                    Ok(()) => Wake::Command,
                    Err(error) => Wake::NotificationsFailed(error),
                },
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
                Wake::Tick | Wake::Command | Wake::Finished if !leads => continue,
                Wake::Refresh => {
                    self.refresh_gauges().await;
                    continue;
                }
                Wake::Command => {
                    paused_until = None;
                    if let Some(notifications) = notifications.as_mut() {
                        notifications.skip_arrived().await;
                    }
                    ticks.reset();
                }
                Wake::NotificationsFailed(error) => {
                    tracing::warn!(
                        %error,
                        "command notifications failed; polling, trying again in {}s",
                        NOTIFICATION_RETRY.as_secs()
                    );
                    paused_until = Some(tokio::time::Instant::now() + NOTIFICATION_RETRY);
                    continue;
                }
                Wake::Finished => {
                    // At most one round every ROUND_GAP: under a stream of finishes each
                    // round takes what arrived meanwhile, and the lease's renewals and the
                    // workers' messages get their turns.
                    if let Some(ended) = last_round {
                        tokio::time::sleep_until(ended + ROUND_GAP).await;
                    }
                    ticks.reset();
                }
                Wake::Tick => {}
            }
            self.timed_tick().await;
            last_round = Some(tokio::time::Instant::now());
        }
    }

    /// A round, its duration observed.
    async fn timed_tick(&self) {
        let started = std::time::Instant::now();
        if let Err(error) = self.tick().await {
            tracing::error!(%error, "master tick failed");
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
    async fn deliver(&self, outgoing: Vec<Outgoing>) {
        let streams = self.streams.lock().await;
        for Outgoing { worker_id, message } in outgoing {
            if let Some(stream) = streams.get(&worker_id) {
                if stream.send(Ok(message)).await.is_err() {
                    tracing::warn!(worker_id, "worker stream closed; message dropped");
                }
            } else {
                tracing::warn!(worker_id, "no stream for worker; message dropped");
            }
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
        ));
        while let Some(first) = queued.recv().await {
            // Not while paused: another master may be leading.
            self.while_leading().await;
            // Everything already queued, up to MESSAGES_PER_TURN, in one turn of the lock: one
            // message per turn left a worker's messages behind every round in between (B12).
            let mut outgoing = Vec::new();
            {
                let mut core = self.core.lock().await;
                let mut next = Some(first);
                let mut handled = 0;
                let mut finished = false;
                while let Some(message) = next.take() {
                    finished |= matches!(message, worker_message::Message::Finished(_));
                    match core.handle(&worker_id, message, (self.clock)()).await {
                        Ok(out) => outgoing.extend(out),
                        Err(error) => {
                            tracing::error!(worker_id, %error, "worker message not handled");
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
        }
        if let Err(error) = reader.await {
            tracing::error!(worker_id, %error, "worker stream reader failed");
        }
        let now = (self.clock)();
        if let Err(error) = self.core.lock().await.disconnected(&worker_id, now).await {
            tracing::error!(worker_id, %error, "worker disconnect not recorded");
        }
        let mut streams = self.streams.lock().await;
        if streams
            .get(&worker_id)
            .is_some_and(|s| s.same_channel(&stream))
        {
            streams.remove(&worker_id);
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
) {
    loop {
        match inbound.message().await {
            Ok(Some(message)) => {
                arrivals.arrived(&worker_id, clock());
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
        // A sign of life as it arrives, before it waits for the lock (B6: a reconnect during
        // a stall).
        self.arrivals.arrived(&register.worker_id, (self.clock)());
        // Not while paused: another master may be leading.
        self.while_leading().await;
        let registered = self
            .core
            .lock()
            .await
            .register(&register, (self.clock)())
            .await;
        let (registered, outgoing) = registered.map_err(|error| match error {
            CoreError::Refused(reason) => Status::failed_precondition(reason),
            other => Status::internal(other.to_string()),
        })?;
        let (stream, receiver) = mpsc::channel(STREAM_BUFFER);
        let first = pb::MasterMessage {
            message: Some(master_message::Message::Registered(registered)),
        };
        stream
            .send(Ok(first))
            .await
            .map_err(|_| Status::internal("the stream closed at once"))?;
        self.streams
            .lock()
            .await
            .insert(register.worker_id.clone(), stream.clone());
        self.deliver(outgoing).await;
        tokio::spawn(
            self.clone()
                .serve_stream(register.worker_id, stream, inbound),
        );
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn upload_parse_result(
        &self,
        request: Request<pb::ParseResult>,
    ) -> Result<Response<pb::ParseResultAck>, Status> {
        self.while_leading().await;
        let uploaded = self
            .core
            .lock()
            .await
            .upload(request.into_inner(), (self.clock)())
            .await;
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

/// How often the loop beats while it waits between ticks (liveness, see [`Hub::beat`]).
pub const ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// The next command notification, not before `paused_until`; never, without notifications.
async fn next_command(
    notifications: Option<&mut CommandNotifications>,
    paused_until: Option<tokio::time::Instant>,
) -> Result<(), DbError> {
    match notifications {
        Some(notifications) => {
            if let Some(until) = paused_until {
                tokio::time::sleep_until(until).await;
            }
            notifications.next().await
        }
        None => std::future::pending().await,
    }
}
