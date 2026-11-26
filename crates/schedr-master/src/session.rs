//! A worker's session: its current stream, as the hot paths read it without the core's lock
//! (M5.3i, DN43, decision 531).
//!
//! One map of sessions, published with `arc-swap`: a stream's reader, its answers and the
//! master's deliveries load it without a lock; its writer, [`SessionWriter`], is the core's
//! alone (not `Clone`, reached through `&mut MasterCore`), so a session changes only under the
//! core's lock, together with the core's connection and the dispatcher's worker (the
//! invariants: `design/workers.md`, "Invariants of a worker's session"). The one write outside
//! it is [`Sessions::close_all`], when the master stops: it must not wait behind a round, and a
//! registration publishing meanwhile either lands before it, and is closed by it, or sees it
//! and is refused ([`SessionWriter::install`]).
//!
//! A reader may hold the map's version before the latest: what a stream records is checked
//! against Arrivals' latest stream under Arrivals' lock (`Arrivals::registered` numbers it,
//! under the core's lock, before the session is published), and a send to a session just
//! taken out goes to a sender nobody reads, which fails harmlessly or is never read.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use schedr_proto::pb;
use tokio::sync::{mpsc, watch};
use tonic::Status;

/// What carries the master's messages to a worker's stream.
pub type Sender = mpsc::Sender<Result<pb::MasterMessage, Status>>;

/// A worker's current stream (B240): each message's `MasterClock.sequence` taken and the
/// message queued under one lock, by the core's deliveries and the reader's answers alike, so
/// the numbers leave in order.
#[derive(Debug)]
pub struct Session {
    worker_id: String,
    /// Arrivals' number of the stream (`Arrivals::registered`).
    id: u64,
    sender: Sender,
    /// The last number used.
    numbers: std::sync::Mutex<u64>,
    /// The latest heartbeat heard on it (its `Register` is heartbeat 0).
    heard: AtomicU64,
    /// Set once a newer connection of its worker replaced it (decision 525), or the master
    /// stops: its serving and reading stop, and nothing it still brings counts.
    ended: watch::Sender<bool>,
    /// Set when a send found it full (its worker doesn't read it): from then on it counts as
    /// gone for every reader, until the core takes it out (`Hub::disconnect_stuck`).
    stuck: AtomicBool,
}

impl Session {
    /// `worker_id`'s stream numbered `id`, its messages on `sender`.
    #[must_use]
    pub fn new(worker_id: &str, id: u64, sender: Sender) -> Self {
        Self {
            worker_id: worker_id.to_owned(),
            id,
            sender,
            numbers: std::sync::Mutex::default(),
            heard: AtomicU64::new(0),
            ended: watch::channel(false).0,
            stuck: AtomicBool::new(false),
        }
    }

    /// Its worker.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Its number (Arrivals').
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// The latest heartbeat heard on it.
    #[must_use]
    pub fn heard(&self) -> u64 {
        self.heard.load(Ordering::SeqCst)
    }

    /// Heartbeat `number` heard on it.
    pub fn hear(&self, number: u64) {
        self.heard.fetch_max(number, Ordering::SeqCst);
    }

    /// Its worker doesn't read it: gone for every reader from now on.
    pub fn mark_stuck(&self) {
        self.stuck.store(true, Ordering::SeqCst);
    }

    /// Whether [`Self::mark_stuck`] ran.
    #[must_use]
    pub fn is_stuck(&self) -> bool {
        self.stuck.load(Ordering::SeqCst)
    }

    /// Whether it was replaced or closed.
    #[must_use]
    pub fn is_ended(&self) -> bool {
        *self.ended.borrow()
    }

    /// A watch of [`Self::is_ended`], for its serving to stop at once.
    #[must_use]
    pub fn ended(&self) -> watch::Receiver<bool> {
        self.ended.subscribe()
    }

    /// Numbers `message` (stamped already) and queues it: its number. Not queued, the number
    /// isn't used, and the message is dropped.
    ///
    /// # Errors
    ///
    /// Full (its worker doesn't read it) or closed.
    pub fn send(
        &self,
        mut message: pb::MasterMessage,
    ) -> Result<u64, mpsc::error::TrySendError<()>> {
        let mut last = self
            .numbers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let number = *last + 1;
        if let Some(clock) = message.clock.as_mut() {
            clock.sequence = number;
        }
        // The unsent message isn't wanted back: only whether the stream is full or closed.
        self.sender
            .try_send(Ok(message))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
                mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(()),
            })?;
        *last = number;
        Ok(number)
    }

    /// Ends the stream with `status` (unnumbered: no message), its serving and reading
    /// stopped.
    pub fn end(&self, status: Status) {
        self.ended.send_replace(true);
        let _ = self.sender.try_send(Err(status));
    }
}

/// The workers' sessions, read without a lock. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct Sessions {
    table: Arc<ArcSwap<BTreeMap<String, Arc<Session>>>>,
    closing: Arc<AtomicBool>,
}

impl Sessions {
    /// `worker_id`'s current session, if it has one (a stuck one is gone).
    #[must_use]
    pub fn current(&self, worker_id: &str) -> Option<Arc<Session>> {
        self.table
            .load()
            .get(worker_id)
            .filter(|session| !session.is_stuck())
            .cloned()
    }

    /// Whether `session` is still its worker's current one.
    #[must_use]
    pub fn is_current(&self, session: &Session) -> bool {
        !session.is_stuck()
            && self
                .table
                .load()
                .get(session.worker_id())
                .is_some_and(|current| current.id == session.id)
    }

    /// Whether `worker_id` has a session.
    #[must_use]
    pub fn has(&self, worker_id: &str) -> bool {
        self.current(worker_id).is_some()
    }

    /// The latest heartbeat heard on `worker_id`'s current session (0: none yet, or none).
    #[must_use]
    pub fn heard_on(&self, worker_id: &str) -> u64 {
        self.current(worker_id).map_or(0, |session| session.heard())
    }

    /// The master stops: every session ended with `status` and taken out, and none put in
    /// place from here. The one write outside the core's lock (see the module's text).
    pub fn close_all(&self, status: &Status) {
        self.closing.store(true, Ordering::SeqCst);
        let closed = self.table.swap(Arc::default());
        for session in closed.values() {
            session.end(status.clone());
        }
    }

    /// Whether [`Self::close_all`] ran.
    #[must_use]
    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }
}

/// The master is stopping: no session is put in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closing;

/// The sessions' writer: the core's alone, so every change is made under the core's lock.
#[derive(Debug, Default)]
pub struct SessionWriter {
    sessions: Sessions,
}

impl SessionWriter {
    /// The sessions it writes, for the hot paths to read.
    #[must_use]
    pub fn sessions(&self) -> Sessions {
        self.sessions.clone()
    }

    /// Puts `session` in place for its worker: the session it replaced, if any, ended (a newer
    /// connection, decision 525).
    ///
    /// # Errors
    ///
    /// [`Closing`] once the master stops: nothing put in place.
    pub fn install(&mut self, session: &Arc<Session>) -> Result<Option<Arc<Session>>, Closing> {
        let replaced =
            self.publish(|table| table.insert(session.worker_id.clone(), Arc::clone(session)))?;
        if let Some(old) = &replaced {
            old.end(Status::aborted(
                "a newer connection of this worker replaced this stream",
            ));
        }
        Ok(replaced)
    }

    /// Takes `worker_id`'s session out as replaced by a newer connection (decision 525): it
    /// ends, for its worker too if it still hears it, and its serving and reading stop.
    pub fn replace_out(&mut self, worker_id: &str) -> Option<Arc<Session>> {
        let old = self.remove(worker_id, None)?;
        old.end(Status::aborted(
            "a newer connection of this worker replaced this stream",
        ));
        Some(old)
    }

    /// Takes `worker_id`'s session out, if it is `id`'s (any with `None`): the one taken out.
    /// Its stream ends once its own server lets its sender go.
    pub fn remove(&mut self, worker_id: &str, id: Option<u64>) -> Option<Arc<Session>> {
        // A stuck one too: it is still in the map, for the core to take out.
        let current = self.sessions.table.load().get(worker_id).cloned()?;
        if id.is_some_and(|id| id != current.id) {
            return None;
        }
        // Closing meanwhile took it out already.
        self.publish(|table| table.remove(worker_id)).ok().flatten()
    }

    /// A changed copy of the map published, unless the master closes: what `change` returned.
    fn publish<T>(
        &mut self,
        mut change: impl FnMut(&mut BTreeMap<String, Arc<Session>>) -> T,
    ) -> Result<T, Closing> {
        loop {
            let current = self.sessions.table.load_full();
            if self.sessions.is_closing() {
                return Err(Closing);
            }
            let mut next = (*current).clone();
            let changed = change(&mut next);
            let previous = self
                .sessions
                .table
                .compare_and_swap(&current, Arc::new(next));
            // Only closing writes beside this (its swap empties the map): tried again, it sees
            // the flag set before that swap.
            if Arc::ptr_eq(&previous, &current) {
                return Ok(changed);
            }
        }
    }
}

#[cfg(test)]
mod tests;
