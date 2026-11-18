//! The journal writer (M5.2d): the agent's journal writes beside its loop, not in it.
//!
//! The loop hands it a request (an attempt's record to write, or its files to remove) with
//! what to do once it is on disk, and goes on; the writer takes every request pending at that
//! moment as one group ([`Journal::write_many`]: each file written and synced, renamed, then
//! one sync of the directory) and reports each request back, done or failed, through the
//! loop's own channel. Several groups are written at once (`MOST_IN_FLIGHT`), so a request
//! doesn't wait for another attempt's group. Per attempt the order holds: a request waits for
//! the one before it of the same attempt to be on disk. Nothing is sent that a record backs until the loop hears it is on disk, so the
//! journal's rules hold as before: `finished` on disk before `Finished`, the files removed
//! after the acknowledgement.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use tokio::task::JoinSet;

use crate::journal::{Journal, JournalRecord};

#[cfg(test)]
mod tests;

/// The most requests taken as one group.
const MOST_A_GROUP: usize = 64;

/// The most groups written at once (each its own files and syncs): a request doesn't wait
/// for another attempt's group to be on disk.
const MOST_IN_FLIGHT: usize = 4;

/// What is done to an attempt's journal.
#[derive(Debug, Clone)]
pub enum Op {
    /// Its record written (replaced; boxed: a result is large).
    Write(Box<JournalRecord>),
    /// Its files removed.
    Remove,
}

/// A request handled: its attempt, what was to follow, whether it is on disk, and how long its
/// group took.
pub struct Written<T> {
    /// The attempt.
    pub attempt_id: String,
    /// What the loop does now.
    pub then: T,
    /// On disk, or why not.
    pub result: io::Result<()>,
    /// How long its group's writes took.
    pub took: Duration,
    /// Its group's number (each group's once: a metric observed per group, not per record).
    pub group: u64,
}

/// The writer's handle: requests in, and a wait for all of them to be on disk.
pub struct Writer<T> {
    requests: mpsc::UnboundedSender<(String, Op, T)>,
    pending: Arc<(AtomicUsize, Notify)>,
}

impl<T> std::fmt::Debug for Writer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("pending", &self.pending.0.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> Writer<T> {
    /// Starts the writer for `journal`; `report` gets each request once handled. `stall`
    /// delays each group (tests: a slow disk).
    pub fn start(
        journal: Journal,
        report: impl Fn(Written<T>) + Send + 'static,
        stall: Duration,
    ) -> Self {
        let (requests, mut incoming) = mpsc::unbounded_channel::<(String, Op, T)>();
        let pending = Arc::new((AtomicUsize::new(0), Notify::new()));
        let left = Arc::clone(&pending);
        tokio::spawn(async move {
            // Requests not yet in a group: the next ones of an attempt with a write in flight
            // wait here for it.
            let mut later: VecDeque<(String, Op, T)> = VecDeque::new();
            let mut in_flight: BTreeSet<String> = BTreeSet::new();
            let mut running: JoinSet<Vec<Written<T>>> = JoinSet::new();
            // Each group in flight: its attempts, so even one that failed to finish frees them.
            let mut groups: BTreeMap<tokio::task::Id, Vec<String>> = BTreeMap::new();
            let mut number = 0;
            let mut open = true;
            loop {
                while let Ok(request) = incoming.try_recv() {
                    later.push_back(request);
                }
                while running.len() < MOST_IN_FLIGHT {
                    let mut group = Vec::new();
                    let mut taken = BTreeSet::new();
                    let mut rest = VecDeque::new();
                    for request in later.drain(..) {
                        if group.len() < MOST_A_GROUP
                            && !in_flight.contains(&request.0)
                            && taken.insert(request.0.clone())
                        {
                            group.push(request);
                        } else {
                            rest.push_back(request);
                        }
                    }
                    later = rest;
                    if group.is_empty() {
                        break;
                    }
                    number += 1;
                    let ids: Vec<String> = group.iter().map(|(id, _, _)| id.clone()).collect();
                    in_flight.extend(ids.iter().cloned());
                    let task = running.spawn(write_group(journal.clone(), group, stall, number));
                    groups.insert(task.id(), ids);
                }
                if !open && running.is_empty() && later.is_empty() {
                    return;
                }
                tokio::select! {
                    request = incoming.recv(), if open => match request {
                        Some(request) => later.push_back(request),
                        None => open = false,
                    },
                    Some(ended) = running.join_next_with_id() => {
                        let (id, written) = match ended {
                            Ok((id, written)) => (id, written),
                            Err(error) => {
                                tracing::error!(%error, "a journal write group ended unfinished");
                                (error.id(), Vec::new())
                            }
                        };
                        let ids = groups.remove(&id).unwrap_or_default();
                        for attempt_id in &ids {
                            in_flight.remove(attempt_id);
                        }
                        for written in written {
                            report(written);
                        }
                        let count = ids.len();
                        if left.0.fetch_sub(count, Ordering::SeqCst) == count {
                            left.1.notify_waiters();
                        }
                    }
                }
            }
        });
        Self { requests, pending }
    }

    /// Hands `op` on `attempt_id` to the writer; `then` comes back with its outcome.
    pub fn submit(&self, attempt_id: &str, op: Op, then: T) {
        self.pending.0.fetch_add(1, Ordering::SeqCst);
        if self
            .requests
            .send((attempt_id.to_owned(), op, then))
            .is_err()
        {
            // The writer is gone (the runtime ending): nothing will report it.
            self.pending.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Waits until every request handed in so far is handled.
    pub async fn flush(&self) {
        loop {
            // Armed before the count is read: a group ending in between still wakes it.
            let notified = self.pending.1.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.pending.0.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Writes a group (each request of another attempt): its records together, then its removals
/// together; a group write that fails is redone record by record, so each gets its own outcome.
async fn write_group<T: Send + 'static>(
    journal: Journal,
    group: Vec<(String, Op, T)>,
    stall: Duration,
    number: u64,
) -> Vec<Written<T>> {
    let ops: Vec<(String, Op)> = group
        .iter()
        .map(|(id, op, _)| (id.clone(), op.clone()))
        .collect();
    let began = Instant::now();
    let results = tokio::task::spawn_blocking(move || {
        if !stall.is_zero() {
            std::thread::sleep(stall);
        }
        write_ops(&journal, &ops)
    })
    .await
    .unwrap_or_else(|error| {
        let message = error.to_string();
        group
            .iter()
            .map(|_| Err(io::Error::other(message.clone())))
            .collect()
    });
    let took = began.elapsed();
    group
        .into_iter()
        .zip(results)
        .map(|((attempt_id, _, then), result)| Written {
            attempt_id,
            then,
            result,
            took,
            group: number,
        })
        .collect()
}

/// Each op's outcome, in order.
fn write_ops(journal: &Journal, ops: &[(String, Op)]) -> Vec<io::Result<()>> {
    let writes: Vec<(&str, &JournalRecord)> = ops
        .iter()
        .filter_map(|(id, op)| match op {
            Op::Write(record) => Some((id.as_str(), &**record)),
            Op::Remove => None,
        })
        .collect();
    let removes: Vec<&str> = ops
        .iter()
        .filter(|(_, op)| matches!(op, Op::Remove))
        .map(|(id, _)| id.as_str())
        .collect();
    let written_together = writes.is_empty() || journal.write_many(&writes).is_ok();
    let removed_together = removes.is_empty() || journal.remove_many(&removes).is_ok();
    ops.iter()
        .map(|(id, op)| match op {
            Op::Write(_) if written_together => Ok(()),
            Op::Write(record) => journal.write(id, record),
            Op::Remove if removed_together => Ok(()),
            Op::Remove => journal.remove(id),
        })
        .collect()
}
