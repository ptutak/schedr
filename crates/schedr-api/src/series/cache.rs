//! The Metrics endpoints' buckets kept between requests: a bucket whose rows are all written
//! doesn't change, so each request reads only the buckets since the last final one (the
//! user, 2026-11-15: metrics mustn't weigh on schedr; 30 days of 100 processes took seconds a
//! request on SQLite, and the Deployments page asks every 15 seconds in each tab).
//!
//! A bucket is final once it ended [`FINAL_AFTER`] ago: a minute sample is stored within the
//! sampler's grace of its minute (a webserver's late one; `ROLL_GRACE`), a rollup once, and
//! buckets are aligned to the epoch, so a later request's buckets are the same ones. A request
//! for a key waits for one already reading it, so many tabs cost one read.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use jiff::{SignedDuration, Timestamp};
use schedr_db::samples::{Bucket, Latest};

use super::{Range, floor};

#[cfg(test)]
mod tests;

/// How long after a bucket's end its rows are all stored: the sampler's grace (2 minutes)
/// and a minute's write, with room for the clocks of the processes writing them to differ
/// (Codex's review: a webserver stamps its own sample by its clock). A sample stored later than
/// this misses its rollup too (2 minutes), and a webserver that read past it shows it once it
/// starts again.
pub const FINAL_AFTER: SignedDuration = SignedDuration::from_mins(15);

/// The keys kept at most (each range, all processes or one): the least recently used go.
const KEYS: usize = 64;

/// The buckets kept for one key at most: past them a key isn't kept (a deployment of
/// thousands of processes reads each time rather than hold them).
const BUCKETS: usize = 100_000;

/// What is asked for: the samples of each process (all, or one) or the totals, over a range.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    /// The totals (the sparkline) rather than each process's buckets.
    pub totals: bool,
    /// The range.
    pub range: Range,
    /// One process's only.
    pub process: Option<String>,
}

/// One key's final buckets.
#[derive(Debug, Default)]
pub struct Entry {
    /// The buckets before this are final and kept; none: nothing kept yet.
    pub final_to: Option<Timestamp>,
    /// The range's start when they were kept: a request starting before it (the clock went
    /// back) reads all again.
    pub kept_from: Option<Timestamp>,
    /// Each process's final buckets, by process and time.
    pub buckets: Vec<Bucket>,
    /// The totals' final buckets, by time.
    pub totals: Vec<(Timestamp, u64)>,
    /// Each process's latest sample read (rows are only added: a later read replaces it with a
    /// later one).
    pub latest: BTreeMap<String, Latest>,
}

impl Entry {
    /// Where to read from for a request over `[from, to)` in buckets of `bucket_s`: the
    /// first bucket not kept (none kept, or the clock went back: `from`).
    #[must_use]
    pub fn read_from(&self, from: Timestamp, to: Timestamp) -> Timestamp {
        match self.final_to {
            Some(kept) if kept <= to => kept.max(from),
            _ => from,
        }
    }

    /// Forgets what is kept when a request over `[from, to)` isn't covered by it: the clock
    /// went back past its end, or before its start (Codex's review of M5.1j).
    fn clear_unless_covering(&mut self, from: Timestamp, to: Timestamp) {
        if self.final_to.is_some_and(|kept| kept > to)
            || self.kept_from.is_some_and(|start| from < start)
        {
            *self = Self::default();
        }
    }
}

/// The first bucket not final at `to`, in buckets of `bucket_s`.
#[must_use]
pub fn final_to(to: Timestamp, bucket_s: u32) -> Timestamp {
    floor(to.checked_sub(FINAL_AFTER).unwrap_or(to), bucket_s)
}

/// A key's entry, shared by the requests asking for it, and when it was last used.
type Slot = (u64, Arc<tokio::sync::Mutex<Entry>>);

/// The kept buckets of every key.
#[derive(Debug, Default)]
pub struct SeriesCache {
    entries: Mutex<HashMap<Key, Slot>>,
    uses: std::sync::atomic::AtomicU64,
}

impl SeriesCache {
    /// `key`'s entry, locked: a request reading it holds it, the next waits.
    pub async fn entry(
        &self,
        key: Key,
        (from, to): (Timestamp, Timestamp),
    ) -> tokio::sync::OwnedMutexGuard<Entry> {
        let used = self.uses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = {
            let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
            if !entries.contains_key(&key) && entries.len() >= KEYS {
                // Not one a request holds or waits for: another for its key would make a
                // second lock beside it (Codex's review of M5.1j). All held: one key more.
                let oldest = entries
                    .iter()
                    .filter(|(_, (_, slot))| Arc::strong_count(slot) == 1)
                    .min_by_key(|(_, (used, _))| *used)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    entries.remove(&oldest);
                }
            }
            let slot = entries.entry(key).or_default();
            slot.0 = used;
            Arc::clone(&slot.1)
        };
        let mut guard = entry.lock_owned().await;
        guard.clear_unless_covering(from, to);
        guard
    }
}

/// `kept` and `read` (from where `kept` ends) as one list by process and time, those before
/// `from` left out.
#[must_use]
pub fn merged(kept: &[Bucket], read: Vec<Bucket>, from: Timestamp) -> Vec<Bucket> {
    let mut all: Vec<Bucket> = kept.iter().filter(|b| b.at >= from).cloned().collect();
    all.extend(read);
    all.sort_by(|a, b| a.process_id.cmp(&b.process_id).then(a.at.cmp(&b.at)));
    all
}

/// Keeps `buckets`' final ones (before `final_to`) of a range starting `from` in `entry`,
/// unless too many.
pub fn keep_buckets(
    entry: &mut Entry,
    buckets: &[Bucket],
    (from, final_to): (Timestamp, Timestamp),
) {
    if buckets.len() > BUCKETS {
        *entry = Entry::default();
        return;
    }
    entry.buckets = buckets
        .iter()
        .filter(|b| b.at < final_to)
        .cloned()
        .collect();
    entry.final_to = Some(final_to);
    entry.kept_from = Some(from);
}

/// `kept` and `read` totals as one list by time, those before `from` left out; the final
/// ones kept in `entry`.
pub fn merged_totals(
    entry: &mut Entry,
    read: Vec<(Timestamp, u64)>,
    from: Timestamp,
    final_to: Timestamp,
) -> Vec<(Timestamp, u64)> {
    let mut all: Vec<(Timestamp, u64)> = entry
        .totals
        .iter()
        .filter(|(at, _)| *at >= from)
        .copied()
        .collect();
    all.extend(read);
    all.sort_by_key(|(at, _)| *at);
    entry.totals = all
        .iter()
        .filter(|(at, _)| *at < final_to)
        .copied()
        .collect();
    entry.final_to = Some(final_to);
    entry.kept_from = Some(from);
    all
}

/// Each process's latest sample: `kept`'s, replaced by a later one `read`, those before
/// `from` left out.
#[must_use]
pub fn merged_latest(
    kept: &BTreeMap<String, Latest>,
    read: BTreeMap<String, Latest>,
    from: Timestamp,
) -> BTreeMap<String, Latest> {
    let mut all: BTreeMap<String, Latest> = kept
        .iter()
        .filter(|(_, latest)| latest.at >= from)
        .map(|(id, latest)| (id.clone(), latest.clone()))
        .collect();
    for (id, latest) in read {
        if all.get(&id).is_none_or(|old| latest.at > old.at) {
            all.insert(id, latest);
        }
    }
    all
}
