//! Failures injected into writes, for tests (decision 98): a test makes the Nth write from now
//! fail, as a database error would, to check that what the caller does leaves nothing
//! half-done. Compiled only for tests (this crate's, or the `testing` feature's).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::database::{Database, DbError};

/// The writes counted since a test armed or reset the count, and the one to fail.
#[derive(Debug, Default)]
pub(crate) struct Faults {
    writes: AtomicU64,
    /// The write to fail, counted from 1; 0 for none.
    fail_at: AtomicU64,
    /// Whether the next commit's answer is lost (it stores).
    lose_commit: AtomicBool,
    /// The transactions committed (M4B.3: a turn's groups counted).
    commits: AtomicU64,
    /// Whether the next commit is cut off before it stores, its answer lost.
    lose_unstored: AtomicBool,
}

impl Faults {
    /// Counts a write about to run; an error if it is the one to fail.
    pub(crate) fn before_write(&self) -> Result<(), DbError> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_at.load(Ordering::SeqCst) == n {
            return Err(DbError::Sqlx(sqlx::Error::Protocol(format!(
                "write {n} failed on purpose (a test)"
            ))));
        }
        Ok(())
    }

    /// Whether this commit is to be cut off unstored (once).
    pub(crate) fn commit_lost_unstored(&self) -> bool {
        self.lose_unstored.swap(false, Ordering::SeqCst)
    }

    /// Counts a transaction committed.
    pub(crate) fn committed(&self) {
        self.commits.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether this commit's answer is to be lost (once).
    pub(crate) fn commit_answer_lost(&self) -> bool {
        self.lose_commit.swap(false, Ordering::SeqCst)
    }
}

impl Database {
    /// Makes the next transaction's commit store it and then fail with
    /// [`DbError::CommitUnknown`], as when the commit's answer is lost on the way back.
    pub fn lose_commit_answer(&self) {
        self.faults().lose_commit.store(true, Ordering::SeqCst);
    }

    /// Makes the next transaction's commit store nothing and fail with
    /// [`DbError::CommitUnknown`], as when the connection drops before the commit lands.
    pub fn lose_commit_unstored(&self) {
        self.faults().lose_unstored.store(true, Ordering::SeqCst);
    }

    /// Counts the writes from now on, failing none (see [`Self::writes`]).
    pub fn count_writes(&self) {
        self.faults().fail_at.store(0, Ordering::SeqCst);
        self.faults().writes.store(0, Ordering::SeqCst);
    }

    /// Makes the `nth` write from now on fail (counted from 1), as a database error would:
    /// inside [`Database::atomically`] the transaction is then rolled back whole.
    pub fn fail_write(&self, nth: u64) {
        self.faults().writes.store(0, Ordering::SeqCst);
        self.faults().fail_at.store(nth, Ordering::SeqCst);
    }

    /// The transactions committed by [`Database::atomically`] so far.
    #[must_use]
    pub fn commits(&self) -> u64 {
        self.faults().commits.load(Ordering::SeqCst)
    }

    /// The writes since [`Self::count_writes`] or [`Self::fail_write`], the failed one
    /// included.
    #[must_use]
    pub fn writes(&self) -> u64 {
        self.faults().writes.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests;
