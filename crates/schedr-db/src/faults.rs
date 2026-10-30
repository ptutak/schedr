//! Failures injected into writes, for tests (decision 98): a test makes the Nth write from now
//! fail, as a database error would, to check that what the caller does leaves nothing
//! half-done. Compiled only for tests (this crate's, or the `testing` feature's).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::database::{Database, DbError};

/// The writes counted since a test armed or reset the count, and the one to fail.
#[derive(Debug, Default)]
pub(crate) struct Faults {
    writes: AtomicU64,
    /// The write to fail, counted from 1; 0 for none.
    fail_at: AtomicU64,
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
}

impl Database {
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

    /// The writes since [`Self::count_writes`] or [`Self::fail_write`], the failed one
    /// included.
    #[must_use]
    pub fn writes(&self) -> u64 {
        self.faults().writes.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests;
