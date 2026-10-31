//! Change detection for DAG roots: polling, the settle time, and the content hash.
//!
//! The agent scans each root on every poll. A changed scan isn't hashed at once: the root must
//! stay unchanged for the settle time, so half-written files are never parsed. Only a new
//! content hash is reported; a scan that changed with the same contents (e.g. files touched)
//! isn't (see PROJECT.md, Code versions and change detection). The first scan after start
//! settles too.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::scan::{Captured, ScanError, Snapshot, capture, remove_capture, scan};

#[cfg(test)]
mod tests;

/// What a [`ChangeDetector`] decided about a scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// The same as the last settled scan.
    Unchanged,
    /// Changed, and not yet unchanged for the settle time: look again after this long.
    Settling(Duration),
    /// Unchanged for the settle time: hash it.
    Settled(Snapshot),
}

/// Decides when a changed root has settled.
#[derive(Debug, Clone)]
pub struct ChangeDetector {
    settle: Duration,
    settled: Option<Snapshot>,
    pending: Option<(Snapshot, Instant)>,
}

impl ChangeDetector {
    /// A detector with the given settle time and no scan seen yet.
    #[must_use]
    pub const fn new(settle: Duration) -> Self {
        Self {
            settle,
            settled: None,
            pending: None,
        }
    }

    /// Takes a scan made at `now`.
    pub fn observe(&mut self, now: Instant, snapshot: Snapshot) -> Observation {
        if self.settled.as_ref() == Some(&snapshot) {
            self.pending = None;
            return Observation::Unchanged;
        }
        let since = match &self.pending {
            Some((pending, since)) if *pending == snapshot => *since,
            _ => now,
        };
        let waited = now.saturating_duration_since(since);
        if waited >= self.settle {
            self.pending = None;
            Observation::Settled(snapshot)
        } else {
            self.pending = Some((snapshot, since));
            Observation::Settling(self.settle.saturating_sub(waited))
        }
    }

    /// Records a settled scan as the current one (after it was hashed).
    pub fn accept(&mut self, snapshot: Snapshot) {
        self.settled = Some(snapshot);
    }
}

/// The result of polling a root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Poll {
    /// Nothing new to report.
    Unchanged,
    /// The root is changing: poll again after this long.
    Settling(Duration),
    /// The root has a new content hash.
    Changed {
        /// The new content hash.
        hash: String,
        /// The scan it was computed from (its `.py` files are the ones to parse).
        snapshot: Snapshot,
    },
}

/// Watches one root: scans it, waits for it to settle and copies and hashes it.
#[derive(Debug, Clone)]
pub struct RootWatcher {
    dir: PathBuf,
    store: PathBuf,
    settle: Duration,
    detector: ChangeDetector,
    code: Option<Captured>,
    snapshot: Option<Snapshot>,
}

impl RootWatcher {
    /// A watcher for the root at `dir`, its code copied into `store` (decision 102), with no
    /// hash yet.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, store: impl Into<PathBuf>, settle: Duration) -> Self {
        Self {
            dir: dir.into(),
            store: store.into(),
            settle,
            detector: ChangeDetector::new(settle),
            code: None,
            snapshot: None,
        }
    }

    /// The root's directory as configured.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where its code is copied.
    #[must_use]
    pub fn store(&self) -> &Path {
        &self.store
    }

    /// The last reported content hash.
    #[must_use]
    pub fn hash(&self) -> Option<&str> {
        self.code.as_ref().map(|code| code.hash.as_str())
    }

    /// The copy of the code the last reported hash is of: what parse jobs and tasks import.
    #[must_use]
    pub fn code_dir(&self) -> Option<&Path> {
        self.code.as_ref().map(|code| code.dir.as_path())
    }

    /// The scan the last reported hash was computed from (its `.py` files are what a parse
    /// job for that hash imports).
    #[must_use]
    pub const fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Scans the root at `now`; when it has settled, copies and hashes it and checks that it
    /// didn't change meanwhile.
    ///
    /// # Errors
    ///
    /// [`ScanError`] if the root can't be scanned, a file can't be read or the copy can't be
    /// written; the next poll tries again.
    pub fn poll(&mut self, now: Instant) -> Result<Poll, ScanError> {
        let snapshot = match self.detector.observe(now, scan(&self.dir)?) {
            Observation::Unchanged => return Ok(Poll::Unchanged),
            Observation::Settling(wait) => return Ok(Poll::Settling(wait)),
            Observation::Settled(snapshot) => snapshot,
        };
        let captured = capture(&snapshot, &self.store)?;
        // Changed while copied (or gone): a copy made for it is of no use, never reported.
        let drop_fresh = || {
            if captured.fresh
                && let Err(error) = remove_capture(&captured.dir)
            {
                tracing::warn!(dir = %captured.dir.display(), %error, "can't remove a copy of DAG code");
            }
        };
        let after = match scan(&self.dir) {
            Ok(after) => after,
            Err(error) => {
                drop_fresh();
                return Err(error);
            }
        };
        if after != snapshot {
            drop_fresh();
            self.detector.observe(now, after);
            return Ok(Poll::Settling(self.settle));
        }
        self.detector.accept(snapshot.clone());
        if self.hash() == Some(captured.hash.as_str()) {
            return Ok(Poll::Unchanged);
        }
        let hash = captured.hash.clone();
        self.code = Some(captured);
        self.snapshot = Some(snapshot.clone());
        Ok(Poll::Changed { hash, snapshot })
    }
}
