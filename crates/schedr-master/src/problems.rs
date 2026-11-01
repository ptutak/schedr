//! A master's problems now (decision 129): what its heartbeat writes to its row, shown on its
//! node on the Deployments page. Each kind of problem is one entry, set while it lasts and
//! cleared once it is over.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use schedr_db::nodes::NodeProblem;

#[cfg(test)]
mod tests;

/// The kinds of problem a master reports, each at most once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProblemKind {
    /// Its lease isn't renewed in time: it pauses until it is.
    Lease,
    /// A round failed.
    Round,
    /// It can't listen for new commands: it polls.
    Notifications,
}

/// A master's problems now, shared by its parts (cloned, the same list).
#[derive(Debug, Clone, Default)]
pub struct Problems(Arc<Mutex<BTreeMap<ProblemKind, NodeProblem>>>);

impl Problems {
    /// `kind` is a problem now: an error or a warning, saying `message`.
    pub fn set(&self, kind: ProblemKind, error: bool, message: impl Into<String>) {
        self.map().insert(
            kind,
            NodeProblem {
                error,
                message: message.into(),
            },
        );
    }

    /// `kind` is over.
    pub fn clear(&self, kind: ProblemKind) {
        self.map().remove(&kind);
    }

    /// The problems now, in a fixed order.
    #[must_use]
    pub fn now(&self) -> Vec<NodeProblem> {
        self.map().values().cloned().collect()
    }

    fn map(&self) -> std::sync::MutexGuard<'_, BTreeMap<ProblemKind, NodeProblem>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
