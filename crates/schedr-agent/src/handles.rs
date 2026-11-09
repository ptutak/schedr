//! The cleanup handles an attempt registers: what its cleanup needs that only existed in the
//! attempt (see archive/PROJECT.md, Per-task cleanup). Masked as outputs are (decision 222), at
//! most [`MAX_HANDLES_SIZE`] in all (decision 228).

use std::sync::{Arc, Mutex, PoisonError};

use schedr_model::ctx::MAX_CTX_MESSAGE_SIZE;
use schedr_model::runner::CleanupHandle;
use schedr_model::task::TaskCleanup;

use crate::journal::Journal;
use crate::mask::MaskSet;

#[cfg(test)]
mod tests;

/// The most an attempt's handles may take as JSON, in bytes: 1 MiB, less room for what wraps
/// them, so that `schedr task-ctx cleanup list` can read back all that was accepted, in one
/// message of the context channel. They also travel with an assignment (`cleanup_anywhere`),
/// within a worker's 4 MiB message.
pub const MAX_HANDLES_SIZE: usize = MAX_CTX_MESSAGE_SIZE as usize - 1024;

/// Why a handle isn't registered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandleError {
    /// The task has no cleanup to give it to.
    #[error("the task has no cleanup: give it one (`cleanup=`), or don't register handles")]
    NoCleanup,
    /// Registered from the cleanup itself: nothing would clean up after it.
    #[error("a cleanup can't register handles")]
    InCleanup,
    /// The cleanup takes none: it would never see them.
    #[error(
        "the task's cleanup takes no handles: give it a `handles` parameter, or don't register \
         them"
    )]
    NoneTaken,
    /// A type the cleanup doesn't take.
    #[error("handle type `{given}` isn't one its cleanup takes ({taken})")]
    OtherType {
        /// The type given.
        given: String,
        /// The types it takes.
        taken: String,
    },
    /// It can't be journaled: a restart would lose it.
    #[error("the handle can't be written to the journal: {0}")]
    NotJournaled(String),
    /// Over [`MAX_HANDLES_SIZE`].
    #[error(
        "the attempt's handles would take {size} bytes, over the limit of {MAX_HANDLES_SIZE}: \
         keep what is large somewhere else and register where it is"
    )]
    TooLarge {
        /// What they would take.
        size: usize,
    },
}

/// Told each handle once it is journaled, with its place, to send it on to the master.
#[derive(Clone)]
pub struct HandleSink(pub Arc<SendHandle>);

/// How a [`HandleSink`] sends a handle on.
pub type SendHandle = dyn Fn(u32, &CleanupHandle) + Send + Sync;

impl std::fmt::Debug for HandleSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HandleSink")
    }
}

impl PartialEq for HandleSink {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// What an attempt may register.
#[derive(Debug)]
enum Taken {
    /// Nothing: its task has no cleanup.
    NoCleanup,
    /// Any type.
    Any,
    /// These types; none listed, none.
    Only(Vec<String>),
}

#[derive(Debug, Default)]
struct Registered {
    handles: Vec<CleanupHandle>,
    size: usize,
    closed: bool,
}

/// An attempt's handles, in registration order.
#[derive(Debug)]
pub struct AttemptHandles {
    /// The types its cleanup takes (empty: any); `None`: the task has no cleanup.
    taken: Taken,
    mask: Arc<MaskSet>,
    /// Where each is on disk before the task hears it is registered, and under which attempt.
    journal: Option<(Journal, String)>,
    /// Told each handle, after the journal.
    sink: Option<HandleSink>,
    /// Held from a handle's place in the list to its line in the journal: one order in both.
    adding: tokio::sync::Mutex<()>,
    registered: Mutex<Registered>,
}

impl AttemptHandles {
    /// The handles of an attempt of a task with `cleanup`, masked with `mask`.
    #[must_use]
    pub fn new(cleanup: Option<&TaskCleanup>, mask: Arc<MaskSet>) -> Self {
        Self {
            taken: match cleanup {
                None => Taken::NoCleanup,
                Some(TaskCleanup { handles: None, .. }) => Taken::Any,
                Some(TaskCleanup {
                    handles: Some(types),
                    ..
                }) => Taken::Only(types.clone()),
            },
            mask,
            journal: None,
            sink: None,
            adding: tokio::sync::Mutex::new(()),
            registered: Mutex::default(),
        }
    }

    /// These handles journaled, as `attempt_id`'s.
    #[must_use]
    pub fn journaled(mut self, journal: Journal, attempt_id: String) -> Self {
        self.journal = Some((journal, attempt_id));
        self
    }

    /// These handles told to `sink`, once journaled.
    #[must_use]
    pub fn forwarded(mut self, sink: HandleSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// The handles read back from the journal, for a cleanup after a restart: closed.
    #[must_use]
    pub fn restored(handles: Vec<CleanupHandle>, mask: Arc<MaskSet>) -> Self {
        Self {
            taken: Taken::Any,
            mask,
            journal: None,
            sink: None,
            adding: tokio::sync::Mutex::new(()),
            registered: Mutex::new(Registered {
                handles,
                size: 0,
                closed: true,
            }),
        }
    }

    /// Registers a handle; journaled, if the attempt's handles are, when this returns.
    ///
    /// # Errors
    ///
    /// [`HandleError`] if it can't be: the attempt then fails where it registered it.
    pub async fn add(&self, handle: CleanupHandle) -> Result<(), HandleError> {
        match &self.taken {
            Taken::NoCleanup => return Err(HandleError::NoCleanup),
            Taken::Only(types) if types.is_empty() => return Err(HandleError::NoneTaken),
            Taken::Only(types) if !types.contains(&handle.handle_type) => {
                return Err(HandleError::OtherType {
                    given: handle.handle_type,
                    taken: types.join(", "),
                });
            }
            Taken::Any | Taken::Only(_) => {}
        }
        let handle = CleanupHandle {
            value: self.mask.mask_json(&handle.value),
            ..handle
        };
        // As listed: the whole handle and a comma.
        let added = serde_json::to_string(&handle).map_or(usize::MAX, |json| json.len() + 1);
        let _adding = self.adding.lock().await;
        let position = {
            let mut registered = self
                .registered
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if registered.closed {
                return Err(HandleError::InCleanup);
            }
            let size = registered.size.saturating_add(added);
            if size > MAX_HANDLES_SIZE {
                return Err(HandleError::TooLarge { size });
            }
            registered.size = size;
            registered.handles.push(handle.clone());
            u32::try_from(registered.handles.len() - 1).unwrap_or(u32::MAX)
        };
        if let Some((journal, attempt_id)) = &self.journal {
            let (journal, attempt_id) = (journal.clone(), attempt_id.clone());
            let line = handle.clone();
            tokio::task::spawn_blocking(move || journal.add_handle(&attempt_id, &line))
                .await
                .map_err(std::io::Error::other)
                .and_then(|written| written)
                .map_err(|error| HandleError::NotJournaled(error.to_string()))?;
        }
        if let Some(sink) = &self.sink {
            (sink.0)(position, &handle);
        }
        Ok(())
    }

    /// Whether the cleanup has started.
    #[must_use]
    pub fn in_cleanup(&self) -> bool {
        self.registered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed
    }

    /// The handles so far, in registration order.
    #[must_use]
    pub fn list(&self) -> Vec<CleanupHandle> {
        self.registered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handles
            .clone()
    }

    /// Leaves out the first `count` handles: a cleanup that moved had them, this one runs on
    /// the rest (B169).
    pub fn skip(&self, count: usize) {
        let mut registered = self
            .registered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let count = count.min(registered.handles.len());
        registered.handles.drain(..count);
    }

    /// Stops taking handles: the cleanup starts.
    pub fn close(&self) {
        self.registered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
    }
}
