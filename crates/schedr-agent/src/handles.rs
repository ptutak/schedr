//! The cleanup handles an attempt registers: what its cleanup needs that only existed in the
//! attempt (see PROJECT.md, Per-task cleanup). Masked as outputs are (decision 222), at most
//! [`MAX_HANDLES_SIZE`] in all (decision 228).

use std::sync::{Arc, Mutex, PoisonError};

use schedr_model::ctx::MAX_CTX_MESSAGE_SIZE;
use schedr_model::runner::CleanupHandle;
use schedr_model::task::TaskCleanup;

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
            registered: Mutex::default(),
        }
    }

    /// Registers a handle.
    ///
    /// # Errors
    ///
    /// [`HandleError`] if it can't be: the attempt then fails where it registered it.
    pub fn add(&self, handle: CleanupHandle) -> Result<(), HandleError> {
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
        registered.handles.push(handle);
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

    /// Stops taking handles: the cleanup starts.
    pub fn close(&self) {
        self.registered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
    }
}
