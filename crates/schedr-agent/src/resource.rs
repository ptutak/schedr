//! A command's resource value (M4.8d): what a `CommandResource` setup reports with `schedr
//! task-ctx resource set`, and what a resource's `cleanup_command` reads with `resource get`.
//!
//! The value stays in memory: a setup's attempt is stopped when its agent restarts (and runs
//! again), and once it ends its value is its output, journaled with its result. It is masked
//! then, with every secret the attempt read by its end.

/// The most a value may take as stored JSON: what `resource get` can answer in one message of
/// the context channel, less room for what wraps it.
pub const MAX_VALUE_SIZE: usize = MAX_CTX_MESSAGE_SIZE as usize - 1024;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use schedr_model::ctx::MAX_CTX_MESSAGE_SIZE;
use schedr_model::params::to_reported;
use schedr_model::{ResourceRole, Task, TaskKind, TypeDesc};
use serde_json::Value;

use crate::mask::MaskSet;

#[cfg(test)]
mod tests;

/// Why the value can't be reported or read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResourceError {
    /// `resource set` outside a `CommandResource` setup.
    #[error("`resource set` works only in a `CommandResource`'s or `ScriptResource`'s setup")]
    NotSetup,
    /// `resource get` outside a resource's command cleanup.
    #[error("`resource get` works only in a resource's `cleanup_command` or `cleanup_script`")]
    NotCleanup,
    /// The resource has no value (declared without `value=`).
    #[error("the resource has no value: it was declared without one")]
    NoValue,
    /// Over [`MAX_VALUE_SIZE`] as stored.
    #[error(
        "the value would take {0} bytes, over the limit of {MAX_VALUE_SIZE}: keep what is large \
         somewhere else and report where it is"
    )]
    TooLarge(usize),
    /// Reported twice.
    #[error("the resource's value is reported already; it is reported once")]
    Again,
    /// Not of the declared type; `why` never quotes the value.
    #[error("the value doesn't fit the resource's type: {0}")]
    Misfit(String),
    /// The setup's value didn't reach the cleanup.
    #[error("the setup's value didn't reach the cleanup")]
    Missing,
}

#[derive(Debug)]
enum Part {
    /// A setup: what it reports, of `ty`, masked.
    Setup {
        ty: TypeDesc,
        mask: Arc<MaskSet>,
        reported: Mutex<Option<Value>>,
    },
    /// A cleanup: its setup's value, if it reached it.
    Cleanup(Option<Value>),
}

/// An attempt's resource value.
#[derive(Debug)]
pub struct AttemptResource(Part);

impl AttemptResource {
    /// The resource part of an attempt of `task`, with its `inputs` (by task ID); `None` for a
    /// task that is neither a command setup nor a command cleanup.
    #[must_use]
    pub fn of(task: &Task, inputs: &BTreeMap<String, Value>, mask: Arc<MaskSet>) -> Option<Self> {
        let (TaskKind::Command {
            resource: Some(role),
            ..
        }
        | TaskKind::Script {
            resource: Some(role),
            ..
        }) = &task.kind
        else {
            return None;
        };
        Some(Self(match role {
            ResourceRole::Setup => Part::Setup {
                ty: task.output_type.clone(),
                mask,
                reported: Mutex::new(None),
            },
            ResourceRole::Cleanup { setup } => Part::Cleanup(inputs.get(setup.as_str()).cloned()),
        }))
    }

    /// Reports the value: checked against the type, stored tagged and masked.
    ///
    /// # Errors
    ///
    /// [`ResourceError`] outside a setup, for a resource without a value, a second report or
    /// a value of another type.
    pub fn set(&self, value: &Value) -> Result<(), ResourceError> {
        let Part::Setup { ty, reported, .. } = &self.0 else {
            return Err(ResourceError::NotSetup);
        };
        if *ty == TypeDesc::None {
            return Err(ResourceError::NoValue);
        }
        let stored = to_reported(value, ty).map_err(|m| ResourceError::Misfit(m.to_string()))?;
        let size = serde_json::to_string(&stored).map_or(usize::MAX, |json| json.len());
        if size > MAX_VALUE_SIZE {
            return Err(ResourceError::TooLarge(size));
        }
        let mut reported = reported.lock().unwrap_or_else(PoisonError::into_inner);
        if reported.is_some() {
            return Err(ResourceError::Again);
        }
        // Masked when the attempt ends: a secret read after the report is masked too.
        *reported = Some(stored);
        Ok(())
    }

    /// The setup's value, as stored; `null` for a resource without one (or one that reported
    /// `null`, of an optional type).
    ///
    /// # Errors
    ///
    /// [`ResourceError`] outside a cleanup.
    pub fn get(&self) -> Result<Value, ResourceError> {
        match &self.0 {
            Part::Setup { .. } => Err(ResourceError::NotCleanup),
            Part::Cleanup(None) => Err(ResourceError::Missing),
            Part::Cleanup(Some(value)) => Ok(value.clone()),
        }
    }

    /// What a setup's attempt ends with: `Ok(value)` (null for a resource without one), or
    /// `Err` when it has a value and reported none; `None` for a cleanup.
    #[must_use]
    pub fn outcome(&self) -> Option<Result<Value, ()>> {
        let Part::Setup { ty, mask, reported } = &self.0 else {
            return None;
        };
        let reported = reported.lock().unwrap_or_else(PoisonError::into_inner);
        Some(match (&*reported, ty) {
            (_, TypeDesc::None) => Ok(Value::Null),
            (Some(value), _) => Ok(mask.mask_json(value)),
            (None, _) => Err(()),
        })
    }
}
