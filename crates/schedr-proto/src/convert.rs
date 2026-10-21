//! Conversions between protocol enums and `schedr-model`, and the master's reaction to each
//! rejection reason.

use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::{AttemptStatus, RunKind};

use crate::pb;

#[cfg(test)]
mod tests;

/// A protocol value that has no `schedr-model` equivalent.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    /// An enum value that is unspecified (0) or unknown to this version.
    #[error("{field}: unspecified or unknown value {value}")]
    UnknownEnumValue {
        /// The field, e.g. `Finished.status`.
        field: &'static str,
        /// The raw value received.
        value: i32,
    },
    /// A run kind name that isn't a `RunKind`.
    #[error("RunInfo.kind: unknown run kind `{0}`")]
    UnknownRunKind(String),
    /// A DAG in a parsed file that isn't valid DAG JSON.
    #[error("ParsedFile `{path}`: DAG {index} isn't valid DAG JSON: {reason}")]
    InvalidDagJson {
        /// The file.
        path: String,
        /// The DAG's position in the file's list.
        index: usize,
        /// Why it doesn't parse.
        reason: String,
    },
}

/// The protocol's form of a parsed file: each DAG as DAG JSON bytes.
///
/// # Panics
///
/// Never: model DAGs always serialize (their maps have string keys).
#[must_use]
pub fn parsed_file_to_proto(file: &ParsedFile) -> pb::ParsedFile {
    pb::ParsedFile {
        path: file.path.clone(),
        dag_json: file
            .dags
            .iter()
            .map(|dag| serde_json::to_vec(dag).expect("a DAG always serializes"))
            .collect(),
        import_error: file.error.as_ref().map(|e| pb::ImportError {
            exception: e.exception.clone(),
            message: e.message.clone(),
            traceback: e.traceback.clone(),
        }),
    }
}

/// A parsed file from its protocol form.
///
/// # Errors
///
/// Returns [`ConversionError::InvalidDagJson`] for a DAG that doesn't parse as DAG JSON (the
/// master's validation then reports the problems of DAGs that do).
pub fn parsed_file_from_proto(file: pb::ParsedFile) -> Result<ParsedFile, ConversionError> {
    let dags = file
        .dag_json
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            serde_json::from_slice(bytes).map_err(|e| ConversionError::InvalidDagJson {
                path: file.path.clone(),
                index,
                reason: e.to_string(),
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(ParsedFile {
        dags,
        error: file.import_error.map(|e| ErrorSummary {
            exception: e.exception,
            message: e.message,
            traceback: e.traceback,
        }),
        path: file.path,
    })
}

/// The protocol's attempt status for a model status.
#[must_use]
pub const fn attempt_status_to_proto(status: AttemptStatus) -> pb::AttemptStatus {
    match status {
        AttemptStatus::Success => pb::AttemptStatus::Success,
        AttemptStatus::Failed => pb::AttemptStatus::Failed,
        AttemptStatus::TimedOut => pb::AttemptStatus::TimedOut,
        AttemptStatus::OomKilled => pb::AttemptStatus::OomKilled,
        AttemptStatus::Cancelled => pb::AttemptStatus::Cancelled,
        AttemptStatus::Interrupted => pb::AttemptStatus::Interrupted,
        AttemptStatus::Lost => pb::AttemptStatus::Lost,
        AttemptStatus::AssignmentExpired => pb::AttemptStatus::AssignmentExpired,
    }
}

/// The model status for a raw protocol value (as found in `Finished.status`).
///
/// # Errors
///
/// Returns [`ConversionError::UnknownEnumValue`] for `UNSPECIFIED` or an unknown value.
pub fn attempt_status_from_proto(value: i32) -> Result<AttemptStatus, ConversionError> {
    let unknown = ConversionError::UnknownEnumValue {
        field: "Finished.status",
        value,
    };
    Ok(
        match pb::AttemptStatus::try_from(value).map_err(|_| unknown.clone())? {
            pb::AttemptStatus::Unspecified => return Err(unknown),
            pb::AttemptStatus::Success => AttemptStatus::Success,
            pb::AttemptStatus::Failed => AttemptStatus::Failed,
            pb::AttemptStatus::TimedOut => AttemptStatus::TimedOut,
            pb::AttemptStatus::OomKilled => AttemptStatus::OomKilled,
            pb::AttemptStatus::Cancelled => AttemptStatus::Cancelled,
            pb::AttemptStatus::Interrupted => AttemptStatus::Interrupted,
            pb::AttemptStatus::Lost => AttemptStatus::Lost,
            pb::AttemptStatus::AssignmentExpired => AttemptStatus::AssignmentExpired,
        },
    )
}

/// The name a run kind travels as in `RunInfo.kind`.
#[must_use]
pub fn run_kind_to_proto(kind: RunKind) -> String {
    kind.to_string()
}

/// The run kind for a `RunInfo.kind` name.
///
/// # Errors
///
/// Returns [`ConversionError::UnknownRunKind`] for a name that isn't a `RunKind`.
pub fn run_kind_from_proto(name: &str) -> Result<RunKind, ConversionError> {
    match name {
        "scheduled" => Ok(RunKind::Scheduled),
        "manual" => Ok(RunKind::Manual),
        "backfill" => Ok(RunKind::Backfill),
        "triggered" => Ok(RunKind::Triggered),
        other => Err(ConversionError::UnknownRunKind(other.to_owned())),
    }
}

/// How the master reacts to a rejection (see PROJECT.md, Task rejections). A rejection never
/// uses up a retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectReaction {
    /// A momentary condition: requeue at once, any worker.
    Temporary,
    /// This worker can't run it: don't offer this attempt to it again; when every eligible
    /// worker rejected it, the task is `unschedulable`.
    WorkerSpecific,
    /// The code isn't on the worker yet (git-sync lagging): retry after roughly the sync
    /// interval plus settle time, any worker.
    CodeNotThereYet,
}

/// The master's reaction to a raw `Rejected.reason`.
///
/// # Errors
///
/// Returns [`ConversionError::UnknownEnumValue`] for `UNSPECIFIED` or an unknown value.
pub fn reject_reaction(reason: i32) -> Result<RejectReaction, ConversionError> {
    use pb::RejectReason as R;
    let unknown = ConversionError::UnknownEnumValue {
        field: "Rejected.reason",
        value: reason,
    };
    Ok(match R::try_from(reason).map_err(|_| unknown.clone())? {
        R::Unspecified => return Err(unknown),
        R::NoFreeSlot | R::Draining => RejectReaction::Temporary,
        R::ScriptReferenceNotFound | R::DagFileMissing => RejectReaction::CodeNotThereYet,
        R::InterpreterUndefined
        | R::ScriptRootUnknown
        | R::MemoryBudgetTooLarge
        | R::DiskNotWritable
        | R::InvalidSpecification => RejectReaction::WorkerSpecific,
    })
}
