use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

const ALL_STATUSES: [AttemptStatus; 8] = [
    AttemptStatus::Success,
    AttemptStatus::Failed,
    AttemptStatus::TimedOut,
    AttemptStatus::OomKilled,
    AttemptStatus::Cancelled,
    AttemptStatus::Interrupted,
    AttemptStatus::Lost,
    AttemptStatus::AssignmentExpired,
];

#[test]
fn attempt_statuses_round_trip() {
    for status in ALL_STATUSES {
        let proto = attempt_status_to_proto(status);
        assert_eq!(
            attempt_status_from_proto(proto as i32),
            Ok(status),
            "{status}"
        );
    }
}

#[test]
fn attempt_status_names_match_the_model() {
    for status in ALL_STATUSES {
        let proto_name = attempt_status_to_proto(status).as_str_name();
        assert_eq!(
            proto_name,
            format!("ATTEMPT_STATUS_{}", status.to_string().to_uppercase())
        );
    }
}

#[rstest]
#[case(0)]
#[case(99)]
#[case(-1)]
fn unspecified_and_unknown_statuses_are_errors(#[case] value: i32) {
    let err = attempt_status_from_proto(value).unwrap_err();
    assert_eq!(
        err,
        ConversionError::UnknownEnumValue {
            field: "Finished.status",
            value
        }
    );
    assert_eq!(
        err.to_string(),
        format!("Finished.status: unspecified or unknown value {value}")
    );
}

#[rstest]
#[case(RunKind::Scheduled, "scheduled")]
#[case(RunKind::Manual, "manual")]
#[case(RunKind::Backfill, "backfill")]
#[case(RunKind::Triggered, "triggered")]
fn run_kinds_travel_as_their_model_names(#[case] kind: RunKind, #[case] name: &str) {
    assert_eq!(run_kind_to_proto(kind), name);
    assert_eq!(run_kind_from_proto(name), Ok(kind));
}

#[test]
fn unknown_run_kinds_are_errors() {
    let err = run_kind_from_proto("cron").unwrap_err();
    assert_eq!(err, ConversionError::UnknownRunKind("cron".into()));
    assert_eq!(err.to_string(), "RunInfo.kind: unknown run kind `cron`");
}

#[rstest]
#[case(pb::RejectReason::NoFreeSlot, RejectReaction::Temporary)]
#[case(pb::RejectReason::Draining, RejectReaction::Temporary)]
#[case(pb::RejectReason::InterpreterUndefined, RejectReaction::WorkerSpecific)]
#[case(pb::RejectReason::ScriptRootUnknown, RejectReaction::WorkerSpecific)]
#[case(
    pb::RejectReason::ScriptReferenceNotFound,
    RejectReaction::CodeNotThereYet
)]
#[case(pb::RejectReason::DagFileMissing, RejectReaction::CodeNotThereYet)]
#[case(pb::RejectReason::MemoryBudgetTooLarge, RejectReaction::WorkerSpecific)]
#[case(pb::RejectReason::DiskNotWritable, RejectReaction::WorkerSpecific)]
#[case(pb::RejectReason::InvalidSpecification, RejectReaction::WorkerSpecific)]
fn rejection_reactions_follow_the_rejection_table(
    #[case] reason: pb::RejectReason,
    #[case] expected: RejectReaction,
) {
    assert_eq!(reject_reaction(reason as i32), Ok(expected));
}

#[rstest]
#[case(0)]
#[case(42)]
fn unspecified_and_unknown_reasons_are_errors(#[case] value: i32) {
    assert_eq!(
        reject_reaction(value),
        Err(ConversionError::UnknownEnumValue {
            field: "Rejected.reason",
            value
        })
    );
}

fn parsed_file() -> ParsedFile {
    serde_json::from_value(serde_json::json!({
        "path": "team/sync.py",
        "dags": [{
            "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "team/sync.py"},
            "schedule": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
            "catchup": false, "max_active_runs": 1, "tasks": [], "edges": []
        }],
        "error": null
    }))
    .unwrap()
}

#[test]
fn parsed_files_round_trip() {
    let file = parsed_file();
    assert_eq!(
        parsed_file_from_proto(parsed_file_to_proto(&file)).unwrap(),
        file
    );
    let failed = ParsedFile {
        path: "bad.py".to_owned(),
        dags: vec![],
        error: Some(ErrorSummary {
            exception: "ImportError".to_owned(),
            message: "no module".to_owned(),
            traceback: "Traceback ...".to_owned(),
        }),
    };
    let proto = parsed_file_to_proto(&failed);
    assert_eq!(
        proto.import_error.as_ref().unwrap().exception,
        "ImportError"
    );
    assert_eq!(parsed_file_from_proto(proto).unwrap(), failed);
}

#[test]
fn invalid_dag_json_names_the_file_and_the_dag() {
    let mut proto = parsed_file_to_proto(&parsed_file());
    proto.dag_json.push(b"{\"id\": 3}".to_vec());
    let error = parsed_file_from_proto(proto).unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("ParsedFile `team/sync.py`: DAG 1 isn't valid DAG JSON: "),
        "{error}"
    );
}
