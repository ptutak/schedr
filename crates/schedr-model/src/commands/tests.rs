use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::json;

use super::*;

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

#[rstest]
#[case(
    Command::TriggerRun { dag_id: sync(), planned_for: None },
    json!({"kind": "trigger_run", "dag_id": "sync"}),
    "trigger_run"
)]
#[case(
    Command::TriggerRun { dag_id: sync(), planned_for: Some("2026-10-15T03:00:00Z".parse().unwrap()) },
    json!({"kind": "trigger_run", "dag_id": "sync", "planned_for": "2026-10-15T03:00:00Z"}),
    "trigger_run"
)]
#[case(
    Command::CancelRun { run_id: "r1".to_owned() },
    json!({"kind": "cancel_run", "run_id": "r1"}),
    "cancel_run"
)]
#[case(
    Command::PauseDag { dag_id: sync(), cancel_runs: true },
    json!({"kind": "pause_dag", "dag_id": "sync", "cancel_runs": true}),
    "pause_dag"
)]
#[case(
    Command::UnpauseDag { dag_id: sync() },
    json!({"kind": "unpause_dag", "dag_id": "sync"}),
    "unpause_dag"
)]
#[case(
    Command::CancelTask { run_id: "r1".to_owned(), task_id: TaskId::new("load").unwrap() },
    json!({"kind": "cancel_task", "run_id": "r1", "task_id": "load"}),
    "cancel_task"
)]
#[case(
    Command::RetryTask { run_id: "r1".to_owned(), task_id: TaskId::new("load").unwrap() },
    json!({"kind": "retry_task", "run_id": "r1", "task_id": "load"}),
    "retry_task"
)]
fn commands_are_tagged_by_kind(
    #[case] command: Command,
    #[case] stored: serde_json::Value,
    #[case] kind: &str,
) {
    assert_eq!(serde_json::to_value(&command).unwrap(), stored);
    assert_eq!(serde_json::from_value::<Command>(stored).unwrap(), command);
    assert_eq!(command.kind(), kind);
}

#[test]
fn pausing_doesnt_cancel_by_default() {
    let command: Command =
        serde_json::from_value(json!({"kind": "pause_dag", "dag_id": "sync"})).unwrap();
    assert_eq!(
        command,
        Command::PauseDag {
            dag_id: sync(),
            cancel_runs: false
        }
    );
}

#[test]
fn unknown_kinds_and_fields_are_rejected() {
    assert!(serde_json::from_value::<Command>(json!({"kind": "drop_dag", "dag_id": "a"})).is_err());
    assert!(
        serde_json::from_value::<Command>(json!({"kind": "cancel_run", "run_id": "r", "x": 1}))
            .is_err()
    );
}

#[test]
fn statuses_are_snake_case() {
    assert_eq!(
        serde_json::to_value(CommandStatus::Rejected).unwrap(),
        json!("rejected")
    );
}
