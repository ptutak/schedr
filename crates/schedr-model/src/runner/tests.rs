use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn hello_is_tagged_by_type() {
    let message = RunnerMessage::Hello {
        schedr_version: "0.1.0".into(),
        mode: RunnerMode::Parse,
    };
    let expected = json!({"type": "hello", "schedr_version": "0.1.0", "mode": "parse"});
    assert_eq!(serde_json::to_value(&message).unwrap(), expected);
    assert_eq!(
        serde_json::from_value::<RunnerMessage>(expected).unwrap(),
        message
    );
}

#[test]
fn file_messages_round_trip() {
    for message in [
        RunnerMessage::FileStarted {
            path: "team_a/sync.py".into(),
        },
        RunnerMessage::FileParsed {
            path: "helpers.py".into(),
            dags: vec![],
            error: None,
        },
        RunnerMessage::FileParsed {
            path: "broken.py".into(),
            dags: vec![],
            error: Some(ErrorSummary {
                exception: "ModuleNotFoundError".into(),
                message: "No module named 'pandas'".into(),
                traceback: "  File \"broken.py\", line 1".into(),
            }),
        },
        RunnerMessage::ParseFinished,
    ] {
        let json = serde_json::to_value(&message).unwrap();
        assert_eq!(
            serde_json::from_value::<RunnerMessage>(json).unwrap(),
            message
        );
    }
    assert_eq!(
        serde_json::to_value(RunnerMessage::ParseFinished).unwrap(),
        json!({"type": "parse_finished"})
    );
}

#[test]
fn file_parsed_carries_typed_dags() {
    let json = json!({
        "type": "file_parsed", "path": "x.py", "error": null,
        "dags": [{
            "produced_by": "0.1", "id": "x", "location": {"root": "r", "path": "x.py"},
            "schedule": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
            "catchup": false, "max_active_runs": 1, "tasks": [], "edges": []
        }]
    });
    let RunnerMessage::FileParsed { dags, .. } = serde_json::from_value(json).unwrap() else {
        panic!("not file_parsed")
    };
    assert_eq!(dags[0].id.as_str(), "x");
}

#[test]
fn agent_messages_round_trip() {
    let parse = AgentMessage::Parse(ParseRequest {
        root: "etl-repo".into(),
        root_dir: "/git/dags/etl-repo".into(),
        files: vec!["team_a/sync.py".into()],
        sys_path: vec!["/git/dags/etl-repo".into()],
    });
    assert_eq!(
        serde_json::to_value(&parse).unwrap(),
        json!({
            "type": "parse", "root": "etl-repo", "root_dir": "/git/dags/etl-repo",
            "files": ["team_a/sync.py"], "sys_path": ["/git/dags/etl-repo"]
        })
    );
    for message in [
        parse,
        AgentMessage::Welcome,
        AgentMessage::Refused { reason: "x".into() },
    ] {
        let json = serde_json::to_value(&message).unwrap();
        assert_eq!(
            serde_json::from_value::<AgentMessage>(json).unwrap(),
            message
        );
    }
}

#[test]
fn unknown_types_and_fields_are_rejected() {
    assert!(serde_json::from_value::<RunnerMessage>(json!({"type": "shout"})).is_err());
    assert!(
        serde_json::from_value::<RunnerMessage>(
            json!({"type": "file_started", "path": "a", "x": 1})
        )
        .is_err()
    );
}

#[test]
fn the_runner_version_must_match_exactly() {
    assert_eq!(check_runner_version("0.1.0", "0.1.0"), Ok(()));
    let err = check_runner_version("0.1.0", "0.1.1").unwrap_err();
    assert_eq!(
        err,
        "the runner's schedr package is 0.1.1, the agent is 0.1.0: install schedr==0.1.0 in the \
         interpreter's environment"
    );
}

#[test]
fn the_message_size_cap_is_64_mib() {
    assert_eq!(MAX_MESSAGE_SIZE, 67_108_864);
}

fn request() -> TaskRequest {
    TaskRequest {
        dag_file: DagFile {
            root: "etl".into(),
            root_dir: "/git/dags/etl".into(),
            path: "sync.py".into(),
        },
        dag_id: "sync".into(),
        task: serde_json::from_value(json!({
            "id": "load", "kind": "python",
            "arguments": {"rows": {"ref": {"kind": "output", "task": "extract"}}},
            "settings": {"retries": 0, "retry_delay_ms": 300_000, "timeout_ms": null,
                         "queue": "default", "max_output_size": 1_048_576},
            "output_type": {"type": "int"},
        }))
        .unwrap(),
        inputs: [("extract".to_owned(), json!([1, 2]))].into(),
        run: RunInfo {
            dag_id: "sync".into(),
            run_id: "0192c4e1".into(),
            kind: crate::states::RunKind::Manual,
            planned_for: "2026-10-15T03:00:00Z".parse().unwrap(),
            since: "2026-10-14T03:00:00Z".parse().unwrap(),
            try_number: 1,
            map_index: None,
        },
        sys_path: vec!["/git/dags/etl".into()],
    }
}

#[test]
fn task_requests_round_trip() {
    let message = AgentMessage::Task(Box::new(request()));
    let json = serde_json::to_value(&message).unwrap();
    assert_eq!(json["type"], "task");
    assert_eq!(json["dag_file"]["path"], "sync.py");
    assert_eq!(json["run"]["planned_for"], "2026-10-15T03:00:00Z");
    assert_eq!(json["run"]["kind"], "manual");
    assert_eq!(
        serde_json::from_value::<AgentMessage>(json).unwrap(),
        message
    );
}

#[test]
fn task_results_round_trip() {
    for message in [
        RunnerMessage::TaskSucceeded {
            output: json!(3),
            outcome: "SUCCESS".into(),
        },
        RunnerMessage::TaskFailed {
            error: ErrorSummary {
                exception: "ValueError".into(),
                message: "bad".into(),
                traceback: String::new(),
            },
            kind: Some("output_not_serializable".into()),
        },
    ] {
        let json = serde_json::to_value(&message).unwrap();
        assert_eq!(
            serde_json::from_value::<RunnerMessage>(json).unwrap(),
            message
        );
    }
}
