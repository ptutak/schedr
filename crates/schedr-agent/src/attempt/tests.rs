use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::status::{StopReason, Termination};

fn task(kind: &serde_json::Value) -> serde_json::Value {
    let mut task = json!({
        "id": "load",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": 5000,
            "queue": "default", "max_output_size": 1024
        },
        "output_type": {"type": "any"}
    });
    for (key, value) in kind.as_object().unwrap() {
        task[key] = value.clone();
    }
    task
}

fn python_task() -> serde_json::Value {
    task(
        &json!({"kind": "python", "arguments": {"rows": {"ref": {"kind": "output", "task": "extract"}}}}),
    )
}

fn command_task(command: &serde_json::Value, env: &serde_json::Value) -> serde_json::Value {
    task(&json!({"kind": "command", "command": command, "env": env}))
}

fn assign(task: &serde_json::Value) -> pb::AssignTask {
    pb::AssignTask {
        attempt_id: "a1".to_owned(),
        task_id: "load".to_owned(),
        map_index: -1,
        try_number: 2,
        run: Some(pb::RunInfo {
            run_id: "r1".to_owned(),
            kind: "manual".to_owned(),
            planned_for: Some(prost_types::Timestamp {
                seconds: 1_792_036_800,
                nanos: 0,
            }),
            since: Some(prost_types::Timestamp {
                seconds: 1_791_950_400,
                nanos: 0,
            }),
            dag_id: "sync".to_owned(),
        }),
        task_json: serde_json::to_vec(task).unwrap(),
        dag_file: Some(pb::DagFileRef {
            root: "etl".to_owned(),
            path: "team/sync.py".to_owned(),
        }),
        expected_code_hash: "h1".to_owned(),
        inputs_json: br#"{"extract": {"rows": 3, "name": "a b"}}"#.to_vec(),
        inputs_external: false,
        cancel_grace_ms: 30_000,
        memory_budget_bytes: 0,
        setrlimit: false,
    }
}

/// A root `etl` with `team/sync.py`, and the worker set up for it.
fn setup() -> (TempDir, WorkerSetup) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("etl/team")).unwrap();
    std::fs::write(dir.path().join("etl/team/sync.py"), "").unwrap();
    let setup = WorkerSetup {
        roots: BTreeMap::from([("etl".to_owned(), dir.path().join("etl"))]),
        python: PathBuf::from("/usr/bin/python3"),
        agent_env: BTreeMap::from([
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("SECRET".to_owned(), "x".to_owned()),
        ]),
        pass_env: vec![],
        log_dir: dir.path().join("logs"),
    };
    (dir, setup)
}

#[test]
fn an_assignment_is_read_with_its_run_and_inputs() {
    let spec = AttemptSpec::from_assign(&assign(&python_task()), None).unwrap();
    assert_eq!(
        (
            spec.attempt_id.as_str(),
            spec.dag_root.as_str(),
            spec.dag_path.as_str()
        ),
        ("a1", "etl", "team/sync.py")
    );
    assert_eq!(spec.run.kind, schedr_model::RunKind::Manual);
    assert_eq!(spec.run.planned_for.to_string(), "2026-10-15T04:00:00Z");
    assert_eq!((spec.run.try_number, spec.run.map_index), (2, None));
    assert_eq!(spec.inputs["extract"], json!({"rows": 3, "name": "a b"}));
    assert_eq!(spec.cancel_grace, Duration::from_secs(30));
    assert_eq!(spec.timeout(), Some(Duration::from_secs(5)));
    let external = AttemptSpec::from_assign(&assign(&python_task()), Some(br#"{"x": 1}"#)).unwrap();
    assert_eq!(external.inputs["x"], json!(1));
    let mut none = assign(&python_task());
    none.inputs_json.clear();
    assert!(
        AttemptSpec::from_assign(&none, None)
            .unwrap()
            .inputs
            .is_empty()
    );
}

#[test]
fn assignments_that_dont_read_are_invalid() {
    let mut bad_task = assign(&python_task());
    bad_task.task_json = b"{}".to_vec();
    assert!(
        AttemptSpec::from_assign(&bad_task, None)
            .unwrap_err()
            .0
            .starts_with("task: ")
    );
    let mut no_run = assign(&python_task());
    no_run.run = None;
    assert_eq!(
        AttemptSpec::from_assign(&no_run, None)
            .unwrap_err()
            .to_string(),
        "invalid assignment: no run information"
    );
    let mut no_file = assign(&python_task());
    no_file.dag_file = None;
    assert!(AttemptSpec::from_assign(&no_file, None).is_err());
    let mut bad_kind = assign(&python_task());
    bad_kind.run.as_mut().unwrap().kind = "sometimes".to_owned();
    assert!(
        AttemptSpec::from_assign(&bad_kind, None)
            .unwrap_err()
            .0
            .starts_with("run kind: ")
    );
    let mut no_time = assign(&python_task());
    no_time.run.as_mut().unwrap().since = None;
    assert!(AttemptSpec::from_assign(&no_time, None).is_err());
    let mut bad_inputs = assign(&python_task());
    bad_inputs.inputs_json = b"[1]".to_vec();
    assert!(
        AttemptSpec::from_assign(&bad_inputs, None)
            .unwrap_err()
            .0
            .starts_with("inputs: ")
    );
}

#[test]
fn a_python_task_runs_the_runner_with_the_request() {
    let (_dir, setup) = setup();
    let spec = AttemptSpec::from_assign(&assign(&python_task()), None).unwrap();
    let Plan::Python { process, request } = plan(&spec, &setup).unwrap() else {
        panic!("expected a Python plan")
    };
    assert_eq!(process.program, PathBuf::from("/usr/bin/python3"));
    assert_eq!(process.args, ["-m", "schedr.runner", "task"]);
    assert_eq!(process.env["PATH"], "/usr/bin");
    assert!(!process.env.contains_key("SECRET"));
    assert_eq!(process.env["SCHEDR_TRY_NUMBER"], "2");
    assert_eq!(process.log_file, setup.log_file("a1"));
    assert_eq!(process.max_output_size, 1024);
    assert_eq!(request.dag_file.path, "team/sync.py");
    assert_eq!(
        request.dag_file.root_dir,
        setup.roots["etl"].to_string_lossy()
    );
    assert_eq!(request.sys_path, setup.sys_path());
    assert_eq!(request.inputs, spec.inputs);
}

#[test]
fn a_command_is_rendered_with_its_inputs_and_environment() {
    let (_dir, setup) = setup();
    let command = json!({"shell": [
        {"text": "load --rows "},
        {"ref": {"kind": "output", "task": "extract", "path": ["rows"]}},
        {"text": " --name "},
        {"ref": {"kind": "output", "task": "extract", "path": ["name"]}}
    ]});
    let env = json!({"TARGET": [{"text": "db-"}, {"ref": {"kind": "dag_ctx", "field": "dag_id"}}]});
    let spec = AttemptSpec::from_assign(&assign(&command_task(&command, &env)), None).unwrap();
    let Plan::Command(process) = plan(&spec, &setup).unwrap() else {
        panic!("expected a command plan")
    };
    assert_eq!(process.program, PathBuf::from("/bin/sh"));
    assert_eq!(process.args, ["-c", "load --rows 3 --name 'a b'"]);
    assert_eq!(process.env["TARGET"], "db-sync");
    assert_eq!(process.env["SCHEDR_DAG_ID"], "sync");
    assert_eq!(process.working_dir, setup.roots["etl"].join("team"));
}

#[test]
fn missing_code_is_a_rejection_and_bad_templates_a_failure() {
    let (_dir, setup) = setup();
    let mut elsewhere = assign(&python_task());
    elsewhere.dag_file.as_mut().unwrap().root = "ml".to_owned();
    let spec = AttemptSpec::from_assign(&elsewhere, None).unwrap();
    assert_eq!(
        plan(&spec, &setup).unwrap_err(),
        NotRun::Rejected {
            reason: pb::RejectReason::DagFileMissing,
            detail: "DAG root `ml` isn't on this worker".to_owned()
        }
    );
    let mut missing = assign(&python_task());
    missing.dag_file.as_mut().unwrap().path = "gone.py".to_owned();
    let spec = AttemptSpec::from_assign(&missing, None).unwrap();
    assert!(matches!(
        plan(&spec, &setup).unwrap_err(),
        NotRun::Rejected {
            reason: pb::RejectReason::DagFileMissing,
            ..
        }
    ));

    let ghost = json!({"shell": [{"ref": {"kind": "output", "task": "ghost"}}]});
    let spec = AttemptSpec::from_assign(&assign(&command_task(&ghost, &json!({}))), None).unwrap();
    let NotRun::Failed { kind, .. } = plan(&spec, &setup).unwrap_err() else {
        panic!("expected a failure")
    };
    assert_eq!(kind, RENDER_FAILED);
    let reserved = json!({"SCHEDR_X": [{"text": "1"}]});
    let spec = AttemptSpec::from_assign(
        &assign(&command_task(
            &json!({"shell": [{"text": "true"}]}),
            &reserved,
        )),
        None,
    )
    .unwrap();
    assert!(matches!(
        plan(&spec, &setup).unwrap_err(),
        NotRun::Failed { .. }
    ));
    let bad_env = json!({"X": [{"ref": {"kind": "output", "task": "ghost"}}]});
    let spec = AttemptSpec::from_assign(
        &assign(&command_task(
            &json!({"shell": [{"text": "true"}]}),
            &bad_env,
        )),
        None,
    )
    .unwrap();
    assert!(matches!(
        plan(&spec, &setup).unwrap_err(),
        NotRun::Failed { .. }
    ));

    let empty = AttemptSpec::from_assign(&assign(&task(&json!({"kind": "empty"}))), None).unwrap();
    assert!(matches!(
        plan(&empty, &setup).unwrap_err(),
        NotRun::Rejected {
            reason: pb::RejectReason::InvalidSpecification,
            ..
        }
    ));
}

fn ran(
    termination: Termination,
    stopped: Option<StopReason>,
    runner: Option<RunnerMessage>,
) -> Ran {
    Ran {
        supervised: Supervised {
            termination,
            stopped,
            killed: false,
        },
        runner_result: runner,
        output: None,
        duration: Duration::from_millis(1500),
    }
}

#[test]
fn a_python_task_reports_the_runners_result() {
    let success = RunnerMessage::TaskSucceeded {
        output: json!([1, 2]),
        outcome: "SUCCESS".to_owned(),
    };
    let message = finished("a1", &ran(Termination::Exited(0), None, Some(success)));
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Success));
    assert_eq!(message.output_json, b"[1,2]");
    assert_eq!(
        (
            message.outcome.as_str(),
            message.exit_code,
            message.duration_ms
        ),
        ("SUCCESS", Some(0), 1500)
    );

    let error = ErrorSummary {
        exception: "ValueError".to_owned(),
        message: "no rows".to_owned(),
        traceback: String::new(),
    };
    let failure = RunnerMessage::TaskFailed {
        error: error.clone(),
        kind: Some("input_mismatch".to_owned()),
    };
    let message = finished("a1", &ran(Termination::Exited(1), None, Some(failure)));
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(message.error_kind, "input_mismatch");
    assert_eq!(
        serde_json::from_slice::<ErrorSummary>(&message.error_summary_json).unwrap(),
        error
    );
}

#[test]
fn a_runner_without_a_result_or_stopped_by_the_agent_says_so() {
    let silent = finished("a1", &ran(Termination::Exited(0), None, None));
    assert_eq!(silent.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(silent.error_kind, RUNNER_FAILED);
    let timed_out = finished(
        "a1",
        &ran(Termination::Signaled(15), Some(StopReason::TimedOut), None),
    );
    assert_eq!(timed_out.status, i32::from(pb::AttemptStatus::TimedOut));
    assert_eq!((timed_out.signal, timed_out.exit_code), (Some(15), None));
    let crashed = finished("a1", &ran(Termination::Signaled(11), None, None));
    assert_eq!(crashed.status, i32::from(pb::AttemptStatus::Failed));
    let cancelled = RunnerMessage::TaskFailed {
        error: ErrorSummary {
            exception: "KeyboardInterrupt".to_owned(),
            message: String::new(),
            traceback: String::new(),
        },
        kind: None,
    };
    let message = finished(
        "a1",
        &ran(
            Termination::Exited(1),
            Some(StopReason::Cancelled),
            Some(cancelled),
        ),
    );
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Cancelled));
}

#[test]
fn a_command_reports_its_process_result() {
    let mut stdout = TailBuffer::new(100);
    stdout.push(b"done\n");
    let stderr = TailBuffer::new(100);
    let mut success = ran(Termination::Exited(0), None, None);
    success.output = Some((stdout.clone(), stderr.clone()));
    let message = finished("a1", &success);
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Success));
    let output: serde_json::Value = serde_json::from_slice(&message.output_json).unwrap();
    assert_eq!(
        (output["stdout"].clone(), output["exit_code"].clone()),
        (json!("done\n"), json!(0))
    );
    let mut failure = ran(Termination::Exited(2), None, None);
    failure.output = Some((stdout, stderr));
    let message = finished("a1", &failure);
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(message.output_json, Vec::<u8>::new());
    let result: serde_json::Value = serde_json::from_slice(&message.error_summary_json).unwrap();
    assert_eq!(result["exit_code"], json!(2));
}

#[test]
fn a_failure_before_starting_is_reported_as_failed() {
    let message = failed_before_start("a1", RENDER_FAILED, "no output of `ghost`");
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(message.error_kind, RENDER_FAILED);
    let summary: ErrorSummary = serde_json::from_slice(&message.error_summary_json).unwrap();
    assert_eq!(summary.message, "no output of `ghost`");
}
