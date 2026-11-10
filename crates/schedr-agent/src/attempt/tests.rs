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

/// A command declaring no outcomes: `SUCCESS` on exit code 0.
fn plain_task() -> Task {
    serde_json::from_value(command_task(
        &json!({"shell": [{"text": "true"}]}),
        &json!({}),
    ))
    .unwrap()
}

fn assign(task: &serde_json::Value) -> pb::AssignTask {
    pb::AssignTask {
        attempt_id: "a1".to_owned(),
        task_id: "load".to_owned(),
        map_index: -1,
        try_number: 2,
        run: Some(pb::RunInfo {
            event_json: Vec::new(),
            params_json: Vec::new(),
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
        upstream_outcomes: std::collections::HashMap::new(),
        connections_json: Vec::new(),
        cleanup_only: None,
        resource_cleanup: false,
        first_started: None,
    }
}

/// A root `etl` with `team/sync.py`, and the worker set up for it.
fn setup() -> (TempDir, WorkerSetup) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("etl/team")).unwrap();
    std::fs::write(dir.path().join("etl/team/sync.py"), "").unwrap();
    let setup = WorkerSetup {
        roots: BTreeMap::from([("etl".to_owned(), dir.path().join("etl"))]),
        parse_only: BTreeMap::new(),
        script_roots: BTreeMap::new(),
        interpreters: BTreeMap::default(),
        python: PathBuf::from("/usr/bin/python3"),
        agent_env: BTreeMap::from([
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("SECRET".to_owned(), "x".to_owned()),
        ]),
        pass_env: vec![],
        log_dir: dir.path().join("logs"),
        connection_dir: dir.path().join("connections"),
        memory: crate::memory::MemoryWatch::default(),
        timeouts: crate::attempt::WorkerTimeouts::default(),
        fork_server: None,
    };
    (dir, setup)
}

#[test]
fn a_resource_cleanup_without_a_timeout_gets_the_workers() {
    let timeouts = crate::attempt::WorkerTimeouts::default();
    let mut untimed = python_task();
    untimed["settings"]["timeout_ms"] = json!(null);
    let read = |task: &serde_json::Value, resource_cleanup: bool| {
        let message = pb::AssignTask {
            resource_cleanup,
            ..assign(task)
        };
        let mut spec = AttemptSpec::from_assign(&message, None).unwrap();
        spec.default_timeout(&message, &timeouts);
        spec.timeout()
    };
    assert_eq!(read(&untimed, true), Some(Duration::from_mins(30)));
    // Its own timeout is kept; any other task keeps none.
    assert_eq!(read(&python_task(), true), Some(Duration::from_secs(5)));
    assert_eq!(read(&untimed, false), None);
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
    assert_eq!(process.args, ["-P", "-m", "schedr.runner", "task"]);
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
fn a_task_planned_at_a_revision_runs_and_imports_from_it() {
    // Decision 102: the revision behind the configured root (git-sync's link), for the DAG
    // file, the working directory and the root's `sys.path` entry; other roots stay.
    let (dir, mut setup) = setup();
    setup
        .roots
        .insert("shared".to_owned(), dir.path().join("shared"));
    let revision = dir.path().join("rev-a");
    std::fs::create_dir_all(revision.join("team")).unwrap();
    std::fs::write(revision.join("team/sync.py"), "").unwrap();
    let spec = AttemptSpec::from_assign(&assign(&python_task()), None).unwrap();
    let Plan::Python { process, request } = plan_at(&spec, &setup, &revision).unwrap() else {
        panic!("expected a Python plan")
    };
    assert_eq!(process.working_dir, revision);
    assert_eq!(request.dag_file.root_dir, revision.to_string_lossy());
    let shared = dir.path().join("shared");
    assert_eq!(
        request.sys_path,
        [revision.to_string_lossy(), shared.to_string_lossy()]
    );
    assert_eq!(setup.sys_path_with("etl", &revision), request.sys_path);
    // A revision without the DAG file is code not there yet.
    let empty = dir.path().join("rev-b");
    std::fs::create_dir_all(&empty).unwrap();
    assert!(matches!(
        plan_at(&spec, &setup, &empty),
        Err(NotRun::Rejected {
            reason: pb::RejectReason::DagFileMissing,
            ..
        })
    ));
}

#[test]
fn a_command_is_rendered_with_its_inputs_and_environment() {
    let (_dir, setup) = setup();
    let command = json!({"argv": [
        [{"text": "load"}],
        [{"text": "--rows="}, {"ref": {"kind": "output", "task": "extract", "path": ["rows"]}}],
        [{"ref": {"kind": "output", "task": "extract", "path": ["name"]}}]
    ]});
    let env = json!({"TARGET": [{"text": "db-"}, {"ref": {"kind": "dag_ctx", "field": "dag_id"}}]});
    let spec = AttemptSpec::from_assign(&assign(&command_task(&command, &env)), None).unwrap();
    let Plan::Command(process) = plan(&spec, &setup).unwrap() else {
        panic!("expected a command plan")
    };
    assert_eq!(process.program, PathBuf::from("load"));
    assert_eq!(process.args, ["--rows=3", "a b"]);
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

    let ghost = json!({"argv": [[{"ref": {"kind": "output", "task": "ghost"}}]]});
    let spec = AttemptSpec::from_assign(&assign(&command_task(&ghost, &json!({}))), None).unwrap();
    let NotRun::Failed { kind, .. } = plan(&spec, &setup).unwrap_err() else {
        panic!("expected a failure")
    };
    assert_eq!(kind, RENDER_FAILED);
    // Decision 99: a reference in a shell command never runs, whatever the master sent.
    let shell =
        json!({"shell": [{"text": "echo "}, {"ref": {"kind": "output", "task": "extract"}}]});
    let spec = AttemptSpec::from_assign(&assign(&command_task(&shell, &json!({}))), None).unwrap();
    let NotRun::Failed { kind, message } = plan(&spec, &setup).unwrap_err() else {
        panic!("expected a failure")
    };
    assert_eq!(kind, RENDER_FAILED);
    assert!(message.contains("in a shell command"), "{message}");
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
        start: None,
        forked: false,
        ended: crate::usage::Ended::default(),
        moved_to: None,
        host_lost: None,
        struck: false,
    }
}

#[test]
fn a_python_task_reports_the_runners_result() {
    let success = RunnerMessage::TaskSucceeded {
        output: json!([1, 2]),
        outcome: "SUCCESS".to_owned(),
    };
    let message = finished(
        "a1",
        &plain_task(),
        &ran(Termination::Exited(0), None, Some(success)),
    );
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Success));
    assert_eq!(message.output_json, b"[1,2]");
    assert_eq!(
        (
            message.outcome.as_str(),
            message.exit_code,
            message.duration_ms
        ),
        ("SUCCESS", Some(0), Some(1500))
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
    let message = finished(
        "a1",
        &plain_task(),
        &ran(Termination::Exited(1), None, Some(failure)),
    );
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(message.error_kind, "input_mismatch");
    assert_eq!(
        serde_json::from_slice::<ErrorSummary>(&message.error_summary_json).unwrap(),
        error
    );
}

#[test]
fn a_runner_without_a_result_or_stopped_by_the_agent_says_so() {
    let silent = finished(
        "a1",
        &plain_task(),
        &ran(Termination::Exited(0), None, None),
    );
    assert_eq!(silent.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(silent.error_kind, RUNNER_FAILED);
    let timed_out = finished(
        "a1",
        &plain_task(),
        &ran(Termination::Signaled(15), Some(StopReason::TimedOut), None),
    );
    assert_eq!(timed_out.status, i32::from(pb::AttemptStatus::TimedOut));
    assert_eq!((timed_out.signal, timed_out.exit_code), (Some(15), None));
    let crashed = finished(
        "a1",
        &plain_task(),
        &ran(Termination::Signaled(11), None, None),
    );
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
        &plain_task(),
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
    let message = finished("a1", &plain_task(), &success);
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Success));
    let output: serde_json::Value = serde_json::from_slice(&message.output_json).unwrap();
    assert_eq!(
        (output["stdout"].clone(), output["exit_code"].clone()),
        (json!("done\n"), json!(0))
    );
    let mut failure = ran(Termination::Exited(2), None, None);
    failure.output = Some((stdout, stderr));
    let message = finished("a1", &plain_task(), &failure);
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(message.output_json, Vec::<u8>::new());
    let result: serde_json::Value = serde_json::from_slice(&message.error_summary_json).unwrap();
    assert_eq!(result["exit_code"], json!(2));
}

#[test]
fn a_resource_setups_output_is_its_value_and_none_reported_fails_it() {
    let output = || (TailBuffer::new(100), TailBuffer::new(100));
    let mut success = ran(Termination::Exited(0), None, None);
    success.output = Some(output());
    let mut message = finished("a1", &plain_task(), &success);
    with_resource_value(&mut message, Ok(json!({"cluster_id": "c-1"})));
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Success));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&message.output_json).unwrap(),
        json!({"cluster_id": "c-1"})
    );

    let mut silent = finished("a1", &plain_task(), &success);
    with_resource_value(&mut silent, Err(()));
    assert_eq!(silent.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(silent.error_kind, RESOURCE_NOT_REPORTED);
    assert!(silent.output_json.is_empty() && silent.outcome.is_empty());
    let summary: serde_json::Value = serde_json::from_slice(&silent.error_summary_json).unwrap();
    assert!(
        summary.to_string().contains("resource value not reported"),
        "{summary}"
    );

    // A failure stays the process's own.
    let mut failure = ran(Termination::Exited(3), None, None);
    failure.output = Some(output());
    let mut failed = finished("a1", &plain_task(), &failure);
    let before = failed.clone();
    with_resource_value(&mut failed, Err(()));
    assert_eq!(failed, before);
}

#[test]
fn a_failure_before_starting_is_reported_as_failed() {
    let message = failed_before_start("a1", RENDER_FAILED, "no output of `ghost`");
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(message.error_kind, RENDER_FAILED);
    let summary: ErrorSummary = serde_json::from_slice(&message.error_summary_json).unwrap();
    assert_eq!(summary.message, "no output of `ghost`");
    assert_eq!(message.duration_ms, None, "it never ran");
}

#[test]
fn a_forked_task_whose_shim_died_failed_with_an_unknown_status() {
    let message = finished("a1", &plain_task(), &ran(Termination::Unknown, None, None));
    assert_eq!(message.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!((message.exit_code, message.signal), (None, None));
    assert_eq!(message.error_kind, PROCESS_STATUS_UNKNOWN);
    let summary: ErrorSummary = serde_json::from_slice(&message.error_summary_json).unwrap();
    assert_eq!(summary.exception, "ProcessStatusUnknown");
    // Stopped by the agent first: its status stays, the unknown end is still explained.
    let stopped = finished(
        "a1",
        &plain_task(),
        &ran(Termination::Unknown, Some(StopReason::Cancelled), None),
    );
    assert_eq!(stopped.status, i32::from(pb::AttemptStatus::Cancelled));
    assert_eq!(stopped.error_kind, PROCESS_STATUS_UNKNOWN);
}

#[test]
fn a_commands_exit_code_names_its_outcome() {
    let mut declared = command_task(&json!({"shell": [{"text": "./check.sh"}]}), &json!({}));
    declared["outcomes"] = json!([
        {"name": "CHANGED", "exit_code": 0},
        {"name": "UNCHANGED", "exit_code": 3}
    ]);
    let task: Task = serde_json::from_value(declared).unwrap();
    let with_code = |code| {
        let mut ran = ran(Termination::Exited(code), None, None);
        ran.output = Some((TailBuffer::new(100), TailBuffer::new(100)));
        finished("a1", &task, &ran)
    };
    let changed = with_code(0);
    assert_eq!(changed.status, i32::from(pb::AttemptStatus::Success));
    assert_eq!(changed.outcome, "CHANGED");
    // A declared non-zero code is a success with its outcome, not a failure.
    let unchanged = with_code(3);
    assert_eq!(unchanged.status, i32::from(pb::AttemptStatus::Success));
    assert_eq!(unchanged.outcome, "UNCHANGED");
    // An undeclared one fails, 1 as any other.
    let other = with_code(1);
    assert_eq!(other.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(other.outcome, "");
}

#[tokio::test]
async fn a_cleanup_gate_waits_while_held_and_says_what_the_master_answered() {
    let (gate, passes) = tokio::sync::watch::channel(Gate::Open);
    let (_deadline, never) = tokio::sync::watch::channel(None);
    let of = |passes: &tokio::sync::watch::Receiver<Gate>| CleanupGate {
        gate: passes.clone(),
        deadline: never.clone(),
    };
    assert_eq!(of(&passes).passes().await, Some(Pass::All));
    gate.send_replace(Gate::Held);
    let waiting = tokio::spawn(of(&passes).passes());
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    gate.send_replace(Gate::Open);
    assert_eq!(waiting.await.unwrap(), Some(Pass::All));
    // B169: the master had the first two handles, moved elsewhere: the rest is cleaned here.
    gate.send_replace(Gate::Rest(2));
    assert_eq!(of(&passes).passes().await, Some(Pass::From(2)));
    gate.send_replace(Gate::Dropped);
    assert_eq!(of(&passes).passes().await, None);
    drop(gate);
    assert_eq!(of(&passes).passes().await, None);
}

#[tokio::test]
async fn an_open_gate_past_the_masters_deadline_waits_for_its_answer() {
    // B162: frozen past the lease, the worker's loop hadn't held the gate yet.
    let (gate, passes) = tokio::sync::watch::channel(Gate::Open);
    let past = tokio::time::Instant::now() - Duration::from_secs(1);
    let (deadline, late) = tokio::sync::watch::channel(Some(past));
    let gated = CleanupGate {
        gate: passes,
        deadline: late,
    };
    let waiting = tokio::spawn(gated.clone().passes());
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "it doesn't pass on its own");
    // The loop holds it, then the master answers `Kill`: never run here.
    gate.send_replace(Gate::Held);
    gate.send_replace(Gate::Dropped);
    assert_eq!(waiting.await.unwrap(), None);
    // Or `Continue`, heard just now: it runs.
    gate.send_replace(Gate::Held);
    let waiting = tokio::spawn(gated.passes());
    deadline.send_replace(Some(tokio::time::Instant::now() + Duration::from_secs(60)));
    gate.send_replace(Gate::Open);
    assert_eq!(waiting.await.unwrap(), Some(Pass::All));
}

#[tokio::test]
async fn an_open_gate_past_its_deadline_passes_when_the_master_is_heard_in_time() {
    // Codex's review of B162: a heartbeat's answer heard just before the lease ran out moves
    // the deadline on with the gate left open; the cleanup waited for a change of the gate
    // that never came.
    let (_gate, passes) = tokio::sync::watch::channel(Gate::Open);
    let past = tokio::time::Instant::now() - Duration::from_secs(1);
    let (deadline, late) = tokio::sync::watch::channel(Some(past));
    let waiting = tokio::spawn(
        CleanupGate {
            gate: passes,
            deadline: late,
        }
        .passes(),
    );
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    deadline.send_replace(Some(tokio::time::Instant::now() + Duration::from_secs(60)));
    let woken = tokio::time::timeout(Duration::from_secs(5), waiting).await;
    assert_eq!(
        woken.expect("woken by the deadline").unwrap(),
        Some(Pass::All)
    );
}

/// A DAG's script task using `jobs:team/schedr.toml#report` (script `team/report.py`).
fn script_task() -> serde_json::Value {
    task(&json!({
        "kind": "script",
        "job": "jobs:team/schedr.toml#report",
        "script": "team/report.py",
        "interpreter": "etl-venv",
        "args": [[{"text": "--rows="}, {"ref": {"kind": "output", "task": "extract", "path": ["rows"]}}]],
        "env": {"MODE": [{"text": "full"}]},
        "working_dir": "team"
    }))
}

/// [`setup`] with a script root `jobs` holding `team/report.py`, and `etl-venv`.
fn script_setup() -> (TempDir, WorkerSetup) {
    let (dir, mut setup) = setup();
    std::fs::create_dir_all(dir.path().join("jobs/team")).unwrap();
    std::fs::write(dir.path().join("jobs/team/report.py"), "").unwrap();
    setup
        .script_roots
        .insert("jobs".to_owned(), dir.path().join("jobs"));
    setup
        .interpreters
        .insert("etl-venv".to_owned(), PathBuf::from("/opt/etl/bin/python"));
    (dir, setup)
}

#[test]
fn a_script_runs_with_its_interpreter_its_rendered_args_and_its_root_on_pythonpath() {
    let (dir, setup) = script_setup();
    let spec = AttemptSpec::from_assign(&assign(&script_task()), None).unwrap();
    let Plan::Command(process) = plan(&spec, &setup).unwrap() else {
        panic!("expected a process")
    };
    let root = &setup.script_roots["jobs"];
    assert_eq!(process.program, PathBuf::from("/opt/etl/bin/python"));
    assert_eq!(
        process.args,
        [
            root.join("team/report.py").to_string_lossy().into_owned(),
            "--rows=3".to_owned()
        ]
    );
    assert_eq!(process.env["MODE"], "full");
    assert_eq!(process.env["PYTHONPATH"], root.to_string_lossy());
    assert_eq!(process.env["SCHEDR_DAG_ID"], "sync");
    assert_eq!(process.env["SCHEDR_TASK_ID"], "load");
    assert!(!process.env.contains_key("SECRET"));
    assert_eq!(process.working_dir, root.join("team"));
    // Planned against a copy of the script root: run from it.
    let mut copied = spec;
    std::fs::create_dir_all(dir.path().join("copy/team")).unwrap();
    std::fs::write(dir.path().join("copy/team/report.py"), "").unwrap();
    copied.script_dir = Some(dir.path().join("copy"));
    let Plan::Command(process) = plan(&copied, &setup).unwrap() else {
        panic!("expected a process")
    };
    assert_eq!(process.working_dir, dir.path().join("copy/team"));
}

#[test]
fn a_script_without_its_interpreter_root_or_file_here_is_rejected_with_why() {
    let reason = |setup: &WorkerSetup| match plan(
        &AttemptSpec::from_assign(&assign(&script_task()), None).unwrap(),
        setup,
    ) {
        Err(NotRun::Rejected { reason, detail }) => (reason, detail),
        other => panic!("expected a rejection, got {other:?}"),
    };
    let (_dir, mut no_interpreter) = script_setup();
    no_interpreter.interpreters.clear();
    let (why, detail) = reason(&no_interpreter);
    assert_eq!(why, pb::RejectReason::InterpreterUndefined);
    assert!(detail.contains("interpreter `etl-venv`"), "{detail}");
    let (_dir, mut no_root) = script_setup();
    no_root.script_roots.clear();
    let (why, detail) = reason(&no_root);
    assert_eq!(why, pb::RejectReason::ScriptRootUnknown);
    assert!(detail.contains("script root `jobs`"), "{detail}");
    let (dir, no_file) = script_setup();
    std::fs::remove_file(dir.path().join("jobs/team/report.py")).unwrap();
    let (why, detail) = reason(&no_file);
    assert_eq!(why, pb::RejectReason::ScriptReferenceNotFound);
    assert!(detail.contains("`team/report.py`"), "{detail}");
}

/// [`script_task`] with `team/undo.sh`, run by `interpreter`, as its cleanup.
fn script_task_with_cleanup(interpreter: &str) -> serde_json::Value {
    let mut task = script_task();
    task["cleanup"] = json!({
        "run": {"script": {"script": "team/undo.sh", "interpreter": interpreter}},
        "retries": 0,
        "retry_delay_ms": 0,
    });
    task
}

#[test]
fn a_worker_that_couldnt_run_a_scripts_cleanup_rejects_the_task() {
    let reason = |task: &serde_json::Value, setup: &WorkerSetup| match plan(
        &AttemptSpec::from_assign(&assign(task), None).unwrap(),
        setup,
    ) {
        Err(NotRun::Rejected { reason, detail }) => (reason, detail),
        other => panic!("expected a rejection, got {other:?}"),
    };
    let (dir, setup) = script_setup();
    let (why, detail) = reason(&script_task_with_cleanup("other"), &setup);
    assert_eq!(why, pb::RejectReason::InterpreterUndefined);
    assert!(detail.contains("interpreter `other`"), "{detail}");
    let (why, detail) = reason(&script_task_with_cleanup("etl-venv"), &setup);
    assert_eq!(why, pb::RejectReason::ScriptReferenceNotFound);
    assert!(detail.contains("`team/undo.sh`"), "{detail}");
    std::fs::write(dir.path().join("jobs/team/undo.sh"), "").unwrap();
    let spec =
        AttemptSpec::from_assign(&assign(&script_task_with_cleanup("etl-venv")), None).unwrap();
    assert!(matches!(plan(&spec, &setup), Ok(Plan::Command(_))));
}

#[test]
fn a_moved_or_recovered_cleanup_needs_only_what_the_cleanup_runs() {
    let (dir, mut setup) = script_setup();
    std::fs::write(dir.path().join("jobs/team/undo.sh"), "").unwrap();
    setup.interpreters.clear();
    setup
        .interpreters
        .insert("bash".to_owned(), PathBuf::from("/bin/bash"));
    let mut spec =
        AttemptSpec::from_assign(&assign(&script_task_with_cleanup("bash")), None).unwrap();
    assert!(matches!(
        plan(&spec, &setup),
        Err(NotRun::Rejected {
            reason: pb::RejectReason::InterpreterUndefined,
            ..
        })
    ));
    use_cleanup_script(&mut spec.task);
    let Plan::Command(process) = plan(&spec, &setup).unwrap() else {
        panic!("expected a process")
    };
    assert_eq!(process.program, PathBuf::from("/bin/bash"));
    assert_eq!(
        process.args.len(),
        1,
        "the cleanup's arguments, not the task's"
    );
    assert!(
        process.args[0].ends_with("team/undo.sh"),
        "{:?}",
        process.args
    );
    assert_eq!(process.env["MODE"], "full", "the task's environment");
    assert!(
        process.working_dir.ends_with("team"),
        "its working directory"
    );
}

#[test]
fn a_cleanup_script_leaving_its_script_root_isnt_run() {
    let (_dir, setup) = script_setup();
    let mut task = script_task_with_cleanup("etl-venv");
    task["cleanup"]["run"]["script"]["script"] = json!("../secret.sh");
    let spec = AttemptSpec::from_assign(&assign(&task), None).unwrap();
    let Err(NotRun::Failed { message, .. }) = plan(&spec, &setup) else {
        panic!("expected a failure")
    };
    assert!(message.contains("cleanup script leaves"), "{message}");
}

#[test]
fn a_standalone_jobs_script_comes_from_its_own_root() {
    let (dir, mut setup) = script_setup();
    setup
        .roots
        .insert("jobs".to_owned(), dir.path().join("jobs"));
    std::fs::write(dir.path().join("jobs/team/schedr.toml"), "").unwrap();
    let mut job = assign(&script_task());
    job.dag_file = Some(pb::DagFileRef {
        root: "jobs".to_owned(),
        path: "team/schedr.toml".to_owned(),
    });
    let spec = AttemptSpec::from_assign(&job, None).unwrap();
    let Plan::Command(process) = plan(&spec, &setup).unwrap() else {
        panic!("expected a process")
    };
    assert_eq!(process.working_dir, dir.path().join("jobs/team"));
}

#[test]
fn a_script_or_working_dir_leaving_its_script_root_isnt_run() {
    let (dir, setup) = script_setup();
    std::fs::write(dir.path().join("payload.py"), "").unwrap();
    for (field, value) in [("script", "../payload.py"), ("working_dir", "../..")] {
        let mut task = script_task();
        task[field] = json!(value);
        let spec = AttemptSpec::from_assign(&assign(&task), None).unwrap();
        match plan(&spec, &setup) {
            Err(NotRun::Failed { message, .. }) => {
                assert!(message.ends_with("leaves its script root"), "{message}");
            }
            other => panic!("{field} = {value}: expected a failure, got {other:?}"),
        }
    }
}

#[test]
fn a_wait_step_without_a_max_wait_gets_the_workers_default_and_a_limit_past_its_grace() {
    let timeouts = WorkerTimeouts {
        sensor_max_wait: Duration::from_hours(2),
        ..WorkerTimeouts::default()
    };
    let margin = u64::try_from(timeouts.runner_start.as_millis()).unwrap() + 3000;
    let wait_of = |mut task: serde_json::Value| {
        task["kind"] = json!("sensor");
        task["mode"] = json!("loop");
        task["settings"]["timeout_ms"] = json!(null);
        let message = pb::AssignTask {
            cancel_grace_ms: 3000,
            ..assign(&task)
        };
        let mut spec = AttemptSpec::from_assign(&message, None).unwrap();
        spec.default_timeout(&message, &timeouts);
        let TaskKind::Sensor { max_wait_ms, .. } = spec.task.kind else {
            panic!("a wait step");
        };
        (max_wait_ms, spec.task.settings.timeout_ms)
    };
    assert_eq!(
        wait_of(python_task()),
        (Some(7_200_000), Some(7_200_000 + margin))
    );
    let mut own = python_task();
    own["max_wait_ms"] = json!(5000);
    assert_eq!(wait_of(own), (Some(5000), Some(5000 + margin)));
}

#[test]
fn a_wait_step_s_first_start_is_the_masters_or_now_on_its_first_try() {
    // Codex's review of M4.11g: a host restarted in the same attempt reuses the request, so a
    // first try's `delay` counts from when the agent took it, not again from zero.
    let mut wait = python_task();
    wait["kind"] = json!("sensor");
    wait["mode"] = json!("loop");
    let before = Timestamp::now();
    let first = AttemptSpec::from_assign(&assign(&wait), None).unwrap();
    let taken = first.run.first_started.expect("now, on its first try");
    assert!(before <= taken && taken <= Timestamp::now());
    let earlier = prost_types::Timestamp {
        seconds: 1_792_036_800,
        nanos: 0,
    };
    let again = pb::AssignTask {
        first_started: Some(earlier),
        ..assign(&wait)
    };
    let spec = AttemptSpec::from_assign(&again, None).unwrap();
    assert_eq!(
        spec.run.first_started,
        Some(Timestamp::from_second(1_792_036_800).unwrap()),
        "the master's, tried again"
    );
    let task = AttemptSpec::from_assign(&assign(&python_task()), None).unwrap();
    assert_eq!(task.run.first_started, None, "a task has none");
}

#[test]
fn a_task_over_what_this_worker_gives_one_is_rejected_a_capped_one_starts_through_task_exec() {
    let (_dir, mut setup) = setup();
    setup.memory.ceiling = Some(1 << 30);
    let mut asking = command_task(
        &json!({"argv": [[{"text": "echo"}], [{"text": "hi"}]]}),
        &json!({}),
    );
    asking["settings"]["memory_bytes"] = json!(2_u64 << 30);
    let spec = AttemptSpec::from_assign(&assign(&asking), None).unwrap();
    let Err(NotRun::Rejected { reason, detail }) = plan(&spec, &setup) else {
        panic!("rejected")
    };
    assert_eq!(reason, pb::RejectReason::MemoryBudgetTooLarge);
    assert!(
        detail.contains("of 2 GiB") && detail.contains("at most 1 GiB"),
        "{detail}"
    );

    // Within it, capped: `schedr task-exec` sets the limit, then runs the command.
    asking["settings"]["memory_bytes"] = json!(512_u64 << 20);
    asking["settings"]["hard_memory_limit"] = json!(true);
    let spec = AttemptSpec::from_assign(&assign(&asking), None).unwrap();
    let Err(NotRun::Failed { message, .. }) = plan(&spec, &setup) else {
        panic!("without schedr to start it, it fails")
    };
    assert!(message.contains("can't cap a task's memory"), "{message}");
    setup.memory.task_exec = Some(PathBuf::from("/usr/bin/schedr"));
    let Ok(Plan::Command(process)) = plan(&spec, &setup) else {
        panic!("a command")
    };
    assert_eq!(process.program, PathBuf::from("/usr/bin/schedr"));
    assert_eq!(
        process.args,
        ["task-exec", "--data-limit", "536870912", "--", "echo", "hi"]
    );
    // A Python task's runner the same.
    let mut python = python_task();
    python["settings"]["memory_bytes"] = json!(512_u64 << 20);
    python["settings"]["hard_memory_limit"] = json!(true);
    let spec = AttemptSpec::from_assign(&assign(&python), None).unwrap();
    let Ok(Plan::Python { process, .. }) = plan(&spec, &setup) else {
        panic!("a Python task")
    };
    assert_eq!(
        process.args[..5],
        [
            "task-exec",
            "--data-limit",
            "536870912",
            "--",
            "/usr/bin/python3"
        ]
    );
}
