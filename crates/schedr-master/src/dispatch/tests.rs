use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::runs::NewRun;
use schedr_model::{Dag, DagId, RunKind, RunState};
use serde_json::{Value as Json, json};

use super::*;
use pb::master_message::Message;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

fn later(seconds: i64) -> Timestamp {
    ts(NOW)
        .checked_add(SignedDuration::from_secs(seconds))
        .unwrap()
}

fn task(name: &str, arguments: &Json, retries: u32) -> Json {
    json!({
        "id": name, "kind": "python", "arguments": arguments,
        "settings": {
            "retries": retries, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "any"}
    })
}

/// `extract` → `load(rows=extract)`.
fn dag(retries: u32) -> Dag {
    let rows = json!({"rows": {"ref": {"kind": "output", "task": "extract"}}});
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "team/sync.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [task("extract", &json!({}), retries), task("load", &rows, 0)],
        "edges": [{"from": "extract", "to": "load", "kind": "data"}]
    }))
    .unwrap()
}

/// A dispatcher over a database with a running run of `dag`, and worker `w1` (2 slots).
async fn with_run(dag: Dag) -> (Dispatcher, Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts(NOW)).await.unwrap();
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    let dag_id = DagId::new("sync").unwrap();
    let run_id = db
        .insert_run(&NewRun {
            dag_id: &dag_id,
            dag_version_id: &version_id,
            kind: RunKind::Scheduled,
            state: RunState::Running,
            planned_for: ts(NOW),
            since: ts("2026-10-20T08:00:00Z"),
            triggered_at: ts(NOW),
            triggered_by: "scheduler",
        })
        .await
        .unwrap()
        .unwrap();
    let mut dispatcher = Dispatcher::new(db.clone(), DEFAULT_CANCEL_GRACE);
    dispatcher
        .register("w1", "i1", &[], 2, ts(NOW))
        .await
        .unwrap();
    (dispatcher, db, run_id)
}

fn assignment(outgoing: &Outgoing) -> &pb::AssignTask {
    match &outgoing.message.message {
        Some(Message::AssignTask(assign)) => assign,
        other => panic!("expected AssignTask, got {other:?}"),
    }
}

fn cancelled_attempt(outgoing: &Outgoing) -> &str {
    match &outgoing.message.message {
        Some(Message::CancelAttempt(cancel)) => &cancel.attempt_id,
        other => panic!("expected CancelAttempt, got {other:?}"),
    }
}

fn started(attempt_id: &str) -> pb::Started {
    pb::Started {
        attempt_id: attempt_id.to_owned(),
        start_method: pb::StartMethod::Spawn.into(),
    }
}

fn finished(attempt_id: &str, status: pb::AttemptStatus, output: &Json) -> pb::Finished {
    pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: status.into(),
        output_json: serde_json::to_vec(output).unwrap(),
        outcome: "SUCCESS".to_owned(),
        code_hash: "sha256:abc".to_owned(),
        ..pb::Finished::default()
    }
}

fn rejected(attempt_id: &str, reason: pb::RejectReason, detail: &str) -> pb::Rejected {
    pb::Rejected {
        attempt_id: attempt_id.to_owned(),
        reason: reason.into(),
        detail: detail.to_owned(),
    }
}

async fn state_of(db: &Database, run_id: &str, task: &str) -> (TaskInstanceState, u32) {
    let run = db.run_tasks(run_id).await.unwrap().unwrap();
    let instance = run
        .instances
        .iter()
        .find(|i| i.task_id == id(task))
        .unwrap();
    (instance.state, instance.try_number)
}

#[tokio::test]
async fn a_run_goes_through_assignment_start_and_finish() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].worker_id, "w1");
    let assign = assignment(&out[0]);
    assert_eq!(
        (assign.task_id.as_str(), assign.try_number, assign.map_index),
        ("extract", 1, -1)
    );
    let run = assign.run.as_ref().unwrap();
    assert_eq!(
        (run.run_id.as_str(), run.kind.as_str(), run.dag_id.as_str()),
        (run_id.as_str(), "scheduled", "sync")
    );
    assert_eq!(
        run.planned_for.as_ref().unwrap().seconds,
        ts(NOW).as_second()
    );
    assert_eq!(assign.dag_file.as_ref().unwrap().path, "team/sync.py");
    assert_eq!(assign.expected_code_hash, "h1");
    assert_eq!(assign.inputs_json, b"{}");
    assert!(!assign.inputs_external);
    assert_eq!(assign.cancel_grace_ms, 30_000);
    let task: schedr_model::Task = serde_json::from_slice(&assign.task_json).unwrap();
    assert_eq!(task.id, id("extract"));
    assert_eq!(dispatcher.workers().get("w1").unwrap().free_slots(), 1);

    let extract_attempt = assign.attempt_id.clone();
    assert_eq!(
        dispatcher
            .started("w1", &started(&extract_attempt), ts(NOW))
            .await
            .unwrap(),
        None
    );
    let ack = dispatcher
        .finished(
            "w1",
            &finished(&extract_attempt, pb::AttemptStatus::Success, &json!([1, 2])),
            ts(NOW),
        )
        .await
        .unwrap();
    assert!(matches!(ack.message.message, Some(Message::FinishedAck(_))));
    assert_eq!(dispatcher.workers().get("w1").unwrap().free_slots(), 2);

    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let load = assignment(&out[0]);
    assert_eq!(load.task_id, "load");
    let inputs: Json = serde_json::from_slice(&load.inputs_json).unwrap();
    assert_eq!(inputs, json!({"extract": [1, 2]}));
    dispatcher
        .started("w1", &started(&load.attempt_id), ts(NOW))
        .await
        .unwrap();
    dispatcher
        .finished(
            "w1",
            &finished(&load.attempt_id, pb::AttemptStatus::Success, &json!(null)),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(
        db.run_tasks(&run_id).await.unwrap().unwrap().state,
        RunState::Success
    );
}

#[tokio::test]
async fn the_duration_is_the_workers_even_when_the_clock_steps_back() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let later = ts(NOW) + jiff::SignedDuration::from_secs(5);
    dispatcher
        .started("w1", &started(&attempt), later)
        .await
        .unwrap();
    let done = pb::Finished {
        duration_ms: Some(300),
        ..finished(&attempt, pb::AttemptStatus::Success, &json!([1]))
    };
    // The master's wall clock stepped back 4 s between `Started` and `Finished`.
    let stepped_back = later - jiff::SignedDuration::from_secs(4);
    dispatcher
        .finished("w1", &done, stepped_back)
        .await
        .unwrap();
    let attempts = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    assert_eq!(attempts[0].duration_ms, Some(300));
    assert!(attempts[0].ended_at < attempts[0].started_at);
}

#[tokio::test]
async fn without_a_free_slot_nothing_is_assigned() {
    let (mut dispatcher, _db, _run_id) = with_run(dag(0)).await;
    dispatcher
        .register("w1", "i2", &[], 0, ts(NOW))
        .await
        .unwrap();
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    dispatcher
        .register("w1", "i2", &[], 1, ts(NOW))
        .await
        .unwrap();
    dispatcher.draining("w1");
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
}

#[tokio::test]
async fn a_repeated_finish_is_acknowledged_and_changes_nothing() {
    let (mut dispatcher, db, run_id) = with_run(dag(1)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .started("w1", &started(&attempt), ts(NOW))
        .await
        .unwrap();
    let failed = finished(&attempt, pb::AttemptStatus::Failed, &json!(null));
    dispatcher.finished("w1", &failed, ts(NOW)).await.unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::UpForRetry
    );
    let again = dispatcher.finished("w1", &failed, ts(NOW)).await.unwrap();
    assert!(matches!(
        again.message.message,
        Some(Message::FinishedAck(_))
    ));
    let unknown = finished(
        "0190a3e5-0000-7000-8000-000000000000",
        pb::AttemptStatus::Success,
        &json!(1),
    );
    dispatcher.finished("w1", &unknown, ts(NOW)).await.unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(run.instances[0].retries_used, 1);
}

#[tokio::test]
async fn a_start_for_an_attempt_taken_back_is_answered_with_a_cancel() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    // No Started within 10 s: taken back, and assigned again (try 2, no retry used).
    let out = dispatcher.step(later(11)).await.unwrap();
    let second = assignment(&out[0]);
    assert_eq!(second.try_number, 2);
    assert_eq!(
        state_of(&db, &run_id, "extract").await,
        (TaskInstanceState::Running, 2)
    );
    let late = dispatcher
        .started("w1", &started(&attempt), later(12))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled_attempt(&late), attempt);
    let other = dispatcher
        .started("w2", &started(&second.attempt_id), later(12))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(other.worker_id, "w2");
}

#[tokio::test]
async fn temporary_rejections_queue_the_task_again_at_once() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .rejected(
            "w1",
            &rejected(&attempt, pb::RejectReason::NoFreeSlot, ""),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await,
        (TaskInstanceState::Queued, 0)
    );
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(assignment(&out[0]).try_number, 1);
}

#[tokio::test]
async fn code_not_there_yet_waits_before_the_next_try() {
    let (mut dispatcher, _db, _run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let missing = rejected(
        &attempt,
        pb::RejectReason::DagFileMissing,
        "team/sync.py not found",
    );
    dispatcher.rejected("w1", &missing, ts(NOW)).await.unwrap();
    assert_eq!(dispatcher.step(later(34)).await.unwrap(), []);
    assert_eq!(dispatcher.step(later(35)).await.unwrap().len(), 1);
}

#[tokio::test]
async fn worker_specific_rejections_try_other_workers_then_make_the_task_unschedulable() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher
        .register("w2", "i1", &[], 1, ts(NOW))
        .await
        .unwrap();
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let first = assignment(&out[0]).attempt_id.clone();
    assert_eq!(out[0].worker_id, "w1");
    let no_venv = |a: &str| rejected(a, pb::RejectReason::InterpreterUndefined, "no etl-venv");
    dispatcher
        .rejected("w1", &no_venv(&first), ts(NOW))
        .await
        .unwrap();
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(out[0].worker_id, "w2");
    let second = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .rejected("w2", &no_venv(&second), ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Unschedulable
    );
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    // A new worker gives it another chance.
    dispatcher
        .register("w3", "i1", &[], 1, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
}

#[tokio::test]
async fn rejections_that_dont_match_an_open_assignment_are_ignored() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let reason = rejected(&attempt, pb::RejectReason::NoFreeSlot, "");
    dispatcher.rejected("w2", &reason, ts(NOW)).await.unwrap();
    dispatcher
        .started("w1", &started(&attempt), ts(NOW))
        .await
        .unwrap();
    dispatcher.rejected("w1", &reason, ts(NOW)).await.unwrap();
    let unknown = rejected(
        "0190a3e5-0000-7000-8000-000000000000",
        pb::RejectReason::NoFreeSlot,
        "",
    );
    dispatcher.rejected("w1", &unknown, ts(NOW)).await.unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Running
    );
}

#[tokio::test]
async fn messages_with_unknown_values_are_errors() {
    let (mut dispatcher, _db, _run_id) = with_run(dag(0)).await;
    let bad = pb::Rejected {
        reason: 99,
        ..rejected("a", pb::RejectReason::NoFreeSlot, "")
    };
    let error = dispatcher.rejected("w1", &bad, ts(NOW)).await.unwrap_err();
    assert!(
        error.to_string().starts_with("message from worker `w1`: "),
        "{error}"
    );
    let bad = pb::Finished {
        status: 0,
        ..pb::Finished::default()
    };
    assert!(matches!(
        dispatcher.finished("w1", &bad, ts(NOW)).await,
        Err(DispatchError::BadMessage { .. })
    ));
}

#[tokio::test]
async fn a_lost_workers_attempts_use_a_retry() {
    let (mut dispatcher, db, run_id) = with_run(dag(1)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .started("w1", &started(&attempt), ts(NOW))
        .await
        .unwrap();
    dispatcher.worker_lost("w1", ts(NOW)).await.unwrap();
    assert_eq!(dispatcher.workers().get("w1"), None);
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::UpForRetry
    );
    assert_eq!(
        db.attempt(&attempt).await.unwrap().unwrap().status,
        Some(AttemptStatus::Lost)
    );
    let shown = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    let summary: ErrorSummary =
        serde_json::from_str(shown[0].error_summary.as_deref().unwrap()).unwrap();
    assert_eq!(summary.exception, "WorkerLost");
}

#[tokio::test]
async fn cancelling_a_run_cancels_its_running_attempts() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .started("w1", &started(&attempt), ts(NOW))
        .await
        .unwrap();
    let out = dispatcher.cancel_run(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(
        (out[0].worker_id.as_str(), cancelled_attempt(&out[0])),
        ("w1", attempt.as_str())
    );
    let done = finished(&attempt, pb::AttemptStatus::Cancelled, &json!(null));
    dispatcher.finished("w1", &done, ts(NOW)).await.unwrap();
    assert_eq!(
        db.run_tasks(&run_id).await.unwrap().unwrap().state,
        RunState::Cancelled
    );
}

#[tokio::test]
async fn a_replaced_incarnation_loses_only_its_started_unreported_attempts() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let none = BTreeSet::new();
    assert_eq!(
        dispatcher
            .incarnation_replaced("w1", &none, ts(NOW))
            .await
            .unwrap(),
        0,
        "not started: left to expire"
    );
    dispatcher
        .started("w1", &started(&attempt), ts(NOW))
        .await
        .unwrap();
    let reported = BTreeSet::from([attempt.as_str()]);
    assert_eq!(
        dispatcher
            .incarnation_replaced("w1", &reported, ts(NOW))
            .await
            .unwrap(),
        0,
        "reported in the journal: kept"
    );
    assert_eq!(
        dispatcher
            .incarnation_replaced("w1", &none, ts(NOW))
            .await
            .unwrap(),
        1
    );
    let ended = db.attempt(&attempt).await.unwrap().unwrap();
    assert_eq!(ended.status, Some(AttemptStatus::Lost));
    let task = assignment(&out[0]).task_id.clone();
    let shown = db.task_attempts(&run_id, &id(&task)).await.unwrap();
    let summary: ErrorSummary =
        serde_json::from_str(shown[0].error_summary.as_deref().unwrap()).unwrap();
    assert_eq!(summary.exception, "WorkerRestarted");
    assert_eq!(
        summary.message,
        "worker `w1` restarted while the attempt ran (possibly out of memory)"
    );
}
