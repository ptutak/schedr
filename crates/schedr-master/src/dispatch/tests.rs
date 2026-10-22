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
    assert_eq!(
        assign.expected_code_hash, "",
        "no current code known: no check"
    );
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

async fn reason_of(db: &Database, run_id: &str, task: &str) -> Option<String> {
    db.run_instances(run_id)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.task_id == id(task))
        .and_then(|i| i.queued_reason)
}

#[tokio::test]
async fn a_task_that_waits_says_why() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher
        .register("w1", "i2", &["large".to_owned()], 2, ts(NOW))
        .await
        .unwrap();
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("no active worker serves queue `default`")
    );
    dispatcher
        .register("w2", "i1", &[], 0, ts(NOW))
        .await
        .unwrap();
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("waiting for a free slot on the workers serving queue `default`")
    );
    dispatcher
        .register("w2", "i2", &[], 1, ts(NOW))
        .await
        .unwrap();
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(out[0].worker_id, "w2");
    assert_eq!(reason_of(&db, &run_id, "extract").await, None);
}

#[tokio::test]
async fn code_missing_everywhere_for_the_code_wait_makes_the_task_unschedulable() {
    let (dispatcher, db, run_id) = with_run(dag(0)).await;
    let mut dispatcher = dispatcher.with_code_wait(Duration::from_secs(60));
    dispatcher
        .register("w2", "i1", &[], 1, ts(NOW))
        .await
        .unwrap();
    let missing = |a: &str| rejected(a, pb::RejectReason::DagFileMissing, "no team/sync.py");
    let mut at = 0;
    for (worker, expected) in [
        // w2 hasn't said so yet: it is asked next.
        ("w1", TaskInstanceState::Queued),
        // Both have, but not for the code wait yet.
        ("w2", TaskInstanceState::Queued),
        // Still missing everywhere, 70 s after the first: unschedulable.
        ("w1", TaskInstanceState::Unschedulable),
    ] {
        let out = dispatcher.step(later(at)).await.unwrap();
        let attempt = out.iter().find(|o| o.worker_id == worker).map_or_else(
            || panic!("nothing for {worker} at {at} s: {out:?}"),
            |o| assignment(o).attempt_id.clone(),
        );
        dispatcher
            .rejected(worker, &missing(&attempt), later(at))
            .await
            .unwrap();
        assert_eq!(
            state_of(&db, &run_id, "extract").await.0,
            expected,
            "{at} s"
        );
        at += 35;
    }
    let reason = reason_of(&db, &run_id, "extract").await.unwrap();
    assert_eq!(
        reason,
        "the code is missing on every eligible worker after 1 min \
         (w1: no team/sync.py; w2: no team/sync.py)"
    );
    assert!(matches!(
        dispatcher
            .retry_task(&run_id, &id("extract"), later(at))
            .await
            .unwrap(),
        TaskRetry::Queued(_)
    ));
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
    assert_eq!(
        dispatcher
            .retry_task(&run_id, &id("extract"), later(at))
            .await
            .unwrap(),
        TaskRetry::NotUnschedulable(TaskInstanceState::Queued)
    );
    assert_eq!(
        dispatcher
            .retry_task(&run_id, &id("load"), later(at))
            .await
            .unwrap(),
        TaskRetry::NotUnschedulable(TaskInstanceState::Scheduled)
    );
}

#[tokio::test]
async fn retrying_needs_a_known_task_of_an_unfinished_run() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    assert_eq!(
        dispatcher
            .retry_task(&run_id, &id("extract"), ts(NOW))
            .await
            .unwrap(),
        TaskRetry::RunQueued,
        "no instances yet"
    );
    assert!(matches!(
        dispatcher.retry_task(&run_id, &id("ghost"), ts(NOW)).await,
        Err(DispatchError::Execution(ExecutionError::UnknownTask { .. }))
    ));
    assert!(matches!(
        dispatcher
            .retry_task(
                "0190a3e5-0000-7000-8000-000000000000",
                &id("extract"),
                ts(NOW)
            )
            .await,
        Err(DispatchError::Execution(ExecutionError::UnknownRun(_)))
    ));
    db.finish_run(&run_id, RunState::Failed, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .retry_task(&run_id, &id("extract"), ts(NOW))
            .await
            .unwrap(),
        TaskRetry::RunFinished(RunState::Failed)
    );
}

#[tokio::test]
async fn a_running_task_is_cancelled_through_its_worker() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let (cancelled, out) = dispatcher
        .cancel_task(&run_id, &id("extract"), ts(NOW))
        .await
        .unwrap();
    assert_eq!(cancelled, TaskCancel::Stopping);
    assert_eq!(cancelled_attempt(&out[0]), attempt);
    let done = finished(&attempt, pb::AttemptStatus::Cancelled, &json!(null));
    dispatcher.finished("w1", &done, ts(NOW)).await.unwrap();
    assert_eq!(
        db.run_tasks(&run_id).await.unwrap().unwrap().state,
        RunState::Cancelled
    );
    let (cancelled, out) = dispatcher
        .cancel_task(&run_id, &id("load"), ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        (cancelled, out.len()),
        (TaskCancel::RunFinished(RunState::Cancelled), 0)
    );
}

#[tokio::test]
async fn a_rejected_assignment_of_a_task_being_cancelled_cancels_it() {
    for whole_run in [false, true] {
        let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
        let out = dispatcher.step(ts(NOW)).await.unwrap();
        let attempt = assignment(&out[0]).attempt_id.clone();
        if whole_run {
            dispatcher.cancel_run(&run_id, ts(NOW)).await.unwrap();
        } else {
            dispatcher
                .cancel_task(&run_id, &id("extract"), ts(NOW))
                .await
                .unwrap();
        }
        let busy = rejected(&attempt, pb::RejectReason::NoFreeSlot, "no free slot");
        dispatcher.rejected("w1", &busy, ts(NOW)).await.unwrap();
        assert_eq!(
            state_of(&db, &run_id, "extract").await,
            (TaskInstanceState::Cancelled, 0),
            "never a try (whole run: {whole_run})"
        );
        assert_eq!(
            db.run_tasks(&run_id).await.unwrap().unwrap().state,
            RunState::Cancelled,
            "B5: the run isn't left running (whole run: {whole_run})"
        );
    }
}

#[tokio::test]
async fn worker_specific_and_code_rejections_together_make_the_task_unschedulable() {
    let (dispatcher, db, run_id) = with_run(dag(0)).await;
    let mut dispatcher = dispatcher.with_code_wait(Duration::from_secs(60));
    dispatcher
        .register("w2", "i1", &[], 1, ts(NOW))
        .await
        .unwrap();
    let mut at = 0;
    for (worker, reason, expected) in [
        (
            "w1",
            pb::RejectReason::InterpreterUndefined,
            TaskInstanceState::Queued,
        ),
        (
            "w2",
            pb::RejectReason::DagFileMissing,
            TaskInstanceState::Queued,
        ),
        (
            "w2",
            pb::RejectReason::DagFileMissing,
            TaskInstanceState::Queued,
        ),
        (
            "w2",
            pb::RejectReason::DagFileMissing,
            TaskInstanceState::Unschedulable,
        ),
    ] {
        let out = dispatcher.step(later(at)).await.unwrap();
        assert_eq!(out[0].worker_id, worker, "{at} s");
        let attempt = assignment(&out[0]).attempt_id.clone();
        dispatcher
            .rejected(worker, &rejected(&attempt, reason, "no"), later(at))
            .await
            .unwrap();
        assert_eq!(
            state_of(&db, &run_id, "extract").await.0,
            expected,
            "{at} s"
        );
        at += 35;
    }
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("every eligible worker rejected it (w1: no; w2: no)")
    );
}

#[tokio::test]
async fn a_task_that_started_somewhere_forgets_who_rejected_it() {
    let (dispatcher, db, run_id) = with_run(dag(1)).await;
    let mut dispatcher = dispatcher.with_code_wait(Duration::from_secs(60));
    let missing = |a: &str| rejected(a, pb::RejectReason::DagFileMissing, "no team/sync.py");
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let first = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .rejected("w1", &missing(&first), ts(NOW))
        .await
        .unwrap();
    let out = dispatcher.step(later(35)).await.unwrap();
    let second = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .started("w1", &started(&second), later(35))
        .await
        .unwrap();
    let failed = finished(&second, pb::AttemptStatus::Failed, &json!(null));
    dispatcher.finished("w1", &failed, later(36)).await.unwrap();
    // The retry, long after the first rejection, meets missing code again: a fresh wait.
    let out = dispatcher.step(later(600)).await.unwrap();
    let third = assignment(&out[0]).attempt_id.clone();
    dispatcher
        .rejected("w1", &missing(&third), later(600))
        .await
        .unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
}

#[tokio::test]
async fn a_task_every_remaining_worker_rejected_says_so() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher
        .register("w2", "i1", &[], 1, ts(NOW))
        .await
        .unwrap();
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let no_venv = rejected(&attempt, pb::RejectReason::InterpreterUndefined, "no venv");
    dispatcher.rejected("w1", &no_venv, ts(NOW)).await.unwrap();
    dispatcher.worker_lost("w2", ts(NOW)).await.unwrap();
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("every active worker serving queue `default` rejected it")
    );
}

#[tokio::test]
async fn only_workers_with_the_newest_code_get_the_task_and_are_told_its_hash() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher.report_root("w1", "etl", "h1");
    dispatcher.set_current_code("etl", &BTreeSet::from(["h2".to_owned()]), None);
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("waiting for workers serving queue `default` to have the newest code of root `etl`")
    );
    dispatcher
        .register("w2", "i1", &[], 1, ts(NOW))
        .await
        .unwrap();
    dispatcher.report_root("w2", "etl", "h2");
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(out[0].worker_id, "w2");
    assert_eq!(assignment(&out[0]).expected_code_hash, "h2");
}

#[tokio::test]
async fn a_worker_older_than_the_parse_doesnt_get_the_task() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher.set_worker_version("w1", Some(SchedrVersion::new(0, 0)));
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("no worker with schedr ≥ 0.1 serves queue `default`")
    );
    dispatcher.set_worker_version("w1", Some(SchedrVersion::new(0, 1)));
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(
        assignment(&out[0]).expected_code_hash,
        "",
        "no check while the master knows no current code"
    );
}

#[tokio::test]
async fn the_current_codes_version_counts_too() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher.set_worker_version("w1", Some(SchedrVersion::new(0, 1)));
    dispatcher.report_root("w1", "etl", "h2");
    dispatcher.set_current_code(
        "etl",
        &BTreeSet::from(["h2".to_owned()]),
        Some(SchedrVersion::new(0, 2)),
    );
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("no worker with schedr ≥ 0.2 serves queue `default`")
    );
}

#[tokio::test]
async fn no_eligible_worker_left_isnt_everyone_rejecting_it() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher.report_root("w1", "etl", "h1");
    dispatcher.set_current_code("etl", &BTreeSet::from(["h1".to_owned()]), None);
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    // Meanwhile newer code is current, which w1 hasn't synced: nobody is eligible.
    dispatcher.set_current_code("etl", &BTreeSet::from(["h2".to_owned()]), None);
    let no_venv = rejected(&attempt, pb::RejectReason::InterpreterUndefined, "no venv");
    dispatcher.rejected("w1", &no_venv, ts(NOW)).await.unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
}

/// Stores a newer version of DAG `sync` (from parse `h9`) defined as `dag`.
async fn redefine(db: &Database, dag: Dag) {
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h9",
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
}

/// Runs `extract`, which fails with `kind` and `summary`, after `change`; returns the stored
/// error summary.
async fn gone_after(
    change: impl AsyncFnOnce(&Database),
    kind: &str,
    summary: Option<&str>,
) -> Option<ErrorSummary> {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    change(&db).await;
    let failed = pb::Finished {
        error_kind: kind.to_owned(),
        error_summary_json: summary.map(|s| s.as_bytes().to_vec()).unwrap_or_default(),
        ..finished(&attempt, pb::AttemptStatus::Failed, &json!(null))
    };
    dispatcher.finished("w1", &failed, ts(NOW)).await.unwrap();
    db.task_attempts(&run_id, &id("extract")).await.unwrap()[0]
        .error_summary
        .as_deref()
        .map(|s| serde_json::from_str(s).unwrap())
}

#[tokio::test]
async fn a_task_or_file_gone_since_the_run_started_is_explained() {
    let not_found =
        r#"{"exception": "TaskSetupError", "message": "no task `extract`", "traceback": ""}"#;
    let removed = async |db: &Database| {
        let mut newer = dag(0);
        newer.tasks.retain(|t| t.id.as_str() == "load");
        newer.edges.clear();
        redefine(db, newer).await;
    };
    let summary = gone_after(removed, "task_not_found", Some(not_found))
        .await
        .unwrap();
    assert_eq!(
        (summary.exception.as_str(), summary.message.as_str()),
        (
            "TaskSetupError",
            "task `extract` was removed from DAG `sync` (`etl:team/sync.py`) after this run \
             started: no task `extract`"
        )
    );

    let moved = async |db: &Database| {
        let mut newer = dag(0);
        newer.location.path = "team/other.py".to_owned();
        redefine(db, newer).await;
    };
    let summary = gone_after(moved, "dag_file_missing", None).await.unwrap();
    assert_eq!(
        (summary.exception.as_str(), summary.message.as_str()),
        (
            "CodeChanged",
            "DAG `sync` moved from `etl:team/sync.py` to `etl:team/other.py` after this run \
             started"
        )
    );

    let dropped = async |db: &Database| {
        let gone = DagChange::Remove {
            dag_id: DagId::new("sync").unwrap(),
        };
        db.apply_dag_changes(&[gone], ts(NOW)).await.unwrap();
    };
    let summary = gone_after(dropped, "dag_file_missing", None).await.unwrap();
    assert_eq!(
        summary.message,
        "DAG `sync` was removed from `etl:team/sync.py` after this run started"
    );

    let unchanged = async |_: &Database| {};
    assert_eq!(gone_after(unchanged, "task_not_found", None).await, None);
    let other_failure = async |db: &Database| redefine(db, dag(0)).await;
    assert_eq!(gone_after(other_failure, "import_failed", None).await, None);
}

#[tokio::test]
async fn a_worker_on_newer_code_whose_parse_is_on_its_way_gets_the_task() {
    let (mut dispatcher, _db, _run_id) = with_run(dag(0)).await;
    dispatcher.report_root("w1", "etl", "h3");
    let accepted = BTreeSet::from(["h2".to_owned(), "h3".to_owned()]);
    dispatcher.set_current_code("etl", &accepted, None);
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    assert_eq!(
        assignment(&out[0]).expected_code_hash,
        "h3",
        "it checks the hash it reported"
    );
}

#[tokio::test]
async fn resuming_a_worker_gives_unschedulable_tasks_another_chance() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    let out = dispatcher.step(ts(NOW)).await.unwrap();
    let attempt = assignment(&out[0]).attempt_id.clone();
    let no_venv = rejected(&attempt, pb::RejectReason::InterpreterUndefined, "no venv");
    dispatcher.rejected("w1", &no_venv, ts(NOW)).await.unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Unschedulable
    );
    dispatcher.drain("w1");
    dispatcher.resume("w1", ts(NOW)).await.unwrap();
    assert_eq!(
        state_of(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
}

#[tokio::test]
async fn a_queue_whose_workers_are_drained_says_so() {
    let (mut dispatcher, db, run_id) = with_run(dag(0)).await;
    dispatcher.drain("w1");
    assert_eq!(dispatcher.step(ts(NOW)).await.unwrap(), []);
    assert_eq!(
        reason_of(&db, &run_id, "extract").await.as_deref(),
        Some("the workers serving queue `default` are drained (`schedr workers resume`)")
    );
}
