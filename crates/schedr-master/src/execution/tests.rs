use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::runs::NewRun;
use schedr_model::{Dag, DagId, RunKind};
use serde_json::{Value as Json, json};

use super::*;
use TaskInstanceState as S;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

fn task(name: &str, kind: &str, retries: u32) -> Json {
    json!({
        "id": name, "kind": kind,
        "settings": {
            "retries": retries, "retry_delay_ms": 60_000, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "none"}
    })
}

fn dag(tasks: &Json, edges: &Json) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "schedule": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
        "catchup": false, "max_active_runs": 1, "tasks": tasks, "edges": edges
    }))
    .unwrap()
}

/// `start` (empty) → `extract` (1 retry) → `load`.
fn chain() -> Dag {
    dag(
        &json!([
            task("start", "empty", 0),
            task("extract", "python", 1),
            task("load", "python", 0)
        ]),
        &json!([
            {"from": "start", "to": "extract", "kind": "order"},
            {"from": "extract", "to": "load", "kind": "order"}
        ]),
    )
}

const NOW: &str = "2026-10-21T08:00:00Z";

/// Execution over a database with `dag` stored and one run of it in `state`.
async fn with_run(dag: Dag, state: RunState) -> (Execution, Database, String) {
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
            kind: RunKind::Manual,
            state,
            planned_for: ts(NOW),
            since: ts(NOW),
            triggered_at: ts(NOW),
            triggered_by: "anna",
        })
        .await
        .unwrap()
        .unwrap();
    (Execution::new(db.clone()), db, run_id)
}

async fn states(db: &Database, run_id: &str) -> Vec<(String, TaskInstanceState)> {
    db.run_tasks(run_id)
        .await
        .unwrap()
        .unwrap()
        .instances
        .iter()
        .map(|i| (i.task_id.to_string(), i.state))
        .collect()
}

fn expected(list: &[(&str, TaskInstanceState)]) -> Vec<(String, TaskInstanceState)> {
    list.iter().map(|(t, s)| ((*t).to_owned(), *s)).collect()
}

/// What assignment (1.10b) does: the task's attempt starts.
async fn start(db: &Database, run_id: &str, task: &str) {
    let running = InstanceUpdate {
        task_id: id(task),
        state: S::Running,
        ready_at: None,
        outcome: None,
        used_retry: false,
    };
    db.update_instances(run_id, &[running], ts(NOW))
        .await
        .unwrap();
}

fn ended(status: AttemptStatus) -> AttemptResult {
    AttemptResult {
        status,
        error_kind: None,
        output: None,
    }
}

async fn run_state(db: &Database, run_id: &str) -> RunState {
    db.run_tasks(run_id).await.unwrap().unwrap().state
}

#[tokio::test]
async fn a_new_run_gets_its_instances_once() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    assert_eq!(
        execution.prepare_runs(ts(NOW)).await.unwrap(),
        std::slice::from_ref(&run_id)
    );
    assert_eq!(
        states(&db, &run_id).await,
        expected(&[
            ("extract", S::Queued),
            ("load", S::Scheduled),
            ("start", S::Success)
        ])
    );
    assert_eq!(
        execution.prepare_runs(ts(NOW)).await.unwrap(),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_run_of_empty_tasks_finishes_when_prepared() {
    let only_empty = dag(&json!([task("start", "empty", 0)]), &json!([]));
    let (execution, db, run_id) = with_run(only_empty, RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    assert_eq!(run_state(&db, &run_id).await, RunState::Success);
}

#[tokio::test]
async fn successes_move_the_run_forward_and_finish_it() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    start(&db, &run_id, "extract").await;
    let result = AttemptResult {
        output: Some(json!({"rows": 3})),
        ..ended(AttemptStatus::Success)
    };
    let recorded = execution
        .attempt_finished(&run_id, &id("extract"), &result, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        recorded,
        Recorded {
            state: Some(S::Success),
            run_result: None
        }
    );
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    let extract = run
        .instances
        .iter()
        .find(|i| i.task_id == id("extract"))
        .unwrap();
    assert_eq!(
        db.output(&extract.id).await.unwrap(),
        Some((SUCCESS.to_owned(), json!({"rows": 3})))
    );
    assert_eq!(
        states(&db, &run_id).await[1],
        ("load".to_owned(), S::Queued)
    );

    start(&db, &run_id, "load").await;
    let recorded = execution
        .attempt_finished(
            &run_id,
            &id("load"),
            &ended(AttemptStatus::Success),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(recorded.run_result, Some(RunState::Success));
    assert_eq!(run_state(&db, &run_id).await, RunState::Success);
}

#[tokio::test]
async fn a_failure_with_retries_left_waits_then_queues_again() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    start(&db, &run_id, "extract").await;
    let recorded = execution
        .attempt_finished(
            &run_id,
            &id("extract"),
            &ended(AttemptStatus::Failed),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(recorded.state, Some(S::UpForRetry));
    assert_eq!(
        execution
            .queue_due_retries(ts("2026-10-21T08:00:59Z"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        execution
            .queue_due_retries(ts("2026-10-21T08:01:00Z"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        states(&db, &run_id).await[0],
        ("extract".to_owned(), S::Queued)
    );

    start(&db, &run_id, "extract").await;
    let recorded = execution
        .attempt_finished(
            &run_id,
            &id("extract"),
            &ended(AttemptStatus::TimedOut),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(recorded.state, Some(S::Failed));
    assert_eq!(recorded.run_result, Some(RunState::Failed));
    assert_eq!(
        states(&db, &run_id).await,
        expected(&[
            ("extract", S::Failed),
            ("load", S::UpstreamFailed),
            ("start", S::Success)
        ])
    );
    let extract = &db.run_tasks(&run_id).await.unwrap().unwrap().instances[0];
    assert_eq!(extract.retries_used, 2);
}

#[tokio::test]
async fn an_interrupted_attempt_runs_again_without_a_retry() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    start(&db, &run_id, "extract").await;
    let recorded = execution
        .attempt_finished(
            &run_id,
            &id("extract"),
            &ended(AttemptStatus::Interrupted),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(recorded.state, Some(S::Queued));
    let extract = &db.run_tasks(&run_id).await.unwrap().unwrap().instances[0];
    assert_eq!((extract.retries_used, extract.ready_at), (0, Some(ts(NOW))));
}

#[tokio::test]
async fn a_repeated_result_is_ignored() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    start(&db, &run_id, "extract").await;
    let success = ended(AttemptStatus::Success);
    execution
        .attempt_finished(&run_id, &id("extract"), &success, ts(NOW))
        .await
        .unwrap();
    let again = execution
        .attempt_finished(&run_id, &id("extract"), &success, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        again,
        Recorded {
            state: None,
            run_result: None
        }
    );
}

#[tokio::test]
async fn unknown_runs_and_tasks_are_errors() {
    let (execution, _db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    let error = execution
        .attempt_finished(
            &run_id,
            &id("ghost"),
            &ended(AttemptStatus::Success),
            ts(NOW),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("run `{run_id}` has no task `ghost`")
    );
    let missing = "0190a3e5-0000-7000-8000-000000000000";
    let error = execution.cancel_run(missing, ts(NOW)).await.unwrap_err();
    assert_eq!(error.to_string(), format!("no run `{missing}`"));
}

#[tokio::test]
async fn cancelling_a_queued_run_ends_it_at_once() {
    let (execution, db, run_id) = with_run(chain(), RunState::Queued).await;
    let cancelled = execution.cancel_run(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(
        cancelled,
        Cancelled {
            running: vec![],
            run_result: Some(RunState::Cancelled)
        }
    );
    assert_eq!(run_state(&db, &run_id).await, RunState::Cancelled);
    let again = execution.cancel_run(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(
        again,
        Cancelled {
            running: vec![],
            run_result: None
        }
    );
}

#[tokio::test]
async fn cancelling_a_running_run_stops_the_rest_and_its_attempts_decide_the_result() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    start(&db, &run_id, "extract").await;
    let cancelled = execution.cancel_run(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(
        cancelled,
        Cancelled {
            running: vec![id("extract")],
            run_result: None
        }
    );
    assert_eq!(
        states(&db, &run_id).await,
        expected(&[
            ("extract", S::Running),
            ("load", S::Cancelled),
            ("start", S::Success)
        ])
    );
    // The attempt failed before the cancel reached it: a failure isn't retried, and counts.
    let recorded = execution
        .attempt_finished(
            &run_id,
            &id("extract"),
            &ended(AttemptStatus::Failed),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(
        recorded,
        Recorded {
            state: Some(S::Failed),
            run_result: Some(RunState::Failed)
        }
    );
}

#[tokio::test]
async fn a_cancelled_attempt_ends_the_run_cancelled() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    start(&db, &run_id, "extract").await;
    execution.cancel_run(&run_id, ts(NOW)).await.unwrap();
    let recorded = execution
        .attempt_finished(
            &run_id,
            &id("extract"),
            &ended(AttemptStatus::Cancelled),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(recorded.run_result, Some(RunState::Cancelled));
}

#[tokio::test]
async fn cancelling_when_nothing_runs_finishes_the_run() {
    let (execution, db, run_id) = with_run(chain(), RunState::Running).await;
    execution.prepare_runs(ts(NOW)).await.unwrap();
    let cancelled = execution.cancel_run(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(cancelled.run_result, Some(RunState::Cancelled));
    assert_eq!(run_state(&db, &run_id).await, RunState::Cancelled);
}
