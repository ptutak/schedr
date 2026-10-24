use pretty_assertions::assert_eq;
use schedr_model::{DagId, RunKind};
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::runs::NewRun;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

fn dag() -> Dag {
    let task = |name: &str| {
        json!({
            "id": name, "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        })
    };
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [task("extract"), task("load")],
        "edges": [{"from": "extract", "to": "load", "kind": "order"}]
    }))
    .unwrap()
}

/// A database with DAG `sync` and one run of it in `state`; returns it with the run's ID.
async fn with_run(state: RunState) -> (Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts("2026-10-01T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag()),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-10-01T00:00:00Z"))
        .await
        .unwrap();
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    let dag_id = DagId::new("sync").unwrap();
    let run_id = db
        .insert_run(&NewRun {
            dag_id: &dag_id,
            dag_version_id: &version_id,
            kind: RunKind::Manual,
            state,
            planned_for: ts("2026-10-21T08:00:00Z"),
            since: ts("2026-10-20T08:00:00Z"),
            triggered_at: ts("2026-10-21T08:00:00Z"),
            triggered_by: "anna",
        })
        .await
        .unwrap()
        .unwrap();
    (db, run_id)
}

#[tokio::test]
async fn running_runs_without_instances_are_newly_running() {
    let (db, run_id) = with_run(RunState::Running).await;
    let new = db.newly_running().await.unwrap();
    assert_eq!(new.len(), 1);
    assert_eq!(
        (new[0].run_id.as_str(), &*new[0].dag),
        (run_id.as_str(), &dag())
    );
    let now = ts("2026-10-21T08:00:01Z");
    db.create_instances(
        &run_id,
        &[
            (id("extract"), TaskInstanceState::Queued),
            (id("load"), TaskInstanceState::Scheduled),
        ],
        now,
    )
    .await
    .unwrap();
    assert_eq!(db.newly_running().await.unwrap(), []);
    let (queued_db, _) = with_run(RunState::Queued).await;
    assert_eq!(queued_db.newly_running().await.unwrap(), []);
}

#[tokio::test]
async fn instances_are_read_back_with_the_run() {
    let (db, run_id) = with_run(RunState::Running).await;
    let now = ts("2026-10-21T08:00:01Z");
    db.create_instances(
        &run_id,
        &[
            (id("load"), TaskInstanceState::Scheduled),
            (id("extract"), TaskInstanceState::Queued),
        ],
        now,
    )
    .await
    .unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(run.state, RunState::Running);
    assert!(!run.cancel_requested);
    assert_eq!(*run.dag, dag());
    assert_eq!(run.parse_version, "0.1.0");
    let summary: Vec<(&str, TaskInstanceState, Option<Timestamp>)> = run
        .instances
        .iter()
        .map(|i| (i.task_id.as_str(), i.state, i.ready_at))
        .collect();
    assert_eq!(
        summary,
        [
            ("extract", TaskInstanceState::Queued, Some(now)),
            ("load", TaskInstanceState::Scheduled, None)
        ]
    );
    assert_eq!(
        (run.instances[0].try_number, run.instances[0].retries_used),
        (0, 0)
    );
    assert_eq!(
        db.run_tasks("0190a3e5-0000-7000-8000-000000000000")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn updates_change_states_and_count_retries() {
    let (db, run_id) = with_run(RunState::Running).await;
    let now = ts("2026-10-21T08:00:01Z");
    db.create_instances(&run_id, &[(id("extract"), TaskInstanceState::Running)], now)
        .await
        .unwrap();
    let retry_at = ts("2026-10-21T08:05:00Z");
    let retry = InstanceUpdate {
        task_id: id("extract"),
        state: TaskInstanceState::UpForRetry,
        ready_at: Some(retry_at),
        outcome: None,
        used_retry: true,
    };
    db.update_instances(&run_id, &[retry], now).await.unwrap();
    let instance = &db.run_tasks(&run_id).await.unwrap().unwrap().instances[0];
    assert_eq!(
        (instance.state, instance.retries_used, instance.ready_at),
        (TaskInstanceState::UpForRetry, 1, Some(retry_at))
    );
    assert_eq!(
        db.due_retries(ts("2026-10-21T08:04:59Z")).await.unwrap(),
        []
    );
    assert_eq!(
        db.due_retries(retry_at).await.unwrap(),
        [(run_id.clone(), id("extract"))]
    );
}

#[tokio::test]
async fn outputs_are_stored_once_per_instance() {
    let (db, run_id) = with_run(RunState::Running).await;
    db.create_instances(
        &run_id,
        &[(id("extract"), TaskInstanceState::Running)],
        ts("2026-10-21T08:00:01Z"),
    )
    .await
    .unwrap();
    let instance_id = db.run_tasks(&run_id).await.unwrap().unwrap().instances[0]
        .id
        .clone();
    assert_eq!(db.output(&instance_id).await.unwrap(), None);
    db.store_output(&instance_id, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    db.store_output(&instance_id, "SUCCESS", &json!({"rows": 4}))
        .await
        .unwrap();
    assert_eq!(
        db.output(&instance_id).await.unwrap(),
        Some(("SUCCESS".to_owned(), json!({"rows": 4})))
    );
}

#[tokio::test]
async fn runs_are_cancelled_once_and_finished() {
    let (db, run_id) = with_run(RunState::Running).await;
    db.request_cancel(&run_id, ts("2026-10-21T09:00:00Z"))
        .await
        .unwrap();
    db.request_cancel(&run_id, ts("2026-10-21T10:00:00Z"))
        .await
        .unwrap();
    assert!(
        db.run_tasks(&run_id)
            .await
            .unwrap()
            .unwrap()
            .cancel_requested
    );
    db.finish_run(&run_id, RunState::Cancelled, ts("2026-10-21T09:00:01Z"))
        .await
        .unwrap();
    assert_eq!(
        db.run_tasks(&run_id).await.unwrap().unwrap().state,
        RunState::Cancelled
    );
}

#[tokio::test]
async fn stored_values_that_dont_read_are_errors() {
    let (db, run_id) = with_run(RunState::Running).await;
    db.create_instances(
        &run_id,
        &[(id("extract"), TaskInstanceState::UpForRetry)],
        ts("2026-10-21T08:00:01Z"),
    )
    .await
    .unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query("UPDATE task_instances SET ready_at = 'soon'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.run_tasks(&run_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in task_instances.ready_at: "),
        "{error}"
    );
    sqlx::query(
        "UPDATE task_instances SET ready_at = '2026-01-01T00:00:00.000000Z', task_id = 'bad id!'",
    )
    .execute(pool)
    .await
    .unwrap();
    let error = db
        .due_retries(ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in task_instances.task_id: "),
        "{error}"
    );
}

#[tokio::test]
async fn a_task_cancel_is_recorded_and_an_unschedulable_task_retried() {
    let (db, run_id) = with_run(RunState::Running).await;
    let now = ts("2026-10-21T08:00:01Z");
    db.create_instances(
        &run_id,
        &[
            (id("extract"), TaskInstanceState::Running),
            (id("load"), TaskInstanceState::Unschedulable),
        ],
        now,
    )
    .await
    .unwrap();
    db.request_task_cancel(&run_id, &id("extract"), now)
        .await
        .unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    let cancelling: Vec<(&str, bool)> = run
        .instances
        .iter()
        .map(|i| (i.task_id.as_str(), i.cancel_requested))
        .collect();
    assert_eq!(cancelling, [("extract", true), ("load", false)]);

    assert!(
        !db.retry_unschedulable(&run_id, &id("extract"), now)
            .await
            .unwrap()
    );
    assert!(
        db.retry_unschedulable(&run_id, &id("load"), now)
            .await
            .unwrap()
    );
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(
        (run.instances[1].state, run.instances[1].ready_at),
        (TaskInstanceState::Queued, Some(now))
    );
    assert!(
        !db.retry_unschedulable(&run_id, &id("load"), now)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_wide_runs_instances_are_created_in_a_few_statements_all_of_them() {
    // B18: one statement a task made a 400-task run's start cost a round trip per task; now a
    // few statements of up to INSTANCES_PER_STATEMENT rows, which must store every one.
    let (db, run_id) = with_run(RunState::Running).await;
    let count = INSTANCES_PER_STATEMENT * 2 + 201;
    let states: Vec<(TaskId, TaskInstanceState)> = (0..count)
        .map(|i| {
            let state = if i % 2 == 0 {
                TaskInstanceState::Queued
            } else {
                TaskInstanceState::Scheduled
            };
            (id(&format!("t{i:04}")), state)
        })
        .collect();
    db.create_instances(&run_id, &states, ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(run.instances.len(), count);
    for (instance, (task_id, state)) in run.instances.iter().zip(&states) {
        assert_eq!((&instance.task_id, instance.state), (task_id, *state));
        assert_eq!(
            instance.ready_at.is_some(),
            *state == TaskInstanceState::Queued
        );
    }
}

#[tokio::test]
async fn many_updates_in_a_few_statements_change_each_task_as_one_by_one() {
    // B18: one statement a task; now up to UPDATES_PER_STATEMENT a statement, which must
    // change each task as a statement of its own did (state, ready time, outcome, retries).
    let (db, run_id) = with_run(RunState::Running).await;
    let count = UPDATES_PER_STATEMENT * 2 + 201;
    let at = ts("2026-10-21T08:00:00Z");
    let tasks: Vec<(TaskId, TaskInstanceState)> = (0..count)
        .map(|i| (id(&format!("t{i:04}")), TaskInstanceState::Scheduled))
        .collect();
    db.create_instances(&run_id, &tasks, at).await.unwrap();
    let updates: Vec<InstanceUpdate> = (0..count)
        .map(|i| {
            let queued = i % 3 == 0;
            InstanceUpdate {
                task_id: id(&format!("t{i:04}")),
                state: if queued {
                    TaskInstanceState::Queued
                } else {
                    TaskInstanceState::Success
                },
                ready_at: queued.then_some(at),
                outcome: (!queued).then(|| "SUCCESS".to_owned()),
                used_retry: i % 2 == 0,
            }
        })
        .collect();
    db.update_instances(&run_id, &updates, at).await.unwrap();
    db.update_instances(&run_id, &updates, at).await.unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(run.instances.len(), count);
    for (instance, update) in run.instances.iter().zip(&updates) {
        assert_eq!(instance.task_id, update.task_id);
        assert_eq!(instance.state, update.state);
        assert_eq!(instance.ready_at, update.ready_at);
        // Counted twice: the second update added to the first.
        assert_eq!(instance.retries_used, if update.used_retry { 2 } else { 0 });
    }
}

#[tokio::test]
async fn waiting_reasons_are_set_together_on_queued_tasks_only() {
    let (db, run_id) = with_run(RunState::Running).await;
    let at = ts("2026-10-21T08:00:00Z");
    let tasks: Vec<(TaskId, TaskInstanceState)> = (0..5)
        .map(|i| {
            let state = if i == 4 {
                TaskInstanceState::Scheduled
            } else {
                TaskInstanceState::Queued
            };
            (id(&format!("t{i}")), state)
        })
        .collect();
    db.create_instances(&run_id, &tasks, at).await.unwrap();
    let ids: Vec<String> = db
        .run_tasks(&run_id)
        .await
        .unwrap()
        .unwrap()
        .instances
        .iter()
        .map(|i| i.id.clone())
        .collect();
    let slot = "waiting for a free slot".to_owned();
    let queue = "no active worker serves queue `gpu`".to_owned();
    let reasons = vec![
        (ids[0].clone(), slot.clone()),
        (ids[1].clone(), slot.clone()),
        (ids[2].clone(), queue.clone()),
        // Not queued: left as it is.
        (ids[4].clone(), slot.clone()),
    ];
    db.set_queued_reasons(&reasons, at).await.unwrap();
    db.set_queued_reasons(&[], at).await.unwrap();
    let schedr_db_pool = match db.pool() {
        crate::database::Pool::Sqlite(pool) => pool.clone(),
        crate::database::Pool::Postgres(_) => unreachable!("SQLite here"),
    };
    let shown: Vec<Option<String>> =
        sqlx::query_scalar("SELECT queued_reason FROM task_instances ORDER BY task_id")
            .fetch_all(&schedr_db_pool)
            .await
            .unwrap();
    assert_eq!(
        shown,
        [Some(slot.clone()), Some(slot), Some(queue), None, None]
    );
}

#[tokio::test]
async fn a_task_updated_twice_at_once_ends_as_its_last_update_with_both_retries() {
    // Code review: one statement for many rows applied one of a task's two updates, any;
    // a statement each applied both in turn.
    let (db, run_id) = with_run(RunState::Running).await;
    let at = ts("2026-10-21T08:00:00Z");
    db.create_instances(&run_id, &[(id("t0"), TaskInstanceState::Scheduled)], at)
        .await
        .unwrap();
    let queued = InstanceUpdate {
        task_id: id("t0"),
        state: TaskInstanceState::Queued,
        ready_at: Some(at),
        outcome: None,
        used_retry: true,
    };
    let done = InstanceUpdate {
        state: TaskInstanceState::Success,
        ready_at: None,
        outcome: Some("SUCCESS".to_owned()),
        ..queued.clone()
    };
    db.update_instances(&run_id, &[queued, done], at)
        .await
        .unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    let t0 = &run.instances[0];
    assert_eq!(
        (t0.state, t0.ready_at, t0.retries_used),
        (TaskInstanceState::Success, None, 2)
    );
}
