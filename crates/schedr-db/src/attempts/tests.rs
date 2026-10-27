use pretty_assertions::assert_eq;
use schedr_model::{DagId, RunState};
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::instances::InstanceUpdate;
use crate::runs::NewRun;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

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
        "edges": [{"from": "extract", "to": "load", "kind": "data"}]
    }))
    .unwrap()
}

/// A database with a running run of `sync`: `extract` queued, `load` scheduled.
async fn with_run() -> (Database, String) {
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
        dag: Box::new(dag()),
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
            state: RunState::Running,
            planned_for: ts(NOW),
            since: ts("2026-10-20T08:00:00Z"),
            triggered_at: ts(NOW),
            triggered_by: "anna",
        })
        .await
        .unwrap()
        .unwrap();
    db.create_instances(
        &run_id,
        &[
            (id("extract"), TaskInstanceState::Queued),
            (id("load"), TaskInstanceState::Scheduled),
        ],
        ts(NOW),
    )
    .await
    .unwrap();
    (db, run_id)
}

async fn instance_state(db: &Database, run_id: &str, task: &str) -> (TaskInstanceState, u32) {
    let run = db.run_tasks(run_id).await.unwrap().unwrap();
    let instance = run
        .instances
        .iter()
        .find(|i| i.task_id == id(task))
        .unwrap();
    (instance.state, instance.try_number)
}

#[tokio::test]
async fn ready_tasks_share_their_dags_one_definition() {
    // B13: each ready task brought its run's whole DAG definition and parsed it; a wide DAG
    // made every round read it once per ready task.
    let (db, run_id) = with_run().await;
    db.update_instances(
        &run_id,
        &[crate::instances::InstanceUpdate {
            task_id: id("load"),
            state: TaskInstanceState::Queued,
            ready_at: Some(ts(NOW)),
            outcome: None,
            used_retry: false,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(ready.len(), 2);
    assert!(std::sync::Arc::ptr_eq(&ready[0].dag, &ready[1].dag));
    // And the next round's too: parsed once per version.
    let again = db.ready_instances(ts(NOW)).await.unwrap();
    assert!(std::sync::Arc::ptr_eq(&ready[0].dag, &again[0].dag));
}

#[tokio::test]
async fn ready_instances_are_the_queued_tasks_of_running_runs() {
    let (db, run_id) = with_run().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(ready.len(), 1);
    let extract = &ready[0];
    assert_eq!(
        (extract.run_id.as_str(), extract.task_id.as_str()),
        (run_id.as_str(), "extract")
    );
    assert_eq!((extract.try_number, extract.run_kind), (0, RunKind::Manual));
    assert_eq!(
        (extract.planned_for, extract.since),
        (ts(NOW), ts("2026-10-20T08:00:00Z"))
    );
    assert_eq!(*extract.dag, dag());
    assert_eq!(extract.content_hash, "h1");
    assert_eq!(extract.queued_reason, None);
    assert_eq!(extract.parse_version, "0.1.0");
    db.set_queued_reasons(
        &[(
            extract.instance_id.clone(),
            "no worker serves queue `large`".to_owned(),
        )],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        db.ready_instances(ts(NOW)).await.unwrap()[0]
            .queued_reason
            .as_deref(),
        Some("no worker serves queue `large`")
    );
    assert_eq!(
        db.ready_instances(ts("2026-10-21T07:59:59Z"))
            .await
            .unwrap(),
        []
    );
    let extract_id = extract.task_id.clone();
    db.request_task_cancel(&run_id, &extract_id, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.ready_instances(ts(NOW)).await.unwrap(),
        [],
        "a task being cancelled isn't ready"
    );
    db.request_cancel(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(db.ready_instances(ts(NOW)).await.unwrap(), []);
}

#[tokio::test]
async fn an_instance_is_assigned_once_with_the_next_try_number() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, try_number) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(try_number, 1);
    assert_eq!(
        db.assign_attempt(&instance_id, "w2", ts(NOW))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        instance_state(&db, &run_id, "extract").await,
        (TaskInstanceState::Running, 1)
    );
    let attempt = db.attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(
        attempt,
        StoredAttempt {
            id: attempt_id.clone(),
            instance_id,
            run_id,
            task_id: id("extract"),
            try_number: 1,
            worker_id: "w1".to_owned(),
            status: None,
            started: false,
        }
    );
    assert_eq!(
        db.attempt("0190a3e5-0000-7000-8000-000000000000")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_rejected_assignment_is_taken_back_without_a_try() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let later = ts("2026-10-21T08:00:35Z");
    db.undo_assignment(
        &attempt_id,
        TaskInstanceState::Queued,
        Some(later),
        Some("waiting"),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        instance_state(&db, &run_id, "extract").await,
        (TaskInstanceState::Queued, 0)
    );
    assert_eq!(db.attempt(&attempt_id).await.unwrap(), None);
    assert_eq!(db.ready_instances(ts(NOW)).await.unwrap(), []);
    assert_eq!(db.ready_instances(later).await.unwrap().len(), 1);
}

#[tokio::test]
async fn unschedulable_instances_are_queued_again_when_asked() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    db.undo_assignment(
        &attempt_id,
        TaskInstanceState::Unschedulable,
        None,
        Some("no"),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        instance_state(&db, &run_id, "extract").await.0,
        TaskInstanceState::Unschedulable
    );
    assert_eq!(db.requeue_unschedulable(ts(NOW)).await.unwrap(), 1);
    assert_eq!(
        instance_state(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
}

#[tokio::test]
async fn attempts_start_and_end_once() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.open_attempts(Some("w1")).await.unwrap().len(), 1);
    assert_eq!(db.open_attempts(Some("w2")).await.unwrap(), []);
    assert_eq!(db.open_attempts(None).await.unwrap().len(), 1);
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T08:00:10Z"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(db.unstarted_attempts(ts(NOW)).await.unwrap(), []);

    assert!(
        db.mark_started(&attempt_id, "spawn", ts(NOW))
            .await
            .unwrap()
    );
    assert!(db.attempt(&attempt_id).await.unwrap().unwrap().started);
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T09:00:00Z"))
            .await
            .unwrap(),
        []
    );

    let end = AttemptEnd {
        exit_code: Some(1),
        error_kind: Some("input_mismatch".to_owned()),
        error_summary: Some("{}".to_owned()),
        code_hash: Some("sha256:x".to_owned()),
        peak_memory_bytes: Some(1024),
        signal: None,
        usage: Some(schedr_model::runner::ProcessUsage {
            user_cpu_micros: 1_500_000,
            system_cpu_micros: 250_000,
            peak_rss_bytes: None,
            major_faults: 3,
            voluntary_context_switches: Some(40),
            involuntary_context_switches: Some(7),
        }),
        core_dumped: true,
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    assert!(db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap());
    let stored = &db.task_attempts(&run_id, &id("extract")).await.unwrap()[0];
    assert_eq!((stored.usage, stored.core_dumped), (end.usage, true));
    assert!(
        !db.end_attempt(
            &attempt_id,
            &AttemptEnd::by_master(AttemptStatus::Lost),
            ts(NOW)
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.attempt(&attempt_id).await.unwrap().unwrap().status,
        Some(AttemptStatus::Failed)
    );
    assert!(
        !db.mark_started(&attempt_id, "spawn", ts(NOW))
            .await
            .unwrap()
    );
    assert_eq!(db.open_attempts(None).await.unwrap(), []);
}

#[tokio::test]
async fn run_outputs_are_the_stored_outputs_by_task() {
    let (db, run_id) = with_run().await;
    assert_eq!(db.run_outputs(&run_id).await.unwrap(), BTreeMap::new());
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    db.store_output(&instance_id, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    let done = InstanceUpdate {
        task_id: id("extract"),
        state: TaskInstanceState::Success,
        ready_at: None,
        outcome: Some("SUCCESS".to_owned()),
        used_retry: false,
    };
    db.update_instances(&run_id, &[done], ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.run_outputs(&run_id).await.unwrap(),
        BTreeMap::from([(id("extract"), json!({"rows": 3}))])
    );
    // Only the tasks asked for: an output not stored yet is simply not there.
    assert_eq!(
        db.outputs_of(&run_id, &[&id("extract"), &id("load")])
            .await
            .unwrap(),
        BTreeMap::from([(id("extract"), json!({"rows": 3}))])
    );
    assert_eq!(
        db.outputs_of(&run_id, &[&id("load")]).await.unwrap(),
        BTreeMap::new()
    );
    assert_eq!(db.outputs_of(&run_id, &[]).await.unwrap(), BTreeMap::new());
}

#[tokio::test]
async fn stored_values_that_dont_read_are_errors() {
    let (db, _run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE attempts SET status = 'sometimes'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.attempt(&attempt_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in attempts.status: "),
        "{error}"
    );
    sqlx::query(
        "UPDATE task_instances SET state = 'queued', ready_at = '2026-01-01T00:00:00.000000Z', \
         task_id = 'bad id!' WHERE task_id = 'extract'",
    )
    .execute(pool)
    .await
    .unwrap();
    let error = db.ready_instances(ts(NOW)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in task_instances.task_id: "),
        "{error}"
    );
}
