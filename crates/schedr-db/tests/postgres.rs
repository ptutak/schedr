//! Every query on Postgres (the unit tests run them on SQLite): the same text is typed per
//! backend, so each one must work on both.
//!
//! Needs `SCHEDR_TEST_POSTGRES_URL`, an administrative connection to a Postgres server (`make
//! test-integration` starts one in Docker); each test gets a database of its own.

use std::collections::BTreeMap;

use jiff::Timestamp;
use pretty_assertions::assert_eq;
use schedr_db::attempts::AttemptEnd;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::instances::InstanceUpdate;
use schedr_db::runs::NewRun;
use schedr_db::{Backend, Database, SchemaStatus};
use schedr_model::commands::{Command, CommandStatus};
use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::{
    AttemptStatus, Dag, DagId, DagLocation, RunKind, RunState, TaskId, TaskInstanceState,
};
use serde_json::json;
use sqlx::AssertSqlSafe;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

const NOW: &str = "2026-10-21T08:00:00.250Z";

/// A migrated database of this test's own on the test server.
async fn fresh() -> Database {
    let admin = std::env::var("SCHEDR_TEST_POSTGRES_URL").expect(
        "SCHEDR_TEST_POSTGRES_URL: an administrative Postgres URL (`make test-integration` starts \
         a server and sets it)",
    );
    let name = format!("t_{}", uuid::Uuid::now_v7().simple());
    let pool = sqlx::PgPool::connect(&admin).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {name}")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let (server, _) = admin.rsplit_once('/').unwrap();
    let db = Database::connect(&format!("{server}/{name}"))
        .await
        .unwrap();
    assert_eq!(db.backend(), Backend::Postgres);
    db.migrate().await.unwrap();
    db
}

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

fn dag() -> Dag {
    let task = |name: &str, arguments: serde_json::Value| {
        json!({
            "id": name, "kind": "python", "arguments": arguments,
            "settings": {
                "retries": 1, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "any"}
        })
    };
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw", "catchup": false},
        "initial_since": null,
        "max_active_runs": 1,
        "tasks": [
            task("extract", json!({})),
            task("load", json!({"rows": {"ref": {"kind": "output", "task": "extract"}}}))
        ],
        "edges": [{"from": "extract", "to": "load", "kind": "data"}]
    }))
    .unwrap()
}

fn files() -> Vec<ParsedFile> {
    vec![
        ParsedFile {
            path: "sync.py".to_owned(),
            dags: vec![dag()],
            error: None,
        },
        ParsedFile {
            path: "bad.py".to_owned(),
            dags: vec![],
            error: Some(ErrorSummary {
                exception: "ImportError".to_owned(),
                message: "no module".to_owned(),
                traceback: "...".to_owned(),
            }),
        },
    ]
}

/// A database with DAG `sync` defined from a parse; returns it with the version's ID.
async fn with_dag() -> (Database, String) {
    let db = fresh().await;
    let files = files();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "sha256:h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &files,
        })
        .await
        .unwrap();
    db.apply_dag_changes(
        &[DagChange::Define {
            dag: Box::new(dag()),
            parse_id,
            pause: false,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version)
}

fn run<'a>(version: &'a str, kind: RunKind, state: RunState, planned_for: &str) -> NewRun<'a> {
    static SYNC: std::sync::OnceLock<DagId> = std::sync::OnceLock::new();
    NewRun {
        dag_id: SYNC.get_or_init(sync),
        dag_version_id: version,
        kind,
        state,
        planned_for: ts(planned_for),
        since: ts("2026-10-20T01:00:00Z"),
        triggered_at: ts(planned_for),
        triggered_by: "anna",
    }
}

#[tokio::test]
async fn the_schema_migrates_and_reports_itself() {
    let db = fresh().await;
    assert_eq!(db.schema_status().await.unwrap(), SchemaStatus::UpToDate);
    db.require_usable_schema().await.unwrap();
    assert_eq!(
        db.applied_migrations().await.unwrap(),
        Backend::Postgres.migration_versions()
    );
}

#[tokio::test]
async fn parses_and_the_dag_list_are_stored() {
    let (db, _) = with_dag().await;
    let parse_id = db
        .find_parse("etl", "sha256:h1", "0.1.0")
        .await
        .unwrap()
        .unwrap();
    let again = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "sha256:h1",
            schedr_version: "0.1.0",
            worker_id: "w2",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    assert_eq!(again, parse_id);
    let mut expected = files();
    expected.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(db.parse_files(&parse_id).await.unwrap(), expected);

    let stored = db.stored_dags().await.unwrap();
    assert!(stored[0].paused);
    assert_eq!(stored[0].current.as_ref().unwrap().dag, dag());
    assert!(db.set_paused(&sync(), false).await.unwrap());
    let places = vec![
        DagLocation {
            root: "etl".to_owned(),
            path: "a.py".to_owned(),
        },
        DagLocation {
            root: "etl".to_owned(),
            path: "b.py".to_owned(),
        },
    ];
    let dup = DagId::new("dup").unwrap();
    db.apply_dag_changes(
        &[
            DagChange::Conflict {
                dag_id: dup.clone(),
                places: places.clone(),
                pause: true,
            },
            DagChange::Remove { dag_id: sync() },
        ],
        ts(NOW),
    )
    .await
    .unwrap();
    let stored = db.stored_dags().await.unwrap();
    assert_eq!(
        (stored[0].dag_id.as_str(), stored[0].conflict.clone()),
        ("dup", Some(places))
    );
    assert!(stored[1].removed);
    assert!(!stored[1].paused);
    assert_eq!(db.active_dags().await.unwrap(), []);
}

#[tokio::test]
async fn runs_are_created_counted_queued_and_read() {
    let (db, version) = with_dag().await;
    let first = db
        .insert_run(&run(
            &version,
            RunKind::Scheduled,
            RunState::Running,
            "2026-10-21T01:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.insert_run(&run(
            &version,
            RunKind::Scheduled,
            RunState::Running,
            "2026-10-21T01:00:00Z"
        ))
        .await
        .unwrap(),
        None
    );
    let queued = db
        .insert_run(&run(
            &version,
            RunKind::Event,
            RunState::Queued,
            "2026-10-21T07:00:00.5Z",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.last_scheduled_for(&sync()).await.unwrap(),
        Some(ts("2026-10-21T01:00:00Z"))
    );
    assert_eq!(
        db.nearest_earlier_run(&sync(), ts("2026-10-21T08:00:00Z"))
            .await
            .unwrap(),
        Some(ts("2026-10-21T07:00:00.5Z"))
    );
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);
    let waiting = db.queued_runs(&sync()).await.unwrap();
    assert_eq!(
        (waiting[0].id.clone(), waiting[0].kind),
        (queued.clone(), RunKind::Event)
    );
    assert_eq!(
        db.runs_in_progress(&sync()).await.unwrap(),
        [first.clone(), queued.clone()]
    );
    db.start_run(&queued, ts(NOW)).await.unwrap();
    assert_eq!(db.running_count(&sync()).await.unwrap(), 2);

    let listed = db.dag_runs(&sync(), None, 10).await.unwrap();
    assert_eq!(listed[0].id, queued);
    assert_eq!(listed[0].started_at, Some(ts(NOW)));
    let on_day = db
        .dag_runs(&sync(), Some(ts("2026-10-21T01:00:00Z")), 10)
        .await
        .unwrap();
    assert_eq!(on_day.len(), 1);
    assert_eq!(
        db.run_record(&first).await.unwrap().unwrap().kind,
        RunKind::Scheduled
    );
}

/// A running run of `sync` with its instances: `extract` queued, `load` scheduled.
async fn with_instances() -> (Database, String) {
    let (db, version) = with_dag().await;
    let run_id = db
        .insert_run(&run(
            &version,
            RunKind::Manual,
            RunState::Running,
            "2026-10-21T07:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.newly_running().await.unwrap()[0].run_id, run_id);
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

#[tokio::test]
async fn attempts_and_outputs_move_a_task_forward() {
    let (db, run_id) = with_instances().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(
        (ready.len(), ready[0].content_hash.as_str()),
        (1, "sha256:h1")
    );
    let (attempt_id, try_number) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(try_number, 1);
    assert_eq!(db.open_attempts(Some("w1")).await.unwrap().len(), 1);
    assert_eq!(db.open_attempts(None).await.unwrap().len(), 1);
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T09:00:00Z"))
            .await
            .unwrap()
            .len(),
        1
    );
    db.undo_assignment(
        &attempt_id,
        TaskInstanceState::Queued,
        Some(ts(NOW)),
        Some("again"),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(db.attempt(&attempt_id).await.unwrap(), None);

    let (attempt_id, try_number) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(try_number, 1);
    assert!(
        db.mark_started(&attempt_id, "spawn", ts(NOW))
            .await
            .unwrap()
    );
    let end = AttemptEnd {
        exit_code: Some(0),
        peak_memory_bytes: Some(5_000_000_000),
        duration_ms: Some(3_000_000_000),
        code_hash: Some("sha256:h1".to_owned()),
        ..AttemptEnd::by_master(AttemptStatus::Success)
    };
    assert!(db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap());
    let attempts = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    assert_eq!(attempts[0].duration_ms, Some(3_000_000_000));
    db.store_output(&ready[0].instance_id, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    db.update_instances(
        &run_id,
        &[InstanceUpdate {
            task_id: id("extract"),
            state: TaskInstanceState::Success,
            ready_at: None,
            outcome: Some("SUCCESS".to_owned()),
            used_retry: true,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        db.output(&ready[0].instance_id).await.unwrap(),
        Some(("SUCCESS".to_owned(), json!({"rows": 3})))
    );
    assert_eq!(
        db.run_outputs(&run_id).await.unwrap(),
        BTreeMap::from([(id("extract"), json!({"rows": 3}))])
    );
    let tasks = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(
        (
            tasks.instances[0].retries_used,
            tasks.instances[0].try_number
        ),
        (1, 1)
    );
    let attempts = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    assert_eq!(attempts[0].status, Some(AttemptStatus::Success));
    assert_eq!(
        db.run_instances(&run_id).await.unwrap()[0]
            .outcome
            .as_deref(),
        Some("SUCCESS")
    );
    assert_eq!(attempts[0].error_summary, None);
}

#[tokio::test]
async fn failures_retries_requeues_and_cancels_are_stored() {
    let (db, run_id) = with_instances().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let (attempt_id, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let failed = AttemptEnd {
        exit_code: Some(1),
        error_kind: Some("input_mismatch".to_owned()),
        error_summary: Some(r#"{"exception": "ValueError"}"#.to_owned()),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    assert!(db.end_attempt(&attempt_id, &failed, ts(NOW)).await.unwrap());
    let attempts = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    let summary: serde_json::Value =
        serde_json::from_str(attempts[0].error_summary.as_deref().unwrap()).unwrap();
    assert_eq!(summary, json!({"exception": "ValueError"}));

    db.update_instances(
        &run_id,
        &[InstanceUpdate {
            task_id: id("load"),
            state: TaskInstanceState::UpForRetry,
            ready_at: Some(ts(NOW)),
            outcome: None,
            used_retry: true,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        db.due_retries(ts(NOW)).await.unwrap(),
        [(run_id.clone(), id("load"))]
    );
    db.update_instances(
        &run_id,
        &[InstanceUpdate {
            task_id: id("load"),
            state: TaskInstanceState::Unschedulable,
            ready_at: None,
            outcome: None,
            used_retry: false,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(db.requeue_unschedulable(ts(NOW)).await.unwrap(), 1);
    db.request_cancel(&run_id, ts(NOW)).await.unwrap();
    db.finish_run(&run_id, RunState::Cancelled, ts(NOW))
        .await
        .unwrap();
    let finished = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert!(finished.cancel_requested);
    assert_eq!(finished.state, RunState::Cancelled);
}

#[tokio::test]
async fn commands_are_written_decided_and_read_back() {
    let db = fresh().await;
    let id = db
        .insert_command(
            &Command::TriggerRun {
                dag_id: sync(),
                planned_for: Some(ts("2026-10-15T01:00:00Z")),
            },
            "anna",
            ts(NOW),
        )
        .await
        .unwrap();
    let pending = db.pending_commands().await.unwrap();
    assert_eq!(pending[0].id, id);
    assert!(pending[0].command.is_ok());
    let result = json!({"run_id": "r1"});
    assert!(
        db.decide_command(&id, CommandStatus::Applied, None, Some(&result), ts(NOW))
            .await
            .unwrap()
    );
    let record = db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        (record.status, record.result),
        (CommandStatus::Applied, Some(result))
    );
    assert_eq!(record.requested_at, ts(NOW));
}

#[tokio::test]
async fn a_new_command_notifies_the_listener() {
    let db = fresh().await;
    let mut notifications = db
        .command_notifications()
        .await
        .unwrap()
        .expect("Postgres has notifications");
    db.insert_command(
        &Command::PauseDag {
            dag_id: sync(),
            cancel_runs: false,
        },
        "anna",
        ts(NOW),
    )
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), notifications.next())
        .await
        .expect("a notification within 10 s")
        .unwrap();
}

#[tokio::test]
async fn a_burst_of_commands_is_taken_with_its_first_notification() {
    let db = fresh().await;
    let mut notifications = db.command_notifications().await.unwrap().unwrap();
    for _ in 0..3 {
        db.insert_command(
            &Command::PauseDag {
                dag_id: sync(),
                cancel_runs: false,
            },
            "anna",
            ts(NOW),
        )
        .await
        .unwrap();
    }
    notifications.next().await.unwrap();
    // Give the other two notifications time to arrive, then take them without waiting.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        notifications.skip_arrived(),
    )
    .await
    .expect("taking arrived notifications doesn't wait");
    let nothing_left =
        tokio::time::timeout(std::time::Duration::from_millis(300), notifications.next()).await;
    assert!(nothing_left.is_err(), "no notification should be left");
}
