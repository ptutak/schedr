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
use sqlx::{AssertSqlSafe, Connection as _};

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

const NOW: &str = "2026-10-21T08:00:00.250Z";

/// A migrated database of this test's own on the test server.
async fn fresh() -> Database {
    let db = Database::connect(&fresh_url().await).await.unwrap();
    assert_eq!(db.backend(), Backend::Postgres);
    db.migrate().await.unwrap();
    db
}

/// A new, empty database of this test's own on the test server: its URL.
async fn fresh_url() -> String {
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
    format!("{server}/{name}")
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
    assert!(db.schema_status().await.unwrap().is_usable());
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
    let overview = db.run_overview().await.unwrap();
    assert_eq!(overview.running(&sync()), 1);
    assert_eq!(
        overview.last_scheduled(&sync()),
        Some(ts("2026-10-21T01:00:00Z"))
    );
    assert_eq!(overview.queued(&sync()), waiting.as_slice());
    // The same for one DAG named, its ID bound as a parameter.
    let other = DagId::new("other").unwrap();
    assert_eq!(
        db.run_overview_of(&[&sync(), &other]).await.unwrap(),
        overview
    );
    assert_eq!(db.active_dags_of(&[&sync()]).await.unwrap().len(), 1);
    assert_eq!(db.active_dags_of(&[&other]).await.unwrap(), []);
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
    assert_eq!(
        db.outputs_of(&run_id, &[&id("extract"), &id("load")])
            .await
            .unwrap(),
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

#[tokio::test]
async fn a_query_past_the_query_timeout_is_cut_off_by_schedr() {
    // No `SET statement_timeout` on the connection (it doesn't hold behind a pooler in
    // transaction mode): schedr itself cuts a query off at the limit.
    let url = fresh_url().await;
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let limited = Database::connect_with_timeout(&url, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(limited.query_timeout(), std::time::Duration::from_secs(1));
    // Another session holds the table: the query waits, and is cut off.
    let schedr_db::database::Pool::Postgres(pool) = db.pool() else {
        unreachable!()
    };
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE commands IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *holder)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let error = limited.pending_commands().await.unwrap_err();
    assert!(
        matches!(error, schedr_db::DbError::QueryTimeout(_)),
        "{error}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    holder.rollback().await.unwrap();
}

#[tokio::test]
async fn states_are_counted_on_postgres() {
    let db = fresh().await;
    assert_eq!(
        db.state_counts().await.unwrap(),
        schedr_db::reads::StateCounts::default()
    );
}

#[tokio::test]
async fn the_lease_has_one_holder_by_the_databases_clock_on_postgres() {
    let db = fresh().await;
    db.create_lease_table().await.unwrap();
    let period = std::time::Duration::from_millis(500);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    assert_eq!(db.take_lease("b", period).await.unwrap(), None);
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    db.release_lease("b").await.unwrap();
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(3));
    // The lease holder migrates without the migrator's session lock.
    db.migrate_as_lease_holder().await.unwrap();
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(3));
}

#[tokio::test]
async fn a_lease_statement_past_the_query_timeout_is_cut_off_too() {
    // A renewal stuck on the database must not keep the master believing it leads.
    let url = fresh_url().await;
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let limited = Database::connect_with_timeout(&url, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    let mut holder = admin_pool(&db).begin().await.unwrap();
    sqlx::query("LOCK TABLE master_lease IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *holder)
        .await
        .unwrap();
    let period = std::time::Duration::from_secs(30);
    let taken = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        limited.take_lease("a", period),
    )
    .await
    .expect("the lease statement is cut off at the query limit");
    assert!(
        matches!(taken, Err(schedr_db::DbError::QueryTimeout(_))),
        "{taken:?}"
    );
    holder.rollback().await.unwrap();
}

#[tokio::test]
async fn a_write_past_the_query_timeout_is_stopped_by_postgres_too() {
    // Cut off by schedr only, the statement would go on waiting in Postgres, holding its
    // connection (and, once it runs, its locks).
    let url = fresh_url().await;
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let limited = Database::connect_with_timeout(&url, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    let mut holder = admin_pool(&db).begin().await.unwrap();
    sqlx::query("LOCK TABLE workers IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *holder)
        .await
        .unwrap();
    let error = limited.worker_seen("w1", ts(NOW)).await.unwrap_err();
    assert!(
        matches!(
            error,
            schedr_db::DbError::QueryTimeout(_) | schedr_db::DbError::Sqlx(_)
        ),
        "{error}"
    );
    // The database's trouble (its statement limit, 57014), not the data's: no task is set
    // aside for it.
    assert!(!error.is_data_fault(), "{error}");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // Nothing of the cut-off write waits on the lock any more.
    let waiting: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_locks WHERE NOT granted AND relation = 'workers'::regclass",
    )
    .fetch_one(&mut *holder)
    .await
    .unwrap();
    assert_eq!(waiting, 0);
    holder.rollback().await.unwrap();
}

/// The pool of a Postgres database (for statements of the tests' own).
fn admin_pool(db: &Database) -> &sqlx::PgPool {
    let schedr_db::database::Pool::Postgres(pool) = db.pool() else {
        unreachable!()
    };
    pool
}

#[tokio::test]
async fn a_fenced_write_is_refused_once_taken_over_and_its_connection_stays_usable() {
    let db = fresh().await;
    let period = std::time::Duration::from_millis(300);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    let fenced = db.fenced("a", 1);
    fenced.worker_seen("w1", ts(NOW)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    // More refusals than the pool has connections: each one's connection goes back to it.
    for _ in 0..20 {
        let refused = fenced.worker_seen("w1", ts(NOW)).await;
        assert!(
            matches!(refused, Err(schedr_db::DbError::NotLeading)),
            "{refused:?}"
        );
    }
    let leading = db.fenced("b", 2);
    for _ in 0..20 {
        leading.worker_seen("w1", ts(NOW)).await.unwrap();
    }
}

#[tokio::test]
async fn a_holder_named_with_quotes_and_dollars_is_fenced_as_written() {
    // The fence's values are written into the transaction's start as literals.
    let db = fresh().await;
    let holder = r"it's \ $fence$ '; DROP TABLE workers; --";
    let period = std::time::Duration::from_secs(30);
    assert_eq!(db.take_lease(holder, period).await.unwrap(), Some(1));
    db.fenced(holder, 1)
        .worker_seen("w1", ts(NOW))
        .await
        .unwrap();
    let other = db.fenced("it's", 1).worker_seen("w1", ts(NOW)).await;
    assert!(
        matches!(other, Err(schedr_db::DbError::NotLeading)),
        "{other:?}"
    );
}

#[tokio::test]
async fn several_writes_in_one_transaction_commit_together_or_roll_back_on_postgres() {
    let db = fresh().await;
    let trigger = Command::TriggerRun {
        dag_id: sync(),
        planned_for: None,
    };
    Box::pin(db.atomically(async {
        db.insert_command(&trigger, "anna", ts(NOW)).await?;
        db.insert_command(&trigger, "anna", ts(NOW)).await?;
        // The work reads its own writes.
        assert_eq!(db.pending_commands().await?.len(), 2);
        Ok::<_, schedr_db::DbError>(())
    }))
    .await
    .unwrap();
    let failed = Box::pin(db.atomically(async {
        db.insert_command(&trigger, "anna", ts(NOW)).await?;
        Err::<(), _>(schedr_db::DbError::NotLeading)
    }))
    .await;
    assert!(failed.is_err());
    assert_eq!(db.pending_commands().await.unwrap().len(), 2);
    // A master whose lease was taken over: refused at the start, nothing stored.
    let period = std::time::Duration::from_millis(300);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    let master = db.fenced("a", 1);
    let refused = Box::pin(master.atomically(async {
        master.insert_command(&trigger, "anna", ts(NOW)).await?;
        Ok::<_, schedr_db::DbError>(())
    }))
    .await;
    assert!(
        matches!(refused, Err(schedr_db::DbError::NotLeading)),
        "{refused:?}"
    );
    // Its connections are all still usable.
    for _ in 0..20 {
        db.insert_command(&trigger, "anna", ts(NOW)).await.unwrap();
    }
}

#[tokio::test]
async fn a_master_stalled_in_a_write_doesnt_block_the_takeover() {
    // B10: a master whose connection stalls between a write's start (the fence, holding the
    // lease's row) and its commit left that transaction open: the standby's takeover waited on
    // the row for as long as the stall lasted (no master at all meanwhile).
    let url = fresh_url().await;
    let limit = std::time::Duration::from_secs(1);
    let master = Database::connect_with_timeout(&url, limit).await.unwrap();
    master.migrate().await.unwrap();
    let standby = Database::connect_with_timeout(&url, limit).await.unwrap();
    let period = std::time::Duration::from_secs(3);
    assert_eq!(master.take_lease("a", period).await.unwrap(), Some(1));
    let fenced = master.fenced("a", 1);
    // A write begun, then the master stalls with its transaction open.
    let stalled = tokio::spawn(async move {
        Box::pin(fenced.atomically(async {
            fenced.worker_seen("w1", ts(NOW)).await?;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            Ok::<_, schedr_db::DbError>(())
        }))
        .await
    });
    tokio::time::sleep(period + std::time::Duration::from_millis(500)).await;
    // The lease expired: the standby takes it within a few tries.
    let started = std::time::Instant::now();
    let taken = loop {
        match standby.take_lease("b", period).await {
            Ok(Some(epoch)) => break Some(epoch),
            Ok(None) | Err(schedr_db::DbError::QueryTimeout(_)) => {}
            Err(error) => panic!("{error}"),
        }
        if started.elapsed() > std::time::Duration::from_secs(8) {
            break None;
        }
    };
    assert_eq!(
        taken,
        Some(2),
        "the stalled master's transaction blocked the takeover"
    );
    stalled.abort();
}

#[tokio::test]
async fn straight_to_postgres_the_master_listens_unless_told_not_to() {
    let url = fresh_url().await;
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    // A direct connection keeps its server process: listening is safe.
    let mut y = sqlx::PgConnection::connect(&url).await.unwrap();
    let mut x = sqlx::PgConnection::connect(&url).await.unwrap();
    assert!(
        schedr_db::commands::listening_is_safe(&mut y, &mut x)
            .await
            .unwrap()
    );
    assert!(db.command_notifications().await.unwrap().is_some());
    // Through a separate URL (here the same server).
    let through = db.clone().with_notifications(true, Some(url.clone()));
    assert!(through.command_notifications().await.unwrap().is_some());
    let off = db.with_notifications(false, None);
    assert!(off.command_notifications().await.unwrap().is_none());
}

#[tokio::test]
async fn a_wide_runs_instances_are_all_created_on_postgres() {
    let (db, version) = with_dag().await;
    let run_id = db
        .insert_run(&run(
            &version,
            RunKind::Manual,
            RunState::Running,
            "2026-10-21T02:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let states: Vec<(TaskId, TaskInstanceState)> = (0..1201)
        .map(|i| (id(&format!("t{i:04}")), TaskInstanceState::Queued))
        .collect();
    db.create_instances(&run_id, &states, ts(NOW))
        .await
        .unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    assert_eq!(run.instances.len(), 1201);
    assert!(run.instances.iter().all(|i| i.ready_at == Some(ts(NOW))));
}

#[tokio::test]
async fn many_updates_change_each_task_on_postgres() {
    let (db, version) = with_dag().await;
    let run_id = db
        .insert_run(&run(
            &version,
            RunKind::Manual,
            RunState::Running,
            "2026-10-21T02:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let tasks: Vec<(TaskId, TaskInstanceState)> = (0..1201)
        .map(|i| (id(&format!("t{i:04}")), TaskInstanceState::Scheduled))
        .collect();
    db.create_instances(&run_id, &tasks, ts(NOW)).await.unwrap();
    let updates: Vec<InstanceUpdate> = (0..1201)
        .map(|i| InstanceUpdate {
            task_id: id(&format!("t{i:04}")),
            state: if i % 3 == 0 {
                TaskInstanceState::Queued
            } else {
                TaskInstanceState::Success
            },
            ready_at: (i % 3 == 0).then(|| ts(NOW)),
            outcome: (i % 3 != 0).then(|| "SUCCESS".to_owned()),
            used_retry: i % 2 == 0,
        })
        .collect();
    db.update_instances(&run_id, &updates, ts(NOW))
        .await
        .unwrap();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    for (instance, update) in run.instances.iter().zip(&updates) {
        assert_eq!(
            (&instance.task_id, instance.state, instance.ready_at),
            (&update.task_id, update.state, update.ready_at)
        );
        assert_eq!(instance.retries_used, u32::from(update.used_retry));
    }
}

#[tokio::test]
async fn waiting_reasons_are_set_together_on_postgres() {
    let (db, version) = with_dag().await;
    let run_id = db
        .insert_run(&run(
            &version,
            RunKind::Manual,
            RunState::Running,
            "2026-10-21T02:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let tasks: Vec<(TaskId, TaskInstanceState)> = (0..3)
        .map(|i| (id(&format!("t{i}")), TaskInstanceState::Queued))
        .collect();
    db.create_instances(&run_id, &tasks, ts(NOW)).await.unwrap();
    let instances = db.run_tasks(&run_id).await.unwrap().unwrap().instances;
    let reasons: Vec<(String, String)> = instances
        .iter()
        .map(|i| {
            (
                i.id.clone(),
                format!("reason {}", i.task_id.as_str().len() % 2),
            )
        })
        .collect();
    db.set_queued_reasons(&reasons, ts(NOW)).await.unwrap();
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(ready.len(), 3);
    assert!(ready.iter().all(|r| r.queued_reason.is_some()));
}
