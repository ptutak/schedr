//! Every query on Postgres (the unit tests run them on SQLite): the same text is typed per
//! backend, so each one must work on both.
//!
//! Needs `SCHEDR_TEST_POSTGRES_URL`, an administrative connection to a Postgres server (`make
//! test-integration` starts one in Docker); each test gets a database of its own.

use std::collections::BTreeMap;

use jiff::Timestamp;
use pretty_assertions::assert_eq;
use schedr_db::attempts::{AttemptEnd, CleanupEnd};
use schedr_db::commands::Submitted;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::instances::InstanceUpdate;
use schedr_db::reads::RunFilter;
use schedr_db::runs::NewRun;
use schedr_db::{Backend, Database, SchemaStatus};
use schedr_model::commands::{Command, CommandStatus};
use schedr_model::runner::{ErrorSummary, ParsedFile};
use schedr_model::{
    AttemptStatus, CleanupResult, Dag, DagId, DagLocation, RunKind, RunState, TaskId,
    TaskInstanceState,
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
            script_roots: Vec::new(),
            path: "sync.py".to_owned(),
            dags: vec![dag()],
            connections: Vec::new(),
            error: None,
            warnings: Vec::new(),
        },
        ParsedFile {
            script_roots: Vec::new(),
            path: "bad.py".to_owned(),
            dags: vec![],
            connections: Vec::new(),
            error: Some(ErrorSummary {
                exception: "ImportError".to_owned(),
                message: "no module".to_owned(),
                traceback: "...".to_owned(),
            }),
            warnings: Vec::new(),
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
        params: &schedr_db::runs::NO_PARAMS,
        event: None,
        dag_id: SYNC.get_or_init(sync),
        dag_version_id: version,
        kind,
        state,
        planned_for: ts(planned_for),
        since: ts("2026-10-20T01:00:00Z"),
        triggered_at: ts(planned_for),
        triggered_by: "anna",
        parent_run_id: None,
        backfill_id: None,
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
    // The current parse's import errors (decision 66).
    db.set_current_parse("etl", &parse_id, ts(NOW))
        .await
        .unwrap();
    db.set_current_parse("etl", &parse_id, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.current_parses().await.unwrap(),
        BTreeMap::from([("etl".to_owned(), parse_id.clone())])
    );
    let errors = db.current_import_errors().await.unwrap();
    let failed: Vec<&ParsedFile> = expected.iter().filter(|f| f.error.is_some()).collect();
    assert_eq!(
        errors.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
        failed.iter().map(|f| f.path.as_str()).collect::<Vec<_>>()
    );
    assert!(
        errors
            .iter()
            .all(|e| e.parsed_at == ts(NOW) && e.worker_id == "w1")
    );

    let stored = db.stored_dags().await.unwrap();
    assert!(stored[0].paused);
    assert_eq!(stored[0].current.as_ref().unwrap().dag, dag());
    // Read alone, the same (B136).
    let alone = db.stored_dag(&sync()).await.unwrap().unwrap();
    assert_eq!(alone.current.as_ref().unwrap().dag, dag());
    assert!(alone.paused);
    assert!(
        db.stored_dag(&DagId::new("nowhere").unwrap())
            .await
            .unwrap()
            .is_none()
    );
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
            (id("extract"), -1, TaskInstanceState::Queued),
            (id("load"), -1, TaskInstanceState::Scheduled),
        ],
        ts(NOW),
    )
    .await
    .unwrap();
    (db, run_id)
}

#[tokio::test]
async fn every_dags_runs_are_listed_filtered_and_paged() {
    let (db, run_id) = with_instances().await;
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    let older = db
        .insert_run(&run(
            &version,
            RunKind::Scheduled,
            RunState::Success,
            "2026-10-21T01:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let filter = |state, before: Option<&str>| RunFilter {
        dag_id: Some(sync()),
        state,
        before: before.map(ts),
        before_run: None,
        limit: 10,
    };
    let ids =
        |runs: Vec<schedr_db::reads::RunRecord>| runs.into_iter().map(|r| r.id).collect::<Vec<_>>();
    let all = db
        .runs(&RunFilter {
            limit: 10,
            ..RunFilter::default()
        })
        .await;
    assert_eq!(ids(all.unwrap()), [run_id.as_str(), older.as_str()]);
    let done = db.runs(&filter(Some(RunState::Success), None)).await;
    assert_eq!(ids(done.unwrap()), [older.as_str()]);
    let next = db.runs(&filter(None, Some("2026-10-21T07:00:00Z"))).await;
    assert_eq!(ids(next.unwrap()), [older.as_str()]);
    let other = RunFilter {
        dag_id: Some(DagId::new("other").unwrap()),
        limit: 10,
        ..RunFilter::default()
    };
    assert_eq!(db.runs(&other).await.unwrap(), []);
    // Runs triggered at the same moment page by ID, none skipped.
    let mut together = Vec::new();
    for planned_for in ["2026-10-19T01:00:00Z", "2026-10-20T01:00:00Z"] {
        let new = NewRun {
            triggered_at: ts("2026-10-21T00:00:00Z"),
            ..run(&version, RunKind::Scheduled, RunState::Queued, planned_for)
        };
        together.push(db.insert_run(&new).await.unwrap().unwrap());
    }
    let after_newer = RunFilter {
        before: Some(ts("2026-10-21T00:00:00Z")),
        before_run: Some(together[1].clone()),
        limit: 10,
        ..RunFilter::default()
    };
    assert_eq!(
        ids(db.runs(&after_newer).await.unwrap()),
        [together[0].as_str()]
    );
}

#[tokio::test]
async fn a_runs_graph_output_and_commands_are_read() {
    let (db, run_id) = with_instances().await;
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    let older = db
        .insert_run(&run(
            &version,
            RunKind::Scheduled,
            RunState::Success,
            "2026-10-21T01:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let graph = db.run_dag_json(&run_id).await.unwrap().unwrap();
    assert_eq!(graph, serde_json::to_value(dag()).unwrap());
    let unknown = "0190a3e5-0000-7000-8000-000000000000";
    assert_eq!(db.run_dag_json(unknown).await.unwrap(), None);
    // Parsed, from the cache (B137); its mapped instances counted (none here).
    assert_eq!(*db.run_dag(&run_id).await.unwrap().unwrap(), dag());
    assert!(db.run_dag(unknown).await.unwrap().is_none());
    assert_eq!(
        db.mapped_instances(&run_id, &id("extract")).await.unwrap(),
        0
    );

    assert_eq!(
        db.run_task_output(&run_id, &id("extract")).await.unwrap(),
        None
    );
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    db.store_output(&ready[0].instance_id, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    assert_eq!(
        db.run_task_output(&run_id, &id("extract")).await.unwrap(),
        Some(("SUCCESS".to_owned(), json!({"rows": 3})))
    );

    let trigger = db
        .insert_command(
            &Command::TriggerRun {
                event: None,
                params: None,
                dag_id: sync(),
                planned_for: None,
            },
            "anna",
            ts(NOW),
        )
        .await
        .unwrap();
    let result = json!({"run_id": run_id});
    db.decide_command(
        &trigger,
        CommandStatus::Applied,
        None,
        Some(&result),
        ts(NOW),
    )
    .await
    .unwrap();
    let cancel = db
        .insert_command(
            &Command::CancelRun {
                run_id: run_id.clone(),
            },
            "anna",
            ts("2026-10-21T08:00:01Z"),
        )
        .await
        .unwrap();
    db.insert_command(
        &Command::CancelRun {
            run_id: older.clone(),
        },
        "anna",
        ts(NOW),
    )
    .await
    .unwrap();
    let paused = db
        .insert_command(
            &Command::PauseDag {
                dag_id: sync(),
                cancel_runs: true,
            },
            "anna",
            ts("2026-10-21T08:00:02Z"),
        )
        .await
        .unwrap();
    let cancelled = json!({"cancelled_runs": [older, run_id]});
    db.decide_command(
        &paused,
        CommandStatus::Applied,
        None,
        Some(&cancelled),
        ts(NOW),
    )
    .await
    .unwrap();
    let commands = db.run_commands(&run_id).await.unwrap();
    let ids: Vec<&str> = commands.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, [trigger.as_str(), cancel.as_str(), paused.as_str()]);
}

#[tokio::test]
async fn an_attempts_usage_and_core_dump_are_stored() {
    let (db, run_id) = with_instances().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let (attempt_id, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let end = AttemptEnd {
        usage: Some(schedr_model::runner::ProcessUsage {
            user_cpu_micros: 1_500_000,
            system_cpu_micros: 250_000,
            peak_rss_bytes: Some(80_000_000),
            major_faults: 3,
            voluntary_context_switches: Some(40),
            involuntary_context_switches: Some(7),
        }),
        core_dumped: true,
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    assert!(db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap());
    let attempts = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    assert_eq!(
        (attempts[0].usage, attempts[0].core_dumped),
        (end.usage, true)
    );
}

#[tokio::test]
async fn worker_text_holding_nul_is_stored_with_a_replacement_character() {
    // B105: Postgres refuses NUL in `jsonb` and `text`; a task's error message holding one
    // made the attempt's end fail for ever.
    let (db, run_id) = with_instances().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let (attempt_id, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let summary = json!({"exception": "ValueError", "message": "bad\u{0}", "k\u{0}": 1});
    let end = AttemptEnd {
        error_summary: Some(summary.to_string()),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    assert!(db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap());
    let attempts = db.task_attempts(&run_id, &id("extract")).await.unwrap();
    let stored: serde_json::Value =
        serde_json::from_str(attempts[0].error_summary.as_deref().unwrap()).unwrap();
    assert_eq!(
        stored,
        json!({"exception": "ValueError", "message": "bad\u{fffd}", "k\u{fffd}": 1})
    );
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
        db.unstarted_attempts(ts("2026-10-21T09:00:00Z"), 100)
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
    db.mark_adopted(&attempt_id, ts(NOW)).await.unwrap();
    assert!(db.attempt(&attempt_id).await.unwrap().unwrap().started);
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
            outcome: Some("SUCCESS".to_owned()),
            used_retry: true,
            ..plain_update("extract", TaskInstanceState::Success)
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
            map_index: -1,
            state: TaskInstanceState::UpForRetry,
            ready_at: Some(ts(NOW)),
            outcome: None,
            used_retry: true,
            reason: None,
            map_sizes: None,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        db.due_retries(ts(NOW)).await.unwrap(),
        [(run_id.clone(), id("load"), -1)]
    );
    db.update_instances(
        &run_id,
        &[InstanceUpdate {
            task_id: id("load"),
            map_index: -1,
            state: TaskInstanceState::Unschedulable,
            ready_at: None,
            outcome: None,
            used_retry: false,
            reason: None,
            map_sizes: None,
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
                event: None,
                params: None,
                dag_id: sync(),
                planned_for: Some(ts("2026-10-15T01:00:00Z")),
            },
            "anna",
            ts(NOW),
        )
        .await
        .unwrap();
    let pending = db.pending_commands(100).await.unwrap();
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
async fn writes_announce_what_changed_once_per_run_dag_and_the_workers() {
    use schedr_db::changes::Change;
    let (db, version) = with_dag().await;
    let mut changes = db
        .changes(schedr_db::changes::POLL_INTERVAL)
        .await
        .unwrap()
        .expect("Postgres listens");
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
    let within = std::time::Duration::from_secs(10);
    let next = async |changes: &mut schedr_db::changes::Changes| {
        tokio::time::timeout(within, changes.next())
            .await
            .expect("changes within 10 s")
            .unwrap()
    };
    let run = Change::Run {
        run_id: run_id.clone(),
        dag_id: sync(),
    };
    assert_eq!(next(&mut changes).await, std::slice::from_ref(&run));
    // Two task instances in one statement: the run once.
    db.create_instances(
        &run_id,
        &[
            (id("extract"), -1, TaskInstanceState::Queued),
            (id("load"), -1, TaskInstanceState::Scheduled),
        ],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(next(&mut changes).await, std::slice::from_ref(&run));
    db.set_paused(&sync(), true).await.unwrap();
    assert_eq!(next(&mut changes).await, [Change::Dag { dag_id: sync() }]);
    // A heartbeat of a worker not registered changes nothing: no notification.
    db.worker_seen("w1", ts(NOW)).await.unwrap();
    let quiet = tokio::time::timeout(std::time::Duration::from_secs(2), changes.next()).await;
    assert!(quiet.is_err(), "{quiet:?}");
    insert_worker(&db).await;
    assert_eq!(next(&mut changes).await, [Change::Workers]);
}

#[tokio::test]
async fn the_switch_disables_and_enables_the_change_triggers() {
    use schedr_db::changes::Change;
    let (db, version) = with_dag().await;
    assert!(
        db.change_notifications().await.unwrap(),
        "on after the migration"
    );
    assert!(!db.set_change_notifications(true).await.unwrap());
    let mut changes = db
        .changes(schedr_db::changes::POLL_INTERVAL)
        .await
        .unwrap()
        .expect("Postgres listens");
    assert!(db.set_change_notifications(false).await.unwrap());
    assert!(!db.change_notifications().await.unwrap());
    db.insert_run(&run(
        &version,
        RunKind::Manual,
        RunState::Queued,
        "2026-10-21T07:00:00Z",
    ))
    .await
    .unwrap()
    .unwrap();
    let quiet = tokio::time::timeout(std::time::Duration::from_secs(2), changes.next()).await;
    assert!(quiet.is_err(), "off: nothing announced, {quiet:?}");
    assert!(db.set_change_notifications(true).await.unwrap());
    let run_id = db
        .insert_run(&run(
            &version,
            RunKind::Manual,
            RunState::Queued,
            "2026-10-21T08:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let announced = tokio::time::timeout(std::time::Duration::from_secs(10), changes.next())
        .await
        .expect("on again: announced")
        .unwrap();
    assert_eq!(
        announced,
        [Change::Run {
            run_id,
            dag_id: sync()
        }]
    );
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
    let error = limited.pending_commands(100).await.unwrap_err();
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
        event: None,
        params: None,
        dag_id: sync(),
        planned_for: None,
    };
    Box::pin(db.atomically(async {
        db.insert_command(&trigger, "anna", ts(NOW)).await?;
        db.insert_command(&trigger, "anna", ts(NOW)).await?;
        // The work reads its own writes.
        assert_eq!(db.pending_commands(100).await?.len(), 2);
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
    assert_eq!(db.pending_commands(100).await.unwrap().len(), 2);
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
    let states: Vec<(TaskId, i32, TaskInstanceState)> = (0..1201)
        .map(|i| (id(&format!("t{i:04}")), -1, TaskInstanceState::Queued))
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
    let tasks: Vec<(TaskId, i32, TaskInstanceState)> = (0..1201)
        .map(|i| (id(&format!("t{i:04}")), -1, TaskInstanceState::Scheduled))
        .collect();
    db.create_instances(&run_id, &tasks, ts(NOW)).await.unwrap();
    let updates: Vec<InstanceUpdate> = (0..1201)
        .map(|i| InstanceUpdate {
            task_id: id(&format!("t{i:04}")),
            map_index: -1,
            state: if i % 3 == 0 {
                TaskInstanceState::Queued
            } else {
                TaskInstanceState::Success
            },
            ready_at: (i % 3 == 0).then(|| ts(NOW)),
            outcome: (i % 3 != 0).then(|| "SUCCESS".to_owned()),
            used_retry: i % 2 == 0,
            reason: None,
            map_sizes: None,
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
    let tasks: Vec<(TaskId, i32, TaskInstanceState)> = (0..3)
        .map(|i| (id(&format!("t{i}")), -1, TaskInstanceState::Queued))
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

/// A worker row, written straight to the table.
async fn insert_worker(db: &Database) {
    sqlx::query(AssertSqlSafe(
        "INSERT INTO workers (worker_id, incarnation_id, state, protocol_version, \
         schedr_version, queues, slots, log_endpoint, registered_at, last_heartbeat_at) \
         VALUES ('w1', gen_random_uuid(), 'active', '1.0', '0.1.0', '[]', 1, 'x', now(), now())"
            .to_owned(),
    ))
    .execute(admin_pool(db))
    .await
    .unwrap();
}

/// An update of an unmapped task's state alone.
fn plain_update(task: &str, state: TaskInstanceState) -> InstanceUpdate {
    InstanceUpdate {
        task_id: id(task),
        map_index: -1,
        state,
        ready_at: None,
        outcome: None,
        used_retry: false,
        reason: None,
        map_sizes: None,
    }
}

#[tokio::test]
async fn a_write_cancelled_while_its_transaction_starts_leaves_no_open_transaction_behind() {
    // B84: a write waiting in its start (the fence, behind a lock on the lease's row) is
    // cancelled; Postgres then finishes the start. Its connection must not go back to the pool
    // inside that transaction (holding the row's lock and its settings).
    let url = fresh_url().await;
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let period = std::time::Duration::from_secs(30);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    let fenced = db.fenced("a", 1);
    let mut locker = sqlx::PgConnection::connect(&url).await.unwrap();
    let mut held = locker.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM master_lease FOR UPDATE")
        .execute(&mut *held)
        .await
        .unwrap();
    // Cancelled only once it waits for the lock in its start, not before it got there.
    let mut write = Box::pin(fenced.worker_seen("w1", ts(NOW)));
    let mut probe = sqlx::PgConnection::connect(&url).await.unwrap();
    let mut waits = 0;
    while waits == 0 {
        tokio::select! {
            done = &mut write => panic!("the write didn't wait for the lock: {done:?}"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
        waits = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(&mut probe)
        .await
        .unwrap();
    }
    drop(write);
    held.commit().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    // Every connection the pool hands out is outside any transaction.
    let pool = admin_pool(&db);
    let mut connections = Vec::new();
    for _ in 0..8 {
        let Ok(Ok(mut connection)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), pool.acquire()).await
        else {
            break;
        };
        let savepoint = sqlx::query("SAVEPOINT b84").execute(&mut *connection).await;
        assert!(
            savepoint.is_err(),
            "a pooled connection was inside a transaction"
        );
        connections.push(connection);
    }
    assert!(!connections.is_empty());
}

/// A TCP proxy to the test server for `url`: its URL through the proxy, without TLS, and a
/// switch that, once armed, holds the server's answers back from the next `BEGIN` the client
/// sends until released (`send_replace(false)`): a transaction's start cut short.
async fn begin_holding_proxy(
    url: &str,
) -> (
    String,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    tokio::sync::watch::Sender<bool>,
) {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let at = url.find('@').unwrap() + 1;
    let end = at + url[at..].find('/').unwrap();
    let upstream = url[at..end].to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxied = format!(
        "{}{}{}?sslmode=disable",
        &url[..at],
        listener.local_addr().unwrap(),
        &url[end..]
    );
    let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (held, _) = tokio::sync::watch::channel(false);
    let (armed_for_all, held_for_all) = (armed.clone(), held.clone());
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let server = tokio::net::TcpStream::connect(&upstream).await.unwrap();
            let (mut client_read, mut client_write) = client.into_split();
            let (mut server_read, mut server_write) = server.into_split();
            let (armed, held) = (armed_for_all.clone(), held_for_all.clone());
            tokio::spawn(async move {
                let mut buffer = vec![0; 1 << 16];
                // The end of the last read: a `BEGIN` split between two reads is still seen.
                let mut seen = Vec::new();
                while let Ok(read @ 1..) = client_read.read(&mut buffer).await {
                    seen.extend_from_slice(&buffer[..read]);
                    if seen.windows(5).any(|w| w == b"BEGIN") && armed.swap(false, Ordering::SeqCst)
                    {
                        held.send_replace(true);
                    }
                    seen.drain(..seen.len().saturating_sub(4));
                    if server_write.write_all(&buffer[..read]).await.is_err() {
                        break;
                    }
                }
            });
            let mut released = held_for_all.subscribe();
            tokio::spawn(async move {
                let mut buffer = vec![0; 1 << 16];
                while let Ok(read @ 1..) = server_read.read(&mut buffer).await {
                    let _ = released.wait_for(|held| !held).await;
                    if client_write.write_all(&buffer[..read]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    (proxied, armed, held)
}

/// A start of a transaction cut short where it can be, other than a write's (see
/// [`a_write_cancelled_while_its_transaction_starts_leaves_no_open_transaction_behind`]).
#[derive(Debug, Clone, Copy)]
enum Start {
    Lease,
    ChangeNotifications,
    Migration,
    MigrationAsLeaseHolder,
    NotificationProbe,
}

#[rstest::rstest]
#[case(Start::Lease)]
#[case(Start::ChangeNotifications)]
#[case(Start::Migration)]
#[case(Start::MigrationAsLeaseHolder)]
#[case(Start::NotificationProbe)]
#[tokio::test]
async fn a_start_cancelled_before_begin_answers_leaves_no_open_transaction_behind(
    #[case] start: Start,
) {
    // B84, Codex's reviews: the lease's, the change notifications', the migrations' and the
    // notifications' check's own transactions.
    let url = fresh_url().await;
    if !matches!(start, Start::Migration | Start::MigrationAsLeaseHolder) {
        Database::connect(&url)
            .await
            .unwrap()
            .migrate()
            .await
            .unwrap();
    }
    let (proxied, armed, held) = begin_holding_proxy(&url).await;
    let db = Database::connect(&proxied).await.unwrap();
    let on = db.change_notifications().await.unwrap();
    armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let period = std::time::Duration::from_secs(30);
    let started: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> = match start {
        Start::Lease => Box::pin(async {
            let _ = db.take_lease("a", period).await;
        }),
        Start::ChangeNotifications => Box::pin(async {
            let _ = db.set_change_notifications(!on).await;
        }),
        Start::Migration => Box::pin(async {
            let _ = db.migrate().await;
        }),
        Start::MigrationAsLeaseHolder => Box::pin(async {
            let _ = db.migrate_as_lease_holder().await;
        }),
        // Its check that no pooler in transaction mode is in the way (on the pool's own
        // connections).
        Start::NotificationProbe => Box::pin(async {
            let _ = db.command_notifications().await;
        }),
    };
    // Cut short once its `BEGIN` is sent, its answer held back.
    let mut on_hold = held.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            () = started => panic!("{start:?} ended with its BEGIN unanswered"),
            _ = on_hold.wait_for(|held| *held) => {}
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{start:?} never began a transaction"));
    held.send_replace(false);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let pool = admin_pool(&db);
    let mut connections = Vec::new();
    for _ in 0..8 {
        let Ok(Ok(mut connection)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), pool.acquire()).await
        else {
            break;
        };
        let savepoint = sqlx::query("SAVEPOINT b84").execute(&mut *connection).await;
        assert!(
            savepoint.is_err(),
            "{start:?}: a pooled connection was inside a transaction"
        );
        connections.push(connection);
    }
    assert!(!connections.is_empty());
}

#[tokio::test]
async fn a_command_sent_again_with_its_key_is_stored_once() {
    // B140, on Postgres: the conflict target is the partial unique index.
    let db = fresh().await;
    let pause = Command::PauseDag {
        dag_id: sync(),
        cancel_runs: false,
    };
    let first = db
        .submit_command(&pause, "anna", Some("k1"), ts(NOW))
        .await
        .unwrap();
    let Submitted::New(id) = first else {
        panic!("expected a new command")
    };
    assert_eq!(
        db.submit_command(&pause, "anna", Some("k1"), ts(NOW))
            .await
            .unwrap(),
        Submitted::Again(id)
    );
    let cancel = Command::CancelRun {
        run_id: "r1".to_owned(),
    };
    assert_eq!(
        db.submit_command(&cancel, "anna", Some("k1"), ts(NOW))
            .await
            .unwrap(),
        Submitted::KeyTaken
    );
    assert_eq!(db.pending_commands(100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn open_attempts_are_read_with_their_dag_for_the_limits() {
    // M4.4 (decision 88A): what the master counts when it starts.
    let (db, run_id) = with_instances().await;
    assert_eq!(db.active_attempts().await.unwrap(), []);
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(
        ready[0].version_id,
        db.active_dags().await.unwrap()[0].version_id
    );
    let (attempt_id, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let active = db.active_attempts().await.unwrap();
    assert_eq!(
        active,
        [schedr_db::attempts::ActiveAttempt {
            attempt_id: attempt_id.clone(),
            dag_id: sync(),
            run_id,
            task_id: id("extract"),
        }]
    );
    db.end_attempt(
        &attempt_id,
        &AttemptEnd::by_master(schedr_model::AttemptStatus::Success),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(db.active_attempts().await.unwrap(), []);
}

#[tokio::test]
async fn a_deployments_nodes_are_recorded_and_read() {
    use schedr_db::nodes::{MasterNode, WorkerNode};
    use schedr_db::workers::WorkerRegistration;

    let db = fresh().await;
    let config = json!({"database": {"url": "postgres://schedr:****@db/s"}});
    // Its problems go with its row (decision 129).
    let problems = [schedr_db::nodes::NodeProblem {
        error: true,
        message: "a round failed".to_owned(),
    }];
    let master = |id: &'static str, now: &str| {
        let master = MasterNode {
            master_id: id,
            process_id: "p1",
            host: "node-a",
            schedr_version: "0.1.0",
            config: &config,
            started_at: ts("2026-11-01T08:00:00Z"),
            problems: &problems,
        };
        let db = &db;
        let now = ts(now);
        async move { db.heartbeat_master(&master, now).await.unwrap() }
    };
    master("gone", "2026-11-01T06:00:00Z").await;
    master("m-a", "2026-11-01T08:00:05Z").await;
    master("m-b", "2026-11-01T08:00:05Z").await;
    db.take_lease("m-b", std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let read = db.masters(ts("2026-11-01T08:00:00Z")).await.unwrap();
    assert_eq!(
        read.iter()
            .map(|m| (m.master_id.as_str(), m.leading))
            .collect::<Vec<_>>(),
        vec![("m-b", true), ("m-a", false)]
    );
    assert_eq!(read[0].config, config);
    assert_eq!(read[0].heard_at, ts("2026-11-01T08:00:05Z"));
    assert_eq!(read[0].problems, problems);
    assert_eq!(
        db.masters(Timestamp::UNIX_EPOCH).await.unwrap().len(),
        2,
        "the gone one removed"
    );

    let queues = vec!["default".to_owned()];
    db.register_worker(
        &WorkerRegistration {
            id: "w1",
            incarnation: "0190a0a0-0000-7000-8000-000000000001",
            protocol_version: "1.0",
            schedr_version: "0.1.0",
            queues: &queues,
            slots: 2,
            watcher_slots: 0,
            log_endpoint: "",
        },
        ts(NOW),
    )
    .await
    .unwrap();
    let worker = json!({"worker": {"slots": 2}});
    db.set_worker_node("w1", Some("m-b"), Some("p2"), Some(&worker))
        .await
        .unwrap();
    assert_eq!(
        db.worker_nodes().await.unwrap(),
        vec![WorkerNode {
            worker_id: "w1".to_owned(),
            master_id: Some("m-b".to_owned()),
            process_id: Some("p2".to_owned()),
            config: Some(worker),
        }]
    );
}

#[tokio::test]
async fn secrets_are_set_merged_listed_read_and_deleted() {
    use schedr_db::secrets::SecretKey;
    let db = fresh().await;
    let key = SecretKey::generate().unwrap();
    let values = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    };
    db.set_secret(
        &key,
        "etl/db",
        &values(&[("user", "etl"), ("password", "pw-1")]),
        ts(NOW),
    )
    .await
    .unwrap();
    db.set_secret(&key, "etl/db", &values(&[("password", "pw-2")]), ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.secret(&key, "etl/db").await.unwrap(),
        Some(values(&[("password", "pw-2"), ("user", "etl")]))
    );
    let listed = db.secret_entries("etl/").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].keys, ["password", "user"]);
    assert!(
        listed[0].updated_at.starts_with("2026-10-21T08:00:00"),
        "{listed:?}"
    );
    assert!(db.delete_secret("etl/db").await.unwrap());
    assert_eq!(db.secret(&key, "etl/db").await.unwrap(), None);
}

#[tokio::test]
async fn writes_to_one_secret_at_once_keep_every_key() {
    // Codex's finding in M4.6c's review: a read, a merge and a write lost keys.
    use schedr_db::secrets::SecretKey;
    let db = fresh().await;
    let key = SecretKey::generate().unwrap();
    for path in ["new", "existing"] {
        if path == "existing" {
            let seed = BTreeMap::from([("seed".to_owned(), "s".to_owned())]);
            db.set_secret(&key, path, &seed, ts(NOW)).await.unwrap();
        }
        let writes = (0..16).map(|i| {
            let (db, key) = (db.clone(), key.clone());
            tokio::spawn(async move {
                let values = BTreeMap::from([(format!("k{i:02}"), format!("v{i}"))]);
                db.set_secret(&key, path, &values, ts(NOW)).await.unwrap();
            })
        });
        for write in writes.collect::<Vec<_>>() {
            write.await.unwrap();
        }
        let stored = db.secret(&key, path).await.unwrap().unwrap();
        let expected = 16 + usize::from(path == "existing");
        assert_eq!(stored.len(), expected, "{path}: {:?}", stored.keys());
    }
}

#[tokio::test]
async fn a_cleanups_end_and_result_are_stored() {
    let (db, run_id) = with_instances().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let instance_id = ready[0].instance_id.clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let end = AttemptEnd {
        cleanup: Some(CleanupEnd {
            result: CleanupResult::Failed,
            tries: 4,
            duration_ms: 1_500,
            error_kind: Some("timeout".to_owned()),
            // B105: NUL in `jsonb` must not make the attempt's end fail for ever.
            error_summary: Some(json!({"message": "bad\u{0}"}).to_string()),
        }),
        ..AttemptEnd::by_master(AttemptStatus::Success)
    };
    assert!(db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap());
    let stored = |db: Database, run_id: String, instance_id: String| async move {
        let tasks = db.run_tasks(&run_id).await.unwrap().unwrap();
        let found = tasks.instances.iter().find(|i| i.id == instance_id);
        found.unwrap().cleanup_result
    };
    assert_eq!(
        stored(db.clone(), run_id.clone(), instance_id.clone()).await,
        None
    );
    db.set_cleanup_result(&instance_id, CleanupResult::Failed, ts(NOW))
        .await
        .unwrap();
    db.set_cleanup_result(&instance_id, CleanupResult::Ok, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        stored(db, run_id, instance_id).await,
        Some(CleanupResult::Failed)
    );
}

#[tokio::test]
async fn a_trigger_s_events_and_bookmark_are_stored() {
    let (db, version) = with_dag().await;
    db.trigger_assigned(
        "sync",
        "01a00000-0000-7000-8000-000000000001",
        "w1",
        &version,
        "h1",
        ts(NOW),
    )
    .await
    .unwrap();
    let event = schedr_model::RunEvent {
        key: "a".to_owned(),
        data: serde_json::json!({"n": 1}),
        at: Some(ts(NOW)),
    };
    let mut first = run(&version, RunKind::Event, RunState::Queued, NOW);
    first.event = Some(&event);
    let id = "01a00000-0000-7000-8000-000000000002";
    assert_eq!(
        db.store_event(id, &first, &event).await.unwrap().as_deref(),
        Some(id)
    );
    assert_eq!(db.run_event(id).await.unwrap(), Some(event.clone()));
    let again = "01a00000-0000-7000-8000-000000000003";
    assert_eq!(db.store_event(again, &first, &event).await.unwrap(), None);
    let record = db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(record.bookmark.as_deref(), Some("a"));
    assert_eq!(record.last_planned_for, Some(ts(NOW)));
    assert_eq!(record.dag_version_id.as_deref(), Some(version.as_str()));
    assert!(
        !db.trigger_ended("sync", "01a00000-0000-7000-8000-000000000009", None, None)
            .await
            .unwrap(),
        "another run of it isn't this one"
    );
    assert!(
        db.trigger_ended(
            "sync",
            "01a00000-0000-7000-8000-000000000001",
            Some("failed"),
            Some(ts(NOW))
        )
        .await
        .unwrap()
    );
    let ended = db.triggers().await.unwrap().remove(0);
    assert_eq!(ended.instance_id, None);
    assert_eq!(ended.failures, 1);
    assert_eq!(ended.not_before, Some(ts(NOW)));
    assert_eq!(ended.bookmark.as_deref(), Some("a"));
}

#[tokio::test]
async fn the_runs_of_another_dag_are_found_after_a_cursor_on_postgres() {
    use schedr_db::run_of::{Matching, RunOfFrom, RunOfSeen};
    let (db, run_id) = with_instances().await;
    let ended = ts("2026-10-21T08:30:00Z");
    db.update_instances(
        &run_id,
        &[plain_update("extract", TaskInstanceState::Success)],
        ended,
    )
    .await
    .unwrap();
    db.finish_run(&run_id, RunState::Success, ended)
        .await
        .unwrap();
    let before = ts("2026-10-21T00:00:00Z");
    for task in [None, Some("extract")] {
        let found = db
            .runs_of("sync", task, Matching::EndedAfter(before))
            .await
            .unwrap();
        assert_eq!(
            (found.len(), found[0].run_id.as_str()),
            (1, run_id.as_str())
        );
        let planned = db
            .runs_of(
                "sync",
                task,
                Matching::PlannedFor(ts("2026-10-21T07:00:00Z")),
            )
            .await
            .unwrap();
        assert_eq!(planned.len(), 1);
        let since = db
            .succeeded_since(("sync", task), "sync", RunOfFrom::Since(before), 10)
            .await
            .unwrap();
        assert_eq!(since.len(), 1, "{task:?}");
        assert_eq!(since[0].0, run_id);
        let seen = RunOfSeen {
            at: since[0].2,
            run: run_id.clone(),
        };
        let after = db
            .succeeded_since(("sync", task), "sync", RunOfFrom::After(&seen), 10)
            .await
            .unwrap();
        assert_eq!(after, [], "strictly after");
        let earlier = RunOfSeen {
            at: since[0].2,
            run: "00000000-0000-0000-0000-000000000000".to_owned(),
        };
        let tie = db
            .succeeded_since(("sync", task), "sync", RunOfFrom::After(&earlier), 10)
            .await
            .unwrap();
        assert_eq!(tie.len(), 1, "the same end, a later run ID");
        let ended = db
            .ended_since(("sync", task), RunOfFrom::Since(before), 10)
            .await
            .unwrap();
        assert_eq!(ended.len(), 1, "{task:?}");
    }
    let seen = RunOfSeen {
        at: ended,
        run: run_id.clone(),
    };
    db.set_run_of_seen("sync", r#"{"dag":"orders"}"#, &seen)
        .await
        .unwrap();
    assert_eq!(
        db.run_of_seen("sync", r#"{"dag":"orders"}"#).await.unwrap(),
        Some(seen)
    );
    assert_eq!(
        db.run_of_seen("sync", r#"{"dag":"other"}"#).await.unwrap(),
        None
    );
    // An `@trigger(wait_for=...)`'s run handed, then passed (decision 353).
    let handed = RunOfSeen {
        at: ended,
        run: run_id.clone(),
    };
    db.hand_trigger_run("sync", &handed).await.unwrap();
    assert_eq!(
        db.trigger_run_handed("sync").await.unwrap(),
        Some(handed.clone())
    );
    assert!(
        db.pass_trigger_run("sync", r#"{"dag":"other"}"#, Some("skipped"), None)
            .await
            .unwrap()
    );
    assert_eq!(db.trigger_run_handed("sync").await.unwrap(), None);
    assert_eq!(
        db.run_of_seen("sync", r#"{"dag":"other"}"#).await.unwrap(),
        Some(handed)
    );
    db.set_paused_at(&sync(), true, ts(NOW)).await.unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), None);
    db.set_paused_at(&sync(), false, ended).await.unwrap();
    assert_eq!(db.live_since("sync").await.unwrap(), Some(ended));
}

#[tokio::test]
async fn posted_events_are_stored_once_per_request_key_and_read_in_order_on_postgres() {
    use schedr_db::named_events::{NewNamedEvent, Posted};
    let db = fresh().await;
    let data = serde_json::json!({"amount": 40});
    let event = |key| NewNamedEvent {
        name: "paid",
        key,
        data: &data,
        at: ts("2026-11-08T10:00:00Z"),
    };
    let now = ts("2026-11-08T10:00:01Z");
    let Posted::New(first) = db
        .post_named_event(&event("o-1"), "anna", Some("k1"), now)
        .await
        .unwrap()
    else {
        panic!("a new event")
    };
    let again = db
        .post_named_event(&event("o-1"), "anna", Some("k1"), now)
        .await
        .unwrap();
    assert_eq!(again, Posted::Again(first.clone()));
    let taken = db
        .post_named_event(&event("o-2"), "anna", Some("k1"), now)
        .await
        .unwrap();
    assert_eq!(taken, Posted::KeyTaken);
    let later = ts("2026-11-08T10:00:02Z");
    db.post_named_event(&event("o-2"), "anna", None, later)
        .await
        .unwrap();
    let all = db
        .named_events_unseen("paid", "sync", now, 10)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|e| e.key.as_str()).collect::<Vec<_>>(),
        ["o-1", "o-2"]
    );
    let first_unpassed = db.named_event_unpassed("paid", "sync", now).await.unwrap();
    assert_eq!(first_unpassed.map(|e| e.id), Some(first.clone()));
    let found = db
        .first_named_event("paid", Some("o-2"), now, None)
        .await
        .unwrap();
    assert_eq!(found.map(|e| e.key), Some("o-2".to_owned()));
    let any = db
        .first_named_event("paid", None, later, None)
        .await
        .unwrap();
    assert_eq!(any.map(|e| e.posted_at), Some(later));
    assert_eq!(db.named_events(None, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_backfill_its_runs_and_the_times_covered_are_stored_on_postgres() {
    let (db, version) = with_dag().await;
    let backfill_id = "01a1c0a0-0000-7000-8000-000000000001";
    let backfill = schedr_db::backfills::NewBackfill {
        id: backfill_id,
        dag_id: &sync(),
        range_from: ts("2026-10-01T00:00:00Z"),
        range_to: ts("2026-10-04T00:00:00Z"),
        all_fire_times: true,
        requested_by: "anna",
        created_at: ts(NOW),
        fire_times: 3,
        created_runs: 2,
    };
    assert!(db.insert_backfill(&backfill).await.unwrap());
    assert!(!db.insert_backfill(&backfill).await.unwrap(), "stored once");
    let mut ids = Vec::new();
    // Created at once, the later one first: still placed by `planned_for`.
    for at in ["2026-10-03T01:00:00Z", "2026-10-02T01:00:00Z"] {
        let new = NewRun {
            backfill_id: Some(backfill_id),
            triggered_at: ts(NOW),
            ..run(&version, RunKind::Backfill, RunState::Queued, at)
        };
        ids.push(db.insert_run(&new).await.unwrap().unwrap());
    }
    db.insert_run(&run(
        &version,
        RunKind::Manual,
        RunState::Running,
        "2026-10-01T01:00:00Z",
    ))
    .await
    .unwrap()
    .unwrap();
    db.finish_run(&ids[0], RunState::Success, ts(NOW))
        .await
        .unwrap();
    let stored = db.backfill(backfill_id).await.unwrap().unwrap();
    assert_eq!(
        (
            stored.all_fire_times,
            stored.fire_times,
            stored.created_runs
        ),
        (true, 3, 2)
    );
    let mut runs = stored.runs.clone();
    runs.sort_by_key(|(state, _)| state.to_string());
    assert_eq!(runs, [(RunState::Queued, 1), (RunState::Success, 1)]);
    assert_eq!(db.dag_backfills(&sync(), 5).await.unwrap(), [stored]);
    assert_eq!(
        db.unfinished_backfill_runs(backfill_id).await.unwrap(),
        [ids[1].clone()]
    );
    let place = |run: &str| {
        let db = db.clone();
        let run = run.to_owned();
        async move {
            let place = db
                .run_record(&run)
                .await
                .unwrap()
                .unwrap()
                .backfill
                .unwrap();
            (place.index, place.total)
        }
    };
    assert_eq!(
        (place(&ids[1]).await, place(&ids[0]).await),
        ((1, 2), (2, 2))
    );
    let queued = db.queued_runs(&sync()).await.unwrap();
    assert_eq!(queued[0].planned_for, ts("2026-10-02T01:00:00Z"));
    let covered = db
        .covered_times(
            &sync(),
            ts("2026-10-01T00:00:00Z"),
            ts("2026-10-04T00:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(
        covered.into_iter().collect::<Vec<_>>(),
        [
            ts("2026-10-01T01:00:00Z"),
            ts("2026-10-02T01:00:00Z"),
            ts("2026-10-03T01:00:00Z")
        ]
    );
}

#[tokio::test]
async fn notifications_are_queued_retried_and_deleted_with_what_their_messages_read() {
    use schedr_db::notifications::{FailedTask, NewNotification};
    let (db, run_id) = with_instances().await;
    let now = ts(NOW);
    db.queue_notifications(
        &[NewNotification {
            dag_id: "sync".into(),
            channel: "ops".into(),
            kind: "webhook".into(),
            url: None,
            event: "RUN_FAILED".into(),
            body: json!({"text": "x", "tasks": [1]}),
        }],
        now,
    )
    .await
    .unwrap();
    let due = db.due_notifications(now, 10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].body, json!({"text": "x", "tasks": [1]}));
    assert_eq!(due[0].created_at, now);
    db.retry_notification(&due[0].id, ts("2026-10-21T09:00:00Z"), "HTTP 503")
        .await
        .unwrap();
    assert_eq!(db.due_notifications(now, 10).await.unwrap(), []);
    let later = db
        .due_notifications(ts("2026-10-21T09:00:00Z"), 10)
        .await
        .unwrap();
    assert_eq!(later[0].tries, 1);
    db.delete_notification(&due[0].id).await.unwrap();
    assert_eq!(
        db.due_notifications(ts("2030-01-01T00:00:00Z"), 10)
            .await
            .unwrap(),
        []
    );

    db.update_instances(
        &run_id,
        &[plain_update("extract", TaskInstanceState::Failed)],
        now,
    )
    .await
    .unwrap();
    assert_eq!(
        db.failed_tasks(&run_id, 20).await.unwrap(),
        [FailedTask {
            task_id: "extract".into(),
            map_index: -1,
            error: None
        }]
    );
    let instance = &db.run_tasks(&run_id).await.unwrap().unwrap().instances[0];
    assert_eq!(db.instance_error(&instance.id).await.unwrap(), None);
    assert!(!db.previous_run_failed("sync", &run_id).await.unwrap());
    db.finish_run(&run_id, RunState::Failed, now).await.unwrap();
    let other = uuid::Uuid::now_v7().to_string();
    assert!(db.previous_run_failed("sync", &other).await.unwrap());
}
