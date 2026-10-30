use pretty_assertions::assert_eq;
use schedr_model::Dag;
use serde_json::json;
use sqlx::AssertSqlSafe;

use super::*;
use crate::attempts::AttemptEnd;
use crate::dags::{DagChange, NewParse};
use crate::runs::NewRun;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

fn extract() -> TaskId {
    TaskId::new("extract").unwrap()
}

fn dag() -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [{
            "id": "extract", "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }],
        "edges": []
    }))
    .unwrap()
}

/// A database with DAG `sync` and its version's ID.
async fn with_dag() -> (Database, String) {
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
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version)
}

async fn add_run(db: &Database, version: &str, planned_for: &str, state: RunState) -> String {
    let dag_id = sync();
    db.insert_run(&NewRun {
        dag_id: &dag_id,
        dag_version_id: version,
        kind: RunKind::Manual,
        state,
        planned_for: ts(planned_for),
        since: ts("2026-10-01T00:00:00Z"),
        triggered_at: ts(planned_for),
        triggered_by: "anna",
    })
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn a_dags_runs_are_listed_newest_first_and_can_be_filtered() {
    let (db, version) = with_dag().await;
    let older = add_run(&db, &version, "2026-10-14T03:00:00Z", RunState::Queued).await;
    let newer = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    let runs = db.dag_runs(&sync(), None, 10).await.unwrap();
    let ids: Vec<&str> = runs.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, [newer.as_str(), older.as_str()]);
    let first = &runs[0];
    assert_eq!(
        (
            first.dag_id.as_str(),
            first.kind,
            first.state,
            first.triggered_by.as_str()
        ),
        ("sync", RunKind::Manual, RunState::Running, "anna")
    );
    assert_eq!(first.planned_for, ts("2026-10-15T03:00:00Z"));
    assert_eq!(first.started_at, Some(ts("2026-10-15T03:00:00Z")));
    assert_eq!(runs[1].started_at, None);
    assert_eq!(first.ended_at, None);

    let on_day = db
        .dag_runs(&sync(), Some(ts("2026-10-14T03:00:00Z")), 10)
        .await
        .unwrap();
    assert_eq!(on_day.len(), 1);
    assert_eq!(on_day[0].id, older);
    assert_eq!(db.dag_runs(&sync(), None, 1).await.unwrap().len(), 1);
    assert_eq!(db.run_record(&newer).await.unwrap().unwrap(), runs[0]);
    assert_eq!(
        db.run_record("0190a3e5-0000-7000-8000-000000000000")
            .await
            .unwrap(),
        None
    );
}

/// Defines DAG `other` (the same tasks as `sync`) and returns its version's ID.
async fn add_other_dag(db: &Database) -> String {
    let mut other = dag();
    other.id = DagId::new("other").unwrap();
    let change = DagChange::Define {
        dag: Box::new(other),
        parse_id: db
            .insert_parse(&NewParse {
                root: "etl",
                content_hash: "h2",
                schedr_version: "0.1.0",
                worker_id: "w1",
                parsed_at: ts("2026-10-01T00:00:00Z"),
                files: &[],
            })
            .await
            .unwrap(),
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-10-01T00:00:00Z"))
        .await
        .unwrap();
    db.active_dags()
        .await
        .unwrap()
        .into_iter()
        .find(|d| d.dag.id.as_str() == "other")
        .unwrap()
        .version_id
}

#[tokio::test]
async fn all_runs_are_listed_newest_triggered_first_filtered_and_paged() {
    let (db, version) = with_dag().await;
    let other_version = add_other_dag(&db).await;
    let first = add_run(&db, &version, "2026-10-14T03:00:00Z", RunState::Success).await;
    let second = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Failed).await;
    let other = db
        .insert_run(&NewRun {
            dag_id: &DagId::new("other").unwrap(),
            dag_version_id: &other_version,
            kind: RunKind::Manual,
            state: RunState::Success,
            planned_for: ts("2026-10-16T03:00:00Z"),
            since: ts("2026-10-01T00:00:00Z"),
            triggered_at: ts("2026-10-16T03:00:00Z"),
            triggered_by: "anna",
        })
        .await
        .unwrap()
        .unwrap();
    let ids = |runs: Vec<RunRecord>| runs.into_iter().map(|r| r.id).collect::<Vec<_>>();
    let filter = |dag: Option<&str>, state, before: Option<&str>, limit| RunFilter {
        dag_id: dag.map(|d| DagId::new(d).unwrap()),
        state,
        before: before.map(ts),
        before_run: None,
        limit,
    };

    let all = db.runs(&filter(None, None, None, 10)).await.unwrap();
    assert_eq!(all[0].dag_id.as_str(), "other");
    assert_eq!(ids(all), [other.as_str(), second.as_str(), first.as_str()]);
    let sync_only = db.runs(&filter(Some("sync"), None, None, 10)).await;
    assert_eq!(ids(sync_only.unwrap()), [second.as_str(), first.as_str()]);
    let successes = db
        .runs(&filter(None, Some(RunState::Success), None, 10))
        .await;
    assert_eq!(ids(successes.unwrap()), [other.as_str(), first.as_str()]);
    let page = db.runs(&filter(None, None, None, 2)).await.unwrap();
    let next = db
        .runs(&filter(None, None, Some("2026-10-15T03:00:00Z"), 2))
        .await;
    assert_eq!(ids(page), [other.as_str(), second.as_str()]);
    assert_eq!(ids(next.unwrap()), [first.as_str()]);
    let both = db
        .runs(&filter(Some("sync"), Some(RunState::Failed), None, 10))
        .await;
    assert_eq!(ids(both.unwrap()), [second.as_str()]);
    let capped = db.runs(&filter(None, None, None, u32::MAX)).await;
    assert_eq!(capped.unwrap().len(), 3);
}

#[tokio::test]
async fn runs_triggered_together_are_paged_by_id_none_skipped() {
    let (db, version) = with_dag().await;
    let together = "2026-10-21T08:00:00Z";
    let mut created = Vec::new();
    for planned_for in [
        "2026-10-18T03:00:00Z",
        "2026-10-19T03:00:00Z",
        "2026-10-20T03:00:00Z",
    ] {
        let run = NewRun {
            dag_id: &sync(),
            dag_version_id: &version,
            kind: RunKind::Scheduled,
            state: RunState::Queued,
            planned_for: ts(planned_for),
            since: ts("2026-10-01T00:00:00Z"),
            triggered_at: ts(together),
            triggered_by: "scheduler",
        };
        created.push(db.insert_run(&run).await.unwrap().unwrap());
    }
    let page = |before: Option<&str>, before_run: Option<&String>| RunFilter {
        before: before.map(ts),
        before_run: before_run.cloned(),
        limit: 2,
        ..RunFilter::default()
    };
    let first: Vec<String> = db
        .runs(&page(None, None))
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(first, [created[2].clone(), created[1].clone()]);
    let next = db
        .runs(&page(Some(together), Some(&first[1])))
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].id, created[0]);
    // The time alone skips what is left at that moment.
    assert_eq!(db.runs(&page(Some(together), None)).await.unwrap(), []);
}

#[tokio::test]
async fn a_runs_dag_and_a_tasks_output_are_read() {
    let (db, version) = with_dag().await;
    let run_id = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    let stored = db.run_dag_json(&run_id).await.unwrap().unwrap();
    assert_eq!(stored, serde_json::to_value(dag()).unwrap());
    let unknown = "0190a3e5-0000-7000-8000-000000000000";
    assert_eq!(db.run_dag_json(unknown).await.unwrap(), None);

    let now = ts("2026-10-15T03:00:01Z");
    db.create_instances(&run_id, &[(extract(), -1, TaskInstanceState::Queued)], now)
        .await
        .unwrap();
    assert_eq!(db.run_task_output(&run_id, &extract()).await.unwrap(), None);
    let instance = db.ready_instances(now).await.unwrap()[0]
        .instance_id
        .clone();
    db.store_output(&instance, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    assert_eq!(
        db.run_task_output(&run_id, &extract()).await.unwrap(),
        Some(("SUCCESS".to_owned(), json!({"rows": 3})))
    );
    let missing = TaskId::new("load").unwrap();
    assert_eq!(db.run_task_output(&run_id, &missing).await.unwrap(), None);
    assert_eq!(db.run_task_output(unknown, &extract()).await.unwrap(), None);
}

#[tokio::test]
async fn a_runs_instances_and_attempts_are_shown() {
    let (db, version) = with_dag().await;
    let run_id = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    let now = ts("2026-10-15T03:00:01Z");
    db.create_instances(&run_id, &[(extract(), -1, TaskInstanceState::Queued)], now)
        .await
        .unwrap();
    let ready = db.ready_instances(now).await.unwrap();
    let (attempt_id, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", now)
        .await
        .unwrap()
        .unwrap();
    let end = AttemptEnd {
        exit_code: Some(1),
        error_kind: Some("import_failed".to_owned()),
        error_summary: Some("{}".to_owned()),
        duration_ms: Some(1_234),
        code_hash: Some("sha256:a".to_owned()),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    assert_eq!(
        db.run_code_hashes(&run_id).await.unwrap(),
        Vec::<String>::new()
    );
    db.end_attempt(&attempt_id, &end, now).await.unwrap();
    assert_eq!(db.run_code_hashes(&run_id).await.unwrap(), ["sha256:a"]);

    let instances = db.run_instances(&run_id).await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(
        (
            instances[0].task_id.as_str(),
            instances[0].state,
            instances[0].try_number
        ),
        ("extract", TaskInstanceState::Running, 1)
    );
    let attempts = db.task_attempts(&run_id, &extract()).await.unwrap();
    assert_eq!(attempts.len(), 1);
    let attempt = &attempts[0];
    assert_eq!(
        (
            attempt.try_number,
            attempt.worker_id.as_deref(),
            attempt.status
        ),
        (1, Some("w1"), Some(AttemptStatus::Failed))
    );
    assert_eq!(attempt.exit_code, Some(1));
    assert_eq!(attempt.duration_ms, Some(1_234));
    assert_eq!(attempt.error_kind.as_deref(), Some("import_failed"));
    assert_eq!(attempt.assigned_at, now);
    assert_eq!(attempt.ended_at, Some(now));
    assert_eq!(attempt.started_at, None);
    assert_eq!(attempt.start_method, None);
}

#[tokio::test]
async fn stored_values_that_dont_read_are_errors() {
    let (db, version) = with_dag().await;
    let run_id = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    db.create_instances(
        &run_id,
        &[(extract(), -1, TaskInstanceState::Queued)],
        ts("2026-10-15T03:00:01Z"),
    )
    .await
    .unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    for (statement, what) in [
        ("UPDATE runs SET ended_at = 'later'", "runs.ended_at"),
        (
            "UPDATE runs SET ended_at = NULL, dag_id = 'bad id!'",
            "runs.dag_id",
        ),
    ] {
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(statement.to_owned()))
            .execute(pool)
            .await
            .unwrap();
        let error = db.run_record(&run_id).await.unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with(&format!("invalid stored data in {what}: ")),
            "{error}"
        );
    }
    sqlx::query("UPDATE task_instances SET task_id = 'bad id!'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.run_instances(&run_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in task_instances.task_id: "),
        "{error}"
    );
}

#[tokio::test]
async fn states_are_counted_for_the_masters_gauges() {
    let (db, version) = with_dag().await;
    assert_eq!(db.state_counts().await.unwrap(), StateCounts::default());
    let running = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    add_run(&db, &version, "2026-10-14T03:00:00Z", RunState::Success).await;
    let done = add_run(&db, &version, "2026-10-13T03:00:00Z", RunState::Success).await;
    let now = ts("2026-10-15T03:00:01Z");
    db.create_instances(&running, &[(extract(), -1, TaskInstanceState::Queued)], now)
        .await
        .unwrap();
    // A finished run's tasks don't count.
    db.create_instances(&done, &[(extract(), -1, TaskInstanceState::Success)], now)
        .await
        .unwrap();
    db.register_worker(
        &crate::workers::WorkerRegistration {
            id: "w1",
            incarnation: "0190a0a0-0000-7000-8000-000000000001",
            protocol_version: "1.0",
            schedr_version: "0.1.0",
            queues: &[],
            slots: 2,
            log_endpoint: "",
        },
        now,
    )
    .await
    .unwrap();
    assert_eq!(
        db.state_counts().await.unwrap(),
        StateCounts {
            runs: vec![
                ("running".to_owned(), "manual".to_owned(), 1),
                ("success".to_owned(), "manual".to_owned(), 2),
            ],
            tasks: vec![("queued".to_owned(), 1)],
            workers: vec![("active".to_owned(), 1)],
        }
    );
}
