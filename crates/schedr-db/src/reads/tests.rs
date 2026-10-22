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
        "trigger": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
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

#[tokio::test]
async fn a_runs_instances_and_attempts_are_shown() {
    let (db, version) = with_dag().await;
    let run_id = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    let now = ts("2026-10-15T03:00:01Z");
    db.create_instances(&run_id, &[(extract(), TaskInstanceState::Queued)], now)
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
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    db.end_attempt(&attempt_id, &end, now).await.unwrap();

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
    assert_eq!(attempt.error_kind.as_deref(), Some("import_failed"));
    assert_eq!(attempt.assigned_at, now);
    assert_eq!(attempt.ended_at, Some(now));
    assert_eq!(attempt.started_at, None);
}

#[tokio::test]
async fn stored_values_that_dont_read_are_errors() {
    let (db, version) = with_dag().await;
    let run_id = add_run(&db, &version, "2026-10-15T03:00:00Z", RunState::Running).await;
    db.create_instances(
        &run_id,
        &[(extract(), TaskInstanceState::Queued)],
        ts("2026-10-15T03:00:01Z"),
    )
    .await
    .unwrap();
    let Database::Sqlite(pool) = &db else {
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
