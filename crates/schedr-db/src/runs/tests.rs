use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn dag(id: &str) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": id, "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap()
}

/// A migrated database with DAG `sync` stored; returns it with the version's ID.
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
        dag: Box::new(dag("sync")),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-10-01T00:00:00Z"))
        .await
        .unwrap();
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version_id)
}

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

fn run<'a>(
    dag_id: &'a DagId,
    version: &'a str,
    kind: RunKind,
    state: RunState,
    planned_for: &str,
) -> NewRun<'a> {
    NewRun {
        dag_id,
        dag_version_id: version,
        kind,
        state,
        planned_for: ts(planned_for),
        since: ts("2026-10-14T03:00:00Z"),
        triggered_at: ts(planned_for),
        triggered_by: "scheduler",
    }
}

#[tokio::test]
async fn active_dags_have_a_version_and_their_first_seen_time() {
    let (db, version_id) = with_dag().await;
    let active = db.active_dags().await.unwrap();
    assert_eq!(active.len(), 1);
    assert!(active[0].paused);
    assert_eq!(active[0].first_seen, ts("2026-10-01T00:00:00Z"));
    assert_eq!(active[0].version_id, version_id);
    assert_eq!(active[0].dag, dag("sync"));
    db.apply_dag_changes(
        &[DagChange::Remove { dag_id: sync() }],
        ts("2026-10-02T00:00:00Z"),
    )
    .await
    .unwrap();
    assert_eq!(db.active_dags().await.unwrap(), []);
}

#[tokio::test]
async fn a_scheduled_run_is_created_once_per_planned_for() {
    let (db, version) = with_dag().await;
    let id = sync();
    let scheduled = run(
        &id,
        &version,
        RunKind::Scheduled,
        RunState::Running,
        "2026-10-15T03:00:00Z",
    );
    assert!(db.insert_run(&scheduled).await.unwrap().is_some());
    assert_eq!(db.insert_run(&scheduled).await.unwrap(), None);
    let manual = run(
        &id,
        &version,
        RunKind::Manual,
        RunState::Queued,
        "2026-10-15T03:00:00Z",
    );
    assert!(db.insert_run(&manual).await.unwrap().is_some());
    assert!(db.insert_run(&manual).await.unwrap().is_some());
}

#[tokio::test]
async fn the_scheduler_bookkeeping_counts_only_scheduled_runs() {
    let (db, version) = with_dag().await;
    let id = sync();
    assert_eq!(db.last_scheduled_for(&id).await.unwrap(), None);
    for (kind, at) in [
        (RunKind::Scheduled, "2026-10-14T03:00:00Z"),
        (RunKind::Scheduled, "2026-10-15T03:00:00Z"),
        (RunKind::Manual, "2026-10-20T12:00:00Z"),
    ] {
        db.insert_run(&run(&id, &version, kind, RunState::Running, at))
            .await
            .unwrap();
    }
    assert_eq!(
        db.last_scheduled_for(&id).await.unwrap(),
        Some(ts("2026-10-15T03:00:00Z"))
    );
    assert_eq!(
        db.nearest_earlier_run(&id, ts("2026-10-21T00:00:00Z"))
            .await
            .unwrap(),
        Some(ts("2026-10-20T12:00:00Z"))
    );
    assert_eq!(
        db.nearest_earlier_run(&id, ts("2026-10-15T03:00:00Z"))
            .await
            .unwrap(),
        Some(ts("2026-10-14T03:00:00Z"))
    );
    assert_eq!(
        db.nearest_earlier_run(&id, ts("2026-10-14T03:00:00Z"))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn queued_runs_wait_until_started() {
    let (db, version) = with_dag().await;
    let id = sync();
    let later = db
        .insert_run(&run(
            &id,
            &version,
            RunKind::Backfill,
            RunState::Queued,
            "2026-10-16T03:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let earlier = db
        .insert_run(&run(
            &id,
            &version,
            RunKind::Manual,
            RunState::Queued,
            "2026-10-15T03:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    db.insert_run(&run(
        &id,
        &version,
        RunKind::Scheduled,
        RunState::Running,
        "2026-10-14T03:00:00Z",
    ))
    .await
    .unwrap();
    assert_eq!(db.running_count(&id).await.unwrap(), 1);
    let queued = db.queued_runs(&id).await.unwrap();
    assert_eq!(
        queued,
        [
            QueuedRun {
                id: earlier.clone(),
                kind: RunKind::Manual,
                triggered_at: ts("2026-10-15T03:00:00Z"),
            },
            QueuedRun {
                id: later,
                kind: RunKind::Backfill,
                triggered_at: ts("2026-10-16T03:00:00Z"),
            },
        ]
    );
    db.start_run(&earlier, ts("2026-10-21T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(db.running_count(&id).await.unwrap(), 2);
    assert_eq!(db.queued_runs(&id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn stored_values_that_dont_read_are_errors() {
    let (db, version) = with_dag().await;
    let id = sync();
    db.insert_run(&run(
        &id,
        &version,
        RunKind::Manual,
        RunState::Queued,
        "2026-10-15T03:00:00Z",
    ))
    .await
    .unwrap();
    let crate::Database::Sqlite(pool) = &db else {
        panic!("expected SQLite")
    };
    sqlx::query("UPDATE runs SET triggered_at = 'yesterday'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.queued_runs(&id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in runs.triggered_at: "),
        "{error}"
    );
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET kind = 'sometimes', triggered_at = planned_for")
        .execute(pool)
        .await
        .unwrap();
    let error = db.queued_runs(&id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in runs.kind: "),
        "{error}"
    );
    sqlx::query("UPDATE runs SET kind = 'scheduled', planned_for = 'soon'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.last_scheduled_for(&id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in runs.planned_for: "),
        "{error}"
    );
}

#[tokio::test]
async fn runs_in_progress_are_the_queued_and_running_ones() {
    let (db, version) = with_dag().await;
    let id = sync();
    assert_eq!(
        db.runs_in_progress(&id).await.unwrap(),
        Vec::<String>::new()
    );
    let running = db
        .insert_run(&run(
            &id,
            &version,
            RunKind::Scheduled,
            RunState::Running,
            "2026-10-14T03:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    let queued = db
        .insert_run(&run(
            &id,
            &version,
            RunKind::Manual,
            RunState::Queued,
            "2026-10-15T03:00:00Z",
        ))
        .await
        .unwrap()
        .unwrap();
    db.insert_run(&run(
        &id,
        &version,
        RunKind::Manual,
        RunState::Success,
        "2026-10-16T03:00:00Z",
    ))
    .await
    .unwrap();
    assert_eq!(db.runs_in_progress(&id).await.unwrap(), [running, queued]);
}
