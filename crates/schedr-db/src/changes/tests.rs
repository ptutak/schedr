use pretty_assertions::assert_eq;
use rstest::rstest;
use schedr_model::{Dag, RunKind, RunState};
use serde_json::json;
use sqlx::AssertSqlSafe;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::runs::NewRun;

fn ts(text: &str) -> jiff::Timestamp {
    text.parse().unwrap()
}

fn sync() -> DagId {
    DagId::new("etl:a/b.py#sync").unwrap()
}

#[rstest]
#[case("run r1 sync", Some(Change::Run { run_id: "r1".to_owned(), dag_id: DagId::new("sync").unwrap() }))]
#[case("run r1 etl:a/b.py#sync", Some(Change::Run { run_id: "r1".to_owned(), dag_id: sync() }))]
#[case("dag sync", Some(Change::Dag { dag_id: DagId::new("sync").unwrap() }))]
#[case("workers", Some(Change::Workers))]
#[case("run r1", None)]
#[case("run r1 sync extra", None)]
#[case("dag", None)]
#[case("workers now", None)]
#[case("dag bad id", None)]
#[case("", None)]
fn changes_read_as_the_triggers_write_them(#[case] text: &str, #[case] change: Option<Change>) {
    assert_eq!(Change::parse(text), change);
}

/// A database with DAG `etl:a/b.py#sync` defined, and its version's ID.
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
    let dag: Dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": sync().as_str(), "location": {"root": "etl", "path": "a/b.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": [], "edges": []
    }))
    .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-10-01T00:00:00Z"))
        .await
        .unwrap();
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version)
}

async fn add_run(db: &Database, version: &str, planned_for: &str) -> String {
    db.insert_run(&NewRun {
        dag_id: &sync(),
        dag_version_id: version,
        kind: RunKind::Manual,
        state: RunState::Queued,
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
async fn sqlite_changes_are_read_from_now_on_each_once() {
    let (db, version) = with_dag().await;
    // From now on: the DAG defined before isn't reported.
    let mut changes = db.changes().await.unwrap().expect("SQLite is polled");
    let first = add_run(&db, &version, "2026-10-14T03:00:00Z").await;
    db.start_run(&first, ts("2026-10-14T03:00:01Z"))
        .await
        .unwrap();
    db.set_paused(&sync(), true).await.unwrap();
    let mut seen = changes.next().await.unwrap();
    seen.sort();
    assert_eq!(
        seen,
        [
            Change::Run {
                run_id: first.clone(),
                dag_id: sync()
            },
            Change::Dag { dag_id: sync() },
        ]
    );
    // Nothing new: it waits.
    let waited = tokio::time::timeout(POLL_INTERVAL * 2, changes.next()).await;
    assert!(waited.is_err(), "{waited:?}");
}

#[tokio::test]
async fn rows_pruned_unread_are_reported_missed() {
    let (db, _) = with_dag().await;
    let mut changes = db.changes().await.unwrap().unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    // 2,100 writes: the pruning drops rows the reader hasn't read.
    for _ in 0..21 {
        sqlx::query(AssertSqlSafe(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100) \
             INSERT INTO changes (change) SELECT 'workers' FROM n"
                .to_owned(),
        ))
        .execute(pool)
        .await
        .unwrap();
    }
    assert_eq!(
        changes.next().await.unwrap(),
        [Change::Workers, Change::Missed]
    );
    let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM changes")
        .fetch_one(pool)
        .await
        .unwrap();
    assert!(kept <= 2_000, "{kept}");
}

#[tokio::test]
async fn rows_pruned_unread_from_an_empty_table_are_reported_missed_too() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    // Nothing written yet: the cursor is 0.
    let mut changes = db.changes().await.unwrap().unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    for _ in 0..20 {
        sqlx::query(AssertSqlSafe(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100) \
             INSERT INTO changes (change) SELECT 'workers' FROM n"
                .to_owned(),
        ))
        .execute(pool)
        .await
        .unwrap();
    }
    assert_eq!(
        changes.next().await.unwrap(),
        [Change::Workers, Change::Missed]
    );
}

#[tokio::test]
async fn the_switch_stops_and_starts_the_sqlite_changes() {
    let (db, version) = with_dag().await;
    assert!(
        db.change_notifications().await.unwrap(),
        "on after the migration"
    );
    // Already on: nothing to change.
    assert!(!db.set_change_notifications(true).await.unwrap());
    assert!(db.set_change_notifications(false).await.unwrap());
    assert!(!db.change_notifications().await.unwrap());
    let rows = |db: &Database| {
        let db = db.clone();
        async move {
            let crate::database::Pool::Sqlite(pool) = db.pool() else {
                panic!("expected SQLite")
            };
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM changes")
                .fetch_one(pool)
                .await
                .unwrap()
        }
    };
    let before = rows(&db).await;
    add_run(&db, &version, "2026-10-14T03:00:00Z").await;
    assert_eq!(rows(&db).await, before, "off: nothing recorded");
    assert!(db.set_change_notifications(true).await.unwrap());
    add_run(&db, &version, "2026-10-15T03:00:00Z").await;
    assert_eq!(rows(&db).await, before + 1, "on again: recorded");
}
