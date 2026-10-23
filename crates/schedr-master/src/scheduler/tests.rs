use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_model::Dag;
use serde_json::{Value as Json, json};
use sqlx::AssertSqlSafe;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

/// A daily 03:00 UTC DAG from 2026-10-01, changed by `changes`.
fn dag(changes: &Json) -> Dag {
    let mut dag = json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC", "catchup": false, "start": "2026-10-01T00:00:00Z"},
        "initial_since": null,
        "max_active_runs": 1, "tasks": [], "edges": []
    });
    for (key, value) in changes.as_object().unwrap() {
        dag[key] = value.clone();
    }
    serde_json::from_value(dag).unwrap()
}

/// A scheduler over a fresh database holding `dag` (paused, as new DAGs are).
async fn scheduler_with(dag: Dag) -> (Scheduler, Database) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts("2026-09-15T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-09-15T00:00:00Z"))
        .await
        .unwrap();
    (Scheduler::new(db.clone()), db)
}

/// Runs test SQL; the statements are fixed strings written in this file, so they are safe.
async fn execute(db: &Database, query: &str) {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query(AssertSqlSafe(query.to_owned()))
        .execute(pool)
        .await
        .unwrap();
}

fn planned(pass: &Pass) -> Vec<String> {
    pass.created
        .iter()
        .map(|r| r.times.planned_for.to_string())
        .collect()
}

#[tokio::test]
async fn a_paused_dag_gets_no_scheduled_runs() {
    let (scheduler, _db) = scheduler_with(dag(&json!({}))).await;
    let pass = scheduler.pass(ts("2026-10-21T08:00:00Z")).await.unwrap();
    assert_eq!(planned(&pass), Vec::<String>::new());
}

#[tokio::test]
async fn an_unpaused_dag_gets_its_most_recent_fire_time_once() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    let now = ts("2026-10-21T08:00:00Z");
    let pass = scheduler.pass(now).await.unwrap();
    assert_eq!(planned(&pass), ["2026-10-21T03:00:00Z"]);
    let run = &pass.created[0];
    assert_eq!(run.kind, RunKind::Scheduled);
    assert_eq!(run.times.since, ts("2026-10-20T03:00:00Z"));
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_fire_time_at_the_limit_is_missed_and_the_latest_runs_when_a_place_frees() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    scheduler.pass(ts("2026-10-20T03:00:00Z")).await.unwrap();
    // Still running a day and a half later: 10-21 is missed.
    assert_eq!(
        planned(&scheduler.pass(ts("2026-10-21T15:00:00Z")).await.unwrap()),
        Vec::<String>::new()
    );
    execute(&db, "UPDATE runs SET state = 'success'").await;
    assert_eq!(
        planned(&scheduler.pass(ts("2026-10-22T04:00:00Z")).await.unwrap()),
        ["2026-10-22T03:00:00Z"]
    );
}

#[tokio::test]
async fn with_catchup_missed_fire_times_run_in_order() {
    let (scheduler, db) = scheduler_with(dag(&json!({"trigger": {
            "kind": "cron", "expression": "0 3 * * *", "timezone": "UTC", "catchup": true,
            "start": "2026-10-01T00:00:00Z"
        }, "max_active_runs": 2})))
    .await;
    db.set_paused(&sync(), false).await.unwrap();
    let now = ts("2026-10-04T08:00:00Z");
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()),
        ["2026-10-01T03:00:00Z", "2026-10-02T03:00:00Z"]
    );
    execute(&db, "UPDATE runs SET state = 'success'").await;
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()),
        ["2026-10-03T03:00:00Z", "2026-10-04T03:00:00Z"]
    );
}

#[tokio::test]
async fn manual_runs_of_a_paused_dag_run_and_wait_for_a_place() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    let now = ts("2026-10-21T08:00:00Z");
    let first = scheduler.trigger(&sync(), None, "anna", now).await.unwrap();
    assert_eq!(first.kind, RunKind::Manual);
    assert_eq!(first.times.planned_for, now);
    assert_eq!(first.times.since, ts("2026-10-21T03:00:00Z"));
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);

    let later = ts("2026-10-21T09:00:00Z");
    let second = scheduler
        .trigger(&sync(), None, "anna", later)
        .await
        .unwrap();
    assert_eq!(second.times.since, now, "since the nearest earlier run");
    assert_eq!(db.queued_runs(&sync()).await.unwrap().len(), 1);

    execute(
        &db,
        "UPDATE runs SET state = 'success' WHERE state = 'running'",
    )
    .await;
    let pass = scheduler.pass(later).await.unwrap();
    assert_eq!(pass.started, [second.run_id]);
    assert_eq!(db.queued_runs(&sync()).await.unwrap(), []);
}

#[tokio::test]
async fn scheduled_runs_take_a_freed_place_before_queued_ones() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    scheduler.pass(ts("2026-10-20T04:00:00Z")).await.unwrap();
    scheduler
        .trigger(
            &sync(),
            Some(ts("2026-10-18T03:00:00Z")),
            "anna",
            ts("2026-10-20T05:00:00Z"),
        )
        .await
        .unwrap();
    execute(
        &db,
        "UPDATE runs SET state = 'success' WHERE state = 'running'",
    )
    .await;
    let pass = scheduler.pass(ts("2026-10-21T04:00:00Z")).await.unwrap();
    assert_eq!(planned(&pass), ["2026-10-21T03:00:00Z"]);
    assert_eq!(pass.started, Vec::<String>::new());
    assert_eq!(db.queued_runs(&sync()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_manual_run_for_a_fire_time_gets_its_period() {
    let (scheduler, _db) = scheduler_with(dag(&json!({}))).await;
    let run = scheduler
        .trigger(
            &sync(),
            Some(ts("2026-10-15T03:00:00Z")),
            "anna",
            ts("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(run.times.since, ts("2026-10-14T03:00:00Z"));
}

#[tokio::test]
async fn triggers_that_cant_run_are_errors() {
    let (scheduler, _db) = scheduler_with(dag(&json!({}))).await;
    let now = ts("2026-10-21T08:00:00Z");
    let error = scheduler
        .trigger(&sync(), Some(ts("2026-10-22T00:00:00Z")), "anna", now)
        .await
        .unwrap_err();
    assert!(matches!(error, SchedulerError::ManualRun(_)), "{error}");
    let other = DagId::new("other").unwrap();
    let error = scheduler
        .trigger(&other, None, "anna", now)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "DAG `other` can't run: it doesn't exist, was removed or is in conflict"
    );
}

#[tokio::test]
async fn a_dag_whose_schedule_cant_be_evaluated_is_skipped() {
    let broken = json!({"kind": "cron", "expression": "0 3 * * *", "timezone": "Mars/Base", "catchup": false});
    let (scheduler, db) = scheduler_with(dag(&json!({"trigger": broken}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    let pass = scheduler.pass(ts("2026-10-21T08:00:00Z")).await.unwrap();
    assert_eq!(pass.created, []);
    assert_eq!(
        pass.skipped[0].to_string(),
        "DAG `sync`: unknown time zone `Mars/Base`"
    );
    let error = scheduler
        .trigger(&sync(), None, "anna", ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap_err();
    assert!(matches!(error, SchedulerError::FireTimes { .. }), "{error}");
}

#[tokio::test]
async fn a_manual_run_triggered_again_with_its_id_is_the_same_run() {
    // B7: `trigger_run` passes its command's ID, so applying it again creates nothing new.
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    let now = ts("2026-10-21T08:00:00Z");
    let id = "01a16884-4f28-7563-9d35-7d0555a017f1";
    let first = scheduler
        .trigger_as(Some(id), &sync(), None, "anna", now)
        .await
        .unwrap();
    let again = scheduler
        .trigger_as(Some(id), &sync(), None, "anna", now)
        .await
        .unwrap();
    assert_eq!((first.run_id.as_str(), again.run_id.as_str()), (id, id));
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);
}
