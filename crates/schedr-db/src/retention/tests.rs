use pretty_assertions::assert_eq;
use schedr_model::Dag;
use serde_json::json;
use sqlx::AssertSqlSafe;

use super::*;
use crate::dags::{DagChange, NewParse};

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
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
            parsed_at: ts("2026-01-01T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let dag: Dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC",
                    "catchup": true},
        "initial_since": null, "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version_id)
}

async fn exec(db: &Database, statement: String) {
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query(AssertSqlSafe(statement))
        .execute(pool)
        .await
        .unwrap();
}

async fn count(db: &Database, query: &str) -> i64 {
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query_scalar(AssertSqlSafe(query.to_owned()))
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Run `id` of `sync`: `kind`, `state`, planned for and ended at the given days of 2026-01
/// (`None`: not ended), started by `parent`.
async fn run(
    db: &Database,
    version: &str,
    (id, kind, state): (&str, &str, &str),
    planned_day: u8,
    ended_day: Option<u8>,
    parent: Option<&str>,
) {
    let day = |d: u8| db_time(ts(&format!("2026-01-{d:02}T03:00:00Z")));
    let ended = ended_day.map_or("NULL".to_owned(), |d| format!("'{}'", day(d)));
    let parent = parent.map_or("NULL".to_owned(), |p| format!("'{p}'"));
    exec(
        db,
        format!(
            "INSERT INTO runs (id, dag_id, dag_version_id, kind, state, planned_for, since, \
             triggered_at, triggered_by, parent_run_id, ended_at) VALUES ('{id}', 'sync', \
             '{version}', '{kind}', '{state}', '{p}', '{p}', '{p}', 'test', {parent}, {ended})",
            p = day(planned_day),
        ),
    )
    .await;
    // A task with an output, so its run's deletion reaches every table under it.
    exec(
        db,
        format!(
            "INSERT INTO task_instances (id, run_id, task_id, state, updated_at) VALUES \
             ('{id}-t', '{id}', 'load', 'success', '{p}')",
            p = day(planned_day),
        ),
    )
    .await;
    exec(
        db,
        format!(
            "INSERT INTO outputs (task_instance_id, outcome, value, size_bytes) VALUES \
             ('{id}-t', 'SUCCESS', '42', 2)"
        ),
    )
    .await;
}

async fn runs_left(db: &Database) -> Vec<String> {
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query_scalar("SELECT id FROM runs ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn old_finished_runs_go_but_those_their_dag_still_needs_and_a_child_loses_its_link() {
    let (db, version) = with_dag().await;
    run(
        &db,
        &version,
        ("a", "scheduled", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    // The newest scheduled run: the scheduler's chain.
    run(
        &db,
        &version,
        ("b", "scheduled", "failed"),
        2,
        Some(2),
        None,
    )
    .await;
    // The newest run: a manual run's `since`.
    run(
        &db,
        &version,
        ("c", "manual", "cancelled"),
        3,
        Some(3),
        None,
    )
    .await;
    // Ended before `b`, which is the last to end `success` or `failed`.
    run(
        &db,
        &version,
        ("d", "backfill", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    // Started by `a`, ended after the cut.
    run(
        &db,
        &version,
        ("e", "chained", "success"),
        1,
        Some(20),
        Some("a"),
    )
    .await;
    // Not finished, however old.
    run(&db, &version, ("f", "manual", "running"), 1, None, None).await;
    let cut = ts("2026-01-10T00:00:00Z");

    // One at a time: the oldest first.
    assert_eq!(db.delete_old_runs(cut, 1).await.unwrap(), 1);
    assert_eq!(db.delete_old_runs(cut, 100).await.unwrap(), 1);
    assert_eq!(db.delete_old_runs(cut, 100).await.unwrap(), 0);
    assert_eq!(runs_left(&db).await, ["b", "c", "e", "f"]);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM runs WHERE parent_run_id IS NOT NULL"
        )
        .await,
        0
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM task_instances").await, 4);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM outputs").await, 4);
}

#[tokio::test]
async fn a_removed_dag_keeps_no_run_and_goes_with_its_last() {
    let (db, version) = with_dag().await;
    run(
        &db,
        &version,
        ("a", "scheduled", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    exec(
        &db,
        "INSERT INTO events (dag_id, key, data, received_at) VALUES ('sync', 'k', '{}', \
         '2026-01-01T00:00:00.000000Z')"
            .to_owned(),
    )
    .await;
    let cut = ts("2026-01-10T00:00:00Z");
    // Not removed yet: its only run is kept, and so is the DAG.
    assert_eq!(db.delete_old_runs(cut, 100).await.unwrap(), 0);
    assert_eq!(db.delete_removed_dags(cut).await.unwrap(), 0);

    exec(
        &db,
        format!(
            "UPDATE dags SET removed_at = '{}'",
            db_time(ts("2026-01-05T00:00:00Z"))
        ),
    )
    .await;
    // Removed after the cut: not yet.
    assert_eq!(
        db.delete_removed_dags(ts("2026-01-04T00:00:00Z"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(db.delete_old_runs(cut, 100).await.unwrap(), 1);
    assert_eq!(db.delete_removed_dags(cut).await.unwrap(), 1);
    for table in ["dags", "dag_versions", "events", "runs"] {
        assert_eq!(
            count(&db, &format!("SELECT COUNT(*) FROM {table}")).await,
            0,
            "{table}"
        );
    }
}

#[tokio::test]
async fn old_outputs_go_their_runs_and_tasks_stay_marked() {
    let (db, version) = with_dag().await;
    run(
        &db,
        &version,
        ("a", "scheduled", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    run(
        &db,
        &version,
        ("b", "scheduled", "success"),
        2,
        Some(2),
        None,
    )
    .await;
    let cut = ts("2026-01-10T00:00:00Z");
    let now = ts("2026-01-11T00:00:00Z");
    assert_eq!(db.delete_old_outputs(cut, 100, now).await.unwrap(), 1);
    assert_eq!(
        db.delete_old_outputs(cut, 100, now).await.unwrap(),
        0,
        "marked once"
    );
    assert_eq!(runs_left(&db).await, ["a", "b"]);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM task_instances").await, 2);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM outputs WHERE task_instance_id = 'b-t'"
        )
        .await,
        1,
        "the newest run's stays"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM runs WHERE outputs_deleted_at IS NOT NULL"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn old_events_and_decided_commands_go_with_what_triggers_passed() {
    let (db, _) = with_dag().await;
    let old = db_time(ts("2026-01-01T00:00:00Z"));
    let new = db_time(ts("2026-01-20T00:00:00Z"));
    for (key, at) in [("old", &old), ("new", &new)] {
        exec(
            &db,
            format!(
                "INSERT INTO events (dag_id, key, data, received_at) VALUES \
             ('sync', '{key}', '{{}}', '{at}')"
            ),
        )
        .await;
        exec(
            &db,
            format!(
                "INSERT INTO named_events (id, name, key, data, at, posted_at, \
             posted_by) VALUES ('{key}-id', 'packed', '{key}', '{{}}', '{at}', '{at}', 'anna')"
            ),
        )
        .await;
        exec(
            &db,
            format!("INSERT INTO trigger_passed (dag_id, item) VALUES ('sync', '{key}-id')"),
        )
        .await;
    }
    for (id, status, at) in [
        ("c1", "applied", &old),
        ("c2", "pending", &old),
        ("c3", "rejected", &new),
    ] {
        exec(
            &db,
            format!(
                "INSERT INTO commands (id, kind, payload, requested_by, requested_at, \
             status) VALUES ('{id}', 'pause_dag', '{{}}', 'anna', '{at}', '{status}')"
            ),
        )
        .await;
    }
    let cut = ts("2026-01-10T00:00:00Z");
    assert_eq!(db.delete_old_events(cut, 100).await.unwrap(), 2);
    assert_eq!(db.delete_old_commands(cut, 100).await.unwrap(), 1);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM events WHERE key = 'new'").await,
        1
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM named_events").await, 1);
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM trigger_passed WHERE item = 'new-id'"
        )
        .await,
        1
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM trigger_passed").await, 1);
    // A pending command stays, however old.
    assert_eq!(count(&db, "SELECT COUNT(*) FROM commands").await, 2);
}

#[tokio::test]
async fn a_run_something_unfinished_reads_is_kept_however_old() {
    let (db, version) = with_dag().await;
    for id in ["started", "handed", "triggered", "free"] {
        run(&db, &version, (id, "manual", "success"), 1, Some(1), None).await;
    }
    run(
        &db,
        &version,
        ("newest", "manual", "success"),
        2,
        Some(2),
        None,
    )
    .await;
    // Still running: a `TriggerDagOperator` waiting for `started`, a sensor handed `handed`.
    run(
        &db,
        &version,
        ("waiting", "manual", "running"),
        3,
        None,
        None,
    )
    .await;
    exec(
        &db,
        "INSERT INTO attempts (id, task_instance_id, try_number, assigned_at, \
         chained_run_id) VALUES ('at1', 'waiting-t', 1, '2026-01-03T00:00:00.000000Z', 'started')"
            .to_owned(),
    )
    .await;
    exec(
        &db,
        "UPDATE task_instances SET state = 'up_for_retry', handed_run = 'handed' \
         WHERE id = 'waiting-t'"
            .to_owned(),
    )
    .await;
    // A trigger handed `triggered`, not passed yet.
    exec(
        &db,
        "INSERT INTO triggers (dag_id, run_of_handed_run) VALUES ('sync', 'triggered')".to_owned(),
    )
    .await;
    let cut = ts("2026-01-10T00:00:00Z");
    while db.delete_old_runs(cut, 100).await.unwrap() > 0 {}
    assert_eq!(
        runs_left(&db).await,
        ["handed", "newest", "started", "triggered", "waiting"]
    );
    assert_eq!(
        db.delete_old_outputs(cut, 100, cut).await.unwrap(),
        0,
        "nor their outputs"
    );
}

#[tokio::test]
async fn runs_catchup_counts_as_done_ahead_of_the_newest_scheduled_one_are_kept() {
    let (db, version) = with_dag().await;
    run(
        &db,
        &version,
        ("a", "scheduled", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    // Backfilled ahead of the scheduler, while the DAG was paused: catchup skips their times.
    run(
        &db,
        &version,
        ("b", "backfill", "success"),
        2,
        Some(2),
        None,
    )
    .await;
    run(&db, &version, ("c", "backfill", "failed"), 3, Some(3), None).await;
    run(
        &db,
        &version,
        ("d", "backfill", "success"),
        4,
        Some(4),
        None,
    )
    .await;
    run(&db, &version, ("e", "manual", "success"), 5, Some(5), None).await;
    let cut = ts("2026-01-10T00:00:00Z");
    while db.delete_old_runs(cut, 100).await.unwrap() > 0 {}
    // `c` failed: catchup runs its time again, as it would have.
    assert_eq!(runs_left(&db).await, ["a", "b", "d", "e"]);
}

#[tokio::test]
async fn a_named_event_a_waiting_step_was_handed_is_kept() {
    let (db, version) = with_dag().await;
    run(&db, &version, ("w", "manual", "running"), 1, None, None).await;
    for key in ["handed", "free"] {
        exec(
            &db,
            format!(
                "INSERT INTO named_events (id, name, key, data, at, posted_at, \
             posted_by) VALUES ('{key}', 'packed', '{key}', '{{}}', \
             '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z', 'anna')"
            ),
        )
        .await;
    }
    exec(
        &db,
        "UPDATE task_instances SET state = 'queued', handed_run = 'handed'".to_owned(),
    )
    .await;
    assert_eq!(
        db.delete_old_events(ts("2026-01-10T00:00:00Z"), 100)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM named_events WHERE id = 'handed'").await,
        1
    );
}

#[tokio::test]
async fn a_version_nothing_uses_goes_one_a_run_uses_stays() {
    let (db, old) = with_dag().await;
    run(&db, &old, ("a", "scheduled", "success"), 1, Some(1), None).await;
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h2",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts("2026-01-02T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let dag: Dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "cron", "expression": "0 4 * * *", "timezone": "UTC",
                    "catchup": true},
        "initial_since": null, "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-01-02T00:00:00Z"))
        .await
        .unwrap();
    // The old version: run `a` uses it.
    assert_eq!(db.delete_unused_versions().await.unwrap(), 0);
    exec(&db, "DELETE FROM runs".to_owned()).await;
    assert_eq!(db.delete_unused_versions().await.unwrap(), 1);
    assert_eq!(db.delete_unused_versions().await.unwrap(), 0);
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM dag_versions").await,
        1,
        "the current one"
    );
}

#[tokio::test]
async fn a_dag_never_scheduled_keeps_its_backfills_not_its_manual_runs() {
    let (db, version) = with_dag().await;
    // Paused since it appeared, backfilled: unpaused with catchup, it starts from `start`.
    run(
        &db,
        &version,
        ("b1", "backfill", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    run(
        &db,
        &version,
        ("b2", "backfill", "failed"),
        2,
        Some(2),
        None,
    )
    .await;
    run(&db, &version, ("m1", "manual", "success"), 3, Some(3), None).await;
    run(&db, &version, ("m2", "manual", "success"), 4, Some(4), None).await;
    let cut = ts("2026-01-10T00:00:00Z");
    while db.delete_old_runs(cut, 100).await.unwrap() > 0 {}
    assert_eq!(runs_left(&db).await, ["b1", "m2"]);
}

#[tokio::test]
async fn a_dag_removed_lately_keeps_what_it_needs() {
    let (db, version) = with_dag().await;
    run(
        &db,
        &version,
        ("a", "scheduled", "success"),
        1,
        Some(1),
        None,
    )
    .await;
    run(
        &db,
        &version,
        ("b", "scheduled", "success"),
        2,
        Some(2),
        None,
    )
    .await;
    // Its file missing since yesterday (a bad sync): when it comes back, its chain is there.
    exec(
        &db,
        format!(
            "UPDATE dags SET removed_at = '{}'",
            db_time(ts("2026-01-09T00:00:00Z"))
        ),
    )
    .await;
    let cut = ts("2026-01-05T00:00:00Z");
    while db.delete_old_runs(cut, 100).await.unwrap() > 0 {}
    assert_eq!(runs_left(&db).await, ["b"]);
    assert_eq!(db.delete_removed_dags(cut).await.unwrap(), 0);
}
