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
    assert_eq!(*active[0].dag, dag("sync"));
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
async fn the_overview_has_every_dags_running_last_scheduled_and_queued_runs() {
    let (db, version) = with_dag().await;
    let id = sync();
    let other = DagId::new("other").unwrap();
    assert_eq!(db.run_overview().await.unwrap(), RunOverview::default());
    for (kind, state, at) in [
        (
            RunKind::Scheduled,
            RunState::Success,
            "2026-10-14T03:00:00Z",
        ),
        (
            RunKind::Scheduled,
            RunState::Running,
            "2026-10-15T03:00:00Z",
        ),
        (RunKind::Manual, RunState::Running, "2026-10-20T12:00:00Z"),
        (RunKind::Manual, RunState::Queued, "2026-10-21T12:00:00Z"),
        (RunKind::Event, RunState::Queued, "2026-10-21T11:00:00Z"),
    ] {
        db.insert_run(&run(&id, &version, kind, state, at))
            .await
            .unwrap();
    }
    let overview = db.run_overview().await.unwrap();
    assert_eq!(overview.running(&id), 2);
    assert_eq!(
        overview.last_scheduled(&id),
        Some(ts("2026-10-15T03:00:00Z"))
    );
    let queued: Vec<(RunKind, Timestamp)> = overview
        .queued(&id)
        .iter()
        .map(|r| (r.kind, r.triggered_at))
        .collect();
    assert_eq!(
        queued,
        [
            (RunKind::Event, ts("2026-10-21T11:00:00Z")),
            (RunKind::Manual, ts("2026-10-21T12:00:00Z"))
        ]
    );
    // A DAG without runs: nothing running, never scheduled, nothing queued.
    assert_eq!(overview.running(&other), 0);
    assert_eq!(overview.last_scheduled(&other), None);
    assert_eq!(overview.queued(&other), []);
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
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
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

#[tokio::test]
async fn a_run_inserted_with_its_id_is_stored_once() {
    let (db, version) = with_dag().await;
    let id = "01a16884-4f28-7563-9d35-7d0555a017f1";
    let dag = sync();
    let manual = run(
        &dag,
        &version,
        RunKind::Manual,
        RunState::Running,
        "2026-10-21T08:00:00Z",
    );
    assert_eq!(
        db.insert_run_as(id, &manual).await.unwrap().as_deref(),
        Some(id)
    );
    assert_eq!(db.insert_run_as(id, &manual).await.unwrap(), None);
    assert_eq!(db.running_count(&dag).await.unwrap(), 1);
}

/// `with_dag`'s database with a second DAG, `other`, stored too; its version's ID.
async fn add_other(db: &Database) -> String {
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h2",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts("2026-10-01T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag("other")),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-10-01T00:00:00Z"))
        .await
        .unwrap();
    let active = db.active_dags().await.unwrap();
    active
        .into_iter()
        .find(|a| a.dag.id.as_str() == "other")
        .unwrap()
        .version_id
}

#[tokio::test]
async fn the_overview_and_active_dags_of_some_dags_have_only_theirs() {
    let (db, version) = with_dag().await;
    let other_version = add_other(&db).await;
    let (id, other) = (sync(), DagId::new("other").unwrap());
    for (dag_id, version, state, at) in [
        (&id, &version, RunState::Running, "2026-10-15T03:00:00Z"),
        (&id, &version, RunState::Queued, "2026-10-16T03:00:00Z"),
        (
            &other,
            &other_version,
            RunState::Running,
            "2026-10-15T04:00:00Z",
        ),
    ] {
        db.insert_run(&run(dag_id, version, RunKind::Scheduled, state, at))
            .await
            .unwrap();
    }
    let all = db.run_overview().await.unwrap();
    assert_eq!(db.run_overview_of(&[&id, &other]).await.unwrap(), all);
    let one = db.run_overview_of(&[&id]).await.unwrap();
    assert_eq!(
        (one.running(&id), one.queued(&id).len(), one.running(&other)),
        (1, 1, 0)
    );
    assert_eq!(one.last_scheduled(&other), None);
    // No DAGs: nothing read, not everything.
    assert_eq!(
        db.run_overview_of(&[]).await.unwrap(),
        RunOverview::default()
    );

    let ids = |active: Vec<ActiveDag>| -> Vec<String> {
        active.into_iter().map(|a| a.dag.id.to_string()).collect()
    };
    assert_eq!(ids(db.active_dags_of(&[&other]).await.unwrap()), ["other"]);
    assert_eq!(
        ids(db.active_dags_of(&[&other, &id]).await.unwrap()),
        ["other", "sync"]
    );
    assert_eq!(
        ids(db.active_dags_of(&[]).await.unwrap()),
        Vec::<String>::new()
    );

    // More IDs than one query names: every chunk is read.
    let many: Vec<DagId> = (0..DAGS_PER_QUERY + 10)
        .map(|n| DagId::new(format!("missing{n}")).unwrap())
        .chain([id.clone()])
        .collect();
    let refs: Vec<&DagId> = many.iter().collect();
    assert_eq!(db.run_overview_of(&refs).await.unwrap().running(&id), 1);
    assert_eq!(ids(db.active_dags_of(&refs).await.unwrap()), ["sync"]);
}

#[test]
fn a_hold_is_explained_as_the_files_failure_with_what_to_do() {
    let held = |exception: &str| {
        Held {
            path: "etl/all.py".to_owned(),
            exception: exception.to_owned(),
            message: "the details".to_owned(),
        }
        .reason("sync")
    };
    let too_large = held("ParseResultTooLarge");
    for part in [
        "DAG `sync` is held, no runs are created and it can't be triggered",
        "the parse result of its file `etl/all.py` (the JSON of all the file's DAGs together, \
         not of `sync` alone) is too large",
        "move large DAGs to files of their own",
        "[master] grpc_max_message_size",
    ] {
        assert!(too_large.contains(part), "{part}\n{too_large}");
    }
    let invalid = held("InvalidDag");
    assert!(
        invalid.contains("Every DAG of the file is held while one is invalid"),
        "{invalid}"
    );
    assert!(
        invalid.contains("move it to a file of its own"),
        "{invalid}"
    );
    assert!(held("InvalidPath").contains("isn't inside its root"));
    assert!(
        held("SyntaxError").contains("fails to import (SyntaxError: the details)"),
        "{}",
        held("SyntaxError")
    );
}
