use pretty_assertions::assert_eq;
use schedr_model::{Dag, RunKind};
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::runs::NewRun;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

/// A migrated database with DAG `sync` stored; returns it with the version's ID.
async fn with_dag() -> (Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "1.0.0",
            worker_id: "w1",
            parsed_at: ts("2026-10-01T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let dag: Dag = serde_json::from_value(json!({
        "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC",
                    "catchup": false},
        "initial_since": null, "max_active_runs": 1, "tasks": [], "edges": []
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
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version_id)
}

const BACKFILL: &str = "01a1c0a0-0000-7000-8000-000000000001";

fn backfill(created_runs: u32) -> NewBackfill<'static> {
    static SYNC: std::sync::LazyLock<DagId> = std::sync::LazyLock::new(sync);
    NewBackfill {
        id: BACKFILL,
        dag_id: &SYNC,
        range_from: ts("2026-10-01T00:00:00Z"),
        range_to: ts("2026-10-04T00:00:00Z"),
        all_fire_times: false,
        requested_by: "anna",
        created_at: ts("2026-10-20T00:00:00Z"),
        fire_times: 3,
        created_runs,
    }
}

async fn insert(
    db: &Database,
    version: &str,
    (kind, state): (RunKind, RunState),
    planned_for: &str,
    backfill_id: Option<&str>,
) -> String {
    let dag_id = sync();
    db.insert_run(&NewRun {
        params: &crate::runs::NO_PARAMS,
        event: None,
        dag_id: &dag_id,
        dag_version_id: version,
        kind,
        state,
        planned_for: ts(planned_for),
        since: ts(planned_for),
        triggered_at: ts("2026-10-20T00:00:00Z"),
        triggered_by: "anna",
        parent_run_id: None,
        backfill_id,
    })
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn a_backfill_is_stored_once_with_its_runs_counted_by_state() {
    let (db, version) = with_dag().await;
    assert!(db.insert_backfill(&backfill(2)).await.unwrap());
    assert!(
        !db.insert_backfill(&backfill(2)).await.unwrap(),
        "applied again: there already"
    );
    let first = insert(
        &db,
        &version,
        (RunKind::Backfill, RunState::Queued),
        "2026-10-02T03:00:00Z",
        Some(BACKFILL),
    )
    .await;
    insert(
        &db,
        &version,
        (RunKind::Backfill, RunState::Queued),
        "2026-10-03T03:00:00Z",
        Some(BACKFILL),
    )
    .await;
    db.finish_run(&first, RunState::Success, ts("2026-10-20T01:00:00Z"))
        .await
        .unwrap();
    let stored = db.backfill(BACKFILL).await.unwrap().unwrap();
    assert_eq!(
        (
            stored.fire_times,
            stored.created_runs,
            stored.requested_by.as_str()
        ),
        (3, 2, "anna")
    );
    let mut runs = stored.runs.clone();
    runs.sort_by_key(|(state, _)| state.to_string());
    assert_eq!(runs, [(RunState::Queued, 1), (RunState::Success, 1)]);
    assert!(stored.unfinished());
    assert_eq!(db.dag_backfills(&sync(), 10).await.unwrap(), [stored]);
    assert_eq!(
        db.backfill("01a1c0a0-0000-7000-8000-00000000ffff")
            .await
            .unwrap(),
        None
    );
    // Its runs know their place: oldest first.
    let place = db
        .run_record(&first)
        .await
        .unwrap()
        .unwrap()
        .backfill
        .unwrap();
    assert_eq!(
        (place.id.as_str(), place.index, place.total),
        (BACKFILL, 1, 2)
    );
    assert_eq!(
        db.unfinished_backfill_runs(BACKFILL).await.unwrap().len(),
        1
    );
    // A newer backfill's runs are counted as its own, in the one query of the list.
    let newer = "01a1c0a0-0000-7000-8000-000000000002";
    db.insert_backfill(&NewBackfill {
        id: newer,
        created_at: ts("2026-10-21T00:00:00Z"),
        ..backfill(1)
    })
    .await
    .unwrap();
    insert(
        &db,
        &version,
        (RunKind::Backfill, RunState::Running),
        "2026-10-04T03:00:00Z",
        Some(newer),
    )
    .await;
    let listed: Vec<(String, Vec<(RunState, u32)>)> = db
        .dag_backfills(&sync(), 10)
        .await
        .unwrap()
        .into_iter()
        .map(|b| {
            let mut runs = b.runs;
            runs.sort_by_key(|(state, _)| state.to_string());
            (b.id, runs)
        })
        .collect();
    assert_eq!(
        listed,
        [
            (newer.to_owned(), vec![(RunState::Running, 1)]),
            (
                BACKFILL.to_owned(),
                vec![(RunState::Queued, 1), (RunState::Success, 1)]
            ),
        ]
    );
    assert_eq!(db.dag_backfills(&sync(), 1).await.unwrap().len(), 1);
}

#[tokio::test]
async fn the_fire_times_covered_are_those_with_a_success_or_an_unfinished_run() {
    let (db, version) = with_dag().await;
    for (state, at) in [
        (RunState::Running, "2026-10-01T03:00:00Z"),
        (RunState::Queued, "2026-10-02T03:00:00Z"),
        (RunState::Running, "2026-10-03T03:00:00Z"),
        (RunState::Running, "2026-10-04T03:00:00Z"),
        (RunState::Running, "2026-10-09T03:00:00Z"),
    ] {
        insert(&db, &version, (RunKind::Manual, state), at, None).await;
    }
    let ended = [
        ("2026-10-03T03:00:00Z", RunState::Success),
        ("2026-10-04T03:00:00Z", RunState::Failed),
    ];
    for run in db.dag_runs(&sync(), None, 50).await.unwrap() {
        if let Some((_, state)) = ended.iter().find(|(at, _)| ts(at) == run.planned_for) {
            db.finish_run(&run.id, *state, ts("2026-10-20T01:00:00Z"))
                .await
                .unwrap();
        }
    }
    let covered = db
        .covered_times(
            &sync(),
            ts("2026-10-01T00:00:00Z"),
            ts("2026-10-05T00:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(
        covered.into_iter().collect::<Vec<_>>(),
        [
            ts("2026-10-01T03:00:00Z"),
            ts("2026-10-02T03:00:00Z"),
            ts("2026-10-03T03:00:00Z"),
        ],
        "running, queued, succeeded; not a failed one, nor one outside the range"
    );
}
