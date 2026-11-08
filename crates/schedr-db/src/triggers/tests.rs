use pretty_assertions::assert_eq;
use schedr_model::{Dag, DagId, RunKind, RunState};
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn with_dag() -> (Database, DagId, String) {
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
        "produced_by": "0.1", "id": "ingest", "location": {"root": "etl", "path": "a.py"},
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
    (db, DagId::new("ingest").unwrap(), version)
}

fn run<'a>(dag: &'a DagId, version: &'a str, event: &'a RunEvent, at: &str) -> NewRun<'a> {
    NewRun {
        dag_id: dag,
        dag_version_id: version,
        kind: RunKind::Event,
        state: RunState::Queued,
        planned_for: ts(at),
        since: ts(at),
        triggered_at: ts(at),
        triggered_by: "trigger",
        params: &crate::runs::NO_PARAMS,
        event: Some(event),
    }
}

fn event(key: &str) -> RunEvent {
    RunEvent {
        key: key.to_owned(),
        data: json!({"path": key}),
        at: None,
    }
}

#[tokio::test]
async fn a_new_key_starts_a_run_a_repeat_doesnt_and_either_is_the_bookmark() {
    let (db, dag, version) = with_dag().await;
    let first = event("a");
    let id = "0190a0a0-0000-7000-8000-0000000000a1";
    let stored = db
        .store_event(
            id,
            &run(&dag, &version, &first, "2026-10-14T03:00:00Z"),
            &first,
        )
        .await
        .unwrap();
    assert_eq!(stored.as_deref(), Some(id));
    assert_eq!(db.run_event(id).await.unwrap(), Some(first.clone()));
    let triggers = db.triggers().await.unwrap();
    assert_eq!(triggers[0].bookmark.as_deref(), Some("a"));
    assert_eq!(
        triggers[0].last_planned_for,
        Some(ts("2026-10-14T03:00:00Z"))
    );

    let again = "0190a0a0-0000-7000-8000-0000000000a2";
    let stored = db
        .store_event(
            again,
            &run(&dag, &version, &first, "2026-10-14T04:00:00Z"),
            &first,
        )
        .await
        .unwrap();
    assert_eq!(stored, None, "a repeat starts no run");
    assert_eq!(db.run_event(again).await.unwrap(), None);
    let triggers = db.triggers().await.unwrap();
    assert_eq!(
        triggers[0].last_planned_for,
        Some(ts("2026-10-14T03:00:00Z")),
        "a repeat's moment isn't the next one's start"
    );
}

#[tokio::test]
async fn a_batch_starts_a_run_of_its_new_items_only_and_its_last_key_is_the_bookmark() {
    // B188: a window ending with a repeat lost the new items before it.
    let (db, dag, version) = with_dag().await;
    let items = |keys: &[&str]| -> Vec<(RunEvent, serde_json::Value)> {
        keys.iter()
            .map(|key| {
                (
                    event(key),
                    json!({"key": key, "data": {"path": key}, "at": null}),
                )
            })
            .collect()
    };
    let unused = event("unused");
    let new_run = run(&dag, &version, &unused, "2026-10-14T03:00:00Z");
    let store = async |id: &str, keys: &[&str]| {
        db.store_batch(id, &new_run, keys.last().unwrap(), &items(keys))
            .await
            .unwrap()
    };
    let first = "0190a0a0-0000-7000-8000-0000000000b1";
    assert_eq!(store(first, &["a", "b", "c"]).await.as_deref(), Some(first));
    let second = "0190a0a0-0000-7000-8000-0000000000b2";
    assert_eq!(store(second, &["d", "c"]).await.as_deref(), Some(second));
    let read = db.run_event(second).await.unwrap().unwrap();
    assert_eq!(read.key, "d", "keyed by its last new item");
    assert_eq!(
        read.data,
        json!([{"key": "d", "data": {"path": "d"}, "at": null}])
    );
    assert_eq!(
        db.triggers().await.unwrap()[0].bookmark.as_deref(),
        Some("c"),
        "the window's last key, so `after` goes on from it"
    );
    let third = "0190a0a0-0000-7000-8000-0000000000b3";
    assert_eq!(store(third, &["b", "d"]).await, None, "nothing new: no run");
    assert_eq!(db.run_event(third).await.unwrap(), None);
}

#[tokio::test]
async fn only_the_run_of_a_trigger_it_names_is_ended() {
    let (db, dag, version) = with_dag().await;
    db.trigger_assigned(
        dag.as_str(),
        "new",
        "w1",
        &version,
        "h1",
        ts("2026-10-14T02:00:00Z"),
    )
    .await
    .unwrap();
    let later = ts("2026-10-14T03:00:00Z");
    assert!(
        !db.trigger_ended(dag.as_str(), "old", Some("boom"), Some(later))
            .await
            .unwrap(),
        "a stale instance changes nothing"
    );
    assert_eq!(
        db.triggers().await.unwrap()[0].instance_id.as_deref(),
        Some("new")
    );
    assert!(
        db.trigger_ended(dag.as_str(), "new", Some("boom"), Some(later))
            .await
            .unwrap()
    );
    let now = &db.triggers().await.unwrap()[0];
    assert_eq!(now.instance_id, None);
    assert_eq!(now.failures, 1);
    assert_eq!(now.last_error.as_deref(), Some("boom"));
    assert_eq!(now.not_before, Some(later));
}

#[tokio::test]
async fn events_are_listed_newest_first_a_page_at_a_time_ties_included() {
    let (db, dag, version) = with_dag().await;
    let at = ts("2026-10-14T03:00:00Z");
    for (i, key) in ["a", "b", "c"].iter().enumerate() {
        let event = RunEvent {
            key: (*key).to_owned(),
            data: json!(1),
            at: None,
        };
        let params = serde_json::Value::Object(serde_json::Map::new());
        let run = NewRun {
            dag_id: &dag,
            dag_version_id: &version,
            kind: RunKind::Event,
            state: RunState::Queued,
            planned_for: at,
            since: at,
            triggered_at: at,
            triggered_by: "trigger",
            params: &params,
            event: Some(&event),
        };
        let id = format!("01a00000-0000-7000-8000-00000000000{i}");
        db.store_event(&id, &run, &event).await.unwrap();
    }
    let first = db.events(dag.as_str(), 2, None).await.unwrap();
    let keys: Vec<&str> = first.iter().map(|e| e.event.key.as_str()).collect();
    assert_eq!(keys, ["a", "b"]);
    let rest = db
        .events(dag.as_str(), 2, Some((first[1].received_at, "b")))
        .await
        .unwrap();
    let keys: Vec<&str> = rest.iter().map(|e| e.event.key.as_str()).collect();
    assert_eq!(keys, ["c"]);
    let one = db.stored_event(dag.as_str(), "b").await.unwrap().unwrap();
    assert_eq!(one.key, "b");
    assert_eq!(db.stored_event(dag.as_str(), "z").await.unwrap(), None);
    assert_eq!(db.queued_event_runs_of(dag.as_str()).await.unwrap(), 3);
}
