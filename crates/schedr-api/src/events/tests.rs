use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jiff::Timestamp;
use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::runs::NewRun;
use schedr_model::{DagId, RunKind, RunState};
use tower::ServiceExt;

use super::*;
use crate::router;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

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
    let dag = serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
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

/// Reads the stream until it has `events` events (comments skipped), within 10 s.
async fn read_events(body: &mut Body, events: usize) -> Vec<(String, String)> {
    let mut text = String::new();
    let parsed = |text: &str| -> Vec<(String, String)> {
        text.split("\n\n")
            .filter(|block| block.contains("event:"))
            .map(|block| {
                let field = |name: &str| {
                    block
                        .lines()
                        .find_map(|l| l.strip_prefix(name))
                        .unwrap_or_default()
                        .trim()
                        .to_owned()
                };
                (field("event:"), field("data:"))
            })
            .collect()
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while parsed(&text).len() < events || !text.ends_with("\n\n") {
            let frame = body.frame().await.expect("the stream ends").unwrap();
            if let Ok(data) = frame.into_data() {
                text.push_str(std::str::from_utf8(&data).unwrap());
            }
        }
    })
    .await
    .expect("the events within 10 s");
    parsed(&text)
}

#[tokio::test]
async fn a_change_arrives_as_its_event() {
    let (db, version) = with_dag().await;
    let app = router(ApiState::new(db.clone(), None));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(response.headers()["x-accel-buffering"], "no");
    let mut body = response.into_body();
    // Let the follower start (it reads from where the database is when it does).
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let run_id = db
        .insert_run(&NewRun {
            dag_id: &DagId::new("sync").unwrap(),
            dag_version_id: &version,
            kind: RunKind::Manual,
            state: RunState::Queued,
            planned_for: ts("2026-10-14T03:00:00Z"),
            since: ts("2026-10-01T00:00:00Z"),
            triggered_at: ts("2026-10-14T03:00:00Z"),
            triggered_by: "anna",
        })
        .await
        .unwrap()
        .unwrap();
    let events = read_events(&mut body, 1).await;
    assert_eq!(
        events,
        [(
            "run".to_owned(),
            json!({"run_id": run_id, "dag_id": "sync"}).to_string()
        )]
    );
}

#[test]
fn every_change_has_its_event() {
    let text = |live: &Live| format!("{:?}", event(live));
    for (live, name) in [
        (
            Live::Change(Change::Dag {
                dag_id: DagId::new("sync").unwrap(),
            }),
            "dag",
        ),
        (Live::Change(Change::Workers), "workers"),
        (Live::Change(Change::Missed), "refresh"),
        (Live::Refresh, "refresh"),
    ] {
        assert!(
            text(&live).contains(&format!("event: {name}")),
            "{}",
            text(&live)
        );
    }
}
