use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use jiff::Timestamp;
use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_db::dags::{DagChange, NewParse};
use schedr_db::runs::NewRun;
use schedr_model::{RunKind, RunState, TaskInstanceState};
use serde_json::{Value as Json, json};
use tower::ServiceExt;

use super::*;
use crate::router;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

fn fixed_now() -> Timestamp {
    ts(NOW)
}

/// A database with DAG `sync` and one running run of it, and the API over it.
async fn api() -> (axum::Router, Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    let dag: schedr_model::Dag = serde_json::from_value(json!({
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
    .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts(NOW)).await.unwrap();
    let version = db.active_dags().await.unwrap()[0].version_id.clone();
    let dag_id = DagId::new("sync").unwrap();
    let run_id = db
        .insert_run(&NewRun {
            dag_id: &dag_id,
            dag_version_id: &version,
            kind: RunKind::Manual,
            state: RunState::Running,
            planned_for: ts("2026-10-15T03:00:00Z"),
            since: ts("2026-10-14T03:00:00Z"),
            triggered_at: ts(NOW),
            triggered_by: "anna",
        })
        .await
        .unwrap()
        .unwrap();
    db.create_instances(
        &run_id,
        &[(TaskId::new("extract").unwrap(), TaskInstanceState::Queued)],
        ts(NOW),
    )
    .await
    .unwrap();
    let mut state = ApiState::new(db.clone(), Some("x-forwarded-user".to_owned()));
    state.clock = fixed_now;
    (router(state), db, run_id)
}

async fn send(app: &axum::Router, method: Method, uri: &str, body: &str) -> (StatusCode, Json) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-forwarded-user", "anna")
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Json::Null))
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Json) {
    send(app, Method::GET, uri, "").await
}

#[tokio::test]
async fn dags_are_listed_and_read() {
    let (app, _db, _run) = api().await;
    let (status, body) = get(&app, "/api/v1/dags").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["dags"][0]["dag_id"], "sync");
    assert_eq!(body["dags"][0]["paused"], true);
    let (status, body) = get(&app, "/api/v1/dags/sync").await;
    assert_eq!(
        (status, &body["location"]["path"]),
        (StatusCode::OK, &json!("a.py"))
    );
    assert_eq!(
        get(&app, "/api/v1/dags/ghost").await,
        (StatusCode::NOT_FOUND, json!({"error": "no DAG `ghost`"}))
    );
    let (status, body) = get(&app, "/api/v1/dags/bad%20id").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("bad id"), "{body}");
}

#[tokio::test]
async fn actions_become_pending_commands_by_the_user() {
    let (app, db, run_id) = api().await;
    let (status, body) = send(&app, Method::POST, "/api/v1/dags/sync/pause", "").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        (&body["kind"], &body["status"]),
        (&json!("pause_dag"), &json!("pending"))
    );
    assert_eq!(body["requested_by"], "anna");
    assert_eq!(body["requested_at"], NOW);
    let id = body["command_id"].as_str().unwrap().to_owned();
    let (status, read) = get(&app, &format!("/api/v1/commands/{id}")).await;
    assert_eq!((status, read), (StatusCode::OK, body));

    let pause_cancel = r#"{"cancel_runs": true}"#;
    send(&app, Method::POST, "/api/v1/dags/sync/pause", pause_cancel).await;
    send(&app, Method::POST, "/api/v1/dags/sync/unpause", "").await;
    let at = r#"{"planned_for": "2026-10-15T03:00:00Z"}"#;
    send(&app, Method::POST, "/api/v1/dags/sync/runs", at).await;
    send(&app, Method::POST, "/api/v1/dags/sync/runs", "").await;
    send(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        "",
    )
    .await;
    let commands: Vec<_> = db
        .pending_commands()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.command.unwrap())
        .collect();
    let sync = DagId::new("sync").unwrap();
    assert_eq!(
        commands,
        [
            Command::PauseDag {
                dag_id: sync.clone(),
                cancel_runs: false
            },
            Command::PauseDag {
                dag_id: sync.clone(),
                cancel_runs: true
            },
            Command::UnpauseDag {
                dag_id: sync.clone()
            },
            Command::TriggerRun {
                dag_id: sync.clone(),
                planned_for: Some(ts("2026-10-15T03:00:00Z"))
            },
            Command::TriggerRun {
                dag_id: sync,
                planned_for: None
            },
            Command::CancelRun { run_id },
        ]
    );
}

#[tokio::test]
async fn bad_bodies_and_ids_are_rejected_before_any_command() {
    let (app, db, _run) = api().await;
    let (status, body) = send(&app, Method::POST, "/api/v1/dags/sync/runs", "{not json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .starts_with("invalid body: "),
        "{body}"
    );
    let (status, _) = send(&app, Method::POST, "/api/v1/dags/sync/pause", r#"{"x": 1}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        send(&app, Method::POST, "/api/v1/runs/nope/cancel", "").await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error": "`nope` isn't a run ID"})
        )
    );
    assert_eq!(
        get(&app, "/api/v1/commands/nope").await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error": "`nope` isn't a command ID"})
        )
    );
    assert_eq!(db.pending_commands().await.unwrap(), []);
}

#[tokio::test]
async fn runs_are_listed_and_read_with_their_tasks_and_attempts() {
    let (app, db, run_id) = api().await;
    let (status, body) = get(&app, "/api/v1/dags/sync/runs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["runs"][0]["run_id"], json!(run_id));
    assert_eq!(body["runs"][0]["kind"], "manual");
    let (_, on_day) = get(
        &app,
        "/api/v1/dags/sync/runs?for=2026-10-15T03:00:00Z&limit=5",
    )
    .await;
    assert_eq!(on_day["runs"].as_array().unwrap().len(), 1);
    let (_, other_day) = get(&app, "/api/v1/dags/sync/runs?for=2026-10-16T03:00:00Z").await;
    assert_eq!(other_day["runs"], json!([]));
    let (status, _) = get(&app, "/api/v1/dags/sync/runs?for=tomorrow").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get(&app, "/api/v1/dags/ghost/runs").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, detail) = get(&app, &format!("/api/v1/runs/{run_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["run"]["state"], "running");
    assert_eq!(detail["tasks"][0]["task_id"], "extract");
    assert_eq!(detail["tasks"][0]["state"], "queued");

    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    db.assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap();
    let (status, attempts) = get(
        &app,
        &format!("/api/v1/runs/{run_id}/tasks/extract/attempts"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attempts["attempts"][0]["worker_id"], "w1");
    assert_eq!(attempts["attempts"][0]["status"], Json::Null);
    let (status, _) = get(
        &app,
        &format!("/api/v1/runs/{run_id}/tasks/bad%20id/attempts"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let missing = "0190a3e5-0000-7000-8000-000000000000";
    assert_eq!(
        get(&app, &format!("/api/v1/runs/{missing}")).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": format!("no run `{missing}`")})
        )
    );
    assert_eq!(
        get(&app, &format!("/api/v1/commands/{missing}")).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": format!("no command `{missing}`")})
        )
    );
}

#[tokio::test]
async fn a_database_failure_is_a_server_error() {
    let (app, db, _run) = api().await;
    let Database::Sqlite(pool) = &db else {
        panic!("expected SQLite")
    };
    pool.close().await;
    assert_eq!(
        get(&app, "/api/v1/dags").await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "the database failed; see the webserver's log"})
        )
    );
}
