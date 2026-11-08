use std::sync::Arc;

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
    let extract = json!({
        "id": "extract", "kind": "python",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "none"}
    });
    api_with(extract, &[("extract", -1, TaskInstanceState::Queued)]).await
}

/// [`api`] with a task mapped over `[1, 2]`, `fetch`, expanded: its whole row running, its
/// instances 0 and 1 queued.
async fn mapped_api() -> (axum::Router, Database, String) {
    let fetch = json!({
        "id": "fetch", "kind": "python",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "int"},
        "expand": {"product": [{"name": "n", "value": {"literal": [1, 2]}}]}
    });
    let instances = [
        ("fetch", -1, TaskInstanceState::Running),
        ("fetch", 0, TaskInstanceState::Queued),
        ("fetch", 1, TaskInstanceState::Queued),
    ];
    api_with(fetch, &instances).await
}

/// The API over DAG `sync` with the one `task` and a running run of it with `instances`.
async fn api_with(
    task: Json,
    instances: &[(&str, i32, TaskInstanceState)],
) -> (axum::Router, Database, String) {
    api_over(None, task, instances).await
}

/// [`api_with`], the DAG declaring `params`.
async fn api_over(
    params: Option<Json>,
    task: Json,
    instances: &[(&str, i32, TaskInstanceState)],
) -> (axum::Router, Database, String) {
    api_of(params, &[task], &json!([]), instances).await
}

/// The API over DAG `sync` with `tasks`, `resources` and `params`, and a running run of it
/// with `instances`.
async fn api_of(
    params: Option<Json>,
    tasks: &[Json],
    resources: &Json,
    instances: &[(&str, i32, TaskInstanceState)],
) -> (axum::Router, Database, String) {
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
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": tasks,
        "edges": [],
        "resources": resources,
        "params": params,
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
            params: &schedr_db::runs::NO_PARAMS,
            event: None,
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
    let instances: Vec<_> = instances
        .iter()
        .map(|(task, index, state)| (TaskId::new(*task).unwrap(), *index, *state))
        .collect();
    db.create_instances(&run_id, &instances, ts(NOW))
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
    for action in ["drain", "resume"] {
        let (status, body) = send(
            &app,
            Method::POST,
            &format!("/api/v1/workers/w1/{action}"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["kind"], format!("{action}_worker"));
    }
    for action in ["cancel", "retry"] {
        let path = format!("/api/v1/runs/{run_id}/tasks/extract/{action}");
        let (status, body) = send(&app, Method::POST, &path, "").await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["kind"], format!("{action}_task"));
    }
    let commands: Vec<_> = db
        .pending_commands(100)
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
                event: None,
                params: None,
                dag_id: sync.clone(),
                planned_for: Some(ts("2026-10-15T03:00:00Z"))
            },
            Command::TriggerRun {
                event: None,
                params: None,
                dag_id: sync,
                planned_for: None
            },
            Command::CancelRun {
                run_id: run_id.clone()
            },
            Command::DrainWorker {
                worker_id: "w1".to_owned()
            },
            Command::ResumeWorker {
                worker_id: "w1".to_owned()
            },
            Command::CancelTask {
                run_id: run_id.clone(),
                task_id: TaskId::new("extract").unwrap(),
                map_index: None,
            },
            Command::RetryTask {
                run_id,
                task_id: TaskId::new("extract").unwrap(),
                map_index: None,
            },
        ]
    );
}

#[tokio::test]
async fn bad_bodies_and_ids_are_rejected_before_any_command() {
    let (app, db, run) = api().await;
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
    for action in ["cancel", "retry"] {
        let (status, _) = send(
            &app,
            Method::POST,
            &format!("/api/v1/runs/nope/tasks/load/{action}"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = send(
            &app,
            Method::POST,
            &format!("/api/v1/runs/{run}/tasks/bad%20id/{action}"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        get(&app, "/api/v1/commands/nope").await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error": "`nope` isn't a command ID"})
        )
    );
    assert_eq!(db.pending_commands(100).await.unwrap(), []);
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
    assert_eq!(detail["code_hashes"], json!([]));

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
async fn a_runs_resources_are_shown_with_their_state() {
    let task = |id: &str| {
        json!({
            "id": id, "kind": "empty",
            "settings": serde_json::to_value(schedr_model::TaskSettings::default()).unwrap(),
            "output_type": {"type": "none"}
        })
    };
    let tasks = ["db", "db.cleanup", "load", "tmp", "tmp.cleanup"].map(task);
    let resources = json!([
        {"setup": "db", "cleanup": "db.cleanup", "users": ["load"]},
        {"setup": "tmp", "cleanup": "tmp.cleanup", "users": [], "blocking": false}
    ]);
    let instances = [
        ("db", -1, TaskInstanceState::Success),
        ("db.cleanup", -1, TaskInstanceState::Scheduled),
        ("load", -1, TaskInstanceState::Running),
        ("tmp", -1, TaskInstanceState::Success),
        ("tmp.cleanup", -1, TaskInstanceState::Failed),
    ];
    let (app, _db, run_id) = api_of(None, &tasks, &resources, &instances).await;
    let (status, detail) = get(&app, &format!("/api/v1/runs/{run_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        detail["resources"],
        json!([
            {"setup": "db", "cleanup": "db.cleanup", "users": ["load"], "blocking": true,
             "state": "waiting_for_users"},
            {"setup": "tmp", "cleanup": "tmp.cleanup", "users": [], "blocking": false,
             "state": "cleanup_failed"}
        ])
    );
}

#[tokio::test]
async fn all_runs_are_listed_filtered_and_paged() {
    let (app, _db, run_id) = api().await;
    let (status, body) = get(&app, "/api/v1/runs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["runs"][0]["run_id"], json!(run_id));
    assert_eq!(body["runs"][0]["dag_id"], "sync");
    let runs =
        async |query: &str| get(&app, &format!("/api/v1/runs?{query}")).await.1["runs"].clone();
    let filtered = runs("dag=sync&state=running&limit=1").await;
    assert_eq!(filtered[0]["run_id"], json!(run_id));
    assert_eq!(runs("dag=ghost").await, json!([]));
    assert_eq!(runs("state=failed").await, json!([]));
    // Triggered at NOW: not before it.
    assert_eq!(runs(&format!("before={NOW}")).await, json!([]));
    let later = runs("before=2026-10-21T08:00:01Z").await;
    assert_eq!(later[0]["run_id"], json!(run_id));
    let page = runs(&format!("before=2026-10-21T08:00:00Z&before_run={run_id}")).await;
    assert_eq!(page, json!([]));
    let upper = run_id.to_ascii_uppercase();
    let page = runs(&format!("before=2026-10-21T08:00:01Z&before_run={upper}")).await;
    assert_eq!(page[0]["run_id"], json!(run_id));
    for bad in [
        "before_run=0190a3e5-0000-7000-8000-000000000000",
        "before=2026-10-21T08:00:00Z&before_run=0190a3e50-000-7000-8000-000000000000",
        "before=2026-10-21T08:00:00Z&before_run=r1",
        "state=done",
        "before=soon",
        "dag=bad%20id",
        "limit=-1",
        "page=2",
    ] {
        let (status, _) = get(&app, &format!("/api/v1/runs?{bad}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
}

#[tokio::test]
async fn a_runs_graph_output_and_commands_are_read() {
    let (app, db, run_id) = api().await;
    let (status, graph) = get(&app, &format!("/api/v1/runs/{run_id}/dag")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(graph["id"], "sync");
    assert_eq!(graph["tasks"][0]["id"], "extract");

    let output = format!("/api/v1/runs/{run_id}/tasks/extract/output");
    assert_eq!(
        get(&app, &output).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": format!("no output of `extract` in run `{run_id}`")})
        )
    );
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    db.store_output(&ready[0].instance_id, "SUCCESS", &json!([1, 2]))
        .await
        .unwrap();
    assert_eq!(
        get(&app, &output).await,
        (
            StatusCode::OK,
            json!({"outcome": "SUCCESS", "value": [1, 2]})
        )
    );
    let (status, _) = get(
        &app,
        &format!("/api/v1/runs/{run_id}/tasks/bad%20id/output"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let commands = format!("/api/v1/runs/{run_id}/commands");
    assert_eq!(
        get(&app, &commands).await,
        (StatusCode::OK, json!({"commands": []}))
    );
    let (status, cancel) = send(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (_, listed) = get(&app, &commands).await;
    assert_eq!(listed["commands"], json!([cancel]));

    // Upper case is the same run (Postgres compares UUIDs; SQLite the text, so the API
    // lowercases them), and its commands are found by the ID they were written with.
    let upper = run_id.to_ascii_uppercase();
    let (status, by_upper) = get(&app, &format!("/api/v1/runs/{upper}/commands")).await;
    assert_eq!((status, by_upper), (StatusCode::OK, listed));
    let (status, run) = get(&app, &format!("/api/v1/runs/{upper}")).await;
    assert_eq!(
        (status, &run["run"]["run_id"]),
        (StatusCode::OK, &json!(run_id))
    );
    // Hex digits and dashes in the wrong places: refused, never sent to the database.
    let misplaced = "0190a3e50-000-7000-8000-000000000000";
    for path in ["", "/dag", "/commands", "/tasks/extract/output", "/cancel"] {
        let method = if path == "/cancel" {
            Method::POST
        } else {
            Method::GET
        };
        let (status, _) = send(&app, method, &format!("/api/v1/runs/{misplaced}{path}"), "").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
    }
    let missing = "0190a3e5-0000-7000-8000-000000000000";
    for path in ["dag", "commands", "tasks/extract/output"] {
        let (status, body) = get(&app, &format!("/api/v1/runs/{missing}/{path}")).await;
        assert_eq!(
            (status, body),
            (
                StatusCode::NOT_FOUND,
                json!({"error": format!("no run `{missing}`")})
            ),
            "{path}"
        );
        let (status, _) = get(&app, &format!("/api/v1/runs/not-a-run/{path}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn a_database_failure_is_a_server_error() {
    let (app, db, _run) = api().await;
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
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

#[tokio::test]
async fn workers_are_listed_with_their_state_and_running_attempts() {
    let (app, db, _) = api().await;
    let queues = vec!["large".to_owned()];
    db.register_worker(
        &schedr_db::workers::WorkerRegistration {
            id: "w1",
            incarnation: "0190a0a0-0000-7000-8000-000000000001",
            protocol_version: "1.0",
            schedr_version: "0.1.0",
            queues: &queues,
            slots: 4,
            watcher_slots: 0,
            log_endpoint: "",
        },
        ts("2026-10-22T08:00:00Z"),
    )
    .await
    .unwrap();
    db.set_worker_state(
        "w1",
        schedr_model::WorkerState::Lost,
        ts("2026-10-22T08:02:00Z"),
    )
    .await
    .unwrap();
    let (status, body) = get(&app, "/api/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["workers"][0],
        json!({
            "watcher_slots": 0, "watcher_hosts": [],
            "worker_id": "w1", "state": "lost",
            "incarnation_id": "0190a0a0-0000-7000-8000-000000000001",
            "queues": ["large"], "slots": 4, "running": 0, "schedr_version": "0.1.0",
            "protocol_version": "1.0", "log_endpoint": "",
            "registered_at": "2026-10-22T08:00:00Z",
            "last_heartbeat_at": "2026-10-22T08:00:00Z",
            "offline_at": "2026-10-22T08:02:00Z", "drained": false, "warnings": []
        })
    );
}

/// The deployment's workers: w1 active (with its node and a warning), w2 lost, w3 offline
/// (with an old warning). Returns w1's configuration.
async fn deployment_workers(db: &schedr_db::Database) -> serde_json::Value {
    let queues = ["default".to_owned(), "large".to_owned()];
    for id in ["w1", "w2", "w3"] {
        db.register_worker(
            &schedr_db::workers::WorkerRegistration {
                id,
                incarnation: "0190a0a0-0000-7000-8000-000000000001",
                protocol_version: "1.0",
                schedr_version: "0.1.0",
                queues: &queues,
                slots: 2,
                watcher_slots: 0,
                log_endpoint: "",
            },
            ts(NOW),
        )
        .await
        .unwrap();
    }
    let worker = json!({"worker": {"slots": 2}});
    db.set_worker_node("w1", Some("m-b"), Some("p-2"), Some(&worker))
        .await
        .unwrap();
    db.set_worker_warnings(
        "w1",
        &[schedr_db::workers::WorkerWarning {
            kind: "fork_server_disabled".to_owned(),
            interpreter: String::new(),
            message: "tasks start slower".to_owned(),
        }],
    )
    .await
    .unwrap();
    db.set_worker_state("w2", schedr_model::WorkerState::Lost, ts(NOW))
        .await
        .unwrap();
    // Offline: deregistered on purpose, kept for good; its old warning colours it no more
    // (decision 130).
    db.set_worker_warnings(
        "w3",
        &[schedr_db::workers::WorkerWarning {
            kind: "fork_server_disabled".to_owned(),
            interpreter: String::new(),
            message: "tasks start slower".to_owned(),
        }],
    )
    .await
    .unwrap();
    db.set_worker_state("w3", schedr_model::WorkerState::Offline, ts(NOW))
        .await
        .unwrap();
    worker
}

#[tokio::test]
async fn the_deployment_shows_its_nodes_with_their_configurations() {
    let (_, db, _) = api().await;
    let config = json!({"database": {"url": "postgres://schedr:****@db/s"}});
    let late = [schedr_db::nodes::NodeProblem {
        error: false,
        message: "its lease isn't renewed in time (postgres://u:hunter2@db/s)".to_owned(),
    }];
    for (id, heard) in [
        ("m-a", "2026-10-21T07:59:55Z"),
        ("m-b", "2026-10-21T07:59:55Z"),
        ("m-gone", "2026-10-21T07:59:00Z"),
    ] {
        db.heartbeat_master(
            &schedr_db::nodes::MasterNode {
                master_id: id,
                process_id: "p-1",
                host: "node-a",
                schedr_version: "0.1.0",
                config: &config,
                started_at: ts("2026-10-21T07:00:00Z"),
                problems: if id == "m-a" { &late } else { &[] },
            },
            ts(heard),
        )
        .await
        .unwrap();
    }
    db.take_lease("m-b", std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let worker = deployment_workers(&db).await;
    let mut state = ApiState::new(db.clone(), None).with_node(crate::views::NodeView {
        process_id: "p-3".to_owned(),
        host: "web".to_owned(),
        schedr_version: "0.1.0".to_owned(),
        config: json!({"webserver": {"port": 8080}}),
    });
    state.clock = fixed_now;
    let app = router(state);

    let (status, body) = get(&app, "/api/v1/deployment").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["webserver"],
        json!({"process_id": "p-3", "host": "web", "schedr_version": "0.1.0",
               "config": {"webserver": {"port": 8080}}})
    );
    // Heard from in the last 30 s, the leader first.
    assert_eq!(
        body["masters"],
        json!([
            {"master_id": "m-b", "role": "leader", "process_id": "p-1", "host": "node-a",
             "schedr_version": "0.1.0", "config": config,
             "started_at": "2026-10-21T07:00:00Z", "heard_at": "2026-10-21T07:59:55Z",
             "problems": []},
            {"master_id": "m-a", "role": "standby", "process_id": "p-1", "host": "node-a",
             "schedr_version": "0.1.0", "config": config,
             "started_at": "2026-10-21T07:00:00Z", "heard_at": "2026-10-21T07:59:55Z",
             "problems": [{"error": false,
                           "message": "its lease isn't renewed in time (postgres://u:****@db/s)"}]},
        ])
    );
    assert_eq!(
        body["workers"],
        json!([
            {"worker_id": "w1", "state": "active", "schedr_version": "0.1.0",
             "master_id": "m-b", "process_id": "p-2", "config": worker,
             "queues": ["default", "large"],
             "problems": [{"error": false, "message": "tasks start slower"}]},
            {"worker_id": "w2", "state": "lost", "schedr_version": "0.1.0",
             "master_id": null, "process_id": null, "config": null,
             "queues": ["default", "large"],
             "problems": [{"error": true,
                           "message": "lost: it stopped answering, its attempts were given up"}]},
            {"worker_id": "w3", "state": "offline", "schedr_version": "0.1.0",
             "master_id": null, "process_id": null, "config": null,
             "queues": ["default", "large"], "problems": []},
        ])
    );
    assert_eq!(body["user"], "anonymous");
    // The Workers page's Master column.
    let (_, body) = get(&app, "/api/v1/workers").await;
    assert_eq!(body["workers"][0]["master_id"], "m-b");
    assert_eq!(body["workers"][1].get("master_id"), None);
}

/// A worker that answers what it is told, and remembers what it was asked.
#[derive(Debug, Default)]
struct FakeLogs {
    answer: std::sync::Mutex<Option<Result<crate::logs::WorkerLog, LogFetchError>>>,
    asked: std::sync::Mutex<Vec<(String, String, u64, u64)>>,
    /// A whole log served as a worker does: the bytes asked for, the size, not running.
    whole: std::sync::Mutex<Option<Vec<u8>>>,
}

impl FakeLogs {
    fn answer(&self, answer: Result<crate::logs::WorkerLog, LogFetchError>) {
        *self.answer.lock().unwrap() = Some(answer);
    }

    fn serve(&self, whole: &[u8]) {
        *self.whole.lock().unwrap() = Some(whole.to_vec());
    }
}

impl crate::logs::LogSource for FakeLogs {
    fn fetch<'a>(
        &'a self,
        endpoint: &'a str,
        attempt_id: &'a str,
        from: u64,
        limit: u64,
    ) -> crate::logs::LogFuture<'a> {
        self.asked
            .lock()
            .unwrap()
            .push((endpoint.to_owned(), attempt_id.to_owned(), from, limit));
        if let Some(whole) = self.whole.lock().unwrap().as_ref() {
            let start = usize::try_from(from).unwrap().min(whole.len());
            let most = limit.min(schedr_model::logs::MAX_READ);
            let end = (start + usize::try_from(most).unwrap()).min(whole.len());
            let served = log("w1", whole.len() as u64, false, &whole[start..end]);
            return Box::pin(async move { Ok(served) });
        }
        let answer = self.answer.lock().unwrap().clone().expect("an answer");
        Box::pin(async move { answer })
    }
}

/// The API with a started attempt of `extract` on worker `w1` (at `endpoint`), reading logs
/// from a fake: the router, the database, the run, the attempt and the fake.
async fn with_log(endpoint: &str) -> (axum::Router, Database, String, String, Arc<FakeLogs>) {
    let (_, db, run_id) = api().await;
    let queues = vec![];
    db.register_worker(
        &schedr_db::workers::WorkerRegistration {
            id: "w1",
            incarnation: "0190a0a0-0000-7000-8000-000000000001",
            protocol_version: "1.0",
            schedr_version: "0.1.0",
            queues: &queues,
            slots: 2,
            watcher_slots: 0,
            log_endpoint: endpoint,
        },
        ts(NOW),
    )
    .await
    .unwrap();
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let (attempt_id, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let fake = Arc::new(FakeLogs::default());
    let state = ApiState::new(db.clone(), None).with_logs(fake.clone());
    (router(state), db, run_id, attempt_id, fake)
}

fn log(worker: &str, size: u64, running: bool, bytes: &[u8]) -> crate::logs::WorkerLog {
    crate::logs::WorkerLog {
        worker_id: Some(worker.to_owned()),
        size,
        running,
        bytes: bytes.to_vec(),
    }
}

#[tokio::test]
async fn a_log_is_read_from_the_worker_that_ran_the_attempt() {
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");
    assert_eq!(
        get(&app, &uri).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": "try 1 of task `extract` hasn't started yet: no log so far"})
        )
    );
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    fake.answer(Ok(log("w1", 20, true, b"hello\n")));
    let (status, body) = get(&app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "attempt_id": attempt_id, "try_number": 1, "worker_id": "w1",
            "from": 0, "next": 6, "size": 20, "running": true, "content": "hello\n"
        })
    );
    // From 6 the bytes before are read too (masked with the part, not shown).
    fake.answer(Ok(log("w1", 20, true, b"hello\nworld\n")));
    let (_, body) = get(&app, &format!("{uri}?from=6&limit=99999999")).await;
    assert_eq!((&body["from"], &body["next"]), (&json!(6), &json!(12)));
    assert_eq!(body["content"], "world\n");
    assert_eq!(
        fake.asked.lock().unwrap().clone(),
        [
            (
                "http://w1:8081".to_owned(),
                attempt_id.clone(),
                0,
                schedr_model::logs::MAX_CHUNK
            ),
            (
                "http://w1:8081".to_owned(),
                attempt_id,
                0,
                // The part asked for, and the 6 bytes before it.
                schedr_model::logs::MAX_CHUNK + 6
            ),
        ]
    );
}

#[tokio::test]
async fn a_logs_secrets_are_masked_and_never_split_between_parts() {
    // A task may print an address with its password: shown masked, whole lines only, so
    // neither part shows half of it.
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");
    // Running, a line not ended yet: it waits for its end.
    fake.answer(Ok(log("w1", 40, true, b"connecting\npostgres://u:hun")));
    let (_, body) = get(&app, &uri).await;
    assert_eq!(
        (&body["content"], &body["next"]),
        (&json!("connecting\n"), &json!(11))
    );
    fake.answer(Ok(log(
        "w1",
        40,
        true,
        b"connecting\npostgres://u:hunter2@db/s\n",
    )));
    let (_, body) = get(&app, &format!("{uri}?from=11")).await;
    assert_eq!(body["content"], "postgres://u:****@db/s\n");
    assert_eq!((&body["from"], &body["next"]), (&json!(11), &json!(37)));
    // Read from the middle of a line (the UI's last 256 KiB): from the next line on.
    fake.answer(Ok(log(
        "w1",
        57,
        false,
        b"connecting\npostgres://u:hunter2@db/s\nok password=hunter2\n",
    )));
    let (_, body) = get(&app, &format!("{uri}?from=20")).await;
    assert_eq!(body["content"], "ok password=****\n");
    assert_eq!((&body["from"], &body["next"]), (&json!(37), &json!(57)));
}

#[tokio::test]
async fn a_tasks_output_is_shown_masked_by_its_texts_and_secret_words() {
    let (app, db, run_id) = api().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let value = json!({
        "url": "postgres://u:hunter2@db/s",
        "lines": ["ok", "password=hunter2"],
        "partition_key": "2026-11-01",
        "api_key": "abc",
        "rows": 3
    });
    db.store_output(&ready[0].instance_id, "SUCCESS", &value)
        .await
        .unwrap();
    let (status, body) = get(&app, &format!("/api/v1/runs/{run_id}/tasks/extract/output")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["value"],
        json!({
            "url": "postgres://u:****@db/s",
            "lines": ["ok", "password=****"],
            "partition_key": "2026-11-01",
            "api_key": "****",
            "rows": 3
        })
    );
}

#[tokio::test]
async fn a_secret_isnt_read_in_pieces_by_a_short_limit_or_a_start_inside_its_line() {
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");
    // "postgres://u:hunter2@db/s\n", the attempt over: 4 bytes from inside the password
    // (and the byte before them) show nothing, and pass over the rest of its line.
    // (The bytes before `from` are read too.)
    fake.answer(Ok(log("w1", 26, false, b"postgres://u:hunt")));
    let (_, body) = get(&app, &format!("{uri}?from=13&limit=4")).await;
    assert_eq!(body["content"], "");
    assert_eq!((&body["from"], &body["next"]), (&json!(17), &json!(17)));
    // From its line's start: to the last whitespace in it (none), the line passed over.
    fake.answer(Ok(log("w1", 26, false, b"post")));
    let (_, body) = get(&app, &format!("{uri}?limit=4")).await;
    assert_eq!(
        (&body["content"], &body["next"]),
        (
            &json!("… (the rest of this line is not shown: it is over 4 bytes)\n"),
            &json!(4)
        )
    );
    // The rest of a line whose start was passed over: skipped too.
    fake.answer(Ok(log("w1", 26, false, b"postgres")));
    let (_, body) = get(&app, &format!("{uri}?from=4&limit=4")).await;
    assert_eq!((&body["content"], &body["from"]), (&json!(""), &json!(8)));
}

#[tokio::test]
async fn a_line_longer_than_a_part_is_shown_to_its_last_whitespace_and_says_it_is_cut() {
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");
    let max = usize::try_from(schedr_model::logs::MAX_CHUNK).unwrap();
    let mut line = "ab ".repeat(max / 3).into_bytes();
    line.resize(max - 6, b'a');
    line.extend_from_slice(b" u:hun");
    fake.answer(Ok(log("w1", 3 * 1024 * 1024, true, &line)));
    let (_, body) = get(&app, &uri).await;
    let content = body["content"].as_str().unwrap();
    let note = format!("a … (the rest of this line is not shown: it is over {max} bytes)\n");
    assert!(
        content.ends_with(&note),
        "…{}",
        &content[content.len() - 80..]
    );
    assert!(!content.contains("hun"));
    assert_eq!(body["next"], json!(max));
}

#[tokio::test]
async fn a_log_the_worker_lost_or_cant_serve_says_why() {
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");

    fake.answer(Err(LogFetchError::Missing {
        worker_id: Some("w1".to_owned()),
    }));
    assert_eq!(
        get(&app, &uri).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": "the log file is unavailable at worker `w1` (it may have been deleted)"})
        )
    );
    fake.answer(Err(LogFetchError::Missing {
        worker_id: Some("w2".to_owned()),
    }));
    let stale = json!({"error": "reached worker `w2` instead of `w1` at http://w1:8081: its \
        address is stale; try again shortly"});
    assert_eq!(
        get(&app, &uri).await,
        (StatusCode::SERVICE_UNAVAILABLE, stale.clone())
    );
    fake.answer(Ok(log("w2", 1, false, b"x")));
    assert_eq!(
        get(&app, &uri).await,
        (StatusCode::SERVICE_UNAVAILABLE, stale)
    );
    fake.answer(Err(LogFetchError::Unreachable(
        "connection refused".to_owned(),
    )));
    assert_eq!(
        get(&app, &uri).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "worker `w1` can't be reached at http://w1:8081: connection refused"})
        )
    );

    db.set_worker_state("w1", schedr_model::WorkerState::Offline, ts(NOW))
        .await
        .unwrap();
    let asked = fake.asked.lock().unwrap().len();
    assert_eq!(
        get(&app, &uri).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "worker `w1` is offline: the log of try 1 is unavailable"})
        )
    );
    assert_eq!(
        fake.asked.lock().unwrap().len(),
        asked,
        "a gone worker isn't asked"
    );
}

#[tokio::test]
async fn an_attempt_without_a_log_or_endpoint_is_named() {
    let (app, db, run_id, attempt_id, _fake) = with_log("").await;
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts");
    assert_eq!(
        get(&app, &format!("{uri}/2/log")).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": format!("task `extract` of run `{run_id}` has no try 2")})
        )
    );
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        get(&app, &format!("{uri}/1/log")).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "worker `w1` has no log endpoint: the log of try 1 is unavailable"})
        )
    );
    let (status, _) = get(&app, &format!("{uri}/1/log?from=start")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get(
        &app,
        &format!("/api/v1/runs/{run_id}/tasks/bad%20id/attempts/1/log"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn an_attempt_that_never_started_has_no_log() {
    let (app, db, run_id, attempt_id, _fake) = with_log("http://w1:8081").await;
    db.end_attempt(
        &attempt_id,
        &schedr_db::attempts::AttemptEnd::by_master(schedr_model::AttemptStatus::AssignmentExpired),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        get(
            &app,
            &format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log")
        )
        .await,
        (
            StatusCode::NOT_FOUND,
            json!({"error": "try 1 of task `extract` never started, so it has no log"})
        )
    );
}

#[tokio::test]
async fn the_current_codes_import_errors_are_listed() {
    let (app, db, _) = api().await;
    let (status, body) = get(&app, "/api/v1/import-errors").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"errors": []}));
    let files = [schedr_model::runner::ParsedFile {
        script_roots: Vec::new(),
        path: "broken.py".to_owned(),
        dags: vec![],
        connections: Vec::new(),
        error: Some(schedr_model::runner::ErrorSummary {
            exception: "ModuleNotFoundError".to_owned(),
            // Codex's review of the Deployments batch: an error quoting an address showed its
            // password.
            message: "No module named 'pandas' (from postgres://u:hunter2@db/s)".to_owned(),
            traceback: "  File \"broken.py\", line 1, password=hunter2".to_owned(),
        }),
        warnings: Vec::new(),
    }];
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h2",
            schedr_version: "0.1.0",
            worker_id: "w2",
            parsed_at: ts(NOW),
            files: &files,
        })
        .await
        .unwrap();
    db.set_current_parse("etl", &parse_id, ts(NOW))
        .await
        .unwrap();
    let (_, body) = get(&app, "/api/v1/import-errors").await;
    assert_eq!(
        body,
        json!({"errors": [{
            "root": "etl", "path": "broken.py", "content_hash": "h2",
            "parsed_at": NOW, "worker_id": "w2", "schedr_version": "0.1.0",
            "exception": "ModuleNotFoundError",
            "message": "No module named 'pandas' (from postgres://u:****@db/s)",
            "traceback": "  File \"broken.py\", line 1, password=****"
        }]})
    );
}

#[tokio::test]
async fn a_mapped_tasks_instance_must_be_one_it_has() {
    let (app, db, run_id) = mapped_api().await;
    let task = format!("/api/v1/runs/{run_id}/tasks/fetch");
    for read in ["attempts", "attempts/1/log"] {
        let join = if read.contains('?') { '&' } else { '?' };
        for index in [2, 99, -2] {
            let (status, body) = get(&app, &format!("{task}/{read}{join}map_index={index}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{read} {index}: {body}");
            assert_eq!(
                body["error"],
                format!("task `fetch` has 2 instances, from 0 to 1: no instance {index}")
            );
        }
        // -1 is the whole task: an instance must be named, as when none is given.
        let (status, body) = get(&app, &format!("{task}/{read}{join}map_index=-1")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{read} -1: {body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("give ?map_index=N")
        );
    }
    for action in ["cancel", "retry"] {
        let (status, body) = send(
            &app,
            Method::POST,
            &format!("{task}/{action}?map_index=2"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{action}: {body}");
    }
    let (status, _) = send(
        &app,
        Method::POST,
        &format!("{task}/retry?map_index=-1"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // A cancel of the whole task is a cancel without an instance, whichever way it is said.
    let (status, _) = send(
        &app,
        Method::POST,
        &format!("{task}/cancel?map_index=-1"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _) = send(
        &app,
        Method::POST,
        &format!("{task}/cancel?map_index=1"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let commands: Vec<_> = db
        .pending_commands(100)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.command.unwrap())
        .collect();
    let fetch = TaskId::new("fetch").unwrap();
    assert_eq!(
        commands,
        [
            Command::CancelTask {
                run_id: run_id.clone(),
                task_id: fetch.clone(),
                map_index: None,
            },
            Command::CancelTask {
                run_id: run_id.clone(),
                task_id: fetch,
                map_index: Some(1),
            },
        ]
    );
}

#[tokio::test]
async fn an_unmapped_task_has_no_instance_to_cancel() {
    let (app, _, run_id) = api().await;
    let path = format!("/api/v1/runs/{run_id}/tasks/extract/cancel?map_index=0");
    let (status, body) = send(&app, Method::POST, &path, "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body["error"],
        "task `extract` isn't mapped: it has no instance 0"
    );
}

#[tokio::test]
async fn a_task_the_run_doesnt_have_is_not_found_by_every_task_endpoint() {
    // Not an empty list nor "no output yet", which a real task not tried yet gets.
    let (app, _, run_id) = api().await;
    let task = format!("/api/v1/runs/{run_id}/tasks/ghost");
    for (method, path) in [
        (Method::GET, format!("{task}/attempts")),
        (Method::GET, format!("{task}/output")),
        (Method::GET, format!("{task}/attempts/1/log")),
        (Method::POST, format!("{task}/cancel")),
        (Method::POST, format!("{task}/retry")),
    ] {
        let (status, body) = send(&app, method, &path, "").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {body}");
        assert_eq!(
            body["error"],
            format!("no task `ghost` in run `{run_id}`"),
            "{path}"
        );
    }
    let (status, body) = get(
        &app,
        &format!("/api/v1/runs/{run_id}/tasks/extract/attempts"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"attempts": []}));
}

#[tokio::test]
async fn map_index_minus_one_on_output_is_the_mapped_tasks_whole_output() {
    let (app, db, run_id) = mapped_api().await;
    let path = format!("/api/v1/runs/{run_id}/tasks/fetch/output");
    // Not finished: no whole output yet, the same answer with or without -1.
    let without = get(&app, &path).await;
    assert_eq!(without.0, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, &format!("{path}?map_index=-1")).await, without);
    let done = schedr_db::instances::InstanceUpdate {
        task_id: TaskId::new("fetch").unwrap(),
        map_index: -1,
        state: TaskInstanceState::Success,
        ready_at: None,
        outcome: None,
        used_retry: false,
        reason: None,
        map_sizes: Some(vec![2]),
    };
    db.update_instances(&run_id, &[done], ts(NOW))
        .await
        .unwrap();
    let whole = get(&app, &path).await;
    assert_eq!(whole.0, StatusCode::OK);
    assert_eq!(whole.1["value"], json!([null, null]));
    assert_eq!(get(&app, &format!("{path}?map_index=-1")).await, whole);
}

/// `send` with an `Idempotency-Key`.
async fn send_keyed(app: &axum::Router, uri: &str, body: &str, key: &str) -> (StatusCode, Json) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("x-forwarded-user", "anna")
        .header("content-type", "application/json")
        .header("idempotency-key", key)
        .body(Body::from(body.to_owned()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Json::Null))
}

#[tokio::test]
async fn a_command_sent_again_with_its_key_is_the_same_command() {
    // B140: an answer lost after the command was stored; the client sends it again.
    let (app, db, _run) = api().await;
    let trigger = "/api/v1/dags/sync/runs";
    let (status, first) = send_keyed(&app, trigger, "", "0190a0a0-key").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, again) = send_keyed(&app, trigger, "", "0190a0a0-key").await;
    assert_eq!((status, &again), (StatusCode::ACCEPTED, &first));
    assert_eq!(db.pending_commands(100).await.unwrap().len(), 1);
    // The same key for another command is refused; a bad key too.
    let (status, body) = send_keyed(&app, "/api/v1/dags/sync/pause", "", "0190a0a0-key").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        body["error"].as_str().unwrap().contains("another command"),
        "{body}"
    );
    for bad in ["a b", &"k".repeat(MAX_REQUEST_KEY + 1)] {
        let (status, _) = send_keyed(&app, trigger, "", bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
    assert_eq!(db.pending_commands(100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn one_dags_view_shows_its_limits_with_how_many_run() {
    // M4.4, decision 96: each limit set, with its count of open attempts.
    let fetch = json!({
        "id": "fetch", "kind": "python",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576,
            "max_active": 5, "max_active_per_run": 2
        },
        "output_type": {"type": "int"},
        "expand": {"product": [{"name": "n", "value": {"literal": [1, 2]}}]}
    });
    let instances = [
        ("fetch", -1, TaskInstanceState::Running),
        ("fetch", 0, TaskInstanceState::Queued),
        ("fetch", 1, TaskInstanceState::Queued),
    ];
    let (app, db, _run) = api_with(fetch, &instances).await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    db.assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let (_, body) = get(&app, "/api/v1/dags/sync").await;
    assert_eq!(
        body["limits"],
        json!([
            {"limit": "max_active", "task_id": "fetch", "value": 5, "running": 1},
            {"limit": "max_active_per_run", "task_id": "fetch", "value": 2, "running": 1}
        ])
    );
    // Not in the list: counting there would cost a query per DAG.
    let (_, list) = get(&app, "/api/v1/dags").await;
    assert_eq!(list["dags"][0].get("limits"), None);
}

#[tokio::test]
async fn a_runs_dag_is_parsed_once_for_its_task_reads() {
    // B137: each task read (attempts, output, log, cancel, retry) parsed the run's whole DAG
    // again. A version never changes: once read, it comes from the cache, so the stored
    // JSON isn't read again (shown by spoiling it after the first read).
    let (app, db, run_id) = api().await;
    let ghost = format!("/api/v1/runs/{run_id}/tasks/ghost/attempts");
    let (status, _) = get(&app, &ghost).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let schedr_db::Pool::Sqlite(pool) = db.pool() else {
        unreachable!("SQLite in memory")
    };
    sqlx::query("UPDATE dag_versions SET dag_json = '{'")
        .execute(pool)
        .await
        .unwrap();
    let (status, body) = get(&app, &ghost).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], format!("no task `ghost` in run `{run_id}`"));
}

#[tokio::test]
async fn one_dags_reads_dont_read_the_other_dags() {
    // B136: a DAG's page and its runs read every DAG's definition, so another DAG's, big or
    // unreadable, slowed or broke them.
    let (app, db, _) = api().await;
    let schedr_db::Pool::Sqlite(pool) = db.pool() else {
        unreachable!("SQLite in memory")
    };
    sqlx::query(
        "INSERT INTO dags (dag_id, paused, current_version_id, first_seen_at) \
         VALUES ('other', 1, 'v-other', '2026-10-15T00:00:00.000000Z')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO dag_versions (id, dag_id, parse_result_id, dag_json, created_at) \
         SELECT 'v-other', 'other', parse_result_id, '{', created_at FROM dag_versions LIMIT 1",
    )
    .execute(pool)
    .await
    .unwrap();
    for path in ["/api/v1/dags/sync", "/api/v1/dags/sync/runs"] {
        let (status, body) = get(&app, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
    let (status, _) = get(&app, "/api/v1/dags/nowhere").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_quoted_secret_going_on_over_lines_is_masked_in_each_part() {
    // A quoted value with a newline in it (a PEM key, a shell assignment): a part starting
    // at its second line is masked too, as the whole log is.
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");
    let log_text = b"password='hunter2\nstillsecret'\n";
    fake.answer(Ok(log("w1", 31, false, log_text)));
    let (_, body) = get(&app, &format!("{uri}?from=18")).await;
    let content = body["content"].as_str().unwrap();
    assert!(!content.contains("stillsecret"), "{content}");
}

#[tokio::test]
async fn a_quoted_secret_begun_far_before_a_part_is_masked_in_it() {
    // Codex: a quoted value open 5000 bytes before the part (more than a 4 KiB context).
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    let uri = format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log");
    let text = format!("password='{}\nHUNTER2'\nok\n", "A".repeat(5000));
    fake.serve(text.as_bytes());
    let (_, body) = get(&app, &format!("{uri}?from=5011")).await;
    assert_eq!(
        (&body["from"], &body["content"]),
        (&json!(5011), &json!("****'\nok\n"))
    );
}

#[tokio::test]
async fn a_runs_dag_shows_its_params_defaults_masked() {
    let extract = json!({
        "id": "extract", "kind": "python",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "none"}
    });
    let params = json!({
        "type": {"type": "dataclass", "name": "P", "fields": [
            {"name": "db_password", "type": {"type": "literal", "values": ["hunter2"]}, "required": false},
            {"name": "region", "type": {"type": "str"}, "required": false},
        ]},
        "schema": {"type": "object", "properties": {
            "db_password": {"type": "string", "default": "hunter2"},
            "region": {"type": "string", "default": "postgres://u:hunter3@db/x"},
        }},
        "defaults": {"db_password": "hunter2", "region": "postgres://u:hunter3@db/x"},
    });
    let (app, _db, run_id) = api_over(
        Some(params),
        extract,
        &[("extract", -1, TaskInstanceState::Queued)],
    )
    .await;
    let (status, dag) = get(&app, &format!("/api/v1/runs/{run_id}/dag")).await;
    assert_eq!(status, StatusCode::OK);
    let text = dag.to_string();
    assert!(
        !text.contains("hunter2") && !text.contains("hunter3"),
        "{text}"
    );
    assert_eq!(dag["params"]["defaults"]["db_password"], "****");
    // The schema and the type hold copies of values (a `Literal`'s): not shown.
    assert_eq!(
        dag["params"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["defaults"]
    );
}

#[tokio::test]
async fn a_dags_view_describes_its_params_form_without_secrets() {
    let params = json!({
        "type": {"type": "dataclass", "name": "P", "fields": [
            {"name": "db_password", "type": {"type": "str"}, "required": false},
            {"name": "rows", "type": {"type": "int"}, "required": false},
        ]},
        "schema": {"type": "object", "properties": {
            "db_password": {"type": "string", "default": "hunter2"},
            "rows": {"type": "integer", "default": 10},
        }},
        "defaults": {"db_password": "hunter2", "rows": 10},
    });
    let (app, _db, _run) = api_over(
        Some(params),
        json!({
            "id": "extract", "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }),
        &[],
    )
    .await;
    let (status, dag) = get(&app, "/api/v1/dags/sync").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!dag.to_string().contains("hunter2"), "{dag}");
    assert_eq!(
        dag["params"],
        json!([
            {"name": "db_password", "kind": "text", "required": false, "nullable": false, "secret": true},
            {"name": "rows", "kind": "int", "required": false, "nullable": false, "secret": false, "default": 10},
        ])
    );
    let (_, list) = get(&app, "/api/v1/dags").await;
    assert_eq!(list["dags"][0].get("params"), None);
}

#[tokio::test]
async fn a_bad_body_doesnt_quote_its_text() {
    let (app, _db, _run) = api().await;
    let (status, body) = send(
        &app,
        Method::POST,
        "/api/v1/dags/sync/runs",
        r#"{"params": "postgres://u:hunter2@db/x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = body["error"].as_str().unwrap();
    assert!(!error.contains("hunter2"), "{error}");
    assert!(
        error.starts_with("invalid body: invalid type: string"),
        "{error}"
    );
    let (status, body) = send(
        &app,
        Method::POST,
        "/api/v1/dags/sync/runs",
        r#"{"postgres://u:hunter2@db/x": 1}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = body["error"].as_str().unwrap();
    assert!(!error.contains("hunter2"), "{error}");
}

#[tokio::test]
async fn params_with_nul_are_refused_before_any_command() {
    let (app, db, _run) = api().await;
    let (status, body) = send(
        &app,
        Method::POST,
        "/api/v1/dags/sync/runs",
        r#"{"params": {"db_password": "a\u0000hunter2"}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = body["error"].as_str().unwrap();
    assert!(
        error.contains("NUL") && !error.contains("hunter2"),
        "{error}"
    );
    assert_eq!(db.pending_commands(10).await.unwrap().len(), 0);
}

#[tokio::test]
async fn each_roots_connections_are_listed_with_the_tasks_declaring_them() {
    let task = json!({
        "id": "extract", "kind": "python",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "none"},
        "connections": ["db", "gone"]
    });
    let (app, db, _) = api_with(task, &[]).await;
    let (status, body) = get(&app, "/api/v1/connections").await;
    assert_eq!(status, StatusCode::OK);
    // No current parse yet: the task's declarations alone, refused.
    assert_eq!(body["roots"][0]["root"], "etl");
    assert_eq!(body["roots"][0]["connections"][0]["name"], "db");
    let def = |name: &str, kind| schedr_model::connection::ConnectionDef {
        name: name.to_owned(),
        kind,
        params: std::collections::BTreeMap::from([(
            "password".to_owned(),
            serde_json::from_value(
                json!({"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}),
            )
            .unwrap(),
        )]),
    };
    let file = |path: &str, connections| schedr_model::runner::ParsedFile {
        script_roots: Vec::new(),
        path: path.to_owned(),
        dags: vec![],
        connections,
        error: None,
        warnings: Vec::new(),
    };
    let files = [
        file(
            "conns.py",
            vec![
                def("db", Some(schedr_model::connection::ConnectionType::Sql)),
                def("dup", None),
                def("unused", None),
            ],
        ),
        file("other.py", vec![def("dup", None)]),
    ];
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h2",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &files,
        })
        .await
        .unwrap();
    db.set_current_parse("etl", &parse_id, ts(NOW))
        .await
        .unwrap();
    let (status, body) = get(&app, "/api/v1/connections").await;
    assert_eq!(status, StatusCode::OK);
    let text = body.to_string();
    // Secret names stay in the DAG JSON: the list carries none, and never a value.
    assert!(!text.contains("etl/db"), "{text}");
    let gone = "connection `gone` isn't defined in the DAG folder (a `@connection` function in \
                a file of the DAG root that imports)";
    let dup = "connection `dup` is defined in more than one file (conns.py, other.py): rename one";
    assert_eq!(
        body,
        json!({"roots": [{"root": "etl", "connections": [
            {"name": "db", "type": "sql", "file": "conns.py",
             "used_by": [{"dag_id": "sync", "tasks": ["extract"]}]},
            {"name": "dup", "refused": dup, "used_by": []},
            {"name": "gone", "refused": gone,
             "used_by": [{"dag_id": "sync", "tasks": ["extract"]}]},
            {"name": "unused", "file": "conns.py", "used_by": []},
        ]}]})
    );
}

#[tokio::test]
async fn a_connection_test_without_a_timeout_gets_the_configured_one() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let state = ApiState::new(db.clone(), Some("x-forwarded-user".to_owned()))
        .with_connection_test_timeout(std::time::Duration::from_mins(2));
    let app = router(state);
    for body in ["", r#"{"timeout_ms": 5000}"#] {
        let path = "/api/v1/connections/warehouse/test";
        let (status, _) = send(&app, Method::POST, path, body).await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }
    let timeouts: Vec<_> = db
        .pending_commands(10)
        .await
        .unwrap()
        .into_iter()
        .map(|c| match c.command.unwrap() {
            Command::TestConnection { timeout_ms, .. } => timeout_ms,
            other => panic!("expected a connection test, got {other:?}"),
        })
        .collect();
    assert_eq!(timeouts, [120_000, 5_000]);
}

#[test]
fn a_mapped_tasks_cleanup_is_the_worst_of_its_instances() {
    use schedr_model::CleanupResult as R;
    let instance = |index: i32, cleanup: Option<R>| schedr_db::reads::InstanceRecord {
        task_id: TaskId::new("fetch").unwrap(),
        map_index: index,
        reason: None,
        state: TaskInstanceState::Success,
        outcome: None,
        try_number: 1,
        queued_reason: None,
        cleanup_result: cleanup,
        updated_at: ts(NOW),
    };
    let fetch = TaskId::new("fetch").unwrap();
    let tasks = [
        instance(-1, None),
        instance(0, Some(R::Ok)),
        instance(1, Some(R::Failed)),
        instance(2, Some(R::WorkerLost)),
    ];
    assert_eq!(worst_cleanup(&tasks, &fetch).as_deref(), Some("failed"));
    assert_eq!(worst_cleanup(&tasks[..2], &fetch).as_deref(), Some("ok"));
    assert_eq!(worst_cleanup(&tasks[..1], &fetch), None);
}

#[tokio::test]
async fn a_triggers_events_are_masked_as_outputs_are_and_paged() {
    let (app, db, _) = api().await;
    let active = db.active_dags().await.unwrap().remove(0);
    let at = ts("2026-10-14T03:00:00Z");
    let dag = active.dag.id.clone();
    for (i, key) in ["a", "b"].iter().enumerate() {
        let event = schedr_model::RunEvent {
            key: (*key).to_owned(),
            data: json!({"password": "hunter2", "n": i}),
            at: None,
        };
        let params = json!({});
        let new = schedr_db::runs::NewRun {
            dag_id: &dag,
            dag_version_id: &active.version_id,
            kind: schedr_model::RunKind::Event,
            state: schedr_model::RunState::Queued,
            planned_for: at,
            since: at,
            triggered_at: at,
            triggered_by: "trigger",
            params: &params,
            event: Some(&event),
        };
        let id = format!("01a00000-0000-7000-8000-00000000000{i}");
        db.store_event(&id, &new, &event).await.unwrap();
    }
    let path = format!("/api/v1/dags/{dag}/trigger/events");
    let (status, body) = get(&app, &format!("{path}?limit=1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["key"], "a", "{body}");
    assert_eq!(body["events"][0]["data"]["password"], "****", "{body}");
    assert_eq!(body["events"][0]["data"]["n"], 0, "{body}");
    let (_, rest) = get(
        &app,
        &format!("{path}?before=2026-10-14T03:00:00Z&before_key=a"),
    )
    .await;
    assert_eq!(rest["events"][0]["key"], "b", "{rest}");
    let (status, _) = get(&app, &format!("{path}?before_key=a")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get(&app, &format!("/api/v1/dags/{dag}/trigger")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "it has no trigger");
    let (status, list) = get(&app, "/api/v1/triggers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["triggers"], json!([]));
}

#[tokio::test]
async fn an_event_that_cant_be_stored_is_refused_before_it_becomes_a_command() {
    let (app, _db, _) = api().await;
    let long = "k".repeat(1025);
    for (body, why) in [
        (
            json!({"event_at": "2026-10-14T03:00:00Z"}),
            "event_at needs",
        ),
        (json!({"event_data": 1}), "event_data needs"),
        (json!({"event": "a", "event_key": "b"}), "event is the key"),
        (json!({"event": "a", "event_data": 1}), "event is the key"),
        (json!({"event_key": "a", "event_data": "x\u{0}"}), "NUL"),
        (json!({"event_key": ""}), "key"),
        (json!({"event_key": long}), "key"),
        (json!({"event_key": "a\nb"}), "control"),
    ] {
        let (status, answer) = send(
            &app,
            Method::POST,
            "/api/v1/dags/sync/runs",
            &body.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {answer}");
        let error = answer["error"].as_str().unwrap_or_default();
        assert!(error.contains(why), "{body}: {answer}");
    }
    for body in [
        json!({"event": "a"}),
        json!({"event_key": "a", "event_data": {"n": 1}, "event_at": "2026-10-14T03:00:00Z"}),
    ] {
        let (status, answer) = send(
            &app,
            Method::POST,
            "/api/v1/dags/sync/runs",
            &body.to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}: {answer}");
    }
}

#[tokio::test]
async fn a_dag_without_a_trigger_has_nothing_to_switch_on() {
    let (app, _db, _) = api().await;
    let (status, _) = send(
        &app,
        Method::POST,
        "/api/v1/dags/sync/trigger/switch-on",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // Whether it has one is the master's to say, as for any command.
    let (status, _) = send(&app, Method::POST, "/api/v1/dags/sync/trigger/restart", "").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _) = send(&app, Method::POST, "/api/v1/dags/sync/trigger/skip", "").await;
    assert_eq!(status, StatusCode::ACCEPTED);
}
