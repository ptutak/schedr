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
        &[(
            TaskId::new("extract").unwrap(),
            -1,
            TaskInstanceState::Queued,
        )],
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
        let path = format!("/api/v1/runs/{run_id}/tasks/load/{action}");
        let (status, body) = send(&app, Method::POST, &path, "").await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["kind"], format!("{action}_task"));
    }
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
                task_id: TaskId::new("load").unwrap(),
                map_index: None,
            },
            Command::RetryTask {
                run_id,
                task_id: TaskId::new("load").unwrap(),
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

/// A worker that answers what it is told, and remembers what it was asked.
#[derive(Debug, Default)]
struct FakeLogs {
    answer: std::sync::Mutex<Option<Result<crate::logs::WorkerLog, LogFetchError>>>,
    asked: std::sync::Mutex<Vec<(String, String, u64, u64)>>,
}

impl FakeLogs {
    fn answer(&self, answer: Result<crate::logs::WorkerLog, LogFetchError>) {
        *self.answer.lock().unwrap() = Some(answer);
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
    let (_, body) = get(&app, &format!("{uri}?from=6&limit=99999999")).await;
    assert_eq!((&body["from"], &body["next"]), (&json!(6), &json!(12)));
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
                6,
                schedr_model::logs::MAX_CHUNK
            ),
        ]
    );
}

#[tokio::test]
async fn a_character_cut_by_the_limit_waits_for_the_next_part() {
    let (app, db, run_id, attempt_id, fake) = with_log("http://w1:8081").await;
    db.mark_started(&attempt_id, "spawn", ts(NOW))
        .await
        .unwrap();
    // "zażółć": `ż` is two bytes, cut after its first.
    fake.answer(Ok(log("w1", 10, false, &"zaż".as_bytes()[..3])));
    let (_, body) = get(
        &app,
        &format!("/api/v1/runs/{run_id}/tasks/extract/attempts/1/log?limit=3"),
    )
    .await;
    assert_eq!((&body["content"], &body["next"]), (&json!("za"), &json!(2)));
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
        path: "broken.py".to_owned(),
        dags: vec![],
        error: Some(schedr_model::runner::ErrorSummary {
            exception: "ModuleNotFoundError".to_owned(),
            message: "No module named 'pandas'".to_owned(),
            traceback: "  File \"broken.py\", line 1".to_owned(),
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
            "exception": "ModuleNotFoundError", "message": "No module named 'pandas'",
            "traceback": "  File \"broken.py\", line 1"
        }]})
    );
}
