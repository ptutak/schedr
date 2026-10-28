use std::sync::Mutex;

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

/// Answers requests from a script and records them.
struct Scripted {
    answers: Mutex<Vec<ApiResponse>>,
    seen: Mutex<Vec<ApiRequest>>,
}

impl Scripted {
    fn new(answers: Vec<(u16, serde_json::Value)>) -> Self {
        Self {
            answers: Mutex::new(
                answers
                    .into_iter()
                    .rev()
                    .map(|(status, body)| ApiResponse { status, body })
                    .collect(),
            ),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn seen(&self) -> Vec<(Method, String, Option<serde_json::Value>)> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|r| (r.method, r.path.clone(), r.body.clone()))
            .collect()
    }
}

impl Transport for Scripted {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        tokio::task::yield_now().await; // a real transport waits for the network
        self.seen.lock().unwrap().push(request);
        Ok(self.answers.lock().unwrap().pop().expect("an answer"))
    }
}

fn client(answers: Vec<(u16, serde_json::Value)>) -> Client<Scripted> {
    Client::new(
        Scripted::new(answers),
        Duration::from_millis(1),
        Duration::from_millis(50),
    )
}

fn command(status: &str) -> serde_json::Value {
    json!({
        "command_id": "0190a3e5-0000-7000-8000-000000000000", "kind": "pause_dag",
        "status": status, "reason": if status == "rejected" { json!("no DAG `x`") } else { json!(null) },
        "result": {}, "requested_by": "anna", "requested_at": "2026-10-21T08:00:00Z",
        "decided_at": null
    })
}

#[test]
fn a_token_wins_over_basic_credentials() {
    assert_eq!(
        Auth::from_cli(Some("t"), Some("anna"), Some("secret")),
        Auth::Bearer("t".to_owned())
    );
    assert_eq!(
        Auth::from_cli(None, Some("anna"), Some("secret")),
        Auth::Basic {
            user: "anna".to_owned(),
            password: Some("secret".to_owned())
        }
    );
    assert_eq!(Auth::from_cli(None, None, Some("secret")), Auth::None);
}

#[tokio::test]
async fn requests_go_to_the_api_paths_with_their_bodies() {
    let api = client(vec![
        (200, json!({"dags": []})),
        (200, json!({"runs": []})),
        (202, command("pending")),
        (202, command("pending")),
        (202, command("pending")),
        (202, command("pending")),
    ]);
    api.dags().await.unwrap();
    api.runs("my dag/x", Some("2026-10-15T03:00:00Z".parse().unwrap()))
        .await
        .unwrap();
    api.set_paused("sync", true, true).await.unwrap();
    api.set_paused("sync", false, false).await.unwrap();
    api.trigger("sync", None).await.unwrap();
    api.cancel("r1").await.unwrap();
    assert_eq!(
        api.transport().seen(),
        [
            (Method::Get, "/api/v1/dags".to_owned(), None),
            (
                Method::Get,
                "/api/v1/dags/my%20dag%2Fx/runs?for=2026-10-15T03:00:00Z".to_owned(),
                None
            ),
            (
                Method::Post,
                "/api/v1/dags/sync/pause".to_owned(),
                Some(json!({"cancel_runs": true}))
            ),
            (Method::Post, "/api/v1/dags/sync/unpause".to_owned(), None),
            (
                Method::Post,
                "/api/v1/dags/sync/runs".to_owned(),
                Some(json!({"planned_for": null}))
            ),
            (Method::Post, "/api/v1/runs/r1/cancel".to_owned(), None),
        ]
    );
}

#[tokio::test]
async fn the_run_pages_reads_go_to_their_paths() {
    let api = client(vec![
        (200, json!({"runs": []})),
        (200, json!({"runs": []})),
        (200, json!({"id": "sync"})),
        (200, json!({"commands": []})),
        (200, json!({"outcome": "SUCCESS", "value": 3})),
        (
            200,
            json!({"attempt_id": "a1", "try_number": 2, "worker_id": "w1", "from": 5,
                   "next": 9, "size": 9, "running": false, "content": "done"}),
        ),
    ]);
    api.all_runs(&AllRunsQuery::default()).await.unwrap();
    api.all_runs(&AllRunsQuery {
        dag: Some(schedr_model::DagId::new("etl:a/b.py#c").unwrap()),
        state: Some(schedr_model::RunState::Failed),
        before: Some("2026-10-15T03:00:00.5+00:00".parse().unwrap()),
        before_run: Some("r 9".to_owned()),
        limit: Some(20),
    })
    .await
    .unwrap();
    assert_eq!(api.run_dag("r1").await.unwrap(), json!({"id": "sync"}));
    api.run_commands("r1").await.unwrap();
    let output = api.output("r1", "a b").await.unwrap();
    assert_eq!(
        output,
        OutputView {
            outcome: "SUCCESS".to_owned(),
            value: json!(3)
        }
    );
    let part = api.log_part("r1", "a b", 2, 5, 4).await.unwrap();
    assert_eq!((part.from, part.content.as_str()), (5, "done"));
    assert_eq!(
        api.transport().seen(),
        [
            (Method::Get, "/api/v1/runs".to_owned(), None),
            (
                Method::Get,
                "/api/v1/runs?dag=etl%3Aa%2Fb.py%23c&state=failed\
                 &before=2026-10-15T03%3A00%3A00.5Z&before_run=r%209&limit=20"
                    .to_owned(),
                None
            ),
            (Method::Get, "/api/v1/runs/r1/dag".to_owned(), None),
            (Method::Get, "/api/v1/runs/r1/commands".to_owned(), None),
            (
                Method::Get,
                "/api/v1/runs/r1/tasks/a%20b/output".to_owned(),
                None
            ),
            (
                Method::Get,
                "/api/v1/runs/r1/tasks/a%20b/attempts/2/log?from=5&limit=4".to_owned(),
                None
            ),
        ]
    );
}

#[tokio::test]
async fn waiting_reads_the_command_back_until_it_is_decided() {
    let api = client(vec![(200, command("pending")), (200, command("applied"))]);
    let pending: CommandView = serde_json::from_value(command("pending")).unwrap();
    let decided = api.wait(pending).await.unwrap();
    assert_eq!(decided.status, "applied");
    assert_eq!(api.transport().seen().len(), 2);
}

#[tokio::test]
async fn a_rejection_and_a_timeout_are_errors() {
    let rejected: CommandView = serde_json::from_value(command("rejected")).unwrap();
    let error = client(vec![]).wait(rejected).await.unwrap_err();
    assert_eq!(error.to_string(), "rejected: no DAG `x`");

    let pending: CommandView = serde_json::from_value(command("pending")).unwrap();
    let answers = (0..200).map(|_| (200, command("pending"))).collect();
    let error = client(answers).wait(pending).await.unwrap_err();
    assert!(matches!(error, ClientError::Timeout { .. }), "{error}");
    assert!(
        error
            .to_string()
            .starts_with("the master hasn't applied the command yet")
    );
}

#[tokio::test]
async fn server_errors_carry_the_servers_message() {
    let api = client(vec![
        (404, json!({"error": "no DAG `x`"})),
        (502, json!(null)),
    ]);
    let error = api.runs("x", None).await.unwrap_err();
    assert!(matches!(error, ClientError::Server { status: 404, .. }));
    assert_eq!(error.to_string(), "no DAG `x`");
    assert_eq!(
        api.dags().await.unwrap_err().to_string(),
        "the server answered 502"
    );
}

#[tokio::test]
async fn answers_that_dont_read_are_errors() {
    let api = client(vec![(200, json!({"dags": "many"}))]);
    assert!(matches!(
        api.dags().await.unwrap_err(),
        ClientError::Unexpected(_)
    ));
}

#[tokio::test]
async fn an_unreachable_server_is_a_connect_error() {
    let transport = HttpTransport::new("http://127.0.0.1:1/", Auth::None);
    assert_eq!(transport.server(), "http://127.0.0.1:1");
    assert_eq!(transport.auth(), &Auth::None);
    let api = Client::new(transport, POLL_INTERVAL, WAIT_TIMEOUT);
    let error = api.dags().await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("can't reach the schedr server at http://127.0.0.1:1: "),
        "{error}"
    );
}

#[tokio::test]
async fn the_router_transport_calls_the_api_in_process() {
    use axum::Json;
    use axum::routing::{get, post};
    let app = axum::Router::new()
        .route(
            "/api/v1/echo",
            post(
                |headers: axum::http::HeaderMap, Json(body): Json<serde_json::Value>| async move {
                    let version = headers[CLIENT_VERSION_HEADER].to_str().unwrap().to_owned();
                    (
                        axum::http::StatusCode::ACCEPTED,
                        Json(json!({"got": body, "version": version})),
                    )
                },
            ),
        )
        .route("/api/v1/plain", get(|| async { "not json" }));
    let transport = RouterTransport::new(app);
    let echoed = transport
        .send(ApiRequest {
            method: Method::Post,
            path: "/api/v1/echo".to_owned(),
            body: Some(json!({"a": 1})),
        })
        .await
        .unwrap();
    assert_eq!(
        echoed,
        ApiResponse {
            status: 202,
            body: json!({"got": {"a": 1}, "version": VERSION}),
        }
    );
    let plain = transport
        .send(ApiRequest {
            method: Method::Get,
            path: "/api/v1/plain".to_owned(),
            body: None,
        })
        .await
        .unwrap();
    assert_eq!((plain.status, plain.body), (200, serde_json::Value::Null));
}

#[tokio::test]
async fn events_stream_from_the_api_in_process() {
    use axum::response::sse::{Event, Sse};
    let app = axum::Router::new().route(
        "/api/v1/events",
        axum::routing::get(|| async {
            let events = [("run", "{\"run_id\":\"r1\"}"), ("workers", "{}")].map(|(name, data)| {
                Ok::<_, std::convert::Infallible>(Event::default().event(name).data(data))
            });
            Sse::new(tokio_stream::iter(events))
        }),
    );
    let api = Client::new(RouterTransport::new(app), Duration::ZERO, Duration::ZERO);
    let mut events = api.events().await.unwrap();
    let mut seen = Vec::new();
    while let Some(event) = events.next().await {
        let event = event.unwrap();
        seen.push((event.name, event.data));
    }
    assert_eq!(
        seen,
        [
            ("run".to_owned(), "{\"run_id\":\"r1\"}".to_owned()),
            ("workers".to_owned(), "{}".to_owned())
        ]
    );
    // A transport that can't stream says so.
    let scripted = client(vec![]);
    assert!(matches!(
        scripted.events().await,
        Err(ClientError::Unexpected(_))
    ));
}
