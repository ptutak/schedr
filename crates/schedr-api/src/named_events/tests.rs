use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use schedr_db::Database;
use serde_json::{Value as Json, json};
use tower::ServiceExt;

use crate::router;
use crate::state::ApiState;

async fn app(max_event_size: usize) -> axum::Router {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let state =
        ApiState::new(db, Some("x-forwarded-user".to_owned())).with_max_event_size(max_event_size);
    router(state)
}

async fn post(app: &axum::Router, body: &Json, key: Option<&str>) -> (StatusCode, Json) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/events")
        .header("x-forwarded-user", "anna")
        .header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    let request = request.body(Body::from(body.to_string())).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Json::Null))
}

async fn list(app: &axum::Router, query: &str) -> Json {
    let request = Request::builder()
        .uri(format!("/api/v1/events{query}"))
        .header("x-forwarded-user", "anna")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn an_event_is_posted_each_time_and_a_retry_with_its_key_is_the_same() {
    let app = app(1024).await;
    let event = json!({"name": "payment-completed", "key": "order-123", "data": {"amount": 40}});
    let (status, first) = post(&app, &event, Some("k1")).await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    assert_eq!(first["posted_by"], "anna");
    assert_eq!(first["data"], json!({"amount": 40}));
    let (status, again) = post(&app, &event, Some("k1")).await;
    assert_eq!((status, &again["id"]), (StatusCode::OK, &first["id"]));
    let other = json!({"name": "payment-completed", "key": "order-124"});
    let (status, _) = post(&app, &other, Some("k1")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    // The same name and key again, without a key of the client's: a new event.
    let (status, repeat) = post(&app, &event, None).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_ne!(repeat["id"], first["id"]);
    let listed = list(&app, "?name=payment-completed").await;
    assert_eq!(listed["events"].as_array().unwrap().len(), 2);
    assert_eq!(list(&app, "?name=other").await["events"], json!([]));
}

#[tokio::test]
async fn what_an_event_cant_be_is_refused() {
    let app = app(64).await;
    let cases = [
        (json!({"name": "a b", "key": "k"}), StatusCode::BAD_REQUEST),
        (json!({"name": "paid", "key": ""}), StatusCode::BAD_REQUEST),
        (
            json!({"name": "paid", "key": "k\n"}),
            StatusCode::BAD_REQUEST,
        ),
        (json!({"name": "paid"}), StatusCode::BAD_REQUEST),
        (
            json!({"name": "paid", "key": "k", "extra": 1}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name": "paid", "key": "k", "data": "\u{0}"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name": "paid", "key": "k", "data": "x".repeat(100)}),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ];
    for (event, want) in cases {
        let (status, body) = post(&app, &event, None).await;
        assert_eq!(status, want, "{event}: {body}");
    }
    assert_eq!(list(&app, "").await["events"], json!([]));
}

#[tokio::test]
async fn a_listed_event_s_data_is_masked() {
    let app = app(1024).await;
    let event = json!({
        "name": "connected", "key": "k",
        "data": {"url": "postgres://etl:hunter2@db/dw"}
    });
    let (status, _) = post(&app, &event, None).await;
    assert_eq!(status, StatusCode::CREATED);
    let listed = list(&app, "").await;
    let shown = listed["events"][0]["data"]["url"].as_str().unwrap();
    assert!(!shown.contains("hunter2"), "{shown}");
}

#[tokio::test]
async fn an_event_up_to_the_configured_size_is_taken_over_axum_s_default() {
    // Codex's review: axum refused bodies over 2 MiB before `[events] max_size` was read.
    let app = app(4 * 1024 * 1024).await;
    let event = json!({"name": "big", "key": "k", "data": "x".repeat(3 * 1024 * 1024)});
    let (status, body) = post(&app, &event, None).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn a_retry_with_another_moment_of_its_own_is_another_event() {
    // Codex's review: a changed `at` came back as the first event.
    let app = app(1024).await;
    let first = json!({"name": "paid", "key": "o-1", "at": "2026-11-01T10:00:00Z"});
    let (status, _) = post(&app, &first, Some("k1")).await;
    assert_eq!(status, StatusCode::CREATED);
    let moved = json!({"name": "paid", "key": "o-1", "at": "2026-11-02T10:00:00Z"});
    let (status, _) = post(&app, &moved, Some("k1")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = post(&app, &first, Some("k1")).await;
    assert_eq!(status, StatusCode::OK, "the same again is the retry it is");
    let unset = json!({"name": "paid", "key": "o-2"});
    let (status, _) = post(&app, &unset, Some("k2")).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = post(&app, &unset, Some("k2")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "no moment of its own: \"now\" differs on a retry"
    );
}

#[tokio::test]
async fn an_event_for_a_moment_ahead_is_refused() {
    // Grok's review: a run planned years ahead started at once.
    let app = app(1024).await;
    let event = json!({"name": "paid", "key": "o-1", "at": "2099-01-01T00:00:00Z"});
    let (status, body) = post(&app, &event, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let key = json!({"name": "paid", "key": "a\u{2028}b"});
    let (status, _) = post(&app, &key, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A bad event's error shows none of its values, quoted or not (decision 134, B194).
#[tokio::test]
async fn a_bad_event_s_error_shows_none_of_its_values() {
    let app = app(1 << 20).await;
    for body in [
        json!({"name": "deploy", "key": 31_415_926, "data": {}}),
        json!({"name": "deploy", "key": "k", "data": {}, "at": "hunter2-not-a-time"}),
        json!({"name": ["hunter2"], "key": "k", "data": {}}),
    ] {
        let (status, answer) = post(&app, &body, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let error = answer["error"].as_str().unwrap();
        assert!(error.starts_with("the event: "), "{error}");
        for value in ["31415926", "hunter2"] {
            assert!(!error.contains(value), "{body}: {error}");
        }
    }
}
