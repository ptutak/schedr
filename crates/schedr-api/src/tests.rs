use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use schedr_db::Database;
use tower::ServiceExt;

use super::*;
use crate::version::{CLIENT_VERSION_HEADER, SERVER_VERSION_HEADER};

async fn app() -> Router {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    router(ApiState::new(db, None))
}

async fn call(app: &Router, uri: &str, client: Option<&str>) -> axum::response::Response {
    let mut request = Request::builder().uri(uri);
    if let Some(version) = client {
        request = request.header(CLIENT_VERSION_HEADER, version);
    }
    app.clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn every_response_carries_the_servers_version() {
    let app = app().await;
    for uri in ["/api/v1/dags", "/api/v1/nothing-here"] {
        let response = call(&app, uri, None).await;
        assert_eq!(
            response.headers()[SERVER_VERSION_HEADER],
            env!("CARGO_PKG_VERSION")
        );
    }
}

#[tokio::test]
async fn a_client_of_another_major_is_refused() {
    let app = app().await;
    let response = call(&app, "/api/v1/dags", Some("99.0.0")).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.headers().contains_key(SERVER_VERSION_HEADER));
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        error["error"]
            .as_str()
            .unwrap()
            .contains("this client is schedr 99.0"),
        "{error}"
    );
    let same = call(&app, "/api/v1/dags", Some(env!("CARGO_PKG_VERSION"))).await;
    assert_eq!(same.status(), StatusCode::OK);
}

#[tokio::test]
async fn the_openapi_description_is_served_and_lists_every_endpoint() {
    let app = app().await;
    let response = call(&app, "/api/v1/openapi.json", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let served: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(served, openapi_json());
    let paths: Vec<&str> = served["paths"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        paths,
        [
            "/api/v1/commands/{command_id}",
            "/api/v1/dags",
            "/api/v1/dags/{dag_id}",
            "/api/v1/dags/{dag_id}/pause",
            "/api/v1/dags/{dag_id}/runs",
            "/api/v1/dags/{dag_id}/unpause",
            "/api/v1/runs/{run_id}",
            "/api/v1/runs/{run_id}/cancel",
            "/api/v1/runs/{run_id}/tasks/{task_id}/attempts",
            "/api/v1/runs/{run_id}/tasks/{task_id}/attempts/{try_number}/log",
            "/api/v1/runs/{run_id}/tasks/{task_id}/cancel",
            "/api/v1/runs/{run_id}/tasks/{task_id}/retry",
            "/api/v1/workers",
            "/api/v1/workers/{worker_id}/drain",
            "/api/v1/workers/{worker_id}/resume",
        ]
    );
}

#[tokio::test]
async fn requests_are_counted_by_route_template_and_status() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let metrics = metrics::ApiMetrics::default();
    let app = router(ApiState::new(db, None).with_metrics(metrics.clone()));
    call(&app, "/api/v1/dags", None).await;
    call(&app, "/api/v1/dags/ghost", None).await;
    call(&app, "/api/v1/dags/other", None).await;
    let count = |route: &str, status: u16| {
        metrics
            .requests
            .get_or_create(&metrics::RequestLabels {
                route: route.to_owned(),
                status,
            })
            .get()
    };
    assert_eq!(count("/api/v1/dags", 200), 1);
    // By template: the IDs in paths never become labels.
    assert_eq!(count("/api/v1/dags/{dag_id}", 404), 2);
    // A client of another major version is refused, and counted.
    call(&app, "/api/v1/dags", Some("99.0.0")).await;
    assert_eq!(count("/api/v1/dags", 400), 1);
    call(&app, "/nowhere", None).await;
    assert_eq!(count("unmatched", 404), 1);
}
