use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

fn with_prefix(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        FORWARDED_PREFIX_HEADER,
        HeaderValue::from_str(value).unwrap(),
    );
    headers
}

#[rstest]
#[case("/service/schedr", Some("/service/schedr"))]
#[case("/service/schedr/", Some("/service/schedr"))]
#[case("/a-b_c.d~e/f", Some("/a-b_c.d~e/f"))]
#[case("/", None)]
#[case("", None)]
#[case("service/schedr", None)]
#[case("//evil.example.com", None)]
#[case("https://evil.example.com", None)]
#[case("/a\"b", None)]
#[case("/a b", None)]
fn only_a_plain_path_is_taken_as_the_prefix(#[case] value: &str, #[case] expected: Option<&str>) {
    assert_eq!(
        forwarded_prefix(&with_prefix(value)).as_deref(),
        expected,
        "{value:?}"
    );
}

#[test]
fn no_header_is_no_prefix() {
    assert_eq!(forwarded_prefix(&HeaderMap::new()), None);
}

async fn json(response: Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn the_description_gets_the_prefix_as_its_server() {
    let response = description(with_prefix("/p")).await;
    assert_eq!(response.headers()[header::VARY], FORWARDED_PREFIX_HEADER);
    assert_eq!(
        json(response).await["servers"],
        serde_json::json!([{"url": "/p"}])
    );
    let response = description(HeaderMap::new()).await;
    assert_eq!(response.headers()[header::VARY], FORWARDED_PREFIX_HEADER);
    let doc = json(response).await;
    assert_eq!(doc.get("servers"), None);
    assert_eq!(doc, openapi_json());
}

#[tokio::test]
async fn the_redirect_is_relative() {
    let response = redirect().await.into_response();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["location"], "docs/");
}

#[tokio::test]
async fn files_are_served_with_their_type_or_not_found() {
    let page = file(None).await;
    assert_eq!(page.status(), StatusCode::OK);
    assert_eq!(page.headers()[header::CONTENT_TYPE], "text/html");
    assert_eq!(
        page.headers()[header::CACHE_CONTROL],
        "public, max-age=3600"
    );
    let script = file(Some(Path("swagger-ui-bundle.js".to_owned()))).await;
    assert_eq!(script.status(), StatusCode::OK);
    assert_eq!(script.headers()[header::CONTENT_TYPE], "text/javascript");
    let missing = file(Some(Path("missing.css".to_owned()))).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}
