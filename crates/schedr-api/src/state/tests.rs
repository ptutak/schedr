use axum::http::HeaderValue;
use pretty_assertions::assert_eq;

use super::*;

async fn state(identity_header: Option<&str>) -> ApiState {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    ApiState::new(db, identity_header.map(str::to_owned))
}

#[tokio::test]
async fn the_user_comes_from_the_identity_header_or_is_anonymous() {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-user", HeaderValue::from_static("anna"));
    assert_eq!(state(Some("x-forwarded-user")).await.user(&headers), "anna");
    assert_eq!(state(None).await.user(&headers), ANONYMOUS);
    assert_eq!(state(Some("x-user")).await.user(&headers), ANONYMOUS);
    headers.insert("x-user", HeaderValue::from_static(""));
    assert_eq!(state(Some("x-user")).await.user(&headers), ANONYMOUS);
}

#[tokio::test]
async fn the_state_carries_this_builds_version() {
    let state = state(None).await;
    assert_eq!(state.version, SchedrVersion::current());
    assert_eq!(state.version_text, env!("CARGO_PKG_VERSION"));
}
