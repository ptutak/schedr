use axum::body::to_bytes;
use pretty_assertions::assert_eq;

use super::*;

async fn body(error: ApiError) -> (StatusCode, serde_json::Value) {
    let response = error.into_response();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn errors_are_json_with_their_status() {
    assert_eq!(
        body(ApiError::BadRequest("bad".to_owned())).await,
        (StatusCode::BAD_REQUEST, serde_json::json!({"error": "bad"}))
    );
    assert_eq!(
        body(ApiError::NotFound("no DAG `x`".to_owned())).await,
        (
            StatusCode::NOT_FOUND,
            serde_json::json!({"error": "no DAG `x`"})
        )
    );
    assert_eq!(
        body(ApiError::Unavailable("worker `w1` is lost".to_owned())).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error": "worker `w1` is lost"})
        )
    );
}

#[tokio::test]
async fn database_errors_hide_their_details() {
    let error = ApiError::Db(DbError::InvalidData {
        what: "runs.kind",
        reason: "secret detail".to_owned(),
    });
    assert_eq!(
        body(error).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": "the database failed; see the webserver's log"})
        )
    );
}
