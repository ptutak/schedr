//! The API's description and Swagger UI over it (see PROJECT.md, Web UI and CLI: Swagger UI).
//!
//! Both work behind a gateway that serves schedr under a path prefix and rewrites it away
//! (Istio's `rewrite` with an `X-Forwarded-Prefix` header): Swagger UI's page refers to the
//! description and redirects by relative URLs, and the description names the prefix as its
//! server, so "Try it out" calls the API through the same prefix.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use axum::Json;
use axum::body::Bytes;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use utoipa_swagger_ui::Config;

use crate::openapi_json;

#[cfg(test)]
mod tests;

/// The header a gateway names the path prefix it removed in.
pub const FORWARDED_PREFIX_HEADER: &str = "x-forwarded-prefix";

/// The description, built once.
static DESCRIPTION: LazyLock<serde_json::Value> = LazyLock::new(openapi_json);

/// Swagger UI's settings: the description relative to the page (`/api/v1/docs/`).
static CONFIG: LazyLock<Arc<Config<'static>>> =
    LazyLock::new(|| Arc::new(Config::new(["../openapi.json"])));

/// `X-Forwarded-Prefix` when it is a plain path (`/service/schedr`), without a trailing
/// slash; anything else is ignored, as is a prefix of `/` alone.
#[must_use]
pub fn forwarded_prefix(headers: &HeaderMap) -> Option<String> {
    let prefix = headers.get(FORWARDED_PREFIX_HEADER)?.to_str().ok()?;
    let prefix = prefix.trim_end_matches('/');
    let plain = prefix.starts_with('/')
        && !prefix.contains("//")
        && prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-._~".contains(&b));
    plain.then(|| prefix.to_owned())
}

/// The OpenAPI description, naming the forwarded prefix as its server when there is one
/// (`Vary`, so a cache in front keeps one per prefix).
pub async fn description(headers: HeaderMap) -> Response {
    let doc = match forwarded_prefix(&headers) {
        Some(prefix) => {
            let mut doc = DESCRIPTION.clone();
            doc["servers"] = serde_json::json!([{ "url": prefix }]);
            Json(doc).into_response()
        }
        None => Json(&*DESCRIPTION).into_response(),
    };
    ([(header::VARY, FORWARDED_PREFIX_HEADER)], doc).into_response()
}

/// `/api/v1/docs` to `/api/v1/docs/`, relative, so a prefix in front stays.
pub async fn redirect() -> Redirect {
    Redirect::to("docs/")
}

/// A file of Swagger UI (its page for `/api/v1/docs/`), cached by browsers for an hour (the
/// files change only with schedr's version).
pub async fn file(path: Option<Path<String>>) -> Response {
    let tail = path.as_ref().map_or("", |Path(tail)| tail.as_str());
    match utoipa_swagger_ui::serve(tail, CONFIG.clone()) {
        Ok(Some(file)) => {
            // The built-in files as they are; only the initializer is made per request.
            let bytes = match file.bytes {
                Cow::Borrowed(bytes) => Bytes::from_static(bytes),
                Cow::Owned(bytes) => Bytes::from(bytes),
            };
            (
                [
                    (header::CONTENT_TYPE, file.content_type),
                    (header::CACHE_CONTROL, "public, max-age=3600".to_owned()),
                ],
                bytes,
            )
                .into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(%error, "Swagger UI's file `{tail}` couldn't be served");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
