//! The UI's static files, embedded in the binary.

use axum::http::header;
use axum::response::IntoResponse;

/// The stylesheet: light and dark themes as CSS variables (see `assets/style.css`).
pub const STYLE: &str = include_str!("../assets/style.css");

/// htmx, vendored (see PROJECT.md, Libraries): its version in its path, so it is cached for
/// good.
pub const HTMX: &str = include_str!("../assets/htmx.min.js");

/// Where htmx is served (the version in the name: a new one is a new URL).
pub const HTMX_PATH: &str = "/assets/htmx-2.0.11.min.js";

/// `GET /assets/htmx-<version>.min.js`.
pub async fn htmx() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        HTMX,
    )
}

/// `GET /assets/style.css`.
pub async fn style() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        STYLE,
    )
}
