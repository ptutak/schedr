//! The UI's static files, embedded in the binary.

use axum::http::header;
use axum::response::IntoResponse;

/// The stylesheet: light and dark themes as CSS variables (see `assets/style.css`).
pub const STYLE: &str = include_str!("../assets/style.css");

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
