//! The web UI: server-rendered HTML pages over webservers' REST APIs (see PROJECT.md, Web UI).
//!
//! It is `schedr ui`, a role of its own talking only to webservers' JSON APIs (decision 21B),
//! one per deployment in its list (one the common case); `schedr standalone` serves it too,
//! calling its own API in process. Pages are askama templates (checked at compile time) with
//! one stylesheet of our own, light and dark (decision 26A and the user's style); times show
//! in UTC and in the browser's zone (a cookie its inline script sets); links follow
//! `X-Forwarded-Prefix`; the documentation is served at `/docs` from the image (28A).

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use schedr_client::{Client, Transport};

mod ansi;
mod assets;
mod docs;
pub mod graph;
pub mod page;
mod pages;
mod runs;

#[cfg(test)]
mod tests;

/// One deployment the UI shows: a webserver's API, by the name the UI gives it.
#[derive(Debug)]
pub struct Deployment<T> {
    /// Its name in the UI and in URLs (`/d/<name>/...`).
    pub name: String,
    /// Its webserver's API.
    pub client: Client<T>,
}

/// What the UI serves.
#[derive(Debug)]
pub struct Ui<T> {
    /// The deployments it shows, in the order given (one the common case).
    pub deployments: Vec<Deployment<T>>,
    /// Where the documentation site is (`[ui] docs_dir`); none: no `/docs`.
    pub docs_dir: Option<PathBuf>,
}

/// The UI's routes: `/` (the DAGs), `/d/<deployment>/dags/<dag>`, `/runs`,
/// `/d/<deployment>/runs/<run>` (and its tasks' panels), `/assets/...`, `/docs/...`.
/// Every answer varies with `X-Forwarded-Prefix` (its links do), so a cache keeps them apart.
pub fn router<T: Transport + Send + Sync + 'static>(ui: Ui<T>) -> Router {
    Router::new()
        .route("/", get(pages::dags::<T>))
        .route("/d/{deployment}/dags/{dag}", get(pages::dag::<T>))
        .route("/runs", get(runs::list::<T>))
        .route("/d/{deployment}/runs/{run}", get(runs::run::<T>))
        .route(
            "/d/{deployment}/runs/{run}/tasks/{task}",
            get(runs::panel::<T>),
        )
        .route(
            "/d/{deployment}/runs/{run}/tasks/{task}/output.json",
            get(runs::output_download::<T>),
        )
        .route("/assets/style.css", get(assets::style))
        .route(assets::HTMX_PATH, get(assets::htmx))
        .route("/docs", get(docs::to_index))
        .route("/docs/", get(docs::index::<T>))
        .route("/docs/{*path}", get(docs::file::<T>))
        .fallback(pages::not_found::<T>)
        .layer(axum::middleware::map_response(vary_on_prefix))
        .with_state(Arc::new(ui))
}

async fn vary_on_prefix(mut response: axum::response::Response) -> axum::response::Response {
    response.headers_mut().append(
        axum::http::header::VARY,
        axum::http::HeaderValue::from_static(page::PREFIX_HEADER),
    );
    response
}
