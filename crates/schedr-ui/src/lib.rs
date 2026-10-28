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
use axum::routing::{get, post};
use schedr_client::{Client, Transport};

mod actions;
mod ansi;
mod assets;
mod docs;
pub mod graph;
pub mod live;
pub mod page;
mod pages;
mod runs;
mod workers;

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
    /// Who the person using it is (`[ui.auth]`; standalone's: `[webserver.auth]`, one port).
    pub auth: schedr_api::auth::Auth,
}

/// The UI's routes: `/` (the DAGs), `/d/<deployment>/dags/<dag>`, `/runs`,
/// `/d/<deployment>/runs/<run>` (and its tasks' panels and log tails), `/workers`, the
/// actions (POSTs, see `actions`), `/events`, `/time`, `/assets/...`, `/docs/...`.
/// Every answer varies with `X-Forwarded-Prefix` (its links do), so a cache keeps them apart.
pub fn router<T: Transport + Send + Sync + 'static>(ui: Ui<T>) -> Router {
    let relays = Arc::new(live::Relays::new(ui.deployments.len()));
    let state = Arc::new(ui);
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
        .route("/workers", get(workers::list::<T>))
        .route("/time", get(actions::time))
        .route(
            "/d/{deployment}/dags/{dag}/pause",
            post(actions::pause::<T>),
        )
        .route(
            "/d/{deployment}/dags/{dag}/unpause",
            post(actions::unpause::<T>),
        )
        .route(
            "/d/{deployment}/dags/{dag}/trigger",
            post(actions::trigger::<T>),
        )
        .route(
            "/d/{deployment}/runs/{run}/cancel",
            post(actions::cancel_run::<T>),
        )
        .route(
            "/d/{deployment}/runs/{run}/tasks/{task}/{action}",
            post(actions::task::<T>),
        )
        .route(
            "/d/{deployment}/workers/{worker}/{action}",
            post(actions::worker::<T>),
        )
        .route(
            "/d/{deployment}/commands/{command}",
            get(actions::command::<T>),
        )
        .route("/events", get(live::events::<T>))
        .route(
            "/d/{deployment}/runs/{run}/tasks/{task}/tail",
            get(live::tail::<T>),
        )
        .route("/assets/style.css", get(assets::style))
        .route(assets::SSE_PATH, get(assets::sse))
        .route(assets::HTMX_PATH, get(assets::htmx))
        .route("/docs", get(docs::to_index))
        .route("/docs/", get(docs::index::<T>))
        .route("/docs/{*path}", get(docs::file::<T>))
        .fallback(pages::not_found::<T>)
        .layer(axum::Extension(relays))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            signed_in::<T>,
        ))
        .layer(axum::middleware::from_fn(actions::guard))
        .layer(axum::middleware::map_response(vary_on_prefix))
        .with_state(state)
}

/// Who the person is, by the UI's `auth` (refused: 401, a sign-in prompt in `basic` mode),
/// their name carried to the webservers with every call made for this request
/// ([`schedr_client::USER`]). The stylesheet and scripts are served to anyone (the sign-in
/// page needs them).
async fn signed_in<T: Transport>(
    axum::extract::State(ui): axum::extract::State<Arc<Ui<T>>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    if request.uri().path().starts_with("/assets/") {
        return next.run(request).await;
    }
    match ui.auth.user(request.headers(), false) {
        Ok(user) => schedr_client::USER.scope(user, next.run(request)).await,
        Err(refused) => {
            let page = page::Page::new(request.headers(), ui.docs_dir.is_some());
            let htmx = request.headers().contains_key("hx-request");
            ui.auth.refusal(&refused, |message| {
                if htmx {
                    axum::response::Html(format!(
                        "<div class=\"banner warning\"><strong>Not signed in</strong>: {}</div>",
                        ansi::to_html(message)
                    ))
                    .into_response()
                } else {
                    pages::error_page(
                        &page,
                        "Not signed in",
                        message,
                        axum::http::StatusCode::UNAUTHORIZED,
                    )
                }
            })
        }
    }
}

/// Every answer varies with the prefix, and may not be shown in another site's frame (its
/// buttons can't be clicked through one: clickjacking).
async fn vary_on_prefix(mut response: axum::response::Response) -> axum::response::Response {
    use axum::http::{HeaderValue, header};
    let headers = response.headers_mut();
    headers.append(header::VARY, HeaderValue::from_static(page::PREFIX_HEADER));
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("frame-ancestors 'none'"),
    );
    response
}
