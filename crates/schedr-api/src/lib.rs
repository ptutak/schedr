//! The schedr webserver: REST API and web UI.
//!
//! So far the REST API subset the CLI needs (`/api/v1`: DAGs, runs, tasks, attempts,
//! commands), with version headers and a generated OpenAPI description (see PROJECT.md, Web
//! UI and CLI). Reads come straight from the database; actions are command rows the master
//! applies.

use axum::routing::{get, post};
use axum::{Json, Router};
use utoipa::OpenApi;

pub mod error;
pub mod routes;
pub mod state;
pub mod version;
pub mod views;

#[cfg(test)]
mod tests;

pub use error::ApiError;
pub use state::ApiState;

/// The OpenAPI description of `/api/v1`, generated from the handlers and views.
#[derive(OpenApi)]
#[openapi(
    info(title = "schedr", description = "The schedr REST API."),
    paths(
        routes::list_dags,
        routes::get_dag,
        routes::pause_dag,
        routes::unpause_dag,
        routes::list_runs,
        routes::trigger_run,
        routes::get_run,
        routes::cancel_run,
        routes::list_attempts,
        routes::cancel_task,
        routes::retry_task,
        routes::get_command,
        routes::list_workers,
    ),
    tags(
        (name = "dags", description = "DAGs and pausing"),
        (name = "runs", description = "Runs, their tasks and attempts"),
        (name = "commands", description = "Actions and the master's decisions"),
        (name = "workers", description = "The worker registry"),
    )
)]
pub struct ApiDoc;

/// The OpenAPI description as committed under `schema/` (pretty JSON).
///
/// # Panics
///
/// Never in practice: the generated description always serializes.
#[must_use]
pub fn openapi_json() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).expect("the OpenAPI description serializes")
}

/// The API's routes over `state`.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/api/v1/dags", get(routes::list_dags))
        .route("/api/v1/dags/{dag_id}", get(routes::get_dag))
        .route("/api/v1/dags/{dag_id}/pause", post(routes::pause_dag))
        .route("/api/v1/dags/{dag_id}/unpause", post(routes::unpause_dag))
        .route(
            "/api/v1/dags/{dag_id}/runs",
            get(routes::list_runs).post(routes::trigger_run),
        )
        .route("/api/v1/runs/{run_id}", get(routes::get_run))
        .route("/api/v1/runs/{run_id}/cancel", post(routes::cancel_run))
        .route(
            "/api/v1/runs/{run_id}/tasks/{task_id}/attempts",
            get(routes::list_attempts),
        )
        .route(
            "/api/v1/runs/{run_id}/tasks/{task_id}/cancel",
            post(routes::cancel_task),
        )
        .route(
            "/api/v1/runs/{run_id}/tasks/{task_id}/retry",
            post(routes::retry_task),
        )
        .route("/api/v1/commands/{command_id}", get(routes::get_command))
        .route("/api/v1/workers", get(routes::list_workers))
        .route(
            "/api/v1/openapi.json",
            get(|| async { Json(openapi_json()) }),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            version::versions,
        ))
        .with_state(state)
}
