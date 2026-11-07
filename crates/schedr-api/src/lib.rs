//! The schedr webserver: REST API and web UI.
//!
//! So far the REST API subset the CLI needs (`/api/v1`: DAGs, runs, tasks, attempts and their
//! logs, commands, workers), with version headers, a generated OpenAPI description and Swagger UI
//! over it at `/api/v1/docs` (see PROJECT.md, Web UI and CLI). Reads come straight from the
//! database; actions are command rows the master applies.

use axum::Router;
use axum::routing::{get, post};
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityRequirement, SecurityScheme};
use utoipa::{Modify, OpenApi};

pub mod auth;
pub mod docs;
pub mod error;
pub mod events;
pub mod logs;
pub mod metrics;
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
        routes::list_run_resources,
        routes::get_run_resource,
        routes::list_switched_off_sensors,
        routes::switch_on_sensor,
        routes::list_triggers,
        routes::dag_trigger,
        routes::trigger_events,
        routes::restart_trigger,
        routes::switch_on_trigger,
        routes::list_runs,
        routes::trigger_run,
        routes::list_all_runs,
        routes::get_run,
        routes::get_run_dag,
        routes::list_run_commands,
        routes::cancel_run,
        routes::list_attempts,
        routes::get_log,
        routes::get_output,
        routes::cancel_task,
        routes::retry_task,
        routes::get_command,
        routes::list_workers,
        routes::get_deployment,
        routes::list_import_errors,
        events::events,
        routes::drain_worker,
        routes::resume_worker,
        routes::list_connections,
        routes::check_connections,
        routes::test_connection,
    ),
    tags(
        (name = "dags", description = "DAGs and pausing"),
        (name = "runs", description = "Runs, their tasks and attempts"),
        (name = "commands", description = "Actions and the master's decisions"),
        (name = "workers", description = "The worker registry"),
        (name = "connections", description = "Checking and testing connections on a worker"),
    ),
    modifiers(&BearerToken)
)]
pub struct ApiDoc;

/// Where Swagger UI is served (always: it shows only what `openapi.json` already does).
pub const DOCS_PATH: &str = "/api/v1/docs";

/// Where the OpenAPI description is served.
pub const OPENAPI_PATH: &str = "/api/v1/openapi.json";

/// An optional bearer token on every operation: schedr checks none itself (until the UI's
/// auth modes, milestone M3), but a mesh or proxy in front may require one (e.g. Istio with a
/// Keycloak token), and Swagger UI's "Authorize" sends it.
struct BearerToken;

impl Modify for BearerToken {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let mut http = Http::new(HttpAuthScheme::Bearer);
        http.description = Some(
            "A token for a mesh or proxy in front of schedr that requires one; schedr itself \
             checks none."
                .to_owned(),
        );
        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme("bearer", SecurityScheme::Http(http));
        // The empty requirement first: no token is also accepted.
        openapi.security = Some(vec![
            SecurityRequirement::default(),
            SecurityRequirement::new("bearer", Vec::<String>::new()),
        ]);
    }
}

/// The OpenAPI description as committed under `schema/` (pretty JSON).
///
/// # Panics
///
/// Never in practice: the generated description always serializes.
#[must_use]
pub fn openapi_json() -> serde_json::Value {
    serde_json::to_value(ApiDoc::openapi()).expect("the OpenAPI description serializes")
}

/// The routes of `schedr triggers`.
fn trigger_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/v1/triggers", get(routes::list_triggers))
        .route("/api/v1/dags/{dag_id}/trigger", get(routes::dag_trigger))
        .route(
            "/api/v1/dags/{dag_id}/trigger/events",
            get(routes::trigger_events),
        )
        .route(
            "/api/v1/dags/{dag_id}/trigger/restart",
            post(routes::restart_trigger),
        )
        .route(
            "/api/v1/dags/{dag_id}/trigger/switch-on",
            post(routes::switch_on_trigger),
        )
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
        .route("/api/v1/runs", get(routes::list_all_runs))
        .route("/api/v1/events", get(events::events))
        .route("/api/v1/runs/{run_id}", get(routes::get_run))
        .route("/api/v1/runs/{run_id}/dag", get(routes::get_run_dag))
        .route(
            "/api/v1/runs/{run_id}/commands",
            get(routes::list_run_commands),
        )
        .route(
            "/api/v1/runs/{run_id}/tasks/{task_id}/output",
            get(routes::get_output),
        )
        .route("/api/v1/runs/{run_id}/cancel", post(routes::cancel_run))
        .route(
            "/api/v1/runs/{run_id}/resources",
            get(routes::list_run_resources),
        )
        .route(
            "/api/v1/runs/{run_id}/resources/{resource}",
            get(routes::get_run_resource),
        )
        .merge(trigger_routes())
        .route(
            "/api/v1/sensors/switched-off",
            get(routes::list_switched_off_sensors),
        )
        .route(
            "/api/v1/dags/{dag_id}/sensors/{sensor}/switch-on",
            post(routes::switch_on_sensor),
        )
        .route(
            "/api/v1/runs/{run_id}/tasks/{task_id}/attempts",
            get(routes::list_attempts),
        )
        .route(
            "/api/v1/runs/{run_id}/tasks/{task_id}/attempts/{try_number}/log",
            get(routes::get_log),
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
        .route("/api/v1/deployment", get(routes::get_deployment))
        .route("/api/v1/import-errors", get(routes::list_import_errors))
        .route(
            "/api/v1/workers/{worker_id}/drain",
            post(routes::drain_worker),
        )
        .route(
            "/api/v1/workers/{worker_id}/resume",
            post(routes::resume_worker),
        )
        .route("/api/v1/connections", get(routes::list_connections))
        .route("/api/v1/connections/check", post(routes::check_connections))
        .route(
            "/api/v1/connections/{name}/test",
            post(routes::test_connection),
        )
        .route(OPENAPI_PATH, get(docs::description))
        .route(DOCS_PATH, get(docs::redirect))
        .route("/api/v1/docs/", get(docs::file))
        .route("/api/v1/docs/{*path}", get(docs::file))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require,
        ))
        .layer(axum::middleware::from_fn(auth::refuse_cross_site))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            version::versions,
        ))
        // Outside the version check, so the requests it refuses are counted too.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            metrics::count_requests,
        ))
        .with_state(state)
}
