//! The `/api/v1` endpoints: reads straight from the database, actions as command rows.
//!
//! A handler's errors are its OpenAPI responses (`ErrorBody` with 400 / 404 / 500); the doc
//! comments are the endpoints' descriptions in the OpenAPI document, so they carry no
//! `# Errors` sections.
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use schedr_model::commands::Command;
use schedr_model::{DagId, TaskId};

use crate::error::ApiError;
use crate::state::ApiState;
use crate::views::{
    AttemptList, CommandView, DagList, DagView, ErrorBody, PauseRequest, RunDetail, RunList,
    RunView, RunsQuery, TaskView, TriggerRequest, WorkerList, WorkerView,
};

#[cfg(test)]
mod tests;

/// How many runs a list returns when the client doesn't say.
pub const DEFAULT_RUNS: u32 = 50;

type ApiResult<T> = Result<Json<T>, ApiError>;
type Accepted = Result<(StatusCode, Json<CommandView>), ApiError>;

/// Every DAG not removed.
#[utoipa::path(get, path = "/api/v1/dags", tag = "dags",
    responses((status = 200, body = DagList)))]
pub async fn list_dags(State(state): State<ApiState>) -> ApiResult<DagList> {
    let dags = state.db.stored_dags().await?;
    Ok(Json(DagList {
        dags: dags
            .iter()
            .filter(|d| !d.removed)
            .map(DagView::from)
            .collect(),
    }))
}

/// Every worker the master has seen, gone ones included.
#[utoipa::path(get, path = "/api/v1/workers", tag = "workers",
    responses((status = 200, body = WorkerList)))]
pub async fn list_workers(State(state): State<ApiState>) -> ApiResult<WorkerList> {
    let workers = state.db.workers().await?;
    Ok(Json(WorkerList {
        workers: workers.iter().map(WorkerView::from).collect(),
    }))
}

/// One DAG.
#[utoipa::path(get, path = "/api/v1/dags/{dag_id}", tag = "dags",
    params(("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 200, body = DagView), (status = 404, body = ErrorBody)))]
pub async fn get_dag(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
) -> ApiResult<DagView> {
    let dag_id = parse_dag_id(&dag_id)?;
    Ok(Json(DagView::from(&known_dag(&state, &dag_id).await?)))
}

/// Pause a DAG: the scheduler creates no runs of it (optionally cancelling those in
/// progress). Applied by the master; poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/pause", tag = "dags",
    params(("dag_id" = String, Path, description = "The DAG's ID")),
    request_body(content = PauseRequest, description = "Optional; `{}` by default"),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn pause_dag(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let request: PauseRequest = optional_body(&body)?;
    submit(
        &state,
        &headers,
        Command::PauseDag {
            dag_id,
            cancel_runs: request.cancel_runs,
        },
    )
    .await
}

/// Unpause a DAG. Applied by the master; poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/unpause", tag = "dags",
    params(("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn unpause_dag(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    headers: HeaderMap,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    submit(&state, &headers, Command::UnpauseDag { dag_id }).await
}

/// A DAG's runs, newest first.
#[utoipa::path(get, path = "/api/v1/dags/{dag_id}/runs", tag = "runs",
    params(("dag_id" = String, Path, description = "The DAG's ID"), RunsQuery),
    responses((status = 200, body = RunList), (status = 404, body = ErrorBody)))]
pub async fn list_runs(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    query: Result<Query<RunsQuery>, QueryRejection>,
) -> ApiResult<RunList> {
    let dag_id = parse_dag_id(&dag_id)?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    known_dag(&state, &dag_id).await?;
    let runs = state
        .db
        .dag_runs(
            &dag_id,
            query.planned_for,
            query.limit.unwrap_or(DEFAULT_RUNS),
        )
        .await?;
    Ok(Json(RunList {
        runs: runs.iter().map(RunView::from).collect(),
    }))
}

/// Start a manual run (`schedr runs trigger`). Applied by the master; poll the command for the
/// decision and the run's ID.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/runs", tag = "runs",
    params(("dag_id" = String, Path, description = "The DAG's ID")),
    request_body(content = TriggerRequest, description = "Optional; now by default"),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn trigger_run(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let request: TriggerRequest = optional_body(&body)?;
    submit(
        &state,
        &headers,
        Command::TriggerRun {
            dag_id,
            planned_for: request.planned_for,
        },
    )
    .await
}

/// A run with its tasks.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}", tag = "runs",
    params(("run_id" = String, Path, description = "The run's ID")),
    responses((status = 200, body = RunDetail), (status = 404, body = ErrorBody)))]
pub async fn get_run(
    State(state): State<ApiState>,
    Path(run_id): Path<String>,
) -> ApiResult<RunDetail> {
    let run = known_run(&state, &run_id).await?;
    let tasks = state.db.run_instances(&run_id).await?;
    Ok(Json(RunDetail {
        run: RunView::from(&run),
        tasks: tasks.iter().map(TaskView::from).collect(),
    }))
}

/// Cancel a run (`schedr runs cancel`). Applied by the master; poll the command for the
/// decision.
#[utoipa::path(post, path = "/api/v1/runs/{run_id}/cancel", tag = "runs",
    params(("run_id" = String, Path, description = "The run's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn cancel_run(
    State(state): State<ApiState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Accepted {
    parse_uuid(&run_id, "run")?;
    submit(&state, &headers, Command::CancelRun { run_id }).await
}

/// The attempts of a run's task, oldest first.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/tasks/{task_id}/attempts", tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID")
    ),
    responses((status = 200, body = AttemptList), (status = 404, body = ErrorBody)))]
pub async fn list_attempts(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
) -> ApiResult<AttemptList> {
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    known_run(&state, &run_id).await?;
    let attempts = state.db.task_attempts(&run_id, &task_id).await?;
    Ok(Json(AttemptList {
        attempts: attempts.iter().map(Into::into).collect(),
    }))
}

/// A command and the master's decision about it.
#[utoipa::path(get, path = "/api/v1/commands/{command_id}", tag = "commands",
    params(("command_id" = String, Path, description = "The command's ID")),
    responses((status = 200, body = CommandView), (status = 404, body = ErrorBody)))]
pub async fn get_command(
    State(state): State<ApiState>,
    Path(command_id): Path<String>,
) -> ApiResult<CommandView> {
    parse_uuid(&command_id, "command")?;
    let command = state
        .db
        .command(&command_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no command `{command_id}`")))?;
    Ok(Json(CommandView::from(&command)))
}

async fn submit(state: &ApiState, headers: &HeaderMap, command: Command) -> Accepted {
    let id = state
        .db
        .insert_command(&command, &state.user(headers), (state.clock)())
        .await?;
    let record = state
        .db
        .command(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no command `{id}`")))?;
    Ok((StatusCode::ACCEPTED, Json(CommandView::from(&record))))
}

async fn known_dag(
    state: &ApiState,
    dag_id: &DagId,
) -> Result<schedr_db::dags::StoredDag, ApiError> {
    state
        .db
        .stored_dags()
        .await?
        .into_iter()
        .find(|d| &d.dag_id == dag_id && !d.removed)
        .ok_or_else(|| ApiError::NotFound(format!("no DAG `{dag_id}`")))
}

async fn known_run(
    state: &ApiState,
    run_id: &str,
) -> Result<schedr_db::reads::RunRecord, ApiError> {
    parse_uuid(run_id, "run")?;
    state
        .db
        .run_record(run_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no run `{run_id}`")))
}

fn parse_dag_id(text: &str) -> Result<DagId, ApiError> {
    DagId::new(text).map_err(|e| ApiError::BadRequest(e.to_string()))
}

/// Run and command IDs are UUIDs; anything else can't exist (and Postgres would refuse it).
fn parse_uuid(text: &str, what: &str) -> Result<(), ApiError> {
    let hex = text.bytes().filter(u8::is_ascii_hexdigit).count();
    let dashes = text.bytes().filter(|b| *b == b'-').count();
    if text.len() == 36 && hex == 32 && dashes == 4 {
        Ok(())
    } else {
        Err(ApiError::BadRequest(format!("`{text}` isn't a {what} ID")))
    }
}

/// A JSON body that may be left out (then the request's defaults apply).
fn optional_body<T: Default + serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|e| ApiError::BadRequest(format!("invalid body: {e}")))
}
