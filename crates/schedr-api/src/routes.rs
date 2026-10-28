//! The `/api/v1` endpoints: reads straight from the database, actions as command rows.
//!
//! A handler's errors are its OpenAPI responses (`ErrorBody` with 400 / 404 / 503 / 500); the doc
//! comments are the endpoints' descriptions in the OpenAPI document, so they carry no
//! `# Errors` sections.
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use schedr_db::reads::RunFilter;
use schedr_model::commands::Command;
use schedr_model::logs::{MAX_CHUNK, MIN_CHUNK};
use schedr_model::{DagId, TaskId};

use crate::auth::User;
use crate::error::ApiError;
use crate::logs::{LogFetchError, text_of};
use crate::state::ApiState;
use crate::views::{
    AllRunsQuery, AttemptList, CommandList, CommandView, DagList, DagView, ErrorBody, LogChunk,
    LogQuery, OutputView, PauseRequest, RunDetail, RunList, RunView, RunsQuery, TaskView,
    TriggerRequest, WorkerList, WorkerView,
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

/// Drain a worker for maintenance (`schedr workers drain`): no new tasks, its running ones
/// finish; kept across restarts until resumed. Applied by the master; poll the command for
/// the decision.
#[utoipa::path(post, path = "/api/v1/workers/{worker_id}/drain", tag = "workers",
    params(("worker_id" = String, Path, description = "The worker's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn drain_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<String>,
    Extension(user): Extension<User>,
) -> Accepted {
    submit(&state, &user, Command::DrainWorker { worker_id }).await
}

/// Let a drained worker take tasks again (`schedr workers resume`). Applied by the master;
/// poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/workers/{worker_id}/resume", tag = "workers",
    params(("worker_id" = String, Path, description = "The worker's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn resume_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<String>,
    Extension(user): Extension<User>,
) -> Accepted {
    submit(&state, &user, Command::ResumeWorker { worker_id }).await
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
    Extension(user): Extension<User>,
    body: Bytes,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let request: PauseRequest = optional_body(&body)?;
    submit(
        &state,
        &user,
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
    Extension(user): Extension<User>,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    submit(&state, &user, Command::UnpauseDag { dag_id }).await
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
    Extension(user): Extension<User>,
    body: Bytes,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let request: TriggerRequest = optional_body(&body)?;
    submit(
        &state,
        &user,
        Command::TriggerRun {
            dag_id,
            planned_for: request.planned_for,
        },
    )
    .await
}

/// Runs of every DAG, newest triggered first (then by ID); filtered by DAG and state, paged
/// with `before` and `before_run` (the last listed run's `triggered_at` and ID).
#[utoipa::path(get, path = "/api/v1/runs", tag = "runs",
    params(AllRunsQuery),
    responses((status = 200, body = RunList), (status = 400, body = ErrorBody)))]
pub async fn list_all_runs(
    State(state): State<ApiState>,
    query: Result<Query<AllRunsQuery>, QueryRejection>,
) -> ApiResult<RunList> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    if query.before_run.is_some() && query.before.is_none() {
        return Err(ApiError::BadRequest(
            "`before_run` needs `before` (the run's `triggered_at`)".to_owned(),
        ));
    }
    let before_run = query
        .before_run
        .as_deref()
        .map(|id| parse_uuid(id, "run"))
        .transpose()?;
    let runs = state
        .db
        .runs(&RunFilter {
            dag_id: query.dag,
            state: query.state,
            before: query.before,
            before_run,
            limit: query.limit.unwrap_or(DEFAULT_RUNS),
        })
        .await?;
    Ok(Json(RunList {
        runs: runs.iter().map(RunView::from).collect(),
    }))
}

/// The DAG a run ran with: its version's DAG JSON (tasks, dependencies, settings), for its
/// graph.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/dag", tag = "runs",
    params(("run_id" = String, Path, description = "The run's ID")),
    responses((status = 200, body = Object), (status = 404, body = ErrorBody)))]
pub async fn get_run_dag(
    State(state): State<ApiState>,
    Path(run_id): Path<String>,
) -> ApiResult<serde_json::Value> {
    let run_id = known_run(&state, &run_id).await?.id;
    state
        .db
        .run_dag_json(&run_id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("no run `{run_id}`")))
}

/// A task's stored output in a run, with its outcome; 404 while it has none.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/tasks/{task_id}/output", tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID")
    ),
    responses((status = 200, body = OutputView), (status = 404, body = ErrorBody)))]
pub async fn get_output(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
) -> ApiResult<OutputView> {
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let run_id = known_run(&state, &run_id).await?.id;
    let (outcome, value) = state
        .db
        .run_task_output(&run_id, &task_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no output of `{task_id}` in run `{run_id}`")))?;
    Ok(Json(OutputView { outcome, value }))
}

/// The commands about a run, oldest first: the trigger that made it, and cancelling it or
/// cancelling or retrying its tasks, with the master's decisions.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/commands", tag = "runs",
    params(("run_id" = String, Path, description = "The run's ID")),
    responses((status = 200, body = CommandList), (status = 404, body = ErrorBody)))]
pub async fn list_run_commands(
    State(state): State<ApiState>,
    Path(run_id): Path<String>,
) -> ApiResult<CommandList> {
    let run_id = known_run(&state, &run_id).await?.id;
    let commands = state.db.run_commands(&run_id).await?;
    Ok(Json(CommandList {
        commands: commands.iter().map(CommandView::from).collect(),
    }))
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
    let run_id = &run.id;
    let tasks = state.db.run_instances(run_id).await?;
    Ok(Json(RunDetail {
        run: RunView::from(&run),
        tasks: tasks.iter().map(TaskView::from).collect(),
        code_hashes: state.db.run_code_hashes(run_id).await?,
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
    Extension(user): Extension<User>,
) -> Accepted {
    let run_id = parse_uuid(&run_id, "run")?;
    submit(&state, &user, Command::CancelRun { run_id }).await
}

/// Cancel one task of a run (`schedr tasks cancel`): stopped if it runs, cancelled at once if
/// it hasn't started; the tasks downstream of it are cancelled. Applied by the master; poll
/// the command for the decision.
#[utoipa::path(post, path = "/api/v1/runs/{run_id}/tasks/{task_id}/cancel", tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID")
    ),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn cancel_task(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
    Extension(user): Extension<User>,
) -> Accepted {
    let run_id = parse_uuid(&run_id, "run")?;
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    submit(&state, &user, Command::CancelTask { run_id, task_id }).await
}

/// Retry an `unschedulable` task of an unfinished run (`schedr tasks retry`). Applied by the
/// master; poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/runs/{run_id}/tasks/{task_id}/retry", tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID")
    ),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn retry_task(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
    Extension(user): Extension<User>,
) -> Accepted {
    let run_id = parse_uuid(&run_id, "run")?;
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    submit(&state, &user, Command::RetryTask { run_id, task_id }).await
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
    let run_id = known_run(&state, &run_id).await?.id;
    let attempts = state.db.task_attempts(&run_id, &task_id).await?;
    Ok(Json(AttemptList {
        attempts: attempts.iter().map(Into::into).collect(),
    }))
}

/// A part of an attempt's log, read from the worker that ran it; read on from `next` to follow
/// a running task. 404 when the attempt has no log (yet), or the worker no longer has it; 503
/// when the worker is gone or can't be reached.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/tasks/{task_id}/attempts/{try_number}/log",
    tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID"),
        ("try_number" = u32, Path, description = "The attempt's try number"),
        LogQuery
    ),
    responses(
        (status = 200, body = LogChunk),
        (status = 404, body = ErrorBody),
        (status = 503, body = ErrorBody)
    ))]
pub async fn get_log(
    State(state): State<ApiState>,
    Path((run_id, task_id, try_number)): Path<(String, String, u32)>,
    query: Result<Query<LogQuery>, QueryRejection>,
) -> ApiResult<LogChunk> {
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let run_id = known_run(&state, &run_id).await?.id;
    let attempt = state
        .db
        .task_attempts(&run_id, &task_id)
        .await?
        .into_iter()
        .find(|a| a.try_number == try_number)
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "task `{task_id}` of run `{run_id}` has no try {try_number}"
            ))
        })?;
    let attempt_id = attempt.id;
    let worker_id = match (attempt.worker_id, attempt.started_at, attempt.status) {
        (Some(worker_id), Some(_), _) => worker_id,
        (_, _, None) => {
            return Err(ApiError::NotFound(format!(
                "try {try_number} of task `{task_id}` hasn't started yet: no log so far"
            )));
        }
        (_, _, Some(_)) => {
            return Err(ApiError::NotFound(format!(
                "try {try_number} of task `{task_id}` never started, so it has no log"
            )));
        }
    };
    let endpoint = match state.db.worker_log_endpoint(&worker_id).await? {
        Some((worker_state, _)) if worker_state.is_gone() => {
            state.metrics.log_error("worker_gone");
            return Err(ApiError::Unavailable(format!(
                "worker `{worker_id}` is {worker_state}: the log of try {try_number} is \
                 unavailable"
            )));
        }
        Some((_, endpoint)) if !endpoint.is_empty() => endpoint,
        _ => {
            state.metrics.log_error("no_endpoint");
            return Err(ApiError::Unavailable(format!(
                "worker `{worker_id}` has no log endpoint: the log of try {try_number} is \
                 unavailable"
            )));
        }
    };
    let from = query.from.unwrap_or(0);
    let limit = query.limit.unwrap_or(MAX_CHUNK).clamp(MIN_CHUNK, MAX_CHUNK);
    let answered = |answered: Option<&String>| match answered {
        Some(other) if *other != worker_id => Err({
            state.metrics.log_error("stale_address");
            ApiError::Unavailable(format!(
                "reached worker `{other}` instead of `{worker_id}` at {endpoint}: its address \
                 is stale; try again shortly"
            ))
        }),
        _ => Ok(()),
    };
    let log = match state.logs.fetch(&endpoint, &attempt_id, from, limit).await {
        Ok(log) => log,
        Err(LogFetchError::Missing { worker_id: other }) => {
            answered(other.as_ref())?;
            state.metrics.log_error("missing");
            return Err(ApiError::NotFound(format!(
                "the log file is unavailable at worker `{worker_id}` (it may have been deleted)"
            )));
        }
        Err(LogFetchError::Unreachable(reason)) => {
            state.metrics.log_error("unreachable");
            return Err(ApiError::Unavailable(format!(
                "worker `{worker_id}` can't be reached at {endpoint}: {reason}"
            )));
        }
    };
    answered(log.worker_id.as_ref())?;
    let from = from.min(log.size);
    let read = u64::try_from(log.bytes.len()).unwrap_or(u64::MAX);
    let (content, used) = text_of(&log.bytes, log.running || from + read < log.size);
    Ok(Json(LogChunk {
        attempt_id,
        try_number,
        worker_id,
        from,
        next: from + u64::try_from(used).unwrap_or(u64::MAX),
        size: log.size,
        running: log.running,
        content,
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
    let command_id = parse_uuid(&command_id, "command")?;
    let command = state
        .db
        .command(&command_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no command `{command_id}`")))?;
    Ok(Json(CommandView::from(&command)))
}

async fn submit(state: &ApiState, user: &User, command: Command) -> Accepted {
    let id = state
        .db
        .insert_command(&command, &user.0, (state.clock)())
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
    let run_id = parse_uuid(run_id, "run")?;
    state
        .db
        .run_record(&run_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no run `{run_id}`")))
}

fn parse_dag_id(text: &str) -> Result<DagId, ApiError> {
    DagId::new(text).map_err(|e| ApiError::BadRequest(e.to_string()))
}

/// Run and command IDs are UUIDs, `8-4-4-4-12` hex digits; anything else can't exist (and
/// Postgres would refuse it). Lowercased, as schedr writes them: Postgres compares UUIDs
/// whatever their case, SQLite compares the text, and commands name runs in JSON (text on both).
fn parse_uuid(text: &str, what: &str) -> Result<String, ApiError> {
    let canonical = text.len() == 36
        && text.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        });
    if canonical {
        Ok(text.to_ascii_lowercase())
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
