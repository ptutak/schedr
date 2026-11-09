//! The `/api/v1` endpoints: reads straight from the database, actions as command rows.
//!
//! A handler's errors are its OpenAPI responses (`ErrorBody` with 400 / 404 / 503 / 500); the doc
//! comments are the endpoints' descriptions in the OpenAPI document, so they carry no
//! `# Errors` sections.
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use std::collections::{BTreeSet, HashMap};

use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use jiff::Timestamp;
use schedr_db::commands::Submitted;
use schedr_db::reads::RunFilter;
use schedr_db::runs::Held;
use schedr_model::commands::{AskedEvent, Command};
use schedr_model::logs::{MASK_CONTEXT, MAX_CHUNK, MIN_CHUNK};
use schedr_model::{DagId, ResourceState, TaskId, TaskInstanceState, WorkerState};

use crate::auth::User;
use crate::error::ApiError;
use crate::logs::{LogFetchError, masked_lines};
use crate::state::ApiState;
use crate::views::{
    AllRunsQuery, AttemptList, CheckConnectionsRequest, CommandList, CommandView, ConnectionList,
    DagList, DagView, DeploymentView, ErrorBody, EventList, EventView, ImportErrorList,
    ImportErrorView, InstanceQuery, LimitView, LogChunk, LogQuery, MasterView, NodeView,
    OutputView, ParamFieldView, PauseRequest, ProblemView, ResourceList, ResourceView, RunDetail,
    RunList, RunView, RunsQuery, SensorList, SensorView, TaskView, TestConnectionRequest,
    TriggerList, TriggerRequest, TriggerView, WorkerList, WorkerNodeView, WorkerView,
};

#[cfg(test)]
mod tests;

/// How many runs a list returns when the client doesn't say.
pub const DEFAULT_RUNS: u32 = 50;

pub(crate) type ApiResult<T> = Result<Json<T>, ApiError>;
type Accepted = Result<(StatusCode, Json<CommandView>), ApiError>;

/// Every DAG not removed.
#[utoipa::path(get, path = "/api/v1/dags", tag = "dags",
    responses((status = 200, body = DagList)))]
pub async fn list_dags(State(state): State<ApiState>) -> ApiResult<DagList> {
    let dags = state.db.stored_dags().await?;
    let held = Held::by_file(&state.db.current_import_errors().await?);
    Ok(Json(DagList {
        dags: dags
            .iter()
            .filter(|d| !d.removed)
            .map(|d| DagView::of(d, &held))
            .collect(),
    }))
}

/// The files of each DAG root's current code that failed to import (decision 66). A DAG
/// defined in such a file keeps its last good version (still listed) but is held: no run is
/// created and manual triggers are refused until the file is fixed (decision 72). A timeout or
/// a runner that died holds nothing: the file is parsed again (72.2). Fixing the file and
/// syncing the root makes the entry go.
#[utoipa::path(get, path = "/api/v1/import-errors", tag = "dags",
    responses((status = 200, body = ImportErrorList)))]
pub async fn list_import_errors(State(state): State<ApiState>) -> ApiResult<ImportErrorList> {
    let errors = state.db.current_import_errors().await?;
    Ok(Json(ImportErrorList {
        errors: errors.iter().map(ImportErrorView::from).collect(),
    }))
}

/// Every worker the master has seen, gone ones included.
#[utoipa::path(get, path = "/api/v1/workers", tag = "workers",
    responses((status = 200, body = WorkerList)))]
pub async fn list_workers(State(state): State<ApiState>) -> ApiResult<WorkerList> {
    let workers = state.db.workers().await?;
    let mut masters: HashMap<String, Option<String>> = state
        .db
        .worker_nodes()
        .await?
        .into_iter()
        .map(|node| (node.worker_id, node.master_id))
        .collect();
    Ok(Json(WorkerList {
        workers: workers
            .iter()
            .map(|worker| WorkerView {
                master_id: masters.remove(&worker.id).flatten(),
                ..WorkerView::from(worker)
            })
            .collect(),
    }))
}

/// The deployment this webserver serves, as nodes (decisions 121-123): this webserver, the
/// masters heard from in the last 30 s (the leader first; standbys too), and every worker,
/// each with its effective configuration as the node masked it (secrets never in clear).
#[utoipa::path(get, path = "/api/v1/deployment", tag = "workers",
    responses((status = 200, body = DeploymentView)))]
pub async fn get_deployment(
    State(state): State<ApiState>,
    Extension(user): Extension<User>,
) -> ApiResult<DeploymentView> {
    let since = (state.clock)()
        .checked_sub(MASTERS_HEARD_WITHIN)
        .unwrap_or(Timestamp::UNIX_EPOCH);
    let masters = state.db.masters(since).await?;
    // The import errors of files each worker parsed: a warning on its node.
    let mut import_errors: HashMap<String, usize> = HashMap::new();
    for error in state.db.current_import_errors().await? {
        *import_errors.entry(error.worker_id).or_default() += 1;
    }
    let mut nodes: HashMap<String, schedr_db::nodes::WorkerNode> = state
        .db
        .worker_nodes()
        .await?
        .into_iter()
        .map(|node| (node.worker_id.clone(), node))
        .collect();
    let workers = state
        .db
        .workers()
        .await?
        .into_iter()
        .map(|worker| {
            let node = nodes.remove(&worker.id);
            let problems = worker_problems(&worker, import_errors.get(&worker.id).copied());
            WorkerNodeView {
                problems,
                queues: worker.queues.clone(),
                state: worker.state.to_string(),
                schedr_version: worker.schedr_version,
                master_id: node.as_ref().and_then(|n| n.master_id.clone()),
                process_id: node.as_ref().and_then(|n| n.process_id.clone()),
                config: node.and_then(|n| n.config),
                worker_id: worker.id,
            }
        })
        .collect();
    Ok(Json(DeploymentView {
        webserver: NodeView::clone(&state.node),
        masters: masters.into_iter().map(MasterView::from).collect(),
        workers,
        user: user.0,
    }))
}

/// A worker's problems now (decision 129): lost (an error), disconnected, what it reports wrong
/// and the import errors of the files it parsed (warnings). One offline deregistered on purpose
/// (a restart, a scale-down) and has none: kept for good, it would stay coloured (decision 130).
fn worker_problems(
    worker: &schedr_db::workers::WorkerRecord,
    import_errors: Option<usize>,
) -> Vec<ProblemView> {
    let mut problems = Vec::new();
    if worker.state == WorkerState::Offline {
        return problems;
    }
    match worker.state {
        WorkerState::Lost => problems.push(ProblemView {
            error: true,
            message: "lost: it stopped answering, its attempts were given up".to_owned(),
        }),
        WorkerState::Disconnected => problems.push(ProblemView {
            error: false,
            message: "disconnected: lost unless it comes back within its lease".to_owned(),
        }),
        WorkerState::Active | WorkerState::Draining | WorkerState::Offline => {}
    }
    problems.extend(worker.warnings.iter().map(|warning| ProblemView {
        error: false,
        message: schedr_config::masked::mask_text(&warning.message),
    }));
    if let Some(count) = import_errors {
        problems.push(ProblemView {
            error: false,
            message: format!(
                "{count} file{} it parsed failed to import",
                if count == 1 { "" } else { "s" }
            ),
        });
    }
    problems
}

/// How lately a master must have written its row to be shown (it writes it every 10 s).
pub const MASTERS_HEARD_WITHIN: jiff::SignedDuration = jiff::SignedDuration::from_secs(30);

/// Drain a worker for maintenance (`schedr workers drain`): no new tasks, its running ones
/// finish; kept across restarts until resumed. Applied by the master; poll the command for
/// the decision.
#[utoipa::path(post, path = "/api/v1/workers/{worker_id}/drain", tag = "workers",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("worker_id" = String, Path, description = "The worker's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn drain_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    submit(&state, &user, key, Command::DrainWorker { worker_id }).await
}

/// Each DAG root's connections in its current code (decision 211): name, type, the file
/// defining it, why one can't be used, and the DAGs and tasks declaring it; never a value.
/// A task declaring a connection no file defines lists it too, refused.
#[utoipa::path(get, path = "/api/v1/connections", tag = "connections",
    responses((status = 200, body = ConnectionList)))]
pub async fn list_connections(State(state): State<ApiState>) -> ApiResult<ConnectionList> {
    let mut registries = std::collections::BTreeMap::new();
    for root in state.db.current_parses().await?.into_keys() {
        let registry = state.db.current_connections(&root).await?;
        registries.insert(root, registry);
    }
    let dags = state.db.stored_dags().await?;
    Ok(Json(ConnectionList::of(&registries, &dags)))
}

/// The default time a connection test may take (decision 200), unless `[webserver]
/// connection_test_timeout` says.
pub const CONNECTION_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(1);

/// Render connections on a worker (`schedr connections check`, decision 174): each secret
/// they name present or missing, never a value. The command's result fills in as the
/// workers answer: poll it until no job is `running`.
#[utoipa::path(post, path = "/api/v1/connections/check", tag = "connections",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)")),
    request_body(content = CheckConnectionsRequest, description = "Optional; every root's connections by default"),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn check_connections(
    State(state): State<ApiState>,
    Extension(user): Extension<User>,
    key: RequestKey,
    body: Bytes,
) -> Accepted {
    let request: CheckConnectionsRequest = optional_body(&body)?;
    let command = Command::CheckConnections {
        root: request.root,
        name: request.name,
        worker: request.worker,
    };
    submit(&state, &user, key, command).await
}

/// Open a connection on a worker and run its check (`schedr connections test`, decisions
/// 198-200), beside the tasks. The command's result fills in when the worker answers.
#[utoipa::path(post, path = "/api/v1/connections/{name}/test", tag = "connections",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("name" = String, Path, description = "The connection's name")),
    request_body(content = TestConnectionRequest, description = "Optional"),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn test_connection(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
    body: Bytes,
) -> Accepted {
    let request: TestConnectionRequest = optional_body(&body)?;
    let default_ms = u64::try_from(state.connection_test_timeout.as_millis()).unwrap_or(u64::MAX);
    let timeout_ms = request.timeout_ms.unwrap_or(default_ms);
    if timeout_ms == 0 || timeout_ms > schedr_model::commands::MAX_CONNECTION_TEST_MS {
        return Err(ApiError::BadRequest(format!(
            "timeout_ms: 1 to {}",
            schedr_model::commands::MAX_CONNECTION_TEST_MS
        )));
    }
    let command = Command::TestConnection {
        name,
        root: request.root,
        worker: request.worker,
        timeout_ms,
    };
    submit(&state, &user, key, command).await
}

/// Let a drained worker take tasks again (`schedr workers resume`). Applied by the master;
/// poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/workers/{worker_id}/resume", tag = "workers",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("worker_id" = String, Path, description = "The worker's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn resume_worker(
    State(state): State<ApiState>,
    Path(worker_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    submit(&state, &user, key, Command::ResumeWorker { worker_id }).await
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
    let dag = known_dag(&state, &dag_id).await?;
    let held = Held::by_file(&state.db.current_import_errors().await?);
    let mut view = DagView::of(&dag, &held);
    if let Some(current) = &dag.current {
        if let Some(params) = &current.dag.params {
            view.params = ParamFieldView::of(params);
        }
        if current.dag.tasks.iter().any(|t| !t.connections.is_empty()) {
            let root = &current.dag.location.root;
            view.warnings = state
                .db
                .current_connections(root)
                .await?
                .problems(&current.dag);
        }
        let limited = current.dag.max_active_tasks.is_some()
            || current.dag.tasks.iter().any(|t| {
                t.settings.max_active.is_some() || t.settings.max_active_per_run.is_some()
            });
        if limited {
            let counts = state.db.active_counts(&dag_id).await?;
            view.limits = LimitView::of(&current.dag, &counts);
        }
    }
    Ok(Json(view))
}

/// Pause a DAG: the scheduler creates no runs of it (optionally cancelling those in
/// progress). Applied by the master; poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/pause", tag = "dags",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("dag_id" = String, Path, description = "The DAG's ID")),
    request_body(content = PauseRequest, description = "Optional; `{}` by default"),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn pause_dag(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
    body: Bytes,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let request: PauseRequest = optional_body(&body)?;
    submit(
        &state,
        &user,
        key,
        Command::PauseDag {
            dag_id,
            cancel_runs: request.cancel_runs,
        },
    )
    .await
}

/// Unpause a DAG. Applied by the master; poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/unpause", tag = "dags",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn unpause_dag(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    submit(&state, &user, key, Command::UnpauseDag { dag_id }).await
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
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("dag_id" = String, Path, description = "The DAG's ID")),
    request_body(content = TriggerRequest, description = "Optional; now by default"),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn trigger_run(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
    body: Bytes,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let request: TriggerRequest = optional_body(&body)?;
    // Refused here: the command's storage would replace it (decision 114), and the master
    // would see other values than those given.
    if let Some(params) = &request.params
        && schedr_model::params::nul_at(&serde_json::Value::Object(params.clone())).is_some()
    {
        return Err(ApiError::BadRequest(
            "params: text containing NUL (\\u0000) can't be stored".to_owned(),
        ));
    }
    let event = asked_event(&request)?;
    submit(
        &state,
        &user,
        key,
        Command::TriggerRun {
            dag_id,
            planned_for: request.planned_for,
            params: request.params,
            event,
        },
    )
    .await
}

/// The event a manual run asks for; what can't be stored is refused here, as params are.
fn asked_event(request: &TriggerRequest) -> Result<Option<AskedEvent>, ApiError> {
    let bad = |why: &str| Err(ApiError::BadRequest(why.to_owned()));
    if request.event_at.is_some() && request.event_key.is_none() {
        return bad("event_at needs an event_key");
    }
    match (&request.event, &request.event_key, &request.event_data) {
        (None, None, None) => Ok(None),
        (Some(key), None, None) => Ok(Some(AskedEvent::Stored { key: key.clone() })),
        (None, Some(key), data) => {
            let data = data.clone().unwrap_or(serde_json::Value::Null);
            if schedr_model::params::nul_at(&data).is_some() {
                return bad("event_data: text containing NUL (\\u0000) can't be stored");
            }
            schedr_model::trigger::check_event_key(key).map_err(ApiError::BadRequest)?;
            Ok(Some(AskedEvent::Given {
                key: key.clone(),
                data,
                at: request.event_at,
            }))
        }
        (None, None, Some(_)) => bad("event_data needs an event_key"),
        (Some(_), _, _) => bad("event is the key of a stored event: give it or an event_key"),
    }
}

/// A trigger's view from its stored row.
fn trigger_view(
    active: &schedr_db::runs::ActiveDag,
    record: Option<&schedr_db::triggers::TriggerRecord>,
    switched_off: bool,
    now: Timestamp,
) -> Option<TriggerView> {
    let schedr_model::Trigger::Event {
        task, start, end, ..
    } = &active.dag.trigger
    else {
        return None;
    };
    let outside = start.is_some_and(|start| now < start) || end.is_some_and(|end| end < now);
    let state = if active.paused || active.held.is_some() || outside {
        "paused"
    } else if switched_off {
        "switched_off"
    } else if record.is_some_and(|r| r.instance_id.is_some()) {
        "running"
    } else if record.is_some_and(|r| r.stopped_why.is_some()) {
        "stopped"
    } else {
        "waiting"
    };
    let empty = schedr_db::triggers::TriggerRecord::default();
    let record = record.unwrap_or(&empty);
    Some(TriggerView {
        dag_id: active.dag.id.to_string(),
        trigger: task.id.to_string(),
        state: state.to_owned(),
        worker: record.worker_id.clone(),
        // Masked when read, as an output is: what a run reads is the stored one.
        bookmark: record
            .bookmark
            .as_deref()
            .map(schedr_config::masked::mask_text),
        stopped_why: record.stopped_why.clone(),
        failures: record.failures,
        last_error: record
            .last_error
            .as_deref()
            .map(schedr_config::masked::mask_text),
        not_before: record.not_before,
        started_at: record.started_at,
        last_event_at: record.last_event_at,
        last_poked_at: record.last_poked_at,
        last_restart_at: record.last_restart_at,
        called_for: None,
    })
}

/// The run an `@trigger(wait_for=...)` is called for (decision 353), on its view.
async fn add_called_for(
    db: &schedr_db::Database,
    active: &schedr_db::runs::ActiveDag,
    view: &mut TriggerView,
) -> Result<(), ApiError> {
    if let schedr_model::Trigger::Event { task, .. } = &active.dag.trigger
        && matches!(
            task.kind,
            schedr_model::TaskKind::Sensor {
                wait_for: Some(_),
                ..
            }
        )
    {
        view.called_for = db
            .trigger_run_handed(active.dag.id.as_str())
            .await?
            .map(|handed| handed.run);
    }
    Ok(())
}

/// Every DAG's trigger.
#[utoipa::path(get, path = "/api/v1/triggers", tag = "triggers",
    responses((status = 200, body = TriggerList)))]
pub async fn list_triggers(State(state): State<ApiState>) -> ApiResult<TriggerList> {
    let records = state.db.triggers().await?;
    let off = state.db.switched_off_wait_steps().await?;
    let now = Timestamp::now();
    let mut triggers = Vec::new();
    for active in &state.db.active_dags().await? {
        let dag = active.dag.id.as_str();
        let function = match &active.dag.trigger {
            schedr_model::Trigger::Event { task, .. } => task.id.as_str(),
            _ => continue,
        };
        let Some(mut view) = trigger_view(
            active,
            records.iter().find(|r| r.dag_id == dag),
            off.iter().any(|s| s.dag_id == dag && s.task_id == function),
            now,
        ) else {
            continue;
        };
        add_called_for(&state.db, active, &mut view).await?;
        triggers.push(view);
    }
    Ok(Json(TriggerList { triggers }))
}

/// A DAG's trigger: `404` for a DAG without one.
#[utoipa::path(get, path = "/api/v1/dags/{dag_id}/trigger", tag = "triggers",
    params(("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 200, body = TriggerView), (status = 404, body = ErrorBody)))]
pub async fn dag_trigger(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
) -> ApiResult<TriggerView> {
    let id = parse_dag_id(&dag_id)?;
    let active = state.db.active_dags_of(&[&id]).await?;
    let none = || ApiError::NotFound(format!("no DAG `{dag_id}` with an event trigger"));
    let active = active.first().ok_or_else(none)?;
    let function = match &active.dag.trigger {
        schedr_model::Trigger::Event { task, .. } => task.id.as_str(),
        _ => return Err(none()),
    };
    let off = state
        .db
        .wait_step_strikes(&dag_id, function)
        .await?
        .is_some_and(|strikes| strikes.off_at.is_some());
    let record = state.db.trigger(&dag_id).await?;
    let mut view = trigger_view(active, record.as_ref(), off, Timestamp::now()).ok_or_else(none)?;
    add_called_for(&state.db, active, &mut view).await?;
    Ok(Json(view))
}

/// How many events one call gives at most, by default and at most.
const DEFAULT_EVENTS: u32 = 50;
const MAX_EVENTS: u32 = 500;

/// Which of a trigger's events.
#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct EventsQuery {
    /// How many, 50 by default and 500 at most.
    pub limit: Option<u32>,
    /// Those after the one last listed: its `received_at`.
    pub before: Option<Timestamp>,
    /// And its `key`, for those received at the same moment.
    pub before_key: Option<String>,
}

/// What a DAG's trigger found, newest received first.
#[utoipa::path(get, path = "/api/v1/dags/{dag_id}/trigger/events", tag = "triggers",
    params(("dag_id" = String, Path, description = "The DAG's ID"), EventsQuery),
    responses((status = 200, body = EventList), (status = 400, body = ErrorBody)))]
pub async fn trigger_events(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    query: Result<Query<EventsQuery>, QueryRejection>,
) -> ApiResult<EventList> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let limit = query.limit.unwrap_or(DEFAULT_EVENTS).min(MAX_EVENTS);
    if query.before_key.is_some() && query.before.is_none() {
        return Err(ApiError::BadRequest(
            "`before_key` needs `before` (the event's `received_at`)".to_owned(),
        ));
    }
    let after = query
        .before
        .map(|at| (at, query.before_key.as_deref().unwrap_or_default()));
    let events = state
        .db
        .events(&dag_id, limit, after)
        .await?
        .into_iter()
        .map(|mut stored| {
            // Masked when read, as an output is: what a run reads is the stored one.
            schedr_config::masked::mask_data(&mut stored.event.data);
            EventView {
                key: schedr_config::masked::mask_text(&stored.event.key),
                data: stored.event.data,
                at: stored.event.at,
                received_at: stored.received_at,
            }
        })
        .collect();
    Ok(Json(EventList { events }))
}

/// Start a DAG's trigger again from its bookmark, whatever stopped it. Applied by the master;
/// poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/trigger/restart", tag = "triggers",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's"),
        ("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn restart_trigger(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    submit(&state, &user, key, Command::RestartTrigger { dag_id }).await
}

/// Pass the run an `@trigger(wait_for=...)` is called for and keeps failing on, so its next call
/// gets the next run (decision 353); refused while a call of it runs. Applied by the master; poll
/// the command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/trigger/skip", tag = "triggers",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's"),
        ("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn skip_trigger_run(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    submit(&state, &user, key, Command::SkipTriggerRun { dag_id }).await
}

/// Switch a DAG's trigger back on, forgetting its strikes. Applied by the master; poll the
/// command for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/trigger/switch-on", tag = "triggers",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's"),
        ("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody),
        (status = 404, body = ErrorBody)))]
pub async fn switch_on_trigger(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let id = parse_dag_id(&dag_id)?;
    let active = state.db.active_dags_of(&[&id]).await?;
    let sensor = active.first().and_then(|active| match &active.dag.trigger {
        schedr_model::Trigger::Event { task, .. } => Some(task.id.clone()),
        _ => None,
    });
    let Some(sensor) = sensor else {
        return Err(ApiError::NotFound(format!(
            "no DAG `{dag_id}` with an event trigger"
        )));
    };
    submit(
        &state,
        &user,
        key,
        Command::SwitchOnSensor { dag_id: id, sensor },
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
    let mut dag = state
        .db
        .run_dag_json(&run_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no run `{run_id}`")))?;
    // Its params hold values (the defaults, and the schema's and type's copies of them, such
    // as a `Literal`'s): only the defaults are shown, masked (decision 134).
    if let Some(params) = dag
        .get_mut("params")
        .and_then(serde_json::Value::as_object_mut)
    {
        params.retain(|key, _| key == "defaults");
        if let Some(defaults) = params.get_mut("defaults") {
            schedr_config::masked::mask_data(defaults);
        }
    }
    Ok(Json(dag))
}

/// A task's stored output in a run, with its outcome; 404 while it has none.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/tasks/{task_id}/output", tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID"),
        InstanceQuery
    ),
    responses(
        (status = 200, body = OutputView),
        (status = 400, body = ErrorBody),
        (status = 404, body = ErrorBody)
    ))]
pub async fn get_output(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
    query: Result<Query<InstanceQuery>, QueryRejection>,
) -> ApiResult<OutputView> {
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let run_id = known_run(&state, &run_id).await?.id;
    let none = || ApiError::NotFound(format!("no output of `{task_id}` in run `{run_id}`"));
    let mapped = mapped_count(&state, &run_id, &task_id).await?;
    if let (Some(_), None | Some(-1)) = (mapped, query.map_index) {
        // The mapped task's whole output: the list a downstream task gets (decision 81).
        let whole = state
            .db
            .run_instances(&run_id)
            .await?
            .into_iter()
            .find(|i| i.task_id == task_id && i.map_index == -1)
            .filter(|i| i.state == schedr_model::TaskInstanceState::Success)
            .ok_or_else(none)?;
        let list = state
            .db
            .instance_results(&run_id, &task_id)
            .await?
            .into_iter()
            .map(|r| {
                if r.state == schedr_model::TaskInstanceState::Success {
                    r.output.unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Null
                }
            })
            .collect();
        return Ok(Json(OutputView {
            outcome: whole.outcome.unwrap_or_else(|| "SUCCESS".to_owned()),
            value: shown(serde_json::Value::Array(list)),
        }));
    }
    let map_index = instance_of(mapped, &task_id, query.map_index, false)?;
    let (outcome, value) = state
        .db
        .instance_output(&run_id, &task_id, map_index)
        .await?
        .ok_or_else(none)?;
    Ok(Json(OutputView {
        outcome,
        value: shown(value),
    }))
}

/// A task's output as shown: masked as a task's own data is (decision 131: its texts, and a
/// field whose name has a word naming a secret, `partition_key` shown). Downstream tasks get
/// it as stored.
fn shown(mut value: serde_json::Value) -> serde_json::Value {
    schedr_config::masked::mask_data(&mut value);
    value
}

/// How many instances `task_id` has in the run when it is mapped (decision 80); `None` for
/// an unmapped task; a 404 for a task the run doesn't have.
async fn mapped_count(
    state: &ApiState,
    run_id: &str,
    task_id: &TaskId,
) -> Result<Option<usize>, ApiError> {
    let Some((tasks, mapped)) = run_tasks(state, run_id).await? else {
        return Ok(None);
    };
    if !tasks.contains(task_id) {
        return Err(ApiError::NotFound(format!(
            "no task `{task_id}` in run `{run_id}`"
        )));
    }
    if !mapped.contains(task_id) {
        return Ok(None);
    }
    // Counted, not every instance of the run read (B137).
    Ok(Some(state.db.mapped_instances(run_id, task_id).await?))
}

/// The run's tasks and, of those, its mapped ones, from the DAG version it runs (`None` when
/// that version can't be read).
async fn run_tasks(
    state: &ApiState,
    run_id: &str,
) -> Result<Option<(BTreeSet<TaskId>, BTreeSet<TaskId>)>, ApiError> {
    // Parsed once per version (B137); one that doesn't read is `None`, as before.
    let dag = match state.db.run_dag(run_id).await {
        Ok(dag) => dag,
        Err(schedr_db::DbError::InvalidData { .. }) => None,
        Err(error) => return Err(error.into()),
    };
    Ok(dag.map(|dag| {
        let mapped = dag
            .tasks
            .iter()
            .filter(|t| t.expand.is_some())
            .map(|t| t.id.clone())
            .collect();
        (dag.tasks.iter().map(|t| t.id.clone()).collect(), mapped)
    }))
}

/// The `map_index` a request about `task_id` means: the instance asked for of a mapped task
/// (`required`: a mapped task needs one, decision 82), -1 for an unmapped one.
fn instance_of(
    mapped: Option<usize>,
    task_id: &TaskId,
    asked: Option<i32>,
    required: bool,
) -> Result<i32, ApiError> {
    // -1 is the whole task, as listed: the same as naming no instance.
    match (mapped, asked.filter(|index| *index != -1)) {
        (Some(count), None) if required => Err(ApiError::BadRequest(format!(
            "task `{task_id}` is mapped ({count} instances): give ?map_index=N, from 0 to {}",
            count.saturating_sub(1)
        ))),
        (Some(_) | None, None) => Ok(-1),
        (Some(count), Some(index)) if usize::try_from(index).is_ok_and(|i| i < count) => Ok(index),
        (Some(0), Some(index)) => Err(ApiError::BadRequest(format!(
            "task `{task_id}` has no instances (yet): no instance {index}"
        ))),
        (Some(count), Some(index)) => Err(ApiError::BadRequest(format!(
            "task `{task_id}` has {count} instances, from 0 to {}: no instance {index}",
            count - 1
        ))),
        (None, Some(index)) => Err(ApiError::BadRequest(format!(
            "task `{task_id}` isn't mapped: it has no instance {index}"
        ))),
    }
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
    let dag = match state.db.run_dag(run_id).await {
        Ok(dag) => dag,
        Err(schedr_db::DbError::InvalidData { .. }) => None,
        Err(error) => return Err(error.into()),
    };
    let mapped: BTreeSet<&TaskId> = dag
        .iter()
        .flat_map(|dag| dag.tasks.iter().filter(|t| t.expand.is_some()))
        .map(|t| &t.id)
        .collect();
    let state_of = |task: &TaskId| {
        tasks
            .iter()
            .find(|i| &i.task_id == task && i.map_index == -1)
            .map_or(TaskInstanceState::Scheduled, |i| i.state)
    };
    let resources = dag
        .iter()
        .flat_map(|dag| &dag.resources)
        .map(|r| resource_view(r, &state_of))
        .collect();
    let count = |task: &TaskId| {
        tasks
            .iter()
            .filter(|i| &i.task_id == task && i.map_index >= 0)
            .count()
    };
    Ok(Json(RunDetail {
        run: RunView::from(&run),
        tasks: tasks
            .iter()
            .map(|i| {
                let whole = i.map_index == -1 && mapped.contains(&&i.task_id);
                let mut view = TaskView::of(i, whole.then(|| count(&i.task_id)));
                if whole && view.cleanup.is_none() {
                    view.cleanup = worst_cleanup(&tasks, &i.task_id);
                }
                view
            })
            .collect(),
        code_hashes: state.db.run_code_hashes(run_id).await?,
        params: shown(schedr_model::params::shown(
            &state
                .db
                .run_params(run_id)
                .await?
                .unwrap_or_else(|| serde_json::json!({})),
        )),
        resources,
    }))
}

/// A mapped task's cleanup as a whole: the worst of its instances' (`failed`, then
/// `worker_lost`, `ok`, `not_needed`), so a failed one shows on the task's row.
fn worst_cleanup(tasks: &[schedr_db::reads::InstanceRecord], task: &TaskId) -> Option<String> {
    use schedr_model::CleanupResult as R;
    let rank = |r: R| match r {
        R::Failed => 3,
        R::WorkerLost => 2,
        R::Ok => 1,
        R::NotNeeded => 0,
    };
    tasks
        .iter()
        .filter(|i| &i.task_id == task && i.map_index >= 0)
        .filter_map(|i| i.cleanup_result)
        .max_by_key(|r| rank(*r))
        .map(|r| r.to_string())
}

fn resource_view(
    resource: &schedr_model::Resource,
    state_of: &impl Fn(&TaskId) -> TaskInstanceState,
) -> ResourceView {
    ResourceView {
        setup: resource.setup.to_string(),
        cleanup: resource.cleanup.to_string(),
        users: resource.users.iter().map(ToString::to_string).collect(),
        blocking: resource.blocking,
        state: name_of(ResourceState::of(
            state_of(&resource.setup),
            state_of(&resource.cleanup),
        )),
    }
}

/// A run's resources, each with its setup, cleanup, users and state (decision 340).
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/resources", tag = "resources",
    params(("run_id" = String, Path, description = "The run's ID")),
    responses((status = 200, body = ResourceList), (status = 404, body = ErrorBody)))]
pub async fn list_run_resources(
    State(state): State<ApiState>,
    Path(run_id): Path<String>,
) -> ApiResult<ResourceList> {
    let run = known_run(&state, &run_id).await?;
    let resources = get_run(State(state), Path(run.id)).await?.0.resources;
    Ok(Json(ResourceList { resources }))
}

/// One resource of a run, by its name (decision 340).
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/resources/{resource}", tag = "resources",
    params(("run_id" = String, Path, description = "The run's ID"),
        ("resource" = String, Path, description = "The resource: its setup's task ID")),
    responses((status = 200, body = ResourceView), (status = 404, body = ErrorBody)))]
pub async fn get_run_resource(
    State(state): State<ApiState>,
    Path((run_id, resource)): Path<(String, String)>,
) -> ApiResult<ResourceView> {
    let run = known_run(&state, &run_id).await?;
    let resources = get_run(State(state), Path(run.id)).await?.0.resources;
    resources
        .into_iter()
        .find(|found| found.setup == resource)
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("no resource `{resource}` in run `{run_id}`")))
}

/// The wait steps switched off for getting their host killed too many times in a row
/// (decision 338).
#[utoipa::path(get, path = "/api/v1/sensors/switched-off", tag = "sensors",
    responses((status = 200, body = SensorList)))]
pub async fn list_switched_off_sensors(State(state): State<ApiState>) -> ApiResult<SensorList> {
    let sensors = state
        .db
        .switched_off_wait_steps()
        .await?
        .into_iter()
        .map(|strikes| SensorView {
            dag_id: strikes.dag_id,
            sensor: strikes.task_id,
            strikes: strikes.strikes,
            switched_off_at: strikes.off_at.map(|at| at.to_string()).unwrap_or_default(),
        })
        .collect();
    Ok(Json(SensorList { sensors }))
}

/// Switch a wait step back on, forgetting its strikes. Applied by the master; poll the command
/// for the decision.
#[utoipa::path(post, path = "/api/v1/dags/{dag_id}/sensors/{sensor}/switch-on", tag = "sensors",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request"),
        ("dag_id" = String, Path, description = "The DAG's ID"),
        ("sensor" = String, Path, description = "The wait step's task ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn switch_on_sensor(
    State(state): State<ApiState>,
    Path((dag_id, sensor)): Path<(String, String)>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let dag_id = parse_dag_id(&dag_id)?;
    let sensor = TaskId::new(sensor).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    submit(
        &state,
        &user,
        key,
        Command::SwitchOnSensor { dag_id, sensor },
    )
    .await
}

/// A resource's state as the API names it (`waiting_for_users`).
fn name_of(state: ResourceState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Cancel a run (`schedr runs cancel`). Applied by the master; poll the command for the
/// decision.
#[utoipa::path(post, path = "/api/v1/runs/{run_id}/cancel", tag = "runs",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("run_id" = String, Path, description = "The run's ID")),
    responses((status = 202, body = CommandView), (status = 400, body = ErrorBody)))]
pub async fn cancel_run(
    State(state): State<ApiState>,
    Path(run_id): Path<String>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let run_id = parse_uuid(&run_id, "run")?;
    submit(&state, &user, key, Command::CancelRun { run_id }).await
}

/// Cancel one task of a run (`schedr tasks cancel`): stopped if it runs, cancelled at once if
/// it hasn't started; the tasks downstream of it are cancelled. Applied by the master; poll
/// the command for the decision.
#[utoipa::path(post, path = "/api/v1/runs/{run_id}/tasks/{task_id}/cancel", tag = "runs",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID"),
        InstanceQuery
    ),
    responses(
        (status = 202, body = CommandView),
        (status = 400, body = ErrorBody),
        (status = 404, body = ErrorBody)
    ))]
pub async fn cancel_task(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
    query: Result<Query<InstanceQuery>, QueryRejection>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let run_id = parse_uuid(&run_id, "run")?;
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    // An instance must be one the task has; checked when the run is known.
    let mut map_index = query.map_index.filter(|index| *index != -1);
    if let Some(found) = state.db.run_record(&run_id).await? {
        let mapped = mapped_count(&state, &found.id, &task_id).await?;
        map_index = Some(instance_of(mapped, &task_id, query.map_index, false)?)
            .filter(|index| *index >= 0);
    }
    let command = Command::CancelTask {
        run_id,
        task_id,
        map_index,
    };
    submit(&state, &user, key, command).await
}

/// Retry an `unschedulable` task of an unfinished run (`schedr tasks retry`). Applied by the
/// master; poll the command for the decision.
#[utoipa::path(post, path = "/api/v1/runs/{run_id}/tasks/{task_id}/retry", tag = "runs",
    params(("Idempotency-Key" = Option<String>, Header, description = "A key of the client's for this request: sent again with it, it is the command stored the first time (B140)"),
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID"),
        InstanceQuery
    ),
    responses(
        (status = 202, body = CommandView),
        (status = 400, body = ErrorBody),
        (status = 404, body = ErrorBody)
    ))]
pub async fn retry_task(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
    query: Result<Query<InstanceQuery>, QueryRejection>,
    Extension(user): Extension<User>,
    key: RequestKey,
) -> Accepted {
    let run_id = parse_uuid(&run_id, "run")?;
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    // A mapped task's instance must be named (decision 82); checked when the run is known.
    let mut map_index = query.map_index.filter(|index| *index != -1);
    if let Some(found) = state.db.run_record(&run_id).await? {
        let mapped = mapped_count(&state, &found.id, &task_id).await?;
        map_index =
            Some(instance_of(mapped, &task_id, query.map_index, true)?).filter(|index| *index >= 0);
    }
    let command = Command::RetryTask {
        run_id,
        task_id,
        map_index,
    };
    submit(&state, &user, key, command).await
}

/// The attempts of a run's task, oldest first.
#[utoipa::path(get, path = "/api/v1/runs/{run_id}/tasks/{task_id}/attempts", tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run's ID"),
        ("task_id" = String, Path, description = "The task's ID"),
        InstanceQuery
    ),
    responses(
        (status = 200, body = AttemptList),
        (status = 400, body = ErrorBody),
        (status = 404, body = ErrorBody)
    ))]
pub async fn list_attempts(
    State(state): State<ApiState>,
    Path((run_id, task_id)): Path<(String, String)>,
    query: Result<Query<InstanceQuery>, QueryRejection>,
) -> ApiResult<AttemptList> {
    let task_id = TaskId::new(task_id).map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let run_id = known_run(&state, &run_id).await?.id;
    let mapped = mapped_count(&state, &run_id, &task_id).await?;
    let map_index = instance_of(mapped, &task_id, query.map_index, true)?;
    let attempts = state
        .db
        .instance_attempts(&run_id, &task_id, map_index)
        .await?;
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
    let mapped = mapped_count(&state, &run_id, &task_id).await?;
    let map_index = instance_of(mapped, &task_id, query.map_index, true)?;
    let attempt = state
        .db
        .instance_attempts(&run_id, &task_id, map_index)
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
    // One byte before `from` too: whether the part starts at a line (see `masked_lines`).
    // Some bytes before `from` too, masked with the part (see `shown_part`).
    let asked = from.saturating_sub(MASK_CONTEXT);
    let fetched = limit + (from - asked);
    let log = match state
        .logs
        .fetch(&endpoint, &attempt_id, asked, fetched)
        .await
    {
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
    let (from, content, next) = shown_part(&log, asked, from, fetched);
    Ok(Json(LogChunk {
        attempt_id,
        try_number,
        worker_id,
        from,
        content,
        next,
        size: log.size,
        running: log.running,
    }))
}

/// The part of `log` (read from `asked`, `fetched` bytes asked for) shown from `from`: where
/// its content starts, its masked content, and where the next part starts.
fn shown_part(
    log: &crate::logs::WorkerLog,
    asked: u64,
    from: u64,
    fetched: u64,
) -> (u64, String, u64) {
    let from = from.min(log.size);
    let before = usize::try_from(from.saturating_sub(asked))
        .unwrap_or(usize::MAX)
        .min(log.bytes.len());
    let (context, bytes) = log.bytes.split_at(before);
    // `from` starts a line: the log's start, or a newline before it.
    let at_line = from == 0 || context.last() == Some(&b'\n');
    let read = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let more = log.running || from + read < log.size;
    // As long as asked: a line with no end in it would never end in a part this long.
    let full = u64::try_from(log.bytes.len()).unwrap_or(u64::MAX) >= fetched;
    let part = masked_lines(context, bytes, at_line, more, full);
    let from = from + u64::try_from(part.skipped).unwrap_or(u64::MAX);
    let next = from + u64::try_from(part.used).unwrap_or(u64::MAX);
    (from, part.content, next)
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

async fn submit(state: &ApiState, user: &User, key: RequestKey, command: Command) -> Accepted {
    let submitted = state
        .db
        .submit_command(&command, &user.0, key.0.as_deref(), (state.clock)())
        .await?;
    let id = match submitted {
        Submitted::New(id) | Submitted::Again(id) => id,
        Submitted::KeyTaken => {
            return Err(ApiError::Unprocessable(
                "this `Idempotency-Key` was sent before with another command: use a new key \
                 for each command"
                    .to_owned(),
            ));
        }
    };
    let record = state
        .db
        .command(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no command `{id}`")))?;
    Ok((StatusCode::ACCEPTED, Json(CommandView::from(&record))))
}

/// The longest `Idempotency-Key` taken.
pub const MAX_REQUEST_KEY: usize = 128;

/// A command request's `Idempotency-Key` header, if sent (B140): the client's key for this
/// request, so one sent again after its answer was lost is stored once. 1 to
/// [`MAX_REQUEST_KEY`] visible ASCII characters (a UUID, say).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestKey(pub Option<String>);

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for RequestKey {
    type Rejection = ApiError;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(Self::read(parts))
    }
}

impl RequestKey {
    /// The key in `parts`' headers, if any, or why it isn't one.
    fn read(parts: &axum::http::request::Parts) -> Result<Self, ApiError> {
        let Some(value) = parts.headers.get("idempotency-key") else {
            return Ok(Self(None));
        };
        let key = value
            .to_str()
            .ok()
            .filter(|k| {
                (1..=MAX_REQUEST_KEY).contains(&k.len()) && k.bytes().all(|b| b.is_ascii_graphic())
            })
            .ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "`Idempotency-Key` must be 1 to {MAX_REQUEST_KEY} visible ASCII characters"
                ))
            })?;
        Ok(Self(Some(key.to_owned())))
    }
}

async fn known_dag(
    state: &ApiState,
    dag_id: &DagId,
) -> Result<schedr_db::dags::StoredDag, ApiError> {
    state
        .db
        .stored_dag(dag_id)
        .await?
        .filter(|d| !d.removed)
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
    serde_json::from_slice(body)
        .map_err(|e| ApiError::BadRequest(format!("invalid body: {}", body_error(&e))))
}

/// Why a JSON body can't be read, without any value from it (decision 134): serde's message
/// with the quoted and backticked values taken out, then masked as text.
pub(crate) fn body_error(error: &serde_json::Error) -> String {
    schedr_config::masked::mask_text(&without_values(&without_quoted(&error.to_string())))
}

/// The kinds serde shows a value of in backticks (``integer `5` ``, decision 134, B194);
/// names it puts in backticks (``unknown field `x` ``) stay.
const BACKTICKED_VALUES: [&str; 4] = ["integer `", "floating point `", "boolean `", "character `"];

/// `text` with each value serde shows in backticks as `` `…` ``.
fn without_values(text: &str) -> String {
    let mut out = text.to_owned();
    for kind in BACKTICKED_VALUES {
        let mut from = 0;
        while let Some(at) = out[from..].find(kind) {
            let start = from + at + kind.len();
            let Some(length) = out[start..].find('`') else {
                break;
            };
            out.replace_range(start..start + length, "…");
            from = start + '…'.len_utf8() + 1;
        }
    }
    out
}

/// `text` with each double-quoted part (a value from the body, which may be a secret) as
/// `"…"`; names stay (serde quotes them in backticks), to be masked as texts.
fn without_quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '"' {
            out.push(c);
            continue;
        }
        out.push_str("\"…\"");
        let mut escaped = false;
        for c in chars.by_ref() {
            match c {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => break,
                _ => {}
            }
        }
    }
    out
}
