//! Actions (M3.4): pause and unpause a DAG, trigger a run, cancel a run or a task, retry a
//! task, drain and resume a worker. Each is a POST from a confirmation dialog of our own (35A),
//! sent on as the API's command; the answer is a fragment showing the command, which reads it
//! again every second until the master has decided (applied, or rejected with its reason).
//!
//! A POST is taken only from the UI's own pages (24A): `Sec-Fetch-Site: same-origin` (or
//! `none`, typed by the user), or, from a browser that doesn't send it, an `Origin` whose host
//! is the request's (`X-Forwarded-Host` behind a proxy). Anything else is refused (403), so
//! another site can't make a visitor's browser act.

use std::sync::Arc;

use askama::Template;
use axum::Form;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use schedr_api::views::CommandView;
use schedr_client::times::parse_for;
use schedr_client::{ClientError, TaskAction, Transport, WorkerAction, path_segment};
use serde::Deserialize;

use crate::page::Page;
use crate::{Deployment, Ui};

/// How often a pending command's fragment reads it again, in milliseconds.
pub const POLL_MS: u32 = 1_000;

/// A command, as its fragment shows it.
#[derive(Template)]
#[template(path = "command.html")]
struct CommandFragment {
    /// What was asked, as people say it ("Pause sync").
    what: String,
    /// `pending`, `applied` or `rejected`; `failed` when it couldn't be sent.
    status: String,
    /// The master's reason for a rejection, or why it couldn't be sent.
    reason: String,
    /// Where to read it again while it is pending; empty once decided.
    poll: String,
    /// A link the result points to (a triggered run's page), with its label.
    link: Option<(String, String)>,
    poll_ms: u32,
}

/// Whether a POST comes from the UI's own pages (see the module documentation).
#[must_use]
pub fn same_origin(headers: &HeaderMap) -> bool {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(site) = header("sec-fetch-site") {
        return site == "same-origin" || site == "none";
    }
    let Some(origin) = header(header::ORIGIN.as_str()) else {
        return false;
    };
    let host = header("x-forwarded-host").or_else(|| header(header::HOST.as_str()));
    let (scheme, origin_host) = origin.split_once("://").unwrap_or(("", origin));
    let without_default_port = |host: &str| {
        let default = match scheme {
            "https" => ":443",
            "http" => ":80",
            _ => return host.to_ascii_lowercase(),
        };
        host.strip_suffix(default)
            .unwrap_or(host)
            .to_ascii_lowercase()
    };
    host.is_some_and(|host| {
        without_default_port(host) == without_default_port(origin_host.trim_end_matches('/'))
    })
}

/// Refuses POSTs from elsewhere (a layer on the UI's router).
pub async fn guard(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    if request.method() == Method::POST && !same_origin(request.headers()) {
        return (
            StatusCode::FORBIDDEN,
            Html(
                "<div class=\"banner warning\"><strong>Refused</strong>: actions are taken \
                  only from schedr's own pages.</div>",
            ),
        )
            .into_response();
    }
    next.run(request).await
}

/// The trigger dialog's form.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TriggerForm {
    /// The moment the run is for (`--for`'s forms); empty: now.
    #[serde(default, rename = "for")]
    pub planned_for: String,
}

/// The pause dialog's form.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PauseForm {
    /// `on` (a checked box): also cancel its queued and running runs.
    #[serde(default)]
    pub cancel_runs: Option<String>,
}

/// `POST /d/<deployment>/dags/<dag>/pause`.
pub async fn pause<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, dag_id)): Path<(String, String)>,
    headers: HeaderMap,
    Form(form): Form<PauseForm>,
) -> Response {
    let cancel_runs = form.cancel_runs.is_some();
    let what = if cancel_runs {
        format!("Pause {dag_id} and cancel its runs")
    } else {
        format!("Pause {dag_id}")
    };
    act(&ui, &headers, &deployment, what, async |d| {
        d.client.set_paused(&dag_id, true, cancel_runs).await
    })
    .await
}

/// `POST /d/<deployment>/dags/<dag>/unpause`.
pub async fn unpause<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, dag_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    act(
        &ui,
        &headers,
        &deployment,
        format!("Unpause {dag_id}"),
        async |d| d.client.set_paused(&dag_id, false, false).await,
    )
    .await
}

/// `POST /d/<deployment>/dags/<dag>/trigger`.
pub async fn trigger<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, dag_id)): Path<(String, String)>,
    headers: HeaderMap,
    Form(form): Form<TriggerForm>,
) -> Response {
    let text = form.planned_for.trim();
    let planned_for = if text.is_empty() {
        None
    } else {
        match parse_for(text) {
            Ok(at) => Some(at.at),
            Err(message) => return failed(format!("Trigger {dag_id}"), &message),
        }
    };
    act(
        &ui,
        &headers,
        &deployment,
        format!("Trigger {dag_id}"),
        async |d| d.client.trigger(&dag_id, planned_for).await,
    )
    .await
}

/// `POST /d/<deployment>/runs/<run>/cancel`.
pub async fn cancel_run<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    act(
        &ui,
        &headers,
        &deployment,
        format!("Cancel run {run_id}"),
        async |d| d.client.cancel(&run_id).await,
    )
    .await
}

/// `POST /d/<deployment>/runs/<run>/tasks/<task>/cancel` or `.../retry`.
pub async fn task<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id, task_id, action)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let (action, verb) = match action.as_str() {
        "cancel" => (TaskAction::Cancel, "Cancel"),
        "retry" => (TaskAction::Retry, "Retry"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    act(
        &ui,
        &headers,
        &deployment,
        format!("{verb} task {task_id}"),
        async |d| d.client.task_action(&run_id, &task_id, action).await,
    )
    .await
}

/// `POST /d/<deployment>/workers/<worker>/drain` or `.../resume`.
pub async fn worker<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, worker_id, action)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let (action, verb) = match action.as_str() {
        "drain" => (WorkerAction::Drain, "Drain"),
        "resume" => (WorkerAction::Resume, "Resume"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    act(
        &ui,
        &headers,
        &deployment,
        format!("{verb} worker {worker_id}"),
        async |d| d.client.worker_action(&worker_id, action).await,
    )
    .await
}

/// The command polled: `GET /d/<deployment>/commands/<command>?what=`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PollQuery {
    /// What was asked, as the fragment says it.
    #[serde(default)]
    pub what: String,
}

/// `GET /d/<deployment>/commands/<command>`: the command's fragment, as it stands now.
pub async fn command<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, command_id)): Path<(String, String)>,
    Query(query): Query<PollQuery>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return failed(
            query.what,
            &format!("There is no deployment `{deployment}` here."),
        );
    };
    match deployment.client.command(&command_id).await {
        Ok(command) => shown(&page, deployment, query.what, &command),
        // Sent and accepted: it may well be applied. Said, and read again (never "not sent",
        // which would invite sending it twice).
        Err(error) => fragment(&CommandFragment {
            poll: poll_url(&page, deployment, &command_id, &query.what),
            what: query.what,
            status: "pending".to_owned(),
            reason: format!("can't read it now ({error}); trying again"),
            link: None,
            poll_ms: POLL_MS,
        }),
    }
}

/// `GET /time?for=`: the moment a trigger's "for" names, in UTC and the browser's zone, or why
/// it can't be read (the dialog shows it as it is typed).
pub async fn time(Query(form): Query<TriggerForm>, headers: HeaderMap) -> Html<String> {
    let page = Page::new(&headers, false);
    let text = form.planned_for.trim();
    Html(if text.is_empty() {
        "Now.".to_owned()
    } else {
        match parse_for(text) {
            Ok(at) => {
                let note = at.warning.map_or("", |_| " (no offset given: read as UTC)");
                escape(&format!("{}{note}", page.time(&at.at)))
            }
            Err(message) => format!("<span class=\"error\">{}</span>", escape(&message)),
        }
    })
}

/// Sends a command to `deployment` with `send` and answers with its fragment.
async fn act<T: Transport>(
    ui: &Arc<Ui<T>>,
    headers: &HeaderMap,
    deployment: &str,
    what: String,
    send: impl AsyncFnOnce(&Deployment<T>) -> Result<CommandView, ClientError>,
) -> Response {
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return failed(
            what,
            &format!("There is no deployment `{deployment}` here."),
        );
    };
    match send(deployment).await {
        Ok(command) => shown(&Page::new(headers, false), deployment, what, &command),
        Err(error) => failed(what, &error.to_string()),
    }
}

/// `command`'s fragment: still pending, it reads itself again.
fn shown<T>(
    page: &Page,
    deployment: &Deployment<T>,
    what: String,
    command: &CommandView,
) -> Response {
    let poll = if command.status == "pending" {
        poll_url(page, deployment, &command.command_id, &what)
    } else {
        String::new()
    };
    let link = command
        .result
        .as_ref()
        .and_then(|r| r["run_id"].as_str())
        .filter(|_| command.status == "applied" && command.kind == "trigger_run")
        .map(|run_id| {
            (
                format!(
                    "{}/d/{}/runs/{}",
                    page.base,
                    path_segment(&deployment.name),
                    path_segment(run_id)
                ),
                format!("run {run_id}"),
            )
        });
    fragment(&CommandFragment {
        what,
        status: command.status.clone(),
        reason: command.reason.clone().unwrap_or_default(),
        poll,
        link,
        poll_ms: POLL_MS,
    })
}

/// Where a pending command's fragment reads it again.
fn poll_url<T>(page: &Page, deployment: &Deployment<T>, command_id: &str, what: &str) -> String {
    format!(
        "{}/d/{}/commands/{}?what={}",
        page.base,
        path_segment(&deployment.name),
        path_segment(command_id),
        path_segment(what)
    )
}

/// A command that couldn't be sent.
fn failed(what: String, reason: &str) -> Response {
    fragment(&CommandFragment {
        what,
        status: "failed".to_owned(),
        reason: reason.to_owned(),
        poll: String::new(),
        link: None,
        poll_ms: POLL_MS,
    })
}

fn fragment(template: &CommandFragment) -> Response {
    match template.render() {
        Ok(html) => ([(header::CACHE_CONTROL, "no-store")], Html(html)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the fragment couldn't be rendered: {error}"),
        )
            .into_response(),
    }
}

/// `text` HTML-escaped.
fn escape(text: &str) -> String {
    crate::ansi::to_html(text)
}
