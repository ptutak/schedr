//! The pages: the DAGs (every deployment's), one DAG with its recent runs, and the error pages.

use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use schedr_api::views::{DagView, LocationView, SensorView};
use schedr_client::times::trigger_text;
use schedr_client::{ClientError, Transport, path_segment};

use crate::page::Page;
use crate::params_form::TriggerParams;
use crate::runs::{RunRow, run_row};
use crate::{Deployment, Ui};

/// A DAG's row in the list.
#[derive(Debug, Clone)]
pub struct DagRow {
    /// The deployment it belongs to.
    pub deployment: String,
    /// Its page.
    pub href: String,
    /// Its ID.
    pub dag_id: String,
    /// Whether it is paused.
    pub paused: bool,
    /// Its trigger as people write it (`0 3 * * * (Europe/Warsaw)`, `every 1h`, `-`).
    pub schedule: String,
    /// Its tags.
    pub tags: Vec<String>,
    /// Where it is defined (`root:path`).
    pub location: String,
    /// Whether other files define the same ID.
    pub conflict: bool,
    /// Why it is held, while its file fails to import (decision 72): no runs are created and
    /// it can't be triggered.
    pub held: Option<String>,
}

/// `GET /`: the DAGs of every deployment, by ID.
#[derive(Template)]
#[template(path = "dags.html")]
struct DagsPage {
    page: Page,
    rows: Vec<DagRow>,
    /// The deployments that didn't answer, with why.
    unavailable: Vec<String>,
    /// Whether there is more than one deployment (then a column names it).
    several: bool,
    /// Its wait steps switched off, of all deployments.
    switched_off: Vec<SwitchedOffRow>,
}

/// What a manual run of an event-triggered DAG asks for, in its dialog.
const EVENT_FIELDS: &str = r#"<label class="field">Event key <input name="event_key" required size="28" autocomplete="off"></label>
<label class="field">Event data (JSON) <textarea name="event_data" rows="3" cols="40" placeholder="null"></textarea></label>
<p class="dim">Its tasks read this as <code>dag_ctx.event</code>. It isn't stored as an event, so the real one with this key still starts a run.</p>"#;

/// How many events a DAG's page lists.
const EVENTS_SHOWN: u32 = 20;

/// A DAG's trigger, on its page.
#[derive(Debug, Clone, Default)]
struct TriggerPanel {
    /// Why it can't be told now.
    unavailable: String,
    state: String,
    function: String,
    worker: String,
    bookmark: String,
    failures: u32,
    last_error: String,
    /// How long it has gone on, as people read it: label and moment.
    moments: Vec<(&'static str, String)>,
    /// Where it is started again.
    restart: String,
    /// `@trigger(wait_for=...)`: the run it is called for, and where that run is passed.
    called_for: String,
    skip: String,
    events: Vec<EventRow>,
}

/// An event a trigger found, on its DAG's page.
#[derive(Debug, Clone)]
struct EventRow {
    key: String,
    received: String,
    data: String,
    /// Where a run of it is started.
    action: String,
}

/// The wait steps of a DAG that are switched off; why that can't be told.
async fn switched_off_in<T: Transport>(
    page: &Page,
    deployment: &Deployment<T>,
    dag_id: &str,
) -> (Vec<SwitchedOffRow>, String) {
    let (mut switched_off, why) = switched_off_of(
        page,
        deployment,
        deployment.client.switched_off_sensors().await,
    );
    switched_off.retain(|sensor| sensor.dag_id == dag_id);
    (switched_off, why)
}

/// Whether a manual run of the DAG brings an event: an event trigger's or a `RunOf`'s
/// (decision 353).
fn takes_event(dag: &DagView) -> bool {
    dag.trigger
        .as_ref()
        .is_some_and(|trigger| matches!(trigger["kind"].as_str(), Some("event" | "run_of")))
}

/// Its trigger, for a DAG a trigger starts.
async fn dag_trigger_panel<T: Transport>(
    page: &Page,
    deployment: &Deployment<T>,
    dag: &DagView,
) -> Option<TriggerPanel> {
    if dag.trigger.as_ref()?["kind"] != "event" {
        return None;
    }
    Some(trigger_panel(page, deployment, &dag.dag_id).await)
}

async fn trigger_panel<T: Transport>(
    page: &Page,
    deployment: &Deployment<T>,
    dag_id: &str,
) -> TriggerPanel {
    let trigger = match deployment.client.dag_trigger(dag_id).await {
        Ok(trigger) => trigger,
        Err(error) => {
            return TriggerPanel {
                unavailable: error.to_string(),
                ..TriggerPanel::default()
            };
        }
    };
    let actions = format!(
        "{}/d/{}/dags/{}/trigger",
        page.base,
        path_segment(&deployment.name),
        path_segment(dag_id)
    );
    let (events, unavailable) = match deployment.client.trigger_events(dag_id, EVENTS_SHOWN).await {
        Ok(list) => (list.events, String::new()),
        Err(error) => (Vec::new(), error.to_string()),
    };
    let events = events
        .iter()
        .map(|event| EventRow {
            key: event.key.clone(),
            received: page.time(&event.received_at),
            data: event.data.to_string(),
            action: format!("{actions}/run-event?key={}", path_segment(&event.key)),
        })
        .collect();
    let moments = [
        ("Started", trigger.started_at),
        ("Last event", trigger.last_event_at),
        ("Last poked", trigger.last_poked_at),
        ("Last idle restart", trigger.last_restart_at),
        ("Starts again from", trigger.not_before),
    ]
    .into_iter()
    .filter_map(|(what, at)| at.map(|at| (what, page.time(&at))))
    .collect();
    TriggerPanel {
        unavailable,
        state: trigger.state,
        function: trigger.trigger,
        worker: trigger.worker.unwrap_or_default(),
        bookmark: trigger.bookmark.unwrap_or_default(),
        failures: trigger.failures,
        last_error: trigger.last_error.unwrap_or_default(),
        moments,
        restart: format!("{actions}/restart"),
        called_for: trigger.called_for.unwrap_or_default(),
        skip: format!("{actions}/skip"),
        events,
    }
}

/// A wait step switched off, as a banner or a DAG's page lists it.
#[derive(Debug, Clone)]
struct SwitchedOffRow {
    deployment: String,
    dag_id: String,
    /// Its DAG's page.
    dag_href: String,
    sensor: String,
    strikes: u32,
    /// When, as people read it.
    at: String,
    /// Where it is switched on.
    action: String,
}

impl SwitchedOffRow {
    fn of<T>(page: &Page, deployment: &Deployment<T>, sensor: &SensorView) -> Self {
        let dag_href = format!(
            "{}/d/{}/dags/{}",
            page.base,
            path_segment(&deployment.name),
            path_segment(&sensor.dag_id)
        );
        Self {
            deployment: deployment.name.clone(),
            dag_id: sensor.dag_id.clone(),
            action: format!(
                "{dag_href}/sensors/{}/switch-on",
                path_segment(&sensor.sensor)
            ),
            dag_href,
            sensor: sensor.sensor.clone(),
            strikes: sensor.strikes,
            at: sensor
                .switched_off_at
                .parse()
                .map_or_else(|_| sensor.switched_off_at.clone(), |at| page.time(&at)),
        }
    }

    /// The switch-on dialog's title.
    fn title(&self) -> String {
        format!("Switch on {} of {}?", self.sensor, self.dag_id)
    }
}

/// One DAG and its recent runs.
#[derive(Template)]
#[template(path = "dag.html")]
struct DagPage {
    page: Page,
    deployment: String,
    several: bool,
    row: DagRow,
    description: String,
    conflicts: Vec<String>,
    runs: Vec<RunRow>,
    /// The runs list filtered to this DAG.
    runs_href: String,
    /// Where its actions are posted (`.../pause`, `.../trigger`).
    actions: String,
    /// Its concurrency limits, each with how many run now (decision 96).
    limits: Vec<LimitRow>,
    /// The trigger dialog's run parameters, rendered (empty: it takes none).
    params_html: String,
    /// What fails when a run reaches it (a connection its root doesn't define).
    warnings: Vec<String>,
    /// The connections its tasks declare (decision 207): its root's group, if any.
    connections: Option<crate::connections::Group>,
    /// Why they can't be shown now (empty: they can).
    connections_unavailable: String,
    /// Its wait steps switched off (decision 338).
    switched_off: Vec<SwitchedOffRow>,
    /// Why they can't be shown now (empty: they can).
    switched_off_unavailable: String,
    /// Its trigger, for a DAG a trigger starts.
    trigger: Option<TriggerPanel>,
}

/// A concurrency limit as the DAG page shows it.
struct LimitRow {
    /// The limit's name.
    name: String,
    /// What it is on: "the DAG", "task `x`" (per run: "task `x`, in each run").
    of: String,
    value: u32,
    /// How many run now, as said ("7 running", "7 running in its busiest run").
    running: String,
    /// Reached: what it limits waits.
    reached: bool,
}

impl LimitRow {
    fn of(limit: &schedr_api::views::LimitView) -> Self {
        let per_run = limit.limit == "max_active_per_run";
        let of = match (&limit.task_id, per_run) {
            (None, _) => "the DAG".to_owned(),
            (Some(task), false) => format!("task {task}, across its runs"),
            (Some(task), true) => format!("task {task}, in each run"),
        };
        let running = if per_run {
            format!("{} running in its busiest run", limit.running)
        } else {
            format!("{} running", limit.running)
        };
        Self {
            name: limit.limit.clone(),
            of,
            value: limit.value,
            running,
            reached: limit.running >= limit.value,
        }
    }
}

/// An error page.
#[derive(Template)]
#[template(path = "error.html")]
struct ErrorPage<'a> {
    page: &'a Page,
    title: &'a str,
    message: &'a str,
}

/// `GET /`: every deployment asked at once (a slow one doesn't hold the others back).
pub async fn dags<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some()).live("kinds=dag", "/");
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        let ui = Arc::clone(&ui);
        asked.spawn(schedr_client::for_same_user(async move {
            let client = &ui.deployments[index].client;
            let dags = client.dags().await;
            // One that doesn't answer is said once.
            let sensors = match &dags {
                Ok(_) => Some(client.switched_off_sensors().await),
                Err(_) => None,
            };
            (index, dags, sensors)
        }));
    }
    let mut answers: Vec<_> = asked.join_all().await;
    answers.sort_by_key(|(index, _, _)| *index);
    let mut rows = Vec::new();
    let mut unavailable = Vec::new();
    let mut switched_off = Vec::new();
    for (index, answer, sensors) in answers {
        let deployment = &ui.deployments[index];
        match answer {
            Ok(list) => rows.extend(list.dags.iter().map(|d| dag_row(&page, deployment, d))),
            Err(error) => unavailable.push(format!("{}: {error}", deployment.name)),
        }
        if let Some(sensors) = sensors {
            let (sensors, why) = switched_off_of(&page, deployment, sensors);
            switched_off.extend(sensors);
            if !why.is_empty() {
                unavailable.push(format!("{}: {why}", deployment.name));
            }
        }
    }
    rows.sort_by(|a, b| (&a.dag_id, &a.deployment).cmp(&(&b.dag_id, &b.deployment)));
    let several = ui.deployments.len() > 1;
    render(
        &DagsPage {
            page,
            rows,
            unavailable,
            several,
            switched_off,
        },
        StatusCode::OK,
    )
}

/// The wait steps a deployment has switched off, and why they can't be told, if so; a webserver
/// without the list (404) has none to show.
fn switched_off_of<T>(
    page: &Page,
    deployment: &Deployment<T>,
    answer: Result<schedr_api::views::SensorList, ClientError>,
) -> (Vec<SwitchedOffRow>, String) {
    match answer {
        Ok(list) => (
            list.sensors
                .iter()
                .map(|sensor| SwitchedOffRow::of(page, deployment, sensor))
                .collect(),
            String::new(),
        ),
        Err(ClientError::Server { status: 404, .. }) => (Vec::new(), String::new()),
        Err(error) => (Vec::new(), error.to_string()),
    }
}

/// `GET /d/<deployment>/dags/<dag>`.
pub async fn dag<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, dag_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some()).live(
        &format!(
            "deployment={}&dag={}",
            path_segment(&deployment),
            path_segment(&dag_id)
        ),
        &format!(
            "/d/{}/dags/{}",
            path_segment(&deployment),
            path_segment(&dag_id)
        ),
    );
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return not_found_page(
            &page,
            &format!("There is no deployment `{deployment}` here."),
        );
    };
    let dag = match deployment.client.dag(&dag_id).await {
        Ok(dag) => dag,
        Err(ClientError::Server { status: 404, .. }) => {
            return not_found_page(
                &page,
                &format!("`{dag_id}` isn't a DAG of {}.", deployment.name),
            );
        }
        Err(error) => return unavailable_page(&page, &deployment.name, &error),
    };
    let runs = match deployment.client.runs(&dag_id, None).await {
        Ok(list) => list
            .runs
            .iter()
            .map(|r| run_row(&page, deployment, r))
            .collect(),
        Err(error) => return unavailable_page(&page, &deployment.name, &error),
    };
    let conflicts = dag.conflict.iter().flatten().map(location).collect();
    // Asked only for a DAG whose tasks declare connections (the warnings name them, or none);
    // a webserver without the list (404) has none to show.
    let (connections, connections_unavailable) = match deployment.client.connections().await {
        Ok(list) => (
            crate::connections::groups(&page, deployment, &list, Some(&dag.dag_id), false)
                .into_iter()
                .next(),
            String::new(),
        ),
        Err(ClientError::Server { status: 404, .. }) => (None, String::new()),
        Err(error) => (None, error.to_string()),
    };
    let (switched_off, switched_off_unavailable) =
        switched_off_in(&page, deployment, &dag.dag_id).await;
    let trigger = dag_trigger_panel(&page, deployment, &dag).await;
    let params_html = match TriggerParams::of(&dag.params).render() {
        Ok(html) if takes_event(&dag) => format!("{html}{EVENT_FIELDS}"),
        Ok(html) => html,
        Err(error) => {
            return error_page(
                &page,
                "Error",
                &error.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    render(
        &DagPage {
            params_html,
            limits: dag.limits.iter().map(LimitRow::of).collect(),
            warnings: dag.warnings.clone(),
            connections,
            connections_unavailable,
            switched_off,
            switched_off_unavailable,
            trigger,
            row: dag_row(&page, deployment, &dag),
            description: dag.description.clone().unwrap_or_default(),
            deployment: deployment.name.clone(),
            several: ui.deployments.len() > 1,
            conflicts,
            runs,
            runs_href: format!(
                "{}/runs?deployment={}&dag={}",
                page.base,
                path_segment(&deployment.name),
                path_segment(&dag_id)
            ),
            actions: format!(
                "{}/d/{}/dags/{}",
                page.base,
                path_segment(&deployment.name),
                path_segment(&dag_id)
            ),
            page,
        },
        StatusCode::OK,
    )
}

/// Any other path: the UI's 404 page; under `/api/`, a bare 404 as the API answers it (the UI
/// is merged with the API in `schedr standalone`, and its fallback is the merged router's).
pub async fn not_found<T>(
    State(ui): State<Arc<Ui<T>>>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    if uri.path() == "/api" || uri.path().starts_with("/api/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let page = Page::new(&headers, ui.docs_dir.is_some());
    not_found_page(&page, "There is no such page.")
}

/// A 404 page saying `message`.
pub fn not_found_page(page: &Page, message: &str) -> Response {
    error_page(page, "Not found", message, StatusCode::NOT_FOUND)
}

/// A 502 page: a deployment's webserver didn't answer as it should.
pub fn unavailable_page(page: &Page, deployment: &str, error: &ClientError) -> Response {
    let message = format!("{deployment}: {error}");
    error_page(
        page,
        "Deployment unavailable",
        &message,
        StatusCode::BAD_GATEWAY,
    )
}

/// An error page titled `title` saying `message`, with `status`.
pub fn error_page(page: &Page, title: &str, message: &str, status: StatusCode) -> Response {
    render(
        &ErrorPage {
            page,
            title,
            message,
        },
        status,
    )
}

fn dag_row<T>(page: &Page, deployment: &Deployment<T>, dag: &DagView) -> DagRow {
    let utc = jiff::tz::TimeZone::UTC;
    DagRow {
        deployment: deployment.name.clone(),
        href: format!(
            "{}/d/{}/dags/{}",
            page.base,
            path_segment(&deployment.name),
            path_segment(&dag.dag_id)
        ),
        dag_id: dag.dag_id.clone(),
        paused: dag.paused,
        schedule: dag.trigger.as_ref().map_or_else(
            || "-".to_owned(),
            |t| trigger_text(t, page.zone.as_ref().unwrap_or(&utc)),
        ),
        tags: dag.tags.clone(),
        location: dag.location.as_ref().map(location).unwrap_or_default(),
        conflict: dag.conflict.as_ref().is_some_and(|c| !c.is_empty()),
        held: dag.held.as_ref().map(|h| h.reason.clone()),
    }
}

/// Where a DAG is defined, as the CLI writes it: `root:path`.
fn location(at: &LocationView) -> String {
    format!("{}:{}", at.root, at.path)
}

/// `template` as HTML with `status`; a template that fails to render (a bug) is a 500.
pub fn render(template: &impl Template, status: StatusCode) -> Response {
    match template.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the page couldn't be rendered: {error}"),
        )
            .into_response(),
    }
}
