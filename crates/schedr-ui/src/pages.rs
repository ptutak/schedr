//! The pages: the DAGs (every deployment's), one DAG with its recent runs, and the error pages.

use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use schedr_api::views::{DagView, LocationView};
use schedr_client::times::trigger_text;
use schedr_client::{ClientError, Transport, path_segment};

use crate::page::Page;
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
        asked.spawn(async move { (index, ui.deployments[index].client.dags().await) });
    }
    let mut answers: Vec<_> = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    let mut rows = Vec::new();
    let mut unavailable = Vec::new();
    for (index, answer) in answers {
        let deployment = &ui.deployments[index];
        match answer {
            Ok(list) => rows.extend(list.dags.iter().map(|d| dag_row(&page, deployment, d))),
            Err(error) => unavailable.push(format!("{}: {error}", deployment.name)),
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
        },
        StatusCode::OK,
    )
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
    render(
        &DagPage {
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
