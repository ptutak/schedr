//! The import errors page (M4.2c, decisions 66 and 67): the files of each DAG root's current
//! code that failed to import, in every deployment, with a count in the navigation and a
//! notice on the DAG list (both loaded after the page, so no page waits for them).

use std::sync::Arc;

use askama::Template;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use schedr_api::views::{ImportErrorList, ImportErrorView};
use schedr_client::{ClientError, Transport};

use crate::page::Page;
use crate::pages::render;
use crate::{Deployment, Ui};

/// A failed file's row.
#[derive(Debug, Clone)]
pub struct ImportErrorRow {
    /// Its deployment.
    pub deployment: String,
    /// Its DAG root.
    pub root: String,
    /// The file, relative to the root.
    pub path: String,
    /// The exception's class name.
    pub exception: String,
    /// The message.
    pub message: String,
    /// The traceback's last lines.
    pub traceback: String,
    /// The content hash parsed, shortened.
    pub hash: String,
    /// When it was parsed, as people read it.
    pub parsed_at: String,
    /// The worker that parsed it.
    pub worker_id: String,
}

#[derive(Template)]
#[template(path = "import_errors.html")]
struct ImportErrorsPage {
    page: Page,
    rows: Vec<ImportErrorRow>,
    unavailable: Vec<String>,
    several: bool,
}

/// Every deployment's import errors (asked at once), in deployment order.
async fn ask_all<T: Transport + Send + Sync + 'static>(
    ui: &Arc<Ui<T>>,
) -> Vec<(usize, Result<ImportErrorList, ClientError>)> {
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        let ui = Arc::clone(ui);
        asked.spawn(schedr_client::for_same_user(async move {
            (index, ui.deployments[index].client.import_errors().await)
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    answers
}

/// `GET /import-errors`: every deployment's failed files, by deployment, root and path.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let mut rows = Vec::new();
    let mut unavailable = Vec::new();
    for (index, answer) in ask_all(&ui).await {
        let deployment = &ui.deployments[index];
        match answer {
            Ok(list) => rows.extend(list.errors.iter().map(|e| row(&page, deployment, e))),
            Err(error) => unavailable.push(format!("{}: {error}", deployment.name)),
        }
    }
    render(
        &ImportErrorsPage {
            several: ui.deployments.len() > 1,
            page,
            rows,
            unavailable,
        },
        StatusCode::OK,
    )
}

/// How many files failed in the deployments that answered.
async fn count<T: Transport + Send + Sync + 'static>(ui: &Arc<Ui<T>>) -> usize {
    ask_all(ui)
        .await
        .into_iter()
        .filter_map(|(_, answer)| answer.ok())
        .map(|list| list.errors.len())
        .sum()
}

/// `GET /import-errors/count`: the navigation's count, ` (2)`, or nothing.
pub async fn nav_count<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
) -> Response {
    match count(&ui).await {
        0 => Html(String::new()).into_response(),
        n => Html(format!(r#"<span class="count failed">{n}</span>"#)).into_response(),
    }
}

/// `GET /import-errors/notice`: the DAG list's notice when files failed, or nothing.
pub async fn notice<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    match count(&ui).await {
        0 => Html(String::new()).into_response(),
        n => {
            let files = if n == 1 {
                "1 file"
            } else {
                &format!("{n} files")
            };
            Html(format!(
                r#"<div class="banner warning">{files} failed to import, so their DAGs aren't listed: <a href="{}/import-errors">see import errors</a></div>"#,
                page.base
            ))
            .into_response()
        }
    }
}

fn row<T>(page: &Page, deployment: &Deployment<T>, error: &ImportErrorView) -> ImportErrorRow {
    let short = error
        .content_hash
        .rsplit(':')
        .next()
        .unwrap_or(&error.content_hash);
    ImportErrorRow {
        deployment: deployment.name.clone(),
        root: error.root.clone(),
        path: error.path.clone(),
        exception: error.exception.clone(),
        message: error.message.clone(),
        traceback: error.traceback.clone(),
        hash: short.chars().take(12).collect(),
        parsed_at: page.time(&error.parsed_at),
        worker_id: error.worker_id.clone(),
    }
}
