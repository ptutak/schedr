//! The workers page (M3.4): every deployment's workers, their state, slots and warnings, with
//! drain and resume.

use std::sync::Arc;

use askama::Template;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use schedr_api::views::WorkerView;
use schedr_client::{Transport, path_segment};

use crate::page::Page;
use crate::pages::render;
use crate::{Deployment, Ui};

/// A worker's row.
#[derive(Debug, Clone)]
pub struct WorkerRow {
    /// Its deployment.
    pub deployment: String,
    /// Its ID.
    pub worker_id: String,
    /// `active`, `draining`, `disconnected`, `offline` or `lost` (also its CSS class).
    pub state: String,
    /// Drained for maintenance.
    pub drained: bool,
    /// The queues it serves.
    pub queues: String,
    /// Running attempts, of its slots: `2 / 4`.
    pub load: String,
    /// Its schedr version.
    pub version: String,
    /// Its last sign of life, as people read it.
    pub last_seen: String,
    /// What is wrong with it now.
    pub warnings: Vec<String>,
    /// Where to drain it, or resume it when drained.
    pub action: String,
    /// Whether it can be drained or resumed now (not when it is gone).
    pub actionable: bool,
}

#[derive(Template)]
#[template(path = "workers.html")]
struct WorkersPage {
    page: Page,
    rows: Vec<WorkerRow>,
    unavailable: Vec<String>,
    several: bool,
}

/// `GET /workers`: every deployment's workers (asked at once), by deployment and ID.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some()).live("kinds=workers", "/workers");
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        let ui = Arc::clone(&ui);
        asked.spawn(schedr_client::for_same_user(async move {
            (index, ui.deployments[index].client.workers().await)
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    let mut rows = Vec::new();
    let mut unavailable = Vec::new();
    for (index, answer) in answers {
        let deployment = &ui.deployments[index];
        match answer {
            Ok(list) => rows.extend(list.workers.iter().map(|w| row(&page, deployment, w))),
            Err(error) => unavailable.push(format!("{}: {error}", deployment.name)),
        }
    }
    rows.sort_by(|a, b| (&a.deployment, &a.worker_id).cmp(&(&b.deployment, &b.worker_id)));
    render(
        &WorkersPage {
            several: ui.deployments.len() > 1,
            page,
            rows,
            unavailable,
        },
        StatusCode::OK,
    )
}

fn row<T>(page: &Page, deployment: &Deployment<T>, worker: &WorkerView) -> WorkerRow {
    let verb = if worker.drained { "resume" } else { "drain" };
    WorkerRow {
        deployment: deployment.name.clone(),
        worker_id: worker.worker_id.clone(),
        state: worker.state.clone(),
        drained: worker.drained,
        queues: worker.queues.join(", "),
        load: format!("{} / {}", worker.running, worker.slots),
        version: worker.schedr_version.clone(),
        last_seen: page.time(&worker.last_heartbeat_at),
        warnings: worker.warnings.iter().map(|w| w.message.clone()).collect(),
        action: format!(
            "{}/d/{}/workers/{}/{verb}",
            page.base,
            path_segment(&deployment.name),
            path_segment(&worker.worker_id)
        ),
        actionable: matches!(
            worker.state.as_str(),
            "active" | "draining" | "disconnected"
        ),
    }
}
