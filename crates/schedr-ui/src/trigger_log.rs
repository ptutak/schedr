//! A DAG's trigger's log on its page (B272): a trigger belongs to no run, so its log is reached
//! by its DAG, a run of it at a time, read from the worker that ran it and masked as a task's.
//!
//! The DAG page reads it once into an element kept across the page's live updates
//! (`hx-preserve`): `GET /d/<deployment>/dags/<dag>/trigger/log?run=` gives the fragment (the
//! newest run by default, the others linked), `.../trigger/tail?run=&from=` follows it while
//! the trigger runs, as a task's log is followed.

use std::sync::Arc;

use askama::Template;
use axum::extract::rejection::QueryRejection;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use schedr_client::{ClientError, Transport, path_segment};
use serde::Deserialize;

use crate::live::{TAIL_READ, TailPart, tail_answer, tail_reader};
use crate::page::{Page, prefix};
use crate::pages::render;
use crate::runs::{LONG_LAST_LINE, bytes, log_end};
use crate::{Ui, ansi};

/// Which run's log.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogQuery {
    /// The run (its instance); the newest by default.
    #[serde(default)]
    pub run: Option<String>,
}

/// A trigger run, as the log's run links list it.
#[derive(Debug, Clone)]
struct RunLink {
    href: String,
    started: String,
    /// Why it ended, if it failed (masked by the API).
    error: String,
    running: bool,
    open: bool,
}

#[derive(Template)]
#[template(path = "trigger_log.html")]
struct LogFragment {
    runs: Vec<RunLink>,
    /// Why there is no log to show; empty when there is.
    none: String,
    html: String,
    cut: bool,
    size: String,
    running: bool,
    worker: String,
    tail: Option<String>,
}

/// `GET /d/<deployment>/dags/<dag>/trigger/log?run=`: the fragment with a trigger run's log.
pub async fn log<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, dag_id)): Path<(String, String)>,
    query: Result<Query<LogQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Response {
    let Ok(Query(query)) = query else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let base = format!(
        "{}/d/{}/dags/{}/trigger",
        page.base,
        path_segment(&deployment.name),
        path_segment(&dag_id)
    );
    let fragment = |none: String, runs: Vec<RunLink>| LogFragment {
        runs,
        none,
        html: String::new(),
        cut: false,
        size: String::new(),
        running: false,
        worker: String::new(),
        tail: None,
    };
    let client = &deployment.client;
    let listed = match client.trigger_runs(&dag_id).await {
        Ok(list) => list.runs,
        Err(error) => return render(&fragment(error.to_string(), Vec::new()), StatusCode::OK),
    };
    let Some(newest) = listed.first() else {
        return render(
            &fragment("it hasn't run yet.".to_owned(), Vec::new()),
            StatusCode::OK,
        );
    };
    let shown = query
        .run
        .filter(|run| listed.iter().any(|r| r.instance_id == *run))
        .unwrap_or_else(|| newest.instance_id.clone());
    let runs: Vec<RunLink> = listed
        .iter()
        .map(|run| RunLink {
            href: format!("{base}/log?run={}", path_segment(&run.instance_id)),
            started: page.time(&run.started_at),
            error: run.error.clone().unwrap_or_default(),
            running: run.ended_at.is_none(),
            open: run.instance_id == shown,
        })
        .collect();
    // The size first (the API's smallest part, 4 bytes), then the end, as a task's log.
    let head = match client.trigger_log_part(&dag_id, &shown, 0, 4).await {
        Ok(head) => head,
        Err(ClientError::Server { message, .. }) => {
            return render(&fragment(message, runs), StatusCode::OK);
        }
        Err(error) => return render(&fragment(error.to_string(), runs), StatusCode::OK),
    };
    let from = head.size.saturating_sub(TAIL_READ);
    let part = match client
        .trigger_log_part(&dag_id, &shown, from, TAIL_READ)
        .await
    {
        Ok(part) => part,
        Err(error) => return render(&fragment(error.to_string(), runs), StatusCode::OK),
    };
    let (text, trimmed) = log_end(&part.content);
    let mut colours = ansi::Converter::default();
    let html = if text.is_empty() && part.from > 0 && part.next >= part.size {
        LONG_LAST_LINE.to_owned()
    } else {
        crate::log_lines::to_html(&mut colours, text)
    };
    let style = match colours.sgr() {
        sgr if sgr.is_empty() => String::new(),
        sgr => format!("&style={}", sgr.replace(';', "%3B")),
    };
    let tail = (part.running || part.next < part.size).then(|| {
        tail_reader(
            &format!(
                "{base}/tail?run={}&from={}{style}",
                path_segment(&shown),
                part.next
            ),
            false,
            ui.timings.tail_interval,
        )
    });
    render(
        &LogFragment {
            runs,
            none: String::new(),
            html,
            cut: part.from > 0 || trimmed,
            size: bytes(part.size),
            running: part.running,
            worker: part.worker_id,
            tail,
        },
        StatusCode::OK,
    )
}

/// A trigger log tail's query: which run, where its log continues, and how it was shown.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailQuery {
    /// The run (its instance).
    pub run: String,
    /// The first byte not shown yet.
    pub from: u64,
    /// The colours in force where the log continues.
    #[serde(default)]
    pub style: String,
    /// Since when the log has been out of reach, in milliseconds since the epoch.
    #[serde(default)]
    pub out: Option<i64>,
}

/// `GET /d/<deployment>/dags/<dag>/trigger/tail?run=&from=`: a trigger run's log from `from`,
/// followed as [`crate::live::tail`] follows a task's.
pub async fn tail<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, dag_id)): Path<(String, String)>,
    query: Result<Query<TailQuery>, QueryRejection>,
    uri: OriginalUri,
    headers: HeaderMap,
) -> Response {
    let Ok(Query(query)) = query else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let part = deployment
        .client
        .trigger_log_part(&dag_id, &query.run, query.from, TAIL_READ)
        .await
        .map(|part| TailPart {
            content: part.content,
            next: part.next,
            size: part.size,
            running: part.running,
        });
    let url = format!(
        "{}{}?run={}",
        prefix(&headers),
        uri.path(),
        path_segment(&query.run)
    );
    tail_answer(
        &ui.timings,
        part,
        (query.from, &query.style, query.out),
        (&url, ""),
    )
}
