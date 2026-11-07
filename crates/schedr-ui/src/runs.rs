//! The run pages: the runs of every deployment (one list, 32A), a run with its graph (30A),
//! its tasks and its command history, and a task's panel beside the graph (31A): attempts,
//! output (its last 64 KiB, 33), log, and cleanup (M4.8e); the run's resources, as a table and
//! on the graph (decisions 262, 272).

use futures_util::StreamExt as _;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use askama::Template;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use jiff::Timestamp;
use schedr_api::views::{
    AllRunsQuery, AttemptView, CleanupView, CommandView, ResourceView, RunView, TaskView,
};
use schedr_client::{Client, ClientError, Transport, path_segment};
use serde::Deserialize;

use crate::graph::{self, NODE_HEIGHT, NODE_WIDTH};
use crate::page::Page;
use crate::pages::{not_found_page, render, unavailable_page};
use crate::{Deployment, Ui, ansi};

/// Runs on a page of the list.
pub const PAGE_SIZE: u32 = 50;

/// How much of an output the panel shows: its end.
pub const OUTPUT_TAIL: usize = 64 * 1024;

/// How much of a log the panel shows: its end, from a line's start (more when its last line
/// is longer).
pub const LOG_TAIL: usize = 256 * 1024;

/// What the panel shows of a log whose last line is longer than the most it reads.
const LONG_LAST_LINE: &str =
    "<span class=\"dim\">… (its last line is longer than 1 MiB: not shown here)</span>";

/// The most of a log read at once (the API's longest part).
const MAX_CHUNK: u64 = schedr_model::logs::MAX_CHUNK;

/// `text`, whole lines of a log's end, kept to its last [`LOG_TAIL`] bytes from a line's
/// start, or to its last line when that one is longer; and whether anything was left out.
pub(crate) fn log_end(text: &str) -> (&str, bool) {
    if text.len() <= LOG_TAIL {
        return (text, false);
    }
    let bytes = text.as_bytes();
    let tail_from = text.len() - LOG_TAIL;
    // The first line starting in the tail, or the last line's start (a newline is one byte:
    // the byte after it starts a character).
    let line_start = bytes[tail_from - 1..]
        .iter()
        .position(|b| *b == b'\n')
        .map(|at| tail_from + at)
        .filter(|at| *at < text.len())
        .or_else(|| {
            bytes[..text.len() - 1]
                .iter()
                .rposition(|b| *b == b'\n')
                .map(|at| at + 1)
        });
    match line_start {
        Some(at) if at > 0 => (&text[at..], true),
        _ => (text, false),
    }
}

/// The run states, as the filter offers them.
const RUN_STATES: [&str; 5] = ["queued", "running", "success", "failed", "cancelled"];

/// The panel's tabs, in order.
const TABS: [(&str, &str); 4] = [
    ("attempts", "Attempts"),
    ("output", "Output"),
    ("logs", "Logs"),
    ("cleanup", "Cleanup"),
];

/// A mapped task's panel's tabs (decision 83): its instances, and its whole output.
const MAPPED_TABS: [(&str, &str); 2] = [("instances", "Instances"), ("output", "Output")];

/// The runs list's query: filters and the page's cursor.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListQuery {
    /// Only this deployment's runs.
    #[serde(default)]
    pub deployment: Option<String>,
    /// Only this DAG's runs.
    #[serde(default)]
    pub dag: Option<String>,
    /// Only runs in this state.
    #[serde(default)]
    pub state: Option<String>,
    /// The cursor: runs triggered before this moment...
    #[serde(default)]
    pub before: Option<Timestamp>,
    /// ...or at it, with a lower ID than this run.
    #[serde(default)]
    pub before_run: Option<String>,
}

impl ListQuery {
    /// The filters with an empty value dropped (a form sends `dag=` for "any").
    fn normalised(mut self) -> Self {
        for value in [&mut self.deployment, &mut self.dag, &mut self.state] {
            if value.as_deref().is_some_and(str::is_empty) {
                *value = None;
            }
        }
        self
    }

    /// This query's URL with the cursor at `run` (the next page).
    fn next_page(&self, base: &str, run: &ListedRun) -> String {
        let mut url = format!("{base}/runs?");
        for (key, value) in [
            ("deployment", &self.deployment),
            ("dag", &self.dag),
            ("state", &self.state),
        ] {
            if let Some(value) = value {
                let _ = write!(url, "{key}={}&", path_segment(value));
            }
        }
        let _ = write!(
            url,
            "before={}&before_run={}",
            path_segment(&run.triggered_at.to_string()),
            path_segment(&run.run_id)
        );
        url
    }
}

/// A run in a list.
#[derive(Debug, Clone)]
pub struct RunRow {
    /// Its deployment.
    pub deployment: String,
    /// Its page.
    pub href: String,
    /// Its DAG's page.
    pub dag_href: String,
    /// Its ID.
    pub run_id: String,
    /// Its DAG.
    pub dag_id: String,
    /// `scheduled`, `manual`, ...
    pub kind: String,
    /// Its state (also its CSS class).
    pub state: String,
    /// The moment it is for, as people read it.
    pub planned_for: String,
    /// When it was triggered, as people read it.
    pub triggered_at: String,
    /// Who or what triggered it.
    pub triggered_by: String,
    /// When it ended, as people read it; empty while it runs.
    pub ended_at: String,
    /// Why a `success` is yellow (decision 309), as people read it; empty if it isn't.
    pub late: String,
}

/// A run as listed, before it is shown: what orders the merged list.
struct ListedRun {
    deployment: usize,
    run_id: String,
    triggered_at: Timestamp,
    view: RunView,
}

#[derive(Template)]
#[template(path = "runs.html")]
struct RunsPage {
    page: Page,
    rows: Vec<RunRow>,
    unavailable: Vec<String>,
    several: bool,
    deployments: Vec<String>,
    states: [&'static str; 5],
    query: ListQuery,
    next: Option<String>,
}

/// `GET /runs`: the newest runs of every deployment (or one), merged by when they were
/// triggered; the same cursor pages every deployment, as they share that order (32A).
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    query: Result<Query<ListQuery>, QueryRejection>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let query = match query {
        Ok(Query(query)) => query.normalised(),
        Err(rejection) => return bad_request_page(&page, &rejection.body_text()),
    };
    let mut follow = "kinds=run".to_owned();
    for (key, value) in [("deployment", &query.deployment), ("dag", &query.dag)] {
        if let Some(value) = value {
            let _ = write!(follow, "&{key}={}", path_segment(value));
        }
    }
    let here = uri
        .query()
        .map_or_else(|| "/runs".to_owned(), |q| format!("/runs?{q}"));
    let page = page.live(&follow, &here);
    let api_query = match api_query(&query) {
        Ok(q) => q,
        Err(message) => return bad_request_page(&page, &message),
    };
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        if query
            .deployment
            .as_ref()
            .is_some_and(|d| *d != ui.deployments[index].name)
        {
            continue;
        }
        let (ui, api_query) = (Arc::clone(&ui), api_query.clone());
        asked.spawn(schedr_client::for_same_user(async move {
            let answer = ui.deployments[index].client.all_runs(&api_query).await;
            (index, answer)
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    let mut listed = Vec::new();
    let mut unavailable = Vec::new();
    let mut more = false;
    for (index, answer) in answers {
        match answer {
            Ok(list) => {
                more |= list.runs.len() >= usize::try_from(PAGE_SIZE).unwrap_or(usize::MAX);
                listed.extend(list.runs.into_iter().map(|view| ListedRun {
                    deployment: index,
                    run_id: view.run_id.clone(),
                    triggered_at: view.triggered_at,
                    view,
                }));
            }
            Err(error) => unavailable.push(format!("{}: {error}", ui.deployments[index].name)),
        }
    }
    listed.sort_by(|a, b| (b.triggered_at, &b.run_id).cmp(&(a.triggered_at, &a.run_id)));
    let page_size = usize::try_from(PAGE_SIZE).unwrap_or(usize::MAX);
    more |= listed.len() > page_size;
    listed.truncate(page_size);
    let next = listed
        .last()
        .filter(|_| more)
        .map(|last| query.next_page(&page.base, last));
    let rows = listed
        .iter()
        .map(|r| run_row(&page, &ui.deployments[r.deployment], &r.view))
        .collect();
    let mut deployments: Vec<String> = ui.deployments.iter().map(|d| d.name.clone()).collect();
    deployments.sort();
    render(
        &RunsPage {
            several: ui.deployments.len() > 1,
            page,
            rows,
            unavailable,
            deployments,
            states: RUN_STATES,
            query,
            next,
        },
        StatusCode::OK,
    )
}

/// The API's query for a page of the list: its filters checked here, so a wrong one is the
/// page's 400 rather than every deployment's.
fn api_query(query: &ListQuery) -> Result<AllRunsQuery, String> {
    let dag = query
        .dag
        .as_deref()
        .map(schedr_model::DagId::new)
        .transpose()
        .map_err(|e| format!("That isn't a DAG ID: {e}"))?;
    let state = query
        .state
        .as_deref()
        .map(|s| serde_json::from_value(serde_json::Value::String(s.to_owned())))
        .transpose()
        .map_err(|_| "That isn't a run state.".to_owned())?;
    if query.before_run.is_some() && query.before.is_none() {
        return Err("A page's cursor needs its moment (`before`).".to_owned());
    }
    Ok(AllRunsQuery {
        dag,
        state,
        before: query.before,
        before_run: query.before_run.clone(),
        limit: Some(PAGE_SIZE),
    })
}

/// A run's row in a list (also the DAG page's).
pub fn run_row<T>(page: &Page, deployment: &Deployment<T>, run: &RunView) -> RunRow {
    let at = format!("{}/d/{}", page.base, path_segment(&deployment.name));
    RunRow {
        deployment: deployment.name.clone(),
        href: format!("{at}/runs/{}", path_segment(&run.run_id)),
        dag_href: format!("{at}/dags/{}", path_segment(&run.dag_id)),
        run_id: run.run_id.clone(),
        dag_id: run.dag_id.clone(),
        kind: run.kind.clone(),
        state: run.state.clone(),
        planned_for: page.time(&run.planned_for),
        triggered_at: page.time(&run.triggered_at),
        triggered_by: run.triggered_by.clone(),
        ended_at: run.ended_at.map(|t| page.time(&t)).unwrap_or_default(),
        late: run
            .late_failure
            .as_deref()
            .map(late_said)
            .unwrap_or_default(),
    }
}

/// A run page's address.
fn run_href_of(page: &Page, deployment: &str, run_id: &str) -> String {
    let (deployment, run_id) = (path_segment(deployment), path_segment(run_id));
    format!("{}/d/{deployment}/runs/{run_id}", page.base)
}

/// Why `run` is a yellow success (decision 309), as people read it; empty if it isn't.
fn late_of(run: &RunView) -> String {
    run.late_failure
        .as_deref()
        .map(late_said)
        .unwrap_or_default()
}

/// Why a run that ended `success` is yellow (decision 309), as people read it.
#[must_use]
pub fn late_said(reason: &str) -> String {
    match reason {
        "late_cleanup_failed" => {
            "late cleanup failed: a cleanup after the run's end failed, handles may be left \
             behind"
                .to_owned()
        }
        other => other.replace('_', " "),
    }
}

/// The run page's and the panel's query.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RunQuery {
    /// The task whose panel is open.
    #[serde(default)]
    pub task: Option<String>,
    /// The panel's tab (`attempts` by default).
    #[serde(default)]
    pub tab: Option<String>,
    /// The try whose log is shown (the last by default).
    #[serde(default, rename = "try")]
    pub try_number: Option<u32>,
    /// The instance of a mapped task whose panel is open (none: the task as a whole).
    #[serde(default)]
    pub map_index: Option<u32>,
}

/// A task node of the graph.
#[derive(Debug, Clone)]
pub struct Node {
    /// Whether the master ends it with the first of several tasks (`first_of`, `at_least`).
    pub first_of: bool,
    /// Whether it is what ended such a node.
    pub won: bool,
    /// The box's left edge.
    pub left: f64,
    /// The box's top edge.
    pub top: f64,
    /// The task's ID.
    pub task_id: String,
    /// The ID as it fits in the box.
    pub label: String,
    /// Its state (`none` before the run created it).
    pub state: String,
    /// How its state looks (its CSS classes).
    pub look: String,
    /// Its outcome when it says something (not `SUCCESS`).
    pub outcome: String,
    /// A mapped task: how far its instances are (`2/3 done · 1 failed`).
    pub counts: String,
    /// Its panel: the run page with it open.
    pub href: String,
    /// Its panel alone (htmx).
    pub panel: String,
    /// Whether its panel is open.
    pub selected: bool,
    /// A resource's setup or cleanup: its cap's outline (rounded on top for the setup, at the
    /// bottom for the cleanup, decision 262); empty for any other task.
    pub cap: String,
    /// The band across a cap's rounded side, in the resources' colour: `(x1, x2, y)`.
    pub band: (f64, f64, f64),
    /// The resource it sets up or cleans up; empty for any other task.
    pub resource: String,
    /// The resources it uses, a dot each.
    pub uses: Vec<Badge>,
}

/// A resource's rail on the graph: a line in the side gutter from its setup to its cleanup,
/// shown on hover or selection (decision 262).
#[derive(Debug, Clone)]
pub struct Rail {
    /// The resource.
    pub resource: String,
    /// The SVG path.
    pub path: String,
}

/// An edge of the graph.
#[derive(Debug, Clone)]
pub struct EdgeLine {
    /// The SVG path.
    pub path: String,
    /// Its upstream task.
    pub from: String,
    /// Its downstream task.
    pub to: String,
    /// `data`, `order`, `selection` or `requirement`, with `released` for the link to a
    /// cleanup nothing waits for (its CSS classes).
    pub kind: String,
    /// The outcomes it is taken on (`Rows | Unavailable`); empty for any.
    pub on: String,
    /// Where its label goes: beside its end.
    pub label_x: f64,
    /// See `label_x`.
    pub label_y: f64,
    /// Whether its upstream task ended with an outcome it isn't taken on, was skipped, or is a
    /// branch that didn't choose its downstream task.
    pub not_taken: bool,
    /// A resource's line drawn only on hover or selection of the resource (its setup), or its
    /// summary line (decision 291); empty for any other.
    pub resource: String,
}

/// A dot on a task using a resource, in the resources' colour (decision 291).
#[derive(Debug, Clone)]
pub struct Badge {
    /// Its centre.
    pub x: f64,
    /// See `x`.
    pub y: f64,
    /// The resource (its setup's ID).
    pub resource: String,
}

/// The graph as drawn.
#[derive(Debug, Clone)]
pub struct Graph {
    /// Its width.
    pub width: f64,
    /// Its height.
    pub height: f64,
    /// The task boxes' size.
    pub node_width: f64,
    /// The task boxes' size.
    pub node_height: f64,
    /// The tasks.
    pub nodes: Vec<Node>,
    /// The dependencies.
    pub edges: Vec<EdgeLine>,
    /// The task groups' boxes, outer before inner (decision 65).
    pub groups: Vec<GroupFrame>,
    /// The resources' rails.
    pub rails: Vec<Rail>,
    /// The resources' section, left of the tasks: `(left, top, width, height)`.
    pub resources: Option<(f64, f64, f64, f64)>,
}

/// A task group's box on the graph.
#[derive(Debug, Clone)]
pub struct GroupFrame {
    /// Its left edge.
    pub left: f64,
    /// Its top edge.
    pub top: f64,
    /// Its width.
    pub width: f64,
    /// Its height.
    pub height: f64,
    /// Its name (its own part of its ID).
    pub label: String,
    /// Its ID, for the tooltip.
    pub group_id: String,
    /// Whether it holds what a `first_of` chooses from, not a group of the DAG.
    pub options: bool,
}

/// A task's row in the run's table.
#[derive(Debug, Clone)]
pub struct TaskRow {
    /// Its ID as shown: with its instance count for a mapped task (`fetch ×3`).
    pub label: String,
    /// Its panel (the run page with it open).
    pub href: String,
    /// Its panel alone (htmx).
    pub panel: String,
    /// How its state looks (its CSS classes).
    pub look: String,
    /// `state · outcome`, the outcome only when it says something.
    pub status: String,
    /// Attempts made.
    pub tries: u32,
    /// Why it waits or can't be scheduled.
    pub reason: String,
    /// When it last changed, as people read it.
    pub updated_at: String,
}

/// A resource's row in the run's table: one row in two parts, its setup and its cleanup
/// (decision 262).
#[derive(Debug, Clone)]
pub struct ResourceRow {
    /// Its name: its setup's task ID.
    pub name: String,
    /// Its state (`waiting_for_users`, `cleaned`, ...; also its CSS class).
    pub state: String,
    /// The state as people read it (`waiting for users`).
    pub status: String,
    /// The setup: its state, panel and page.
    pub setup: ResourcePart,
    /// The cleanup: its state, panel and page.
    pub cleanup: ResourcePart,
    /// The tasks that use it.
    pub users: String,
    /// Whether what follows its group waits for its cleanup.
    pub blocking: bool,
}

/// A resource's setup or cleanup in its row.
#[derive(Debug, Clone)]
pub struct ResourcePart {
    /// Its task's ID.
    pub task_id: String,
    /// Its task's state (also its CSS class).
    pub state: String,
    /// Its panel (htmx).
    pub panel: String,
    /// The run page with its panel open.
    pub href: String,
}

/// A command about the run.
#[derive(Debug, Clone)]
pub struct CommandRow {
    /// When it was asked for, as people read it.
    pub requested_at: String,
    /// Who asked.
    pub requested_by: String,
    /// What (`trigger_run`, `cancel_task`, ...).
    pub kind: String,
    /// `pending`, `applied` or `rejected` (also its CSS class).
    pub status: String,
    /// The master's reason for a rejection.
    pub reason: String,
}

#[derive(Template)]
#[template(path = "run.html")]
struct RunPage {
    page: Page,
    /// The run's page (its actions are posted under it).
    run_href: String,
    deployment: String,
    several: bool,
    dag_href: String,
    runs_href: String,
    run: RunView,
    planned_for: String,
    triggered_at: String,
    started_at: String,
    ended_at: String,
    /// Why a `success` is yellow (decision 309), as people read it; empty if it isn't.
    late: String,
    summary: String,
    code_hashes: Vec<String>,
    /// The run's parameters, each name with its value as given (masked, decision 134).
    params: Vec<(String, String)>,
    graph: Graph,
    tasks: Vec<TaskRow>,
    resources: Vec<ResourceRow>,
    commands: Vec<CommandRow>,
    /// The open panel's HTML (rendered by [`PanelFragment`]), empty when none is open.
    panel: String,
}

/// A run's parameters as shown: masked again (the webserver did, decision 134), a text as
/// it is, any other value as JSON.
fn shown_params(mut params: serde_json::Value) -> Vec<(String, String)> {
    schedr_config::masked::mask_data(&mut params);
    params
        .as_object()
        .into_iter()
        .flatten()
        .map(|(name, value)| {
            let shown = match value {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            (name.clone(), shown)
        })
        .collect()
}

/// `GET /d/<deployment>/runs/<run>`: a run with its graph, tasks and commands; with `?task=`
/// its panel open.
pub async fn run<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id)): Path<(String, String)>,
    query: Result<Query<RunQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Response {
    let page = live_run(
        Page::new(&headers, ui.docs_dir.is_some()),
        &deployment,
        &run_id,
    );
    let query = match query {
        Ok(Query(query)) => query,
        Err(rejection) => return bad_request_page(&page, &rejection.body_text()),
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return no_deployment(&page, &deployment);
    };
    let client = &deployment.client;
    let (detail, dag, commands) = tokio::join!(
        client.run(&run_id),
        client.run_dag(&run_id),
        client.run_commands(&run_id)
    );
    let (detail, dag, commands) = match (detail, dag, commands) {
        (Ok(detail), Ok(dag), Ok(commands)) => (detail, dag, commands),
        (
            Err(ClientError::Server {
                status: 404 | 400, ..
            }),
            _,
            _,
        ) => {
            return not_found_page(
                &page,
                &format!("There is no run `{run_id}` in {}.", deployment.name),
            );
        }
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
            return unavailable_page(&page, &deployment.name, &error);
        }
    };
    let run_href = run_href_of(&page, &deployment.name, &run_id);
    let tail = ui.timings.tail_interval;
    let panel = match &query.task {
        Some(task) => {
            match task_panel(&page, deployment, &run_id, &run_href, task, &query, tail).await {
                Ok(html) => html,
                Err(failure) => return failure.page(&page),
            }
        }
        None => String::new(),
    };
    let (whole, tasks) = task_rows(&page, &run_href, &detail.tasks, &WaitSteps::of(&dag));
    let graph = run_graph(deployment, &run_id, &dag, &detail.tasks, &run_href, &query).await;
    let at = format!("{}/d/{}", page.base, path_segment(&deployment.name));
    render(
        &RunPage {
            deployment: deployment.name.clone(),
            several: ui.deployments.len() > 1,
            dag_href: format!("{at}/dags/{}", path_segment(&detail.run.dag_id)),
            runs_href: format!(
                "{}/runs?deployment={}&dag={}",
                page.base,
                path_segment(&deployment.name),
                path_segment(&detail.run.dag_id)
            ),
            planned_for: page.time(&detail.run.planned_for),
            triggered_at: page.time(&detail.run.triggered_at),
            started_at: detail
                .run
                .started_at
                .map(|t| page.time(&t))
                .unwrap_or_default(),
            ended_at: detail
                .run
                .ended_at
                .map(|t| page.time(&t))
                .unwrap_or_default(),
            summary: summary(&detail.run.state, &whole),
            late: late_of(&detail.run),
            run_href: run_href.clone(),
            code_hashes: detail.code_hashes,
            params: shown_params(detail.params),
            commands: commands
                .commands
                .iter()
                .map(|c| command_row(&page, c))
                .collect(),
            resources: (detail.resources.iter())
                .map(|r| resource_row(&run_href, r, &detail.tasks))
                .collect(),
            run: detail.run,
            graph,
            tasks,
            panel,
            page,
        },
        StatusCode::OK,
    )
}

/// A run's page live: it follows the run, and reads its live parts again without the open
/// panel (they don't depend on it).
fn live_run(page: Page, deployment: &str, run_id: &str) -> Page {
    let (deployment, run_id) = (path_segment(deployment), path_segment(run_id));
    page.live(
        &format!("deployment={deployment}&run={run_id}"),
        &format!("/d/{deployment}/runs/{run_id}"),
    )
}

/// The run's result in one line: its state, and for a failed or cancelled one the tasks that
/// made it so.
fn summary(state: &str, tasks: &[TaskView]) -> String {
    let named = |states: &[&str]| -> Vec<String> {
        tasks
            .iter()
            .filter(|t| states.contains(&t.state.as_str()))
            .map(|t| t.task_id.clone())
            .collect()
    };
    let listed = |verb: &str, ids: &[String]| -> String {
        match ids {
            [] => String::new(),
            [one] => format!(": task {one} {verb}"),
            many => format!(": tasks {} {verb}", many.join(", ")),
        }
    };
    match state {
        "failed" => format!("failed{}", listed("failed", &named(&["failed"]))),
        "cancelled" => format!("cancelled{}", listed("cancelled", &named(&["cancelled"]))),
        "running" => {
            let running = named(&["running", "cleaning_up"]).len();
            let stuck = named(&["unschedulable"]);
            let mut line = format!("running: {running} of {} tasks running", tasks.len());
            if !stuck.is_empty() {
                let _ = write!(line, "; can't be scheduled: {}", stuck.join(", "));
            }
            line
        }
        other => other.to_owned(),
    }
}

/// The outcome of a wait step whose time ran out.
const TIMED_OUT: &str = "TimedOut";

/// A DAG's wait steps, for how they look.
#[derive(Debug, Default)]
struct WaitSteps(BTreeSet<String>);

impl WaitSteps {
    fn of(dag: &serde_json::Value) -> Self {
        Self(
            dag["tasks"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|task| task["kind"] == "sensor")
                .filter_map(|task| task["id"].as_str().map(str::to_owned))
                .collect(),
        )
    }

    /// How a task's state looks (its CSS classes). A wait step at work pulses. One whose time
    /// ran out looks skipped, as nothing broke and what it waited for didn't come; it says
    /// `TimedOut` all the same, as it did run.
    fn look(&self, task: &TaskView) -> String {
        let wait_step = self.0.contains(&task.task_id);
        if wait_step && task.state == "running" {
            "running waiting".to_owned()
        } else if wait_step && task.state == "success" && task.outcome.as_deref() == Some(TIMED_OUT)
        {
            "skipped waited-out".to_owned()
        } else {
            task.state.clone()
        }
    }
}

/// `state · outcome · cleanup`, each part only when it says something: `SUCCESS` doesn't, nor
/// a cleanup that went well or wasn't needed.
fn status(task: &TaskView) -> String {
    let mut line = task.state.clone();
    if let Some(outcome) = task.outcome.as_deref().filter(|o| *o != "SUCCESS") {
        let _ = write!(line, " · {outcome}");
    }
    if let Some(cleanup) = cleanup_said(task.cleanup.as_deref()) {
        let _ = write!(line, " · {cleanup}");
    }
    line
}

/// How a cleanup ended, when it says something: `cleanup failed`, `cleanup lost` (its worker
/// was lost with what it should have cleaned up).
fn cleanup_said(result: Option<&str>) -> Option<&'static str> {
    match result? {
        "failed" => Some("cleanup failed"),
        "worker_lost" => Some("cleanup lost"),
        _ => None,
    }
}

/// The run's tasks as a whole, and their rows; a mapped task's instances are in its panel
/// (decision 83).
fn task_rows(
    page: &Page,
    run_href: &str,
    tasks: &[TaskView],
    wait_steps: &WaitSteps,
) -> (Vec<TaskView>, Vec<TaskRow>) {
    let whole: Vec<TaskView> = tasks
        .iter()
        .filter(|t| t.map_index == -1)
        .cloned()
        .collect();
    let rows = whole
        .iter()
        .map(|t| task_row(page, run_href, t, wait_steps))
        .collect();
    (whole, rows)
}

fn task_row(page: &Page, run_href: &str, task: &TaskView, wait_steps: &WaitSteps) -> TaskRow {
    let id = path_segment(&task.task_id);
    TaskRow {
        label: task.instances.map_or_else(
            || task.task_id.clone(),
            |n| format!("{} ×{n}", task.task_id),
        ),
        href: format!("{run_href}?task={id}"),
        panel: format!("{run_href}/tasks/{id}"),
        look: wait_steps.look(task),
        status: status(task),
        tries: task.try_number,
        reason: task.reason.clone().unwrap_or_default(),
        updated_at: page.time(&task.updated_at),
    }
}

fn resource_row(run_href: &str, resource: &ResourceView, tasks: &[TaskView]) -> ResourceRow {
    let part = |task_id: &str| {
        let segment = path_segment(task_id);
        ResourcePart {
            task_id: task_id.to_owned(),
            state: tasks
                .iter()
                .find(|t| t.task_id == task_id && t.map_index == -1)
                .map_or_else(|| "scheduled".to_owned(), |t| t.state.clone()),
            panel: format!("{run_href}/tasks/{segment}"),
            href: format!("{run_href}?task={segment}"),
        }
    };
    ResourceRow {
        name: resource.setup.clone(),
        state: resource.state.clone(),
        status: resource.state.replace('_', " "),
        setup: part(&resource.setup),
        cleanup: part(&resource.cleanup),
        users: resource.users.join(", "),
        blocking: resource.blocking,
    }
}

fn command_row(page: &Page, command: &CommandView) -> CommandRow {
    CommandRow {
        requested_at: page.time(&command.requested_at),
        requested_by: command.requested_by.clone(),
        kind: command.kind.clone(),
        status: command.status.clone(),
        reason: command.reason.clone().unwrap_or_default(),
    }
}

/// The run's graph, with what its branches chose.
async fn run_graph<T: Transport>(
    deployment: &Deployment<T>,
    run_id: &str,
    dag: &serde_json::Value,
    tasks: &[TaskView],
    run_href: &str,
    query: &RunQuery,
) -> Graph {
    let mut chosen = chosen_by_branches(deployment, run_id, dag, tasks).await;
    chosen.extend(first_of_winners(deployment, run_id, dag, tasks).await);
    graph_of(dag, tasks, &chosen, run_href, query.task.as_deref())
}

/// The tasks of a DAG's `first_of` nodes (`first_of`, `at_least`).
fn first_of_nodes(dag: &serde_json::Value) -> impl Iterator<Item = &str> {
    dag["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| t["kind"].as_str() == Some("first_of"))
        .filter_map(|t| t["id"].as_str())
}

/// Who won each `first_of` that succeeded, from its stored output; one that can't be read is
/// left out, its options drawn with no winner.
async fn first_of_winners<T: Transport>(
    deployment: &Deployment<T>,
    run_id: &str,
    dag: &serde_json::Value,
    tasks: &[TaskView],
) -> BTreeMap<String, Vec<String>> {
    let nodes: Vec<String> = first_of_nodes(dag)
        .filter(|id| {
            tasks
                .iter()
                .any(|t| t.task_id == *id && t.state == "success")
        })
        .map(str::to_owned)
        .collect();
    futures_util::stream::iter(nodes)
        .map(|id| async move {
            let output = deployment.client.output(run_id, &id).await.ok()?;
            let winner = &output.value["winner"];
            let winners = match winner.as_str() {
                Some(one) => vec![one.to_owned()],
                None => winner
                    .as_array()?
                    .iter()
                    .filter_map(|w| w.as_str().map(str::to_owned))
                    .collect(),
            };
            Some((id, winners))
        })
        .buffer_unordered(BRANCH_READS)
        .filter_map(std::future::ready)
        .collect()
        .await
}

/// A box around the options of a `first_of`.
struct OptionsBox {
    /// The node they lead to.
    node: usize,
    /// Its label.
    label: String,
    /// The options.
    options: Vec<usize>,
}

/// Which `first_of` nodes have their options in one box (decision 351): not a task that is an
/// option of two nodes, options at different depths, in different groups or among the
/// resources. Those keep a dashed line each.
fn options_boxes(
    dag: &serde_json::Value,
    ids: &[String],
    pairs: &[(usize, usize)],
    node_group: &[Option<usize>],
    sides: &[(usize, usize)],
) -> Vec<OptionsBox> {
    let index = |id: &str| ids.iter().position(|i| i == id);
    let layer = graph::layers(ids.len(), pairs);
    let aside = |n: usize| {
        sides
            .iter()
            .any(|(setup, cleanup)| *setup == n || *cleanup == n)
    };
    let wanted: Vec<OptionsBox> = dag["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| t["kind"].as_str() == Some("first_of"))
        .filter_map(|t| {
            let node = index(t["id"].as_str()?)?;
            let options: Vec<usize> = pairs
                .iter()
                .filter(|(_, to)| *to == node)
                .map(|(from, _)| *from)
                .filter(|from| !aside(*from))
                .collect();
            let label = t["at_least"]
                .as_u64()
                .map_or_else(|| "first of".to_owned(), |n| format!("at least {n} of"));
            Some(OptionsBox {
                node,
                label,
                options,
            })
        })
        .collect();
    let in_one = |b: &OptionsBox| {
        let first = b.options.first().copied();
        first.is_some_and(|first| {
            b.options
                .iter()
                .all(|o| layer[*o] == layer[first] && node_group.get(*o) == node_group.get(first))
        }) && wanted.iter().all(|other| {
            other.node == b.node || !other.options.iter().any(|o| b.options.contains(o))
        })
    };
    let keep: Vec<bool> = wanted.iter().map(in_one).collect();
    wanted
        .into_iter()
        .zip(keep)
        .filter_map(|(b, keep)| keep.then_some(b))
        .collect()
}

/// What each `@branch` task that succeeded chose (its stored output, the IDs of its targets;
/// anything else chose nothing, as the master reads it); a branch whose output can't be read
/// is left out, its edges drawn as taken.
pub(crate) async fn chosen_by_branches<T: Transport>(
    deployment: &Deployment<T>,
    run_id: &str,
    dag: &serde_json::Value,
    tasks: &[TaskView],
) -> BTreeMap<String, Vec<String>> {
    let succeeded = |id: &str| {
        tasks
            .iter()
            .any(|t| t.task_id == id && t.state == "success")
    };
    // Owned: a stream of borrowed IDs isn't `Send` for every lifetime the router asks.
    let branches: Vec<String> = dag["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| t["branch"].as_bool() == Some(true))
        .filter_map(|t| t["id"].as_str())
        .filter(|id| succeeded(id))
        .map(str::to_owned)
        .collect();
    // Read together, a few at once (B147): one at a time, a refresh took their sum.
    futures_util::stream::iter(branches)
        .map(|id| async move {
            let output = deployment.client.output(run_id, &id).await.ok()?;
            // As the master reads it: anything but a list of IDs chose nothing.
            let targets = output.value.as_array().into_iter().flatten();
            let targets = targets.filter_map(|v| v.as_str().map(str::to_owned));
            Some((id, targets.collect()))
        })
        .buffer_unordered(BRANCH_READS)
        .filter_map(std::future::ready)
        .collect()
        .await
}

/// How many branches' outputs a run's page reads at once.
const BRANCH_READS: usize = 8;

/// The graph of the DAG version the run ran with, its tasks coloured by their state; `chosen`
/// is what its branches chose (see [`chosen_by_branches`]).
fn graph_of(
    dag: &serde_json::Value,
    tasks: &[TaskView],
    chosen: &BTreeMap<String, Vec<String>>,
    run_href: &str,
    selected: Option<&str>,
) -> Graph {
    let ids: Vec<String> = dag["tasks"]
        .as_array()
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|t| t["id"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let wait_steps = WaitSteps::of(dag);
    let index = |id: &str| ids.iter().position(|i| i == id);
    let resources = resources_of(dag, index);
    let mut edges = joined_edges(dag, index);
    resource_edges(&mut edges, &resources);
    let pairs: Vec<(usize, usize)> = edges.iter().map(|(f, t, _, _)| (*f, *t)).collect();
    let (mut group_ids, mut specs, mut node_group) = groups_of(dag, &ids);
    // The resources beside the tasks, in a section of their own (the user, 2026-11-05).
    let sides: Vec<(usize, usize)> = resources.iter().map(|r| (r.setup, r.cleanup)).collect();
    let declared_groups = group_ids.len();
    let boxes = options_boxes(dag, &ids, &pairs, &node_group, &sides);
    add_options_groups(&boxes, &mut group_ids, &mut specs, &mut node_group);
    let is_first_of = |id: &str| first_of_nodes(dag).any(|node| node == id);
    let layout = graph::layout_sided(ids.len(), &pairs, &specs, &node_group, &sides);
    let nodes = ids
        .iter()
        .zip(&layout.nodes)
        .map(|(id, placed)| {
            let task = tasks.iter().find(|t| &t.task_id == id && t.map_index == -1);
            let segment = path_segment(id);
            let label = task
                .and_then(|t| t.instances)
                .map_or_else(|| fitted(id, 22), |n| format!("{} ×{n}", fitted(id, 17)));
            let (left, top) = (placed.x - NODE_WIDTH / 2.0, placed.y - NODE_HEIGHT / 2.0);
            let (cap, band, resource) = cap_of(&resources, &ids, index(id), left, top);
            let uses = badges(&resources, &ids, index(id), left, top);
            Node {
                uses,
                left,
                top,
                cap,
                band,
                resource,
                task_id: id.clone(),
                label,
                first_of: is_first_of(id),
                won: first_of_nodes(dag)
                    .any(|node| chosen.get(node).is_some_and(|won| won.contains(id))),
                counts: task
                    .and_then(|t| t.instances)
                    .map(|n| counts(id, n, tasks))
                    .unwrap_or_default(),
                state: task.map_or_else(|| "none".to_owned(), |t| t.state.clone()),
                look: task.map_or_else(|| "none".to_owned(), |t| wait_steps.look(t)),
                outcome: chosen
                    .get(id)
                    .filter(|_| is_first_of(id))
                    .map(|won| won.join(", "))
                    .or_else(|| task.and_then(|t| t.outcome.clone()))
                    .filter(|o| o != "SUCCESS")
                    .map(|o| fitted(&o, 24))
                    .unwrap_or_default(),
                href: format!("{run_href}?task={segment}"),
                panel: format!("{run_href}/tasks/{segment}"),
                selected: selected == Some(id.as_str()),
            }
        })
        .collect();
    let lines = edge_lines(&layout, &edges, tasks, chosen, &ids, dag);
    let grouping = (
        group_ids.as_slice(),
        specs.as_slice(),
        node_group.as_slice(),
    );
    let lines = resource_lines(&resources, &ids, &edges, &layout, grouping, lines);
    let mut groups = group_frames(&layout.boxes, &group_ids);
    let lines = one_line_per_box(lines, &boxes, &layout.boxes, declared_groups, &ids);
    mark_options_frames(&mut groups, &layout.boxes, &boxes, declared_groups, &ids);
    Graph {
        width: layout.width,
        height: layout.height,
        node_width: NODE_WIDTH,
        node_height: NODE_HEIGHT,
        nodes,
        edges: lines,
        groups,
        rails: rails_of(&resources, &ids, &layout.nodes),
        resources: layout.side.map(|b| (b.left, b.top, b.width, b.height)),
    }
}

/// Each options box as a group of the layout, inside the group its tasks are in.
fn add_options_groups(
    boxes: &[OptionsBox],
    group_ids: &mut Vec<String>,
    specs: &mut Vec<graph::GroupSpec>,
    node_group: &mut [Option<usize>],
) {
    for options in boxes {
        let parent = options.options.first().and_then(|o| node_group[*o]);
        specs.push(graph::GroupSpec { parent });
        for option in &options.options {
            node_group[*option] = Some(group_ids.len());
        }
        group_ids.push(options.label.clone());
    }
}

/// Which of the frames are options boxes, and what they say.
fn mark_options_frames(
    frames: &mut [GroupFrame],
    placed: &[graph::GroupBox],
    boxes: &[OptionsBox],
    declared_groups: usize,
    ids: &[String],
) {
    for (frame, group) in frames.iter_mut().zip(placed) {
        if let Some(options) = group.group.checked_sub(declared_groups) {
            frame.options = true;
            frame.group_id = format!("what {} chooses from", ids[boxes[options].node]);
            frame.label.clone_from(&boxes[options].label);
        }
    }
}

/// A line per route; into a `first_of`, it is an option's.
fn edge_lines(
    layout: &graph::Layout,
    edges: &[Joined],
    tasks: &[TaskView],
    chosen: &BTreeMap<String, Vec<String>>,
    ids: &[String],
    dag: &serde_json::Value,
) -> Vec<EdgeLine> {
    layout
        .routes
        .iter()
        .map(|route| {
            let (from, to, kind, on) = &edges[route.edge];
            let upstream = tasks.iter().find(|t| t.task_id == ids[*from]);
            let chose = chosen.get(&ids[*from]);
            let not_taken = not_taken(&edges[route.edge], upstream, chose, &ids[*to]);
            let end = route.points.last().copied().unwrap_or_default();
            EdgeLine {
                from: ids[*from].clone(),
                to: ids[*to].clone(),
                path: path_of(&route.points),
                kind: if first_of_nodes(dag).any(|node| node == ids[*to]) {
                    "option".to_owned()
                } else {
                    kind.clone()
                },
                on: on
                    .as_ref()
                    .map(|on| fitted(&on.join(" | "), 28))
                    .unwrap_or_default(),
                label_x: end.0 + 6.0,
                label_y: end.1 - 12.0,
                not_taken,
                resource: String::new(),
            }
        })
        .collect()
}

/// Each options box's lines replaced by one dashed line from the box to its node (decision 351).
fn one_line_per_box(
    lines: Vec<EdgeLine>,
    boxes: &[OptionsBox],
    placed: &[graph::GroupBox],
    declared_groups: usize,
    ids: &[String],
) -> Vec<EdgeLine> {
    let mut kept = Vec::with_capacity(lines.len());
    let mut joined: Vec<(usize, (f64, f64))> = Vec::new();
    for line in lines {
        let its_box = boxes
            .iter()
            .position(|b| ids[b.node] == line.to && b.options.iter().any(|o| ids[*o] == line.from));
        match its_box {
            Some(at) => {
                if !joined.iter().any(|(b, _)| *b == at) {
                    let end = end_of(&line.path).unwrap_or_default();
                    joined.push((at, end));
                }
            }
            None => kept.push(line),
        }
    }
    for (at, end) in joined {
        let frame = placed
            .iter()
            .find(|b| b.group.checked_sub(declared_groups) == Some(at));
        let Some(frame) = frame else { continue };
        let start = (frame.left + frame.width / 2.0, frame.top + frame.height);
        kept.push(EdgeLine {
            from: String::new(),
            to: ids[boxes[at].node].clone(),
            path: path_of(&[start, end]),
            kind: "option".to_owned(),
            on: String::new(),
            label_x: 0.0,
            label_y: 0.0,
            not_taken: false,
            resource: String::new(),
        });
    }
    kept
}

/// The last point of an SVG path from [`path_of`].
fn end_of(path: &str) -> Option<(f64, f64)> {
    let last = path
        .rsplit([' ', 'C', 'M', 'L'])
        .find(|part| !part.is_empty())?;
    let (x, y) = last.split_once(',')?;
    Some((x.parse().ok()?, y.parse().ok()?))
}

/// The dots of the resources the task at `at` uses, along its box's top right corner.
fn badges(
    resources: &[GraphResource],
    ids: &[String],
    at: Option<usize>,
    left: f64,
    top: f64,
) -> Vec<Badge> {
    resources
        .iter()
        .filter(|r| at.is_some_and(|at| r.users.contains(&at)))
        .enumerate()
        .map(|(k, r)| Badge {
            x: left + NODE_WIDTH - 10.0 - 10.0 * to_f64(k),
            y: top + 10.0,
            resource: ids[r.setup].clone(),
        })
        .collect()
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a small count: a task's resources"
)]
const fn to_f64(n: usize) -> f64 {
    n as f64
}

/// A resource's lines cut down (decision 291): when its users share a group, one line from its
/// setup into the group's box and one from the box to its cleanup, the lines to and from each
/// user drawn only on hover; otherwise the lines to its first users and from its last ones, the
/// others on hover. `lines` are `layout`'s routes drawn, in the same order.
fn resource_lines(
    resources: &[GraphResource],
    ids: &[String],
    edges: &[Joined],
    layout: &graph::Layout,
    groups: (&[String], &[graph::GroupSpec], &[Option<usize>]),
    mut lines: Vec<EdgeLine>,
) -> Vec<EdgeLine> {
    let (group_ids, specs, node_group) = groups;
    let mut summaries = Vec::new();
    for r in resources {
        let setup_id = &ids[r.setup];
        let into: Vec<usize> = (0..layout.routes.len())
            .filter(|&i| {
                let route = &layout.routes[i];
                route.from == r.setup && r.users.contains(&route.to)
            })
            .collect();
        let out: Vec<usize> = (0..layout.routes.len())
            .filter(|&i| {
                let route = &layout.routes[i];
                route.to == r.cleanup && r.users.contains(&route.from)
            })
            .collect();
        let shared = shared_group(&r.users, (specs, node_group))
            .and_then(|g| layout.boxes.iter().find(|b| b.group == g));
        let hidden: Vec<usize> = if let (Some(frame), Some(&first_in), Some(&first_out)) =
            (shared, into.first(), out.first())
        {
            let rows = r
                .users
                .iter()
                .filter_map(|&u| layout.nodes.get(u))
                .map(|n| n.y);
            let first = rows.clone().fold(f64::INFINITY, f64::min);
            let last = rows.fold(f64::NEG_INFINITY, f64::max);
            let (going, coming) = (&layout.routes[first_in], &layout.routes[first_out]);
            if going.points.len() < 3 || coming.points.len() < 4 {
                continue;
            }
            // Out of the setup to its track (as its own lines go), along it to the users'
            // first row, into the box's left side.
            let mut to_box = going.points[..3].to_vec();
            let track = going.points[2].0;
            to_box.extend([(track, first), (frame.left, first)]);
            // Out of the box's left side at the users' last row, to the cleanup's track,
            // and on as the users' own lines go.
            let back = coming.points[2].0;
            let mut from_box = vec![(frame.left, last), (back, last)];
            from_box.extend_from_slice(&coming.points[3..]);
            let group_id = group_ids.get(frame.group).cloned().unwrap_or_default();
            for (points, kind, from, to) in [
                (
                    to_box,
                    "requirement".to_owned(),
                    setup_id.clone(),
                    group_id.clone(),
                ),
                (
                    from_box,
                    lines[first_out].kind.clone(),
                    group_id,
                    ids[r.cleanup].clone(),
                ),
            ] {
                summaries.push(EdgeLine {
                    path: path_of(&points),
                    from,
                    to,
                    kind: format!("{kind} resource-summary"),
                    on: String::new(),
                    label_x: 0.0,
                    label_y: 0.0,
                    not_taken: false,
                    resource: setup_id.clone(),
                });
            }
            into.into_iter().chain(out).collect()
        } else {
            // Only the lines to its first users stay; those from its last ones are all there
            // is (`resource_edges`).
            let first = first_users(edges, &r.users);
            into.into_iter()
                .filter(|&i| !first.contains(&layout.routes[i].to))
                .collect()
        };
        for i in hidden {
            let line = &mut lines[i];
            line.kind.push_str(" resource-detail");
            line.resource.clone_from(setup_id);
        }
    }
    lines.extend(summaries);
    lines
}

/// The innermost group every one of `users` is in, if any (`groups`: the specs and each
/// node's innermost group).
fn shared_group(
    users: &[usize],
    (specs, node_group): (&[graph::GroupSpec], &[Option<usize>]),
) -> Option<usize> {
    let path = |n: usize| {
        let mut path = Vec::new();
        let mut at = node_group.get(n).copied().flatten();
        while let Some(g) = at.filter(|&g| g < specs.len() && path.len() <= specs.len()) {
            path.push(g);
            at = specs[g].parent;
        }
        path.reverse();
        path
    };
    let mut paths = users.iter().map(|&u| path(u));
    let mut common = paths.next()?;
    for other in paths {
        let same = common
            .iter()
            .zip(&other)
            .take_while(|(a, b)| a == b)
            .count();
        common.truncate(same);
    }
    common.last().copied()
}

/// The users no other user comes before: [`last_users`] on the lines reversed.
fn first_users(edges: &[Joined], users: &[usize]) -> Vec<usize> {
    let reversed: Vec<Joined> = edges
        .iter()
        .map(|(f, t, kind, on)| (*t, *f, kind.clone(), on.clone()))
        .collect();
    last_users(&reversed, users)
}

/// The task groups' boxes as drawn, labelled with their last segment.
fn group_frames(boxes: &[graph::GroupBox], group_ids: &[String]) -> Vec<GroupFrame> {
    boxes
        .iter()
        .map(|b| {
            let id = &group_ids[b.group];
            GroupFrame {
                left: b.left,
                top: b.top,
                width: b.width,
                height: b.height,
                label: fitted(id.rsplit('.').next().unwrap_or(id), 28),
                group_id: id.clone(),
                options: false,
            }
        })
        .collect()
}

/// The users no other user follows: one search from each user over the lines (`O(users ×
/// lines)`), each line read once per search.
fn last_users(edges: &[Joined], users: &[usize]) -> Vec<usize> {
    let size = edges
        .iter()
        .map(|(f, t, _, _)| f.max(t) + 1)
        .max()
        .unwrap_or(0);
    let mut next: Vec<Vec<usize>> = vec![Vec::new(); size];
    for (f, t, _, _) in edges {
        next[*f].push(*t);
    }
    let is_user: Vec<bool> = (0..size).map(|i| users.contains(&i)).collect();
    users
        .iter()
        .copied()
        .filter(|&user| {
            let mut seen = vec![false; size];
            let mut stack = next.get(user).cloned().unwrap_or_default();
            while let Some(at) = stack.pop() {
                if std::mem::replace(&mut seen[at], true) {
                    continue;
                }
                if at != user && is_user[at] {
                    return false;
                }
                stack.extend(&next[at]);
            }
            true
        })
        .collect()
}

/// A node's cap, the band across its rounded side, and its resource, for the task at `at` in
/// `ids`, placed at `(left, top)`: none for a task that is neither a setup nor a cleanup.
fn cap_of(
    resources: &[GraphResource],
    ids: &[String],
    at: Option<usize>,
    left: f64,
    top: f64,
) -> (String, (f64, f64, f64), String) {
    let (from, to) = (left + CAP_RADIUS, left + NODE_WIDTH - CAP_RADIUS);
    if let Some(r) = resources.iter().find(|r| Some(r.setup) == at) {
        return (
            cap_path(left, top, true),
            (from, to, top + 1.5),
            ids[r.setup].clone(),
        );
    }
    if let Some(r) = resources.iter().find(|r| Some(r.cleanup) == at) {
        let band = (from, to, top + NODE_HEIGHT - 1.5);
        return (cap_path(left, top, false), band, ids[r.setup].clone());
    }
    (String::new(), (0.0, 0.0, 0.0), String::new())
}

/// The resources' rails: from each setup's left side out into the gutter, down, and into its
/// cleanup's left side.
fn rails_of(resources: &[GraphResource], ids: &[String], nodes: &[graph::Placed]) -> Vec<Rail> {
    resources
        .iter()
        .filter_map(|r| {
            let (setup, cleanup) = (nodes.get(r.setup)?, nodes.get(r.cleanup)?);
            let gutter = setup.x.min(cleanup.x) - NODE_WIDTH / 2.0 - RAIL_GUTTER;
            let (from, to) = (setup.x - NODE_WIDTH / 2.0, cleanup.x - NODE_WIDTH / 2.0);
            Some(Rail {
                resource: ids[r.setup].clone(),
                path: format!(
                    "M{from:.1},{:.1} H{gutter:.1} V{:.1} H{to:.1}",
                    setup.y, cleanup.y
                ),
            })
        })
        .collect()
}

/// How far left of its tasks a resource's rail runs (within the graph's margin).
const RAIL_GUTTER: f64 = 8.0;

/// The radius of a cap's rounded side.
const CAP_RADIUS: f64 = 14.0;

/// A cap's outline at `(left, top)`: rounded on top (a setup) or at the bottom (a cleanup).
fn cap_path(left: f64, top: f64, setup: bool) -> String {
    let (w, h, r, k) = (NODE_WIDTH, NODE_HEIGHT, CAP_RADIUS, 3.0);
    let (right, bottom) = (left + w, top + h);
    // The other side's corners stay slightly rounded, like the other boxes.
    if setup {
        format!(
            "M{left:.1},{:.1} A{r},{r} 0 0 1 {:.1},{top:.1} H{:.1} A{r},{r} 0 0 1 {right:.1},{:.1} \
             V{:.1} A{k},{k} 0 0 1 {:.1},{bottom:.1} H{:.1} A{k},{k} 0 0 1 {left:.1},{:.1} Z",
            top + r,
            left + r,
            right - r,
            top + r,
            bottom - k,
            right - k,
            left + k,
            bottom - k,
        )
    } else {
        format!(
            "M{left:.1},{:.1} A{k},{k} 0 0 1 {:.1},{top:.1} H{:.1} A{k},{k} 0 0 1 {right:.1},{:.1} \
             V{:.1} A{r},{r} 0 0 1 {:.1},{bottom:.1} H{:.1} A{r},{r} 0 0 1 {left:.1},{:.1} Z",
            top + k,
            left + k,
            right - k,
            top + k,
            bottom - r,
            right - r,
            left + r,
            bottom - r,
        )
    }
}

/// A resource on the graph: its setup, cleanup and users by their position among the tasks.
#[derive(Debug)]
struct GraphResource {
    setup: usize,
    cleanup: usize,
    users: Vec<usize>,
    blocking: bool,
}

/// The DAG's resources whose setup and cleanup it has (`index` gives a task ID's position).
fn resources_of(
    dag: &serde_json::Value,
    index: impl Fn(&str) -> Option<usize>,
) -> Vec<GraphResource> {
    dag["resources"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            Some(GraphResource {
                setup: index(r["setup"].as_str()?)?,
                cleanup: index(r["cleanup"].as_str()?)?,
                users: r["users"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|u| index(u.as_str()?))
                    .collect(),
                blocking: r["blocking"].as_bool().unwrap_or(true),
            })
        })
        .collect()
}

/// The lines around resources as the graph draws them: the users' requirement on the cleanup,
/// from the last users only (decision 262), instead of the setup's own link to it; with
/// `blocking`, what follows the group drawn from the cleanup alone, the users' edges to it left
/// out (decision 272); without, the link to the cleanup dashed (`released`).
fn resource_edges(edges: &mut Vec<Joined>, resources: &[GraphResource]) {
    for resource in resources {
        let (setup, cleanup) = (resource.setup, resource.cleanup);
        let kind = if resource.blocking {
            "requirement"
        } else {
            "requirement released"
        };
        if resource.users.is_empty() {
            // The setup's own link is the cleanup's only one: dashed when nothing waits for it.
            for edge in edges.iter_mut() {
                if (edge.0, edge.1) == (setup, cleanup) && edge.2 == "requirement" {
                    kind.clone_into(&mut edge.2);
                }
            }
            continue;
        }
        let last = last_users(edges, &resource.users);
        edges.retain(|(f, t, kind, _)| !((*f, *t) == (setup, cleanup) && kind != "data"));
        if resource.blocking {
            let held: Vec<usize> = edges
                .iter()
                .filter(|(f, _, _, _)| *f == cleanup)
                .map(|(_, t, _, _)| *t)
                .collect();
            // A data edge stays: it says where the input comes from, not only the order.
            edges.retain(|(f, t, kind, _)| {
                kind == "data" || !(resource.users.contains(f) && held.contains(t))
            });
        }
        for user in last {
            if !edges.iter().any(|(f, t, _, _)| (*f, *t) == (user, cleanup)) {
                edges.push((user, cleanup, kind.to_owned(), None));
            }
        }
    }
}

/// How far a mapped task's `n` instances are: `2/3 done`, with how many failed.
fn counts(task_id: &str, n: usize, tasks: &[TaskView]) -> String {
    let instances = || {
        tasks
            .iter()
            .filter(move |t| t.task_id == task_id && t.map_index >= 0)
    };
    let finished = |s: &str| {
        matches!(
            s,
            "success" | "failed" | "cancelled" | "skipped" | "upstream_failed"
        )
    };
    let done = instances().filter(|t| finished(&t.state)).count();
    let failed = instances()
        .filter(|t| matches!(t.state.as_str(), "failed" | "upstream_failed"))
        .count();
    let mut line = format!("{done}/{n} done");
    if failed > 0 {
        let _ = write!(line, " · {failed} failed");
    }
    line
}

/// An instance's row in a mapped task's panel.
#[derive(Debug, Clone)]
pub struct InstanceRow {
    /// Its `map_index`.
    pub map_index: i32,
    /// Its state (also its CSS class).
    pub state: String,
    /// `state · outcome`.
    pub status: String,
    /// Attempts made.
    pub tries: u32,
    /// Its panel (htmx).
    pub panel: String,
    /// The run page with its panel open.
    pub href: String,
}

/// The DAG's groups (decision 64): their IDs, their nesting, and each task's innermost group
/// (`ids` is the tasks' order). A group or parent the DAG doesn't declare is left out.
fn groups_of(
    dag: &serde_json::Value,
    ids: &[String],
) -> (Vec<String>, Vec<graph::GroupSpec>, Vec<Option<usize>>) {
    let declared: Vec<(String, Option<String>)> = dag["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|g| {
            let id = g["id"].as_str()?.to_owned();
            Some((id, g["parent"].as_str().map(str::to_owned)))
        })
        .collect();
    let index = |id: &str| declared.iter().position(|(g, _)| g == id);
    let specs = declared
        .iter()
        .map(|(_, parent)| graph::GroupSpec {
            parent: parent.as_deref().and_then(index),
        })
        .collect();
    let tasks = dag["tasks"].as_array().cloned().unwrap_or_default();
    let node_group = ids
        .iter()
        .map(|id| {
            tasks
                .iter()
                .find(|t| t["id"].as_str() == Some(id))
                .and_then(|t| t["group"].as_str())
                .and_then(index)
        })
        .collect();
    (
        declared.into_iter().map(|(g, _)| g).collect(),
        specs,
        node_group,
    )
}

/// A line of the graph: the pair's upstream and downstream tasks (by index), `data` or
/// `order`, and the outcomes it is taken on (`None`: any).
type Joined = (usize, usize, String, Option<Vec<String>>);

/// The DAG's edges, one per pair of tasks (`index` gives a task ID's position).
fn joined_edges(dag: &serde_json::Value, index: impl Fn(&str) -> Option<usize>) -> Vec<Joined> {
    // One line per pair of tasks: a pair joined both ways (data and order) is drawn as data,
    // which already orders them; its outcomes those of all its edges (none: any outcome, as
    // soon as one edge takes any).
    let mut edges: Vec<Joined> = Vec::new();
    for edge in dag["edges"].as_array().into_iter().flatten() {
        let (Some(from), Some(to)) = (
            edge["from"].as_str().and_then(&index),
            edge["to"].as_str().and_then(&index),
        ) else {
            continue;
        };
        let kind = edge["kind"].as_str().unwrap_or("data").to_owned();
        let on: Vec<String> = edge["on"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|o| o.as_str().map(str::to_owned))
            .collect();
        let on = (!on.is_empty()).then_some(on);
        match edges.iter_mut().find(|(f, t, _, _)| (*f, *t) == (from, to)) {
            Some(known) => {
                if kind == "data" {
                    known.2 = kind;
                }
                known.3 = match (known.3.take(), on) {
                    (Some(mut all), Some(more)) => {
                        all.extend(
                            more.into_iter()
                                .filter(|o| !all.contains(o))
                                .collect::<Vec<_>>(),
                        );
                        Some(all)
                    }
                    _ => None,
                };
            }
            None => edges.push((from, to, kind, on)),
        }
    }
    edges
}

/// Whether `edge` wasn't taken: its upstream task ended with an outcome it isn't taken on or
/// was skipped, or it is a selection edge to a target its branch didn't choose (`chose`, when
/// known).
fn not_taken(
    edge: &Joined,
    upstream: Option<&TaskView>,
    chose: Option<&Vec<String>>,
    to: &str,
) -> bool {
    let (_, _, kind, on) = edge;
    let skipped = upstream.is_some_and(|t| t.state == "skipped");
    if kind == "selection" {
        return skipped || chose.is_some_and(|targets| !targets.iter().any(|t| t == to));
    }
    on.as_ref().is_some_and(|on| {
        skipped
            || upstream.is_some_and(|t| {
                t.state == "success"
                    && !on.contains(t.outcome.as_ref().unwrap_or(&"SUCCESS".to_owned()))
            })
    })
}

/// An SVG path through `points`: straight down from each point, then a curve to the next.
fn path_of(points: &[(f64, f64)]) -> String {
    let mut path = String::new();
    for (i, (x, y)) in points.iter().enumerate() {
        if i == 0 {
            let _ = write!(path, "M{x:.1},{y:.1}");
        } else {
            let (px, py) = points[i - 1];
            let mid = f64::midpoint(py, *y);
            let _ = write!(path, " C{px:.1},{mid:.1} {x:.1},{mid:.1} {x:.1},{y:.1}");
        }
    }
    path
}

/// `text` cut to `chars` characters, an ellipsis marking the cut.
fn fitted(text: &str, chars: usize) -> String {
    if text.chars().count() <= chars {
        text.to_owned()
    } else {
        let mut cut: String = text.chars().take(chars.saturating_sub(1)).collect();
        cut.push('…');
        cut
    }
}

/// A tab of the panel.
#[derive(Debug, Clone)]
pub struct Tab {
    /// Its name in URLs.
    pub key: &'static str,
    /// Its label.
    pub label: &'static str,
    /// The panel with it open (htmx).
    pub panel: String,
    /// The run page with it open.
    pub href: String,
    /// Whether it is the one shown.
    pub open: bool,
}

/// An attempt's row in the panel.
#[derive(Debug, Clone)]
pub struct AttemptRow {
    /// Its try number.
    pub try_number: u32,
    /// How it ended (also its CSS class); `running` while it runs.
    pub status: String,
    /// The worker it ran on.
    pub worker: String,
    /// `spawn` or `fork`.
    pub start_method: String,
    /// How long it ran.
    pub duration: String,
    /// Its peak memory.
    pub memory: String,
    /// When it started, as people read it.
    pub started_at: String,
    /// The error kind schedr recognised, and the exception's message.
    pub error: String,
    /// The traceback, when there is one.
    pub traceback: String,
}

/// A try in the log tab's picker.
#[derive(Debug, Clone)]
pub struct TryLink {
    /// Its number.
    pub number: u32,
    /// The panel with its log (htmx).
    pub panel: String,
    /// The run page with its log.
    pub href: String,
    /// Whether its log is shown.
    pub open: bool,
}

/// The panel's content for its open tab.
#[derive(Debug, Clone)]
pub enum TabBody {
    /// The attempts, oldest first.
    Attempts(Vec<AttemptRow>),
    /// The output: its outcome, its (end of the) pretty JSON, whether it was cut, its size,
    /// and its download.
    Output {
        /// The outcome.
        outcome: String,
        /// The pretty JSON, or its last [`OUTPUT_TAIL`] bytes.
        text: String,
        /// Whether `text` is only the end.
        cut: bool,
        /// The pretty JSON's size.
        size: String,
        /// The whole value's download.
        download: String,
    },
    /// No output (yet).
    NoOutput,
    /// A try's log: its end as HTML, whether it was cut, the tries to pick from.
    Log {
        /// The log's (end) as HTML.
        html: String,
        /// Whether only its end is shown.
        cut: bool,
        /// Its size.
        size: String,
        /// Whether the attempt still runs.
        running: bool,
        /// The tries.
        tries: Vec<TryLink>,
        /// While it runs: its tail, where the log continues (23A).
        tail: Option<String>,
    },
    /// Why there is no log to show.
    NoLog(String),
    /// A mapped task's instances, in order (decision 83).
    Instances(Vec<InstanceRow>),
    /// The cleanup: what ran after each attempt, and the resource the task sets up or
    /// cleans up.
    Cleanup {
        /// The attempts' cleanups, oldest first: only attempts that have one.
        rows: Vec<CleanupRow>,
        /// The resource the task belongs to: what it is, and the other part's task.
        resource: Option<(String, ResourcePart)>,
    },
}

/// An attempt's cleanup in the panel.
#[derive(Debug, Clone)]
pub struct CleanupRow {
    /// The attempt's try number.
    pub try_number: u32,
    /// How it ended (also its CSS class): `ok`, `failed`, `worker_lost`, `not_needed`, or
    /// `pending` until it has.
    pub result: String,
    /// The tries it made, and the moves to another worker: `2 tries · moved once`.
    pub tries: String,
    /// How long its tries ran.
    pub duration: String,
    /// The error of a failed cleanup.
    pub error: String,
    /// The traceback, when there is one.
    pub traceback: String,
    /// The handles the attempt registered: their type and value (masked).
    pub handles: Vec<(String, String)>,
}

#[derive(Template)]
#[template(path = "task_panel.html")]
struct PanelFragment {
    /// Where the panel is read again from when its task changes; empty on the log tab (the log
    /// streams instead).
    refresh: String,
    /// The task, or its instance (`fetch[1]`).
    task_id: String,
    /// The graph's node for it: the task (its instances share their task's node).
    node: String,
    status: String,
    state: String,
    close: String,
    /// The pages' prefix (the dialogs' links).
    base: String,
    /// Where the task's actions are posted (`.../cancel`, `.../retry`).
    actions: String,
    /// The query after an action's URL: `?map_index=N` for an instance, empty otherwise.
    action_query: String,
    /// For an instance: the mapped task's panel, its instances listed (htmx); empty otherwise.
    back: String,
    /// See `back`: the run page with it open.
    back_href: String,
    /// Whether it can be cancelled: it hasn't ended.
    cancellable: bool,
    /// What its Cancel dialog says cancelling does (a resource's cleanup warns of a leftover).
    cancel_note: String,
    /// Whether it can be retried: every eligible worker rejected it.
    retryable: bool,
    tabs: Vec<Tab>,
    body: TabBody,
}

/// What cancelling the task does, for its Cancel dialog: a resource's cleanup stops freeing
/// what its setup made (decision 279).
fn cancel_note(resources: &[ResourceView], task_id: &str) -> String {
    match resources.iter().find(|r| r.cleanup == task_id) {
        Some(resource) => format!(
            "It stops freeing resource {}: what its setup made may be left behind{}. The run \
             ends cancelled, or failed if a task of it failed.",
            resource.setup,
            if resource.blocking {
                ", and what follows its group is cancelled"
            } else {
                ""
            }
        ),
        None => "Stopped if it runs, cancelled at once if it hasn't started; the tasks \
                 downstream of it are cancelled."
            .to_owned(),
    }
}

/// `GET /d/<deployment>/runs/<run>/tasks/<task>`: a task's panel alone, for htmx; a browser
/// asking for it as a page gets the run page with the panel open.
pub async fn panel<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id, task_id)): Path<(String, String, String)>,
    query: Result<Query<RunQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    // Asked for by htmx, every answer is a fragment for the panel, errors too.
    let from_htmx = headers.contains_key("hx-request");
    let failed = |failure: Failure| {
        if from_htmx {
            failure.fragment()
        } else {
            failure.page(&page)
        }
    };
    let query = match query {
        Ok(Query(query)) => query,
        Err(rejection) => return failed(Failure::bad_request(rejection.body_text())),
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return failed(Failure::missing(format!(
            "There is no deployment `{deployment}` here."
        )));
    };
    let run_href = format!(
        "{}/d/{}/runs/{}",
        page.base,
        path_segment(&deployment.name),
        path_segment(&run_id)
    );
    if !from_htmx {
        let mut to = format!("{run_href}?task={}", path_segment(&task_id));
        if let Some(tab) = &query.tab {
            let _ = write!(to, "&tab={}", path_segment(tab));
        }
        if let Some(index) = query.map_index {
            let _ = write!(to, "&map_index={index}");
        }
        return axum::response::Redirect::to(&to).into_response();
    }
    match task_panel(
        &page,
        deployment,
        &run_id,
        &run_href,
        &task_id,
        &query,
        ui.timings.tail_interval,
    )
    .await
    {
        Ok(html) => (
            [(header::CACHE_CONTROL, "no-store")],
            axum::response::Html(html),
        )
            .into_response(),
        Err(failure) => failure.fragment(),
    }
}

/// Why a panel can't be shown: as a page (the run page asked for), or as a banner in the panel
/// (htmx asked for the panel alone; it swaps error answers in too, see `base.html`).
#[derive(Debug)]
struct Failure {
    status: StatusCode,
    title: &'static str,
    message: String,
}

impl Failure {
    fn missing(message: String) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            title: "Not found",
            message,
        }
    }

    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            title: "Bad request",
            message,
        }
    }

    fn unavailable(deployment: &str, error: &ClientError) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            title: "Deployment unavailable",
            message: format!("{deployment}: {error}"),
        }
    }

    fn page(&self, page: &Page) -> Response {
        crate::pages::error_page(page, self.title, &self.message, self.status)
    }

    fn fragment(&self) -> Response {
        let html = format!(
            "<div class=\"banner warning\"><strong>{}</strong>: {}</div>",
            self.title,
            ansi::to_html(&self.message)
        );
        (self.status, axum::response::Html(html)).into_response()
    }
}

/// How a task's state looks; without its DAG (a webserver that can't say) as any task's does.
async fn looks_in_panel<T: Transport>(client: &Client<T>, run_id: &str, task: &TaskView) -> String {
    client
        .run_dag(run_id)
        .await
        .map_or_else(|_| WaitSteps::default(), |dag| WaitSteps::of(&dag))
        .look(task)
}

/// The panel's HTML for `task_id`, or why it can't be shown.
async fn task_panel<T: Transport>(
    page: &Page,
    deployment: &Deployment<T>,
    run_id: &str,
    run_href: &str,
    task_id: &str,
    query: &RunQuery,
    tail_interval: std::time::Duration,
) -> Result<String, Failure> {
    let client = &deployment.client;
    let missing = || Failure::missing(format!("There is no task `{task_id}` in run `{run_id}`."));
    let detail = client.run(run_id).await.map_err(|error| match error {
        ClientError::Server {
            status: 404 | 400, ..
        } => missing(),
        error => Failure::unavailable(&deployment.name, &error),
    })?;
    let (task, mapped, map_index) = panel_task(&detail.tasks, run_id, task_id, query)?;
    let tabs_shown: &[(&str, &str)] = if mapped && map_index.is_none() {
        &MAPPED_TABS
    } else {
        &TABS
    };
    let tab = query
        .tab
        .as_deref()
        .filter(|t| tabs_shown.iter().any(|(key, _)| key == t))
        .unwrap_or(tabs_shown[0].0);
    let segment = path_segment(task_id);
    let panel_href = format!("{run_href}/tasks/{segment}");
    let instance = map_index.map_or_else(String::new, |i| format!("&map_index={i}"));
    let at = Instance {
        task_id,
        map_index,
        panel_href: &panel_href,
        run_href,
    };
    let body = match tab {
        "instances" => TabBody::Instances(instance_rows(&detail.tasks, &at)),
        "output" => output_tab(deployment, run_id, &at).await?,
        "logs" => log_tab(deployment, run_id, &at, query, tail_interval).await?,
        "cleanup" => {
            let attempts = client
                .instance_attempts(run_id, task_id, map_index)
                .await
                .map_err(|error| Failure::unavailable(&deployment.name, &error))?;
            TabBody::Cleanup {
                rows: attempts.attempts.iter().filter_map(cleanup_row).collect(),
                resource: resource_of(&detail.resources, &detail.tasks, task_id, run_href),
            }
        }
        _ => {
            let attempts = client
                .instance_attempts(run_id, task_id, map_index)
                .await
                .map_err(|error| Failure::unavailable(&deployment.name, &error))?;
            TabBody::Attempts(
                attempts
                    .attempts
                    .iter()
                    .map(|a| attempt_row(page, a))
                    .collect(),
            )
        }
    };
    let tabs = tabs_shown
        .iter()
        .map(|(key, label)| Tab {
            key,
            label,
            panel: format!("{panel_href}?tab={key}{instance}"),
            href: format!("{run_href}?task={segment}&tab={key}{instance}"),
            open: *key == tab,
        })
        .collect();
    let refresh = match (tab, query.try_number) {
        ("logs", _) => String::new(),
        (tab, _) => format!("{panel_href}?tab={tab}{instance}"),
    };
    let fragment = PanelFragment {
        refresh,
        task_id: map_index.map_or_else(|| task_id.to_owned(), |i| format!("{task_id}[{i}]")),
        node: task_id.to_owned(),
        status: status(task),
        state: looks_in_panel(client, run_id, task).await,
        close: run_href.to_owned(),
        base: page.base.clone(),
        action_query: map_index.map_or_else(String::new, |i| format!("?map_index={i}")),
        back: map_index.map_or_else(String::new, |_| format!("{panel_href}?tab=instances")),
        back_href: map_index.map_or_else(String::new, |_| {
            format!("{run_href}?task={segment}&tab=instances")
        }),
        actions: panel_href.clone(),
        cancellable: !matches!(
            task.state.as_str(),
            "success" | "failed" | "cancelled" | "skipped" | "upstream_failed"
        ),
        retryable: task.state == "unschedulable",
        cancel_note: cancel_note(&detail.resources, task_id),
        tabs,
        body,
    };
    fragment.render().map_err(|error| Failure {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        title: "Internal error",
        message: format!("the panel couldn't be rendered: {error}"),
    })
}

/// The task (or the instance of a mapped one asked for) whose panel is shown: it, whether
/// the task is mapped, and the instance's `map_index`.
fn panel_task<'a>(
    tasks: &'a [TaskView],
    run_id: &str,
    task_id: &str,
    query: &RunQuery,
) -> Result<(&'a TaskView, bool, Option<u32>), Failure> {
    let whole = tasks
        .iter()
        .find(|t| t.task_id == task_id && t.map_index == -1)
        .ok_or_else(|| {
            Failure::missing(format!("There is no task `{task_id}` in run `{run_id}`."))
        })?;
    let mapped = whole.instances.is_some();
    let map_index = query.map_index.filter(|_| mapped);
    let task = match map_index {
        Some(index) => tasks
            .iter()
            .find(|t| t.task_id == task_id && i64::from(t.map_index) == i64::from(index))
            .ok_or_else(|| {
                Failure::missing(format!(
                    "Task `{task_id}` of run `{run_id}` has no instance {index}."
                ))
            })?,
        None => whole,
    };
    Ok((task, mapped, map_index))
}

/// A mapped task's instances, as its panel lists them (decision 83).
fn instance_rows(tasks: &[TaskView], at: &Instance<'_>) -> Vec<InstanceRow> {
    let segment = path_segment(at.task_id);
    tasks
        .iter()
        .filter(|t| t.task_id == at.task_id && t.map_index >= 0)
        .map(|t| InstanceRow {
            map_index: t.map_index,
            state: t.state.clone(),
            status: status(t),
            tries: t.try_number,
            panel: format!("{}?map_index={}", at.panel_href, t.map_index),
            href: format!("{}?task={segment}&map_index={}", at.run_href, t.map_index),
        })
        .collect()
}

/// Which task (or instance of one) a panel tab is about, and where its links go.
struct Instance<'a> {
    task_id: &'a str,
    map_index: Option<u32>,
    panel_href: &'a str,
    run_href: &'a str,
}

impl Instance<'_> {
    /// The query naming the instance, after others (`&map_index=N`); empty for none.
    fn query(&self) -> String {
        self.map_index
            .map_or_else(String::new, |i| format!("&map_index={i}"))
    }
}

async fn output_tab<T: Transport>(
    deployment: &Deployment<T>,
    run_id: &str,
    at: &Instance<'_>,
) -> Result<TabBody, Failure> {
    let (task_id, panel_href) = (at.task_id, at.panel_href);
    let output = deployment
        .client
        .instance_output(run_id, task_id, at.map_index)
        .await;
    match output {
        Ok(output) => {
            let pretty = serde_json::to_string_pretty(&output.value).unwrap_or_default();
            let (text, cut) = tail(&pretty, OUTPUT_TAIL);
            Ok(TabBody::Output {
                outcome: output.outcome,
                text: text.to_owned(),
                cut,
                size: bytes(pretty.len() as u64),
                download: format!(
                    "{panel_href}/output.json{}",
                    at.map_index
                        .map_or_else(String::new, |i| format!("?map_index={i}"))
                ),
            })
        }
        Err(ClientError::Server { status: 404, .. }) => Ok(TabBody::NoOutput),
        Err(error) => Err(Failure::unavailable(&deployment.name, &error)),
    }
}

async fn log_tab<T: Transport>(
    deployment: &Deployment<T>,
    run_id: &str,
    at: &Instance<'_>,
    query: &RunQuery,
    tail_interval: std::time::Duration,
) -> Result<TabBody, Failure> {
    let (task_id, panel_href, run_href) = (at.task_id, at.panel_href, at.run_href);
    let instance = at.query();
    let client = &deployment.client;
    let attempts = client
        .instance_attempts(run_id, task_id, at.map_index)
        .await
        .map_err(|error| Failure::unavailable(&deployment.name, &error))?
        .attempts;
    let Some(last) = attempts.last().map(|a| a.try_number) else {
        return Ok(TabBody::NoLog("It hasn't been tried yet.".to_owned()));
    };
    let shown = query
        .try_number
        .filter(|n| attempts.iter().any(|a| a.try_number == *n))
        .unwrap_or(last);
    let segment = path_segment(task_id);
    let tries = attempts
        .iter()
        .map(|a| TryLink {
            number: a.try_number,
            panel: format!("{panel_href}?tab=logs&try={}{instance}", a.try_number),
            href: format!(
                "{run_href}?task={segment}&tab=logs&try={}{instance}",
                a.try_number
            ),
            open: a.try_number == shown,
        })
        .collect();
    // The size first (the API's smallest part, 4 bytes), then the end.
    let head = match client
        .instance_log_part(run_id, (task_id, at.map_index), shown, 0, 4)
        .await
    {
        Ok(head) => head,
        Err(ClientError::Server { message, .. }) => return Ok(TabBody::NoLog(message)),
        Err(error) => return Err(Failure::unavailable(&deployment.name, &error)),
    };
    // The last part the API gives (whole lines, so a line up to it is shown), kept to its
    // last LOG_TAIL from a line's start, or its last line when that one is longer.
    let from = head.size.saturating_sub(MAX_CHUNK);
    let part = match client
        .instance_log_part(run_id, (task_id, at.map_index), shown, from, MAX_CHUNK)
        .await
    {
        Ok(part) => part,
        Err(ClientError::Server { message, .. }) => return Ok(TabBody::NoLog(message)),
        Err(error) => return Err(Failure::unavailable(&deployment.name, &error)),
    };
    // Cut or not, it starts at a line: the API starts a part after a cut line's end.
    let (text, trimmed) = log_end(&part.content);
    // Nothing from inside the log: its last line began before the last 1 MiB (where, the
    // panel can't know; the API shows no line from inside it).
    // The style in force at its end carries into the followed part.
    let mut colours = ansi::Converter::default();
    let html = if text.is_empty() && part.from > 0 && part.next >= part.size {
        LONG_LAST_LINE.to_owned()
    } else {
        colours.push(text)
    };
    let style = match colours.sgr() {
        sgr if sgr.is_empty() => String::new(),
        sgr => format!("&style={}", sgr.replace(';', "%3B")),
    };
    Ok(TabBody::Log {
        html,
        cut: part.from > 0 || trimmed,
        size: bytes(part.size),
        running: part.running,
        tries,
        // Followed while it runs, and to its end when it grew past this part and ended
        // between the two reads (B146).
        tail: (part.running || part.next < part.size).then(|| {
            crate::live::tail_reader(
                &format!(
                    "{panel_href}/tail?try={shown}&from={}{instance}{style}",
                    part.next
                ),
                false,
                tail_interval,
            )
        }),
    })
}

/// `GET /d/<deployment>/runs/<run>/tasks/<task>/output.json`: the whole output, to download.
pub async fn output_download<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, run_id, task_id)): Path<(String, String, String)>,
    query: Result<Query<RunQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let map_index = match query {
        Ok(Query(query)) => query.map_index,
        Err(rejection) => return bad_request_page(&page, &rejection.body_text()),
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return no_deployment(&page, &deployment);
    };
    let output = deployment
        .client
        .instance_output(&run_id, &task_id, map_index)
        .await;
    match output {
        Ok(output) => {
            let body = serde_json::to_string_pretty(&output.value).unwrap_or_default();
            let name: String = task_id
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || "-_.".contains(c) {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            (
                [
                    (header::CONTENT_TYPE, "application/json".to_owned()),
                    (
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{name}.json\""),
                    ),
                ],
                body,
            )
                .into_response()
        }
        Err(ClientError::Server {
            status: 404 | 400, ..
        }) => not_found_page(
            &page,
            &format!("`{task_id}` has no output in run `{run_id}`."),
        ),
        Err(error) => unavailable_page(&page, &deployment.name, &error),
    }
}

fn attempt_row(page: &Page, attempt: &AttemptView) -> AttemptRow {
    let summary = attempt.error_summary.as_ref();
    let exception = summary
        .and_then(|s| {
            let kind = s["exception"].as_str().or_else(|| s["type"].as_str())?;
            let message = s["message"].as_str().unwrap_or_default();
            Some(if message.is_empty() {
                kind.to_owned()
            } else {
                format!("{kind}: {message}")
            })
        })
        .unwrap_or_default();
    let error = match (attempt.error_kind.as_deref(), exception.is_empty()) {
        (Some(kind), true) => kind.to_owned(),
        (Some(kind), false) => format!("{kind} · {exception}"),
        (None, _) => exception,
    };
    AttemptRow {
        try_number: attempt.try_number,
        status: attempt
            .status
            .clone()
            .unwrap_or_else(|| "running".to_owned()),
        worker: attempt.worker_id.clone().unwrap_or_default(),
        start_method: attempt.start_method.clone().unwrap_or_default(),
        duration: attempt.duration_ms.map(duration).unwrap_or_default(),
        memory: attempt.peak_memory_bytes.map(bytes).unwrap_or_default(),
        started_at: attempt
            .started_at
            .map(|t| page.narrow_time(&t))
            .unwrap_or_default(),
        // Masked again here (an older webserver didn't): a message may quote an address.
        error: schedr_config::masked::mask_text(&error),
        traceback: schedr_config::masked::mask_text(
            summary
                .and_then(|s| s["traceback"].as_str())
                .unwrap_or_default(),
        ),
    }
}

/// What a task is to a resource: `sets up` or `cleans up`, with the resource's other part.
fn resource_of(
    resources: &[ResourceView],
    tasks: &[TaskView],
    task_id: &str,
    run_href: &str,
) -> Option<(String, ResourcePart)> {
    resources.iter().find_map(|r| {
        let row = || resource_row(run_href, r, tasks);
        if r.setup == task_id {
            Some((
                format!("It sets up resource {}; its cleanup is", r.setup),
                row().cleanup,
            ))
        } else if r.cleanup == task_id {
            Some((
                format!("It cleans up resource {}, set up by", r.setup),
                row().setup,
            ))
        } else {
            None
        }
    })
}

/// An attempt's cleanup row; none for an attempt without one.
fn cleanup_row(attempt: &AttemptView) -> Option<CleanupRow> {
    let cleanup: &CleanupView = attempt.cleanup.as_ref()?;
    let mut tries = match cleanup.tries {
        Some(1) => "1 try".to_owned(),
        Some(n) => format!("{n} tries"),
        None => String::new(),
    };
    if cleanup.moves > 0 {
        let moved = match cleanup.moves {
            1 => "moved once".to_owned(),
            n => format!("moved {n} times"),
        };
        tries = if tries.is_empty() {
            moved
        } else {
            format!("{tries} · {moved}")
        };
    }
    let summary = cleanup.error_summary.as_ref();
    let exception = summary
        .and_then(|s| {
            let kind = s["exception"].as_str()?;
            let message = s["message"].as_str().unwrap_or_default();
            Some(if message.is_empty() {
                kind.to_owned()
            } else {
                format!("{kind}: {message}")
            })
        })
        .unwrap_or_default();
    let error = match (cleanup.error_kind.as_deref(), exception.is_empty()) {
        (Some(kind), true) => kind.to_owned(),
        (Some(kind), false) => format!("{kind} · {exception}"),
        (None, _) => exception,
    };
    Some(CleanupRow {
        try_number: attempt.try_number,
        result: cleanup
            .result
            .clone()
            .unwrap_or_else(|| "pending".to_owned()),
        tries,
        duration: cleanup.duration_ms.map(duration).unwrap_or_default(),
        // Masked again here, as the attempts' errors.
        error: schedr_config::masked::mask_text(&error),
        traceback: schedr_config::masked::mask_text(
            summary
                .and_then(|s| s["traceback"].as_str())
                .unwrap_or_default(),
        ),
        handles: cleanup
            .handles
            .iter()
            .map(|h| {
                let kind = h["handle_type"].as_str().unwrap_or("?").to_owned();
                let mut value = h["value"].clone();
                schedr_config::masked::mask_data(&mut value);
                (kind, value.to_string())
            })
            .collect(),
    })
}

/// The end of `text`, at most `limit` bytes, from a character boundary; and whether it was
/// cut.
fn tail(text: &str, limit: usize) -> (&str, bool) {
    if text.len() <= limit {
        return (text, false);
    }
    let mut start = text.len() - limit;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    (&text[start..], true)
}

/// `ms` as people read it: `850 ms`, `12.3 s`, `4 min 05 s`, `2 h 03 min`.
fn duration(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms} ms"),
        1_000..60_000 => format!("{}.{} s", ms / 1_000, ms % 1_000 / 100),
        60_000..3_600_000 => format!("{} min {:02} s", ms / 60_000, ms % 60_000 / 1_000),
        _ => format!("{} h {:02} min", ms / 3_600_000, ms % 3_600_000 / 60_000),
    }
}

/// `n` bytes as people read it: `512 B`, `1.5 KiB`, `20.0 MiB`.
fn bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n;
    let mut unit = 0;
    while value >= 1024 * 1024 && unit < UNITS.len() - 1 {
        value /= 1024;
        unit += 1;
    }
    // One decimal, from integers: `value` is in units of 1/1024 of UNITS[unit].
    let tenths = value * 10 / 1024;
    format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
}

fn no_deployment(page: &Page, name: &str) -> Response {
    not_found_page(page, &format!("There is no deployment `{name}` here."))
}

fn bad_request_page(page: &Page, message: &str) -> Response {
    crate::pages::error_page(page, "Bad request", message, StatusCode::BAD_REQUEST)
}

#[cfg(test)]
mod resource_tests {
    use super::*;

    #[test]
    fn the_last_users_are_those_no_other_user_follows_and_many_are_quick() {
        // 0 -> 1 -> 2, and 3 alone: 2 and 3 are last.
        let edge = |f: usize, t: usize| (f, t, "data".to_owned(), None);
        let edges = vec![edge(0, 1), edge(1, 2), edge(5, 3)];
        assert_eq!(last_users(&edges, &[0, 1, 2, 3]), [2, 3]);
        // Thousands of independent users: one search each, not one per pair (Codex's review).
        let n = 3_000;
        let wide: Vec<Joined> = (1..=n).map(|u| edge(0, u)).collect();
        let users: Vec<usize> = (1..=n).collect();
        let started = std::time::Instant::now();
        assert_eq!(last_users(&wide, &users).len(), n);
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }
}
