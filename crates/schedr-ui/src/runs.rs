//! The run pages: the runs of every deployment (one list, 32A), a run with its graph (30A),
//! its tasks and its command history, and a task's panel beside the graph (31A): attempts,
//! output (its last 64 KiB, 33), log, and cleanup (a shell until M4).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use askama::Template;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use jiff::Timestamp;
use schedr_api::views::{AllRunsQuery, AttemptView, CommandView, RunView, TaskView};
use schedr_client::{ClientError, Transport, path_segment};
use serde::Deserialize;

use crate::graph::{self, NODE_HEIGHT, NODE_WIDTH};
use crate::page::Page;
use crate::pages::{not_found_page, render, unavailable_page};
use crate::{Deployment, Ui, ansi};

/// Runs on a page of the list.
pub const PAGE_SIZE: u32 = 50;

/// How much of an output the panel shows: its end.
pub const OUTPUT_TAIL: usize = 64 * 1024;

/// How much of a log the panel shows: its end.
pub const LOG_TAIL: u64 = 256 * 1024;

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
}

/// An edge of the graph.
#[derive(Debug, Clone)]
pub struct EdgeLine {
    /// The SVG path.
    pub path: String,
    /// `data`, `order` or `selection` (its CSS class).
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
    /// Its state.
    pub state: String,
    /// `state · outcome`, the outcome only when it says something.
    pub status: String,
    /// Attempts made.
    pub tries: u32,
    /// Why it waits or can't be scheduled.
    pub reason: String,
    /// When it last changed, as people read it.
    pub updated_at: String,
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
    summary: String,
    code_hashes: Vec<String>,
    graph: Graph,
    tasks: Vec<TaskRow>,
    commands: Vec<CommandRow>,
    /// The open panel's HTML (rendered by [`PanelFragment`]), empty when none is open.
    panel: String,
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
    let run_href = format!(
        "{}/d/{}/runs/{}",
        page.base,
        path_segment(&deployment.name),
        path_segment(&run_id)
    );
    let panel = match &query.task {
        Some(task) => match task_panel(&page, deployment, &run_id, &run_href, task, &query).await {
            Ok(html) => html,
            Err(failure) => return failure.page(&page),
        },
        None => String::new(),
    };
    let (whole, tasks) = task_rows(&page, &run_href, &detail.tasks);
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
            run_href: run_href.clone(),
            code_hashes: detail.code_hashes,
            commands: commands
                .commands
                .iter()
                .map(|c| command_row(&page, c))
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

/// `state · outcome`, the outcome only when it says something (`SUCCESS` doesn't).
fn status(task: &TaskView) -> String {
    match task.outcome.as_deref() {
        Some(outcome) if outcome != "SUCCESS" => format!("{} · {outcome}", task.state),
        _ => task.state.clone(),
    }
}

/// The run's tasks as a whole, and their rows; a mapped task's instances are in its panel
/// (decision 83).
fn task_rows(page: &Page, run_href: &str, tasks: &[TaskView]) -> (Vec<TaskView>, Vec<TaskRow>) {
    let whole: Vec<TaskView> = tasks
        .iter()
        .filter(|t| t.map_index == -1)
        .cloned()
        .collect();
    let rows = whole.iter().map(|t| task_row(page, run_href, t)).collect();
    (whole, rows)
}

fn task_row(page: &Page, run_href: &str, task: &TaskView) -> TaskRow {
    let id = path_segment(&task.task_id);
    TaskRow {
        label: task.instances.map_or_else(
            || task.task_id.clone(),
            |n| format!("{} ×{n}", task.task_id),
        ),
        href: format!("{run_href}?task={id}"),
        panel: format!("{run_href}/tasks/{id}"),
        state: task.state.clone(),
        status: status(task),
        tries: task.try_number,
        reason: task.reason.clone().unwrap_or_default(),
        updated_at: page.time(&task.updated_at),
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
    let chosen = chosen_by_branches(deployment, run_id, dag, tasks).await;
    graph_of(dag, tasks, &chosen, run_href, query.task.as_deref())
}

/// What each `@branch` task that succeeded chose (its stored output, the IDs of its targets;
/// anything else chose nothing, as the master reads it); a branch whose output can't be read
/// is left out, its edges drawn as taken.
async fn chosen_by_branches<T: Transport>(
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
    let branches: Vec<&str> = dag["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| t["branch"].as_bool() == Some(true))
        .filter_map(|t| t["id"].as_str())
        .filter(|id| succeeded(id))
        .collect();
    let mut chosen = BTreeMap::new();
    for id in branches {
        let Ok(output) = deployment.client.output(run_id, id).await else {
            continue;
        };
        // As the master reads it: anything but a list of IDs chose nothing.
        let targets = output.value.as_array().into_iter().flatten();
        let targets = targets.filter_map(|v| v.as_str().map(str::to_owned));
        chosen.insert(id.to_owned(), targets.collect());
    }
    chosen
}

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
    let index = |id: &str| ids.iter().position(|i| i == id);
    let edges = joined_edges(dag, index);
    let pairs: Vec<(usize, usize)> = edges.iter().map(|(f, t, _, _)| (*f, *t)).collect();
    let (group_ids, specs, node_group) = groups_of(dag, &ids);
    let layout = graph::layout_grouped(ids.len(), &pairs, &specs, &node_group);
    let nodes = ids
        .iter()
        .zip(&layout.nodes)
        .map(|(id, placed)| {
            let task = tasks.iter().find(|t| &t.task_id == id && t.map_index == -1);
            let segment = path_segment(id);
            let label = task
                .and_then(|t| t.instances)
                .map_or_else(|| fitted(id, 22), |n| format!("{} ×{n}", fitted(id, 17)));
            Node {
                left: placed.x - NODE_WIDTH / 2.0,
                top: placed.y - NODE_HEIGHT / 2.0,
                task_id: id.clone(),
                label,
                counts: task
                    .and_then(|t| t.instances)
                    .map(|n| counts(id, n, tasks))
                    .unwrap_or_default(),
                state: task.map_or_else(|| "none".to_owned(), |t| t.state.clone()),
                outcome: task
                    .and_then(|t| t.outcome.clone())
                    .filter(|o| o != "SUCCESS")
                    .map(|o| fitted(&o, 24))
                    .unwrap_or_default(),
                href: format!("{run_href}?task={segment}"),
                panel: format!("{run_href}/tasks/{segment}"),
                selected: selected == Some(id.as_str()),
            }
        })
        .collect();
    let lines = layout
        .routes
        .iter()
        .map(|route| {
            let (from, to, kind, on) = &edges[route.edge];
            let upstream = tasks.iter().find(|t| t.task_id == ids[*from]);
            let chose = chosen.get(&ids[*from]);
            let not_taken = not_taken(&edges[route.edge], upstream, chose, &ids[*to]);
            let end = route.points.last().copied().unwrap_or_default();
            EdgeLine {
                path: path_of(&route.points),
                kind: kind.clone(),
                on: on
                    .as_ref()
                    .map(|on| fitted(&on.join(" | "), 28))
                    .unwrap_or_default(),
                label_x: end.0 + 6.0,
                label_y: end.1 - 12.0,
                not_taken,
            }
        })
        .collect();
    let groups = layout
        .boxes
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
            }
        })
        .collect();
    Graph {
        width: layout.width,
        height: layout.height,
        node_width: NODE_WIDTH,
        node_height: NODE_HEIGHT,
        nodes,
        edges: lines,
        groups,
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
    /// The cleanup: nothing until M4's per-task cleanups (a shell, 25B).
    Cleanup,
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
    /// Whether it can be retried: every eligible worker rejected it.
    retryable: bool,
    tabs: Vec<Tab>,
    body: TabBody,
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
    match task_panel(&page, deployment, &run_id, &run_href, &task_id, &query).await {
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

/// The panel's HTML for `task_id`, or why it can't be shown.
async fn task_panel<T: Transport>(
    page: &Page,
    deployment: &Deployment<T>,
    run_id: &str,
    run_href: &str,
    task_id: &str,
    query: &RunQuery,
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
        "logs" => log_tab(deployment, run_id, &at, query).await?,
        "cleanup" => TabBody::Cleanup,
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
        state: task.state.clone(),
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
    let from = head.size.saturating_sub(LOG_TAIL);
    let part = match client
        .instance_log_part(run_id, (task_id, at.map_index), shown, from, LOG_TAIL)
        .await
    {
        Ok(part) => part,
        Err(ClientError::Server { message, .. }) => return Ok(TabBody::NoLog(message)),
        Err(error) => return Err(Failure::unavailable(&deployment.name, &error)),
    };
    let mut text = part.content.as_str();
    if part.from > 0 {
        // Cut: from the first whole line, when there is one after the cut (a line longer than
        // the tail is shown from the cut).
        if let Some((_, rest)) = text.split_once('\n')
            && !rest.is_empty()
        {
            text = rest;
        }
    }
    Ok(TabBody::Log {
        html: ansi::to_html(text),
        cut: part.from > 0,
        size: bytes(part.size),
        running: part.running,
        tries,
        tail: part
            .running
            .then(|| format!("{panel_href}/tail?try={shown}&from={}{instance}", part.next)),
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
        error,
        traceback: summary
            .and_then(|s| s["traceback"].as_str())
            .unwrap_or_default()
            .to_owned(),
    }
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
