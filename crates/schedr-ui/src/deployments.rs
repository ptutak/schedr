//! The Deployments and Config pages (M4.4b, decisions 121-125): the deployments the UI shows,
//! each one's nodes drawn by process with their configurations, and the UI's own side (its
//! configuration, version and connections). Every configuration was masked by the node it
//! belongs to before it left that node's process, and is masked again before it is shown.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use askama::Template;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use schedr_api::views::{DeploymentView, MasterView, WorkerNodeView};
use schedr_client::{ClientError, Transport, path_segment};

use crate::Ui;
use crate::page::Page;
use crate::pages::{not_found_page, render, unavailable_page};

/// A deployment's row on the list.
#[derive(Debug, Clone)]
pub struct DeploymentRow {
    /// Its name.
    pub name: String,
    /// Its page.
    pub href: String,
    /// Its leading master's host (`-` with none heard from).
    pub leader: String,
    /// Its masters heard from lately.
    pub masters: usize,
    /// Its workers active now, of all it has seen: `2 / 3`.
    pub workers: String,
    /// Its webserver's version.
    pub version: String,
    /// Why it couldn't be read (the rest is then empty).
    pub error: Option<String>,
}

#[derive(Template)]
#[template(path = "deployments.html")]
struct DeploymentsPage {
    page: Page,
    rows: Vec<DeploymentRow>,
}

/// Each deployment's nodes (asked at once), in deployment order, each with how long its answer
/// took.
async fn ask_all<T: Transport + Send + Sync + 'static>(
    ui: &Arc<Ui<T>>,
) -> Vec<(Result<DeploymentView, ClientError>, Duration)> {
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        let ui = Arc::clone(ui);
        asked.spawn(schedr_client::for_same_user(async move {
            let started = Instant::now();
            let answer = ui.deployments[index].client.deployment().await;
            (index, answer, started.elapsed())
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _, _)| *index);
    answers
        .into_iter()
        .map(|(_, answer, took)| (answer, took))
        .collect()
}

/// `GET /deployments`: the deployments this UI shows, each asked at once.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let answers = ask_all(&ui).await;
    let rows = ui
        .deployments
        .iter()
        .zip(answers)
        .map(|(deployment, (answer, _))| {
            let href = format!(
                "{}/deployments/{}",
                page.base,
                path_segment(&deployment.name)
            );
            match answer {
                Ok(view) => DeploymentRow {
                    name: deployment.name.clone(),
                    href,
                    leader: view
                        .masters
                        .iter()
                        .find(|m| m.role == "leader")
                        .map_or_else(|| "-".to_owned(), |m| m.node.host.clone()),
                    masters: view.masters.len(),
                    workers: format!(
                        "{} / {}",
                        view.workers.iter().filter(|w| w.state == "active").count(),
                        view.workers.len()
                    ),
                    version: view.webserver.schedr_version,
                    error: None,
                },
                Err(error) => DeploymentRow {
                    name: deployment.name.clone(),
                    href,
                    leader: String::new(),
                    masters: 0,
                    workers: String::new(),
                    version: String::new(),
                    error: Some(error.to_string()),
                },
            }
        })
        .collect();
    render(&DeploymentsPage { page, rows }, StatusCode::OK)
}

/// A node on a deployment's graph.
#[derive(Debug, Clone)]
pub struct NodeBox {
    /// Its anchor (`node-master-<id>`, `node-worker-<id>`): its configuration's panel.
    pub anchor: String,
    /// `master` or `worker`.
    pub kind: &'static str,
    /// Its name on the graph.
    pub label: String,
    /// Its role or state (`leader`, `standby`; a worker's state), also its CSS class.
    pub state: String,
    /// Where it is drawn: its top-left corner.
    pub x: u32,
    /// Where it is drawn: its top.
    pub y: u32,
}

/// A process on a deployment's graph: a rectangle around its nodes.
#[derive(Debug, Clone)]
pub struct ProcessBox {
    /// Its host (and process ID, shortened) as its title.
    pub title: String,
    /// Where it is drawn: its top-left corner.
    pub x: u32,
    /// Where it is drawn: its top.
    pub y: u32,
    /// Its height (the width is [`PROCESS_WIDTH`]).
    pub height: u32,
    /// Its nodes.
    pub nodes: Vec<NodeBox>,
}

/// A node's configuration, shown when its node is clicked.
#[derive(Debug, Clone)]
pub struct NodePanel {
    /// Its anchor (see [`NodeBox::anchor`]).
    pub anchor: String,
    /// Its title: `Master 0190…` (leader), `Worker w1`.
    pub title: String,
    /// What else there is to say: its host, version, master.
    pub facts: Vec<(String, String)>,
    /// Its configuration as JSON, indented; empty when it sent none (an older agent).
    pub config: String,
}

/// A process's width on the graph.
pub const PROCESS_WIDTH: u32 = 220;
/// A node's height on the graph.
pub const NODE_HEIGHT: u32 = 36;
/// The space above a process's first node (its title).
const PROCESS_TITLE: u32 = 28;
/// The space between nodes, and around them in a process.
const GAP: u32 = 10;
/// Processes side by side, at most.
const PER_ROW: usize = 4;

#[derive(Template)]
#[template(path = "deployment.html")]
struct DeploymentPage {
    page: Page,
    name: String,
    processes: Vec<ProcessBox>,
    width: u32,
    height: u32,
    panels: Vec<NodePanel>,
    webserver: NodePanel,
    user: String,
}

/// `GET /deployments/<name>`: the deployment's nodes as a graph (one rectangle per process),
/// each node's configuration on clicking it, and the webserver's.
pub async fn deployment<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    let refresh = format!("/deployments/{}", path_segment(&name));
    let page = Page::new(&headers, ui.docs_dir.is_some()).live("kinds=workers", &refresh);
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == name) else {
        return not_found_page(&page, &format!("There is no deployment `{name}` here."));
    };
    let view = match deployment.client.deployment().await {
        Ok(view) => view,
        Err(error) => return unavailable_page(&page, &deployment.name, &error),
    };
    let processes = layout(&view);
    let rows = processes.len().div_ceil(PER_ROW);
    let tallest = processes.iter().map(|p| p.height).max().unwrap_or(0);
    let columns = u32::try_from(processes.len().min(PER_ROW)).unwrap_or(1);
    let panels = view
        .masters
        .iter()
        .map(master_panel)
        .chain(view.workers.iter().map(worker_panel))
        .collect();
    let webserver = NodePanel {
        anchor: "node-webserver".to_owned(),
        title: "Webserver".to_owned(),
        facts: vec![
            ("Host".to_owned(), view.webserver.host.clone()),
            ("Version".to_owned(), view.webserver.schedr_version.clone()),
        ],
        config: pretty(&view.webserver.config),
    };
    render(
        &DeploymentPage {
            page,
            name: deployment.name.clone(),
            width: columns * (PROCESS_WIDTH + GAP) + GAP,
            height: u32::try_from(rows).unwrap_or(0) * (tallest + GAP) + GAP,
            processes,
            panels,
            webserver,
            user: view.user,
        },
        StatusCode::OK,
    )
}

/// The deployment's nodes grouped by process (the masters' first, the leader's first; then
/// the workers', by ID), laid out in rows of 4. A node with no process known (an
/// older agent) is a process of its own.
#[must_use]
pub fn layout(view: &DeploymentView) -> Vec<ProcessBox> {
    // (process key, host, nodes)
    let mut groups: Vec<(String, String, Vec<NodeBox>)> = Vec::new();
    let mut add = |key: String, host: String, node: NodeBox| {
        if let Some(group) = groups.iter_mut().find(|(k, _, _)| *k == key) {
            group.2.push(node);
        } else {
            groups.push((key, host, vec![node]));
        }
    };
    for master in &view.masters {
        add(
            master.node.process_id.clone(),
            master.node.host.clone(),
            NodeBox {
                anchor: master_anchor(&master.master_id),
                kind: "master",
                label: format!("master {}", short(&master.master_id)),
                state: master.role.clone(),
                x: 0,
                y: 0,
            },
        );
    }
    for worker in &view.workers {
        let key = worker
            .process_id
            .clone()
            .unwrap_or_else(|| format!("worker:{}", worker.worker_id));
        add(
            key,
            String::new(),
            NodeBox {
                anchor: worker_anchor(&worker.worker_id),
                kind: "worker",
                label: format!("worker {}", worker.worker_id),
                state: worker.state.clone(),
                x: 0,
                y: 0,
            },
        );
    }
    let node_count = |nodes: &[NodeBox]| u32::try_from(nodes.len()).unwrap_or(u32::MAX);
    let height = |nodes: &[NodeBox]| {
        PROCESS_TITLE + node_count(nodes).saturating_mul(NODE_HEIGHT + GAP) + GAP / 2
    };
    let tallest = groups.iter().map(|(_, _, n)| height(n)).max().unwrap_or(0);
    groups
        .into_iter()
        .enumerate()
        .map(|(index, (key, host, mut nodes))| {
            let column = u32::try_from(index % PER_ROW).unwrap_or(0);
            let row = u32::try_from(index / PER_ROW).unwrap_or(0);
            let (x, y) = (
                GAP + column * (PROCESS_WIDTH + GAP),
                GAP + row * (tallest + GAP),
            );
            for (i, node) in nodes.iter_mut().enumerate() {
                node.x = x + GAP;
                node.y = y + PROCESS_TITLE + u32::try_from(i).unwrap_or(0) * (NODE_HEIGHT + GAP);
            }
            let mut title = if host.is_empty() {
                "process".to_owned()
            } else {
                host
            };
            if !key.starts_with("worker:") && !key.is_empty() {
                let _ = write!(title, " · {}", short(&key));
            }
            ProcessBox {
                title,
                x,
                y,
                height: height(&nodes),
                nodes,
            }
        })
        .collect()
}

/// A master's anchor on its deployment's page.
#[must_use]
pub fn master_anchor(master_id: &str) -> String {
    format!("node-master-{}", path_segment(master_id))
}

fn worker_anchor(worker_id: &str) -> String {
    format!("node-worker-{}", path_segment(worker_id))
}

/// An ID's last 8 characters: a UUIDv7's are random, enough to tell its masters apart (its
/// first ones are its time, the same for masters started within a minute).
#[must_use]
pub fn short(id: &str) -> &str {
    id.char_indices()
        .rev()
        .nth(7)
        .map_or(id, |(at, _)| &id[at..])
}

/// A configuration as shown: masked again here (each node masked its own before it left its
/// process; one that didn't, an older or altered node, still shows no secret), indented.
fn pretty(config: &serde_json::Value) -> String {
    let mut config = config.clone();
    schedr_config::masked::mask(&mut config);
    serde_json::to_string_pretty(&config).unwrap_or_default()
}

fn master_panel(master: &MasterView) -> NodePanel {
    NodePanel {
        anchor: master_anchor(&master.master_id),
        title: format!("Master {} ({})", short(&master.master_id), master.role),
        facts: vec![
            ("ID".to_owned(), master.master_id.clone()),
            ("Host".to_owned(), master.node.host.clone()),
            ("Version".to_owned(), master.node.schedr_version.clone()),
        ],
        config: pretty(&master.node.config),
    }
}

fn worker_panel(worker: &WorkerNodeView) -> NodePanel {
    let mut facts = vec![
        ("State".to_owned(), worker.state.clone()),
        ("Version".to_owned(), worker.schedr_version.clone()),
    ];
    if let Some(master) = &worker.master_id {
        facts.push((
            "Registered with".to_owned(),
            format!("master {}", short(master)),
        ));
    }
    NodePanel {
        anchor: worker_anchor(&worker.worker_id),
        title: format!("Worker {}", worker.worker_id),
        facts,
        config: worker.config.as_ref().map(pretty).unwrap_or_default(),
    }
}

/// A connection's row on the Config page.
#[derive(Debug, Clone)]
pub struct ConnectionRow {
    /// The deployment.
    pub name: String,
    /// Its page.
    pub href: String,
    /// Its webserver's version (empty when unreachable).
    pub version: String,
    /// Whether that differs from this UI's.
    pub mismatch: bool,
    /// How long the answer took, in milliseconds.
    pub latency: String,
    /// How the UI proves itself to it.
    pub proof: &'static str,
    /// Who the webserver took the request as.
    pub user: String,
    /// Why it couldn't be reached.
    pub error: Option<String>,
}

#[derive(Template)]
#[template(path = "config.html")]
struct ConfigPage {
    page: Page,
    version: &'static str,
    user: String,
    config: String,
    connections: Vec<ConnectionRow>,
}

/// `GET /config`: the UI's own side (decision 125): its configuration (masked), its version
/// beside each webserver's, each connection's state, and who is signed in.
pub async fn config<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let version = env!("CARGO_PKG_VERSION");
    let answers = ask_all(&ui).await;
    let connections = ui
        .deployments
        .iter()
        .zip(answers)
        .map(|(deployment, (answer, took))| {
            let href = format!(
                "{}/deployments/{}",
                page.base,
                path_segment(&deployment.name)
            );
            let latency = format!("{} ms", took.as_millis());
            match answer {
                Ok(view) => ConnectionRow {
                    name: deployment.name.clone(),
                    href,
                    mismatch: view.webserver.schedr_version != version,
                    version: view.webserver.schedr_version,
                    latency,
                    proof: deployment.proof,
                    user: view.user,
                    error: None,
                },
                Err(error) => ConnectionRow {
                    name: deployment.name.clone(),
                    href,
                    version: String::new(),
                    mismatch: false,
                    latency,
                    proof: deployment.proof,
                    user: String::new(),
                    error: Some(error.to_string()),
                },
            }
        })
        .collect();
    render(
        &ConfigPage {
            page,
            version,
            user: schedr_client::current_user().unwrap_or_else(|| "anonymous".to_owned()),
            config: pretty(&ui.config),
            connections,
        },
        StatusCode::OK,
    )
}
