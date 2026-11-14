//! The Deployments page (M4.4b, decisions 121-129): one graph of everything the UI shows. The
//! UI on top (in standalone inside its process), linked by HTTP to each deployment's
//! webserver; in each deployment's frame its webserver, linked by the database to its
//! masters, and its workers, each linked by gRPC to the master it registered with. Each
//! rectangle around nodes is a process. A node with a problem is yellow (a warning) or red
//! (an error). Clicking a node opens its panel: its facts, its problems and its configuration,
//! as TOML. Every configuration was masked by the node it belongs to before it left that
//! node's process, and is masked again before it is shown.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use askama::Template;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use schedr_api::views::{DeploymentView, MasterView, NodeView, ProblemView, WorkerNodeView};
use schedr_client::{ClientError, Transport};

use crate::Ui;
use crate::page::Page;
use crate::pages::render;

/// A node's width on the graph.
pub const NODE_WIDTH: u32 = 200;
/// A node's height on the graph (three lines).
pub const NODE_HEIGHT: u32 = 52;
/// The space above a process's first node (its title).
const PROCESS_TITLE: u32 = 24;
/// The space above a deployment frame's first process (its name).
const FRAME_TITLE: u32 = 30;
/// The space between nodes, and around them in a process.
const GAP: u32 = 10;
/// The space between rows (and around frames): room for the lines between them.
const ROW_GAP: u32 = 40;
/// A process's width.
const PROCESS_WIDTH: u32 = NODE_WIDTH + 2 * GAP;
/// Processes side by side in a row of a frame, at most.
const PER_ROW: usize = 3;
/// A node's label, at most (characters; the rest is cut, the whole kept in its tooltip).
const LABEL_CHARS: usize = 24;
/// A node's other lines, at most.
const LINE_CHARS: usize = 30;

/// How bad a node's problems are: its colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// None.
    Fine,
    /// A warning: yellow.
    Warning,
    /// An error: red.
    Error,
}

impl Level {
    /// The worst of `problems`.
    #[must_use]
    pub fn of(problems: &[ProblemView]) -> Self {
        problems.iter().fold(Self::Fine, |level, problem| {
            level.max(if problem.error {
                Self::Error
            } else {
                Self::Warning
            })
        })
    }

    /// Its CSS class (empty when fine).
    #[must_use]
    pub const fn class(self) -> &'static str {
        match self {
            Self::Fine => "",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

/// A node on the graph.
#[derive(Debug, Clone)]
pub struct GraphNode {
    /// Its anchor: its panel's ID.
    pub anchor: String,
    /// `ui`, `webserver`, `master` or `worker`.
    pub kind: &'static str,
    /// Its name on the graph (cut to fit).
    pub label: String,
    /// Its second line: its role or state, and its version.
    pub sub: String,
    /// Its third line (a worker's queues; may be empty).
    pub extra: String,
    /// Its whole name and lines, as its tooltip.
    pub tooltip: String,
    /// Its role or state as a CSS class (`leader`, `standby`, a worker's state).
    pub state: String,
    /// How bad its problems are.
    pub level: Level,
    /// Its top-left corner.
    pub x: u32,
    /// Its top.
    pub y: u32,
}

impl GraphNode {
    /// Its box's CSS classes: its state, and its problems' level, which colours it.
    #[must_use]
    pub fn classes(&self) -> String {
        ["box", self.state.as_str(), self.level.class()]
            .into_iter()
            .filter(|class| !class.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The mark on its corner: `!` for a warning, `×` for an error.
    #[must_use]
    pub const fn mark(&self) -> &'static str {
        match self.level {
            Level::Fine => "",
            Level::Warning => "!",
            Level::Error => "×",
        }
    }
}

/// A rectangle on the graph: a process (around its nodes) or a deployment's frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Its title.
    pub title: String,
    /// Its top-left corner.
    pub x: u32,
    /// Its top.
    pub y: u32,
    /// Its width.
    pub width: u32,
    /// Its height.
    pub height: u32,
}

/// A line between two nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    /// `http`, `database` or `grpc`: its CSS class.
    pub kind: &'static str,
    /// Drawn dashed: not connected now.
    pub dashed: bool,
    /// What it is, as its tooltip.
    pub title: String,
    /// Its SVG path.
    pub path: String,
}

/// The graph: deployment frames, processes, the lines between nodes, the nodes, in drawing
/// order.
#[derive(Debug, Clone)]
pub struct Graph {
    /// Its width.
    pub width: u32,
    /// Its height.
    pub height: u32,
    /// Each deployment's frame.
    pub frames: Vec<Frame>,
    /// Each process.
    pub processes: Vec<Frame>,
    /// The lines (none inside a process).
    pub edges: Vec<Edge>,
    /// The nodes.
    pub nodes: Vec<GraphNode>,
}

/// A node's panel, shown when its node is clicked.
#[derive(Debug, Clone)]
pub struct NodePanel {
    /// Its anchor (see [`GraphNode::anchor`]).
    pub anchor: String,
    /// Its title: `Master …0d3a9c1f (leader) · prod`.
    pub title: String,
    /// What else there is to say: its host, process, version, master.
    pub facts: Vec<(String, String)>,
    /// Its problems now.
    pub problems: Vec<ProblemView>,
    /// Its configuration as TOML (masked, unset values left out); empty when it sent none.
    pub config: String,
    /// What to say when there is no configuration (empty: nothing).
    pub no_config: &'static str,
    /// The UI's connections (the UI's panel only).
    pub connections: Vec<ConnectionRow>,
}

/// One of the UI's connections, in its panel.
#[derive(Debug, Clone)]
pub struct ConnectionRow {
    /// The deployment.
    pub name: String,
    /// Its webserver's node.
    pub href: String,
    /// Its webserver's version (empty when unreachable).
    pub version: String,
    /// Whether that differs from this UI's.
    pub mismatch: bool,
    /// How long the answer took.
    pub latency: String,
    /// How the UI proves itself to it.
    pub proof: &'static str,
    /// Who the webserver took the request as.
    pub user: String,
    /// Why it couldn't be reached.
    pub error: Option<String>,
}

/// The UI's own side, as the graph shows it.
#[derive(Debug, Clone)]
pub struct UiSide {
    /// Its process (empty: not known).
    pub process_id: String,
    /// Its host (empty: not known).
    pub host: String,
    /// Its version.
    pub version: String,
    /// Who is signed in.
    pub user: String,
    /// Its configuration, masked.
    pub config: serde_json::Value,
}

/// A deployment as asked.
#[derive(Debug, Clone)]
pub struct Asked {
    /// Its name.
    pub name: String,
    /// How the UI proves itself to its webserver.
    pub proof: &'static str,
    /// Its answer, or why there was none.
    pub answer: Result<DeploymentView, String>,
    /// How long that took.
    pub took: Duration,
}

/// Said in a panel of a node that sent no configuration.
const NO_CONFIG: &str = "It sent no configuration (an older agent).";

/// The UI's anchor.
pub const UI_ANCHOR: &str = "node.ui";

/// A part of an anchor: every character but `A-Z a-z 0-9 _ -` percent-encoded, so the `.`
/// between parts never comes from a name (two nodes never share an anchor).
fn anchor_part(text: &str) -> String {
    let mut part = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
            part.push(char::from(byte));
        } else {
            let _ = write!(part, "%{byte:02X}");
        }
    }
    part
}

/// A deployment's webserver's anchor.
#[must_use]
pub fn webserver_anchor(deployment: &str) -> String {
    format!("node.{}.webserver", anchor_part(deployment))
}

/// A master's anchor on the Deployments page.
#[must_use]
pub fn master_anchor(deployment: &str, master_id: &str) -> String {
    format!(
        "node.{}.master.{}",
        anchor_part(deployment),
        anchor_part(master_id)
    )
}

/// A worker's anchor on the Deployments page.
#[must_use]
pub fn worker_anchor(deployment: &str, worker_id: &str) -> String {
    format!(
        "node.{}.worker.{}",
        anchor_part(deployment),
        anchor_part(worker_id)
    )
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

/// `text` cut to `chars` characters, `…` saying it was.
fn cut(text: &str, chars: usize) -> String {
    if text.chars().count() <= chars {
        return text.to_owned();
    }
    let mut cut: String = text.chars().take(chars.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

/// `value` without its unset (null) values, in objects and arrays alike.
fn without_nulls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            map.values_mut().for_each(without_nulls);
        }
        serde_json::Value::Array(items) => {
            items.retain(|v| !v.is_null());
            items.iter_mut().for_each(without_nulls);
        }
        _ => {}
    }
}

/// A configuration as shown: masked again here (each node masked its own before it left its
/// process; one that didn't, an older or altered node, still shows no secret), its unset
/// values left out, as TOML (as JSON, indented, should TOML not hold it).
#[must_use]
pub fn pretty(config: &serde_json::Value) -> String {
    let mut config = config.clone();
    schedr_config::masked::mask(&mut config);
    without_nulls(&mut config);
    if config.is_object()
        && let Ok(toml) = toml::to_string_pretty(&config)
    {
        // An empty one is still one (not "it sent none").
        return if toml.is_empty() {
            "# nothing set".to_owned()
        } else {
            toml
        };
    }
    serde_json::to_string_pretty(&config).unwrap_or_default()
}

/// A process being laid out: its key, host, and nodes (with their tiers).
struct Group {
    key: String,
    host: String,
    nodes: Vec<(u8, GraphNode)>,
}

impl Group {
    fn tier(&self) -> u8 {
        self.nodes.first().map_or(u8::MAX, |(tier, _)| *tier)
    }

    fn height(&self) -> u32 {
        let count = u32::try_from(self.nodes.len()).unwrap_or(u32::MAX);
        PROCESS_TITLE + count.saturating_mul(NODE_HEIGHT + GAP)
    }

    fn title(&self) -> String {
        let host = if self.host.is_empty() {
            "process"
        } else {
            &self.host
        };
        if self.key.contains(':') || self.key.is_empty() {
            host.to_owned()
        } else {
            format!("{host} · {}", short(&self.key))
        }
    }
}

/// The processes being laid out, found by key.
#[derive(Default)]
struct Groups {
    list: Vec<Group>,
    by_key: HashMap<String, usize>,
}

impl Groups {
    /// Adds `node` to its process's group (`key`), or a new one.
    fn add(&mut self, key: String, host: &str, tier: u8, node: GraphNode) {
        if let Some(&at) = self.by_key.get(&key) {
            let group = &mut self.list[at];
            if group.host.is_empty() {
                host.clone_into(&mut group.host);
            }
            group.nodes.push((tier, node));
        } else {
            self.by_key.insert(key.clone(), self.list.len());
            self.list.push(Group {
                key,
                host: host.to_owned(),
                nodes: vec![(tier, node)],
            });
        }
    }
}

fn node(anchor: String, kind: &'static str, label: &str, sub: &str) -> GraphNode {
    GraphNode {
        anchor,
        kind,
        tooltip: format!("{label}\n{sub}"),
        label: cut(label, LABEL_CHARS),
        sub: cut(sub, LINE_CHARS),
        extra: String::new(),
        state: String::new(),
        level: Level::Fine,
        x: 0,
        y: 0,
    }
}

/// Lays `groups` out from (`x`, `y`): a row for each tier (wrapping at [`PER_ROW`]), the
/// processes' nodes stacked. Returns each process's rectangle, with its nodes, and the size.
fn place(groups: Vec<Group>, x: u32, y: u32) -> (Vec<(Frame, Vec<GraphNode>)>, u32, u32) {
    let mut rows: Vec<Vec<Group>> = Vec::new();
    let mut groups = groups;
    groups.sort_by_key(Group::tier);
    for group in groups {
        match rows.last_mut() {
            Some(row) if row.len() < PER_ROW && row[0].tier() == group.tier() => row.push(group),
            _ => rows.push(vec![group]),
        }
    }
    let mut placed = Vec::new();
    let (mut width, mut top) = (0, y);
    for row in rows {
        let tallest = row.iter().map(Group::height).max().unwrap_or(0);
        let mut left = x;
        for group in row {
            let height = group.height();
            let frame = Frame {
                title: group.title(),
                x: left,
                y: top,
                width: PROCESS_WIDTH,
                height,
            };
            let nodes = group
                .nodes
                .into_iter()
                .enumerate()
                .map(|(i, (_, mut node))| {
                    node.x = left + GAP;
                    node.y = top
                        + PROCESS_TITLE
                        + u32::try_from(i)
                            .unwrap_or(0)
                            .saturating_mul(NODE_HEIGHT + GAP);
                    node
                })
                .collect();
            placed.push((frame, nodes));
            left += PROCESS_WIDTH + GAP;
        }
        width = width.max(left - GAP - x);
        top += tallest + ROW_GAP;
    }
    (placed, width, (top - y).saturating_sub(ROW_GAP))
}

/// A line to draw: from a node to a node (anchors), its kind, dashed, its tooltip.
struct Link {
    from: String,
    to: String,
    kind: &'static str,
    dashed: bool,
    title: String,
}

/// The SVG path from `from`'s bottom to `to`'s top (`to`'s left side from `from`'s right when
/// `to` isn't below it).
fn path(from: (u32, u32), to: (u32, u32)) -> String {
    let (fx, fy) = (from.0 + NODE_WIDTH / 2, from.1 + NODE_HEIGHT);
    let (tx, ty) = (to.0 + NODE_WIDTH / 2, to.1);
    if ty > fy {
        let bend = (ty - fy) / 2;
        format!(
            "M{fx} {fy} C{fx} {} {tx} {} {tx} {ty}",
            fy + bend,
            ty - bend
        )
    } else {
        let (fx, fy) = (from.0 + NODE_WIDTH, from.1 + NODE_HEIGHT / 2);
        let (tx, ty) = (to.0, to.1 + NODE_HEIGHT / 2);
        format!("M{fx} {fy} L{tx} {ty}")
    }
}

/// What the graph gathers while going through the deployments: the panels, the lines, the
/// UI's connections and problems.
#[derive(Default)]
struct Gathered {
    panels: Vec<NodePanel>,
    links: Vec<Link>,
    connections: Vec<ConnectionRow>,
    ui_problems: Vec<ProblemView>,
}

impl Gathered {
    /// A deployment whose webserver couldn't be reached (`error`): a red placeholder.
    fn unreachable(&mut self, asked: &Asked, error: &str, groups: &mut Groups) {
        let ws_anchor = webserver_anchor(&asked.name);
        let latency = format!("{} ms", asked.took.as_millis());
        self.ui_problems.push(ProblemView {
            error: true,
            message: format!("can't reach `{}`'s webserver: {error}", asked.name),
        });
        let mut placeholder = node(ws_anchor.clone(), "webserver", "webserver", "unreachable");
        "unreachable".clone_into(&mut placeholder.state);
        placeholder.level = Level::Error;
        groups.add(format!("webserver:{}", asked.name), "", 1, placeholder);
        self.panels.push(NodePanel {
            anchor: ws_anchor.clone(),
            title: format!("Webserver · {}", asked.name),
            facts: vec![
                ("The UI proves itself by".to_owned(), asked.proof.to_owned()),
                ("Gave up after".to_owned(), latency.clone()),
            ],
            problems: vec![ProblemView {
                error: true,
                message: format!("unreachable: {error}"),
            }],
            config: String::new(),
            no_config: "",
            connections: Vec::new(),
        });
        self.links.push(Link {
            from: UI_ANCHOR.to_owned(),
            to: ws_anchor.clone(),
            kind: "http",
            dashed: true,
            title: format!("HTTP: `{}` unreachable", asked.name),
        });
        self.connections.push(ConnectionRow {
            name: asked.name.clone(),
            href: format!("#{ws_anchor}"),
            version: String::new(),
            mismatch: false,
            latency,
            proof: asked.proof,
            user: String::new(),
            error: Some(error.to_owned()),
        });
    }

    /// A deployment that answered (`view`); the UI's node drawn in its webserver's process when
    /// `ui_node` is given.
    fn answered(
        &mut self,
        (ui, ui_node): (&UiSide, Option<GraphNode>),
        asked: &Asked,
        view: &DeploymentView,
        groups: &mut Groups,
    ) {
        let ws_anchor = webserver_anchor(&asked.name);
        let latency = format!("{} ms", asked.took.as_millis());
        let mismatch = view.webserver.schedr_version != ui.version;
        if mismatch {
            self.ui_problems.push(ProblemView {
                error: false,
                message: format!(
                    "`{}`'s webserver is version {}; this UI is {}",
                    asked.name, view.webserver.schedr_version, ui.version
                ),
            });
        }
        self.connections.push(ConnectionRow {
            name: asked.name.clone(),
            href: format!("#{ws_anchor}"),
            version: view.webserver.schedr_version.clone(),
            mismatch,
            latency: latency.clone(),
            proof: asked.proof,
            user: view.user.clone(),
            error: None,
        });
        let ws_key = process_key(&view.webserver.process_id, "webserver", &asked.name);
        if let Some(ui_node) = ui_node {
            groups.add(ws_key.clone(), &view.webserver.host, 0, ui_node);
        }
        deployment_nodes(asked, view, &ws_key, groups, self);
        if let Some(panel) = self.panels.iter_mut().find(|p| p.anchor == ws_anchor) {
            panel.facts.extend([
                ("Answered in".to_owned(), latency),
                ("The UI proves itself by".to_owned(), asked.proof.to_owned()),
                ("It took the UI's requests as".to_owned(), view.user.clone()),
            ]);
        }
        self.links.push(Link {
            from: UI_ANCHOR.to_owned(),
            to: ws_anchor,
            kind: "http",
            dashed: false,
            title: format!("HTTP: the UI reads `{}`", asked.name),
        });
    }
}

/// The graph and the nodes' panels for the UI (`ui`) and the deployments as asked.
#[must_use]
pub fn graph(ui: &UiSide, deployments: &[Asked]) -> (Graph, Vec<NodePanel>) {
    let mut gathered = Gathered::default();
    // The UI's node, in its own process unless it shares one with a webserver (standalone).
    let ui_home = deployments.iter().position(|d| {
        !ui.process_id.is_empty()
            && d.answer
                .as_ref()
                .is_ok_and(|v| v.webserver.process_id == ui.process_id)
    });
    let mut ui_node = Some(node(
        UI_ANCHOR.to_owned(),
        "ui",
        "UI",
        &format!("signed in as {} · {}", ui.user, ui.version),
    ));
    let mut deployment_groups = Vec::new();
    for (index, asked) in deployments.iter().enumerate() {
        let mut groups = Groups::default();
        match &asked.answer {
            Err(error) => gathered.unreachable(asked, error, &mut groups),
            Ok(view) => {
                let home = if ui_home == Some(index) {
                    ui_node.take()
                } else {
                    None
                };
                gathered.answered((ui, home), asked, view, &mut groups);
            }
        }
        deployment_groups.push((asked.name.clone(), groups.list));
    }
    let Gathered {
        mut panels,
        links,
        connections,
        ui_problems,
    } = gathered;
    let ui_level = Level::of(&ui_problems);
    let mut ui_facts = vec![
        ("Signed in as".to_owned(), ui.user.clone()),
        ("Version".to_owned(), ui.version.clone()),
    ];
    if !ui.host.is_empty() {
        ui_facts.push(("Host".to_owned(), ui.host.clone()));
    }
    if !ui.process_id.is_empty() {
        ui_facts.push(("Process".to_owned(), ui.process_id.clone()));
    }
    panels.insert(
        0,
        NodePanel {
            anchor: UI_ANCHOR.to_owned(),
            title: "UI".to_owned(),
            facts: ui_facts,
            problems: ui_problems,
            config: pretty(&ui.config),
            no_config: NO_CONFIG,
            connections,
        },
    );
    let ui_group = ui_node.map(|ui_node| Group {
        key: process_key(&ui.process_id, "ui", ""),
        host: ui.host.clone(),
        nodes: vec![(0, ui_node)],
    });
    let mut graph = lay_out(ui_group, deployment_groups, links);
    if let Some(ui) = graph.nodes.iter_mut().find(|n| n.anchor == UI_ANCHOR) {
        ui.level = ui_level;
    }
    (graph, panels)
}

/// The graph laid out: the UI's own process (`ui`, when it has one) on top, the deployments'
/// frames side by side below it, and the lines between nodes of different processes.
fn lay_out(ui: Option<Group>, deployments: Vec<(String, Vec<Group>)>, links: Vec<Link>) -> Graph {
    let mut placed: Vec<(Frame, Vec<GraphNode>)> = Vec::new();
    let mut frames = Vec::new();
    let mut top = ROW_GAP / 2;
    let mut width = 0;
    if let Some(group) = ui {
        let (mut process, w, h) = place(vec![group], ROW_GAP / 2, top);
        placed.append(&mut process);
        width = width.max(w);
        top += h + ROW_GAP;
    }
    let mut left = ROW_GAP / 2;
    let mut bottom = top;
    for (name, groups) in deployments {
        let (mut processes, w, h) = place(groups, left + GAP, top + FRAME_TITLE);
        let frame = Frame {
            title: name,
            x: left,
            y: top,
            width: w + 2 * GAP,
            height: FRAME_TITLE + h + GAP,
        };
        left += frame.width + ROW_GAP;
        bottom = bottom.max(frame.y + frame.height);
        frames.push(frame);
        placed.append(&mut processes);
    }
    width = width.max(left.saturating_sub(ROW_GAP / 2));
    let mut processes = Vec::new();
    let mut nodes = Vec::new();
    let mut at = HashMap::new();
    for (process, process_nodes) in placed {
        for node in &process_nodes {
            at.insert(node.anchor.clone(), (node.x, node.y, processes.len()));
        }
        processes.push(process);
        nodes.extend(process_nodes);
    }
    let edges = links
        .into_iter()
        .filter_map(|link| {
            let (fx, fy, from_process) = *at.get(&link.from)?;
            let (tx, ty, to_process) = *at.get(&link.to)?;
            (from_process != to_process).then(|| Edge {
                kind: link.kind,
                dashed: link.dashed,
                title: link.title,
                path: path((fx, fy), (tx, ty)),
            })
        })
        .collect();
    Graph {
        width: width + ROW_GAP / 2,
        height: bottom + ROW_GAP / 2,
        frames,
        processes,
        edges,
        nodes,
    }
}

/// A process's key: its ID, or (an older node, which doesn't send it) one of its own.
fn process_key(process_id: &str, kind: &str, id: &str) -> String {
    if process_id.is_empty() {
        format!("{kind}:{id}")
    } else {
        process_id.to_owned()
    }
}

/// One deployment's webserver, masters and workers: their nodes in `groups`, their panels and
/// the lines between them.
fn deployment_nodes(
    asked: &Asked,
    view: &DeploymentView,
    ws_key: &str,
    groups: &mut Groups,
    gathered: &mut Gathered,
) {
    let Gathered { panels, links, .. } = gathered;
    let name = &asked.name;
    let ws_anchor = webserver_anchor(name);
    let ws = &view.webserver;
    groups.add(
        ws_key.to_owned(),
        &ws.host,
        1,
        node(
            ws_anchor.clone(),
            "webserver",
            "webserver",
            &ws.schedr_version,
        ),
    );
    panels.push(NodePanel {
        anchor: ws_anchor.clone(),
        title: format!("Webserver · {name}"),
        facts: node_facts(ws),
        problems: Vec::new(),
        config: pretty(&ws.config),
        no_config: NO_CONFIG,
        connections: Vec::new(),
    });
    for master in &view.masters {
        let anchor = master_anchor(name, &master.master_id);
        let mut graph_node = node(
            anchor.clone(),
            "master",
            &format!("master {}", short(&master.master_id)),
            &format!("{} · {}", master.role, master.node.schedr_version),
        );
        graph_node.state.clone_from(&master.role);
        graph_node.level = Level::of(&master.problems);
        groups.add(
            process_key(&master.node.process_id, "master", &master.master_id),
            &master.node.host,
            2,
            graph_node,
        );
        panels.push(master_panel(name, master));
        links.push(Link {
            from: ws_anchor.clone(),
            to: anchor,
            kind: "database",
            dashed: false,
            title: format!(
                "database: the webserver and master {} share it",
                short(&master.master_id)
            ),
        });
    }
    for worker in &view.workers {
        worker_node(name, view, worker, groups, (panels, links));
    }
}

/// A worker's node in `groups`, its panel, and its line to its master.
fn worker_node(
    name: &str,
    view: &DeploymentView,
    worker: &WorkerNodeView,
    groups: &mut Groups,
    (panels, links): (&mut Vec<NodePanel>, &mut Vec<Link>),
) {
    let anchor = worker_anchor(name, &worker.worker_id);
    let mut graph_node = node(
        anchor.clone(),
        "worker",
        &format!("worker {}", worker.worker_id),
        &format!("{} · {}", worker.state, worker.schedr_version),
    );
    if !worker.queues.is_empty() {
        let queues = format!("queues: {}", worker.queues.join(", "));
        graph_node.extra = cut(&queues, LINE_CHARS);
        graph_node.tooltip = format!("{}\n{queues}", graph_node.tooltip);
    }
    graph_node.state.clone_from(&worker.state);
    graph_node.level = Level::of(&worker.problems);
    groups.add(
        process_key(
            worker.process_id.as_deref().unwrap_or_default(),
            "worker",
            &worker.worker_id,
        ),
        "",
        3,
        graph_node,
    );
    panels.push(worker_panel(name, view, worker));
    if let Some(master) = &worker.master_id
        && view.masters.iter().any(|m| &m.master_id == master)
    {
        let connected = matches!(worker.state.as_str(), "active" | "draining");
        links.push(Link {
            from: master_anchor(name, master),
            to: anchor,
            kind: "grpc",
            dashed: !connected,
            title: format!(
                "gRPC: worker {} {} master {}",
                worker.worker_id,
                if connected {
                    "is connected to"
                } else {
                    "isn't connected to"
                },
                short(master)
            ),
        });
    }
}

/// A node's problems as shown: masked again (an older webserver didn't).
fn masked_problems(problems: &[ProblemView]) -> Vec<ProblemView> {
    problems
        .iter()
        .map(|problem| ProblemView {
            error: problem.error,
            message: schedr_config::masked::mask_text(&problem.message),
        })
        .collect()
}

fn node_facts(node: &NodeView) -> Vec<(String, String)> {
    let mut facts = Vec::new();
    if !node.host.is_empty() {
        facts.push(("Host".to_owned(), node.host.clone()));
    }
    if !node.process_id.is_empty() {
        facts.push(("Process".to_owned(), node.process_id.clone()));
    }
    facts.push(("Version".to_owned(), node.schedr_version.clone()));
    facts
}

fn master_panel(deployment: &str, master: &MasterView) -> NodePanel {
    let mut facts = vec![
        ("ID".to_owned(), master.master_id.clone()),
        ("Role".to_owned(), master.role.clone()),
    ];
    facts.extend(node_facts(&master.node));
    NodePanel {
        anchor: master_anchor(deployment, &master.master_id),
        title: format!(
            "Master {} ({}) · {deployment}",
            short(&master.master_id),
            master.role
        ),
        facts,
        problems: masked_problems(&master.problems),
        config: pretty(&master.node.config),
        no_config: NO_CONFIG,
        connections: Vec::new(),
    }
}

fn worker_panel(deployment: &str, view: &DeploymentView, worker: &WorkerNodeView) -> NodePanel {
    let mut facts = vec![
        ("State".to_owned(), worker.state.clone()),
        ("Queues".to_owned(), worker.queues.join(", ")),
        ("Version".to_owned(), worker.schedr_version.clone()),
    ];
    if let Some(process) = &worker.process_id {
        facts.push(("Process".to_owned(), process.clone()));
    }
    if let Some(master) = &worker.master_id {
        let heard = view.masters.iter().any(|m| &m.master_id == master);
        facts.push((
            "Registered with".to_owned(),
            if heard {
                format!("master {}", short(master))
            } else {
                format!("master {} (not heard from lately)", short(master))
            },
        ));
    }
    NodePanel {
        anchor: worker_anchor(deployment, &worker.worker_id),
        title: format!("Worker {} · {deployment}", worker.worker_id),
        facts,
        problems: masked_problems(&worker.problems),
        config: worker.config.as_ref().map(pretty).unwrap_or_default(),
        no_config: NO_CONFIG,
        connections: Vec::new(),
    }
}

/// The worker states in the order the Status block counts them: the ones that need a look
/// first after `active`.
pub const WORKER_STATES: [&str; 5] = ["active", "draining", "disconnected", "lost", "offline"];

/// A problem in the Status block's list, opening its node's panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusProblem {
    /// An error; a warning otherwise.
    pub error: bool,
    /// The node it is on (`master 1a2b3c4d`, `worker w1`).
    pub node: String,
    /// What it is.
    pub message: String,
    /// Its node's panel.
    pub href: String,
}

/// A deployment's line in the Status block above the graph (decision 462).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusRow {
    /// The deployment.
    pub name: String,
    /// Its webserver's panel.
    pub href: String,
    /// Why its webserver couldn't be reached; none when it answered.
    pub unreachable: Option<String>,
    /// How long the answer (or giving up) took.
    pub took: String,
    /// Its masters leading (one, unless the lease is changing hands) and standing by.
    pub leaders: usize,
    /// Its standbys.
    pub standbys: usize,
    /// Its workers by state, those there are: `active`, `draining`, `disconnected`, `lost`,
    /// `offline`, in that order.
    pub workers: Vec<(&'static str, usize)>,
    /// The attempts running on its active workers.
    pub running: u32,
    /// Its active workers' task slots.
    pub slots: u32,
    /// Its nodes' problems, errors first.
    pub problems: Vec<StatusProblem>,
    /// Its schedr versions, each with how many processes run it (webserver, masters and the
    /// workers not gone): more than one is warned.
    pub versions: Vec<(String, usize)>,
}

impl StatusRow {
    /// How many of its problems are errors.
    #[must_use]
    pub fn errors(&self) -> usize {
        self.problems.iter().filter(|p| p.error).count()
    }

    /// How many are warnings.
    #[must_use]
    pub fn warnings(&self) -> usize {
        self.problems.len() - self.errors()
    }

    /// No master leads: nothing is scheduled.
    #[must_use]
    pub const fn leaderless(&self) -> bool {
        self.unreachable.is_none() && self.leaders == 0
    }

    /// Its processes run more than one version.
    #[must_use]
    pub fn mixed(&self) -> bool {
        self.versions.len() > 1
    }
}

/// The Status block: a line per deployment as asked, in their order (decision 462).
#[must_use]
pub fn status(deployments: &[Asked]) -> Vec<StatusRow> {
    deployments.iter().map(status_row).collect()
}

/// One deployment's line.
fn status_row(asked: &Asked) -> StatusRow {
    let name = &asked.name;
    let mut row = StatusRow {
        name: name.clone(),
        href: format!("#{}", webserver_anchor(name)),
        unreachable: None,
        took: format!("{} ms", asked.took.as_millis()),
        leaders: 0,
        standbys: 0,
        workers: Vec::new(),
        running: 0,
        slots: 0,
        problems: Vec::new(),
        versions: Vec::new(),
    };
    let view = match &asked.answer {
        Err(error) => {
            row.unreachable = Some(error.clone());
            return row;
        }
        Ok(view) => view,
    };
    let mut versions: BTreeMap<&str, usize> = BTreeMap::new();
    *versions.entry(&view.webserver.schedr_version).or_default() += 1;
    for master in &view.masters {
        if master.role == "leader" {
            row.leaders += 1;
        } else {
            row.standbys += 1;
        }
        *versions.entry(&master.node.schedr_version).or_default() += 1;
        let href = format!("#{}", master_anchor(name, &master.master_id));
        row.problems.extend(
            masked_problems(&master.problems)
                .into_iter()
                .map(|p| StatusProblem {
                    error: p.error,
                    node: format!("master {}", short(&master.master_id)),
                    message: p.message,
                    href: href.clone(),
                }),
        );
    }
    let mut states: BTreeMap<usize, usize> = BTreeMap::new();
    for worker in &view.workers {
        let at = WORKER_STATES
            .iter()
            .position(|s| *s == worker.state)
            .unwrap_or(WORKER_STATES.len() - 1);
        *states.entry(at).or_default() += 1;
        if worker.state == "active" {
            row.running += worker.running;
            row.slots += worker.slots;
        }
        if !matches!(worker.state.as_str(), "offline" | "lost") {
            *versions.entry(&worker.schedr_version).or_default() += 1;
        }
        let href = format!("#{}", worker_anchor(name, &worker.worker_id));
        row.problems.extend(
            masked_problems(&worker.problems)
                .into_iter()
                .map(|p| StatusProblem {
                    error: p.error,
                    node: format!("worker {}", worker.worker_id),
                    message: p.message,
                    href: href.clone(),
                }),
        );
    }
    row.workers = states
        .into_iter()
        .map(|(at, count)| (WORKER_STATES[at], count))
        .collect();
    // Errors first, each kind in node order.
    row.problems.sort_by_key(|p| !p.error);
    row.versions = versions
        .into_iter()
        .map(|(version, count)| (version.to_owned(), count))
        .collect();
    row
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

#[derive(Template)]
#[template(path = "deployments.html")]
struct DeploymentsPage {
    page: Page,
    status: Vec<StatusRow>,
    graph: Graph,
    panels: Vec<NodePanel>,
}

/// `GET /deployments`: the graph of everything the UI shows, each deployment asked at once,
/// and each node's panel.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some()).live("kinds=workers", "/deployments");
    let asked: Vec<Asked> = ui
        .deployments
        .iter()
        .zip(ask_all(&ui).await)
        .map(|(deployment, (answer, took))| Asked {
            name: deployment.name.clone(),
            proof: deployment.proof,
            answer: answer.map_err(|e| e.to_string()),
            took,
        })
        .collect();
    let side = UiSide {
        process_id: ui.process_id.clone(),
        host: ui.host.clone(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        user: schedr_client::current_user().unwrap_or_else(|| "anonymous".to_owned()),
        config: ui.config.clone(),
    };
    let (graph, panels) = graph(&side, &asked);
    render(
        &DeploymentsPage {
            page,
            status: status(&asked),
            graph,
            panels,
        },
        StatusCode::OK,
    )
}
