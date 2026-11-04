//! Connections (M4.7e, decisions 207-211): the `/connections` page (every deployment's, by
//! root) and a DAG page's section (those its tasks declare), each root's with **Check all**
//! and a **Test** per row.
//!
//! Both are commands (`POST /api/v1/connections/check`, `.../{name}/test`): the answer is a
//! fragment reading the command again every second until its worker answered (or the job was
//! lost). A check's answer is a line under its button, then each connection's result in its
//! row's cell (out of band); a test's goes in its row's cell. Results live on the page only
//! (decision 209).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use schedr_api::views::{
    CheckConnectionsRequest, CommandView, ConnectionInfo, ConnectionList, TestConnectionRequest,
};
use schedr_client::{ClientError, Transport, path_segment};
use schedr_model::commands::{
    ConnectionJobResult, ConnectionReport, ConnectionsResult, JobState, SecretState,
};
use serde::Deserialize;

use crate::actions::{POLL_MS, definite};
use crate::page::Page;
use crate::pages::render;
use crate::{Deployment, Ui};

/// One root's connections, as a table.
#[derive(Debug, Clone)]
pub struct Group {
    /// Its heading on the Connections page: `dev · dags` (the root alone with one deployment).
    pub title: String,
    /// Its rows, by name.
    pub rows: Vec<Row>,
    /// Where Check all posts.
    pub check_post: String,
    /// The element its answer goes to.
    pub status_id: String,
}

/// A connection's row.
#[derive(Debug, Clone)]
pub struct Row {
    /// Its name.
    pub name: String,
    /// Its type (`sql`, ...); empty for a `@connection` function or a refused one.
    pub kind: String,
    /// The file defining it; empty when none does.
    pub file: String,
    /// Why it can't be used.
    pub refused: Option<String>,
    /// The DAGs whose tasks declare it.
    pub used_by: Vec<Used>,
    /// Its result cell's ID.
    pub cell_id: String,
    /// Where Test posts.
    pub test_post: String,
    /// The test dialog's title.
    pub test_title: String,
    /// The test dialog's text.
    pub test_text: String,
}

/// A DAG declaring a connection.
#[derive(Debug, Clone)]
pub struct Used {
    /// The DAG.
    pub dag_id: String,
    /// Its page.
    pub href: String,
    /// Its tasks declaring it: `extract, load`.
    pub tasks: String,
}

/// An element ID for `parts` (letters, digits and `-` only, the same for the same parts):
/// FNV-1a of them, so names of any characters make a valid ID.
fn element_id(prefix: &str, parts: &[&str]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for byte in part.bytes().chain(std::iter::once(0)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    format!("{prefix}-{hash:016x}")
}

/// The cell of connection `name` of `root` in `deployment`.
fn cell_id(deployment: &str, root: &str, name: &str) -> String {
    element_id("connection", &[deployment, root, name])
}

/// The groups of `deployment`'s `list`; with `dag`, only that DAG's connections (its root's
/// it declares).
#[must_use]
pub fn groups<T>(
    page: &Page,
    deployment: &Deployment<T>,
    list: &ConnectionList,
    dag: Option<&str>,
    several: bool,
) -> Vec<Group> {
    let mut groups = Vec::new();
    for root in &list.roots {
        let rows: Vec<Row> = root
            .connections
            .iter()
            .filter(|c| dag.is_none_or(|dag| c.used_by.iter().any(|u| u.dag_id == dag)))
            .map(|c| row(page, deployment, &root.root, c))
            .collect();
        if rows.is_empty() {
            continue;
        }
        // The refused aren't checked (nothing to render): no place for their results.
        let names: Vec<&str> = rows
            .iter()
            .filter(|r| r.refused.is_none())
            .map(|r| r.name.as_str())
            .collect();
        groups.push(Group {
            title: if several {
                format!("{} · {}", deployment.name, root.root)
            } else {
                root.root.clone()
            },
            check_post: format!(
                "{}/d/{}/connections/check?root={}&names={}",
                page.base,
                path_segment(&deployment.name),
                path_segment(&root.root),
                path_segment(&names.join(","))
            ),
            status_id: element_id("connections-check", &[&deployment.name, &root.root]),
            rows,
        });
    }
    groups
}

fn row<T>(page: &Page, deployment: &Deployment<T>, root: &str, c: &ConnectionInfo) -> Row {
    Row {
        name: c.name.clone(),
        kind: c.kind.clone().unwrap_or_default(),
        file: c.file.clone().unwrap_or_default(),
        refused: c.refused.clone(),
        used_by: c
            .used_by
            .iter()
            .map(|u| Used {
                href: format!(
                    "{}/d/{}/dags/{}",
                    page.base,
                    path_segment(&deployment.name),
                    path_segment(&u.dag_id)
                ),
                dag_id: u.dag_id.clone(),
                tasks: u.tasks.join(", "),
            })
            .collect(),
        cell_id: cell_id(&deployment.name, root, &c.name),
        test_post: format!(
            "{}/d/{}/connections/{}/test?root={}",
            page.base,
            path_segment(&deployment.name),
            path_segment(&c.name),
            path_segment(root)
        ),
        test_title: format!("Test {} of {}?", c.name, deployment.name),
        test_text: format!(
            "It opens {} on a worker of {} and runs its check: a real login to the system it \
             names, with {}'s secrets, for up to {} s.",
            c.name,
            deployment.name,
            deployment.name,
            schedr_api::routes::CONNECTION_TEST_TIMEOUT_MS / 1000
        ),
    }
}

#[derive(Template)]
#[template(path = "connections.html")]
struct ConnectionsPage {
    page: Page,
    groups: Vec<Group>,
    unavailable: Vec<String>,
}

/// `GET /connections`: every deployment's connections (asked at once), by deployment and root.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        let ui = Arc::clone(&ui);
        asked.spawn(schedr_client::for_same_user(async move {
            (index, ui.deployments[index].client.connections().await)
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    let several = ui.deployments.len() > 1;
    let mut groups = Vec::new();
    let mut unavailable = Vec::new();
    for (index, answer) in answers {
        let deployment = &ui.deployments[index];
        match answer {
            Ok(list) => groups.extend(self::groups(&page, deployment, &list, None, several)),
            Err(error) => unavailable.push(format!("{}: {error}", deployment.name)),
        }
    }
    render(
        &ConnectionsPage {
            page,
            groups,
            unavailable,
        },
        StatusCode::OK,
    )
}

/// What a check or a test is about: its root, and a check's connections shown (their cells).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Scope {
    /// The root.
    #[serde(default)]
    pub root: String,
    /// A check's connections whose cells are on the page, comma-separated.
    #[serde(default)]
    pub names: String,
}

/// `POST /d/<deployment>/connections/check?root=&names=`: Check all of a root.
pub async fn check<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path(deployment): Path<String>,
    Query(scope): Query<Scope>,
    headers: HeaderMap,
) -> Response {
    let request = CheckConnectionsRequest {
        root: Some(scope.root.clone()),
        name: None,
        worker: None,
    };
    let job = Job::Check {
        names: split(&scope.names),
    };
    send(
        &ui,
        &headers,
        &deployment,
        &scope.root,
        job,
        Request::Check(&request),
    )
    .await
}

/// `POST /d/<deployment>/connections/<name>/test?root=`: Test one.
pub async fn test<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, name)): Path<(String, String)>,
    Query(scope): Query<Scope>,
    headers: HeaderMap,
) -> Response {
    let request = TestConnectionRequest {
        root: Some(scope.root.clone()),
        worker: None,
        timeout_ms: None,
    };
    let job = Job::Test { name: name.clone() };
    let call = Request::Test(&name, &request);
    send(&ui, &headers, &deployment, &scope.root, job, call).await
}

/// Where a poll reads a command again.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PollQuery {
    /// `check` or `test`.
    #[serde(default)]
    pub kind: String,
    /// The root.
    #[serde(default)]
    pub root: String,
    /// A check's connections shown, comma-separated; a test's connection.
    #[serde(default)]
    pub names: String,
}

/// `GET /d/<deployment>/connections/commands/<id>?kind=&root=&names=`: a check's or a test's
/// answer, read again.
pub async fn poll<T: Transport>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, command_id)): Path<(String, String)>,
    Query(query): Query<PollQuery>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, false);
    let job = if query.kind == "test" {
        Job::Test { name: query.names }
    } else {
        Job::Check {
            names: split(&query.names),
        }
    };
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return fragment(&job.unsent(&format!("There is no deployment `{deployment}` here.")));
    };
    let poll = job.poll_url(&page, &deployment.name, &query.root, &command_id);
    match deployment.client.command(&command_id).await {
        Ok(command) => fragment(&job.answer(&deployment.name, &query.root, &command, poll)),
        // Sent and stored: read again (never "not sent", which would invite a second one).
        Err(error) => fragment(&job.waiting(
            &deployment.name,
            &query.root,
            &format!("can't read it now ({error}); trying again"),
            poll,
        )),
    }
}

fn split(names: &str) -> Vec<String> {
    names
        .split(',')
        .filter(|n| !n.is_empty())
        .map(str::to_owned)
        .collect()
}

/// The command a check or a test sends.
#[derive(Debug, Clone, Copy)]
enum Request<'a> {
    Check(&'a CheckConnectionsRequest),
    Test(&'a str, &'a TestConnectionRequest),
}

impl Request<'_> {
    async fn send<T: Transport>(
        self,
        client: &schedr_client::Client<T>,
    ) -> Result<CommandView, ClientError> {
        match self {
            Self::Check(request) => client.check_connections(request).await,
            Self::Test(name, request) => client.test_connection(name, request).await,
        }
    }
}

/// Sends `request` (under the browser's key) and answers with `job`'s fragment.
async fn send<T: Transport>(
    ui: &Arc<Ui<T>>,
    headers: &HeaderMap,
    deployment: &str,
    root: &str,
    job: Job,
    request: Request<'_>,
) -> Response {
    let page = Page::new(headers, false);
    let Some(deployment) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return definite(fragment(
            &job.unsent(&format!("There is no deployment `{deployment}` here.")),
        ));
    };
    let key = headers
        .get("idempotency-key")
        .and_then(|k| k.to_str().ok())
        .filter(|k| !k.is_empty())
        .map_or_else(schedr_client::new_command_key, str::to_owned);
    match deployment
        .client
        .submit(key, || request.send(&deployment.client))
        .await
    {
        Ok(command) => {
            let poll = job.poll_url(&page, &deployment.name, root, &command.command_id);
            definite(fragment(&job.answer(
                &deployment.name,
                root,
                &command,
                poll,
            )))
        }
        // The webserver refused it: nothing stored.
        Err(error @ ClientError::Server { status, .. }) if status < 500 => {
            definite(fragment(&job.unsent(&error.to_string())))
        }
        // No answer, or the server failed: it may have been stored. The browser keeps its key
        // (a test's dialog), so sending it again gives the one stored, if it was.
        Err(error) => fragment(&job.unknown(&error.to_string())),
    }
}

/// A check's or a test's answer.
#[derive(Template)]
#[template(path = "connection_answer.html")]
struct Answer {
    /// `check` or `test`.
    kind: &'static str,
    /// What was asked: `Check all of dev · dags`.
    what: String,
    /// The answer's CSS state: a command's (`pending`, `applied`, `rejected`, `failed`) for a
    /// check, a cell's for a test without results.
    state: String,
    /// Where it stands, as said.
    status_line: String,
    /// Where to read it again; empty once every job answered.
    poll: String,
    poll_ms: u32,
    /// The connections' results.
    cells: Vec<Cell>,
}

/// A connection's result, in its cell.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Cell {
    /// The cell's ID.
    id: String,
    /// `success`, `failed`, `running` or `lost`.
    state: String,
    /// `check: ok`, `test: failed (1.2 s)`, ...
    label: String,
    /// The error, and the secrets that aren't there.
    details: Vec<String>,
}

/// A check (of the connections shown) or a test (of one).
#[derive(Debug, Clone)]
enum Job {
    Check { names: Vec<String> },
    Test { name: String },
}

impl Job {
    const fn kind(&self) -> &'static str {
        match self {
            Self::Check { .. } => "check",
            Self::Test { .. } => "test",
        }
    }

    fn what(&self, deployment: &str, root: &str) -> String {
        match self {
            Self::Check { .. } => format!("Check all of {deployment} · {root}"),
            Self::Test { name } => format!("Test {name} of {deployment}"),
        }
    }

    fn poll_url(&self, page: &Page, deployment: &str, root: &str, command_id: &str) -> String {
        let names = match self {
            Self::Check { names } => names.join(","),
            Self::Test { name } => name.clone(),
        };
        format!(
            "{}/d/{}/connections/commands/{}?kind={}&root={}&names={}",
            page.base,
            path_segment(deployment),
            path_segment(command_id),
            self.kind(),
            path_segment(root),
            path_segment(&names)
        )
    }

    /// Not sent (or not known to be): why.
    fn unsent(&self, reason: &str) -> Answer {
        Answer {
            kind: self.kind(),
            what: match self {
                Self::Check { .. } => "Check all".to_owned(),
                Self::Test { name } => format!("Test {name}"),
            },
            state: "failed".to_owned(),
            status_line: format!("not sent: {reason}"),
            poll: String::new(),
            poll_ms: POLL_MS,
            cells: Vec::new(),
        }
    }

    /// No answer said whether it was stored: it may have been (never "not sent", which
    /// would invite sending it twice). The browser keeps its key, so sending it again from
    /// this page gives the one stored, if it was.
    fn unknown(&self, error: &str) -> Answer {
        Answer {
            state: "unknown".to_owned(),
            status_line: format!(
                "may have arrived, no answer came ({error}); look before doing it again: \
                 done again from this page, it is the same command, never a second"
            ),
            ..self.unsent("")
        }
    }

    /// The names asked about: a check's shown, a test's one.
    fn names(&self) -> Vec<String> {
        match self {
            Self::Check { names } => names.clone(),
            Self::Test { name } => vec![name.clone()],
        }
    }

    /// The place of `name`'s result of this kind in its row.
    fn slot(&self, deployment: &str, root: &str, name: &str) -> String {
        format!("{}-{}", cell_id(deployment, root, name), self.kind())
    }

    /// Sent, its answer not here yet: a check's rows marked running (a test's answer is in
    /// its row already).
    fn waiting(&self, deployment: &str, root: &str, line: &str, poll: String) -> Answer {
        let cells = match self {
            Self::Check { names } => names
                .iter()
                .map(|name| Cell {
                    id: self.slot(deployment, root, name),
                    state: "running".to_owned(),
                    label: "check: running".to_owned(),
                    details: Vec::new(),
                })
                .collect(),
            Self::Test { .. } => Vec::new(),
        };
        Answer {
            kind: self.kind(),
            what: self.what(deployment, root),
            state: match self {
                Self::Check { .. } => "pending",
                Self::Test { .. } => "running",
            }
            .to_owned(),
            status_line: line.to_owned(),
            poll,
            poll_ms: POLL_MS,
            cells,
        }
    }

    /// `command` as it stands.
    fn answer(&self, deployment: &str, root: &str, command: &CommandView, poll: String) -> Answer {
        let what = self.what(deployment, root);
        match command.status.as_str() {
            "pending" => {
                return self.waiting(deployment, root, "sent; waiting for the master", poll);
            }
            "applied" => {}
            _ => {
                return Answer {
                    kind: self.kind(),
                    what,
                    state: match self {
                        Self::Check { .. } => "rejected",
                        Self::Test { .. } => "failed",
                    }
                    .to_owned(),
                    status_line: format!(
                        "rejected: {}",
                        command.reason.clone().unwrap_or_default()
                    ),
                    poll: String::new(),
                    poll_ms: POLL_MS,
                    cells: Vec::new(),
                };
            }
        }
        let result: Option<ConnectionsResult> = command
            .result
            .clone()
            .and_then(|r| serde_json::from_value(r).ok());
        let job = result
            .as_ref()
            .and_then(|r| r.jobs.iter().find(|j| j.root == root));
        let Some(job) = job else {
            return Answer {
                kind: self.kind(),
                what,
                state: "failed".to_owned(),
                status_line: "applied, but the master sent it to no worker".to_owned(),
                poll: String::new(),
                poll_ms: POLL_MS,
                cells: Vec::new(),
            };
        };
        match job.state {
            JobState::Running => {
                let line = match self {
                    Self::Check { .. } => format!("running on worker {}", job.worker),
                    Self::Test { .. } => format!("testing on worker {}", job.worker),
                };
                self.waiting(deployment, root, &line, poll)
            }
            JobState::Lost => self.ended(deployment, root, job, Vec::new()),
            JobState::Done => {
                let cells = self.cells(deployment, root, job);
                self.ended(deployment, root, job, cells)
            }
        }
    }

    /// A job that answered (`cells`) or was lost.
    fn ended(
        &self,
        deployment: &str,
        root: &str,
        job: &ConnectionJobResult,
        mut cells: Vec<Cell>,
    ) -> Answer {
        let lost = job.state == JobState::Lost;
        let why = job.reason.clone().unwrap_or_default();
        if lost {
            cells = self
                .names()
                .iter()
                .map(|name| Cell {
                    id: self.slot(deployment, root, name),
                    state: "lost".to_owned(),
                    label: format!("{}: lost", self.kind()),
                    details: vec![why.clone()],
                })
                .collect();
        }
        let failed = cells.iter().filter(|c| c.state != "success").count();
        let status_line = if lost {
            format!("lost on worker {}: {why}", job.worker)
        } else {
            let mut line = format!("done on worker {}: {} ok", job.worker, cells.len() - failed);
            if failed > 0 {
                let _ = write!(line, ", {failed} failed");
            }
            line
        };
        Answer {
            kind: self.kind(),
            what: self.what(deployment, root),
            state: if lost || failed > 0 {
                "failed"
            } else {
                "applied"
            }
            .to_owned(),
            status_line,
            poll: String::new(),
            poll_ms: POLL_MS,
            cells,
        }
    }

    /// The results of `job`'s reports for the connections asked about (a check's shown, a
    /// test's one); one the worker didn't report (gone from the code since the page was read)
    /// failed, never passed in silence.
    fn cells(&self, deployment: &str, root: &str, job: &ConnectionJobResult) -> Vec<Cell> {
        self.names()
            .iter()
            .map(|name| {
                let id = self.slot(deployment, root, name);
                match job.connections.iter().find(|r| r.name == *name) {
                    Some(report) => Cell {
                        id,
                        state: if report.ok { "success" } else { "failed" }.to_owned(),
                        label: label(self.kind(), report),
                        details: details(report),
                    },
                    None => Cell {
                        id,
                        state: "failed".to_owned(),
                        label: format!("{}: no answer", self.kind()),
                        details: vec![
                            "the worker didn't report it: is it still in the code? Read the \
                             page again"
                                .to_owned(),
                        ],
                    },
                }
            })
            .collect()
    }
}

/// `check: ok`, `test: failed (1.2 s)`.
fn label(kind: &str, report: &ConnectionReport) -> String {
    let mut label = format!("{kind}: {}", if report.ok { "ok" } else { "failed" });
    if let Some(ms) = report.duration_ms {
        let _ = write!(label, " ({:.1} s)", Duration::from_millis(ms).as_secs_f64());
    }
    label
}

/// A report's error, then each secret that isn't plainly there; for a check that passed, how
/// many secrets were.
fn details(report: &ConnectionReport) -> Vec<String> {
    let mut lines: Vec<String> = report.error.iter().cloned().collect();
    for secret in &report.secrets {
        let state = match secret.state {
            SecretState::Present => continue,
            SecretState::Default => "not there: its default stands in",
            SecretState::Missing => "missing",
            SecretState::Denied => "denied",
            SecretState::Unavailable => "unavailable",
        };
        let mut line = format!("{}: {state}", secret.secret);
        if let Some(message) = &secret.message {
            let _ = write!(line, " ({message})");
        }
        lines.push(line);
    }
    if lines.is_empty() && !report.secrets.is_empty() {
        let count = report.secrets.len();
        lines.push(format!(
            "{count} secret{} there",
            if count == 1 { "" } else { "s" }
        ));
    }
    lines
}

fn fragment(answer: &Answer) -> Response {
    match answer.render() {
        Ok(html) => ([(header::CACHE_CONTROL, "no-store")], Html(html)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the fragment couldn't be rendered: {error}"),
        )
            .into_response(),
    }
}
