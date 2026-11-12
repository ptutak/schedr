//! The Deployments page (M4.4b, decisions 128-129), and the Workers page's Master column.

use pretty_assertions::assert_eq;

use super::*;

use std::time::Duration;

use crate::deployments::{Asked, Level, UiSide, graph, pretty};

const LEADER: &str = "0190a0a0-0000-7000-8000-00000000000a";
const STANDBY: &str = "0190a0a0-0000-7000-8000-00000000000b";

fn master(id: &str, role: &str, process: &str, config: &Value) -> Value {
    json!({
        "master_id": id, "role": role, "process_id": process, "host": format!("host-{process}"),
        "schedr_version": "1.0.0", "config": config,
        "started_at": "2026-10-15T01:00:00Z", "heard_at": "2026-10-15T01:00:05Z"
    })
}

fn worker(id: &str, state: &str, master: Option<&str>, process: Option<&str>) -> Value {
    json!({
        "worker_id": id, "state": state, "schedr_version": "1.0.0", "master_id": master,
        "process_id": process, "config": process.map(|_| json!({"worker": {"slots": 2}}))
    })
}

/// A distributed deployment: a leader and a standby, two workers (one from an older agent,
/// which sent no process or configuration).
fn distributed() -> Value {
    let secret = json!({"database": {"url": "postgres://schedr:****@db:5432/schedr"}});
    json!({
        "webserver": {"process_id": "p-web", "host": "web-0", "schedr_version": "1.0.0",
                      "config": {"webserver": {"port": 8080}}},
        "masters": [master(LEADER, "leader", "p-m1", &secret),
                    master(STANDBY, "standby", "p-m2", &secret)],
        "workers": [worker("w1", "active", Some(LEADER), Some("p-w1")),
                    worker("w2", "disconnected", None, None)],
        "user": "anna"
    })
}

fn with_deployment(view: Value) -> Scripted {
    let mut etl = etl();
    etl.answers
        .insert("/api/v1/deployment".to_owned(), (200, view));
    etl
}

fn asked(name: &str, view: Value) -> Asked {
    Asked {
        name: name.to_owned(),
        proof: "token",
        answer: Ok(serde_json::from_value(view).unwrap()),
        took: Duration::from_millis(3),
    }
}

fn side(process_id: &str) -> UiSide {
    UiSide {
        process_id: process_id.to_owned(),
        host: "ui-host".to_owned(),
        version: "1.0.0".to_owned(),
        user: "anna".to_owned(),
        config: json!({"ui": {"port": 8082, "docs_dir": null}}),
    }
}

fn standalone() -> Value {
    json!({
        "webserver": {"process_id": "p", "host": "h", "schedr_version": "1.0.0", "config": {}},
        "masters": [master(LEADER, "leader", "p", &json!({}))],
        "workers": [worker("standalone", "active", Some(LEADER), Some("p"))],
        "user": "anonymous"
    })
}

#[test]
fn in_standalone_the_ui_is_drawn_inside_the_one_process_with_no_lines_in_it() {
    let (graph, panels) = graph(&side("p"), &[asked("default", standalone())]);
    assert_eq!(graph.processes.len(), 1);
    assert_eq!(graph.frames.len(), 1);
    assert_eq!(graph.frames[0].title, "default");
    assert_eq!(
        graph
            .nodes
            .iter()
            .map(|n| (n.kind, n.anchor.as_str()))
            .collect::<Vec<_>>(),
        [
            ("ui", "node.ui"),
            ("webserver", "node.default.webserver"),
            ("master", &*format!("node.default.master.{LEADER}")),
            ("worker", "node.default.worker.standalone"),
        ]
    );
    // Stacked inside their process's rectangle, inside the deployment's frame.
    let (p, f) = (&graph.processes[0], &graph.frames[0]);
    assert!(graph.nodes.windows(2).all(|n| n[1].y > n[0].y));
    assert!(graph.nodes.iter().all(|n| n.x > p.x
        && n.y > p.y
        && n.y + crate::deployments::NODE_HEIGHT <= p.y + p.height));
    assert!(p.x > f.x && p.y > f.y && p.y + p.height <= f.y + f.height);
    // All in one process: no line.
    assert_eq!(graph.edges, []);
    assert_eq!(panels[0].anchor, "node.ui");
}

#[test]
fn a_distributed_deployment_is_drawn_in_rows_linked_by_http_database_and_grpc() {
    let mut view = distributed();
    view["workers"]
        .as_array_mut()
        .unwrap()
        .push(worker("w3", "lost", Some(STANDBY), Some("p-w3")));
    let (graph, _) = graph(&side("p-ui"), &[asked("etl", view)]);
    // The UI's own process on top, then the webserver's, the masters', the workers'.
    assert_eq!(
        graph
            .processes
            .iter()
            .map(|p| p.title.as_str())
            .collect::<Vec<_>>(),
        [
            "ui-host · p-ui",
            "web-0 · p-web",
            "host-p-m1 · p-m1",
            "host-p-m2 · p-m2",
            "process · p-w1",
            "process",
            "process · p-w3"
        ]
    );
    let y = |i: usize| graph.processes[i].y;
    assert!(y(0) < graph.frames[0].y);
    assert!(y(1) < y(2) && y(2) == y(3) && y(3) < y(4) && y(4) == y(5) && y(5) == y(6));
    assert_eq!(
        graph
            .edges
            .iter()
            .map(|e| (e.kind, e.dashed))
            .collect::<Vec<_>>(),
        [
            ("database", false),
            ("database", false),
            ("grpc", false),
            ("grpc", true),
            ("http", false)
        ]
    );
    assert!(
        graph.edges[3]
            .title
            .contains("w3 isn't connected to master 0000000b")
    );
    // Wide and tall enough for all of it.
    assert!(
        graph
            .processes
            .iter()
            .all(|p| p.x + p.width <= graph.width && p.y + p.height <= graph.height)
    );
}

#[test]
fn an_unreachable_deployment_is_a_red_webserver_and_the_ui_says_so() {
    let down = Asked {
        name: "ml".to_owned(),
        proof: "mTLS",
        answer: Err("connection refused".to_owned()),
        took: Duration::from_millis(5),
    };
    let mut newer = distributed();
    newer["webserver"]["schedr_version"] = json!("9.9.9");
    let (graph, panels) = graph(&side(""), &[asked("etl", newer), down]);
    let ml = graph
        .nodes
        .iter()
        .find(|n| n.anchor == "node.ml.webserver")
        .unwrap();
    assert_eq!(ml.classes(), "box unreachable error");
    let ui = &graph.nodes[0];
    assert_eq!((ui.kind, ui.level), ("ui", Level::Error));
    assert_eq!(
        panels[0]
            .problems
            .iter()
            .map(|p| (p.error, p.message.as_str()))
            .collect::<Vec<_>>(),
        [
            (
                false,
                "`etl`'s webserver is version 9.9.9; this UI is 1.0.0"
            ),
            (true, "can't reach `ml`'s webserver: connection refused")
        ]
    );
    let line = graph
        .edges
        .iter()
        .find(|e| e.title.contains("`ml`"))
        .unwrap();
    assert!(line.dashed);
    // Its frames side by side.
    assert!(graph.frames[1].x >= graph.frames[0].x + graph.frames[0].width);
}

#[test]
fn a_configuration_is_shown_as_toml_without_its_unset_values() {
    let config = json!({
        "ui": {"port": 8082, "docs_dir": null, "deployments": [{"name": "etl", "token_file": null}]},
        "log_level": "info",
        "database": {"url": "postgres://schedr:hunter2@db/schedr"}
    });
    let shown = pretty(&config);
    assert!(
        !shown.contains("null") && !shown.contains("docs_dir") && !shown.contains("token_file"),
        "{shown}"
    );
    assert!(!shown.contains("hunter2"), "{shown}");
    assert!(shown.contains("log_level = \"info\""), "{shown}");
    assert!(shown.contains("[ui]\nport = 8082"), "{shown}");
    assert!(
        shown.contains("[[ui.deployments]]\nname = \"etl\""),
        "{shown}"
    );
    // Not a table: JSON.
    assert_eq!(pretty(&json!([1, null])), "[\n  1\n]");
}

/// A UI with `etl` (a distributed deployment whose nodes have problems) and `ml` (down).
fn two_deployments() -> axum::Router {
    let mut view = distributed();
    view["workers"][0]["queues"] = json!(["default", "large"]);
    view["workers"][1]["problems"] = json!([{"error": false, "message": "disconnected: lost unless it comes back within its lease"}]);
    view["masters"][1]["problems"] =
        json!([{"error": true, "message": "a round failed: the database is gone"}]);
    ui(vec![
        deployment("etl", with_deployment(view)),
        deployment(
            "ml",
            Scripted {
                down: true,
                ..Scripted::default()
            },
        ),
    ])
}

#[tokio::test]
async fn the_deployments_page_draws_everything_with_each_nodes_panel() {
    let app = two_deployments();
    let (status, html) = get(&app, "/deployments", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let anchors = attrs(&html, "svg.deployment a.node", "href");
    assert_eq!(
        anchors,
        [
            "#node.ui".to_owned(),
            "#node.etl.webserver".to_owned(),
            format!("#node.etl.master.{LEADER}"),
            format!("#node.etl.master.{STANDBY}"),
            "#node.etl.worker.w1".to_owned(),
            "#node.etl.worker.w2".to_owned(),
            "#node.ml.webserver".to_owned(),
        ]
    );
    assert_eq!(
        attrs(&html, "svg.deployment rect.box", "class"),
        [
            "box error",
            "box",
            "box leader",
            "box standby error",
            "box active",
            "box disconnected warning",
            "box unreachable error",
        ]
    );
    assert_eq!(
        texts(&html, "svg.deployment text.mark"),
        ["×", "×", "!", "×"]
    );
    assert_eq!(
        texts(&html, "svg.deployment text.frame-label"),
        ["etl", "ml"]
    );
    assert!(texts(&html, "svg.deployment text.sub").contains(&"queues: default, large".to_owned()));
    // Each node's panel, shown when its node is clicked (the URL's target).
    assert_eq!(
        attrs(&html, "section.node-config", "id"),
        [
            "node.ui",
            "node.etl.webserver",
            &*format!("node.etl.master.{LEADER}"),
            &*format!("node.etl.master.{STANDBY}"),
            "node.etl.worker.w1",
            "node.etl.worker.w2",
            "node.ml.webserver",
        ]
    );
    let problems = texts(&html, "section.node-config ul.problems li");
    assert!(
        problems.contains(&"Error: a round failed: the database is gone".to_owned()),
        "{problems:?}"
    );
    assert!(
        problems.contains(
            &"Warning: disconnected: lost unless it comes back within its lease".to_owned()
        )
    );
    let configs = texts(&html, "section.node-config pre.config");
    assert!(
        configs
            .iter()
            .any(|c| c.contains("url = \"postgres://schedr:****@db:5432/schedr\"")),
        "{configs:?}"
    );
    assert!(
        configs
            .iter()
            .any(|c| c.contains("[webserver]\nport = 8080")),
        "{configs:?}"
    );
    assert_eq!(
        texts(&html, "section.node-config .empty"),
        ["It sent no configuration (an older agent)."]
    );
}

#[tokio::test]
async fn the_ui_and_webserver_panels_say_how_the_ui_reaches_each_deployment() {
    let app = two_deployments();
    let (_, html) = get(&app, "/deployments", &[]).await;
    // The UI's panel: who is signed in, its version, its connections.
    let ui_facts = texts(&html, "section[id='node.ui'] dl.facts dd");
    assert_eq!(ui_facts[..2], ["anonymous", env!("CARGO_PKG_VERSION")]);
    let etl = texts(
        &html,
        "section[id='node.ui'] table.connections tbody tr:first-child td",
    );
    assert_eq!(etl[0], "etl");
    assert_eq!(etl[3], "none");
    assert_eq!(etl[4], "anna");
    assert!(etl[2].ends_with(" ms"));
    assert_eq!(
        attrs(&html, "section[id='node.ui'] table.connections a", "href"),
        ["#node.etl.webserver", "#node.ml.webserver"]
    );
    assert!(
        texts(&html, "section[id='node.ui'] table.connections .error")[0]
            .starts_with("Unreachable: ")
    );
    // The webserver's: who it took the UI's requests as.
    let web: Vec<String> = texts(&html, "section[id='node.etl.webserver'] dl.facts dd");
    assert!(web.contains(&"anna".to_owned()), "{web:?}");
    // Live with the workers, and read again every 15 s for the masters, whose heartbeats
    // send no event (a takeover, a new or a gone master, their problems).
    assert_eq!(attrs(&html, "#live", "hx-get"), ["/deployments"]);
    assert_eq!(
        attrs(&html, "#live", "hx-trigger"),
        ["live:change, every 15s"]
    );
    // An open panel stays open across a refresh, the page not moved (marked again, not
    // navigated to; the browser test checks it).
    assert!(html.contains("document.addEventListener(\"htmx:afterSettle\", show)"));
    assert!(!html.contains("location.replace"));
    // In the navigation, in its place; the Config page and each deployment's page are gone.
    assert_eq!(
        texts(&html, "header nav a")[..6],
        [
            "DAGs",
            "Runs",
            "Deployments",
            "Workers",
            "Connections",
            "Import errors"
        ]
    );
    for gone in ["/config", "/deployments/etl"] {
        let (status, _) = get(&app, gone, &[]).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{gone}");
    }
}

#[tokio::test]
async fn no_secret_a_node_sent_in_clear_is_shown() {
    // The user's requirement (2026-10-31): a secret, a database URL's password first, is never
    // shown in clear. Each node masks its own; one that didn't (an older or altered node) is
    // masked here again.
    let mut view = distributed();
    view["masters"][0]["config"] = json!({"database": {"url": "postgres://schedr:hunter2@db:5432/schedr?sslpassword=hunter2"}});
    view["workers"][0]["config"] = json!({"worker": {"api_token": "hunter2"}});
    view["webserver"]["config"] = json!({"webserver": {"secret": "hunter2"}});
    let app = router(Ui {
        deployments: vec![deployment("etl", with_deployment(view))],
        docs_dir: None,
        timings: crate::live::UiTimings::default(),
        auth: schedr_api::auth::Auth::default(),
        config: json!({"ui": {"deployments": [{"url": "https://u:hunter2@etl:8443"}]}}),
        process_id: String::new(),
        host: String::new(),
    });
    let (status, html) = get(&app, "/deployments", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.contains("hunter2"), "{html}");
    assert!(html.contains("postgres://schedr:****@db:5432/schedr"));
}

#[tokio::test]
async fn the_workers_page_shows_each_ones_master_and_the_dialogs_name_them() {
    let mut etl = etl();
    let mut w1 = super::actions::worker("w1", "active", false, &json!([]));
    w1["master_id"] = json!(LEADER);
    let mut w2 = super::actions::worker("w2", "disconnected", false, &json!([]));
    w2["master_id"] = json!(STANDBY);
    let w3 = super::actions::worker("w3", "active", false, &json!([]));
    etl.answers.insert(
        "/api/v1/workers".to_owned(),
        (200, json!({"workers": [w1, w2, w3]})),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(&app, "/workers", &[]).await;
    assert_eq!(
        attrs(&html, "table.workers td a", "href"),
        [
            format!("/deployments#node.etl.master.{LEADER}"),
            format!("/deployments#node.etl.master.{STANDBY}"),
        ]
    );
    assert_eq!(texts(&html, "table.workers td a"), ["0000000a", "0000000b"]);
    // The last one a disconnected worker was registered with: greyed.
    assert_eq!(attrs(&html, "table.workers td a.stale", "href").len(), 1);
    // Drain names the deployment and the master, and what holds across a takeover.
    let dialogs = texts(&html, "dialog p");
    assert!(
        dialogs[0].contains("Deployment etl's leading master applies it (the worker is registered with master 0000000a)"),
        "{dialogs:?}"
    );
    assert!(dialogs[0].contains("a takeover by another master"));
    assert_eq!(texts(&html, "dialog h3")[0], "Drain worker w1 of etl?");
}

#[test]
fn masters_made_together_get_different_short_ids() {
    // UUIDv7s begin with their time: two masters started within a minute share 8 characters.
    assert_eq!(&LEADER[..8], &STANDBY[..8]);
    assert_ne!(
        crate::deployments::short(LEADER),
        crate::deployments::short(STANDBY)
    );
    let a = uuid::Uuid::now_v7().to_string();
    let b = uuid::Uuid::now_v7().to_string();
    assert_ne!(crate::deployments::short(&a), crate::deployments::short(&b));
}

#[test]
fn no_two_nodes_share_an_anchor_whatever_their_names() {
    // Codex's review of the Deployments batch: `prod`'s worker `a-webserver` and
    // `prod-worker-a`'s webserver were both `node-prod-worker-a-webserver`.
    let mut prod = distributed();
    prod["workers"][0]["worker_id"] = json!("a-webserver");
    prod["workers"][1]["worker_id"] = json!("a.b/c");
    let (graph, panels) = graph(
        &side(""),
        &[asked("prod", prod), asked("prod-worker-a", distributed())],
    );
    let anchors: std::collections::BTreeSet<&str> =
        panels.iter().map(|p| p.anchor.as_str()).collect();
    assert_eq!(anchors.len(), panels.len(), "{anchors:?}");
    assert!(anchors.contains("node.prod.worker.a-webserver"));
    assert!(anchors.contains("node.prod-worker-a.webserver"));
    assert!(anchors.contains("node.prod.worker.a%2Eb%2Fc"));
    // Each line found both of its ends: 2 HTTP, 4 database, 2 gRPC.
    assert_eq!(graph.edges.len(), 8);
}

#[tokio::test]
async fn a_problem_an_older_webserver_sent_in_clear_is_masked() {
    let mut view = distributed();
    view["masters"][0]["problems"] =
        json!([{"error": true, "message": "a round failed: postgres://u:hunter2@db/s"}]);
    let app = ui(vec![deployment("etl", with_deployment(view))]);
    let (_, html) = get(&app, "/deployments", &[]).await;
    assert!(!html.contains("hunter2"), "{html}");
    assert!(html.contains("a round failed: postgres://u:****@db/s"));
}
