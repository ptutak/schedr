//! The Deployments and Config pages (M4.4b), and the Workers page's Master column.

use pretty_assertions::assert_eq;
use schedr_api::views::DeploymentView;

use super::*;
use crate::deployments::layout;

const LEADER: &str = "0190a0a0-0000-7000-8000-00000000000a";
const STANDBY: &str = "0190a0a0-0000-7000-8000-00000000000b";

fn master(id: &str, role: &str, process: &str, config: &Value) -> Value {
    json!({
        "master_id": id, "role": role, "process_id": process, "host": format!("host-{process}"),
        "schedr_version": "0.1.0", "config": config,
        "started_at": "2026-10-15T01:00:00Z", "heard_at": "2026-10-15T01:00:05Z"
    })
}

fn worker(id: &str, state: &str, master: Option<&str>, process: Option<&str>) -> Value {
    json!({
        "worker_id": id, "state": state, "schedr_version": "0.1.0", "master_id": master,
        "process_id": process, "config": process.map(|_| json!({"worker": {"slots": 2}}))
    })
}

/// A distributed deployment: a leader and a standby, two workers (one from an older agent,
/// which sent no process or configuration).
fn distributed() -> Value {
    let secret = json!({"database": {"url": "postgres://schedr:****@db:5432/schedr"}});
    json!({
        "webserver": {"process_id": "p-web", "host": "web-0", "schedr_version": "0.1.0",
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

#[test]
fn a_standalone_process_holds_its_master_and_worker_and_others_one_node_each() {
    let standalone: DeploymentView = serde_json::from_value(json!({
        "webserver": {"process_id": "p", "host": "h", "schedr_version": "0.1.0", "config": {}},
        "masters": [master(LEADER, "leader", "p", &json!({}))],
        "workers": [worker("standalone", "active", Some(LEADER), Some("p"))],
        "user": "anonymous"
    }))
    .unwrap();
    let processes = layout(&standalone);
    assert_eq!(processes.len(), 1);
    assert_eq!(
        processes[0]
            .nodes
            .iter()
            .map(|n| (n.kind, n.state.as_str()))
            .collect::<Vec<_>>(),
        [("master", "leader"), ("worker", "active")]
    );
    // Stacked inside their process's rectangle.
    let (p, nodes) = (&processes[0], &processes[0].nodes);
    assert!(nodes[1].y > nodes[0].y);
    assert!(nodes.iter().all(|n| n.x > p.x && n.y > p.y));
    assert!(nodes[1].y + crate::deployments::NODE_HEIGHT <= p.y + p.height);

    let distributed: DeploymentView = serde_json::from_value(distributed()).unwrap();
    let processes = layout(&distributed);
    assert_eq!(
        processes.iter().map(|p| p.nodes.len()).collect::<Vec<_>>(),
        [1, 1, 1, 1]
    );
    assert_eq!(processes[0].title, "host-p-m1 · p-m1");
    // Side by side, apart.
    assert!(processes[1].x >= processes[0].x + crate::deployments::PROCESS_WIDTH);
}

#[tokio::test]
async fn the_deployments_page_lists_each_with_its_leader_and_workers() {
    let app = ui(vec![
        deployment("etl", with_deployment(distributed())),
        deployment(
            "ml",
            Scripted {
                down: true,
                ..Scripted::default()
            },
        ),
    ]);
    let (status, html) = get(&app, "/deployments", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        attrs(&html, "table.deployments a", "href"),
        ["/deployments/etl", "/deployments/ml"]
    );
    let etl: Vec<String> = texts(&html, "table.deployments tbody tr:first-child td");
    assert_eq!(etl, ["etl", "host-p-m1", "2", "1 / 2", "0.1.0"]);
    assert!(
        texts(&html, "table.deployments .error")[0].starts_with("Unavailable: "),
        "{html}"
    );
    // In the navigation, in its place.
    assert_eq!(
        texts(&html, "header nav a")[..6],
        [
            "DAGs",
            "Runs",
            "Deployments",
            "Config",
            "Workers",
            "Import errors"
        ]
    );
}

#[tokio::test]
async fn a_deployments_page_draws_its_nodes_and_shows_each_ones_configuration() {
    let app = ui(vec![deployment("etl", with_deployment(distributed()))]);
    let (status, html) = get(&app, "/deployments/etl", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "svg.deployment .process").len(), 4);
    let anchors = attrs(&html, "svg.deployment a.node", "href");
    assert_eq!(
        anchors,
        [
            format!("#node-master-{LEADER}"),
            format!("#node-master-{STANDBY}"),
            "#node-worker-w1".to_owned(),
            "#node-worker-w2".to_owned(),
        ]
    );
    assert_eq!(
        attrs(&html, "svg.deployment rect.box", "class"),
        [
            "box leader",
            "box standby",
            "box active",
            "box disconnected"
        ]
    );
    // Each node's panel, shown when its node is clicked (the URL's target).
    assert_eq!(
        attrs(&html, "section.node-config", "id"),
        anchors
            .iter()
            .map(|a| a.trim_start_matches('#').to_owned())
            .collect::<Vec<_>>()
    );
    let configs = texts(&html, "section.node-config pre.config");
    assert!(configs[0].contains("postgres://schedr:****@db:5432/schedr"));
    assert_eq!(
        texts(&html, "section.node-config .empty"),
        ["It sent no configuration (an older agent)."]
    );
    // The webserver's own, and who it took the requests as.
    assert!(texts(&html, "details pre.config")[0].contains("\"port\": 8080"));
    assert!(texts(&html, "p.dim span.mono").contains(&"anna".to_owned()));
    // Live with the workers, and read again every 15 s for the masters, whose heartbeats
    // send no event (a takeover, a new or a gone master).
    assert_eq!(attrs(&html, "#live", "hx-get"), ["/deployments/etl"]);
    assert_eq!(
        attrs(&html, "#live", "hx-trigger"),
        ["sse:change, every 15s"]
    );
    // An open panel stays open across a refresh (`:target` set on the new one).
    assert!(html.contains("location.replace(hash)"));

    let (status, _) = get(&app, "/deployments/nowhere", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
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
    let mut app_ui = with_deployment(view);
    app_ui
        .answers
        .insert("/api/v1/workers".to_owned(), (200, json!({"workers": []})));
    let app = router(Ui {
        deployments: vec![deployment("etl", app_ui)],
        docs_dir: None,
        auth: schedr_api::auth::Auth::default(),
        config: json!({"ui": {"deployments": [{"url": "https://u:hunter2@etl:8443"}]}}),
    });
    for path in ["/deployments/etl", "/config", "/deployments"] {
        let (status, html) = get(&app, path, &[]).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(!html.contains("hunter2"), "{path}: {html}");
    }
    let (_, html) = get(&app, "/deployments/etl", &[]).await;
    assert!(html.contains("postgres://schedr:****@db:5432/schedr"));
}

#[tokio::test]
async fn the_config_page_shows_the_uis_side_and_each_connection() {
    let mut newer = distributed();
    newer["webserver"]["schedr_version"] = json!("9.9.9");
    let app = router(Ui {
        deployments: vec![
            Deployment {
                proof: "token and mTLS",
                ..deployment("etl", with_deployment(distributed()))
            },
            deployment("ml", with_deployment(newer)),
            deployment(
                "down",
                Scripted {
                    down: true,
                    ..Scripted::default()
                },
            ),
        ],
        docs_dir: None,
        auth: schedr_api::auth::Auth::default(),
        config: json!({"ui": {"port": 8082}}),
    });
    let (status, html) = get(&app, "/config", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "dl.facts dd"),
        ["anonymous", env!("CARGO_PKG_VERSION")]
    );
    let rows: Vec<String> = texts(&html, "table.connections tbody tr");
    assert_eq!(rows.len(), 3);
    let etl = texts(&html, "table.connections tbody tr:first-child td");
    assert_eq!(etl[0], "etl");
    assert_eq!(etl[3], "token and mTLS");
    assert_eq!(etl[4], "anna");
    assert!(etl[2].ends_with(" ms"));
    // A version that differs from the UI's is marked.
    assert_eq!(
        texts(&html, "table.connections .badge.warning"),
        ["differs"]
    );
    assert!(texts(&html, "table.connections .error")[0].starts_with("Unreachable: "));
    assert!(texts(&html, "pre.config")[0].contains("8082"));
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
            format!("/deployments/etl#node-master-{LEADER}"),
            format!("/deployments/etl#node-master-{STANDBY}"),
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
