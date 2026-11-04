//! Connections (M4.7e): the page and a DAG's section, Check all and Test, their results filled
//! in as the workers answer.

use axum::http::Method;
use pretty_assertions::assert_eq;

use super::*;

const SAME: (&str, &str) = ("sec-fetch-site", "same-origin");

/// The list `GET /api/v1/connections` answers: in `etl`, `db` (used by `sync`'s `extract` and
/// `load`), `api` used by nothing, `gone` declared by `sync` and defined nowhere.
fn list() -> Value {
    json!({"roots": [{"root": "etl", "connections": [
        {"name": "api", "type": "http", "file": "conns.py", "used_by": []},
        {"name": "db", "type": "sql", "file": "conns.py",
         "used_by": [{"dag_id": "sync", "tasks": ["extract", "load"]}]},
        {"name": "gone", "refused": "connection `gone` isn't defined in the DAG folder",
         "used_by": [{"dag_id": "sync", "tasks": ["extract"]}]},
    ]}]})
}

fn with_connections() -> Scripted {
    let mut etl = etl();
    etl.answers
        .insert("/api/v1/connections".to_owned(), (200, list()));
    etl
}

fn command(id: &str, kind: &str, status: &str, result: &Value) -> Value {
    json!({
        "command_id": id, "kind": kind, "status": status, "reason": null, "result": result,
        "requested_by": "anonymous", "requested_at": "2026-10-15T01:00:00Z", "decided_at": null
    })
}

async fn post(app: &axum::Router, path: &str, headers: &[(&str, &str)]) -> (StatusCode, String) {
    let mut request = Request::builder().method(Method::POST).uri(path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn the_connections_page_lists_each_roots_with_check_all_and_a_test_per_row() {
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![
        deployment("etl", with_connections()),
        deployment("ml", down),
    ]);
    let (status, html) = get(&app, "/connections", &[]).await;
    assert_eq!(status, StatusCode::OK);
    // A heading per deployment and root; the one that doesn't answer said so.
    assert_eq!(texts(&html, "h2"), ["etl · etl"]);
    assert_eq!(texts(&html, ".banner.warning").len(), 1);
    assert!(texts(&html, ".banner.warning")[0].starts_with("Unavailable: ml: "));
    assert_eq!(
        texts(&html, "table.connections tbody td.mono:first-child"),
        ["api", "db", "gone"]
    );
    assert_eq!(
        texts(&html, "table.connections tbody td:nth-child(4)"),
        ["no DAG", "sync extract, load", "sync extract"]
    );
    assert_eq!(
        attrs(&html, "table.connections a", "href")[0],
        "/d/etl/dags/sync"
    );
    // A refused one says why, and can't be tested.
    assert_eq!(
        texts(&html, "td.connection-result .warning-text"),
        ["connection `gone` isn't defined in the DAG folder"]
    );
    assert_eq!(
        attrs(&html, "dialog form", "hx-post"),
        [
            "/d/etl/connections/api/test?root=etl",
            "/d/etl/connections/db/test?root=etl"
        ]
    );
    // A row's check and test results have places of their own (a check never replaces a
    // test still being read); a test's answer goes in its row's test place; its dialog,
    // named by its title, says it logs in for real.
    let checks = attrs(&html, "td.connection-result .connection-check", "id");
    let tests = attrs(&html, "td.connection-result .connection-test", "id");
    assert_eq!(checks.len(), 2);
    assert_eq!(tests.len(), 2);
    assert!(checks.iter().all(|id| id.ends_with("-check")), "{checks:?}");
    assert_eq!(
        tests[0],
        format!("{}-test", checks[0].trim_end_matches("-check"))
    );
    assert_eq!(
        attrs(&html, "dialog form", "hx-target"),
        [format!("#{}", tests[0]), format!("#{}", tests[1])]
    );
    assert_eq!(
        attrs(&html, "dialog", "aria-label"),
        texts(&html, "dialog h3")
    );
    assert_eq!(texts(&html, "dialog h3").len(), 2);
    assert!(texts(&html, "dialog p")[1].contains("a real login"));
    // Check all: the root's shown connections but the refused, its answer under the button,
    // under a key of its own kept until a definite answer (B140), as a dialog's form.
    assert_eq!(
        attrs(&html, "button[hx-post]", "hx-post"),
        ["/d/etl/connections/check?root=etl&names=api%2Cdb"]
    );
    assert_eq!(
        attrs(&html, "button[hx-post]", "hx-on::config-request"),
        ["event.detail.headers['Idempotency-Key'] = schedrKey(this)"]
    );
    assert!(texts(&html, "nav a").contains(&"Connections".to_owned()));
}

#[tokio::test]
async fn a_dag_page_shows_the_connections_its_tasks_declare_above_its_runs_kept_when_live() {
    let app = ui(vec![deployment("etl", with_connections())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        texts(&html, "table.connections tbody td.mono:first-child"),
        ["db", "gone"]
    );
    assert_eq!(
        attrs(&html, "button[hx-post]", "hx-post"),
        ["/d/etl/connections/check?root=etl&names=db"]
    );
    // Above the runs, in the live part: its rows read again with it (the code may have
    // changed), its results kept as they are (by their IDs).
    let headings = texts(&html, "#live h2");
    assert_eq!(headings[0], "Connections");
    assert!(headings[1].starts_with("Runs"), "{headings:?}");
    assert_eq!(
        attrs(&html, "#live .connection-group", "hx-preserve"),
        Vec::<String>::new()
    );
    assert_eq!(
        attrs(&html, "#live [hx-preserve]", "class"),
        ["results", "connection-check", "connection-test"]
    );
    // A webserver without the list (an older one) shows no section.
    let app = ui(vec![deployment("etl", etl())]);
    let (status, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "table.connections"), Vec::<String>::new());
    assert!(!texts(&html, "h2").contains(&"Connections".to_owned()));
}

/// A check of `etl`, running at first, then done: `api` ok, `db` failed, `other` (not on the
/// page) ok.
fn checking() -> Scripted {
    let mut etl = with_connections();
    etl.answers.insert(
        "/api/v1/connections/check".to_owned(),
        (
            202,
            command(
                "c1",
                "check_connections",
                "applied",
                &json!({"jobs": [{"root": "etl", "worker": "w1", "state": "running"}]}),
            ),
        ),
    );
    etl.answers.insert(
        "/api/v1/commands/c1".to_owned(),
        (
            200,
            command(
                "c1",
                "check_connections",
                "applied",
                &json!({"jobs": [{"root": "etl", "worker": "w1", "state": "done", "connections": [
                    {"name": "api", "ok": true, "secrets": [
                        {"secret": "secrets['etl/api']['token']", "state": "present"}]},
                    {"name": "db", "ok": false, "secrets": [
                        {"secret": "secrets['etl/db']['password']", "state": "missing"}],
                     "error": "a secret it names is missing"},
                    {"name": "other", "ok": true},
                ]}]}),
            ),
        ),
    );
    etl
}

#[tokio::test]
async fn check_all_shows_each_connections_result_in_its_row_once_the_worker_answers() {
    let etl = checking();
    let sent = etl.sent.clone();
    let app = ui(vec![deployment("etl", etl)]);
    let (status, html) = post(
        &app,
        "/d/etl/connections/check?root=etl&names=api%2Cdb",
        &[SAME],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = sent
        .lock()
        .unwrap()
        .iter()
        .find(|(path, _)| path == "/api/v1/connections/check")
        .and_then(|(_, body)| body.clone());
    assert_eq!(body, Some(json!({"root": "etl"})));
    assert_eq!(
        texts(&html, ".command"),
        ["Check all of etl · etl: running on worker w1"]
    );
    // Each row marked running meanwhile; read again a second after a read that failed.
    assert_eq!(
        texts(&html, "[hx-swap-oob] .state"),
        ["check: running", "check: running"]
    );
    assert_eq!(
        attrs(&html, ".command[data-poll]", "hx-trigger"),
        ["load delay:1000ms, retry delay:1s"]
    );
    let poll = attrs(&html, ".command", "hx-get");
    assert_eq!(
        poll,
        ["/d/etl/connections/commands/c1?kind=check&root=etl&names=api%2Cdb"]
    );
    let (_, html) = get(&app, &poll[0], &[]).await;
    assert_eq!(
        texts(&html, ".command"),
        ["Check all of etl · etl: done on worker w1: 1 ok, 1 failed"]
    );
    assert_eq!(attrs(&html, ".command", "class"), ["command failed"]);
    assert_eq!(attrs(&html, ".command", "hx-get"), Vec::<String>::new());
    // Each shown connection's result, for its row's cell (not `other`: not on the page).
    let page = get(
        &ui(vec![deployment("etl", with_connections())]),
        "/connections",
        &[],
    )
    .await
    .1;
    let cells = attrs(&page, "td.connection-result .connection-check", "id");
    assert_eq!(
        attrs(&html, "[hx-swap-oob]", "hx-swap-oob"),
        [
            format!("innerHTML:#{}", cells[0]),
            format!("innerHTML:#{}", cells[1])
        ]
    );
    assert_eq!(
        texts(&html, "[hx-swap-oob] .state"),
        ["check: ok", "check: failed"]
    );
    assert_eq!(
        texts(&html, "[hx-swap-oob] div"),
        [
            "1 secret there",
            "a secret it names is missing",
            "secrets['etl/db']['password']: missing"
        ]
    );
}

#[tokio::test]
async fn a_connection_asked_about_that_the_worker_didnt_report_failed() {
    // Gone from the code since the page was read: failed, never passed in silence.
    let app = ui(vec![deployment("etl", checking())]);
    let (_, html) = get(
        &app,
        "/d/etl/connections/commands/c1?kind=check&root=etl&names=api%2Cdb%2Cold",
        &[],
    )
    .await;
    assert_eq!(
        texts(&html, ".command"),
        ["Check all of etl · etl: done on worker w1: 1 ok, 2 failed"]
    );
    assert_eq!(
        texts(&html, "[hx-swap-oob] .state"),
        ["check: ok", "check: failed", "check: no answer"]
    );
}

#[tokio::test]
async fn a_test_fills_its_rows_cell_with_how_it_went_or_why_it_was_lost() {
    let mut etl = with_connections();
    etl.answers.insert(
        "/api/v1/connections/db/test".to_owned(),
        (
            202,
            command("c2", "test_connection", "pending", &json!(null)),
        ),
    );
    etl.answers.insert(
        "/api/v1/commands/c2".to_owned(),
        (
            200,
            command(
                "c2",
                "test_connection",
                "applied",
                &json!({"jobs": [{"root": "etl", "worker": "w1", "state": "done", "connections": [
                    {"name": "db", "ok": false, "duration_ms": 1234,
                     "error": "OperationalError: password authentication failed for user ****"}
                ]}]}),
            ),
        ),
    );
    let sent = etl.sent.clone();
    let app = ui(vec![deployment("etl", etl)]);
    let (status, html) = post(&app, "/d/etl/connections/db/test?root=etl", &[SAME]).await;
    assert_eq!(status, StatusCode::OK);
    let body = sent
        .lock()
        .unwrap()
        .iter()
        .find(|(path, _)| path == "/api/v1/connections/db/test")
        .and_then(|(_, body)| body.clone());
    assert_eq!(body, Some(json!({"root": "etl"})));
    assert_eq!(texts(&html, ".state"), ["sent; waiting for the master"]);
    let poll = attrs(&html, ".connection-answer", "hx-get");
    assert_eq!(
        poll,
        ["/d/etl/connections/commands/c2?kind=test&root=etl&names=db"]
    );
    let (_, html) = get(&app, &poll[0], &[]).await;
    assert_eq!(texts(&html, ".state"), ["test: failed (1.2 s)"]);
    assert_eq!(
        texts(&html, ".warning-text"),
        ["OperationalError: password authentication failed for user ****"]
    );
    assert_eq!(
        attrs(&html, ".connection-answer", "hx-get"),
        Vec::<String>::new()
    );

    // Lost: its worker went away before answering.
    let mut etl = with_connections();
    etl.answers.insert(
        "/api/v1/commands/c3".to_owned(),
        (
            200,
            command(
                "c3",
                "test_connection",
                "applied",
                &json!({"jobs": [{"root": "etl", "worker": "w1", "state": "lost",
                    "reason": "worker `w1` restarted before answering"}]}),
            ),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(
        &app,
        "/d/etl/connections/commands/c3?kind=test&root=etl&names=db",
        &[],
    )
    .await;
    assert_eq!(texts(&html, ".state"), ["test: lost"]);
    assert_eq!(
        texts(&html, ".warning-text"),
        ["worker `w1` restarted before answering"]
    );
}

#[tokio::test]
async fn a_refused_check_or_test_says_why_and_another_sites_post_is_refused() {
    let mut etl = with_connections();
    etl.answers.insert(
        "/api/v1/connections/db/test".to_owned(),
        (
            400,
            json!({"error": "connection `db` isn't in root `etl`'s current code"}),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = post(&app, "/d/etl/connections/db/test?root=etl", &[SAME]).await;
    assert!(
        texts(&html, ".state")[0].starts_with("not sent: "),
        "{html}"
    );
    assert!(texts(&html, ".state")[0].contains("isn't in root `etl`'s current code"));
    let (status, _) = post(
        &app,
        "/d/etl/connections/db/test?root=etl",
        &[("sec-fetch-site", "cross-site")],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // No answer from the webserver: it may have arrived, never "not sent".
    let mut etl = with_connections();
    etl.answers.insert(
        "/api/v1/connections/db/test".to_owned(),
        (502, json!({"error": "bad gateway"})),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = post(&app, "/d/etl/connections/db/test?root=etl", &[SAME]).await;
    let state = texts(&html, ".state.unknown");
    assert_eq!(state.len(), 1, "{html}");
    assert!(state[0].starts_with("may have arrived"), "{state:?}");

    // A test the worker answered without its report: failed.
    let mut etl = with_connections();
    etl.answers.insert(
        "/api/v1/commands/c4".to_owned(),
        (
            200,
            command(
                "c4",
                "test_connection",
                "applied",
                &json!({"jobs": [{"root": "etl", "worker": "w1", "state": "done", "connections": []}]}),
            ),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(
        &app,
        "/d/etl/connections/commands/c4?kind=test&root=etl&names=db",
        &[],
    )
    .await;
    assert_eq!(texts(&html, ".state.failed"), ["test: no answer"]);
}
