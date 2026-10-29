//! The run pages: the merged runs list, a run with its graph, and a task's panel.

use pretty_assertions::assert_eq;

use super::*;
use crate::runs::OUTPUT_TAIL;

/// A run of `dag` triggered `seconds` after 01:00 on 2026-10-15.
fn run_at(id: &str, dag: &str, state: &str, seconds: u32) -> Value {
    json!({
        "run_id": id, "dag_id": dag, "kind": "scheduled", "state": state,
        "planned_for": "2026-10-15T01:00:00Z", "since": "2026-10-14T01:00:00Z",
        "triggered_at": format!("2026-10-15T01:{:02}:{:02}Z", seconds / 60, seconds % 60),
        "triggered_by": "scheduler", "started_at": null, "ended_at": null
    })
}

fn scripted(answers: &[(&str, u16, Value)]) -> Scripted {
    Scripted {
        answers: answers
            .iter()
            .map(|(path, status, body)| ((*path).to_owned(), (*status, body.clone())))
            .collect(),
        down: false,
    }
}

#[tokio::test]
async fn the_runs_of_every_deployment_are_merged_newest_first() {
    let etl = scripted(&[(
        "/api/v1/runs?limit=50",
        200,
        json!({"runs": [run_at("e2", "sync", "running", 30), run_at("e1", "sync", "success", 10)]}),
    )]);
    let ml = scripted(&[(
        "/api/v1/runs?limit=50",
        200,
        json!({"runs": [run_at("m1", "train", "failed", 20)]}),
    )]);
    let app = ui(vec![deployment("etl", etl), deployment("ml", ml)]);
    let (status, html) = get(&app, "/runs", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "table.runs tbody td:first-child a"),
        ["e2", "m1", "e1"]
    );
    assert_eq!(
        attrs(&html, "table.runs tbody td:first-child a", "href"),
        ["/d/etl/runs/e2", "/d/ml/runs/m1", "/d/etl/runs/e1"]
    );
    assert!(texts(&html, "table.runs th").contains(&"Deployment".to_owned()));
    assert_eq!(
        texts(&html, "table.runs .state"),
        ["running", "failed", "success"]
    );
    // Fewer than a page from each: no next page.
    assert_eq!(texts(&html, ".pager"), Vec::<String>::new());
}

#[tokio::test]
async fn a_full_page_links_the_next_with_the_last_runs_moment_and_id() {
    let runs: Vec<Value> = (0..50)
        .map(|i| run_at(&format!("r{:02}", 99 - i), "sync", "success", 3000 - i))
        .collect();
    let next = "/api/v1/runs?dag=sync&before=2026-10-15T01%3A49%3A11Z&before_run=r50&limit=50";
    let etl = scripted(&[
        (
            "/api/v1/runs?dag=sync&limit=50",
            200,
            json!({ "runs": runs }),
        ),
        (
            next,
            200,
            json!({"runs": [run_at("r49", "sync", "success", 1)]}),
        ),
    ]);
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(&app, "/runs?dag=sync&state=", &[]).await;
    assert_eq!(texts(&html, "table.runs tbody tr").len(), 50);
    let older = attrs(&html, ".pager a", "href");
    assert_eq!(
        older,
        ["/runs?dag=sync&before=2026-10-15T01%3A49%3A11Z&before_run=r50"]
    );
    let (status, html) = get(&app, &older[0], &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "table.runs tbody td:first-child a"), ["r49"]);
    // The filter is kept and shown.
    assert_eq!(attrs(&html, "input[name=dag]", "value"), ["sync"]);
}

#[tokio::test]
async fn filters_go_to_the_api_and_a_deployment_filter_asks_only_that_one() {
    let etl = scripted(&[
        (
            "/api/v1/runs?dag=etl%3Aa%2Fb.py%23sync&state=failed&limit=50",
            200,
            json!({"runs": [run_at("e1", "etl:a/b.py#sync", "failed", 1)]}),
        ),
        (
            "/api/v1/runs?state=failed&limit=50",
            200,
            json!({"runs": []}),
        ),
    ]);
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![deployment("etl", etl), deployment("ml", down)]);
    let (status, html) = get(
        &app,
        "/runs?deployment=etl&dag=etl%3Aa%2Fb.py%23sync&state=failed",
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(texts(&html, ".banner").is_empty(), "ml wasn't asked");
    assert_eq!(texts(&html, "table.runs tbody td:first-child a"), ["e1"]);
    assert_eq!(
        attrs(&html, "select[name=state] option[selected]", "value"),
        ["failed"]
    );
    // Every deployment asked: the one down is named, the others listed.
    let (status, html) = get(&app, "/runs?state=failed", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let banners = texts(&html, ".banner.warning");
    assert_eq!(banners.len(), 1);
    // Its runs can't be placed by one cursor: the page says they're missing, not just late.
    assert!(banners[0].contains("Its runs are missing"), "{banners:?}");
    for bad in [
        "/runs?state=done",
        "/runs?dag=bad%20id",
        "/runs?before_run=r1",
        "/runs?before=soon",
    ] {
        let (status, _) = get(&app, bad, &[]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
}

fn task(id: &str, state: &str, outcome: Option<&str>, reason: Option<&str>) -> Value {
    json!({
        "task_id": id, "state": state, "outcome": outcome, "try_number": 2,
        "reason": reason, "updated_at": "2026-10-15T01:05:00Z"
    })
}

fn attempt(try_number: u32, status: Option<&str>) -> Value {
    json!({
        "attempt_id": format!("a{try_number}"), "try_number": try_number, "worker_id": "w1",
        "status": status, "exit_code": 1, "error_kind": "task_failed",
        "error_summary": {"exception": "ValueError", "message": "no rows",
                          "traceback": "Traceback (most recent call last):\n  <here>"},
        "assigned_at": "2026-10-15T01:01:00Z", "started_at": "2026-10-15T01:01:01Z",
        "ended_at": "2026-10-15T01:01:05Z", "duration_ms": 4_200,
        "peak_memory_bytes": 52_428_800, "start_method": "fork", "usage": null,
        "core_dumped": false
    })
}

fn log(from: u64, size: u64, content: &str) -> Value {
    json!({"attempt_id": "a2", "try_number": 2, "worker_id": "w1", "from": from,
           "next": size, "size": size, "running": false, "content": content})
}

/// A failed run `r2` of `sync`: extract -> load (data) -> report (order), load failed.
fn failed_run() -> Vec<(&'static str, u16, Value)> {
    vec![
        (
            "/api/v1/runs/r2",
            200,
            json!({
                "run": run_at("r2", "sync", "failed", 2),
                "tasks": [
                    task("extract", "success", Some("SUCCESS"), None),
                    task("load", "failed", None, None),
                    task("report", "upstream_failed", None, None),
                ],
                "code_hashes": ["sha256:a", "sha256:b"]
            }),
        ),
        (
            "/api/v1/runs/r2/dag",
            200,
            json!({
                "id": "sync",
                "tasks": [{"id": "extract"}, {"id": "load"}, {"id": "report"}],
                "edges": [
                    {"from": "extract", "to": "load", "kind": "data"},
                    {"from": "load", "to": "report", "kind": "order"}
                ]
            }),
        ),
        (
            "/api/v1/runs/r2/commands",
            200,
            json!({"commands": [{
                "command_id": "c1", "kind": "retry_task", "status": "rejected",
                "reason": "`load` isn't unschedulable", "result": null,
                "requested_by": "anna", "requested_at": "2026-10-15T01:06:00Z",
                "decided_at": "2026-10-15T01:06:01Z"
            }]}),
        ),
        (
            "/api/v1/runs/r2/tasks/load/attempts",
            200,
            json!({"attempts": [attempt(1, Some("failed")), attempt(2, Some("failed"))]}),
        ),
        (
            "/api/v1/runs/r2/tasks/report/attempts",
            200,
            json!({"attempts": []}),
        ),
        (
            "/api/v1/runs/r2/tasks/extract/output",
            200,
            json!({"outcome": "SUCCESS", "value": {"rows": 3}}),
        ),
        (
            "/api/v1/runs/r2/tasks/load/attempts/2/log?from=0&limit=4",
            200,
            log(0, 300_000, "star"),
        ),
        (
            "/api/v1/runs/r2/tasks/load/attempts/2/log?from=37856&limit=262144",
            200,
            log(
                37_856,
                300_000,
                "cut line\n\u{1b}[31mERROR\u{1b}[0m <boom>\n",
            ),
        ),
    ]
}

#[tokio::test]
async fn a_run_shows_its_graph_tasks_and_commands() {
    let app = ui(vec![deployment("etl", scripted(&failed_run()))]);
    let (status, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "h1 .state"), ["failed"]);
    assert_eq!(texts(&html, ".summary"), ["failed: task load failed"]);
    // Two code versions: named.
    assert_eq!(texts(&html, ".banner.warning li"), ["sha256:a", "sha256:b"]);
    // The graph: a node per task, coloured by state, top to bottom; an edge per dependency.
    assert_eq!(
        attrs(&html, "svg.dag a.node", "data-task"),
        ["extract", "load", "report"]
    );
    assert_eq!(
        attrs(&html, "svg.dag rect.box", "class"),
        ["box success", "box failed", "box upstream_failed"]
    );
    let tops: Vec<f64> = attrs(&html, "svg.dag rect.box", "y")
        .iter()
        .map(|y| y.parse().unwrap())
        .collect();
    assert!(tops[0] < tops[1] && tops[1] < tops[2], "{tops:?}");
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        ["edge data", "edge order"]
    );
    // Clicking a task opens its panel beside the graph (htmx), or the page with it open.
    assert_eq!(
        attrs(&html, "svg.dag a.node", "hx-get"),
        [
            "/d/etl/runs/r2/tasks/extract",
            "/d/etl/runs/r2/tasks/load",
            "/d/etl/runs/r2/tasks/report"
        ]
    );
    assert_eq!(
        attrs(&html, "svg.dag a.node", "href")[1],
        "/d/etl/runs/r2?task=load"
    );
    assert_eq!(texts(&html, "#task-panel .empty").len(), 1);
    assert_eq!(
        texts(&html, "table.tasks .state"),
        ["success", "failed", "upstream_failed"]
    );
    assert_eq!(
        texts(&html, "table.commands tbody td"),
        [
            "2026-10-15 01:06 UTC",
            "anna",
            "retry_task",
            "rejected `load` isn't unschedulable"
        ]
    );
    assert_eq!(
        attrs(&html, "script[src]", "src"),
        [
            "/assets/htmx-2.0.11.min.js",
            "/assets/htmx-ext-sse-2.2.4.js"
        ]
    );
    // Live: it follows its run, and reads its live parts again (without the panel) on a change.
    assert_eq!(
        attrs(&html, "body", "sse-connect"),
        ["/events?deployment=etl&run=r2"]
    );
    assert_eq!(attrs(&html, "#live-head", "hx-get"), ["/d/etl/runs/r2"]);
    // Every change counts: a change during a read is read again after it.
    assert_eq!(
        attrs(&html, "#live-head", "hx-trigger"),
        ["sse:change[!schedrDialogOpen()]"]
    );
    assert_eq!(attrs(&html, "#live-head", "hx-sync"), ["this:queue last"]);
    assert_eq!(
        attrs(&html, "#live-head", "hx-select-oob"),
        ["#live-graph,#live-tasks"]
    );
    assert_eq!(texts(&html, "#live-graph svg.dag a.node").len(), 3);
    assert_eq!(texts(&html, "#live-tasks table.tasks").len(), 1);
}

#[tokio::test]
async fn a_tasks_panel_shows_its_attempts_with_their_errors() {
    let app = ui(vec![deployment("etl", scripted(&failed_run()))]);
    // As htmx asks for it: the fragment alone.
    let (status, html) = get(&app, "/d/etl/runs/r2/tasks/load", &[("hx-request", "true")]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.contains("<html"), "a fragment, not a page");
    assert_eq!(texts(&html, ".panel-head h2"), ["load"]);
    assert_eq!(attrs(&html, ".panel-head", "data-task"), ["load"]);
    // Read again on a change, unless something else is loading into the panel; a click on a
    // tab replaces a refresh in flight (it would swap the tab it was for).
    assert_eq!(attrs(&html, ".panel-live", "hx-sync"), ["#task-panel:drop"]);
    assert_eq!(
        attrs(&html, ".panel-live", "hx-trigger"),
        ["sse:change[!schedrDialogOpen()]"]
    );
    let tab_syncs = attrs(&html, ".tabs a.tab", "hx-sync");
    assert_eq!(tab_syncs, ["#task-panel:replace"; 4]);
    assert_eq!(attrs(&html, ".tabs .tab.open", "data-tab"), ["attempts"]);
    assert_eq!(
        texts(&html, "table.attempts tbody td:first-child"),
        ["1", "", "2", ""]
    );
    assert_eq!(
        texts(&html, ".error"),
        [
            "task_failed · ValueError: no rows",
            "task_failed · ValueError: no rows"
        ]
    );
    assert!(texts(&html, "table.attempts td").contains(&"4.2 s".to_owned()));
    assert!(texts(&html, "table.attempts td").contains(&"50.0 MiB".to_owned()));
    // As a browser asks for it: the run page with the panel open.
    let (status, page) = get(&app, "/d/etl/runs/r2/tasks/load?tab=logs", &[]).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(page, "");
    let (status, page) = get(&app, "/d/etl/runs/r2?task=load", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&page, "#task-panel .panel-head h2"), ["load"]);
    assert_eq!(
        attrs(&page, "svg.dag a.node.selected", "data-task"),
        ["load"]
    );
    let (status, _) = get(&app, "/d/etl/runs/r2?task=ghost", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A malformed query, asked for by htmx: a fragment too.
    let (status, html) = get(
        &app,
        "/d/etl/runs/r2/tasks/load?try=nope",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!html.contains("<html"), "a fragment, not a page");
    assert_eq!(texts(&html, ".banner.warning strong"), ["Bad request"]);
    // Asked for by htmx, a panel that can't be shown says why inside the panel.
    let (status, html) = get(
        &app,
        "/d/etl/runs/r2/tasks/ghost",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!html.contains("<html"), "a fragment, not a page");
    assert_eq!(
        texts(&html, ".banner.warning"),
        ["Not found: There is no task `ghost` in run `r2`."]
    );
}

#[tokio::test]
async fn the_output_tab_shows_the_value_and_its_download() {
    let mut answers = failed_run();
    let big = "x".repeat(70_000);
    answers.push((
        "/api/v1/runs/r2/tasks/load/output",
        200,
        json!({"outcome": "Empty", "value": big}),
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let fragment = [("hx-request", "true")];
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/extract?tab=output", &fragment).await;
    assert_eq!(texts(&html, "pre.output"), ["{\n  \"rows\": 3\n}"]);
    assert_eq!(
        attrs(&html, "a[download]", "href"),
        ["/d/etl/runs/r2/tasks/extract/output.json"]
    );
    // A large one: its end, marked.
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/load?tab=output", &fragment).await;
    let shown = &texts(&html, "pre.output")[0];
    assert_eq!(shown.len(), OUTPUT_TAIL);
    assert!(shown.ends_with("x\""));
    assert!(html.contains("Only its end is shown"));
    // None yet.
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/report?tab=output", &fragment).await;
    assert_eq!(texts(&html, ".tab-body .empty"), ["No output (yet)."]);
    // The download: the whole value.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/d/etl/runs/r2/tasks/extract/output.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[axum::http::header::CONTENT_DISPOSITION],
        "attachment; filename=\"extract.json\""
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"rows": 3})
    );
}

#[tokio::test]
async fn the_log_tab_shows_the_end_of_the_last_try_in_colour() {
    let mut answers = failed_run();
    answers.push((
        "/api/v1/runs/r2/tasks/load/attempts/1/log?from=0&limit=4",
        503,
        json!({"error": "worker `w1` is gone"}),
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let fragment = [("hx-request", "true")];
    let (status, html) = get(&app, "/d/etl/runs/r2/tasks/load?tab=logs", &fragment).await;
    assert_eq!(status, StatusCode::OK);
    // The last try's, from its first whole line, colours as spans, text escaped.
    assert_eq!(texts(&html, ".tries .try.open"), ["2"]);
    assert_eq!(texts(&html, "pre.log"), ["ERROR <boom>"]);
    assert_eq!(texts(&html, "pre.log .fg-1"), ["ERROR"]);
    assert!(html.contains("Only its end is shown"));
    // An earlier try whose log can't be read says why.
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/load?tab=logs&try=1", &fragment).await;
    assert_eq!(
        texts(&html, ".tab-body .empty"),
        ["No log to show: worker `w1` is gone"]
    );
    // Not tried yet.
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/report?tab=logs", &fragment).await;
    assert_eq!(
        texts(&html, ".tab-body .empty"),
        ["No log to show: It hasn't been tried yet."]
    );
}

#[tokio::test]
async fn an_unknown_run_is_a_404_and_a_deployment_down_a_502() {
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![
        deployment("etl", scripted(&failed_run())),
        deployment("ml", down),
    ]);
    for path in [
        "/d/etl/runs/ghost",
        "/d/nowhere/runs/r2",
        "/d/etl/runs/r2/tasks/ghost/output.json",
    ] {
        let (status, _) = get(&app, path, &[]).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
    let (status, html) = get(&app, "/d/ml/runs/r2", &[]).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(texts(&html, "p")[0].starts_with("ml: can't reach"));
}

#[tokio::test]
async fn the_dag_page_links_its_runs() {
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        attrs(&html, "table.runs tbody a", "href"),
        ["/d/etl/runs/r2", "/d/etl/runs/r1"]
    );
    assert_eq!(
        attrs(&html, "h2 a.more", "href"),
        ["/runs?deployment=etl&dag=sync"]
    );
}

#[tokio::test]
async fn a_log_tail_that_is_one_long_line_is_shown_from_the_cut() {
    for content in ["the end of one long line", "the end of one long line\n"] {
        let mut answers = failed_run();
        answers.retain(|(path, _, _)| !path.contains("from=37856"));
        answers.push((
            "/api/v1/runs/r2/tasks/load/attempts/2/log?from=37856&limit=262144",
            200,
            log(37_856, 300_000, content),
        ));
        let app = ui(vec![deployment("etl", scripted(&answers))]);
        let (_, html) = get(
            &app,
            "/d/etl/runs/r2/tasks/load?tab=logs",
            &[("hx-request", "true")],
        )
        .await;
        assert_eq!(
            texts(&html, "pre.log"),
            ["the end of one long line"],
            "{content:?}"
        );
    }
}

#[tokio::test]
async fn two_dependencies_between_the_same_tasks_are_one_line_drawn_as_data() {
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| *path != "/api/v1/runs/r2/dag");
    answers.push((
        "/api/v1/runs/r2/dag",
        200,
        json!({
            "id": "sync",
            "tasks": [{"id": "extract"}, {"id": "load"}, {"id": "report"}],
            "edges": [
                {"from": "extract", "to": "load", "kind": "order"},
                {"from": "extract", "to": "load", "kind": "data"},
                {"from": "load", "to": "report", "kind": "order"},
                {"from": "load", "to": "report", "kind": "order"}
            ]
        }),
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        ["edge data", "edge order"]
    );
}

#[tokio::test]
async fn an_edge_on_outcomes_is_labelled_and_faint_once_not_taken() {
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| *path != "/api/v1/runs/r2/dag");
    for (path, _, value) in &mut answers {
        if *path == "/api/v1/runs/r2" {
            value["tasks"] = json!([
                task("extract", "success", Some("Unavailable"), None),
                task("load", "skipped", None, None),
                task("report", "success", None, None),
            ]);
        }
    }
    answers.push((
        "/api/v1/runs/r2/dag",
        200,
        json!({
            "id": "sync",
            "tasks": [{"id": "extract"}, {"id": "load"}, {"id": "report"}],
            "edges": [
                {"from": "extract", "to": "load", "kind": "data", "on": ["Rows"]},
                {"from": "extract", "to": "report", "kind": "data", "on": ["Unavailable"]},
                {"from": "extract", "to": "report", "kind": "order", "on": ["Rows"]}
            ]
        }),
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    // One line per pair, its outcomes together; the one whose outcome didn't happen faint.
    assert_eq!(
        texts(&html, "svg.dag text.edge-on"),
        ["Rows", "Unavailable | Rows"]
    );
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        ["edge data not-taken", "edge data"]
    );
    assert_eq!(
        texts(&html, "svg.dag path.edge title"),
        ["on Rows (not taken)", "on Unavailable | Rows"]
    );
}

/// A run whose branch `pick` chose `a` of `a`, `b` and `c`; `c` ran anyway on its order edge
/// from `other` (decision 55B). `chose` is the branch's stored output, if it can be read.
fn branch_run(chose: Option<Value>) -> Vec<(&'static str, u16, Value)> {
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| *path != "/api/v1/runs/r2/dag");
    for (path, _, value) in &mut answers {
        if *path == "/api/v1/runs/r2" {
            value["tasks"] = json!([
                task("pick", "success", None, None),
                task("other", "success", None, None),
                task("a", "success", None, None),
                task("b", "skipped", None, None),
                task("c", "success", None, None),
            ]);
        }
    }
    answers.push((
        "/api/v1/runs/r2/dag",
        200,
        json!({
            "id": "sync",
            "tasks": [
                {"id": "pick", "kind": "python", "branch": true},
                {"id": "other"}, {"id": "a"}, {"id": "b"}, {"id": "c"}
            ],
            "edges": [
                {"from": "pick", "to": "a", "kind": "selection"},
                {"from": "pick", "to": "b", "kind": "selection"},
                {"from": "pick", "to": "c", "kind": "selection"},
                {"from": "other", "to": "c", "kind": "order"}
            ]
        }),
    ));
    if let Some(chose) = chose {
        answers.push((
            "/api/v1/runs/r2/tasks/pick/output",
            200,
            json!({"outcome": "SUCCESS", "value": chose}),
        ));
    }
    answers
}

#[tokio::test]
async fn a_branchs_edge_to_a_target_it_didnt_choose_is_faint() {
    let app = ui(vec![deployment(
        "etl",
        scripted(&branch_run(Some(json!(["a"])))),
    )]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        [
            "edge selection",
            "edge selection not-taken",
            "edge selection not-taken",
            "edge order"
        ]
    );
    // An output that isn't a list chose nothing, as the master reads it.
    let app = ui(vec![deployment(
        "etl",
        scripted(&branch_run(Some(json!(null)))),
    )]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        [
            "edge selection not-taken",
            "edge selection not-taken",
            "edge selection not-taken",
            "edge order"
        ]
    );
    // Without the branch's output, what it chose isn't known: every edge drawn solid.
    let app = ui(vec![deployment("etl", scripted(&branch_run(None)))]);
    let (status, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        [
            "edge selection",
            "edge selection",
            "edge selection",
            "edge order"
        ]
    );
}

#[tokio::test]
async fn a_dags_groups_are_drawn_as_labelled_boxes_around_their_tasks() {
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| *path != "/api/v1/runs/r2/dag");
    answers.push((
        "/api/v1/runs/r2/dag",
        200,
        json!({
            "id": "sync",
            "groups": [{"id": "load"}, {"id": "load.eu", "parent": "load"}],
            "tasks": [
                {"id": "extract"},
                {"id": "load.eu.x", "group": "load.eu"},
                {"id": "load.y", "group": "load"},
                {"id": "report"}
            ],
            "edges": [
                {"from": "extract", "to": "load.eu.x", "kind": "data"},
                {"from": "load.eu.x", "to": "load.y", "kind": "data"},
                {"from": "load.y", "to": "report", "kind": "order"}
            ]
        }),
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(texts(&html, "svg.dag text.group-label"), ["load", "eu"]);
    assert_eq!(
        texts(&html, "svg.dag g.group title"),
        ["group load", "group load.eu"]
    );
    // Boxes come before the tasks, so tasks are drawn over them.
    let group_at = html.find("class=\"group\"").unwrap();
    let node_at = html.find("class=\"node").unwrap();
    assert!(group_at < node_at);
}

fn import_error(path: &str) -> Value {
    json!({
        "root": "etl", "path": path, "content_hash": "sha256:9f1c2b3a4d5e6f708192",
        "parsed_at": "2026-10-29T10:42:00Z", "worker_id": "w1", "schedr_version": "0.1.0",
        "exception": "ModuleNotFoundError", "message": "No module named 'pandas'",
        "traceback": "  File \"broken.py\", line 1"
    })
}

#[tokio::test]
async fn import_errors_are_listed_counted_in_the_nav_and_noticed_on_the_dag_list() {
    let etl = scripted(&[(
        "/api/v1/import-errors",
        200,
        json!({"errors": [import_error("broken.py"), import_error("other.py")]}),
    )]);
    let ml = scripted(&[("/api/v1/import-errors", 200, json!({"errors": []}))]);
    let app = ui(vec![deployment("etl", etl), deployment("ml", ml)]);
    let (status, html) = get(&app, "/import-errors", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "table.import-errors td.mono"),
        ["etl", "broken.py", "etl", "other.py"]
    );
    assert_eq!(
        texts(&html, "table.import-errors .error-kind"),
        ["ModuleNotFoundError", "ModuleNotFoundError"]
    );
    assert_eq!(
        texts(&html, "table.import-errors .sub"),
        ["9f1c2b3a4d5e · w1", "9f1c2b3a4d5e · w1"]
    );
    assert_eq!(
        texts(&html, "pre.traceback")[0],
        "File \"broken.py\", line 1"
    );
    let (_, count) = get(&app, "/import-errors/count", &[]).await;
    assert_eq!(count, r#"<span class="count failed">2</span>"#);
    let (_, notice) = get(&app, "/import-errors/notice", &[]).await;
    assert!(notice.contains("2 files failed to import"), "{notice}");
    assert!(notice.contains(r#"href="/import-errors""#), "{notice}");
}

#[tokio::test]
async fn without_import_errors_the_nav_and_dag_list_say_nothing() {
    let etl = scripted(&[("/api/v1/import-errors", 200, json!({"errors": []}))]);
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(&app, "/import-errors", &[]).await;
    assert_eq!(texts(&html, "p.empty"), ["No import errors."]);
    assert_eq!(get(&app, "/import-errors/count", &[]).await.1, "");
    assert_eq!(get(&app, "/import-errors/notice", &[]).await.1, "");
    // A deployment that is down is said so, and the others still counted.
    let down = ui(vec![deployment(
        "etl",
        Scripted {
            answers: BTreeMap::new(),
            down: true,
        },
    )]);
    let (_, html) = get(&down, "/import-errors", &[]).await;
    assert!(texts(&html, ".banner.warning")[0].starts_with("Unavailable: etl"));
}
