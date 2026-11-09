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
        ..Scripted::default()
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
                "code_hashes": ["sha256:a", "sha256:b"],
                // Masked by the webserver; masked again here, as if it hadn't been.
                "params": {"region": "us", "api_key": "abc", "limits": [1, 2],
                           "url": "postgres://u:pw@db/x"}
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
            log(0, 2_000_000, "star"),
        ),
        (
            // The last 1 MiB (2_000_000 - 1_048_576).
            "/api/v1/runs/r2/tasks/load/attempts/2/log?from=951424&limit=1048576",
            200,
            // The API starts a cut part at a line (after "cut line\n").
            log(951_433, 2_000_000, "\u{1b}[31mERROR\u{1b}[0m <boom>\n"),
        ),
    ]
}

#[tokio::test]
async fn a_run_shows_its_params_masked() {
    let app = ui(vec![deployment("etl", scripted(&failed_run()))]);
    let (status, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let params = html
        .split("<dt>Params</dt>")
        .nth(1)
        .and_then(|rest| rest.split("</dd>").next())
        .expect("a Params fact");
    for shown in [
        "region</span> = <span class=\"mono\">us",
        "api_key</span> = <span class=\"mono\">****",
        "limits</span> = <span class=\"mono\">[1,2]",
        "url</span> = <span class=\"mono\">postgres://u:****@db/x",
    ] {
        assert!(params.contains(shown), "{shown} in {params}");
    }
    assert!(!html.contains("abc") && !html.contains(":pw@"), "{html}");
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
        ["/assets/htmx-2.0.11.min.js"]
    );
    // Live: it follows its run, and reads its live parts again (without the panel) on a change.
    assert_eq!(events_followed(&html), "/events?deployment=etl&run=r2");
    assert_eq!(attrs(&html, "#live-head", "hx-get"), ["/d/etl/runs/r2"]);
    // Every change counts: a change during a read is read again after it.
    assert_eq!(
        attrs(&html, "#live-head", "hx-trigger"),
        ["live:change[!schedrDialogOpen()]"]
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
async fn a_wait_step_pulses_at_work_and_a_timeout_looks_skipped() {
    let tasks = [
        task("working", "running", None, None),
        task("heard", "success", Some("TimedOut"), None),
        task("unheard", "success", Some("TimedOut"), None),
        task("broken", "failed", None, None),
        task("plain", "running", None, None),
        task("returned_it", "success", Some("TimedOut"), None),
        task("found", "success", Some("SUCCESS"), None),
        task("alert", "scheduled", None, None),
    ];
    let sensor = |id: &str| json!({"id": id, "kind": "sensor"});
    let answers = scripted(&[
        (
            "/api/v1/runs/r1",
            200,
            json!({
                "run": run_at("r1", "sync", "running", 2),
                "tasks": tasks, "code_hashes": [],
            }),
        ),
        (
            "/api/v1/runs/r1/dag",
            200,
            json!({
                "id": "sync",
                "tasks": [
                    sensor("working"), sensor("heard"), sensor("unheard"), sensor("broken"), sensor("found"),
                    {"id": "plain", "kind": "python"}, {"id": "returned_it", "kind": "python"},
                    {"id": "alert", "kind": "python"},
                ],
                "edges": [
                    {"from": "heard", "to": "alert", "kind": "order", "on": ["TimedOut"]},
                    {"from": "unheard", "to": "alert", "kind": "order", "on": ["SUCCESS"]},
                ],
            }),
        ),
    ]);
    let mut answers = answers;
    answers.answers.insert(
        "/api/v1/runs/r1/commands".to_owned(),
        (200, json!({"commands": []})),
    );
    let app = ui(vec![deployment("etl", answers)]);
    let (status, html) = get(&app, "/d/etl/runs/r1", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        attrs(&html, "table.tasks tbody .state", "class"),
        [
            "state running waiting",
            "state skipped waited-out",
            "state skipped waited-out",
            "state failed",
            "state running",
            "state success",
            "state success",
            "state scheduled",
        ]
    );
    assert_eq!(
        attrs(&html, "svg.dag rect.box.waiting", "class"),
        ["box running waiting"]
    );
    // Whether a task takes the timeout or not: not taking it is the author's to choose.
    assert_eq!(
        attrs(&html, "svg.dag rect.box.waited-out", "class").len(),
        2
    );
    assert!(
        texts(&html, "table.tasks tbody .state").contains(&"success · TimedOut".to_owned()),
        "it says what happened, whatever it looks like"
    );
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
        ["live:change[!schedrDialogOpen()]"]
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
    // The last try's end, colours as spans, text escaped.
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
        answers.retain(|(path, _, _)| !path.contains("from=951424"));
        answers.push((
            "/api/v1/runs/r2/tasks/load/attempts/2/log?from=951424&limit=1048576",
            200,
            log(951_424, 2_000_000, content),
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
    // One stored before its master was kept (none).
    let mut stored = import_error("broken.py");
    stored["master_id"] = json!("0190a0a0-0000-7000-8000-00000000000a");
    let etl = scripted(&[(
        "/api/v1/import-errors",
        200,
        json!({"errors": [stored, import_error("other.py")]}),
    )]);
    let ml = scripted(&[("/api/v1/import-errors", 200, json!({"errors": []}))]);
    let app = ui(vec![deployment("etl", etl), deployment("ml", ml)]);
    let (status, html) = get(&app, "/import-errors", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "table.import-errors td.mono"),
        [
            "etl",
            "broken.py",
            "0000000a",
            "w1",
            "etl",
            "other.py",
            "-",
            "w1"
        ]
    );
    // Each links to its node on the Deployments page.
    assert_eq!(
        attrs(&html, "table.import-errors td.mono a", "href"),
        [
            "/deployments#node.etl.master.0190a0a0-0000-7000-8000-00000000000a",
            "/deployments#node.etl.worker.w1",
            "/deployments#node.etl.worker.w1"
        ]
    );
    assert_eq!(
        texts(&html, "table.import-errors .error-kind"),
        ["ModuleNotFoundError", "ModuleNotFoundError"]
    );
    assert_eq!(
        texts(&html, "table.import-errors .sub"),
        ["9f1c2b3a4d5e", "9f1c2b3a4d5e"]
    );
    assert_eq!(
        texts(&html, "pre.traceback")[0],
        "File \"broken.py\", line 1"
    );
    let (_, count) = get(&app, "/import-errors/count", &[]).await;
    assert_eq!(count, r#"<span class="count failed">2</span>"#);
    let (_, notice) = get(&app, "/import-errors/notice", &[]).await;
    assert!(
        notice.contains(
            "2 files failed to import: their DAGs are held (no runs) until they are fixed"
        ),
        "{notice}"
    );
    assert!(notice.contains(r#"href="/import-errors""#), "{notice}");
}

#[tokio::test]
async fn the_notice_says_held_only_of_files_that_hold_their_dags() {
    let mut timed_out = import_error("slow.py");
    timed_out["exception"] = json!("ParseTimeout");
    let mut died = import_error("crashed.py");
    died["exception"] = json!("RunnerDied");
    let answer = |errors: Vec<Value>| {
        let etl = scripted(&[("/api/v1/import-errors", 200, json!({"errors": errors}))]);
        ui(vec![deployment("etl", etl)])
    };
    let notice = |app| async move { get(&app, "/import-errors/notice", &[]).await.1 };
    // Only transient failures: nothing is held, the files are parsed again.
    let only = notice(answer(vec![timed_out.clone(), died.clone()])).await;
    assert!(!only.contains("held"), "{only}");
    assert!(
        only.contains(
            "2 files failed to import for now (a timeout or a runner that died): parsed again"
        ),
        "{only}"
    );
    // Both kinds: each said of its own files.
    let both = notice(answer(vec![import_error("broken.py"), timed_out])).await;
    assert!(
        both.contains("1 file failed to import: its DAGs are held (no runs) until it is fixed"),
        "{both}"
    );
    assert!(
        both.contains(
            "1 file failed to import for now (a timeout or a runner that died): parsed again, \
             its DAGs keep their last good version"
        ),
        "{both}"
    );
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
            down: true,
            ..Scripted::default()
        },
    )]);
    let (_, html) = get(&down, "/import-errors", &[]).await;
    assert!(texts(&html, ".banner.warning")[0].starts_with("Unavailable: etl"));
}

#[tokio::test]
async fn a_log_that_grew_past_its_part_before_its_attempt_ended_is_followed_to_its_end() {
    // B146: the size read first, then the part from there; the attempt wrote more and ended
    // in between, so the part stops short of the log's end.
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| !path.contains("/log?"));
    answers.push((
        "/api/v1/runs/r2/tasks/load/attempts/2/log?from=0&limit=4",
        200,
        log(0, 200_000, "star"),
    ));
    let mut part = log(0, 300_000, "the first part\n");
    part["next"] = json!(262_144);
    answers.push((
        "/api/v1/runs/r2/tasks/load/attempts/2/log?from=0&limit=1048576",
        200,
        part,
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(
        &app,
        "/d/etl/runs/r2/tasks/load?tab=logs",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(
        attrs(&html, "pre.log .log-tail", "hx-get"),
        ["/d/etl/runs/r2/tasks/load/tail?try=2&from=262144"]
    );
}

#[tokio::test]
async fn a_followed_log_goes_on_in_the_colour_in_force_where_the_panel_ends() {
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| !path.contains("/log?"));
    answers.push((
        "/api/v1/runs/r2/tasks/load/attempts/2/log?from=0&limit=4",
        200,
        log(0, 200_000, "star"),
    ));
    let mut part = log(0, 300_000, "\u{1b}[1;31mred and bold\n");
    part["next"] = json!(262_144);
    answers.push((
        "/api/v1/runs/r2/tasks/load/attempts/2/log?from=0&limit=1048576",
        200,
        part,
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(
        &app,
        "/d/etl/runs/r2/tasks/load?tab=logs",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(
        attrs(&html, "pre.log .log-tail", "hx-get"),
        ["/d/etl/runs/r2/tasks/load/tail?try=2&from=262144&style=1%3B31"]
    );
}

/// Answers each branch's output after `DELAY`, counting the reads in flight at once.
#[derive(Default)]
struct SlowOutputs {
    in_flight: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
}

impl Transport for SlowOutputs {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        self.most.fetch_max(now, SeqCst);
        tokio::time::sleep(Duration::from_millis(40)).await;
        self.in_flight.fetch_sub(1, SeqCst);
        let branch = request
            .path
            .split('/')
            .nth(6)
            .unwrap_or_default()
            .to_owned();
        Ok(ApiResponse {
            status: 200,
            body: json!({"outcome": "SUCCESS", "value": [format!("{branch}-target")]}),
        })
    }
}

#[tokio::test]
async fn a_runs_branches_are_read_together() {
    // B147: four branches read one at a time made each refresh their latencies' sum.
    let ids = ["b1", "b2", "b3", "b4"];
    let dag = json!({"tasks": ids.iter().map(|id| json!({"id": id, "branch": true})).collect::<Vec<_>>()});
    let tasks: Vec<schedr_api::views::TaskView> = ids
        .iter()
        .map(|id| serde_json::from_value(task(id, "success", Some("SUCCESS"), None)).unwrap())
        .collect();
    let deployment = Deployment {
        name: "etl".to_owned(),
        client: Client::new(
            SlowOutputs::default(),
            Duration::from_millis(1),
            Duration::from_secs(1),
        ),
        proof: "none",
    };
    let chosen = crate::runs::chosen_by_branches(&deployment, "r2", &dag, &tasks).await;
    assert_eq!(chosen.keys().map(String::as_str).collect::<Vec<_>>(), ids);
    assert_eq!(chosen["b3"], ["b3-target"]);
    let most = deployment
        .client
        .transport()
        .most
        .load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(most, ids.len(), "reads at once");
}

#[test]
fn a_logs_end_is_its_last_256_kib_from_a_line_or_its_last_line_whole() {
    use crate::runs::{LOG_TAIL, log_end};
    assert_eq!(log_end("a\nb\n"), ("a\nb\n", false));
    // Many short lines: from the first line starting in the last 256 KiB.
    let line = "x".repeat(99) + "\n";
    let many = line.repeat(3000);
    let (end, cut) = log_end(&many);
    assert!(cut && end.len() <= LOG_TAIL && end.len() > LOG_TAIL - 100);
    assert!(end.starts_with('x') && many.ends_with(end));
    // A last line longer than 256 KiB: shown whole (B: it was dropped).
    let long = format!("first\n{}\n", "a".repeat(300 * 1024));
    assert_eq!(log_end(&long), (&long[6..], true));
    // One line only, longer: all of it.
    let one = format!("{}\n", "a".repeat(300 * 1024));
    assert_eq!(log_end(&one), (one.as_str(), false));
}

#[tokio::test]
async fn a_last_line_longer_than_a_part_is_said_not_shown() {
    // The API shows no line from inside it, and the panel can't know where it starts.
    let mut answers = failed_run();
    answers.retain(|(path, _, _)| !path.contains("from=951424"));
    answers.push((
        "/api/v1/runs/r2/tasks/load/attempts/2/log?from=951424&limit=1048576",
        200,
        {
            let mut part = log(2_000_000, 2_000_000, "");
            part["running"] = json!(false);
            part
        },
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
        ["… (its last line is longer than 1 MiB: not shown here)"]
    );
}

/// A run `r3` with two resources: `w.cluster` in group `w`, used by `w.load` then `w.warm`,
/// with `after` following the group; and `tmp`, used by `tmp.user`, nothing waiting for its
/// cleanup (`blocking=False`), which failed.
fn resourced_run() -> Vec<(&'static str, u16, Value)> {
    let mut failed_cleanup = task("tmp.user", "success", None, None);
    failed_cleanup["cleanup"] = json!("failed");
    vec![
        (
            "/api/v1/runs/r3",
            200,
            json!({
                "run": run_at("r3", "sync", "running", 2),
                "tasks": [
                    task("w.cluster", "success", None, None),
                    task("w.load", "success", None, None),
                    task("w.warm", "running", None, None),
                    task("w.cluster.cleanup", "scheduled", None, None),
                    task("after", "scheduled", None, None),
                    task("tmp", "success", None, None),
                    failed_cleanup,
                    task("tmp.cleanup", "failed", None, None),
                ],
                "code_hashes": [],
                "resources": [
                    {"setup": "w.cluster", "cleanup": "w.cluster.cleanup",
                     "users": ["w.load", "w.warm"], "blocking": true,
                     "state": "waiting_for_users"},
                    {"setup": "tmp", "cleanup": "tmp.cleanup", "users": ["tmp.user"],
                     "blocking": false, "state": "cleanup_failed"}
                ]
            }),
        ),
        (
            "/api/v1/runs/r3/dag",
            200,
            json!({
                "id": "sync",
                "groups": [{"id": "w"}],
                "tasks": [
                    {"id": "w.cluster", "group": "w"},
                    {"id": "w.load", "group": "w"},
                    {"id": "w.warm", "group": "w"},
                    {"id": "w.cluster.cleanup", "group": "w"},
                    {"id": "after"},
                    {"id": "tmp"},
                    {"id": "tmp.user"},
                    {"id": "tmp.cleanup"}
                ],
                "edges": [
                    {"from": "w.cluster", "to": "w.load", "kind": "requirement"},
                    {"from": "w.cluster", "to": "w.warm", "kind": "requirement"},
                    {"from": "w.cluster", "to": "w.cluster.cleanup", "kind": "requirement"},
                    {"from": "w.load", "to": "w.warm", "kind": "data"},
                    {"from": "w.warm", "to": "after", "kind": "order"},
                    {"from": "w.cluster.cleanup", "to": "after", "kind": "order"},
                    {"from": "tmp", "to": "tmp.user", "kind": "requirement"},
                    {"from": "tmp", "to": "tmp.cleanup", "kind": "requirement"}
                ],
                "resources": [
                    {"setup": "w.cluster", "cleanup": "w.cluster.cleanup",
                     "users": ["w.load", "w.warm"]},
                    {"setup": "tmp", "cleanup": "tmp.cleanup", "users": ["tmp.user"],
                     "blocking": false}
                ]
            }),
        ),
        ("/api/v1/runs/r3/commands", 200, json!({"commands": []})),
    ]
}

/// The graph's lines as `from -> to: class`, sorted.
fn lines(html: &str) -> Vec<String> {
    let (from, to, class) = (
        attrs(html, "svg.dag path.edge", "data-from"),
        attrs(html, "svg.dag path.edge", "data-to"),
        attrs(html, "svg.dag path.edge", "class"),
    );
    let mut lines: Vec<String> = (from.iter().zip(&to).zip(&class))
        .map(|((f, t), c)| format!("{f} -> {t}: {c}"))
        .collect();
    lines.sort();
    lines
}

#[tokio::test]
async fn a_resource_is_two_caps_on_a_rail_its_cleanup_reached_from_its_last_users() {
    let app = ui(vec![deployment("etl", scripted(&resourced_run()))]);
    let (status, html) = get(&app, "/d/etl/runs/r3", &[]).await;
    assert_eq!(status, StatusCode::OK);
    // Decision 262: the cleanup reached from the last user only, not the setup; decision 272:
    // what follows the group drawn from the cleanup alone; a cleanup nothing waits for dashed.
    // Decision 291: `w.cluster`'s users share group `w`: one line into its box and one out,
    // the lines to and from each user shown on hover only.
    assert_eq!(
        lines(&html),
        [
            "tmp -> tmp.user: edge requirement",
            "tmp.user -> tmp.cleanup: edge requirement released",
            "w -> w.cluster.cleanup: edge requirement resource-summary",
            "w.cluster -> w.load: edge requirement resource-detail",
            "w.cluster -> w.warm: edge requirement resource-detail",
            "w.cluster -> w: edge requirement resource-summary",
            "w.cluster.cleanup -> after: edge order",
            "w.load -> w.warm: edge data",
            "w.warm -> w.cluster.cleanup: edge requirement resource-detail",
        ]
    );
    // Hovering the resource shows them: they and the summaries are named by it.
    assert_eq!(
        attrs(&html, "svg.dag path.edge[data-resource]", "data-resource"),
        ["w.cluster"; 5]
    );
    // A dot on each user.
    assert_eq!(
        attrs(&html, "svg.dag circle.uses-dot", "data-resource"),
        ["w.cluster", "w.cluster", "tmp"]
    );
    // The caps: the setup's and the cleanup's, named by their resource; the rails.
    assert_eq!(
        attrs(&html, "svg.dag a.node[data-resource]", "data-task"),
        ["w.cluster", "w.cluster.cleanup", "tmp", "tmp.cleanup"]
    );
    assert_eq!(texts(&html, "svg.dag path.box.cap").len(), 4);
    assert_eq!(
        attrs(&html, "svg.dag path.rail", "data-resource"),
        ["w.cluster", "tmp"]
    );
    assert_eq!(texts(&html, "svg.dag line.cap-band").len(), 4);
}

#[tokio::test]
async fn a_runs_resources_are_listed_and_a_failed_cleanup_is_in_the_tasks_status() {
    let app = ui(vec![deployment("etl", scripted(&resourced_run()))]);
    let (_, html) = get(&app, "/d/etl/runs/r3", &[]).await;
    assert_eq!(
        texts(&html, "table.resources tbody tr td:first-child"),
        ["w.cluster", "tmp"]
    );
    assert_eq!(
        texts(&html, "table.resources .resource-state"),
        ["waiting for users", "cleanup failed"]
    );
    assert_eq!(
        texts(&html, "table.resources td.cleanup .state"),
        ["scheduled", "failed"]
    );
    assert_eq!(
        texts(&html, "table.resources td:nth-child(5)"),
        ["w.load, w.warm", "tmp.user"]
    );
    assert!(
        texts(&html, "table.resources td:nth-child(2)")[1].contains("not waited for"),
        "a cleanup nothing waits for says so"
    );
    // state · outcome · cleanup (only when it says something).
    assert!(texts(&html, "table.tasks .state").contains(&"success · cleanup failed".to_owned()));
}

#[tokio::test]
async fn the_cleanup_tab_shows_each_attempts_cleanup_and_handles_masked() {
    let mut answers = resourced_run();
    let mut cleaned = attempt(1, Some("success"));
    cleaned["cleanup"] = json!({
        "result": "failed", "tries": 2, "duration_ms": 1_500, "error_kind": "exit",
        "error_summary": {"exception": "OSError", "message": "gone",
                          "traceback": "Traceback: here"},
        "moves": 1,
        "handles": [{"handle_type": "Job", "value": {"id": "j-1", "password": "hunter2"}}]
    });
    let quiet = attempt(2, Some("success"));
    answers.push((
        "/api/v1/runs/r3/tasks/tmp.user/attempts",
        200,
        json!({"attempts": [cleaned, quiet]}),
    ));
    answers.push((
        "/api/v1/runs/r3/tasks/tmp/attempts",
        200,
        json!({"attempts": [attempt(1, Some("success"))]}),
    ));
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(
        &app,
        "/d/etl/runs/r3/tasks/tmp.user?tab=cleanup",
        &[("hx-request", "true")],
    )
    .await;
    // Only the attempt that has a cleanup.
    assert_eq!(texts(&html, "table.cleanups .cleanup"), ["failed"]);
    assert_eq!(
        texts(&html, "table.cleanups tbody tr:first-child td"),
        ["1", "failed", "2 tries · moved once", "1.5 s"]
    );
    assert_eq!(
        texts(&html, "table.cleanups .error"),
        ["exit · OSError: gone"]
    );
    let handles = texts(&html, "ul.handles li");
    assert_eq!(handles.len(), 1);
    assert!(handles[0].starts_with("Job "), "{handles:?}");
    assert!(!handles[0].contains("hunter2"), "{handles:?}");
    assert!(
        texts(&html, "p.resource-note").is_empty(),
        "a user isn't a resource's part"
    );

    // A setup names its cleanup; without cleanups, the tab says why.
    let (_, html) = get(
        &app,
        "/d/etl/runs/r3/tasks/tmp?tab=cleanup",
        &[("hx-request", "true")],
    )
    .await;
    let note = texts(&html, "p.resource-note");
    assert!(
        note[0].starts_with("It sets up resource tmp; its cleanup is tmp.cleanup failed"),
        "{note:?}"
    );
    assert_eq!(texts(&html, "table.cleanups").len(), 0);
    assert_eq!(
        texts(&html, "p.empty"),
        ["No cleanup has run after its attempts (yet)."]
    );
}

#[tokio::test]
async fn cancelling_a_resources_cleanup_warns_of_what_may_be_left() {
    // Decision 279: the cleanup's Cancel dialog says what cancelling it does.
    let mut answers = resourced_run();
    for path in [
        "/api/v1/runs/r3/tasks/w.cluster.cleanup/attempts",
        "/api/v1/runs/r3/tasks/w.warm/attempts",
    ] {
        answers.push((path, 200, json!({"attempts": []})));
    }
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let dialog = |html: &str| texts(html, "dialog.confirm p");
    let (_, html) = get(
        &app,
        "/d/etl/runs/r3/tasks/w.cluster.cleanup",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(
        dialog(&html),
        [
            "It stops freeing resource w.cluster: what its setup made may be left behind, and \
          what follows its group is cancelled. The run ends cancelled, or failed if a task of \
          it failed."
        ]
    );
    let (_, html) = get(
        &app,
        "/d/etl/runs/r3/tasks/w.warm",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(
        dialog(&html),
        [
            "Stopped if it runs, cancelled at once if it hasn't started; the tasks downstream of \
          it are cancelled."
        ]
    );
}

#[tokio::test]
async fn users_in_no_shared_group_keep_the_lines_to_the_first_and_from_the_last() {
    let mut answers = resourced_run();
    let (_, _, dag) = answers
        .iter_mut()
        .find(|(path, _, _)| *path == "/api/v1/runs/r3/dag")
        .unwrap();
    // `w.warm` out of group `w`: the users share no group.
    for task in dag["tasks"].as_array_mut().unwrap() {
        if task["id"] == "w.warm" {
            task.as_object_mut().unwrap().remove("group");
        }
    }
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(&app, "/d/etl/runs/r3", &[]).await;
    let drawn = lines(&html);
    for line in [
        "w.cluster -> w.load: edge requirement",
        "w.cluster -> w.warm: edge requirement resource-detail",
        "w.warm -> w.cluster.cleanup: edge requirement",
    ] {
        assert!(drawn.contains(&line.to_owned()), "{line} not in {drawn:?}");
    }
    assert!(
        !drawn.iter().any(|l| l.contains("resource-summary")),
        "{drawn:?}"
    );
}

#[tokio::test]
async fn a_data_edge_to_what_follows_stays_and_an_unused_resources_link_is_dashed() {
    let mut answers = resourced_run();
    let (_, _, dag) = answers
        .iter_mut()
        .find(|(path, _, _)| *path == "/api/v1/runs/r3/dag")
        .unwrap();
    // `after` takes `w.warm`'s output; `tmp` has no users and nothing waits for its cleanup.
    for edge in dag["edges"].as_array_mut().unwrap() {
        if edge["from"] == "w.warm" && edge["to"] == "after" {
            edge["kind"] = json!("data");
        }
    }
    dag["edges"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["to"] != "tmp.user");
    dag["resources"][1]["users"] = json!([]);
    let app = ui(vec![deployment("etl", scripted(&answers))]);
    let (_, html) = get(&app, "/d/etl/runs/r3", &[]).await;
    let drawn = lines(&html);
    assert!(
        drawn.contains(&"w.warm -> after: edge data".to_owned()),
        "{drawn:?}"
    );
    assert!(
        drawn.contains(&"tmp -> tmp.cleanup: edge requirement released".to_owned()),
        "{drawn:?}"
    );
}

/// A run of a DAG where `a` and `b` lead to a `first_of` that `b` won, `after` following it.
fn first_of_run(tasks: &Value, edges: &Value) -> Scripted {
    scripted(&[
        (
            "/api/v1/runs/r2",
            200,
            json!({
                "run": run_at("r2", "sync", "running", 2),
                "tasks": [
                    task("a", "running", None, None),
                    task("b", "success", Some("SUCCESS"), None),
                    task("first", "success", Some("SUCCESS"), None),
                    task("after", "queued", None, None),
                ],
                "code_hashes": ["sha256:a"],
                "params": {}
            }),
        ),
        (
            "/api/v1/runs/r2/dag",
            200,
            json!({"id": "sync", "tasks": tasks, "edges": edges}),
        ),
        ("/api/v1/runs/r2/commands", 200, json!({"commands": []})),
        (
            "/api/v1/runs/r2/tasks/first/output",
            200,
            json!({"outcome": "SUCCESS", "value": {"$value": 1, "winner": "b"}}),
        ),
    ])
}

fn first_of_edges() -> Value {
    json!([
        {"from": "a", "to": "first", "kind": "data"},
        {"from": "b", "to": "first", "kind": "data"},
        {"from": "first", "to": "after", "kind": "data"}
    ])
}

#[tokio::test]
async fn what_a_first_of_chooses_from_is_one_box_with_one_dashed_line() {
    let tasks = json!([
        {"id": "a"}, {"id": "b"}, {"id": "first", "kind": "first_of"}, {"id": "after"}
    ]);
    let app = ui(vec![deployment(
        "etl",
        first_of_run(&tasks, &first_of_edges()),
    )]);
    let (status, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        attrs(&html, "svg.dag rect.group-box", "class"),
        ["group-box options"]
    );
    assert_eq!(texts(&html, "svg.dag text.group-label"), ["first of"]);
    assert_eq!(
        attrs(&html, "svg.dag path.edge", "class"),
        ["edge data", "edge option"],
        "one line out of the box"
    );
    assert_eq!(
        attrs(&html, "svg.dag a.node.won", "data-task"),
        ["b"],
        "the winner is outlined"
    );
    assert_eq!(
        attrs(&html, "svg.dag a.node.first-of", "data-task"),
        ["first"]
    );
    assert!(
        texts(&html, "svg.dag a.node.first-of text.sub")[0].contains('b'),
        "{html}"
    );
}

#[tokio::test]
async fn at_least_says_its_count_on_its_box() {
    let tasks = json!([
        {"id": "a"}, {"id": "b"}, {"id": "first", "kind": "first_of", "at_least": 2}, {"id": "after"}
    ]);
    let app = ui(vec![deployment(
        "etl",
        first_of_run(&tasks, &first_of_edges()),
    )]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(texts(&html, "svg.dag text.group-label"), ["at least 2 of"]);
}

#[tokio::test]
async fn options_at_different_depths_keep_a_dashed_line_each() {
    let tasks = json!([
        {"id": "a"}, {"id": "b"}, {"id": "first", "kind": "first_of"}, {"id": "after"}
    ]);
    let edges = json!([
        {"from": "a", "to": "b", "kind": "order"},
        {"from": "a", "to": "first", "kind": "data"},
        {"from": "b", "to": "first", "kind": "data"},
        {"from": "first", "to": "after", "kind": "data"}
    ]);
    let app = ui(vec![deployment("etl", first_of_run(&tasks, &edges))]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(
        attrs(&html, "svg.dag rect.group-box", "class"),
        Vec::<String>::new()
    );
    let classes = attrs(&html, "svg.dag path.edge", "class");
    assert_eq!(
        classes.iter().filter(|c| *c == "edge option").count(),
        2,
        "{classes:?}"
    );
}

#[tokio::test]
async fn a_task_given_to_two_nodes_keeps_a_dashed_line_to_each() {
    let tasks = json!([
        {"id": "a"}, {"id": "b"}, {"id": "first", "kind": "first_of"},
        {"id": "after", "kind": "first_of"}
    ]);
    let edges = json!([
        {"from": "a", "to": "first", "kind": "data"},
        {"from": "b", "to": "first", "kind": "data"},
        {"from": "a", "to": "after", "kind": "data"},
        {"from": "b", "to": "after", "kind": "data"}
    ]);
    let app = ui(vec![deployment("etl", first_of_run(&tasks, &edges))]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(
        attrs(&html, "svg.dag rect.group-box", "class"),
        Vec::<String>::new()
    );
    let classes = attrs(&html, "svg.dag path.edge", "class");
    assert_eq!(
        classes.iter().filter(|c| *c == "edge option").count(),
        4,
        "{classes:?}"
    );
}

/// Run `c1` of `orders`, started by run `r2`'s `start`, a `TriggerDagOperator` whose attempt
/// started it.
fn chained_runs() -> Vec<(&'static str, u16, Value)> {
    let mut child = run_at("c1", "orders", "running", 1);
    child["kind"] = json!("chained");
    child["triggered_by"] = json!("r2");
    child["parent_run_id"] = json!("r2");
    let mut started = attempt(1, None);
    started["worker_id"] = Value::Null;
    started["exit_code"] = Value::Null;
    started["error_kind"] = Value::Null;
    started["error_summary"] = Value::Null;
    started["chained_run_id"] = json!("c1");
    vec![
        (
            "/api/v1/runs/c1",
            200,
            json!({"run": child, "tasks": [], "code_hashes": []}),
        ),
        (
            "/api/v1/runs/c1/dag",
            200,
            json!({"id": "orders", "tasks": [], "edges": []}),
        ),
        ("/api/v1/runs/c1/commands", 200, json!({"commands": []})),
        (
            "/api/v1/runs/r2",
            200,
            json!({
                "run": run_at("r2", "sync", "running", 1),
                "tasks": [task("start", "running", None, None)],
                "code_hashes": []
            }),
        ),
        (
            "/api/v1/runs/r2/dag",
            200,
            json!({"id": "sync", "tasks": [{"id": "start"}], "edges": []}),
        ),
        ("/api/v1/runs/r2/commands", 200, json!({"commands": []})),
        (
            "/api/v1/runs/r2/tasks/start/attempts",
            200,
            json!({"attempts": [started]}),
        ),
    ]
}

#[tokio::test]
async fn a_chained_run_and_the_attempt_that_started_it_link_each_other() {
    let app = ui(vec![deployment("etl", scripted(&chained_runs()))]);
    let (status, page) = get(&app, "/d/etl/runs/c1", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&page, ".facts a.mono"), ["run r2"]);
    assert_eq!(attrs(&page, ".facts a.mono", "href"), ["/d/etl/runs/r2"]);
    let (status, html) = get(
        &app,
        "/d/etl/runs/r2/tasks/start",
        &[("hx-request", "true")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "table.attempts td a"), ["run c1"]);
    assert_eq!(
        attrs(&html, "table.attempts td a", "href"),
        ["/d/etl/runs/c1"]
    );
}
