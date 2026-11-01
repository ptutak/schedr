//! Actions: confirmed in a dialog, posted from the UI's own pages only, shown until decided.

use axum::http::Method;
use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;
use crate::actions::same_origin;

fn command(id: &str, kind: &str, status: &str, reason: Option<&str>, result: &Value) -> Value {
    json!({
        "command_id": id, "kind": kind, "status": status, "reason": reason, "result": result,
        "requested_by": "anonymous", "requested_at": "2026-10-15T01:00:00Z", "decided_at": null
    })
}

async fn post(
    app: &axum::Router,
    path: &str,
    form: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/x-www-form-urlencoded");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(form.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

const SAME: (&str, &str) = ("sec-fetch-site", "same-origin");

#[rstest]
#[case::same_origin(&[("sec-fetch-site", "same-origin")], true)]
#[case::typed_by_the_user(&[("sec-fetch-site", "none")], true)]
#[case::another_site(&[("sec-fetch-site", "cross-site"), ("origin", "http://ui:8082"), ("host", "ui:8082")], false)]
#[case::same_site_other_origin(&[("sec-fetch-site", "same-site")], false)]
#[case::origin_is_the_host(&[("origin", "http://ui:8082"), ("host", "ui:8082")], true)]
#[case::origin_elsewhere(&[("origin", "http://evil.example"), ("host", "ui:8082")], false)]
#[case::behind_a_proxy(&[("origin", "https://schedr.example.com"), ("host", "ui:8082"), ("x-forwarded-host", "schedr.example.com")], true)]
#[case::nothing_to_tell(&[("host", "ui:8082")], false)]
#[case::a_default_port_on_either_side(&[("origin", "https://schedr.example.com"), ("host", "schedr.example.com:443")], true)]
#[case::the_wrong_default_port(&[("origin", "https://schedr.example.com"), ("host", "schedr.example.com:80")], false)]
#[case::a_null_origin(&[("origin", "null"), ("host", "ui:8082")], false)]
fn a_post_is_taken_from_the_uis_own_pages_only(
    #[case] headers: &[(&str, &str)],
    #[case] taken: bool,
) {
    let mut map = axum::http::HeaderMap::new();
    for (name, value) in headers {
        let name = axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap();
        map.insert(name, value.parse().unwrap());
    }
    assert_eq!(same_origin(&map), taken);
}

#[tokio::test]
async fn a_post_from_another_site_is_refused_before_anything_is_sent() {
    let app = ui(vec![deployment("etl", etl())]);
    let (status, html) = post(
        &app,
        "/d/etl/dags/sync/pause",
        "",
        &[("sec-fetch-site", "cross-site")],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(texts(&html, ".banner.warning strong"), ["Refused"]);
}

#[tokio::test]
async fn an_action_shows_its_command_until_the_master_decides() {
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/dags/sync/pause".to_owned(),
        (
            202,
            command("c1", "pause_dag", "pending", None, &json!(null)),
        ),
    );
    etl.answers.insert(
        "/api/v1/commands/c1".to_owned(),
        (
            200,
            command(
                "c1",
                "pause_dag",
                "applied",
                None,
                &json!({"cancelled_runs": []}),
            ),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (status, html) = post(&app, "/d/etl/dags/sync/pause", "cancel_runs=on", &[SAME]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attrs(&html, ".command", "class"), ["command pending"]);
    assert!(texts(&html, ".command")[0].starts_with("Pause sync and cancel its runs: sent"));
    // It reads itself again, a second later.
    let poll = attrs(&html, ".command", "hx-get");
    assert_eq!(
        poll,
        ["/d/etl/commands/c1?what=Pause%20sync%20and%20cancel%20its%20runs"]
    );
    assert_eq!(
        attrs(&html, ".command", "hx-trigger"),
        ["load delay:1000ms"]
    );
    let (status, html) = get(&app, &poll[0], &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attrs(&html, ".command", "class"), ["command applied"]);
    assert_eq!(attrs(&html, ".command", "hx-get"), Vec::<String>::new());
    assert_eq!(
        texts(&html, ".command"),
        ["Pause sync and cancel its runs: applied."]
    );
    // Decided, it no longer reads itself again, and still names its command.
    assert_eq!(attrs(&html, ".command", "data-command"), ["c1"]);
}

#[tokio::test]
async fn a_rejection_says_the_masters_reason_and_a_trigger_links_its_run() {
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/runs/r2/cancel".to_owned(),
        (
            202,
            command(
                "c2",
                "cancel_run",
                "rejected",
                Some("the run has ended"),
                &json!(null),
            ),
        ),
    );
    etl.answers.insert(
        "/api/v1/dags/sync/runs".to_owned(),
        (
            202,
            command(
                "c3",
                "trigger_run",
                "applied",
                None,
                &json!({"run_id": "r9"}),
            ),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = post(&app, "/d/etl/runs/r2/cancel", "", &[SAME]).await;
    assert_eq!(
        texts(&html, ".command"),
        ["Cancel run r2: rejected: the run has ended"]
    );
    let (_, html) = post(&app, "/d/etl/dags/sync/trigger", "for=", &[SAME]).await;
    assert_eq!(attrs(&html, ".command a", "href"), ["/d/etl/runs/r9"]);
    // A moment that doesn't read is never sent.
    let (_, html) = post(&app, "/d/etl/dags/sync/trigger", "for=soon", &[SAME]).await;
    assert_eq!(attrs(&html, ".command", "class"), ["command failed"]);
    assert!(texts(&html, ".command")[0].starts_with("Trigger sync: not sent:"));
}

#[tokio::test]
async fn a_failed_send_says_why() {
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![deployment("etl", down)]);
    // No answer: it may have arrived (B140), never "not sent".
    let (_, html) = post(&app, "/d/etl/workers/w1/drain", "", &[SAME]).await;
    assert!(
        texts(&html, ".command.unknown")[0].starts_with("Drain worker w1: may have arrived"),
        "{html}"
    );
    let (_, html) = post(&app, "/d/nowhere/workers/w1/drain", "", &[SAME]).await;
    assert_eq!(
        texts(&html, ".command"),
        ["Drain worker w1: not sent: There is no deployment `nowhere` here."]
    );
    let (status, _) = post(&app, "/d/etl/workers/w1/explode", "", &[SAME]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// What a POST's answer says about its command: its `Schedr-Command-Answer` header.
async fn answer_kind(app: &axum::Router, path: &str) -> Option<String> {
    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/x-www-form-urlencoded")
        .header(SAME.0, SAME.1)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    response
        .headers()
        .get("schedr-command-answer")
        .map(|value| value.to_str().unwrap().to_owned())
}

#[tokio::test]
async fn only_an_answer_from_the_ui_saying_whether_the_command_arrived_drops_the_key() {
    // Codex's review of B140: the browser dropped its key on any answer htmx swapped in, a
    // proxy's 502 too (the page swaps error answers in), though the command may have been
    // stored; sent again, it was a second run. Only the UI's own definite answer, marked by
    // its header, drops it.
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/dags/sync/runs".to_owned(),
        (
            202,
            command("c3", "trigger_run", "pending", None, &json!(null)),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    assert_eq!(
        answer_kind(&app, "/d/etl/dags/sync/trigger")
            .await
            .as_deref(),
        Some("definite")
    );
    assert_eq!(
        answer_kind(&app, "/d/nowhere/workers/w1/drain")
            .await
            .as_deref(),
        Some("definite"),
        "not sent: nothing stored"
    );
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![deployment("etl", down)]);
    assert_eq!(
        answer_kind(&app, "/d/etl/workers/w1/drain").await,
        None,
        "no answer from the webserver: may have arrived"
    );
    // The browser reads the header, not whether htmx took the answer as a success.
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert!(html.contains(r#"getResponseHeader("Schedr-Command-Answer") === "definite""#));
    assert!(!html.contains("event.detail.successful"));
}

#[tokio::test]
async fn the_trigger_dialog_shows_the_moment_in_both_zones() {
    let app = ui(vec![deployment("etl", etl())]);
    let zone = [("cookie", "schedr_tz=Europe%2FWarsaw")];
    let (_, now) = get(&app, "/time?for=", &zone).await;
    assert_eq!(now, "Now.");
    let (_, at) = get(&app, "/time?for=2026-10-15T01:00:00Z", &zone).await;
    assert_eq!(at, "2026-10-15 01:00 UTC (03:00 CEST)");
    let (_, naive) = get(&app, "/time?for=2026-10-15T01:00", &zone).await;
    assert!(naive.ends_with("(no offset given: read as UTC)"), "{naive}");
    let (_, bad) = get(&app, "/time?for=%3Cb%3E", &zone).await;
    assert!(bad.starts_with("<span class=\"error\">"), "{bad}");
    assert!(!bad.contains("<b>"), "{bad}");
}

#[tokio::test]
async fn a_dag_can_be_triggered_and_paused_from_its_page() {
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        texts(&html, ".actions > button.action"),
        ["Trigger", "Pause"]
    );
    // Inside the live part, so the buttons follow the DAG's state; the part passes nothing on
    // (its `hx-select` would empty the dialogs' answers).
    assert_eq!(
        texts(&html, "#live .actions > button.action"),
        ["Trigger", "Pause"]
    );
    assert_eq!(attrs(&html, "#live", "hx-disinherit"), ["*"]);
    assert_eq!(
        attrs(&html, ".actions dialog form", "hx-post"),
        ["/d/etl/dags/sync/trigger", "/d/etl/dags/sync/pause"]
    );
    // The results go to the top of the page's list; the trigger's moment is previewed.
    assert_eq!(
        attrs(&html, ".actions dialog form", "hx-target"),
        ["#action-results"; 2]
    );
    assert_eq!(texts(&html, "#action-results").len(), 1);
    assert_eq!(attrs(&html, "dialog input[name=for]", "hx-get"), ["/time"]);
    // The preview swaps into its own place (not the form's way), and its answer doesn't close
    // the dialog (only the form's own does).
    assert_eq!(
        attrs(&html, "dialog input[name=for]", "hx-swap"),
        ["innerHTML"]
    );
    for close in attrs(&html, ".actions dialog form", "hx-on::after-request") {
        assert_eq!(close, "schedrAnswered(this, event)");
    }
    assert!(html.contains("if (event.detail.elt !== form) return;"));
    // Each dialog opens with the action's key (B140), kept until an answer says whether the
    // command arrived (see `only_an_answer_from_the_ui_saying_whether_…`).
    for open in attrs(&html, ".actions button.action", "onclick") {
        assert_eq!(open, "schedrOpen(this)");
    }
    assert_eq!(
        attrs(&html, "dialog input[name=cancel_runs]", "type"),
        ["checkbox"]
    );
    // One submit at a time (a double click would trigger twice).
    assert_eq!(
        attrs(&html, ".actions dialog form", "hx-sync"),
        ["this:drop"; 2]
    );
    assert_eq!(
        attrs(&html, ".actions dialog form", "hx-disabled-elt"),
        ["find button[type=submit]"; 2]
    );
    // Its live part waits while a dialog is open.
    assert_eq!(
        attrs(&html, "#live", "hx-trigger"),
        ["sse:change[!schedrDialogOpen()]"]
    );
    // A paused one offers Unpause instead.
    let mut paused = etl();
    paused
        .answers
        .insert("/api/v1/dags/sync".to_owned(), (200, dag("sync", true)));
    let app = ui(vec![deployment("etl", paused)]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        texts(&html, ".actions > button.action"),
        ["Trigger", "Unpause"]
    );
}

#[tokio::test]
async fn a_run_and_its_tasks_offer_what_their_state_allows() {
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/runs/r2".to_owned(),
        (
            200,
            json!({
                "run": {
                    "run_id": "r2", "dag_id": "sync", "kind": "manual", "state": "running",
                    "planned_for": "2026-10-15T01:00:00Z", "since": "2026-10-14T01:00:00Z",
                    "triggered_at": "2026-10-15T01:00:02Z", "triggered_by": "anna",
                    "started_at": null, "ended_at": null
                },
                "tasks": [
                    {"task_id": "extract", "state": "success", "outcome": "SUCCESS",
                     "try_number": 1, "reason": null, "updated_at": "2026-10-15T01:05:00Z"},
                    {"task_id": "load", "state": "unschedulable", "outcome": null,
                     "try_number": 0, "reason": "no worker", "updated_at": "2026-10-15T01:05:00Z"},
                ],
                "code_hashes": []
            }),
        ),
    );
    etl.answers.insert(
        "/api/v1/runs/r2/dag".to_owned(),
        (200, json!({"tasks": [], "edges": []})),
    );
    etl.answers.insert(
        "/api/v1/runs/r2/commands".to_owned(),
        (200, json!({"commands": []})),
    );
    etl.answers.insert(
        "/api/v1/runs/r2/tasks/extract/attempts".to_owned(),
        (200, json!({"attempts": []})),
    );
    etl.answers.insert(
        "/api/v1/runs/r2/tasks/load/attempts".to_owned(),
        (200, json!({"attempts": []})),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(&app, "/d/etl/runs/r2", &[]).await;
    assert_eq!(
        attrs(&html, ".actions dialog form", "hx-post"),
        ["/d/etl/runs/r2/cancel"]
    );
    let fragment = [("hx-request", "true")];
    // Ended: nothing to do.
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/extract", &fragment).await;
    assert_eq!(texts(&html, ".actions"), Vec::<String>::new());
    // Rejected by every worker: cancel or retry.
    let (_, html) = get(&app, "/d/etl/runs/r2/tasks/load", &fragment).await;
    assert_eq!(
        attrs(&html, ".actions dialog form", "hx-post"),
        [
            "/d/etl/runs/r2/tasks/load/cancel",
            "/d/etl/runs/r2/tasks/load/retry"
        ]
    );
}

pub(super) fn worker(id: &str, state: &str, drained: bool, warnings: &Value) -> Value {
    json!({
        "worker_id": id, "state": state, "incarnation_id": "i1", "queues": ["default", "gpu"],
        "slots": 4, "running": 1, "schedr_version": "0.1.0", "protocol_version": "1.0",
        "log_endpoint": "w:8081", "registered_at": "2026-10-15T01:00:00Z",
        "last_heartbeat_at": "2026-10-15T01:00:00Z", "offline_at": null, "drained": drained,
        "warnings": warnings
    })
}

#[tokio::test]
async fn the_workers_page_shows_each_with_its_warnings_and_drain_or_resume() {
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/workers".to_owned(),
        (
            200,
            json!({"workers": [
                worker("w2", "active", true, &json!([])),
                worker("w1", "active", false, &json!([{"kind": "fork_server_disabled",
                    "interpreter": "python", "message": "Python tasks start slowly"}])),
                worker("w3", "lost", false, &json!([])),
            ]}),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (status, html) = get(&app, "/workers", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "table.workers tbody td.mono:first-child"),
        ["w1", "w2", "w3"]
    );
    assert_eq!(texts(&html, ".warning-text"), ["Python tasks start slowly"]);
    assert_eq!(
        texts(&html, "table.workers td")
            .iter()
            .filter(|t| *t == "1 / 4")
            .count(),
        3
    );
    // Drain an active one, resume a drained one; a lost one offers nothing.
    assert_eq!(
        attrs(&html, "dialog form", "hx-post"),
        ["/d/etl/workers/w1/drain", "/d/etl/workers/w2/resume"]
    );
    assert_eq!(events_followed(&html), "/events?kinds=workers");
    // A warning row spans the table (one deployment: no Deployment column).
    assert_eq!(texts(&html, "table.workers thead th").len(), 8);
    assert_eq!(attrs(&html, "tr.error-row td[colspan]", "colspan"), ["7"]);
    // The dialogs wait out the live reloads.
    assert_eq!(
        attrs(&html, "#live", "hx-trigger"),
        ["sse:change[!schedrDialogOpen()]"]
    );
}

#[tokio::test]
async fn a_command_that_cant_be_read_now_is_read_again_never_called_not_sent() {
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/dags/sync/runs".to_owned(),
        (
            202,
            command("c4", "trigger_run", "pending", None, &json!(null)),
        ),
    );
    // `/api/v1/commands/c4` isn't scripted: 404, as from a webserver restarting.
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = post(&app, "/d/etl/dags/sync/trigger", "for=", &[SAME]).await;
    let poll = attrs(&html, ".command", "hx-get");
    let (status, html) = get(&app, &poll[0], &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attrs(&html, ".command", "class"), ["command pending"]);
    assert_eq!(attrs(&html, ".command", "hx-get"), poll);
    let text = &texts(&html, ".command")[0];
    assert!(text.contains("can't read it now"), "{text}");
    assert!(!text.contains("not sent"), "{text}");
}

#[tokio::test]
async fn the_uis_pages_cant_be_framed_by_another_site() {
    let app = ui(vec![deployment("etl", etl())]);
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.headers()["x-frame-options"], "DENY");
    assert_eq!(
        response.headers()["content-security-policy"],
        "frame-ancestors 'none'"
    );
}

/// A webserver storing each command once per `Idempotency-Key`, whose first answer is lost
/// after the command was stored (B140, as Codex's proof in F11).
#[derive(Debug, Clone, Default)]
struct LosesFirstAnswer {
    /// The commands stored, by key (none: a command without a key, stored each time).
    stored: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
    /// The keys each POST came with.
    keys: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

impl Transport for LosesFirstAnswer {
    async fn send(&self, request: ApiRequest) -> Result<ApiResponse, ClientError> {
        tokio::task::yield_now().await;
        self.keys.lock().unwrap().push(request.key.clone());
        let first = {
            let mut stored = self.stored.lock().unwrap();
            let known = request.key.is_some() && stored.contains(&request.key);
            if !known {
                stored.push(request.key.clone());
            }
            stored.len() == 1 && self.keys.lock().unwrap().len() == 1
        };
        if first {
            return Err(ClientError::Connect {
                server: "http://etl-webserver:8080".to_owned(),
                reason: "the answer was lost".to_owned(),
            });
        }
        Ok(ApiResponse {
            status: 202,
            body: command("c1", "trigger_run", "pending", None, &json!(null)),
        })
    }
}

#[tokio::test]
async fn a_command_whose_answer_was_lost_is_sent_again_under_its_key_never_twice() {
    // B140: the webserver stored the trigger but its answer was lost. The UI said "not sent",
    // inviting a second trigger (a second run).
    let webserver = LosesFirstAnswer::default();
    let app = router(Ui {
        deployments: vec![Deployment {
            name: "etl".to_owned(),
            client: Client::new(
                webserver.clone(),
                Duration::from_millis(1),
                Duration::from_secs(1),
            ),
            proof: "none",
        }],
        docs_dir: None,
        auth: schedr_api::auth::Auth::default(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    });
    let (_, html) = post(
        &app,
        "/d/etl/dags/sync/trigger",
        "",
        &[SAME, ("idempotency-key", "k1")],
    )
    .await;
    let shown = texts(&html, ".command");
    assert!(shown[0].starts_with("Trigger sync: sent;"), "{shown:?}");
    assert_eq!(webserver.stored.lock().unwrap().len(), 1);
    assert_eq!(
        *webserver.keys.lock().unwrap(),
        [Some("k1".to_owned()), Some("k1".to_owned())]
    );
}
