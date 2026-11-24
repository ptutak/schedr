use std::collections::BTreeMap;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use schedr_client::{ApiRequest, ApiResponse, Client, ClientError, Transport};
use scraper::{Html, Selector};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;

mod actions;
mod connections;
mod deployments;
mod metrics;
mod runs;

/// A webserver's API answering from a script: path -> (status, body); anything else 404.
#[derive(Debug, Clone, Default)]
struct Scripted {
    answers: BTreeMap<String, (u16, Value)>,
    down: bool,
    /// Refuses every connection, nothing sent (B215).
    refused: bool,
    /// The bodies of the requests sent, by path, in order (shared by its clones).
    sent: Sent,
}

/// The requests a [`Scripted`] was sent: each one's path and body.
type Sent = std::sync::Arc<std::sync::Mutex<Vec<(String, Option<Value>)>>>;

impl Transport for Scripted {
    fn send(
        &self,
        request: ApiRequest,
    ) -> impl Future<Output = Result<ApiResponse, ClientError>> + Send {
        self.sent
            .lock()
            .unwrap()
            .push((request.path.clone(), request.body.clone()));
        let answer = if self.refused {
            Err(ClientError::Unreachable {
                server: "http://etl-webserver:8080".to_owned(),
                reason: "connection refused".to_owned(),
            })
        } else if self.down {
            Err(ClientError::Connect {
                server: "http://etl-webserver:8080".to_owned(),
                reason: "connection refused".to_owned(),
            })
        } else {
            let (status, body) = self
                .answers
                .get(&request.path)
                .cloned()
                .unwrap_or((404, json!({"error": "not found"})));
            Ok(ApiResponse { status, body })
        };
        std::future::ready(answer)
    }
}

fn dag(id: &str, paused: bool) -> Value {
    json!({
        "dag_id": id, "trigger_state": if paused { "paused" } else { "active" },
        "location": {"root": "etl", "path": format!("{id}.py")},
        "conflict": null,
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "Europe/Warsaw",
                    "catchup": false, "start": null, "end": null},
        "description": "Sync the orders.", "tags": ["etl", "daily"]
    })
}

fn run(id: &str, state: &str) -> Value {
    json!({
        "run_id": id, "dag_id": "sync", "kind": "scheduled", "state": state,
        "planned_for": "2026-10-15T01:00:00Z", "since": "2026-10-14T01:00:00Z",
        "triggered_at": "2026-10-15T01:00:02Z", "triggered_by": "scheduler",
        "started_at": "2026-10-15T01:00:03Z", "ended_at": null
    })
}

fn etl() -> Scripted {
    let mut answers = BTreeMap::new();
    answers.insert(
        "/api/v1/dags".to_owned(),
        (
            200,
            json!({"dags": [dag("sync", false), dag("archive", true)]}),
        ),
    );
    answers.insert("/api/v1/dags/sync".to_owned(), (200, dag("sync", false)));
    answers.insert(
        "/api/v1/dags/sync/runs".to_owned(),
        (
            200,
            json!({"runs": [run("r2", "running"), run("r1", "success")]}),
        ),
    );
    Scripted {
        answers,
        ..Scripted::default()
    }
}

fn deployment(name: &str, transport: Scripted) -> Deployment<Scripted> {
    Deployment {
        name: name.to_owned(),
        client: Client::new(transport, Duration::from_millis(1), Duration::from_secs(1)),
        proof: "none",
    }
}

fn ui(deployments: Vec<Deployment<Scripted>>) -> axum::Router {
    router(Ui {
        deployments,
        docs_dir: None,
        timings: crate::live::UiTimings::default(),
        auth: schedr_api::auth::Auth::default(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    })
}

async fn get(app: &axum::Router, path: &str, headers: &[(&str, &str)]) -> (StatusCode, String) {
    let mut request = Request::builder().uri(path);
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

fn texts(html: &str, selector: &str) -> Vec<String> {
    let page = Html::parse_document(html);
    page.select(&Selector::parse(selector).unwrap())
        .map(|e| e.text().collect::<String>().trim().to_owned())
        .collect()
}

/// The page's event stream, without when the page was made (checked to be there: a change
/// after it is sent as the stream opens).
fn events_followed(html: &str) -> String {
    let url = attrs(html, "body", "data-events")
        .pop()
        .expect("a live page");
    let (followed, since) = url.rsplit_once("since=").expect("its time");
    since.parse::<i64>().expect("milliseconds");
    followed.trim_end_matches(['&', '?']).to_owned()
}

fn attrs(html: &str, selector: &str, attr: &str) -> Vec<String> {
    let page = Html::parse_document(html);
    page.select(&Selector::parse(selector).unwrap())
        .filter_map(|e| e.value().attr(attr).map(str::to_owned))
        .collect()
}

/// The Trigger column names each trigger's kind before the trigger itself; a DAG without one
/// is `Manual`, with nothing after it.
#[tokio::test]
async fn the_list_names_each_dags_kind_of_trigger() {
    let mut scripted = etl();
    let with = |id: &str, trigger: Value| {
        let mut dag = dag(id, false);
        if trigger["kind"] == "none" {
            dag["trigger_state"] = Value::Null;
        }
        dag["trigger"] = trigger;
        dag
    };
    let dags = [
        with(
            "a_every",
            json!({"kind": "every", "interval_ms": 3_600_000}),
        ),
        with(
            "b_once",
            json!({"kind": "once", "at": "2026-12-24T22:00:00Z"}),
        ),
        with(
            "c_after",
            json!({"kind": "run_of", "run_of": {"dag": "etl", "task": "load"}}),
        ),
        with("d_chained", json!({"kind": "chained", "by": ["ingest"]})),
        with(
            "e_event",
            json!({"kind": "event", "task": {"id": "listen"}}),
        ),
        with(
            "f_posted",
            json!({"kind": "event_named", "event": {"name": "paid"}}),
        ),
        with("g_manual", json!({"kind": "none"})),
    ];
    scripted
        .answers
        .insert("/api/v1/dags".to_owned(), (200, json!({"dags": dags})));
    let app = ui(vec![deployment("etl", scripted)]);
    let (_, html) = get(&app, "/", &[]).await;
    assert_eq!(
        texts(&html, "table.dags td.trigger .trigger-kind"),
        [
            "Schedule", "Schedule", "Run of", "Chained", "Event", "Event", "Manual"
        ]
    );
    assert_eq!(
        texts(&html, "table.dags td.trigger .mono"),
        [
            "every 1h",
            "once at 2026-12-24 22:00Z",
            "after etl load",
            "chained from ingest",
            "@trigger listen",
            "event paid"
        ]
    );
    // Each trigger's switch beside it; a DAG without one has none (decision 459).
    assert_eq!(
        texts(&html, "table.dags td.trigger .badge"),
        ["active", "active", "active", "active", "active", "active"]
    );
    assert!(texts(&html, "table.dags th .th-filters > span").contains(&"Trigger".to_owned()));
}

/// M5.2j (decision 517): the DAGs the filters show, 50 a page, numbered; the filters and the
/// page kept in the numbers and the live update, any filter's link back to the first page.
#[tokio::test]
async fn the_dag_list_is_numbered_in_pages_of_what_the_filters_show() {
    let mut scripted = etl();
    let dags: Vec<Value> = (0..120).map(|i| dag(&format!("d{i:03}"), false)).collect();
    scripted
        .answers
        .insert("/api/v1/dags".to_owned(), (200, json!({"dags": dags})));
    let app = ui(vec![deployment("etl", scripted)]);
    let (_, html) = get(&app, "/", &[]).await;
    assert_eq!(texts(&html, "table.dags tbody tr").len(), 50);
    assert_eq!(texts(&html, ".pager .page.current"), ["1"]);
    assert_eq!(texts(&html, ".pager a.page:not(.next)"), ["2", "3"]);
    assert!(
        texts(&html, ".pager .more").is_empty(),
        "the third is the last"
    );
    let (_, html) = get(&app, "/?page=3", &[]).await;
    assert_eq!(texts(&html, "table.dags tbody tr").len(), 20);
    assert!(
        texts(&html, ".pager a[rel=next]").is_empty(),
        "the last page"
    );
    let previous = attrs(&html, ".pager a[rel=prev]", "href");
    assert!(previous[0].ends_with("&page=2"), "{previous:?}");
    // The live update reads this page again.
    assert!(attrs(&html, "#live", "hx-get")[0].contains("page=3"));
    // With a filter: its DAGs paged, the filter in every number.
    let (_, html) = get(&app, "/?name=d0&page=2", &[]).await;
    assert_eq!(
        texts(&html, "table.dags tbody tr").len(),
        50,
        "d050 to d099"
    );
    assert!(
        attrs(&html, ".pager a.page", "href")
            .iter()
            .all(|h| h.contains("name=d0")),
        "the filter kept"
    );
    // Past the last: an empty page, said so, the numbers back.
    let (_, html) = get(&app, "/?page=9", &[]).await;
    assert!(texts(&html, "table.dags td.empty")[0].contains("on this page"));
    let previous = attrs(&html, ".pager a[rel=prev]", "href");
    assert!(previous[0].ends_with("&page=8"), "{previous:?}");
    // Any page in the address draws a few numbers (agy's review of M5.2j: as many as it said).
    let (status, html) = get(&app, "/?page=4294967295", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        texts(&html, ".pager .page").len() <= 8,
        "first, gap, three, Previous and Next"
    );
}

/// A DAG's description shows under its ID in the list, a long one cut to a line with the
/// whole of it in the tooltip; one without shows nothing there.
#[tokio::test]
async fn the_list_shows_each_dags_description() {
    let mut scripted = etl();
    let mut archive = dag("archive", true);
    archive["description"] = Value::Null;
    scripted.answers.insert(
        "/api/v1/dags".to_owned(),
        (200, json!({"dags": [archive, dag("sync", false)]})),
    );
    let app = ui(vec![deployment("etl", scripted)]);
    let (_, html) = get(&app, "/", &[]).await;
    assert_eq!(texts(&html, "table.dags .summary"), ["Sync the orders."]);
    assert_eq!(
        attrs(&html, "table.dags .summary", "title"),
        ["Sync the orders."]
    );
}

#[tokio::test]
async fn the_dags_are_listed_by_id_with_their_state_and_schedule() {
    let app = ui(vec![deployment("etl", etl())]);
    let (status, html) = get(&app, "/", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "table.dags tbody td:first-child a"),
        ["archive", "sync"]
    );
    assert_eq!(
        attrs(&html, "table.dags tbody td:first-child a", "href"),
        ["/d/etl/dags/archive", "/d/etl/dags/sync"]
    );
    assert_eq!(
        texts(&html, "table.dags td.trigger .badge"),
        ["paused", "active"]
    );
    // The trigger's kind, then the trigger as people write it.
    assert_eq!(
        texts(&html, "table.dags td.trigger .trigger-kind"),
        ["Schedule", "Schedule"]
    );
    assert_eq!(
        texts(&html, "table.dags td.trigger .mono"),
        ["0 3 * * * (Europe/Warsaw)", "0 3 * * * (Europe/Warsaw)"]
    );
    // Where each is defined, as the CLI writes it.
    assert_eq!(
        texts(&html, "table.dags td.mono.dim"),
        ["etl:archive.py", "etl:sync.py"]
    );
    // One deployment: no column naming it.
    assert!(!texts(&html, "table.dags th").contains(&"Deployment".to_owned()));
    // The stylesheet, and both themes' script before the page.
    assert_eq!(
        attrs(&html, "link[rel=stylesheet]", "href"),
        ["/assets/style.css"]
    );
    assert!(html.contains("prefers-color-scheme: dark"));
    // Live parts: a change during a read is read again after it (B141, B142), a failed read
    // leaves the part, saying so (B143); the browser's tabs share one stream (decision 175).
    // Shown in a browser (headless Chromium, the vendored htmx) when fixed.
    for hook in [
        "owed[read.name]",
        "event.detail.shouldSwap = false;\n    stale(elt",
        "new SharedWorker(base + \"/assets/live.js\"",
    ] {
        assert!(html.contains(hook), "{hook}");
    }
}

#[tokio::test]
async fn links_follow_the_proxys_prefix() {
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/", &[("x-forwarded-prefix", "/schedr/")]).await;
    assert_eq!(
        attrs(&html, "link[rel=stylesheet]", "href"),
        ["/schedr/assets/style.css"]
    );
    assert_eq!(
        attrs(&html, "table.dags tbody td:first-child a", "href"),
        ["/schedr/d/etl/dags/archive", "/schedr/d/etl/dags/sync"]
    );
}

#[tokio::test]
async fn a_deployment_that_doesnt_answer_is_shown_unavailable_and_the_others_listed() {
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![deployment("etl", etl()), deployment("ml", down)]);
    let (status, html) = get(&app, "/", &[]).await;
    assert_eq!(status, StatusCode::OK);
    let banners = texts(&html, ".banner.warning");
    assert_eq!(banners.len(), 1);
    assert!(
        banners[0].starts_with("Unavailable: ml: can't reach"),
        "{banners:?}"
    );
    assert_eq!(
        texts(&html, "table.dags tbody td:first-child a"),
        ["archive", "sync"]
    );
    // Several deployments: a column names each DAG's.
    assert!(texts(&html, "table.dags th").contains(&"Deployment".to_owned()));
}

/// Two deployments' DAGs with triggers and tags: `nightly` (a schedule, active, tag `etl`),
/// `hourly` (a schedule, paused, tags `etl` and `ml`), `train` (manual, tag `ml`), `orders`
/// (an event, active).
fn filtered() -> Scripted {
    let mut etl = etl();
    let with = |id: &str, trigger: Value, paused: Option<bool>, tags: &[&str]| {
        let mut dag = dag(id, false);
        dag["trigger"] = trigger;
        dag["trigger_state"] = match paused {
            Some(true) => json!("paused"),
            Some(false) => json!("active"),
            None => Value::Null,
        };
        dag["tags"] = json!(tags);
        dag
    };
    let cron = json!({"kind": "cron", "expression": "0 3 * * *"});
    etl.answers.insert(
        "/api/v1/dags".to_owned(),
        (
            200,
            json!({"dags": [
                with("nightly", cron.clone(), Some(false), &["etl"]),
                with("hourly", cron, Some(true), &["etl", "ml"]),
                with("train", Value::Null, None, &["ml"]),
                with("orders", json!({"kind": "event", "name": "paid"}), Some(false), &[]),
            ]}),
        ),
    );
    etl
}

#[tokio::test]
async fn the_dags_page_filters_by_name_switch_kind_and_tag() {
    // The user, 2026-11-16: a toggle for paused, active or all; a name filter; boxes for tags
    // and trigger kinds. Within a group any box; the groups together.
    let app = ui(vec![deployment("etl", filtered())]);
    let shown = |html: &str| texts(html, "table.dags tbody td:first-child a");
    let (_, html) = get(&app, "/", &[]).await;
    assert_eq!(shown(&html), ["hourly", "nightly", "orders", "train"]);
    // The DAG title sorts the other way, keeping the filters (the user, 2026-11-16).
    assert_eq!(
        attrs(&html, "table.dags th[aria-sort]", "aria-sort"),
        ["ascending"]
    );
    assert_eq!(
        attrs(&html, "table.dags th a.sort", "href"),
        ["/?view=&trigger=&order=desc"]
    );
    let (_, sorted) = get(&app, "/?trigger=&name=r&order=desc", &[]).await;
    assert_eq!(shown(&sorted), ["train", "orders", "hourly"]);
    assert_eq!(
        attrs(&sorted, "table.dags th[aria-sort]", "aria-sort"),
        ["descending"]
    );
    assert_eq!(
        attrs(&sorted, "table.dags th a.sort", "href"),
        ["/?view=&trigger=&name=r"]
    );
    assert_eq!(attrs(&sorted, "input[name=order]", "value"), ["desc"]);
    assert_eq!(
        texts(&html, "#dag-switch-menu label"),
        ["All 4", "Active 2", "Paused 1"],
        "a manual DAG is only under All"
    );
    assert_eq!(
        texts(&html, "#dag-kind-menu label, #dag-tag-menu label"),
        ["Event 1", "Manual 1", "Schedule 2", "etl 2", "ml 2"]
    );

    let (_, html) = get(&app, "/?trigger=paused", &[]).await;
    assert_eq!(shown(&html), ["hourly"]);
    let (_, html) = get(&app, "/?trigger=active", &[]).await;
    assert_eq!(shown(&html), ["nightly", "orders"]);
    // Counted within the other filters: the active ones' kinds and tags.
    assert_eq!(
        texts(&html, "#dag-kind-menu label, #dag-tag-menu label"),
        ["Event 1", "Manual 0", "Schedule 1", "etl 1", "ml 0"]
    );

    // A name, any case; spaces as forms send them.
    let (_, html) = get(&app, "/?name=HOUR", &[]).await;
    assert_eq!(shown(&html), ["hourly"]);
    assert_eq!(attrs(&html, "#dag-name", "value"), ["HOUR"]);
    let (_, html) = get(&app, "/?name=no+such", &[]).await;
    assert_eq!(
        texts(&html, "table.dags td.empty"),
        ["No DAGs match these filters."]
    );

    // Tags: any of those ticked; with a kind: both.
    let (_, html) = get(&app, "/?tag=ml", &[]).await;
    assert_eq!(shown(&html), ["hourly", "train"]);
    let (_, html) = get(&app, "/?tag=ml&tag=etl", &[]).await;
    assert_eq!(shown(&html), ["hourly", "nightly", "train"]);
    assert_eq!(
        attrs(
            &html,
            "#dag-kind-menu input[checked], #dag-tag-menu input[checked]",
            "value"
        ),
        ["etl", "ml"]
    );
    let (_, html) = get(&app, "/?tag=ml&kind=Schedule", &[]).await;
    assert_eq!(shown(&html), ["hourly"]);
    let (_, html) = get(&app, "/?kind=Run%20of", &[]).await;
    assert!(shown(&html).is_empty(), "a kind no DAG has still filters");
    assert_eq!(
        attrs(
            &html,
            "#dag-kind-menu input[checked], #dag-tag-menu input[checked]",
            "value"
        ),
        ["Run of"],
        "and can be unticked"
    );

    // The live reload keeps every filter; the switch is remembered, the rest isn't.
    let (_, html) = get(&app, "/?trigger=active&name=ord&tag=x&kind=Event", &[]).await;
    assert_eq!(
        attrs(&html, "#live", "hx-get"),
        ["/?view=&trigger=active&name=ord&tag=x&kind=Event"]
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/?trigger=paused&name=x")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookies: Vec<_> = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        cookies,
        ["schedr_dags_trigger=paused; Path=/; Max-Age=31536000; SameSite=Lax"]
    );
    let (_, html) = get(&app, "/", &[("cookie", "schedr_dags_trigger=paused")]).await;
    assert_eq!(shown(&html), ["hourly"]);
}

/// A deployment of one DAG, `train`.
fn ml_like() -> Scripted {
    let mut ml = etl();
    ml.answers.insert(
        "/api/v1/dags".to_owned(),
        (200, json!({"dags": [dag("train", false)]})),
    );
    ml
}

#[tokio::test]
async fn the_dags_page_shows_all_deployments_or_one_and_remembers_the_choice() {
    // M5.1l (the user, 2026-11-14): all DAGs in one list, or a tab per deployment with its
    // DAGs only, remembered per viewer.
    let app = ui(vec![deployment("etl", etl()), deployment("ml", ml_like())]);
    let (_, html) = get(&app, "/", &[]).await;
    assert_eq!(texts(&html, "nav.dag-views a"), ["All 3", "etl 2", "ml 1"]);
    assert_eq!(
        attrs(&html, "nav.dag-views a.open", "href"),
        ["/?view=&trigger="]
    );
    assert_eq!(
        texts(&html, "table.dags tbody td:first-child a"),
        ["archive", "sync", "train"]
    );
    assert_eq!(attrs(&html, "#live", "hx-get"), ["/?view=&trigger="]);

    // One deployment's: its DAGs only, no Deployment column, kept by its live reload, and
    // remembered.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/?view=ml")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()["set-cookie"],
        "schedr_dags_view=ml; Path=/; Max-Age=31536000; SameSite=Lax"
    );
    let (_, html) = get(&app, "/?view=ml", &[]).await;
    assert_eq!(texts(&html, "table.dags tbody td:first-child a"), ["train"]);
    assert!(!texts(&html, "table.dags th").contains(&"Deployment".to_owned()));
    assert_eq!(
        attrs(&html, "nav.dag-views a.open", "href"),
        ["/?view=ml&trigger="]
    );
    assert_eq!(attrs(&html, "#live", "hx-get"), ["/?view=ml&trigger="]);
    let (_, html) = get(&app, "/", &[("cookie", "schedr_dags_view=ml")]).await;
    assert_eq!(texts(&html, "table.dags tbody td:first-child a"), ["train"]);

    // A name no deployment has: all, and remembered as all (empty).
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/?view=gone")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()["set-cookie"],
        "schedr_dags_view=; Path=/; Max-Age=31536000; SameSite=Lax"
    );
    let (_, html) = get(&app, "/", &[("cookie", "schedr_dags_view=gone")]).await;
    assert_eq!(texts(&html, "table.dags tbody tr").len(), 3);

    // A deployment named `all` has its own tab beside All (Codex's review of M5.1l1).
    let app = ui(vec![
        deployment("all", etl()),
        deployment("prod", ml_like()),
    ]);
    let (_, html) = get(&app, "/?view=", &[]).await;
    assert_eq!(
        texts(&html, "nav.dag-views a"),
        ["All 3", "all 2", "prod 1"]
    );
    assert_eq!(texts(&html, "table.dags tbody tr").len(), 3);
    let (_, html) = get(&app, "/?view=all", &[]).await;
    assert_eq!(texts(&html, "table.dags tbody tr").len(), 2);

    // One deployment: no tabs.
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/", &[]).await;
    assert_eq!(texts(&html, "nav.dag-views a"), Vec::<String>::new());
}

#[tokio::test]
async fn a_dag_page_shows_its_facts_and_runs_in_utc_and_local_time() {
    let app = ui(vec![deployment("etl", etl())]);
    let cookie = ("cookie", "schedr_tz=Europe%2FWarsaw");
    let (status, html) = get(&app, "/d/etl/dags/sync", &[cookie]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "h1"), ["sync"]);
    assert_eq!(texts(&html, "dd.trigger .badge"), ["active"]);
    assert_eq!(texts(&html, ".description"), ["Sync the orders."]);
    assert_eq!(texts(&html, "table.runs tbody td.mono"), ["r2", "r1"]);
    assert_eq!(
        attrs(&html, "table.runs .state", "class"),
        ["state running", "state success"]
    );
    assert_eq!(
        texts(&html, "table.runs tbody td:nth-child(3)"),
        [
            "2026-10-15 01:00:00 UTC (03:00:00 CEST)",
            "2026-10-15 01:00:00 UTC (03:00:00 CEST)"
        ]
    );
    // The zone is known: no reload, no "UTC only" note.
    assert!(!html.contains("schedr-tz-reloaded"));
    assert_eq!(texts(&html, "footer.note"), Vec::<String>::new());
}

#[tokio::test]
async fn without_the_zone_times_are_utc_and_the_page_says_so() {
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        texts(&html, "table.runs tbody td:nth-child(3)")[0],
        "2026-10-15 01:00:00 UTC"
    );
    assert!(html.contains("schedr-tz-reloaded"));
    assert_eq!(
        texts(&html, "footer.note"),
        ["Times in UTC (the local time zone isn't known yet)."]
    );
}

#[tokio::test]
async fn unknown_dags_deployments_and_paths_are_404_pages() {
    let app = ui(vec![deployment("etl", etl())]);
    for (path, says) in [
        ("/d/etl/dags/nope", "`nope` isn't a DAG of etl."),
        ("/d/ml/dags/sync", "There is no deployment `ml` here."),
        ("/no/such/page", "There is no such page."),
    ] {
        let (status, html) = get(&app, path, &[]).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(texts(&html, "main p:first-of-type"), [says], "{path}");
    }
}

#[tokio::test]
async fn a_deployment_down_on_a_dag_page_is_a_502() {
    let down = Scripted {
        down: true,
        ..Scripted::default()
    };
    let app = ui(vec![deployment("etl", down)]);
    let (status, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(texts(&html, "h1"), ["Deployment unavailable"]);
}

#[tokio::test]
async fn the_stylesheet_has_both_themes() {
    let app = ui(vec![]);
    let (status, css) = get(&app, "/assets/style.css", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        css.contains(r#":root[data-theme="dark"]"#) && css.contains(r#":root[data-theme="light"]"#)
    );
}

#[tokio::test]
async fn the_documentation_is_served_from_its_directory_and_linked() {
    let site = tempfile::TempDir::new().unwrap();
    std::fs::write(site.path().join("index.html"), "<h1>schedr docs</h1>").unwrap();
    std::fs::create_dir(site.path().join("_static")).unwrap();
    std::fs::write(site.path().join("_static/basic.css"), "body {}").unwrap();
    let app = router(Ui {
        deployments: vec![deployment("etl", etl())],
        docs_dir: Some(site.path().to_path_buf()),
        timings: crate::live::UiTimings::default(),
        auth: schedr_api::auth::Auth::default(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    });
    let (status, html) = get(&app, "/docs/", &[]).await;
    assert_eq!(
        (status, html.as_str()),
        (StatusCode::OK, "<h1>schedr docs</h1>")
    );
    let (status, css) = get(&app, "/docs/_static/basic.css", &[]).await;
    assert_eq!((status, css.as_str()), (StatusCode::OK, "body {}"));
    let (status, _) = get(&app, "/docs/../Cargo.toml", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get(&app, "/docs", &[]).await;
    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    let (_, page) = get(&app, "/", &[]).await;
    assert_eq!(
        attrs(&page, "header nav a", "href"),
        [
            "/",
            "/runs",
            "/deployments",
            "/metrics",
            "/extensions",
            "/workers",
            "/connections",
            "/import-errors",
            "/docs/"
        ]
    );
    // Without a directory: no link, no pages.
    let (_, page) = get(&ui(vec![deployment("etl", etl())]), "/", &[]).await;
    assert_eq!(
        attrs(&page, "header nav a", "href"),
        [
            "/",
            "/runs",
            "/deployments",
            "/metrics",
            "/extensions",
            "/workers",
            "/connections",
            "/import-errors"
        ]
    );
}

#[tokio::test]
async fn merged_with_the_api_its_unknown_paths_stay_bare_404s() {
    // As `schedr standalone` serves them: the UI's fallback is the merged router's.
    let api = axum::Router::new().route("/api/v1/dags", axum::routing::get(|| async { "[]" }));
    let app = api.merge(ui(vec![deployment("etl", etl())]));
    for path in ["/api/v1/no-such", "/api"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(
            response.headers().get("content-type").is_none(),
            "{path}: {:?}",
            response.headers()
        );
    }
    let (status, html) = get(&app, "/no/such/page", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(texts(&html, "h1"), ["Not found"]);
}

#[tokio::test]
async fn a_dag_id_with_a_colon_slash_and_hash_links_to_its_page() {
    let id = "etl-repo:team_a/schedr.toml#report_daily";
    let encoded = "etl-repo%3Ateam_a%2Fschedr.toml%23report_daily";
    let mut script = etl();
    script.answers.insert(
        "/api/v1/dags".to_owned(),
        (200, json!({"dags": [dag(id, false)]})),
    );
    script
        .answers
        .insert(format!("/api/v1/dags/{encoded}"), (200, dag(id, false)));
    script.answers.insert(
        format!("/api/v1/dags/{encoded}/runs"),
        (200, json!({"runs": []})),
    );
    let app = ui(vec![deployment("etl", script)]);
    let (_, list) = get(&app, "/", &[]).await;
    let href = format!("/d/etl/dags/{encoded}");
    assert_eq!(
        attrs(&list, "table.dags tbody td:first-child a", "href"),
        std::slice::from_ref(&href)
    );
    let (status, page) = get(&app, &href, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&page, "h1"), [id]);
}

/// A deployment's API that answers after `delay`, counting the calls under way in `busy`
/// (shared by every deployment) and the most at once in `most`.
#[derive(Debug, Clone)]
struct Slow {
    delay: Duration,
    busy: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    most: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Transport for Slow {
    async fn send(&self, _request: ApiRequest) -> Result<ApiResponse, ClientError> {
        use std::sync::atomic::Ordering;
        let now = self.busy.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.busy.fetch_sub(1, Ordering::SeqCst);
        Ok(ApiResponse {
            status: 200,
            body: json!({"dags": []}),
        })
    }
}

#[tokio::test]
async fn every_deployment_is_asked_at_once() {
    let busy = std::sync::Arc::default();
    let most = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let slow = |name: &str| Deployment {
        name: name.to_owned(),
        client: Client::new(
            Slow {
                delay: Duration::from_millis(200),
                busy: std::sync::Arc::clone(&busy),
                most: std::sync::Arc::clone(&most),
            },
            Duration::from_millis(1),
            Duration::from_secs(1),
        ),
        proof: "none",
    };
    let app = router(Ui {
        deployments: vec![slow("a"), slow("b"), slow("c")],
        docs_dir: None,
        timings: crate::live::UiTimings::default(),
        auth: schedr_api::auth::Auth::default(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    });
    let started = std::time::Instant::now();
    let (status, _) = get(&app, "/", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(most.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn every_answer_varies_with_the_prefix_and_docs_redirect_temporarily() {
    let site = tempfile::TempDir::new().unwrap();
    std::fs::write(site.path().join("index.html"), "docs").unwrap();
    let app = router(Ui {
        deployments: vec![deployment("etl", etl())],
        docs_dir: Some(site.path().to_path_buf()),
        timings: crate::live::UiTimings::default(),
        auth: schedr_api::auth::Auth::default(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    });
    for path in ["/", "/assets/style.css", "/docs/", "/docs", "/no/such/page"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.headers().get("vary").and_then(|v| v.to_str().ok()),
            Some("x-forwarded-prefix"),
            "{path}"
        );
        if path == "/docs" {
            assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        }
    }
}

/// A webserver recording who each call was made for (the UI's signed-in user).
#[derive(Debug, Clone, Default)]
struct Recording {
    users: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

impl Transport for Recording {
    async fn send(&self, _request: ApiRequest) -> Result<ApiResponse, ClientError> {
        tokio::task::yield_now().await;
        let user = schedr_client::USER.try_with(Clone::clone).ok();
        self.users.lock().unwrap().push(user);
        Ok(ApiResponse {
            status: 200,
            body: json!({"dags": []}),
        })
    }
}

#[tokio::test]
async fn the_ui_signs_people_in_and_passes_their_name_on() {
    use schedr_config::{AuthMode, AuthSection};
    let dir = tempfile::tempdir().unwrap();
    let users = dir.path().join("users");
    std::fs::write(
        &users,
        format!("anna:{}\n", bcrypt::hash("secret", 4).unwrap()),
    )
    .unwrap();
    let section = AuthSection {
        mode: AuthMode::Basic,
        users_file: Some(users),
        ..AuthSection::default()
    };
    let recording = Recording::default();
    let app = router(Ui {
        deployments: vec![Deployment {
            name: "etl".to_owned(),
            client: Client::new(recording.clone(), Duration::ZERO, Duration::ZERO),
            proof: "none",
        }],
        docs_dir: None,
        timings: crate::live::UiTimings::default(),
        auth: schedr_api::auth::Auth::from_config(
            &section,
            None,
            schedr_api::auth::AuthTimings::default(),
        )
        .unwrap(),
        config: serde_json::json!({}),
        process_id: String::new(),
        host: String::new(),
    });
    // Not signed in: asked to, and nothing is asked of the webserver.
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().contains_key("www-authenticate"));
    assert_eq!(recording.users.lock().unwrap().len(), 0);
    // Asked by htmx: a fragment.
    let (status, html) = get(&app, "/runs", &[("hx-request", "true")]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(!html.contains("<html"), "{html}");
    // The stylesheet is anyone's (the sign-in page needs it).
    let (status, _) = get(&app, "/assets/style.css", &[]).await;
    assert_eq!(status, StatusCode::OK);
    // Signed in: the page, its calls made for anna.
    let basic = "Basic YW5uYTpzZWNyZXQ=";
    let (status, _) = get(&app, "/", &[("authorization", basic)]).await;
    assert_eq!(status, StatusCode::OK);
    // Its DAGs, and its wait steps switched off.
    assert_eq!(
        *recording.users.lock().unwrap(),
        [Some("anna".to_owned()), Some("anna".to_owned())]
    );
}

#[tokio::test]
async fn a_dag_page_shows_its_limits_and_which_are_reached() {
    // M4.4, decision 96.
    let mut webserver = etl();
    let mut sync = dag("sync", false);
    sync["limits"] = json!([
        {"limit": "max_active_tasks", "task_id": null, "value": 10, "running": 3},
        {"limit": "max_active_per_run", "task_id": "fetch", "value": 2, "running": 2}
    ]);
    webserver
        .answers
        .insert("/api/v1/dags/sync".to_owned(), (200, sync));
    let app = ui(vec![deployment("etl", webserver)]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        texts(&html, "ul.limits li"),
        [
            "max_active_tasks 10 on the DAG: 3 running",
            "max_active_per_run 2 on task fetch, in each run: 2 running in its busiest run \
             (reached: the rest wait)"
        ]
    );
    assert_eq!(texts(&html, "ul.limits li.reached").len(), 1);
    // None set: no limits shown.
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(texts(&html, "ul.limits"), Vec::<String>::new());
}

/// `etl()` with DAG `sync` started by a trigger.
fn etl_with_trigger() -> Scripted {
    let mut etl = etl();
    let mut sync = dag("sync", false);
    sync["trigger"] = json!({"kind": "event", "task": {"id": "found"}});
    etl.answers
        .insert("/api/v1/dags/sync".to_owned(), (200, sync));
    etl
}

#[tokio::test]
async fn a_dag_page_shows_its_trigger_and_what_it_found() {
    let mut etl = etl_with_trigger();
    etl.answers.insert(
        "/api/v1/dags/sync/trigger".to_owned(),
        (
            200,
            json!({
                "dag_id": "sync", "trigger": "found", "state": "running", "worker": "w1",
                "bookmark": "inbox/1", "stopped_why": null, "failures": 2,
                "last_error": "no route", "not_before": null,
                "started_at": "2026-10-15T01:00:00Z", "last_event_at": null,
                "last_poked_at": "2026-10-15T01:00:05Z", "last_restart_at": null
            }),
        ),
    );
    etl.answers.insert(
        "/api/v1/dags/sync/trigger/events?limit=20".to_owned(),
        (
            200,
            json!({"events": [{
                "key": "inbox/1", "data": {"n": 1}, "at": null,
                "received_at": "2026-10-15T01:00:06Z"
            }]}),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (status, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("no route (2 in a row)"), "{html}");
    assert!(html.contains("Last poked"), "{html}");
    // Running, nothing holds it back: no Restart.
    assert!(!html.contains("/d/etl/dags/sync/trigger/restart"), "{html}");
    assert!(
        html.contains("/d/etl/dags/sync/trigger/run-event?key=inbox%2F1"),
        "{html}"
    );
    assert!(
        !attrs(&html, ".state.running.waiting", "class").is_empty(),
        "it pulses while it waits: {html}"
    );
}

/// Restart is in the page's one row of actions, and only where it changes something: a
/// trigger stopped, failing and waiting to start again, or stuck; not one running or waiting
/// well, nor a paused one.
#[tokio::test]
async fn restart_is_offered_beside_the_other_actions_only_where_it_changes_something() {
    for (state, failures, stuck, offered) in [
        ("stopped", 0, false, true),
        ("waiting", 3, false, true),
        ("running", 0, true, true),
        ("waiting", 0, false, false),
        ("running", 2, false, false),
        ("paused", 2, false, false),
    ] {
        let mut etl = etl_with_trigger();
        etl.answers.insert(
            "/api/v1/dags/sync/trigger".to_owned(),
            (
                200,
                json!({
                    "dag_id": "sync", "trigger": "found", "state": state, "worker": null,
                    "bookmark": null, "stopped_why": null, "failures": failures, "stuck": stuck,
                    "last_error": null, "not_before": null, "started_at": null,
                    "last_event_at": null, "last_poked_at": null, "last_restart_at": null
                }),
            ),
        );
        etl.answers.insert(
            "/api/v1/dags/sync/trigger/events?limit=20".to_owned(),
            (200, json!({"events": []})),
        );
        let app = ui(vec![deployment("etl", etl)]);
        let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
        let buttons = texts(&html, ".actions > button.action");
        assert_eq!(
            buttons.contains(&"Restart".to_owned()),
            offered,
            "{state} with {failures} failures, stuck {stuck}: {buttons:?}"
        );
        assert_eq!(
            texts(&html, "dd .badge.warning").contains(&"stuck".to_owned()),
            stuck
        );
    }
}

#[tokio::test]
async fn a_trigger_that_cant_be_told_leaves_the_rest_of_the_page() {
    let app = ui(vec![deployment("etl", etl_with_trigger())]);
    let (status, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Its trigger can't be told now"), "{html}");
    assert_eq!(texts(&html, "table.runs tbody td.mono"), ["r2", "r1"]);
}

#[tokio::test]
async fn a_manual_run_of_a_dag_started_by_another_asks_for_its_event() {
    // And of one started by posted events (Grok's review of M4.11e).
    for trigger in [
        json!({"kind": "run_of", "run_of": {"dag": "orders"}}),
        json!({"kind": "event_named", "event": {"name": "paid"}}),
    ] {
        let mut scripted = etl();
        let mut sync = dag("sync", false);
        sync["trigger"] = trigger;
        scripted
            .answers
            .insert("/api/v1/dags/sync".to_owned(), (200, sync));
        let app = ui(vec![deployment("etl", scripted)]);
        let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
        assert!(html.contains(r#"name="event_key""#), "{html}");
    }
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert!(!html.contains(r#"name="event_key""#));
}

#[tokio::test]
async fn a_trigger_waiting_for_runs_shows_the_run_it_is_called_for_and_can_skip_it() {
    let view = |state: &str| {
        json!({
            "dag_id": "sync", "trigger": "found", "state": state, "worker": null,
            "bookmark": null, "stopped_why": null, "failures": 3,
            "last_error": "trigger_failed: no", "not_before": null, "started_at": null,
            "last_event_at": null, "last_poked_at": null, "last_restart_at": null,
            "called_for": "r1"
        })
    };
    for (state, skip) in [("waiting", true), ("running", false)] {
        let mut etl = etl_with_trigger();
        etl.answers
            .insert("/api/v1/dags/sync/trigger".to_owned(), (200, view(state)));
        etl.answers.insert(
            "/api/v1/dags/sync/trigger/events?limit=20".to_owned(),
            (200, json!({"events": []})),
        );
        let app = ui(vec![deployment("etl", etl)]);
        let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
        assert!(html.contains("Called for run"), "{html}");
        assert_eq!(
            html.contains("/d/etl/dags/sync/trigger/skip"),
            skip,
            "only while no call runs: {state}"
        );
    }
}

/// B272: a trigger's log on its DAG's page, read once into an element its live updates keep, a
/// run at a time, followed while it runs.
#[tokio::test]
async fn a_dag_page_reads_its_triggers_log_a_run_at_a_time() {
    let instance = "0190a0a0-0000-7000-8000-0000000000a1";
    let older = "0190a0a0-0000-7000-8000-0000000000a0";
    let mut etl = etl_with_trigger();
    etl.answers.insert(
        "/api/v1/dags/sync/trigger".to_owned(),
        (
            200,
            json!({
                "dag_id": "sync", "trigger": "found", "state": "running", "worker": "w1",
                "bookmark": null, "stopped_why": null, "failures": 0, "last_error": null,
                "not_before": null, "started_at": "2026-10-15T01:00:00Z", "last_event_at": null,
                "last_poked_at": null, "last_restart_at": null, "instance_id": instance
            }),
        ),
    );
    etl.answers.insert(
        "/api/v1/dags/sync/trigger/events?limit=20".to_owned(),
        (200, json!({"events": []})),
    );
    etl.answers.insert(
        "/api/v1/dags/sync/trigger/runs".to_owned(),
        (
            200,
            json!({"runs": [
                {"instance_id": instance, "worker_id": "w1", "started_at": "2026-10-15T01:00:00Z",
                 "ended_at": null, "error": null},
                {"instance_id": older, "worker_id": "w2", "started_at": "2026-10-14T01:00:00Z",
                 "ended_at": "2026-10-14T02:00:00Z", "error": "ValueError: no"}
            ]}),
        ),
    );
    let chunk = |from: u64, content: &str, running: bool| {
        json!({"dag_id": "sync", "instance_id": instance, "worker_id": "w1", "from": from,
            "next": 9, "size": 9, "running": running, "content": content})
    };
    for (query, answer) in [
        ("from=0&limit=4", chunk(0, "", true)),
        ("from=0&limit=1048576", chunk(0, "watching\n", true)),
        ("from=9&limit=1048576", chunk(9, "", false)),
    ] {
        etl.answers.insert(
            format!("/api/v1/dags/sync/trigger/runs/{instance}/log?{query}"),
            (200, answer),
        );
    }
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        attrs(&html, "#trigger-log", "hx-get"),
        ["/d/etl/dags/sync/trigger/log"]
    );
    assert_eq!(attrs(&html, "#trigger-log", "hx-preserve"), ["true"]);
    let (status, fragment) = get(&app, "/d/etl/dags/sync/trigger/log", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(fragment.contains("watching"), "{fragment}");
    assert!(fragment.contains("still running"), "{fragment}");
    assert_eq!(texts(&fragment, "a.try.open").len(), 1, "{fragment}");
    assert_eq!(
        attrs(&fragment, "a.try.failed", "href"),
        [format!("/d/etl/dags/sync/trigger/log?run={older}")]
    );
    let tail = attrs(&fragment, ".log-tail", "hx-get");
    assert_eq!(
        tail,
        [format!(
            "/d/etl/dags/sync/trigger/tail?run={instance}&from=9"
        )]
    );
    let (status, more) = get(&app, &tail[0], &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !more.contains("log-tail"),
        "it ended: no more reads ({more})"
    );
}
