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
mod runs;

/// A webserver's API answering from a script: path -> (status, body); anything else 404.
#[derive(Debug, Clone, Default)]
struct Scripted {
    answers: BTreeMap<String, (u16, Value)>,
    down: bool,
}

impl Transport for Scripted {
    fn send(
        &self,
        request: ApiRequest,
    ) -> impl Future<Output = Result<ApiResponse, ClientError>> + Send {
        let answer = if self.down {
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
        "dag_id": id, "paused": paused,
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
        down: false,
    }
}

fn deployment(name: &str, transport: Scripted) -> Deployment<Scripted> {
    Deployment {
        name: name.to_owned(),
        client: Client::new(transport, Duration::from_millis(1), Duration::from_secs(1)),
    }
}

fn ui(deployments: Vec<Deployment<Scripted>>) -> axum::Router {
    router(Ui {
        deployments,
        docs_dir: None,
        auth: schedr_api::auth::Auth::default(),
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

fn attrs(html: &str, selector: &str, attr: &str) -> Vec<String> {
    let page = Html::parse_document(html);
    page.select(&Selector::parse(selector).unwrap())
        .filter_map(|e| e.value().attr(attr).map(str::to_owned))
        .collect()
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
    assert_eq!(texts(&html, "table.dags .badge"), ["paused", "active"]);
    assert_eq!(
        texts(&html, "table.dags td.mono:not(.dim)"),
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

#[tokio::test]
async fn a_dag_page_shows_its_facts_and_runs_in_utc_and_local_time() {
    let app = ui(vec![deployment("etl", etl())]);
    let cookie = ("cookie", "schedr_tz=Europe%2FWarsaw");
    let (status, html) = get(&app, "/d/etl/dags/sync", &[cookie]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(texts(&html, "h1"), ["sync active"]);
    assert_eq!(texts(&html, ".description"), ["Sync the orders."]);
    assert_eq!(texts(&html, "table.runs tbody td.mono"), ["r2", "r1"]);
    assert_eq!(
        attrs(&html, "table.runs .state", "class"),
        ["state running", "state success"]
    );
    assert_eq!(
        texts(&html, "table.runs tbody td:nth-child(3)"),
        [
            "2026-10-15 01:00 UTC · 03:00 CEST",
            "2026-10-15 01:00 UTC · 03:00 CEST"
        ]
    );
    // The zone is known: no reload, no "UTC only" note.
    assert!(!html.contains("location.reload()"));
    assert_eq!(texts(&html, "footer.note"), Vec::<String>::new());
}

#[tokio::test]
async fn without_the_zone_times_are_utc_and_the_page_says_so() {
    let app = ui(vec![deployment("etl", etl())]);
    let (_, html) = get(&app, "/d/etl/dags/sync", &[]).await;
    assert_eq!(
        texts(&html, "table.runs tbody td:nth-child(3)")[0],
        "2026-10-15 01:00 UTC"
    );
    assert!(html.contains("location.reload()"));
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
        auth: schedr_api::auth::Auth::default(),
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
        ["/", "/runs", "/workers", "/docs/"]
    );
    // Without a directory: no link, no pages.
    let (_, page) = get(&ui(vec![deployment("etl", etl())]), "/", &[]).await;
    assert_eq!(
        attrs(&page, "header nav a", "href"),
        ["/", "/runs", "/workers"]
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
    assert_eq!(texts(&page, "h1"), [format!("{id} active")]);
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
    };
    let app = router(Ui {
        deployments: vec![slow("a"), slow("b"), slow("c")],
        docs_dir: None,
        auth: schedr_api::auth::Auth::default(),
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
        auth: schedr_api::auth::Auth::default(),
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
        }],
        docs_dir: None,
        auth: schedr_api::auth::Auth::from_config(&section, None).unwrap(),
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
    assert_eq!(*recording.users.lock().unwrap(), [Some("anna".to_owned())]);
}
