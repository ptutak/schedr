use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use tower::ServiceExt;

use super::*;

async fn get(ops: &Ops, uri: &str) -> (u16, String, String) {
    let response = router(ops.clone())
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        content_type,
        String::from_utf8(body.to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn readiness_waits_for_every_part() {
    let health = Health::new(Duration::from_secs(120));
    let master = health.starting("master");
    let api = health.starting("webserver");
    let ops = Ops::new(Registry::default(), health);
    assert_eq!(
        get(&ops, "/readyz").await.2,
        "starting: master, webserver\n"
    );
    master.ready();
    assert_eq!(get(&ops, "/readyz").await.0, 503);
    api.ready();
    assert_eq!(
        get(&ops, "/readyz").await,
        (
            200,
            "text/plain; charset=utf-8".to_owned(),
            "ok\n".to_owned()
        )
    );
}

#[tokio::test]
async fn liveness_fails_once_a_loop_stalls_and_never_before_it_starts() {
    let health = Health::new(Duration::from_secs(120));
    let beat = Beat::default();
    health.watch_loop("scheduling loop", beat.clone());
    let ops = Ops::new(Registry::default(), health.clone());
    // Not turned yet: starting, alive.
    assert_eq!(get(&ops, "/healthz").await.0, 200);
    let now = Instant::now();
    beat.beat_at(now);
    assert_eq!(health.stalled(now + Duration::from_secs(60)), None);
    assert_eq!(
        health.stalled(now + Duration::from_secs(150)).as_deref(),
        Some("the scheduling loop hasn't turned for 150 s (limit 120 s)")
    );
    // Over HTTP: with no time allowed, a loop that turned a moment ago has stalled.
    let strict = Health::new(Duration::ZERO);
    strict.watch_loop("worker agent", beat.clone());
    beat.beat();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let (status, _, body) = get(&Ops::new(Registry::default(), strict), "/healthz").await;
    assert_eq!(status, 503);
    assert!(
        body.starts_with("the worker agent hasn't turned for 0 s"),
        "{body}"
    );
}

#[tokio::test]
async fn metrics_hold_the_roles_the_process_and_the_build() {
    let mut registry = Registry::default();
    let counter = prometheus_client::metrics::counter::Counter::<u64>::default();
    registry.sub_registry_with_prefix("schedr").register(
        "assignments",
        "Attempts assigned",
        counter.clone(),
    );
    counter.inc();
    let ops = Ops::new(registry, Health::new(Duration::from_secs(1)));
    let (status, content_type, body) = get(&ops, "/metrics").await;
    assert_eq!((status, content_type.as_str()), (200, METRICS_CONTENT_TYPE));
    assert!(body.contains("schedr_assignments_total 1"), "{body}");
    assert!(
        body.contains(&format!("schedr_build_info{{version=\"{VERSION}\"}} 1")),
        "{body}"
    );
    assert!(body.contains("process_resident_memory_bytes "), "{body}");
    assert!(body.contains("process_cpu_seconds_total "), "{body}");
    assert!(body.contains("process_open_fds "), "{body}");
    assert!(body.contains("process_start_time_seconds "), "{body}");
    assert!(body.ends_with("# EOF\n"), "{body}");
}

#[test]
fn process_stats_are_read_from_proc() {
    let stat = "42 (schedr (worker)) S 1 42 42 0 -1 4194304 1 0 0 0 250 50 0 0 20 0 9 0 1000 \
                123 456";
    let stats = parse_stats(
        stat,
        "1000 300 50 1 0 200 0",
        "cpu 1 2\nbtime 1700000000\n",
        7,
        4096,
        100,
    )
    .unwrap();
    assert_eq!(
        stats,
        ProcessStats {
            resident_bytes: 300 * 4096,
            cpu_seconds: 3.0,
            open_fds: 7,
            start_time_seconds: 1_700_000_010.0,
        }
    );
    assert_eq!(parse_stats("42 (x", "", "", 0, 4096, 100), None);
    assert_eq!(parse_stats(stat, "1 2", "no boot time", 0, 4096, 100), None);
    // This process has a /proc on Linux.
    assert!(process_stats().unwrap().resident_bytes > 0);
}

#[test]
fn the_ops_line_names_the_address() {
    assert_eq!(
        ops_line(std::net::SocketAddr::from(([127, 0, 0, 1], 8090))),
        "ops on http://127.0.0.1:8090"
    );
}
