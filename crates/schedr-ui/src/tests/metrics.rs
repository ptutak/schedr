//! The Metrics page (M5.1j; decisions 465, 466) against scripted webservers.

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

use super::*;

const GIB: u64 = 1 << 30;

/// A day of samples: a worker with its tasks (two points) and a master (one).
fn samples(range: &str) -> Value {
    let point = |at: &str, resident: u64, memory: u64, parts: Option<Value>| {
        let mut point = json!({
            "at": at, "resident_avg": resident, "resident_peak": resident,
            "memory_avg": memory, "memory_peak": memory, "minutes": 6, "cpu_avg": 0.5, "cpu_peak": 0.7,
            "container_avg": memory, "container_peak": memory, "container_limit": 8 * GIB
        });
        if let Some(parts) = parts {
            point["parts"] = parts;
        }
        point
    };
    let parts = json!({"fork_servers_bytes": 0, "watcher_hosts_bytes": 0, "tasks_bytes": 3 * GIB});
    json!({
        "range": range, "step_s": 360,
        "from": "2026-11-13T10:00:00Z", "to": "2026-11-14T10:00:00Z",
        "processes": [
            {
                "process_id": "p-w1", "roles": {"worker": "w1"}, "container_limit": 8 * GIB,
                "points": [
                    point("2026-11-14T09:48:00Z", GIB / 4, GIB / 4 + GIB, Some(json!({"fork_servers_bytes": 0, "watcher_hosts_bytes": 0, "tasks_bytes": GIB}))),
                    point("2026-11-14T09:54:00Z", GIB / 4, GIB / 4 + 3 * GIB, Some(parts.clone())),
                ],
                "parts": {
                    "at": "2026-11-14T09:59:00Z", "fork_servers_bytes": 0,
                    "watcher_hosts_bytes": 0, "tasks_bytes": 3 * GIB,
                    "largest_tasks": [{"attempt_id": "a1", "dag_id": "etl", "run_id": "r/1",
                                       "task_id": "load", "bytes": 2 * GIB}]
                }
            },
            {
                "process_id": "p-m1", "roles": {"master": "0190a0a0-0000-7000-8000-00000000m001"},
                "points": [point("2026-11-14T09:54:00Z", GIB / 8, GIB / 8, None)]
            }
        ]
    })
}

fn with_samples() -> Scripted {
    let mut etl = etl();
    for range in ["1h", "24h", "7d", "30d"] {
        etl.answers.insert(
            format!("/api/v1/metrics/samples?range={range}"),
            (200, samples(range)),
        );
    }
    etl.answers.insert(
        "/api/v1/metrics/samples?range=24h&process=p-w1".to_owned(),
        (200, samples("24h")),
    );
    etl
}

#[tokio::test]
async fn the_page_charts_each_deployments_processes_and_lists_them_by_peak() {
    let app = ui(vec![deployment("etl", with_samples())]);
    let (status, html) = get(&app, "/metrics", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        texts(&html, "nav.choices a.current"),
        ["24 h"],
        "24 h at first; one deployment: no deployment choice"
    );
    // Memory (whole), memory by kind, CPU.
    assert_eq!(html.matches(r#"<svg class="chart""#).count(), 3);
    assert!(html.contains("tasks 3.0 GiB"), "the kinds' legend");
    // By peak: the worker (its tasks with it) first.
    let first = texts(&html, "table.metrics-table tbody tr:first-child td");
    assert_eq!(
        first[..5],
        ["worker w1", "worker", "3.2 GiB", "256.0 MiB", "3.0 GiB"]
    );
    assert_eq!(first[8], "40 %", "3.25 GiB of 8 GiB");
    assert_eq!(
        attrs(&html, "#live", "hx-trigger"),
        ["every 60s"],
        "read again once a minute"
    );
    // Sorted by name, the master first.
    let (_, html) = get(&app, "/metrics?sort=name", &[]).await;
    assert!(texts(&html, "table.metrics-table tbody tr:first-child td")[0].starts_with("master"));
    for range in ["1h", "7d", "30d"] {
        let (status, html) = get(&app, &format!("/metrics?range={range}"), &[]).await;
        assert_eq!(status, StatusCode::OK, "{range}");
        assert_eq!(html.matches(r#"<svg class="chart""#).count(), 3, "{range}");
    }
    let (status, _) = get(&app, "/metrics?range=2h", &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_rows_panel_shows_its_parts_and_largest_tasks() {
    let app = ui(vec![deployment("etl", with_samples())]);
    let (_, html) = get(&app, "/metrics?deployment=etl&process=p-w1", &[]).await;
    assert_eq!(texts(&html, "#process-panel h2"), ["worker w1 · 3.2 GiB"]);
    assert_eq!(
        attrs(&html, "#process-panel .largest-tasks a", "href"),
        ["/d/etl/runs/r%2F1"]
    );
    assert!(
        texts(&html, "#process-panel dl")
            .join(" ")
            .contains("Running tasks")
    );
    assert_eq!(
        attrs(&html, "#process-panel a.close", "href"),
        ["/metrics?range=24h&deployment=etl"]
    );
    // A node's panel on Deployments loads the same chart over 24 h.
    let (status, chart) = get(&app, "/metrics/chart?deployment=etl&process=p-w1", &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(chart.contains(r#"<svg class="chart""#) && chart.contains("tasks"));
    let (status, _) = get(&app, "/metrics/chart?deployment=nope&process=p-w1", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_unreachable_deployment_says_so_and_the_others_are_charted() {
    let mut down = etl();
    down.down = true;
    let app = ui(vec![
        deployment("etl", with_samples()),
        deployment("ml", down),
    ]);
    let (_, html) = get(&app, "/metrics", &[]).await;
    assert_eq!(html.matches(r#"<svg class="chart""#).count(), 3);
    assert!(texts(&html, ".banner.warning")[0].starts_with("Unavailable: "));
    // Two deployments: a choice of each.
    assert_eq!(
        texts(&html, "nav[aria-label=Deployment] a"),
        ["All", "etl", "ml"]
    );
}

#[tokio::test]
async fn a_worker_replaced_within_a_bucket_counts_once_in_memory_by_kind() {
    // Codex's review of M5.1j: w1 for 3 minutes, then w2 for 3, 1 GiB each, beside a master
    // there all 6: the workers held 1 GiB, not 2.
    let point = |minutes: u32, resident: u64| {
        json!({"at": "2026-11-14T09:54:00Z", "resident_avg": resident, "resident_peak": resident,
               "memory_avg": resident, "memory_peak": resident, "minutes": minutes})
    };
    let mut etl = etl();
    etl.answers.insert(
        "/api/v1/metrics/samples?range=24h".to_owned(),
        (
            200,
            json!({
                "range": "24h", "step_s": 360,
                "from": "2026-11-13T10:00:00Z", "to": "2026-11-14T10:00:00Z",
                "processes": [
                    {"process_id": "p1", "roles": {"worker": "w1"}, "points": [point(3, GIB)]},
                    {"process_id": "p2", "roles": {"worker": "w2"}, "points": [point(3, GIB)]},
                    {"process_id": "pm", "roles": {"master": "m-a"}, "points": [point(6, GIB / 8)]}
                ]
            }),
        ),
    );
    let app = ui(vec![deployment("etl", etl)]);
    let (_, html) = get(&app, "/metrics", &[]).await;
    let legend = texts(&html, ".chart-legend .key");
    assert!(legend.contains(&"workers 1.0 GiB".to_owned()), "{legend:?}");
    assert!(
        legend.contains(&"masters and webservers 128.0 MiB".to_owned()),
        "{legend:?}"
    );
}
