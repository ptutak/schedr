use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn a_quantile_is_read_within_its_bucket() {
    let bounds = [1.0, 5.0];
    assert_eq!(quantile(&[0, 0, 0], &bounds, 0.5), None);
    // 4 at most 1, 4 between 1 and 5: the median at the first bucket's top.
    assert_eq!(quantile(&[4, 4, 0], &bounds, 0.5), Some(1.0));
    assert_eq!(quantile(&[4, 4, 0], &bounds, 0.75), Some(3.0));
    // Past the last bound: its bound.
    assert_eq!(quantile(&[0, 0, 5], &bounds, 0.99), Some(5.0));
}

fn extension(panels: &serde_json::Value) -> ExtensionView {
    serde_json::from_value(json!({
        "name": "k", "workers": [], "version": "1", "max_series": 100,
        "events": [{"at": "2026-11-21T10:05:00Z", "worker_id": "w-1", "version": "",
                    "kind": "discovered", "message": "/opt/schedr-ext-k, versions 1"}],
        "attempts": [{"at": "2026-11-21T10:06:00Z", "dag_id": "pods", "run_id": "r-1",
                      "task_id": "count", "map_index": null, "try_number": 2, "ended": null,
                      "phase": "pulling <image>", "message": "busybox", "progress": 0.25}],
        "metrics": [{"name": "running", "kind": "gauge", "help": "R"}],
        "panels": panels.clone(),
    }))
    .unwrap()
}

fn metrics() -> ExtensionMetrics {
    serde_json::from_value(json!({
        "range": "1h", "bucket_s": 60, "from": "2026-11-21T10:00:00Z", "to": "2026-11-21T11:00:00Z",
        "metrics": [{"name": "running", "kind": "gauge", "folded": false, "series": [
            {"labels": {"phase": "Running"}, "points": [
                {"at": "2026-11-21T10:01:00Z", "value": 2.0, "peak": 2.0, "sum": 0.0}]},
            {"labels": {"phase": "Pending"}, "points": [
                {"at": "2026-11-21T10:01:00Z", "value": 1.0, "peak": 1.0, "sum": 0.0}]}
        ]}]
    }))
    .unwrap()
}

#[test]
fn panels_go_in_their_rows_and_cells_in_the_colours_they_name() {
    let panels = json!([
        {"title": "Now", "metric": "running", "chart": "number", "row": 1, "cell": "right"},
        {"title": "Pods", "metric": "running", "chart": "line", "row": 1, "cell": "left",
         "colors": {"Running": "green", "Pending": "amber"}},
        {"title": "Where", "chart": "events", "row": 2, "cell": "full"}
    ]);
    let rows = rows(
        &extension(&panels),
        &metrics(),
        &TimeZone::UTC,
        "/d/default/runs",
    );
    let places: Vec<(u8, Vec<&str>)> = rows
        .iter()
        .map(|r| (r.cells_in_row, r.cells.iter().map(|c| c.place).collect()))
        .collect();
    assert_eq!(places, [(2, vec!["left", "right"]), (1, vec!["full"])]);
    let pods = &rows[0].cells[0];
    assert_eq!(
        pods.legend,
        [(3, "Running".to_owned()), (4, "Pending".to_owned())],
        "green and amber"
    );
    assert!(
        rows[0].cells[1].html.contains("big-number\">3<"),
        "{}",
        rows[0].cells[1].html
    );
    // DN19: the worker's events and the attempts' latest phases, newest first, the attempt
    // linking to its task's panel, its text escaped.
    let events = &rows[1].cells[0].html;
    let attempt = events.find("pulling &lt;image&gt;").expect(events);
    let discovered = events.find(">discovered<").expect(events);
    assert!(attempt < discovered, "newest first: {events}");
    assert!(
        events.contains("href=\"/d/default/runs/r-1?task=count&amp;tab=attempts&amp;try=2\""),
        "{events}"
    );
    assert!(events.contains("busybox (25 %)"), "{events}");
}

#[test]
fn counts_past_the_bounds_read_nothing_beyond_them() {
    // Codex's review: [0, 0, 1] over one bound indexed past it.
    assert_eq!(quantile(&[0, 0, 1], &[1.0], 0.5), None);
    assert_eq!(quantile(&[0, 1, 5], &[1.0], 0.1), Some(1.0));
}

#[test]
fn the_events_list_shows_the_newest_fifty_and_says_older_are_left_out() {
    // The user, 2026-11-21: "events, even with a scroll have to have limit".
    let mut extension = extension(&json!([]));
    let event = extension.events[0].clone();
    extension.events = (0..60)
        .map(|n| {
            let mut e = event.clone();
            e.at += jiff::SignedDuration::from_secs(n);
            e.message = format!("event {n}");
            e
        })
        .collect();
    let html = events(&extension, "/d/default/runs", &jiff::tz::TimeZone::UTC);
    assert_eq!(html.matches("<tr><td class=\"time\">").count(), 50);
    assert!(
        html.contains("event 59") && !html.contains("event 9<"),
        "the newest"
    );
    assert!(
        html.contains("The newest 50 shown; older ones are left out."),
        "{html}"
    );
    let few = events(
        &self::extension(&json!([])),
        "/d/default/runs",
        &jiff::tz::TimeZone::UTC,
    );
    assert!(!few.contains("left out"), "{few}");
}

#[test]
fn a_bars_panel_draws_a_bar_per_bucket_and_leaves_an_empty_one_empty() {
    // The user, 2026-11-21: "you can add \"bars\" chart as another chart type now".
    let mut extension = extension(&json!([
        {"title": "Steps", "metric": "running", "chart": "bars", "row": 1, "cell": "full"}
    ]));
    extension.metrics[0]["kind"] = json!("counter");
    let mut metrics = metrics();
    metrics.metrics[0].kind = "counter".to_owned();
    metrics.metrics[0].series[0].points = [0_i64, 2, 4]
        .iter()
        .map(|m| {
            serde_json::from_value(
                json!({"at": format!("2026-11-21T10:0{m}:00Z"), "value": 1.0, "sum": 0.0}),
            )
            .unwrap()
        })
        .collect();
    let first = rows(
        &extension,
        &metrics,
        &jiff::tz::TimeZone::UTC,
        "/d/default/runs",
    );
    let svg = &first[0].cells[0].html;
    assert_eq!(
        svg.matches("<rect class=\"area s1\"").count(),
        3,
        "a bar each: {svg}"
    );
    assert!(
        !svg.contains("<path class=\"area"),
        "no area across the gaps: {svg}"
    );
    // A counter in bytes draws in bytes (agy's review of M5.2i5: raw numbers).
    extension.metrics[0]["unit"] = json!("bytes");
    for point in &mut metrics.metrics[0].series[0].points {
        point.value = 10_485_760.0;
    }
    let drawn = rows(
        &extension,
        &metrics,
        &jiff::tz::TimeZone::UTC,
        "/d/default/runs",
    );
    let svg = &drawn[0].cells[0].html;
    assert!(svg.contains("10 MiB") || svg.contains("10.0 MiB"), "{svg}");
}
