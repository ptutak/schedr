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

#[test]
fn a_panel_draws_its_own_lines_and_a_number_combines_them_as_it_says() {
    // Decision 516: the API worked panel 0's lines out (one, `max` over workers); panel 1 has
    // none of its own and draws the metric's summed series.
    let panels = json!([
        {"title": "Pending", "metric": "running", "chart": "number", "row": 1, "cell": "left",
         "combine": "max"},
        {"title": "Summed", "metric": "running", "chart": "number", "row": 1, "cell": "right",
         "combine": "max"}
    ]);
    let mut metrics = metrics();
    metrics.panels = serde_json::from_value(json!([{
        "index": 0, "metric": "running", "folded": false, "series": [
            {"labels": {}, "points": [
                {"at": "2026-11-21T10:01:00Z", "value": 10.0, "peak": 10.0, "sum": 0.0}]}
        ]
    }]))
    .unwrap();
    let rows = rows(
        &extension(&panels),
        &metrics,
        &TimeZone::UTC,
        "/d/default/runs",
    );
    let html: Vec<&str> = rows[0].cells.iter().map(|c| c.html.as_str()).collect();
    assert!(html[0].contains("big-number\">10<"), "{}", html[0]);
    // The summed series 2 and 1, their highest.
    assert!(html[1].contains("big-number\">2<"), "{}", html[1]);
}

/// A page of `panels` over metric `m` (kind `kind`, unit seconds) whose panel lines are `lines`.
fn page(
    kind: &str,
    panels: &serde_json::Value,
    lines: &serde_json::Value,
) -> (ExtensionView, ExtensionMetrics) {
    let mut extension = extension(panels);
    extension.metrics = vec![
        json!({"name": "m", "kind": kind, "help": "M", "unit": "seconds",
                                    "buckets": [1.0]}),
    ];
    let metrics = serde_json::from_value(json!({
        "range": "1h", "bucket_s": 600, "from": "2026-11-21T10:00:00Z", "to": "2026-11-21T10:30:00Z",
        "metrics": [{"name": "m", "kind": kind, "folded": false, "buckets": [1.0], "series": []}],
        "panels": lines,
    }))
    .unwrap();
    (extension, metrics)
}

fn point(at: &str, value: f64, counts: &[u64], sum: f64) -> serde_json::Value {
    json!({"at": at, "value": value, "counts": counts, "sum": sum})
}

#[test]
fn a_number_over_the_range_is_a_rate_or_mean_not_their_sum() {
    // agy's review of M5.2i10: three buckets at 0.5/s showed 1.5; a histogram's mean of 2 s, 6 s.
    // (The metric is in seconds: 0.5 a second reads 500 ms.)
    let panels = json!([
        {"title": "Rate", "metric": "m", "chart": "number", "row": 1, "cell": "left", "point": "rate"},
        {"title": "Mean", "metric": "m", "chart": "number", "row": 1, "cell": "right", "point": "mean"}
    ]);
    let buckets = [
        "2026-11-21T10:00:00Z",
        "2026-11-21T10:10:00Z",
        "2026-11-21T10:20:00Z",
    ];
    let rate: Vec<_> = buckets.iter().map(|at| point(at, 0.5, &[], 0.0)).collect();
    let (extension, metrics) = page(
        "counter",
        &panels,
        &json!([{"index": 0, "metric": "m", "folded": false, "series": [{"labels": {}, "points": rate}]}]),
    );
    let drawn = rows(&extension, &metrics, &TimeZone::UTC, "/d/default/runs");
    assert!(
        drawn[0].cells[0].html.contains("big-number\">500 ms<"),
        "{}",
        drawn[0].cells[0].html
    );
    let mean: Vec<_> = buckets
        .iter()
        .map(|at| point(at, 2.0, &[1, 1], 4.0))
        .collect();
    let (extension, metrics) = page(
        "histogram",
        &panels,
        &json!([{"index": 1, "metric": "m", "folded": false, "series": [{"labels": {}, "points": mean}]}]),
    );
    let drawn = rows(&extension, &metrics, &TimeZone::UTC, "/d/default/runs");
    assert!(
        drawn[0].cells[1].html.contains("big-number\">2.0 s<"),
        "{}",
        drawn[0].cells[1].html
    );
}

#[test]
fn a_histograms_count_is_a_number_of_observations_not_its_unit() {
    // agy's review of M5.2i10: 4 observations showed as 4 s.
    let panels =
        json!([{"title": "Seen", "metric": "m", "chart": "number", "row": 1, "cell": "full"}]);
    let (extension, metrics) = page(
        "histogram",
        &panels,
        &json!([{"index": 0, "metric": "m", "folded": false, "series": [
            {"labels": {}, "points": [point("2026-11-21T10:00:00Z", 4.0, &[3, 1], 6.0)]}]}]),
    );
    let drawn = rows(&extension, &metrics, &TimeZone::UTC, "/d/default/runs");
    assert!(
        drawn[0].cells[0].html.contains("big-number\">4<"),
        "{}",
        drawn[0].cells[0].html
    );
}

#[test]
fn a_panel_left_out_keeps_the_next_ones_number_and_other_is_drawn_past_the_charts_lines() {
    // agy's review of M5.2i10: the next panel took a left-out one's lines; a `limit` of 8 hid
    // `other`.
    let panels = json!([
        {"title": "Unreadable", "metric": "m", "chart": "number", "row": 1, "cell": "left",
         "future_option": true},
        {"title": "Lines", "metric": "m", "chart": "line", "row": 1, "cell": "right"}
    ]);
    let mut series: Vec<serde_json::Value> = (0..8)
        .map(|n| json!({"labels": {"pod": format!("p{n}")}, "points": [point("2026-11-21T10:00:00Z", 1.0, &[], 0.0)]}))
        .collect();
    series.push(json!({"labels": {"pod": "other"}, "points": [point("2026-11-21T10:00:00Z", 9.0, &[], 0.0)]}));
    let (extension, metrics) = page(
        "gauge",
        &panels,
        &json!([
            {"index": 0, "metric": "m", "folded": false, "series": [
                {"labels": {}, "points": [point("2026-11-21T10:00:00Z", 111.0, &[], 0.0)]}]},
            {"index": 1, "metric": "m", "folded": true, "series": series}
        ]),
    );
    let drawn = rows(&extension, &metrics, &TimeZone::UTC, "/d/default/runs");
    let legend: Vec<&str> = drawn[0].cells[0]
        .legend
        .iter()
        .map(|(_, l)| l.as_str())
        .collect();
    assert_eq!(legend.len(), 8);
    assert_eq!(legend.last(), Some(&"other"), "{legend:?}");
}
