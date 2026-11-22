#![expect(clippy::float_cmp, reason = "sums of small whole numbers: exact")]

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn declarations() -> Declarations {
    serde_json::from_value(json!({"metrics": [
        {"name": "started", "kind": "counter", "help": "S", "labels": [{"name": "ns"}]},
        {"name": "running", "kind": "gauge", "help": "R"},
        {"name": "queue", "kind": "histogram", "help": "Q", "buckets": [1.0]}
    ]}))
    .unwrap()
}

fn sample(
    worker: &str,
    at: &str,
    metric: &str,
    labels: &[(&str, &str)],
    data: SampleData,
) -> ExtensionSample {
    ExtensionSample {
        name: "k".to_owned(),
        version: "1".to_owned(),
        worker_id: worker.to_owned(),
        metric: metric.to_owned(),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        step_s: 60,
        at: ts(at),
        data,
    }
}

#[test]
fn metrics_are_summed_over_workers_in_buckets() {
    let gauge = |v: f64| SampleData::Gauge {
        avg: v,
        peak: v,
        minutes: 1,
        across: None,
    };
    let samples = [
        sample("w1", "2026-11-21T10:00:00Z", "running", &[], gauge(2.0)),
        sample("w2", "2026-11-21T10:00:00Z", "running", &[], gauge(4.0)),
        sample("w1", "2026-11-21T10:01:00Z", "running", &[], gauge(1.0)),
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "a")],
            SampleData::Counter {
                growth: 2.0,
                across: None,
            },
        ),
        sample(
            "w2",
            "2026-11-21T10:01:00Z",
            "started",
            &[("ns", "a")],
            SampleData::Counter {
                growth: 3.0,
                across: None,
            },
        ),
        sample(
            "w1",
            "2026-11-21T10:01:00Z",
            "queue",
            &[],
            SampleData::Histogram {
                counts: vec![1, 2],
                sum: 9.0,
            },
        ),
    ];
    let metrics = metrics_of(&declarations(), 100, &samples, 360);
    let running = &metrics[1].series[0].points[0];
    assert_eq!(
        (running.value, running.peak),
        (3.5, Some(6.0)),
        "(6 + 1) / 2; 2 + 4"
    );
    assert_eq!(metrics[0].series[0].points[0].value, 5.0);
    assert_eq!(metrics[2].series[0].points[0].counts, [1, 2]);
    assert!(!metrics[0].folded);
}

#[test]
fn series_past_the_cap_are_folded_into_other_smallest_first() {
    let grew = |v: f64| SampleData::Counter {
        growth: v,
        across: None,
    };
    let samples = [
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "big")],
            grew(10.0),
        ),
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "mid")],
            grew(5.0),
        ),
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "small")],
            grew(1.0),
        ),
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "other")],
            grew(2.0),
        ),
    ];
    let metrics = metrics_of(&declarations(), 2, &samples, 60);
    let shown: Vec<(&str, f64)> = metrics[0]
        .series
        .iter()
        .map(|s| (s.labels["ns"].as_str(), s.points[0].value))
        .collect();
    assert_eq!(shown, [("big", 10.0), ("mid", 5.0), ("other", 3.0)]);
    assert!(metrics[0].folded);
}

#[test]
fn a_metric_is_read_only_from_versions_declaring_it_as_the_newest_does() {
    let version = |version: &str, buckets: f64| ExtensionVersion {
        name: "k".to_owned(),
        version: version.to_owned(),
        worker_id: "w1".to_owned(),
        declarations: serde_json::from_value(json!({"metrics": [
            {"name": "started", "kind": "counter", "help": "S", "labels": [{"name": "ns"}]},
            {"name": "queue", "kind": "histogram", "help": "Q", "buckets": [buckets]}
        ]}))
        .unwrap(),
        max_series: 100,
        seen_at: ts("2026-11-21T10:00:00Z"),
    };
    let of = |version: &str, metric: &str| {
        let mut s = sample(
            "w1",
            "2026-11-21T10:00:00Z",
            metric,
            &[],
            SampleData::Counter {
                growth: 1.0,
                across: None,
            },
        );
        version.clone_into(&mut s.version);
        s
    };
    // 2 changed the queue's bounds (Codex's review: 1's ≤100 read as 2's ≤1); the counter
    // is the same in both.
    let versions = [version("2", 1.0), version("1", 100.0)];
    let kept = of_compatible_versions(
        vec![
            of("1", "queue"),
            of("2", "queue"),
            of("1", "started"),
            of("9", "started"),
        ],
        &versions,
    );
    let kept: Vec<(&str, &str)> = kept
        .iter()
        .map(|s| (s.version.as_str(), s.metric.as_str()))
        .collect();
    assert_eq!(kept, [("2", "queue"), ("1", "started")]);
}

#[test]
fn a_series_a_worker_folded_is_said_to_be_folded() {
    // Within the cap, but a worker folded some into `other` (Codex's review: not said).
    let samples = [
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "a")],
            SampleData::Counter {
                growth: 1.0,
                across: None,
            },
        ),
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "other")],
            SampleData::Counter {
                growth: 2.0,
                across: None,
            },
        ),
    ];
    let metrics = metrics_of(&declarations(), 1, &samples, 60);
    assert!(metrics[0].folded);
    let plain = metrics_of(&declarations(), 1, &samples[..1], 60);
    assert!(!plain[0].folded);
}

#[test]
fn two_workers_declaring_one_version_differently_are_kept_apart() {
    // Codex's second review of M5.2i5: matched by version only, w2's ≤100 read as ≤1.
    let version = |worker: &str, buckets: f64, seen: &str| ExtensionVersion {
        name: "k".to_owned(),
        version: "1".to_owned(),
        worker_id: worker.to_owned(),
        declarations: serde_json::from_value(json!({"metrics": [
            {"name": "queue", "kind": "histogram", "help": "Q", "buckets": [buckets]}
        ]}))
        .unwrap(),
        max_series: 100,
        seen_at: ts(seen),
    };
    let versions = [
        version("w1", 1.0, "2026-11-21T10:05:00Z"),
        version("w2", 100.0, "2026-11-21T10:00:00Z"),
    ];
    let of = |worker: &str| {
        sample(
            worker,
            "2026-11-21T10:00:00Z",
            "queue",
            &[],
            SampleData::Histogram {
                counts: vec![1, 0],
                sum: 0.5,
            },
        )
    };
    let kept = of_compatible_versions(vec![of("w1"), of("w2"), of("")], &versions);
    let workers: Vec<&str> = kept.iter().map(|s| s.worker_id.as_str()).collect();
    assert_eq!(
        workers,
        ["w1"],
        "w2's own, and the rollup mixing both, left out"
    );
}

/// Decision 516's panels: `pods` read by two workers per namespace, `started` grown per
/// namespace, `queue` observed.
fn panel_declarations(panels: serde_json::Value) -> Declarations {
    let mut declarations = json!({
        "metrics": [
            {"name": "pods", "kind": "gauge", "help": "P", "labels": [{"name": "ns"}, {"name": "pod"}]},
            {"name": "started", "kind": "counter", "help": "S", "labels": [{"name": "ns"}]},
            {"name": "queue", "kind": "histogram", "help": "Q", "buckets": [1.0]}
        ],
    });
    declarations["panels"] = panels;
    serde_json::from_value(declarations).unwrap()
}

/// Each line's labels and its points' values.
fn lines(view: &ExtensionPanelView) -> Vec<(Vec<String>, Vec<f64>)> {
    view.series
        .iter()
        .map(|s| {
            (
                s.labels.values().cloned().collect(),
                s.points.iter().map(|p| p.value).collect(),
            )
        })
        .collect()
}

/// The panels' lines over `samples` in 5-minute buckets.
fn drawn(panels: serde_json::Value, samples: &[ExtensionSample]) -> Vec<ExtensionPanelView> {
    panels_of(&panel_declarations(panels), 10, samples, 300)
}

fn pods(worker: &str, at: &str, ns: &str, pod: &str, v: f64) -> ExtensionSample {
    sample(
        worker,
        at,
        "pods",
        &[("ns", ns), ("pod", pod)],
        SampleData::gauge(v),
    )
}

#[test]
fn a_gauge_read_by_two_workers_from_one_place_shows_once_with_combine_max() {
    // Both workers read the cluster's 10 pending pods; then 6, then 8.
    let samples: Vec<ExtensionSample> = ["w1", "w2"]
        .iter()
        .flat_map(|w| {
            [
                pods(w, "2026-11-21T10:00:00Z", "a", "p", 10.0),
                pods(w, "2026-11-21T10:01:00Z", "a", "p", 6.0),
                pods(w, "2026-11-21T10:02:00Z", "a", "p", 8.0),
            ]
        })
        .collect();
    let panel = |extra: serde_json::Value| {
        let mut p =
            json!({"title": "Pods", "metric": "pods", "chart": "line", "row": 1, "cell": "full"});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    let views = drawn(
        json!([
            panel(json!({})),
            panel(json!({"combine": "max"})),
            panel(json!({"combine": "max", "point": "last"})),
            panel(json!({"combine": "max", "point": "max"})),
            panel(json!({"combine": "max", "point": "min"})),
        ]),
        &samples,
    );
    // The declared panels only: the page adds one for each metric none shows.
    let values: Vec<f64> = views
        .iter()
        .filter(|v| !v.series.is_empty())
        .map(|v| lines(v)[0].1[0])
        .collect();
    // The default: summed over workers, the minutes' mean (DN26): (20 + 12 + 16) / 3.
    assert_eq!(values, [16.0, 8.0, 8.0, 10.0, 6.0]);
}

#[test]
fn group_by_joins_the_labels_left_out_and_limit_folds_the_rest_into_other() {
    let at = "2026-11-21T10:00:00Z";
    let samples = [
        pods("w1", at, "a", "p1", 1.0),
        pods("w1", at, "a", "p2", 2.0),
        pods("w1", at, "b", "p3", 5.0),
        pods("w1", at, "c", "p4", 3.0),
        pods("w1", at, "d", "p5", 0.5),
    ];
    let panel = |extra: serde_json::Value| {
        let mut p =
            json!({"title": "Pods", "metric": "pods", "chart": "line", "row": 1, "cell": "full"});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    let views = drawn(
        json!([
            panel(json!({})),
            panel(json!({"group_by": ["ns"]})),
            panel(json!({"group_by": []})),
            panel(json!({"group_by": ["ns"], "limit": 2})),
        ]),
        &samples,
    );
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
    // All labels by default: a line each.
    assert_eq!(lines(&views[0]).len(), 5);
    assert_eq!(
        lines(&views[1]),
        [
            (s(&["a"]), vec![3.0]),
            (s(&["b"]), vec![5.0]),
            (s(&["c"]), vec![3.0]),
            (s(&["d"]), vec![0.5])
        ]
    );
    assert_eq!(lines(&views[2]), [(s(&[]), vec![11.5])]);
    // The two largest by their peak (b 5, then a and c 3: a by name), the rest in `other`.
    assert_eq!(
        lines(&views[3]),
        [
            (s(&["a"]), vec![3.0]),
            (s(&["b"]), vec![5.0]),
            (s(&["other"]), vec![3.5])
        ]
    );
    assert!(views[3].folded && !views[1].folded);
}

#[test]
fn a_counters_point_is_its_increase_or_rate_and_a_histograms_its_count_or_mean() {
    let grew = |w: &str, at: &str, v: f64| {
        sample(w, at, "started", &[("ns", "a")], SampleData::counter(v))
    };
    let observed = sample(
        "w1",
        "2026-11-21T10:00:00Z",
        "queue",
        &[],
        SampleData::Histogram {
            counts: vec![3, 1],
            sum: 6.0,
        },
    );
    let samples = [
        grew("w1", "2026-11-21T10:00:00Z", 30.0),
        grew("w2", "2026-11-21T10:00:00Z", 60.0),
        grew("w1", "2026-11-21T10:01:00Z", 30.0),
        observed,
    ];
    let panel = |metric: &str, chart: &str, extra: serde_json::Value| {
        let mut p =
            json!({"title": "T", "metric": metric, "chart": chart, "row": 1, "cell": "full"});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    let views = drawn(
        json!([
            panel("started", "bars", json!({})),
            panel("started", "rate", json!({})),
            panel("started", "bars", json!({"point": "rate"})),
            panel("started", "bars", json!({"combine": "max"})),
            panel("queue", "bars", json!({})),
            panel("queue", "bars", json!({"point": "mean"})),
            panel("queue", "rate", json!({})),
        ]),
        &samples,
    );
    // The declared panels only: the page adds one for each metric none shows.
    let values: Vec<f64> = views
        .iter()
        .filter(|v| !v.series.is_empty())
        .map(|v| lines(v)[0].1[0])
        .collect();
    // Increase 120; per second over 300 s 0.4 (a rate chart's default); max over workers each
    // minute: 60 + 30; the histogram's 4 observations, their mean 1.5, per second.
    assert_eq!(values, [120.0, 0.4, 0.4, 90.0, 4.0, 1.5, 4.0 / 300.0]);
    assert_eq!(views[4].series[0].points[0].counts, [3, 1]);
}

#[test]
fn a_rollups_point_is_read_under_the_panels_combine() {
    // A 15-minute window rolled up by the master: two workers each read 10 the whole time.
    let window = |points: (f64, f64, f64, f64)| schedr_db::extension_samples::GaugePoints {
        last: points.0,
        mean: points.1,
        max: points.2,
        min: points.3,
    };
    let mut rolled = sample(
        "",
        "2026-11-21T10:00:00Z",
        "pods",
        &[("ns", "a"), ("pod", "p")],
        SampleData::Gauge {
            avg: 20.0,
            peak: 24.0,
            minutes: 15,
            across: Some(Box::new(schedr_db::extension_samples::GaugeAcross {
                sum: window((20.0, 20.0, 24.0, 16.0)),
                max: window((10.0, 10.0, 12.0, 8.0)),
                min: window((10.0, 10.0, 12.0, 8.0)),
                mean: window((10.0, 10.0, 12.0, 8.0)),
            })),
        },
    );
    rolled.step_s = schedr_db::samples::ROLLUP;
    let panel = |extra: serde_json::Value| {
        let mut p =
            json!({"title": "Pods", "metric": "pods", "chart": "line", "row": 1, "cell": "full"});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    let views = panels_of(
        &panel_declarations(json!([
            panel(json!({})),
            panel(json!({"combine": "max"})),
            panel(json!({"combine": "max", "point": "max"})),
            panel(json!({"point": "min"})),
        ])),
        10,
        &[rolled],
        3600,
    );
    // The declared panels only: the page adds one for each metric none shows.
    let values: Vec<f64> = views
        .iter()
        .filter(|v| !v.series.is_empty())
        .map(|v| lines(v)[0].1[0])
        .collect();
    assert_eq!(values, [20.0, 10.0, 12.0, 16.0]);
}

#[test]
fn a_panel_names_only_what_its_metrics_kind_has() {
    let check = |panel: serde_json::Value| {
        schedr_model::extension::check_declarations(&panel_declarations(json!([panel])))
    };
    let base = |extra: serde_json::Value| {
        let mut p =
            json!({"title": "T", "metric": "pods", "chart": "line", "row": 1, "cell": "full"});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    assert!(
        check(base(
            json!({"point": "max", "combine": "max", "group_by": ["ns"], "limit": 3})
        ))
        .is_ok()
    );
    assert!(
        check(base(json!({"point": "rate"})))
            .unwrap_err()
            .contains("no point")
    );
    assert!(
        check(base(json!({"group_by": ["node"]})))
            .unwrap_err()
            .contains("not a label")
    );
    assert!(
        check(base(json!({"limit": 0})))
            .unwrap_err()
            .contains("at least 1")
    );
    let histogram = json!({"title": "Q", "metric": "queue", "chart": "bars", "row": 1, "cell": "full", "combine": "max"});
    assert!(check(histogram).unwrap_err().contains("only added up"));
}

#[test]
fn a_folded_series_is_the_other_the_rest_join_and_grouping_comes_before_the_cap() {
    // agy's review of M5.2i10: an `other` ranked among the named made two `other`s; capped
    // before grouping, a folded pod lost its namespace.
    let at = "2026-11-21T10:00:00Z";
    let samples = [
        pods("w1", at, "other", "other", 50.0),
        pods("w1", at, "a", "p1", 20.0),
        pods("w1", at, "b", "p2", 10.0),
        pods("w1", at, "c", "p3", 5.0),
        pods("w1", at, "a", "p4", 1.0),
    ];
    let panel = |extra: serde_json::Value| {
        let mut p = json!({"title": "Pods", "metric": "pods", "chart": "line", "row": 1, "cell": "full",
                           "group_by": ["ns"]});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        p
    };
    let declarations = panel_declarations(json!([panel(json!({"limit": 2})), panel(json!({}))]));
    // A series cap of 3: the groups capped, a's two pods still one line.
    let views = panels_of(&declarations, 3, &samples, 300);
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
    assert_eq!(
        lines(&views[0]),
        [
            (s(&["a"]), vec![21.0]),
            (s(&["b"]), vec![10.0]),
            (s(&["other"]), vec![55.0])
        ]
    );
    assert_eq!(
        lines(&views[1]),
        [
            (s(&["a"]), vec![21.0]),
            (s(&["b"]), vec![10.0]),
            (s(&["c"]), vec![5.0]),
            (s(&["other"]), vec![50.0])
        ]
    );
    assert!(views[0].folded && views[1].folded);
}

#[test]
fn rollup_means_added_up_are_weighted_by_the_minutes_each_series_covered() {
    // agy's review of M5.2i10: a pod seen 1 minute of 15 counted as one seen all 15.
    let rolled = |pod: &str, mean: f64, minutes: u32| {
        let points = schedr_db::extension_samples::GaugePoints {
            last: mean,
            mean,
            max: mean,
            min: mean,
        };
        let mut s = sample(
            "",
            "2026-11-21T10:00:00Z",
            "pods",
            &[("ns", "a"), ("pod", pod)],
            SampleData::Gauge {
                avg: mean,
                peak: mean,
                minutes,
                across: Some(Box::new(schedr_db::extension_samples::GaugeAcross {
                    sum: points,
                    max: points,
                    min: points,
                    mean: points,
                })),
            },
        );
        s.step_s = schedr_db::samples::ROLLUP;
        s
    };
    let declarations = panel_declarations(json!([
        {"title": "Pods", "metric": "pods", "chart": "line", "row": 1, "cell": "full", "group_by": ["ns"]}
    ]));
    let views = panels_of(
        &declarations,
        10,
        &[rolled("steady", 10.0, 15), rolled("brief", 100.0, 1)],
        3600,
    );
    let value = lines(&views[0])[0].1[0];
    assert!(
        (value - (15.0 * 10.0 + 100.0) / 15.0).abs() < 1e-9,
        "{value}"
    );
}

#[test]
fn an_increase_named_on_a_rate_chart_is_its_increase() {
    // agy's review of M5.2i10: it was divided per second all the same.
    let samples = [sample(
        "w1",
        "2026-11-21T10:00:00Z",
        "started",
        &[("ns", "a")],
        SampleData::counter(120.0),
    )];
    let views = drawn(
        json!([{"title": "S", "metric": "started", "chart": "rate", "row": 1, "cell": "full",
                "point": "increase"}]),
        &samples,
    );
    assert_eq!(lines(&views[0])[0].1, [120.0]);
}

#[test]
fn an_events_panel_takes_no_function_and_quantiles_only_on_their_chart() {
    // agy's review of M5.2i10.
    let check = |panel: serde_json::Value| {
        schedr_model::extension::check_declarations(&panel_declarations(json!([panel])))
    };
    let events = json!({"title": "E", "chart": "events", "row": 1, "cell": "full", "limit": 0});
    assert!(check(events).unwrap_err().contains("events panel"));
    let bars = json!({"title": "Q", "metric": "queue", "chart": "bars", "row": 1, "cell": "full",
                      "point": "quantiles"});
    assert!(check(bars).unwrap_err().contains("quantiles"));
    let chart = json!({"title": "Q", "metric": "queue", "chart": "quantiles", "row": 1, "cell": "full",
                       "point": "quantiles"});
    assert!(check(chart).is_ok());
}
