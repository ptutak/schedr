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
            SampleData::Counter { growth: 2.0 },
        ),
        sample(
            "w2",
            "2026-11-21T10:01:00Z",
            "started",
            &[("ns", "a")],
            SampleData::Counter { growth: 3.0 },
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
    let grew = |v: f64| SampleData::Counter { growth: v };
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
            SampleData::Counter { growth: 1.0 },
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
            SampleData::Counter { growth: 1.0 },
        ),
        sample(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            &[("ns", "other")],
            SampleData::Counter { growth: 2.0 },
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
