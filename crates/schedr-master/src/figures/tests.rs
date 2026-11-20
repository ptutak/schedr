use pretty_assertions::assert_eq;
use schedr_model::extension::{MetricFigures, SeriesFigures};

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn figures(started: f64, running: f64, queue: [u64; 2]) -> ExtensionFigures {
    let series = |value: f64, counts: Vec<u64>| SeriesFigures {
        labels: BTreeMap::new(),
        value,
        counts,
        sum: 0.0,
    };
    let metric = |name: &str, kind: MetricKind, buckets: Vec<f64>, series| MetricFigures {
        name: name.to_owned(),
        kind,
        buckets,
        series: vec![series],
    };
    ExtensionFigures {
        extension: "k".to_owned(),
        version: "1".to_owned(),
        max_series: 10,
        declarations: None,
        metrics: vec![
            metric(
                "started",
                MetricKind::Counter,
                Vec::new(),
                series(started, Vec::new()),
            ),
            metric(
                "running",
                MetricKind::Gauge,
                Vec::new(),
                series(running, Vec::new()),
            ),
            metric(
                "queue",
                MetricKind::Histogram,
                vec![1.0],
                series(0.0, queue.to_vec()),
            ),
        ],
    }
}

fn data(samples: &[ExtensionSample]) -> Vec<(&str, SampleData)> {
    samples
        .iter()
        .map(|s| (s.metric.as_str(), s.data.clone()))
        .collect()
}

#[test]
fn a_minute_is_what_the_worker_says_grew_with_no_earlier_figures_needed() {
    // A worker's first figures count whole (Codex's review: they were a baseline only), and
    // a restarted worker's too: what it sends is its growth since it last sent.
    let at = ts("2026-11-21T10:01:00Z");
    let first = minute_samples(&figures(3.0, 2.0, [1, 1]), "w1", at);
    assert_eq!(
        data(&first),
        [
            ("started", SampleData::Counter { growth: 3.0 }),
            (
                "running",
                SampleData::Gauge {
                    avg: 2.0,
                    peak: 2.0,
                    minutes: 1
                }
            ),
            (
                "queue",
                SampleData::Histogram {
                    counts: vec![1, 1],
                    sum: 0.0
                }
            ),
        ]
    );
    // Nothing grew: only the gauge's value.
    let quiet = minute_samples(&figures(0.0, 0.0, [0, 0]), "w1", at);
    assert_eq!(quiet.len(), 1);
    assert_eq!(quiet[0].metric, "running");
}

#[test]
fn past_a_metrics_cap_across_workers_new_series_are_other_until_the_hour_ends() {
    let sample = |worker: &str, value: &str, at: &str| ExtensionSample {
        name: "k".to_owned(),
        version: "1".to_owned(),
        worker_id: worker.to_owned(),
        metric: "started".to_owned(),
        labels: [("pod".to_owned(), value.to_owned())].into(),
        step_s: 60,
        at: ts(at),
        data: SampleData::Counter { growth: 1.0 },
    };
    let mut seen = SeriesSeen::default();
    let at = "2026-11-21T10:01:00Z";
    // Five workers, each within a cap of 2 alone (Codex's review: five named series stored).
    let mut samples: Vec<ExtensionSample> = ["a", "b", "c", "d", "e"]
        .iter()
        .enumerate()
        .map(|(i, pod)| sample(&format!("w{i}"), pod, at))
        .collect();
    seen.fold(&mut samples, 2, ts(at));
    let pods: Vec<&str> = samples.iter().map(|s| s.labels["pod"].as_str()).collect();
    assert_eq!(pods, ["a", "b", "other", "other", "other"]);
    // A series stored this hour stays named; another hour starts again.
    let mut later = vec![
        sample("w9", "a", "2026-11-21T10:30:00Z"),
        sample("w9", "z", "2026-11-21T10:30:00Z"),
    ];
    seen.fold(&mut later, 2, ts("2026-11-21T10:30:00Z"));
    assert_eq!(later[0].labels["pod"], "a");
    assert_eq!(later[1].labels["pod"], "other");
    let mut next_hour = vec![sample("w9", "z", "2026-11-21T11:00:00Z")];
    seen.fold(&mut next_hour, 2, ts("2026-11-21T11:00:00Z"));
    assert_eq!(next_hour[0].labels["pod"], "z");
}

#[test]
fn a_window_rolls_up_across_workers() {
    let minute = |worker: &str, at: &str, metric: &str, data: SampleData| ExtensionSample {
        name: "k".to_owned(),
        version: "1".to_owned(),
        worker_id: worker.to_owned(),
        metric: metric.to_owned(),
        labels: BTreeMap::new(),
        step_s: 60,
        at: ts(at),
        data,
    };
    let gauge = |v: f64| SampleData::Gauge {
        avg: v,
        peak: v,
        minutes: 1,
    };
    let minutes = [
        minute("w1", "2026-11-21T10:00:00Z", "running", gauge(2.0)),
        minute("w2", "2026-11-21T10:00:00Z", "running", gauge(3.0)),
        minute("w1", "2026-11-21T10:01:00Z", "running", gauge(1.0)),
        minute(
            "w1",
            "2026-11-21T10:00:00Z",
            "started",
            SampleData::Counter { growth: 2.0 },
        ),
        minute(
            "w2",
            "2026-11-21T10:01:00Z",
            "started",
            SampleData::Counter { growth: 3.0 },
        ),
    ];
    let rolled = rolled_up(&minutes, ts("2026-11-21T10:00:00Z"));
    assert_eq!(
        data(&rolled),
        [
            (
                "running",
                SampleData::Gauge {
                    avg: 3.0,
                    peak: 5.0,
                    minutes: 2
                }
            ),
            ("started", SampleData::Counter { growth: 5.0 }),
        ]
    );
    assert!(
        rolled
            .iter()
            .all(|s| s.worker_id.is_empty() && s.step_s == 900)
    );
}

#[test]
fn figures_past_their_bounds_are_refused() {
    assert!(checked(figures(1.0, 1.0, [0, 0])).is_ok());
    let mut too_many = figures(1.0, 1.0, [0, 0]);
    too_many.max_series = 0;
    assert!(checked(too_many).is_err());
    let mut wide = figures(1.0, 1.0, [0, 0]);
    wide.metrics[0].series = (0..12).map(|_| wide.metrics[0].series[0].clone()).collect();
    assert!(checked(wide).unwrap_err().contains("series cap"));
    let mut bad = figures(f64::NAN, 1.0, [0, 0]);
    bad.extension = "k".to_owned();
    assert!(checked(bad).is_err());
    // Each series of its metric's kind (Codex's review: counts past the bounds crashed the
    // page): a histogram's counts one past its bounds, which rise; a counter's growth >= 0.
    let mut long = figures(1.0, 1.0, [0, 0]);
    long.metrics[2].series[0].counts = vec![0, 0, 1];
    assert!(checked(long).unwrap_err().contains("series"));
    let mut falling = figures(1.0, 1.0, [0, 0]);
    falling.metrics[2].buckets = vec![2.0, 1.0];
    falling.metrics[2].series[0].counts = vec![0, 0, 0];
    assert!(checked(falling).unwrap_err().contains("buckets"));
    let mut shrinking = figures(-1.0, 1.0, [0, 0]);
    shrinking.metrics[1].buckets = vec![1.0];
    assert!(checked(shrinking).is_err());
    assert!(
        checked(figures(-1.0, 1.0, [0, 0])).is_err(),
        "a counter doesn't shrink"
    );
}

#[test]
fn an_extension_event_is_kept_checked_cut_and_never_in_the_future() {
    let now: Timestamp = "2026-11-21T10:00:00Z".parse().unwrap();
    let json = |extension: &str, at_ms: i64, message: &str| {
        serde_json::to_vec(&serde_json::json!({
            "at_ms": at_ms, "extension": extension, "kind": "refused", "message": message,
        }))
        .unwrap()
    };
    let later = now.as_millisecond() + 60_000;
    let kept = checked_event(&json("kubernetes", later, &"x".repeat(2000)), now).unwrap();
    assert_eq!(
        kept.at_ms,
        now.as_millisecond(),
        "a worker's clock ahead: now"
    );
    assert_eq!(
        kept.message.chars().count(),
        schedr_model::extension::MAX_TEXT
    );
    assert_eq!(checked_event(&json("Bad Name!", 0, "m"), now), None);
    assert_eq!(checked_event(b"not json", now), None);
    // agy's review of M5.2i5: past the cap, the newest are kept.
    let sent: Vec<Vec<u8>> = (0..5)
        .map(|n| json("kubernetes", n, &n.to_string()))
        .collect();
    let kept: Vec<String> = newest_events(&sent, 2, now)
        .into_iter()
        .map(|e| e.message)
        .collect();
    assert_eq!(kept, ["3", "4"]);
}

#[test]
fn a_rollup_keeps_a_minutes_peak() {
    // Codex's second review of M5.2i5: the peak was read from the averages.
    let minute = |worker: &str, avg: f64, peak: f64| ExtensionSample {
        name: "k".to_owned(),
        version: "1".to_owned(),
        worker_id: worker.to_owned(),
        metric: "running".to_owned(),
        labels: BTreeMap::new(),
        step_s: 60,
        at: ts("2026-11-21T10:00:00Z"),
        data: SampleData::Gauge {
            avg,
            peak,
            minutes: 1,
        },
    };
    let rolled = rolled_up(
        &[minute("w1", 3.0, 5.0), minute("w2", 1.0, 2.0)],
        ts("2026-11-21T10:00:00Z"),
    );
    assert_eq!(
        rolled[0].data,
        SampleData::Gauge {
            avg: 4.0,
            peak: 7.0,
            minutes: 1
        }
    );
}

#[test]
fn invented_label_names_dont_pass_the_cap_and_a_takeover_keeps_the_hours_series() {
    let sample = |labels: &[(&str, &str)]| ExtensionSample {
        name: "k".to_owned(),
        version: "1".to_owned(),
        worker_id: "w1".to_owned(),
        metric: "m".to_owned(),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        step_s: 60,
        at: ts("2026-11-21T10:05:00Z"),
        data: SampleData::Counter { growth: 1.0 },
    };
    let at = ts("2026-11-21T10:05:00Z");
    // Codex's second review: other label names each made another `other`.
    let mut seen = SeriesSeen::default();
    let mut samples: Vec<ExtensionSample> = (0..8)
        .map(|n| sample(&[(&format!("invented{n}"), "x")]))
        .collect();
    assert_eq!(seen.fold(&mut samples, 1, at), 7);
    assert_eq!(samples.len(), 1);
    // A master taking over in the hour loads what it stored (Codex's second review: it
    // started again, naming more).
    let mut after = SeriesSeen::default();
    let mut next = vec![sample(&[("pod", "b")])];
    let (hour, unloaded) = after.unloaded(&next, at);
    assert_eq!(hour, ts("2026-11-21T10:00:00Z"));
    assert_eq!(unloaded, [("k".to_owned(), "m".to_owned())]);
    after.load(
        unloaded[0].clone(),
        vec![[("pod".to_owned(), "a".to_owned())].into()],
    );
    after.fold(&mut next, 1, at);
    assert_eq!(next[0].labels["pod"], "other");
    assert_eq!(after.unloaded(&next, at).1, [], "loaded once an hour");
    // agy's review of M5.2i5: a failed read (no load) is tried again with the next figures.
    let mut failed = SeriesSeen::default();
    let mut first = vec![sample(&[("pod", "b")])];
    let (_, unloaded) = failed.unloaded(&first, at);
    assert_eq!(unloaded.len(), 1);
    failed.fold(&mut first, 1, at);
    assert_eq!(failed.unloaded(&first, at).1, unloaded, "read again");
    // A stored series without labels fixes the names too: invented ones are dropped.
    let mut unlabeled = SeriesSeen::default();
    let mut invented = vec![sample(&[("invented", "x")])];
    let _ = unlabeled.unloaded(&invented, at);
    unlabeled.load(("k".to_owned(), "m".to_owned()), vec![Labels::new()]);
    assert_eq!(unlabeled.fold(&mut invented, 1, at), 1);
}
