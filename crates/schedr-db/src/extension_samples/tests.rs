use pretty_assertions::assert_eq;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn db() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn sample(worker: &str, at: &str, data: SampleData) -> ExtensionSample {
    ExtensionSample {
        name: "kubernetes".to_owned(),
        version: "1.0".to_owned(),
        worker_id: worker.to_owned(),
        metric: "pods_started".to_owned(),
        labels: [("namespace".to_owned(), "a".to_owned())].into(),
        step_s: 60,
        at: ts(at),
        data,
    }
}

#[tokio::test]
async fn an_extensions_declarations_are_kept_by_worker_and_renewed() {
    let db = db().await;
    let declarations = Declarations::default();
    db.extension_seen(
        "kubernetes",
        "1.0",
        "w1",
        Some((&declarations, 100)),
        ts("2026-11-21T10:00:00Z"),
    )
    .await
    .unwrap();
    db.extension_seen("kubernetes", "1.0", "w1", None, ts("2026-11-21T10:01:00Z"))
        .await
        .unwrap();
    // Not declared yet: not kept by a bare sighting.
    db.extension_seen("kubernetes", "1.0", "w2", None, ts("2026-11-21T10:01:00Z"))
        .await
        .unwrap();
    let versions = db.extension_versions().await.unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        (versions[0].max_series, versions[0].seen_at),
        (100, ts("2026-11-21T10:01:00Z"))
    );
}

#[tokio::test]
async fn samples_are_stored_once_read_by_step_and_window_and_deleted_when_old() {
    let db = db().await;
    let growth = SampleData::Counter { growth: 2.0 };
    let rows = [
        sample("w1", "2026-11-21T10:00:00Z", growth.clone()),
        sample("w2", "2026-11-21T10:00:00Z", growth.clone()),
        sample(
            "w1",
            "2026-11-21T10:01:00Z",
            SampleData::Histogram {
                counts: vec![1, 0],
                sum: 0.5,
            },
        ),
    ];
    db.insert_extension_samples(&rows).await.unwrap();
    db.insert_extension_samples(&rows[..1]).await.unwrap();
    let read = db
        .extension_samples(
            Some("kubernetes"),
            60,
            ts("2026-11-21T10:00:00Z"),
            ts("2026-11-21T10:02:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(read, rows);
    let none = db
        .extension_samples(
            Some("other"),
            60,
            ts("2026-11-21T10:00:00Z"),
            ts("2026-11-21T10:02:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(none, Vec::new());
    assert_eq!(
        db.delete_old_extension_samples(60, ts("2026-11-21T10:01:00Z"))
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn an_extensions_events_are_kept_newest_first_trimmed_and_deleted_when_old() {
    use schedr_model::extension::{EventKind, ExtensionEvent, KEPT_EVENTS};
    let db = db().await;
    let at = ts("2026-11-21T10:00:00Z").as_millisecond();
    let event = |n: usize, extension: &str| ExtensionEvent {
        at_ms: at + i64::try_from(n).unwrap() * 1000,
        extension: extension.to_owned(),
        version: n.is_multiple_of(2).then(|| "1.0".to_owned()),
        kind: EventKind::Dropped,
        message: format!("event {n}"),
    };
    let many: Vec<ExtensionEvent> = (0..KEPT_EVENTS + 5).map(|n| event(n, "k")).collect();
    db.insert_extension_events("w1", &many, KEPT_EVENTS)
        .await
        .unwrap();
    db.insert_extension_events("w2", &[event(0, "other")], KEPT_EVENTS)
        .await
        .unwrap();
    // The master's own count (D493: `MASTER__EXTENSION_EVENTS_KEPT`): another's, untouched.
    db.insert_extension_events(
        "w3",
        &[event(1, "small"), event(2, "small"), event(3, "small")],
        2,
    )
    .await
    .unwrap();
    let small = db.extension_events("small", None, 10).await.unwrap();
    assert_eq!(
        small.iter().map(|e| e.message.as_str()).collect::<Vec<_>>(),
        ["event 3", "event 2"]
    );
    let kept = db
        .extension_events("k", None, u32::try_from(KEPT_EVENTS * 2).unwrap())
        .await
        .unwrap();
    assert_eq!(kept.len(), KEPT_EVENTS, "the oldest past the cap went");
    assert_eq!(kept[0].message, format!("event {}", KEPT_EVENTS + 4));
    assert_eq!(kept.last().unwrap().message, "event 5");
    assert_eq!(
        (kept[0].worker_id.as_str(), kept[0].kind),
        ("w1", EventKind::Dropped)
    );
    assert_eq!(db.extension_events("k", None, 3).await.unwrap().len(), 3);
    let deleted = db
        .delete_old_extension_events(ts("2026-11-21T10:01:00Z"))
        .await
        .unwrap();
    // Those before 10:01: k's 5 to 59, other's one, small's two kept.
    assert_eq!(deleted, 58);
    assert_eq!(db.extension_events("other", None, 10).await.unwrap(), []);
    // DN21: one worker's only (agy's review of M5.2i5: cut from all workers' newest).
    db.insert_extension_events("w2", &[event(KEPT_EVENTS + 10, "k")], KEPT_EVENTS)
        .await
        .unwrap();
    let of_w2 = db.extension_events("k", Some("w2"), 1).await.unwrap();
    assert_eq!(
        of_w2
            .iter()
            .map(|e| e.worker_id.as_str())
            .collect::<Vec<_>>(),
        ["w2"]
    );
}

#[tokio::test]
async fn a_minutes_samples_sent_twice_add_up() {
    let db = db().await;
    let at = "2026-11-21T10:01:00Z";
    let gauge = |avg: f64| SampleData::Gauge {
        avg,
        peak: avg,
        minutes: 1,
    };
    let histogram = |counts: Vec<u64>, sum: f64| SampleData::Histogram { counts, sum };
    let mut queue = sample("w1", at, histogram(vec![1, 0], 0.5));
    queue.metric = "queue".to_owned();
    let mut running = sample("w1", at, gauge(5.0));
    running.metric = "running".to_owned();
    db.add_extension_minutes(&[
        sample("w1", at, SampleData::Counter { growth: 2.0 }),
        queue.clone(),
        running.clone(),
    ])
    .await
    .unwrap();
    // Again in the same minute, one of them twice in the batch (two series folded into one).
    queue.data = histogram(vec![0, 2], 4.0);
    running.data = gauge(3.0);
    db.add_extension_minutes(&[
        sample("w1", at, SampleData::Counter { growth: 1.0 }),
        sample("w1", at, SampleData::Counter { growth: 0.5 }),
        queue,
        running,
    ])
    .await
    .unwrap();
    let stored = db
        .extension_samples(Some("kubernetes"), 60, ts(at), ts("2026-11-21T10:02:00Z"))
        .await
        .unwrap();
    let by_metric: BTreeMap<&str, SampleData> = stored
        .iter()
        .map(|s| (s.metric.as_str(), s.data.clone()))
        .collect();
    assert_eq!(
        by_metric,
        BTreeMap::from([
            ("pods_started", SampleData::Counter { growth: 3.5 }),
            ("queue", histogram(vec![1, 2], 4.5)),
            (
                "running",
                SampleData::Gauge {
                    avg: 3.0,
                    peak: 5.0,
                    minutes: 1
                }
            ),
        ])
    );
}

#[tokio::test]
async fn gauges_folded_together_add_up_and_a_later_send_replaces_them() {
    // Codex's second review of M5.2i5: two folded gauges kept the later's value.
    let db = db().await;
    let at = "2026-11-21T10:01:00Z";
    let gauge = |v: f64| SampleData::Gauge {
        avg: v,
        peak: v,
        minutes: 1,
    };
    db.add_extension_minutes(&[sample("w1", at, gauge(2.0)), sample("w1", at, gauge(3.0))])
        .await
        .unwrap();
    let read = || async {
        db.extension_samples(Some("kubernetes"), 60, ts(at), ts("2026-11-21T10:02:00Z"))
            .await
            .unwrap()[0]
            .data
            .clone()
    };
    assert_eq!(read().await, gauge(5.0));
    // Sent again in the minute: the latest value, the highest peak.
    db.add_extension_minutes(&[sample("w1", at, gauge(1.0))])
        .await
        .unwrap();
    assert_eq!(
        read().await,
        SampleData::Gauge {
            avg: 1.0,
            peak: 5.0,
            minutes: 1
        }
    );
}

#[tokio::test]
async fn the_hours_series_of_a_metric_are_read_back() {
    let db = db().await;
    let counter = SampleData::Counter { growth: 1.0 };
    let mut b = sample("w2", "2026-11-21T10:30:00Z", counter.clone());
    b.labels = [("namespace".to_owned(), "b".to_owned())].into();
    db.add_extension_minutes(&[
        sample("w1", "2026-11-21T09:59:00Z", counter.clone()),
        sample("w1", "2026-11-21T10:10:00Z", counter),
        b,
    ])
    .await
    .unwrap();
    let mut series = db
        .extension_series_since("kubernetes", "pods_started", ts("2026-11-21T10:00:00Z"))
        .await
        .unwrap();
    series.sort();
    let values: Vec<&str> = series.iter().map(|l| l["namespace"].as_str()).collect();
    assert_eq!(values, ["a", "b"]);
}
