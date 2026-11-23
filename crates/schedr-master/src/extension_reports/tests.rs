//! The writer of extensions' reports (decision 521).

use std::time::{Duration, Instant};

use jiff::Timestamp;
use sqlx::Connection as _;

use super::*;
use schedr_db::extension_samples::ExtensionSample;

fn settings() -> ReportSettings {
    ReportSettings {
        queue: 8,
        flush_interval: Duration::from_millis(20),
        write_timeout: Duration::from_millis(200),
        events_per_flush: 3,
        records: 256,
        events_kept: 200,
    }
}

/// A heartbeat's events of `extension`, `count` of them.
fn events(worker: &str, extension: &str, count: usize) -> Report {
    let at = Timestamp::now().as_millisecond();
    let sent = (0..count)
        .map(|n| {
            serde_json::to_vec(&serde_json::json!({
                "at_ms": at + i64::try_from(n).unwrap(),
                "extension": extension,
                "kind": "refused",
                "message": format!("e{n}"),
            }))
            .unwrap()
        })
        .collect();
    Report::Events {
        worker_id: worker.to_owned(),
        sent,
        now: Timestamp::now(),
    }
}

fn dropped(metrics: &MasterMetrics, reason: &str) -> u64 {
    metrics
        .extension_reports_dropped
        .get_or_create(&crate::metrics::ReasonLabels {
            reason: reason.to_owned(),
        })
        .get()
}

#[tokio::test]
async fn a_flush_is_written_and_events_past_what_it_keeps_of_one_extension_are_dropped() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let metrics = MasterMetrics::default();
    let reports = ExtensionReports::start(db.clone(), settings(), metrics.clone());
    reports.offer(events("w1", "dummy", 5));
    reports.offer(events("w2", "other", 1));
    reports.flushed().await;
    let kept = db.extension_events("dummy", None, 100).await.unwrap();
    assert_eq!(
        kept.len(),
        3,
        "a flush keeps `events_per_flush` of one extension"
    );
    assert_eq!(dropped(&metrics, "many"), 2);
    assert_eq!(
        db.extension_events("other", None, 100).await.unwrap().len(),
        1
    );
}

/// D521's done-when: with the database held up, handing reports over never waits (the master's
/// turn and round go on), and they are dropped, counted: by the write's time limit, and once the
/// queue is full.
#[tokio::test]
async fn a_slow_database_drops_reports_and_never_holds_up_whoever_hands_them_over() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("db.sqlite").display()
    );
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    // Another process holding the write lock: every write waits on it.
    let mut holder = sqlx::SqliteConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut holder)
        .await
        .unwrap();
    let metrics = MasterMetrics::default();
    let reports = ExtensionReports::start(db.clone(), settings(), metrics.clone());
    let began = Instant::now();
    for _ in 0..1000 {
        reports.offer(events("w1", "dummy", 1));
    }
    assert!(
        began.elapsed() < Duration::from_millis(500),
        "handing over 1000 reports took {:?}",
        began.elapsed()
    );
    assert!(dropped(&metrics, "full") > 0, "past the queue, dropped");
    let waited = Instant::now();
    reports.flushed().await;
    assert!(
        dropped(&metrics, "slow") > 0,
        "the held write dropped by its time limit"
    );
    // Each write gave up at its limit: the queue's few flushes, not the database's own timeout.
    assert!(
        waited.elapsed() < Duration::from_secs(5),
        "the writer waited {:?}",
        waited.elapsed()
    );
    sqlx::query("ROLLBACK").execute(&mut holder).await.unwrap();
    // The database back: the next report is written.
    reports.offer(events("w1", "dummy", 1));
    reports.flushed().await;
    assert_eq!(
        db.extension_events("dummy", None, 100).await.unwrap().len(),
        1
    );
}

#[test]
fn an_attempts_phase_changes_are_kept_its_progress_ticks_coalesced_and_its_links_by_label() {
    const A1: &str = "01a2078e-7f86-73d7-a12c-9dedee98c601";
    const A2: &str = "01a2078e-7f86-73d7-a12c-9dedee98c602";
    let mut flush = Flush::default();
    let status = |attempt: &str, phase: &str, progress: f64| pb::AttemptReported {
        attempt_id: attempt.to_owned(),
        extension: "dummy".to_owned(),
        extension_version: "1.0".to_owned(),
        report: Some(pb::attempt_reported::Report::Status(pb::StatusReport {
            at_ms: 0,
            phase: phase.to_owned(),
            message: None,
            progress: Some(progress),
        })),
    };
    let link = |label: &str, url: &str| pb::AttemptReported {
        attempt_id: A1.to_owned(),
        extension: "dummy".to_owned(),
        extension_version: "1.0".to_owned(),
        report: Some(pb::attempt_reported::Report::Link(pb::LinkReport {
            label: label.to_owned(),
            url: url.to_owned(),
        })),
    };
    for (attempt, phase, progress) in [
        (A1, "pulling", 0.1),
        (A1, "pulling", 0.4),
        (A2, "pulling", 0.5),
        (A1, "pulling", 0.8),
        (A1, "running", 0.0),
        (A1, "", 0.1),
        (A1, "running", f64::NAN),
        (A1, "pulling", 0.9),
    ] {
        attempt_report(
            &mut flush,
            "w1".to_owned(),
            status(attempt, phase, progress),
        );
    }
    attempt_report(&mut flush, "w1".to_owned(), link("Pod", "https://a"));
    attempt_report(&mut flush, "w1".to_owned(), link("Pod", "https://b"));
    attempt_report(
        &mut flush,
        "w1".to_owned(),
        link("Bad", "javascript:alert(1)"),
    );
    let kept: Vec<(&str, &str, Option<f64>)> = flush
        .statuses
        .iter()
        .map(|s| {
            (
                s.attempt_id.as_str(),
                s.status.phase.as_str(),
                s.status.progress,
            )
        })
        .collect();
    assert_eq!(
        kept,
        [
            (A1, "pulling", Some(0.8)),
            (A2, "pulling", Some(0.5)),
            // NaN isn't a progress; an empty phase isn't a status.
            (A1, "running", None),
            // A phase again after another: a change, kept.
            (A1, "pulling", Some(0.9)),
        ]
    );
    let links: Vec<&ReportedLink> = flush.links.values().collect();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].link.url, "https://b");
}

fn report(
    worker: &str,
    attempt: &str,
    phase: &str,
    link: Option<(&str, &str)>,
) -> (String, pb::AttemptReported) {
    let report = match link {
        Some((label, url)) => pb::attempt_reported::Report::Link(pb::LinkReport {
            label: label.to_owned(),
            url: url.to_owned(),
        }),
        None => pb::attempt_reported::Report::Status(pb::StatusReport {
            at_ms: 0,
            phase: phase.to_owned(),
            message: Some("a\0b".to_owned()),
            progress: None,
        }),
    };
    (
        worker.to_owned(),
        pb::AttemptReported {
            attempt_id: attempt.to_owned(),
            extension: "dummy".to_owned(),
            extension_version: "1.0".to_owned(),
            report: Some(report),
        },
    )
}

/// agy's review of M5.2i12: another worker's report of an attempt doesn't replace its owner's
/// in the flush (only the owner's is then kept); an ID that isn't a UUID is dropped (it would
/// fail the flush's transaction on Postgres), and a NUL in a text is replaced.
#[test]
fn reports_are_kept_apart_by_worker_and_made_storable() {
    let attempt = "01a2078e-7f86-73d7-a12c-9dedee98c694";
    let mut flush = Flush::default();
    for (worker, reported) in [
        report("w1", attempt, "running", None),
        report("w2", attempt, "running", None),
        report("w1", attempt, "", Some(("Pod", "https://a"))),
        report("w2", attempt, "", Some(("Pod", "https://forged"))),
        report("w1", "not-a-uuid", "running", None),
    ] {
        attempt_report(&mut flush, worker, reported);
    }
    let workers: Vec<&str> = flush
        .statuses
        .iter()
        .map(|s| s.worker_id.as_str())
        .collect();
    assert_eq!(workers, ["w1", "w2"]);
    assert_eq!(
        flush.statuses[0].status.message.as_deref(),
        Some("a\u{fffd}b")
    );
    let links: Vec<(&str, &str)> = flush
        .links
        .values()
        .map(|l| (l.worker_id.as_str(), l.link.url.as_str()))
        .collect();
    assert_eq!(links, [("w1", "https://a"), ("w2", "https://forged")]);
}

fn gauge(value: f64) -> ExtensionSample {
    ExtensionSample {
        name: "dummy".to_owned(),
        version: "1.0".to_owned(),
        worker_id: "w1".to_owned(),
        metric: "in_flight".to_owned(),
        labels: std::collections::BTreeMap::new(),
        step_s: 60,
        at: Timestamp::from_second(1_800_000_000).unwrap(),
        data: schedr_db::extension_samples::SampleData::Gauge {
            avg: value,
            peak: value,
            minutes: 1,
            across: None,
        },
    }
}

/// agy's review of M5.2i12: a worker's gauge in two heartbeats of one flush is its later
/// reading, not their sum; two of its series folded into one in a heartbeat still add up.
#[test]
fn a_gauge_sent_twice_in_a_flush_is_merged_and_folded_series_added() {
    let mut samples = std::collections::BTreeMap::new();
    merge_heartbeat(&mut samples, vec![gauge(100.0)]);
    merge_heartbeat(&mut samples, vec![gauge(100.0)]);
    let data = &samples.values().next().unwrap().data;
    assert!(
        matches!(data, schedr_db::extension_samples::SampleData::Gauge { avg, .. } if (*avg - 100.0).abs() < 1e-9),
        "{data:?}"
    );
    let mut folded = std::collections::BTreeMap::new();
    merge_heartbeat(&mut folded, vec![gauge(100.0), gauge(50.0)]);
    let data = &folded.values().next().unwrap().data;
    assert!(
        matches!(data, schedr_db::extension_samples::SampleData::Gauge { avg, .. } if (*avg - 150.0).abs() < 1e-9),
        "{data:?}"
    );
}

/// A heartbeat's figures of `extension` with one counter, as a worker sends them.
fn figures(worker: &str, extension: &str) -> Report {
    let figures = serde_json::json!({
        "extension": extension,
        "version": "1.0",
        "max_series": 10,
        "declarations": {"metrics": [{"name": "jobs", "kind": "counter", "help": "Jobs"}]},
        "metrics": [{"name": "jobs", "kind": "counter", "series": [{"value": 1.0}]}],
    });
    Report::Figures {
        worker_id: worker.to_owned(),
        sent: vec![serde_json::to_vec(&figures).unwrap()],
        now: Timestamp::now(),
    }
}

/// agy's review of M5.2i12: a series read that fails isn't tried again for each worker of the
/// same flush (each try waiting out its time limit on a slow table); and the writer's writes
/// don't signal a change (the trigger loop would wake for each flush).
#[tokio::test]
async fn a_failed_series_read_is_tried_once_a_flush_and_writes_are_quiet() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("db.sqlite").display()
    );
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let metrics = MasterMetrics::default();
    let reports = ExtensionReports::start(db.clone(), settings(), metrics.clone());
    let changed = {
        let db = db.clone();
        tokio::spawn(async move { db.changed().await })
    };
    reports.offer(events("w1", "dummy", 1));
    reports.flushed().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!changed.is_finished(), "a quiet write signalled a change");
    changed.abort();
    let mut other = sqlx::SqliteConnection::connect(&url).await.unwrap();
    sqlx::query("DROP TABLE extension_samples")
        .execute(&mut other)
        .await
        .unwrap();
    for worker in ["w1", "w2", "w3"] {
        reports.offer(figures(worker, "dummy"));
    }
    reports.flushed().await;
    let mut registry = prometheus_client::registry::Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    let reads: f64 = text
        .lines()
        .find_map(|l| {
            l.strip_prefix("schedr_master_extension_query_seconds_count{query=\"series\"} ")
        })
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0.0);
    assert!(
        (reads - 1.0).abs() < f64::EPSILON,
        "{reads} reads of the failed series for three workers"
    );
}

/// A heartbeat's figures of a `queue`-labelled counter capped at one series, its series `queue`.
fn queued(worker: &str, queue: &str) -> Report {
    let figures = serde_json::json!({
        "extension": "dummy",
        "version": "1.0",
        "max_series": 1,
        "declarations": {"metrics": [{"name": "jobs", "kind": "counter", "help": "Jobs",
                                      "labels": [{"name": "queue"}]}]},
        "metrics": [{"name": "jobs", "kind": "counter",
                     "series": [{"labels": {"queue": queue}, "value": 1.0}]}],
    });
    Report::Figures {
        worker_id: worker.to_owned(),
        sent: vec![serde_json::to_vec(&figures).unwrap()],
        now: Timestamp::now(),
    }
}

/// Grok's review of M5.2i12: a minutes write that failed doesn't spend the hour's series cap
/// on the series it held: the next one, once stored, is kept under its own labels.
#[tokio::test]
async fn a_failed_minutes_write_spends_none_of_the_series_cap() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("db.sqlite").display()
    );
    let db = Database::connect(&url).await.unwrap();
    db.migrate().await.unwrap();
    let reports = ExtensionReports::start(db.clone(), settings(), MasterMetrics::default());
    let mut holder = sqlx::SqliteConnection::connect(&url).await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut holder)
        .await
        .unwrap();
    reports.offer(queued("w1", "a"));
    reports.flushed().await;
    sqlx::query("ROLLBACK").execute(&mut holder).await.unwrap();
    reports.offer(queued("w1", "b"));
    reports.flushed().await;
    let since = Timestamp::now() - jiff::SignedDuration::from_hours(2);
    let series = db
        .extension_series_since("dummy", "jobs", since)
        .await
        .unwrap();
    let queues: Vec<&str> = series
        .iter()
        .map(|labels| labels["queue"].as_str())
        .collect();
    assert_eq!(queues, ["b"], "not folded into `other`");
}
