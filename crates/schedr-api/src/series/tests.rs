use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_model::usage::{TaskMemory, WorkerParts};
use tower::ServiceExt;

use schedr_db::samples::Sample;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

/// The API's clock in these tests.
fn now() -> Timestamp {
    ts("2026-11-14T10:30:20Z")
}

fn minute(process: &str, at: &str, resident: u64, cpu: Option<f64>) -> Sample {
    Sample {
        process_id: process.to_owned(),
        step_s: MINUTE,
        at: ts(at),
        roles: BTreeMap::from([("worker".to_owned(), format!("w-{process}"))]),
        resident_avg: resident,
        resident_peak: resident,
        memory_avg: resident,
        memory_peak: resident,
        minutes: 1,
        cpu_minutes: u32::from(cpu.is_some()),
        container_minutes: 0,
        covered: 1,
        cpu_seconds: Some(1.0),
        cpu_avg: cpu,
        cpu_peak: cpu,
        container_avg: None,
        container_peak: None,
        container_limit: Some(1000),
        parts: None,
    }
}

fn parts(tasks: u64) -> WorkerParts {
    WorkerParts {
        fork_servers_bytes: 1,
        watcher_hosts_bytes: 2,
        tasks_bytes: tasks,
        largest_tasks: vec![TaskMemory {
            attempt_id: "a1".to_owned(),
            dag_id: "etl".to_owned(),
            run_id: "r1".to_owned(),
            task_id: "load".to_owned(),
            map_index: None,
            bytes: tasks,
        }],
    }
}

#[test]
fn ranges_are_named_and_hold_at_most_their_points() {
    for range in Range::ALL {
        assert_eq!(Range::parse(range.name()), Some(range));
        // Codex's review of M5.1j: from the start's bucket to `now`'s, at most span / bucket.
        let from = range.from(now()).as_second();
        let last = floor(now(), range.bucket_s()).as_second();
        let points = (last - from) / i64::from(range.bucket_s()) + 1;
        assert_eq!(
            points,
            range.span_s() / i64::from(range.bucket_s()),
            "{range:?}"
        );
    }
    assert_eq!(Range::parse("2h"), None);
    assert_eq!(Range::Day.from(now()), ts("2026-11-13T10:36:00Z"));
    // The rollups end where the newest stored one does; the minutes from there (none: all).
    let rolled = Some(ts("2026-11-14T10:00:00Z"));
    assert_eq!(Range::Week.cut(now(), rolled), ts("2026-11-14T10:00:00Z"));
    assert_eq!(Range::Week.cut(now(), None), Range::Week.from(now()));
    assert_eq!(Range::Hour.cut(now(), rolled), Range::Hour.from(now()));
}

async fn seeded(samples: &[Sample]) -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db.insert_samples(samples).await.unwrap();
    db
}

/// `sample` as a rollup of `minutes`, its window sampled throughout.
fn rollup(sample: Sample, minutes: u32) -> Sample {
    Sample {
        step_s: ROLLUP,
        minutes,
        cpu_minutes: if sample.cpu_avg.is_some() { minutes } else { 0 },
        container_minutes: if sample.container_avg.is_some() {
            minutes
        } else {
            0
        },
        covered: 15,
        ..sample
    }
}

fn app(db: &Database) -> axum::Router {
    let mut state = ApiState::new(db.clone(), None);
    state.clock = now;
    crate::router(state)
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn the_endpoints_answer_each_range_with_a_workers_parts_now() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db.insert_samples(&[
        // Before the hour: in 24 h only.
        minute("p1", "2026-11-14T09:00:00Z", 100, None),
        Sample {
            parts: Some(parts(30)),
            ..minute("p1", "2026-11-14T10:28:00Z", 300, Some(1.0))
        },
        minute("p2", "2026-11-14T10:28:00Z", 50, None),
        Sample {
            step_s: ROLLUP,
            ..minute("p1", "2026-11-14T09:00:00Z", 100, None)
        },
    ])
    .await
    .unwrap();
    let app = app(&db);

    let (status, hour) = get(&app, "/api/v1/metrics/samples?range=1h").await;
    assert_eq!(status, StatusCode::OK);
    let list: SampleSeriesList = serde_json::from_value(hour).unwrap();
    assert_eq!((list.range.as_str(), list.step_s), ("1h", 60));
    assert_eq!(list.processes.len(), 2);
    assert_eq!(list.processes[0].points.len(), 1);
    let now_parts = list.processes[0].parts.as_ref().unwrap();
    assert_eq!(
        (
            now_parts.tasks_bytes,
            now_parts.largest_tasks[0].task_id.as_str()
        ),
        (30, "load")
    );
    assert_eq!(list.processes[1].parts, None);

    let (_, day) = get(&app, "/api/v1/metrics/samples?process=p1").await;
    let list: SampleSeriesList = serde_json::from_value(day).unwrap();
    assert_eq!(list.range, "24h", "the default");
    assert_eq!(list.processes.len(), 1);
    assert_eq!(list.processes[0].points.len(), 2);

    // The rollups, then the minutes since the window that last closed, with the parts from
    // the minutes; p2, sampled since, is there too.
    let (_, week) = get(&app, "/api/v1/metrics/samples?range=7d").await;
    let list: SampleSeriesList = serde_json::from_value(week).unwrap();
    assert_eq!(list.processes.len(), 2);
    assert_eq!(list.processes[0].points[0].resident_avg, 100);
    assert_eq!(
        list.processes[0].points.len(),
        2,
        "09:00's rollup and 10:28's minute"
    );
    assert!(list.processes[0].parts.is_some());

    let (_, totals) = get(&app, "/api/v1/metrics/memory?range=1h").await;
    let totals: MemoryTotals = serde_json::from_value(totals).unwrap();
    assert_eq!(
        totals.points,
        [MemoryPointView {
            at: ts("2026-11-14T10:28:00Z"),
            resident: 350
        }]
    );

    let (status, error) = get(&app, "/api/v1/metrics/samples?range=2h").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        error["error"].as_str().unwrap().contains("not `2h`"),
        "{error}"
    );
}

#[tokio::test]
async fn the_workers_list_carries_each_workers_memory_with_its_tasks() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let queues = vec!["default".to_owned()];
    for id in ["w1", "w2"] {
        db.register_worker(
            &schedr_db::workers::WorkerRegistration {
                id,
                incarnation: "0190a0a0-0000-7000-8000-000000000001",
                protocol_version: "2.1",
                schedr_version: "1.1.0",
                queues: &queues,
                slots: 2,
                watcher_slots: 0,
                log_endpoint: "",
            },
            now(),
        )
        .await
        .unwrap();
    }
    db.set_worker_usage(
        "w1",
        &schedr_model::usage::WorkerUsage {
            process: schedr_model::usage::NodeUsage {
                resident_bytes: 100,
                cpu_seconds: 1.0,
                container_bytes: Some(2000),
                container_limit_bytes: Some(4000),
            },
            fork_servers_bytes: 10,
            watcher_hosts_bytes: 20,
            tasks_bytes: 1000,
            largest_tasks: Vec::new(),
        },
        now(),
    )
    .await
    .unwrap();
    let (status, list) = get(&app(&db), "/api/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list["workers"][0]["memory"],
        serde_json::json!({"whole_bytes": 1130, "tasks_bytes": 1000,
            "container_bytes": 2000, "container_limit_bytes": 4000})
    );
    assert!(list["workers"][1].get("memory").is_none(), "{list}");
}

#[tokio::test]
async fn buckets_weigh_each_row_by_its_minutes_and_parts_add_up() {
    // Codex's review of M5.1j, finding 2: a minute without a worker and one with tasks.
    let db = seeded(&[
        minute("p1", "2026-11-14T10:00:00Z", 100, None),
        Sample {
            memory_avg: 1200,
            memory_peak: 1200,
            parts: Some(parts(1000)),
            ..minute("p1", "2026-11-14T10:01:00Z", 200, Some(1.0))
        },
    ])
    .await;
    let (_, day) = get(&app(&db), "/api/v1/metrics/samples?range=24h").await;
    let list: SampleSeriesList = serde_json::from_value(day).unwrap();
    let point = &list.processes[0].points[0];
    assert_eq!(
        (point.minutes, point.memory_avg, point.resident_avg),
        (2, 650, 150)
    );
    let parts = point.parts.unwrap();
    assert_eq!(
        point.resident_avg
            + parts.fork_servers_bytes
            + parts.watcher_hosts_bytes
            + parts.tasks_bytes,
        point.memory_avg + 2,
        "the process and its parts add up (the 1 and 2 bytes of fork servers and hosts on \
         one minute round to 1 each)"
    );
    assert_eq!(parts.tasks_bytes, 500);
    assert_eq!((point.cpu_avg, point.cpu_peak), (Some(1.0), Some(1.0)));
}

#[tokio::test]
async fn a_process_replaced_within_a_window_isnt_counted_twice() {
    // Codex's finding 1: A for 7 minutes, then B for 8, 100 bytes each: 100 all along.
    let db = seeded(&[
        rollup(minute("pa", "2026-11-14T09:45:00Z", 100, None), 7),
        rollup(minute("pb", "2026-11-14T09:45:00Z", 100, None), 8),
        rollup(minute("pm", "2026-11-14T09:45:00Z", 50, None), 15),
    ])
    .await;
    let (_, totals) = get(&app(&db), "/api/v1/metrics/memory?range=7d").await;
    let totals: MemoryTotals = serde_json::from_value(totals).unwrap();
    // The master all 15 minutes; A 7/15 and B 8/15 of 100: 150.
    assert_eq!(totals.points[0].resident, 150);
}

#[tokio::test]
async fn an_unlimited_container_now_has_no_limit() {
    // Codex's finding 3.
    let db = seeded(&[
        minute("p1", "2026-11-14T10:24:00Z", 100, None),
        Sample {
            container_limit: None,
            ..minute("p1", "2026-11-14T10:28:00Z", 100, None)
        },
    ])
    .await;
    let (_, day) = get(&app(&db), "/api/v1/metrics/samples?range=1h").await;
    let list: SampleSeriesList = serde_json::from_value(day).unwrap();
    assert_eq!(list.processes[0].container_limit, None);
}

#[tokio::test]
async fn one_process_is_read_alone_and_a_new_one_shows_before_its_rollup() {
    // Codex's findings 4 and 5: another process's rows aren't read (its parts unreadable
    // here); a process sampled after the last rollup is in the 7 days with its parts.
    let db = seeded(&[
        Sample {
            parts: Some(parts(30)),
            ..minute("new-worker", "2026-11-14T10:29:00Z", 300, None)
        },
        rollup(minute("old", "2026-11-14T09:00:00Z", 100, None), 15),
    ])
    .await;
    let app = app(&db);
    let (_, week) = get(&app, "/api/v1/metrics/samples?range=7d").await;
    let list: SampleSeriesList = serde_json::from_value(week).unwrap();
    assert_eq!(
        list.processes
            .iter()
            .map(|p| (p.process_id.as_str(), p.parts.is_some()))
            .collect::<Vec<_>>(),
        [("new-worker", true), ("old", false)]
    );
    let (status, one) = get(&app, "/api/v1/metrics/samples?range=24h&process=old").await;
    assert_eq!(status, StatusCode::OK);
    let list: SampleSeriesList = serde_json::from_value(one).unwrap();
    assert_eq!(
        list.processes.len(),
        0,
        "the 24 h read minutes only: none of `old`'s"
    );
}

#[tokio::test]
async fn a_range_returns_at_most_its_points() {
    // Codex's finding 6: a sample every minute of the day.
    let mut samples = Vec::new();
    let mut at = ts("2026-11-13T10:00:00Z");
    while at < now() {
        samples.push(minute("p1", &at.to_string(), 100, None));
        at = at.checked_add(SignedDuration::from_mins(1)).unwrap();
    }
    let db = seeded(&samples).await;
    let (_, day) = get(&app(&db), "/api/v1/metrics/samples?range=24h").await;
    let list: SampleSeriesList = serde_json::from_value(day).unwrap();
    assert_eq!(list.processes[0].points.len(), 240);
    let (_, hour) = get(&app(&db), "/api/v1/metrics/samples?range=1h").await;
    let list: SampleSeriesList = serde_json::from_value(hour).unwrap();
    assert_eq!(list.processes[0].points.len(), 60);
}

#[tokio::test]
async fn a_bucket_where_every_process_was_replaced_counts_its_memory_once() {
    // Codex's second review, finding 1: A for 3 minutes, then B for 3 (a standalone's
    // restart), 100 bytes each, filling a 6-minute bucket: 100, not 200.
    let mut samples = Vec::new();
    for (process, at) in [
        ("pa", "2026-11-14T10:00:00Z"),
        ("pa", "2026-11-14T10:01:00Z"),
        ("pa", "2026-11-14T10:02:00Z"),
        ("pb", "2026-11-14T10:03:00Z"),
        ("pb", "2026-11-14T10:04:00Z"),
        ("pb", "2026-11-14T10:05:00Z"),
    ] {
        samples.push(minute(process, at, 100, None));
    }
    let db = seeded(&samples).await;
    let app = app(&db);
    let (_, totals) = get(&app, "/api/v1/metrics/memory?range=24h").await;
    let totals: MemoryTotals = serde_json::from_value(totals).unwrap();
    assert_eq!(totals.points[0].resident, 100);
    let (_, day) = get(&app, "/api/v1/metrics/samples?range=24h").await;
    let list: SampleSeriesList = serde_json::from_value(day).unwrap();
    assert_eq!(
        list.coverage,
        [CoverageView {
            at: ts("2026-11-14T10:00:00Z"),
            minutes: 6
        }]
    );
}

#[tokio::test]
async fn cpu_and_container_averages_weigh_only_the_minutes_that_have_them() {
    // Codex's second review, finding 2: a rollup with one CPU figure of 2 (its first minute
    // unknown), then 15 minutes of 0: 2 / 16, not 4 / 17.
    let db = seeded(&[
        Sample {
            cpu_avg: Some(2.0),
            cpu_peak: Some(2.0),
            cpu_minutes: 1,
            container_avg: Some(200),
            container_minutes: 1,
            ..rollup(minute("p1", "2026-11-13T10:00:00Z", 100, None), 2)
        },
        Sample {
            cpu_avg: Some(0.0),
            cpu_peak: Some(0.0),
            cpu_minutes: 15,
            container_avg: Some(0),
            container_minutes: 15,
            ..rollup(minute("p1", "2026-11-13T10:15:00Z", 100, None), 15)
        },
    ])
    .await;
    let (_, week) = get(&app(&db), "/api/v1/metrics/samples?range=7d").await;
    let list: SampleSeriesList = serde_json::from_value(week).unwrap();
    let point = &list.processes[0].points[0];
    assert_eq!(point.cpu_avg, Some(0.125));
    assert_eq!(point.container_avg, Some(13), "200 / 16, rounded");
}

#[tokio::test]
async fn the_latest_limit_holds_after_the_minutes_and_across_rollups() {
    // Codex's second review, finding 3: unlimited at 10:19, nothing since; and a rollup
    // limited, then one unlimited.
    let db = seeded(&[
        Sample {
            container_limit: Some(1000),
            ..minute("p1", "2026-11-14T10:18:00Z", 100, None)
        },
        Sample {
            container_limit: None,
            ..minute("p1", "2026-11-14T10:19:00Z", 100, None)
        },
        Sample {
            container_limit: Some(1000),
            ..rollup(minute("p2", "2026-11-13T09:00:00Z", 100, None), 15)
        },
        Sample {
            container_limit: None,
            ..rollup(minute("p2", "2026-11-13T09:15:00Z", 100, None), 15)
        },
    ])
    .await;
    let app = app(&db);
    let (_, day) = get(&app, "/api/v1/metrics/samples?range=1h").await;
    let list: SampleSeriesList = serde_json::from_value(day).unwrap();
    assert_eq!(list.processes[0].container_limit, None);
    let (_, week) = get(&app, "/api/v1/metrics/samples?range=7d&process=p2").await;
    let list: SampleSeriesList = serde_json::from_value(week).unwrap();
    assert_eq!(list.processes[0].container_limit, None);
}

#[tokio::test]
async fn a_window_the_leader_hasnt_rolled_up_is_read_from_its_minutes() {
    // Codex's second review, finding 4: rollups up to 10:00, a webserver's minute at 10:14
    // not rolled up yet (no leader), asked at 10:30:20.
    let db = seeded(&[
        rollup(minute("p1", "2026-11-14T09:45:00Z", 100, None), 15),
        minute("web", "2026-11-14T10:14:00Z", 50, None),
    ])
    .await;
    let (_, week) = get(&app(&db), "/api/v1/metrics/samples?range=7d").await;
    let list: SampleSeriesList = serde_json::from_value(week).unwrap();
    assert_eq!(
        list.processes
            .iter()
            .map(|p| p.process_id.as_str())
            .collect::<Vec<_>>(),
        ["p1", "web"]
    );
}

#[tokio::test]
async fn an_absurd_byte_count_is_capped_and_the_sums_hold() {
    // Codex's second review, finding 5.
    let db = seeded(&[
        Sample {
            memory_avg: u64::MAX,
            memory_peak: u64::MAX,
            ..rollup(minute("p1", "2026-11-13T09:45:00Z", u64::MAX, None), 15)
        },
        Sample {
            memory_avg: u64::MAX / 2,
            memory_peak: u64::MAX / 2,
            ..minute("p2", "2026-11-14T10:20:00Z", u64::MAX / 2, None)
        },
        Sample {
            memory_avg: u64::MAX / 2,
            memory_peak: u64::MAX / 2,
            ..minute("p3", "2026-11-14T10:20:00Z", u64::MAX / 2, None)
        },
    ])
    .await;
    let app = app(&db);
    for path in [
        "/api/v1/metrics/samples?range=7d",
        "/api/v1/metrics/memory?range=7d",
        "/api/v1/metrics/memory?range=1h",
    ] {
        let (status, body) = get(&app, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}

/// The clock of [`the_kept_buckets_answer_as_a_fresh_read_does`]: seconds.
static MOVING: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn moving() -> Timestamp {
    Timestamp::from_second(MOVING.load(std::sync::atomic::Ordering::Relaxed)).unwrap()
}

#[tokio::test]
async fn the_kept_buckets_answer_as_a_fresh_read_does() {
    // The buckets kept between requests (the user, 2026-11-15: metrics mustn't weigh on
    // schedr): over an hour of passes, each minute's samples stored as the sampler does (a
    // webserver's stored after the requests of its minute, each closed window rolled up 2 minutes after it closes), the
    // answers of one state asked each minute equal a fresh state's.
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let start = ts("2026-11-14T09:00:00Z").as_second();
    // Rollups and minutes before: a history to keep.
    let mut history = Vec::new();
    for window in 0..8 {
        let at = Timestamp::from_second(start - 7200 + 900 * window).unwrap();
        history.push(Sample {
            at,
            ..rollup(
                minute(
                    "p1",
                    "2026-11-14T00:00:00Z",
                    100 + window.cast_unsigned(),
                    Some(0.5),
                ),
                15,
            )
        });
    }
    db.insert_samples(&history).await.unwrap();
    let mut kept = ApiState::new(db.clone(), None);
    kept.clock = moving;
    let kept = crate::router(kept);
    let mut minutes = Vec::new();
    for m in 0..70_i64 {
        let now = start + 60 * m + 20;
        MOVING.store(now, std::sync::atomic::Ordering::Relaxed);
        let minute_at = |at: i64, process: &str, bytes: u64| Sample {
            at: Timestamp::from_second(at).unwrap(),
            parts: (process == "p1").then(|| parts(bytes / 2)),
            ..minute(process, "2026-11-14T00:00:00Z", bytes, Some(0.25))
        };
        let this = start + 60 * m;
        let mut new = vec![minute_at(this, "p1", 1000 + m.cast_unsigned())];
        // A second process for a while, replaced by a third.
        new.push(minute_at(this, if m < 30 { "p2" } else { "p3" }, 500));
        db.insert_samples(&new).await.unwrap();
        minutes.extend(new);
        // Each window rolled 2 minutes after it closes, as the sampler does.
        let closed = this - 120 - 900;
        if closed >= start && closed % 900 == 0 {
            let window: Vec<Sample> = minutes
                .iter()
                .filter(|s| s.at.as_second() >= closed && s.at.as_second() < closed + 900)
                .cloned()
                .collect();
            let mut window = window;
            window.sort_by(|a, b| a.process_id.cmp(&b.process_id).then(a.at.cmp(&b.at)));
            db.insert_samples(&rolled_up_for_test(&window, closed))
                .await
                .unwrap();
        }
        let mut fresh = ApiState::new(db.clone(), None);
        fresh.clock = moving;
        let fresh = crate::router(fresh);
        for range in Range::ALL {
            for path in [
                format!("/api/v1/metrics/samples?range={}", range.name()),
                format!("/api/v1/metrics/samples?range={}&process=p1", range.name()),
                format!("/api/v1/metrics/memory?range={}", range.name()),
            ] {
                let (_, cached) = get(&kept, &path).await;
                let (_, read) = get(&fresh, &path).await;
                assert_eq!(cached, read, "{path} at minute {m}");
            }
        }
        // The webserver's minute before, stored after this minute's requests: a minute late.
        let late = vec![minute_at(this - 60, "web", 300 + m.cast_unsigned())];
        db.insert_samples(&late).await.unwrap();
        minutes.extend(late);
    }
}

/// A window's rollup as the master's sampler writes it, simplified: one row per process, its
/// averages over its minutes (the test's values make them exact).
fn rolled_up_for_test(minutes: &[Sample], at: i64) -> Vec<Sample> {
    let covered = minutes
        .iter()
        .map(|s| s.at)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let mut rows: Vec<Sample> = Vec::new();
    for group in minutes.chunk_by(|a, b| a.process_id == b.process_id) {
        let n = u64::try_from(group.len()).unwrap();
        let avg = |f: fn(&Sample) -> u64| group.iter().map(f).sum::<u64>() / n;
        let parts = group[0].parts.as_ref().map(|_| WorkerParts {
            fork_servers_bytes: 1,
            watcher_hosts_bytes: 2,
            tasks_bytes: avg(|s| s.parts.as_ref().map_or(0, |p| p.tasks_bytes)),
            largest_tasks: Vec::new(),
        });
        let first = &group[0];
        rows.push(Sample {
            step_s: ROLLUP,
            at: Timestamp::from_second(at).unwrap(),
            resident_avg: avg(|s| s.resident_avg),
            resident_peak: group.iter().map(|s| s.resident_peak).max().unwrap(),
            memory_avg: avg(|s| s.memory_avg),
            memory_peak: group.iter().map(|s| s.memory_peak).max().unwrap(),
            minutes: u32::try_from(n).unwrap(),
            cpu_minutes: u32::try_from(n).unwrap(),
            container_minutes: 0,
            covered: u32::try_from(covered).unwrap(),
            cpu_seconds: None,
            parts,
            ..first.clone()
        });
    }
    rows
}
