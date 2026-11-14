use std::collections::BTreeMap;

use pretty_assertions::assert_eq;

use super::*;
use crate::nodes::MasterNode;
use crate::workers::WorkerRegistration;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn minute(process: &str, at: &str, resident: u64, cpu: Option<f64>) -> Sample {
    Sample {
        process_id: process.to_owned(),
        step_s: MINUTE,
        at: ts(at),
        roles: BTreeMap::from([("master".to_owned(), "m-a".to_owned())]),
        resident_avg: resident,
        resident_peak: resident,
        cpu_seconds: Some(10.0),
        cpu_avg: cpu,
        cpu_peak: cpu,
        container_avg: Some(resident * 2),
        container_peak: Some(resident * 2),
        container_limit: Some(1 << 30),
        parts: None,
    }
}

#[tokio::test]
async fn samples_are_stored_once_read_by_step_and_deleted_by_age() {
    // Decision 464.
    let db = migrated().await;
    let parts = WorkerParts {
        fork_servers_bytes: 1,
        watcher_hosts_bytes: 2,
        tasks_bytes: 3,
        largest_tasks: Vec::new(),
    };
    let samples = vec![
        minute("p1", "2026-11-14T10:00:00Z", 100, None),
        minute("p1", "2026-11-14T10:01:00Z", 300, Some(0.5)),
        Sample {
            parts: Some(parts.clone()),
            ..minute("p1", "2026-11-14T10:14:00Z", 200, Some(1.5))
        },
        // The next window's: not in the first rollup.
        minute("p1", "2026-11-14T10:15:00Z", 900, Some(4.0)),
        minute("p2", "2026-11-14T10:05:00Z", 50, None),
    ];
    db.insert_samples(&samples).await.unwrap();
    // The same minute again: the first one kept.
    db.insert_samples(&[minute("p1", "2026-11-14T10:00:00Z", 999, None)])
        .await
        .unwrap();
    let all = |step| db.samples(step, ts("2026-11-14T00:00:00Z"), ts("2026-11-15T00:00:00Z"));
    let stored = all(MINUTE).await.unwrap();
    assert_eq!(stored.len(), 5);
    assert_eq!(stored[0], samples[0]);
    assert_eq!(stored[2].parts, Some(parts));

    assert_eq!(
        db.oldest_minute_sample().await.unwrap(),
        Some(ts("2026-11-14T10:00:00Z"))
    );
    // A rollup row (the master's sampler makes them) is kept apart from the minutes.
    db.insert_samples(&[Sample {
        step_s: ROLLUP,
        ..minute("p1", "2026-11-14T10:00:00Z", 200, Some(1.0))
    }])
    .await
    .unwrap();

    assert_eq!(
        db.delete_old_samples(MINUTE, ts("2026-11-14T10:10:00Z"))
            .await
            .unwrap(),
        3
    );
    assert_eq!(all(MINUTE).await.unwrap().len(), 2);
    assert_eq!(all(ROLLUP).await.unwrap().len(), 1, "rollups kept apart");
    db.delete_old_samples(MINUTE, ts("2026-11-15T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(db.oldest_minute_sample().await.unwrap(), None);
}

#[tokio::test]
async fn the_sources_are_the_masters_heard_and_the_live_workers_with_their_usage() {
    let db = migrated().await;
    let usage = NodeUsage {
        resident_bytes: 10,
        cpu_seconds: 1.0,
        container_bytes: None,
        container_limit_bytes: None,
    };
    let config = serde_json::json!({});
    for (id, heard, with) in [
        ("m-a", "2026-11-14T10:00:00Z", true),
        ("m-old", "2026-11-14T09:00:00Z", true),
        ("m-none", "2026-11-14T10:00:00Z", false),
    ] {
        db.heartbeat_master(
            &MasterNode {
                master_id: id,
                process_id: "p1",
                host: "h",
                schedr_version: "1.1.0",
                config: &config,
                started_at: ts("2026-11-14T08:00:00Z"),
                problems: &[],
                usage: with.then_some(&usage),
            },
            ts(heard),
        )
        .await
        .unwrap();
    }
    let queues = vec!["default".to_owned()];
    for id in ["w1", "w2"] {
        db.register_worker(
            &WorkerRegistration {
                id,
                incarnation: "0190a0a0-0000-7000-8000-000000000001",
                protocol_version: "2.1",
                schedr_version: "1.1.0",
                queues: &queues,
                slots: 2,
                watcher_slots: 0,
                log_endpoint: "",
            },
            ts("2026-11-14T10:00:00Z"),
        )
        .await
        .unwrap();
        db.set_worker_node(id, Some("m-a"), Some("p1"), None)
            .await
            .unwrap();
        db.set_worker_usage(
            id,
            &WorkerUsage {
                process: usage.clone(),
                fork_servers_bytes: 1,
                watcher_hosts_bytes: 2,
                tasks_bytes: 3,
                largest_tasks: Vec::new(),
            },
            ts("2026-11-14T10:00:00Z"),
        )
        .await
        .unwrap();
    }
    db.set_worker_state(
        "w2",
        schedr_model::WorkerState::Lost,
        ts("2026-11-14T10:00:00Z"),
    )
    .await
    .unwrap();
    let sources = db
        .sample_sources(ts("2026-11-14T09:59:30Z"), ts("2026-11-14T09:58:00Z"))
        .await
        .unwrap();
    assert_eq!(
        sources
            .iter()
            .map(|s| (s.role, s.node_id.as_str(), s.parts.is_some(), s.at))
            .collect::<Vec<_>>(),
        [
            ("master", "m-a", false, ts("2026-11-14T10:00:00Z")),
            ("worker", "w1", true, ts("2026-11-14T10:00:00Z"))
        ],
        "the master heard long ago, the one without usage and the lost worker left out"
    );
}
