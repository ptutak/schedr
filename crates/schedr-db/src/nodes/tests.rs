use std::time::Duration;

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;
use crate::workers::WorkerRegistration;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn master<'a>(id: &'a str, config: &'a serde_json::Value) -> MasterNode<'a> {
    MasterNode {
        master_id: id,
        process_id: "p1",
        host: "node-a",
        schedr_version: "0.1.0",
        config,
        started_at: ts("2026-11-01T08:00:00Z"),
        problems: &[],
    }
}

#[tokio::test]
async fn masters_heartbeat_their_rows_and_the_leader_is_the_lease_holder() {
    let db = migrated().await;
    let config = json!({"database": {"url": "postgres://schedr:****@db/s"}});
    db.heartbeat_master(&master("m-b", &config), ts("2026-11-01T08:00:05Z"))
        .await
        .unwrap();
    db.heartbeat_master(&master("m-a", &config), ts("2026-11-01T08:00:05Z"))
        .await
        .unwrap();
    // A later heartbeat brings the row up to date, keeping when it started.
    let newer = json!({"database": {"url": "postgres://schedr:****@db2/s"}});
    // With its problems now (decision 129).
    let problems = [NodeProblem {
        error: false,
        message: "its lease isn't renewed in time".to_owned(),
    }];
    let again = MasterNode {
        started_at: ts("2026-11-01T09:00:00Z"),
        problems: &problems,
        ..master("m-b", &newer)
    };
    db.heartbeat_master(&again, ts("2026-11-01T08:00:10Z"))
        .await
        .unwrap();

    let since = ts("2026-11-01T08:00:00Z");
    let read = db.masters(since).await.unwrap();
    assert_eq!(
        read.iter()
            .map(|m| (m.master_id.as_str(), m.leading))
            .collect::<Vec<_>>(),
        vec![("m-a", false), ("m-b", false)],
        "no lease taken: no leader"
    );
    let b = &read[1];
    assert_eq!(b.config, newer);
    assert_eq!(b.started_at, ts("2026-11-01T08:00:00Z"));
    assert_eq!(b.heard_at, ts("2026-11-01T08:00:10Z"));
    assert_eq!(b.problems, problems);
    assert_eq!(read[0].problems, []);

    db.take_lease("m-b", Duration::from_secs(30)).await.unwrap();
    let read = db.masters(since).await.unwrap();
    assert_eq!(
        read.iter()
            .map(|m| (m.master_id.as_str(), m.leading))
            .collect::<Vec<_>>(),
        vec![("m-b", true), ("m-a", false)],
        "the leader first"
    );
    db.release_lease("m-b").await.unwrap();
    assert!(db.masters(since).await.unwrap().iter().all(|m| !m.leading));
}

#[tokio::test]
async fn masters_not_heard_from_lately_are_left_out_and_long_gone_ones_removed() {
    let db = migrated().await;
    let config = json!({});
    db.heartbeat_master(&master("old", &config), ts("2026-11-01T06:00:00Z"))
        .await
        .unwrap();
    db.heartbeat_master(&master("quiet", &config), ts("2026-11-01T07:59:00Z"))
        .await
        .unwrap();
    db.heartbeat_master(&master("now", &config), ts("2026-11-01T08:00:00Z"))
        .await
        .unwrap();

    let ids =
        |masters: Vec<MasterRecord>| masters.into_iter().map(|m| m.master_id).collect::<Vec<_>>();
    assert_eq!(
        ids(db.masters(ts("2026-11-01T07:59:30Z")).await.unwrap()),
        vec!["now"]
    );
    assert_eq!(
        ids(db.masters(Timestamp::UNIX_EPOCH).await.unwrap()),
        vec!["now", "quiet"],
        "one not heard from for {MASTER_ROW_KEPT:?} is removed at the next heartbeat"
    );
}

#[tokio::test]
async fn a_workers_node_is_recorded_after_its_registration() {
    let db = migrated().await;
    let queues = vec!["default".to_owned()];
    for id in ["w1", "w2"] {
        db.register_worker(
            &WorkerRegistration {
                id,
                incarnation: "0190a0a0-0000-7000-8000-000000000001",
                protocol_version: "1.0",
                schedr_version: "0.1.0",
                queues: &queues,
                slots: 2,
                watcher_slots: 0,
                log_endpoint: "",
            },
            ts("2026-11-01T08:00:00Z"),
        )
        .await
        .unwrap();
    }
    let config = json!({"worker": {"slots": 2}});
    db.set_worker_node("w1", Some("m-a"), Some("p1"), Some(&config))
        .await
        .unwrap();
    assert_eq!(
        db.worker_nodes().await.unwrap(),
        vec![
            WorkerNode {
                worker_id: "w1".to_owned(),
                master_id: Some("m-a".to_owned()),
                process_id: Some("p1".to_owned()),
                config: Some(config),
            },
            WorkerNode {
                worker_id: "w2".to_owned(),
                master_id: None,
                process_id: None,
                config: None,
            },
        ]
    );
}
