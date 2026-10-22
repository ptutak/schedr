use pretty_assertions::assert_eq;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn registration<'a>(
    id: &'a str,
    incarnation: &'a str,
    queues: &'a [String],
) -> WorkerRegistration<'a> {
    WorkerRegistration {
        id,
        incarnation,
        protocol_version: "1.0",
        schedr_version: "0.1.0",
        queues,
        slots: 4,
        log_endpoint: "",
    }
}

#[tokio::test]
async fn a_registration_is_recorded_and_listed() {
    let db = migrated().await;
    let queues = vec!["large".to_owned()];
    db.register_worker(
        &registration("w1", "0190a0a0-0000-7000-8000-000000000001", &queues),
        ts("2026-10-22T08:00:00Z"),
    )
    .await
    .unwrap();
    let workers = db.workers().await.unwrap();
    assert_eq!(
        workers,
        [WorkerRecord {
            id: "w1".to_owned(),
            incarnation: "0190a0a0-0000-7000-8000-000000000001".to_owned(),
            state: WorkerState::Active,
            protocol_version: "1.0".to_owned(),
            schedr_version: "0.1.0".to_owned(),
            queues,
            slots: 4,
            log_endpoint: String::new(),
            registered_at: ts("2026-10-22T08:00:00Z"),
            last_heartbeat_at: ts("2026-10-22T08:00:00Z"),
            offline_at: None,
            running: 0,
            drained: false,
        }]
    );
}

#[tokio::test]
async fn heartbeats_states_and_a_new_incarnation_are_recorded() {
    let db = migrated().await;
    db.register_worker(
        &registration("w1", "0190a0a0-0000-7000-8000-000000000001", &[]),
        ts("2026-10-22T08:00:00Z"),
    )
    .await
    .unwrap();
    db.worker_seen("w1", ts("2026-10-22T08:00:10Z"))
        .await
        .unwrap();
    db.set_worker_state("w1", WorkerState::Disconnected, ts("2026-10-22T08:00:20Z"))
        .await
        .unwrap();
    let worker = &db.workers().await.unwrap()[0];
    assert_eq!(worker.last_heartbeat_at, ts("2026-10-22T08:00:10Z"));
    assert_eq!(
        (worker.state, worker.offline_at),
        (WorkerState::Disconnected, None)
    );

    db.set_worker_state("w1", WorkerState::Lost, ts("2026-10-22T08:01:30Z"))
        .await
        .unwrap();
    let worker = &db.workers().await.unwrap()[0];
    assert_eq!(
        (worker.state, worker.offline_at),
        (WorkerState::Lost, Some(ts("2026-10-22T08:01:30Z")))
    );

    db.register_worker(
        &registration("w1", "0190a0a0-0000-7000-8000-000000000002", &[]),
        ts("2026-10-22T08:05:00Z"),
    )
    .await
    .unwrap();
    let worker = &db.workers().await.unwrap()[0];
    assert_eq!(worker.incarnation, "0190a0a0-0000-7000-8000-000000000002");
    assert_eq!(
        (worker.state, worker.offline_at),
        (WorkerState::Active, None)
    );
    assert_eq!(worker.registered_at, ts("2026-10-22T08:05:00Z"));
}

#[tokio::test]
async fn queues_that_dont_read_are_reported() {
    let db = migrated().await;
    db.register_worker(
        &registration("w1", "0190a0a0-0000-7000-8000-000000000001", &[]),
        ts("2026-10-22T08:00:00Z"),
    )
    .await
    .unwrap();
    let Database::Sqlite(pool) = &db else {
        unreachable!()
    };
    sqlx::query("UPDATE workers SET queues = 'not json'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.workers().await.unwrap_err();
    assert!(error.to_string().contains("workers.queues"), "{error}");
}

#[tokio::test]
async fn a_drain_is_kept_until_resumed_even_across_registrations() {
    let db = migrated().await;
    assert!(
        !db.set_worker_drained("w1", true).await.unwrap(),
        "unknown worker"
    );
    assert!(!db.worker_drained("w1").await.unwrap());
    db.register_worker(
        &registration("w1", "0190a0a0-0000-7000-8000-000000000001", &[]),
        ts("2026-10-22T08:00:00Z"),
    )
    .await
    .unwrap();
    assert!(db.set_worker_drained("w1", true).await.unwrap());
    db.register_worker(
        &registration("w1", "0190a0a0-0000-7000-8000-000000000002", &[]),
        ts("2026-10-22T08:05:00Z"),
    )
    .await
    .unwrap();
    assert!(db.worker_drained("w1").await.unwrap());
    assert!(db.workers().await.unwrap()[0].drained);
    assert_eq!(db.resume_worker("w1").await.unwrap(), Some(true));
    assert!(!db.worker_drained("w1").await.unwrap());
    assert_eq!(db.resume_worker("w1").await.unwrap(), Some(false));
    assert_eq!(db.resume_worker("w9").await.unwrap(), None);
    db.set_worker_drained("w1", false).await.unwrap();
}
