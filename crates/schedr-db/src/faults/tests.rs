use pretty_assertions::assert_eq;

use crate::database::{Database, DbError};

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

/// One write: a worker registered (its own transaction).
async fn register(db: &Database, id: &str) -> Result<(), DbError> {
    let registration = crate::workers::WorkerRegistration {
        id,
        incarnation: "0190a3e5-0000-7000-8000-000000000001",
        protocol_version: "1.0",
        schedr_version: "0.1.0",
        queues: &[],
        slots: 1,
        log_endpoint: "",
    };
    db.register_worker(&registration, "2026-10-30T12:00:00Z".parse().unwrap())
        .await
}

#[tokio::test]
async fn writes_are_counted_and_the_chosen_one_fails_like_a_database_error() {
    let db = migrated().await;
    db.count_writes();
    register(&db, "w1").await.unwrap();
    register(&db, "w2").await.unwrap();
    assert_eq!(db.writes(), 2);
    db.fail_write(2);
    register(&db, "w3").await.unwrap();
    let failed = register(&db, "w4").await.unwrap_err();
    assert!(
        failed.to_string().contains("write 2 failed on purpose"),
        "{failed}"
    );
    register(&db, "w5").await.unwrap();
    let ids: Vec<String> = db
        .workers()
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.id)
        .collect();
    assert_eq!(ids, ["w1", "w2", "w3", "w5"]);
}

#[tokio::test]
async fn a_failed_write_inside_a_transaction_rolls_back_the_writes_before_it() {
    let db = migrated().await;
    db.fail_write(2);
    let both = Box::pin(db.atomically(async {
        register(&db, "w1").await?;
        register(&db, "w2").await
    }))
    .await;
    assert!(both.is_err());
    assert!(db.workers().await.unwrap().is_empty(), "w1 rolled back too");
    // Its error handled by the caller, the transaction still can't commit: as for a real one.
    db.fail_write(1);
    let handled = Box::pin(db.atomically(async {
        let _ = register(&db, "w1").await;
        register(&db, "w2").await
    }))
    .await;
    assert!(
        matches!(handled, Err(DbError::TransactionFailed)),
        "{handled:?}"
    );
}
