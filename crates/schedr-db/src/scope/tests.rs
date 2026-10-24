use std::time::Duration;

use jiff::Timestamp;
use pretty_assertions::assert_eq;
use schedr_model::DagId;
use schedr_model::commands::Command;

use sqlx::Connection as _;

use super::*;

/// A migrated database in a file (several connections, so readers outside a transaction are
/// separate from it).
async fn file_db(dir: &tempfile::TempDir) -> Database {
    let url = format!("sqlite://{}", dir.path().join("scope.db").display());
    let db = Database::connect_with_timeout(&url, Duration::from_secs(2))
        .await
        .unwrap();
    db.migrate().await.unwrap();
    db
}

fn trigger() -> Command {
    Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    }
}

fn now() -> Timestamp {
    "2026-10-24T08:00:00Z".parse().unwrap()
}

#[tokio::test]
async fn work_that_succeeds_is_stored_at_once_and_reads_its_own_writes() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let seen = Box::pin(db.atomically(async {
        db.insert_command(&trigger(), "anna", now()).await?;
        db.insert_command(&trigger(), "anna", now()).await?;
        // Inside, the work's own writes are there (they go through its transaction).
        let inside = db.pending_commands().await?.len();
        // Outside (another task, another connection), not yet.
        let outside = tokio::spawn({
            let db = db.clone();
            async move { db.pending_commands().await.unwrap().len() }
        })
        .await
        .unwrap();
        Ok::<_, DbError>((inside, outside))
    }))
    .await
    .unwrap();
    assert_eq!(seen, (2, 0));
    assert_eq!(db.pending_commands().await.unwrap().len(), 2);
}

#[tokio::test]
async fn work_that_fails_stores_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let failed = Box::pin(db.atomically(async {
        db.insert_command(&trigger(), "anna", now()).await?;
        Err::<(), _>(DbError::NotLeading)
    }))
    .await;
    assert!(matches!(failed, Err(DbError::NotLeading)), "{failed:?}");
    assert_eq!(db.pending_commands().await.unwrap(), []);
    // The connection went back usable.
    db.insert_command(&trigger(), "anna", now()).await.unwrap();
    assert_eq!(db.pending_commands().await.unwrap().len(), 1);
}

#[tokio::test]
async fn work_inside_work_joins_its_transaction() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let failed = Box::pin(db.atomically(async {
        let inner = db.clone();
        Box::pin(inner.atomically(async {
            db.insert_command(&trigger(), "anna", now()).await?;
            Ok::<_, DbError>(())
        }))
        .await?;
        Err::<(), _>(DbError::NotLeading)
    }))
    .await;
    assert!(failed.is_err());
    // The inner work was part of the outer one: rolled back with it.
    assert_eq!(db.pending_commands().await.unwrap(), []);
}

#[tokio::test]
async fn a_masters_work_is_refused_once_its_lease_was_taken_over() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let period = Duration::from_millis(200);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    let master = db.fenced("a", 1);
    Box::pin(master.atomically(async {
        master.insert_command(&trigger(), "anna", now()).await?;
        Ok::<_, DbError>(())
    }))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    let refused = Box::pin(master.atomically(async {
        master.insert_command(&trigger(), "anna", now()).await?;
        Ok::<_, DbError>(())
    }))
    .await;
    assert!(matches!(refused, Err(DbError::NotLeading)), "{refused:?}");
    assert_eq!(db.pending_commands().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_write_that_failed_voids_the_work_even_if_its_error_was_handled() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let Pool::Sqlite(pool) = db.pool() else {
        unreachable!("SQLite here")
    };
    sqlx::query(
        "CREATE TRIGGER refuse_bob BEFORE INSERT ON commands WHEN NEW.requested_by = 'bob' \
         BEGIN SELECT RAISE(ABORT, 'refused by the test'); END",
    )
    .execute(pool)
    .await
    .unwrap();
    let done = Box::pin(db.atomically(async {
        db.insert_command(&trigger(), "anna", now()).await?;
        // Handled and gone on from: the transaction is void all the same (on Postgres it
        // is aborted, and its COMMIT would quietly roll back).
        let refused = db.insert_command(&trigger(), "bob", now()).await;
        assert!(refused.is_err());
        Ok::<_, DbError>(())
    }))
    .await;
    assert!(matches!(done, Err(DbError::TransactionFailed)), "{done:?}");
    assert_eq!(db.pending_commands().await.unwrap(), []);
}

#[tokio::test]
async fn a_refused_statement_is_the_datas_fault_and_a_busy_database_isnt() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = file_db(&dir).await;
    let Pool::Sqlite(pool) = db.pool() else {
        unreachable!("SQLite here")
    };
    let insert = "INSERT INTO workers (worker_id, incarnation_id, state, protocol_version, \
         schedr_version, queues, slots, log_endpoint, registered_at, last_heartbeat_at) \
         VALUES ('w1', 'i', 'active', '1', '0.1', '[]', 1, 'x', 'now', 'now')";
    sqlx::query(insert).execute(pool).await.unwrap();
    let duplicate = DbError::from(sqlx::query(insert).execute(pool).await.unwrap_err());
    assert!(duplicate.is_data_fault(), "{duplicate}");
    // Another connection holds the write lock; one that won't wait finds the database busy.
    // Connected patiently (opening a connection may find the file busy for a moment), then
    // made impatient for the write under test only.
    let url = format!("sqlite://{}", dir.path().join("scope.db").display());
    let mut impatient = sqlx::SqliteConnection::connect(&url).await.unwrap();
    sqlx::query("PRAGMA busy_timeout = 1")
        .execute(&mut impatient)
        .await
        .unwrap();
    let busy = Box::pin(db.atomically(async {
        db.insert_command(&trigger(), "anna", now()).await?;
        Ok::<_, DbError>(
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut impatient)
                .await
                .map_err(DbError::from),
        )
    }))
    .await
    .unwrap()
    .unwrap_err();
    assert!(!busy.is_data_fault(), "{busy}");
    assert!(!DbError::TransactionFailed.is_data_fault());
    assert!(!DbError::QueryTimeout(Duration::from_secs(1)).is_data_fault());
}
