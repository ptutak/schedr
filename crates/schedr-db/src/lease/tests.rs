use std::time::Duration;

use jiff::Timestamp;
use pretty_assertions::assert_eq;
use schedr_model::DagId;
use schedr_model::commands::Command;

use super::*;

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

#[tokio::test]
async fn one_holder_at_a_time_until_its_lease_expires_or_is_released() {
    let db = migrated().await;
    let period = Duration::from_millis(300);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    // Renewing keeps the counter; another master is refused while it lasts.
    assert!(db.renew_lease("a", 1, period).await.unwrap());
    assert_eq!(db.take_lease("b", period).await.unwrap(), None);
    tokio::time::sleep(Duration::from_millis(400)).await;
    // Expired: taken over, the counter moves on.
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    assert_eq!(db.take_lease("a", period).await.unwrap(), None);
    // Given up: taken at once.
    db.release_lease("b").await.unwrap();
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(3));
    // Releasing someone else's lease does nothing.
    db.release_lease("b").await.unwrap();
    assert_eq!(db.take_lease("b", period).await.unwrap(), None);
}

#[tokio::test]
async fn a_renewal_extends_only_the_holders_own_lease_never_taking_it_back() {
    let db = migrated().await;
    let period = Duration::from_millis(300);
    assert_eq!(db.take_lease("a", period).await.unwrap(), Some(1));
    tokio::time::sleep(Duration::from_millis(400)).await;
    // Expired but not taken over: still a's to renew.
    assert!(db.renew_lease("a", 1, period).await.unwrap());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    assert!(!db.renew_lease("a", 1, period).await.unwrap());
    // Once b's expires too, a's renewal still leaves it: a third master takes it at once.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!db.renew_lease("a", 1, period).await.unwrap());
    assert_eq!(db.take_lease("c", period).await.unwrap(), Some(3));
}

#[tokio::test]
async fn a_masters_write_is_refused_once_its_lease_was_taken_over() {
    let db = migrated().await;
    let period = Duration::from_millis(300);
    let epoch = db.take_lease("a", period).await.unwrap().unwrap();
    let master = db.fenced("a", epoch);
    let command = Command::UnpauseDag {
        dag_id: DagId::new("etl").unwrap(),
    };
    let now: Timestamp = "2026-10-23T08:00:00Z".parse().unwrap();
    master.insert_command(&command, "test", now).await.unwrap();
    // Expired but not taken over: no other master wrote, so a's writes still go in.
    tokio::time::sleep(Duration::from_millis(400)).await;
    master.insert_command(&command, "test", now).await.unwrap();
    assert_eq!(db.take_lease("b", period).await.unwrap(), Some(2));
    let refused = master.insert_command(&command, "test", now).await;
    assert!(matches!(refused, Err(DbError::NotLeading)), "{refused:?}");
    assert_eq!(db.pending_commands().await.unwrap().len(), 2);
    // Reads aren't fenced, nor writes through a database that isn't a master's.
    assert_eq!(master.pending_commands().await.unwrap().len(), 2);
    db.insert_command(&command, "test", now).await.unwrap();
}

#[tokio::test]
async fn the_table_is_created_before_and_by_the_migrations_alike() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.create_lease_table().await.unwrap();
    assert_eq!(
        db.take_lease("a", Duration::from_secs(5)).await.unwrap(),
        Some(1)
    );
    // The migrations then find it there.
    db.migrate().await.unwrap();
    db.create_lease_table().await.unwrap();
    assert_eq!(
        db.take_lease("a", Duration::from_secs(5)).await.unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn the_lease_holder_migrates_without_the_migrators_lock() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.create_lease_table().await.unwrap();
    db.migrate_as_lease_holder().await.unwrap();
    assert_eq!(
        db.applied_migrations().await.unwrap(),
        db.backend().migration_versions()
    );
}

#[test]
fn a_literal_reads_back_as_the_text_whatever_it_holds() {
    assert_eq!(literal("a"), "E'a'");
    assert_eq!(literal(r"it's \ $x$"), r"E'it\'s \\ $x$'");
}

#[tokio::test]
async fn a_writes_start_on_postgres_limits_it_and_fences_a_masters() {
    let db = Database::connect_with_timeout("sqlite::memory:", Duration::from_secs(10))
        .await
        .unwrap();
    let plain = db.begin_postgres().0;
    assert_eq!(
        plain,
        "BEGIN; SELECT set_config('statement_timeout', E'10000ms', true);"
    );
    let fenced = db.fenced("a'b", 3).begin_postgres().0;
    assert!(fenced.starts_with(&plain), "{fenced}");
    assert!(
        fenced.contains("set_config('schedr.holder', E'a\\'b', true)"),
        "{fenced}"
    );
    assert!(
        fenced.contains("set_config('schedr.epoch', E'3', true)"),
        "{fenced}"
    );
    assert!(fenced.ends_with(FENCE_POSTGRES), "{fenced}");
}
