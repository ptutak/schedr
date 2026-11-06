use pretty_assertions::assert_eq;

use super::*;

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn now() -> Timestamp {
    "2026-11-06T08:00:00Z".parse().unwrap()
}

const SENSOR: (&str, &str) = ("etl", "polls");

async fn strike(db: &Database, limit: u32) -> bool {
    db.strike_wait_step(SENSOR, limit, "code-1", now())
        .await
        .unwrap()
}

#[tokio::test]
async fn it_is_switched_off_at_its_limit_on_the_code_it_ran() {
    let db = migrated().await;
    assert!(!strike(&db, 3).await);
    assert!(!strike(&db, 3).await);
    assert_eq!(db.switched_off_wait_steps().await.unwrap(), vec![]);
    assert!(strike(&db, 3).await);
    let off = db.switched_off_wait_steps().await.unwrap();
    assert_eq!(off.len(), 1);
    assert_eq!(off[0].strikes, 3);
    assert_eq!(off[0].off_at, Some(now()));
    assert_eq!(off[0].off_code.as_deref(), Some("code-1"));
}

#[tokio::test]
async fn a_success_starts_the_count_again() {
    let db = migrated().await;
    assert!(!strike(&db, 2).await);
    assert!(db.clear_wait_step(SENSOR.0, SENSOR.1).await.unwrap());
    assert!(!db.clear_wait_step(SENSOR.0, SENSOR.1).await.unwrap());
    assert!(!strike(&db, 2).await);
}

#[tokio::test]
async fn new_code_gets_one_chance_and_a_success_gives_the_full_count_back() {
    let db = migrated().await;
    assert!(strike(&db, 1).await);
    db.wait_step_on_new_code(SENSOR.0, SENSOR.1).await.unwrap();
    let on = db
        .wait_step_strikes(SENSOR.0, SENSOR.1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((on.off_at, on.one_chance), (None, true));
    // Far from its limit, off at once.
    assert!(strike(&db, 3).await);
    db.wait_step_on_new_code(SENSOR.0, SENSOR.1).await.unwrap();
    db.clear_wait_step(SENSOR.0, SENSOR.1).await.unwrap();
    assert!(!strike(&db, 3).await);
}

#[tokio::test]
async fn a_limit_of_0_never_switches_off() {
    let db = migrated().await;
    for _ in 0..5 {
        assert!(!strike(&db, 0).await);
    }
    assert_eq!(db.switched_off_wait_steps().await.unwrap(), vec![]);
}
