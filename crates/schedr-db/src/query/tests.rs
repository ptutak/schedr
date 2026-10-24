use pretty_assertions::assert_eq;

use super::*;
use crate::database::Database;

#[test]
fn times_are_stored_in_utc_with_six_fraction_digits() {
    let whole: Timestamp = "2026-10-21T10:00:00+02:00".parse().unwrap();
    assert_eq!(db_time(whole), "2026-10-21T08:00:00.000000Z");
    let fraction: Timestamp = "2026-10-21T08:00:00.25Z".parse().unwrap();
    assert_eq!(db_time(fraction), "2026-10-21T08:00:00.250000Z");
    assert!(
        db_time(fraction) > db_time(whole),
        "the text sorts in time order"
    );
}

/// A write that reads first (as a master's does: its fence), with another connection's write
/// committed between its read and its own write.
async fn read_then_write(db: &Database, other: &Database) -> Result<(), DbError> {
    in_tx!(db, |tx, _backend| {
        let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workers")
            .fetch_one(&mut *tx)
            .await?;
        let other = other.clone();
        let racing = tokio::spawn(async move {
            sqlx::query(
                "INSERT INTO commands (id, kind, payload, requested_by, requested_at) \
                 VALUES ('c2', 'k', '{}', 'bob', '2026-10-24T08:00:00Z')",
            )
            .execute(match other.pool() {
                crate::database::Pool::Sqlite(pool) => pool,
                crate::database::Pool::Postgres(_) => unreachable!("SQLite here"),
            })
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        sqlx::query(
            "INSERT INTO commands (id, kind, payload, requested_by, requested_at) \
             VALUES ('c1', 'k', '{}', 'anna', '2026-10-24T08:00:00Z')",
        )
        .execute(&mut *tx)
        .await?;
        racing
    })
    .await
    .unwrap()?;
    Ok(())
}

#[tokio::test]
async fn an_sqlite_write_waits_for_another_writer_instead_of_failing() {
    // B14: a write transaction began deferred (`BEGIN`) and read first; another connection's
    // write in between made its own write fail at once with "database is locked" (SQLite
    // doesn't wait there), which standalone's roles, writing to one file, hit under load.
    let dir = std::env::temp_dir().join(format!("schedr-b14-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite://{}", dir.join("b14.db").display());
    let limit = std::time::Duration::from_secs(5);
    let db = Database::connect_with_timeout(&url, limit).await.unwrap();
    db.migrate().await.unwrap();
    let other = Database::connect_with_timeout(&url, limit).await.unwrap();
    let written = read_then_write(&db, &other).await;
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(written.is_ok(), "{written:?}");
}
