use pretty_assertions::assert_eq;
use schedr_model::DagId;
use serde_json::json;
use sqlx::AssertSqlSafe;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

async fn migrated() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

fn pause() -> Command {
    Command::PauseDag {
        dag_id: DagId::new("sync").unwrap(),
        cancel_runs: false,
    }
}

#[tokio::test]
async fn commands_wait_as_pending_oldest_first() {
    let db = migrated().await;
    let second = db
        .insert_command(&pause(), "anna", ts("2026-10-21T08:00:01Z"))
        .await
        .unwrap();
    let first = db
        .insert_command(
            &Command::CancelRun {
                run_id: "r1".to_owned(),
            },
            "piotr",
            ts("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    let pending = db.pending_commands().await.unwrap();
    let ids: Vec<&str> = pending.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids, [first.as_str(), second.as_str()]);
    assert_eq!(pending[1].command, Ok(pause()));
    assert_eq!(pending[1].requested_by, "anna");
    let record = db.command(&second).await.unwrap().unwrap();
    assert_eq!(
        (record.kind.as_str(), record.status, record.decided_at),
        ("pause_dag", CommandStatus::Pending, None)
    );
    assert_eq!(record.requested_at, ts("2026-10-21T08:00:01Z"));
}

#[tokio::test]
async fn a_command_is_decided_once() {
    let db = migrated().await;
    let id = db
        .insert_command(&pause(), "anna", ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    let result = json!({"run_id": "r1"});
    let now = ts("2026-10-21T08:00:02Z");
    assert!(
        db.decide_command(&id, CommandStatus::Applied, None, Some(&result), now)
            .await
            .unwrap()
    );
    assert!(
        !db.decide_command(&id, CommandStatus::Rejected, Some("late"), None, now)
            .await
            .unwrap()
    );
    let record = db.command(&id).await.unwrap().unwrap();
    assert_eq!(record.status, CommandStatus::Applied);
    assert_eq!(record.result, Some(result));
    assert_eq!(record.reason, None);
    assert_eq!(record.decided_at, Some(now));
    assert_eq!(db.pending_commands().await.unwrap(), []);
    assert_eq!(
        db.command("0190a3e5-0000-7000-8000-000000000000")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn payloads_that_dont_read_are_reported_for_rejection() {
    let db = migrated().await;
    let id = db
        .insert_command(&pause(), "anna", ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    let Database::Sqlite(pool) = &db else {
        panic!("expected SQLite")
    };
    sqlx::query(AssertSqlSafe(
        "UPDATE commands SET payload = '{\"kind\": \"drop_everything\"}'".to_owned(),
    ))
    .execute(pool)
    .await
    .unwrap();
    let pending = db.pending_commands().await.unwrap();
    assert_eq!(pending[0].id, id);
    assert!(pending[0].command.is_err());
    sqlx::query(AssertSqlSafe(
        "UPDATE commands SET status = 'applied', result = 'not json'".to_owned(),
    ))
    .execute(pool)
    .await
    .unwrap();
    let error = db.command(&id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in commands.result: "),
        "{error}"
    );
}
