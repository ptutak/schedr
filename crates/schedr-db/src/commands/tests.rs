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
    assert_eq!(pending[0].requested_at, ts("2026-10-21T08:00:00Z"));
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
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
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
    // Damaged outside schedr: the index over `result` (evaluated on every write) refuses
    // anything but JSON, so it goes first.
    sqlx::query("DROP INDEX commands_by_result_run")
        .execute(pool)
        .await
        .unwrap();
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

#[tokio::test]
async fn sqlite_has_no_command_notifications() {
    let db = migrated().await;
    assert!(db.command_notifications().await.unwrap().is_none());
    assert_eq!(COMMANDS_CHANNEL, "schedr_commands");
}

#[tokio::test]
async fn a_command_marked_applying_reads_so_until_decided() {
    let db = migrated().await;
    let id = db
        .insert_command(&pause(), "anna", ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap();
    assert!(!db.pending_commands().await.unwrap()[0].applying);
    db.mark_applying(&id, ts("2026-10-21T08:00:01Z"))
        .await
        .unwrap();
    assert!(db.pending_commands().await.unwrap()[0].applying);
    db.decide_command(
        &id,
        CommandStatus::Applied,
        None,
        None,
        ts("2026-10-21T08:00:02Z"),
    )
    .await
    .unwrap();
    // A decided command isn't marked again.
    db.mark_applying(&id, ts("2026-10-21T08:00:03Z"))
        .await
        .unwrap();
    assert_eq!(db.pending_commands().await.unwrap(), []);
}

#[tokio::test]
async fn a_runs_commands_are_those_naming_it_and_its_trigger() {
    let db = migrated().await;
    let run = "0190a3e5-0000-7000-8000-000000000001";
    let other = "0190a3e5-0000-7000-8000-000000000002";
    let trigger = db
        .insert_command(
            &Command::TriggerRun {
                dag_id: DagId::new("sync").unwrap(),
                planned_for: None,
            },
            "anna",
            ts("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    db.decide_command(
        &trigger,
        CommandStatus::Applied,
        None,
        Some(&json!({"run_id": run})),
        ts("2026-10-21T08:00:01Z"),
    )
    .await
    .unwrap();
    let retry = db
        .insert_command(
            &Command::RetryTask {
                run_id: run.to_owned(),
                task_id: schedr_model::TaskId::new("extract").unwrap(),
            },
            "piotr",
            ts("2026-10-21T08:00:03Z"),
        )
        .await
        .unwrap();
    let cancel = db
        .insert_command(
            &Command::CancelRun {
                run_id: run.to_owned(),
            },
            "piotr",
            ts("2026-10-21T08:00:02Z"),
        )
        .await
        .unwrap();
    db.insert_command(
        &Command::CancelRun {
            run_id: other.to_owned(),
        },
        "piotr",
        ts("2026-10-21T08:00:02Z"),
    )
    .await
    .unwrap();
    db.insert_command(&pause(), "anna", ts("2026-10-21T08:00:02Z"))
        .await
        .unwrap();
    // A pause that cancelled it (and another run) lists it in its result.
    let paused = db
        .insert_command(
            &Command::PauseDag {
                dag_id: DagId::new("sync").unwrap(),
                cancel_runs: true,
            },
            "anna",
            ts("2026-10-21T08:00:04Z"),
        )
        .await
        .unwrap();
    db.decide_command(
        &paused,
        CommandStatus::Applied,
        None,
        Some(&json!({"cancelled_runs": [other, run]})),
        ts("2026-10-21T08:00:05Z"),
    )
    .await
    .unwrap();

    let commands = db.run_commands(run).await.unwrap();
    let ids: Vec<&str> = commands.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            trigger.as_str(),
            cancel.as_str(),
            retry.as_str(),
            paused.as_str()
        ]
    );
    assert_eq!(commands[0].result, Some(json!({"run_id": run})));
    assert_eq!(commands[0].status, CommandStatus::Applied);
    assert_eq!(db.run_commands(other).await.unwrap().len(), 2);
    assert_eq!(
        db.run_commands("0190a3e5-0000-7000-8000-000000000003")
            .await
            .unwrap(),
        []
    );
}
