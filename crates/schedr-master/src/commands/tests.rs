use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_model::{Dag, RunState};
use schedr_proto::pb;
use serde_json::Value as Json;
use sqlx::AssertSqlSafe;

use super::*;
use crate::dispatch::DEFAULT_CANCEL_GRACE;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

fn dag() -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "schedule": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC"},
        "start": "2026-10-01T00:00:00Z", "end": null, "initial_since": null,
        "catchup": false, "max_active_runs": 1,
        "tasks": [{
            "id": "extract", "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }],
        "edges": []
    }))
    .unwrap()
}

struct World {
    db: Database,
    commander: Commander,
    scheduler: Scheduler,
    dispatcher: Dispatcher,
}

/// A database with DAG `sync` (paused, as new DAGs are) and worker `w1`.
async fn world() -> World {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag()),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts(NOW)).await.unwrap();
    let mut dispatcher = Dispatcher::new(db.clone(), DEFAULT_CANCEL_GRACE);
    dispatcher
        .register("w1", "i1", &[], 2, ts(NOW))
        .await
        .unwrap();
    World {
        commander: Commander::new(db.clone()),
        scheduler: Scheduler::new(db.clone()),
        dispatcher,
        db,
    }
}

impl World {
    async fn ask(&self, command: &Command) -> String {
        self.db
            .insert_command(command, "anna", ts(NOW))
            .await
            .unwrap()
    }

    async fn apply(&mut self) -> Applied {
        self.commander
            .apply_pending(&self.scheduler, &mut self.dispatcher, ts(NOW))
            .await
            .unwrap()
    }

    async fn paused(&self) -> bool {
        self.db.stored_dags().await.unwrap()[0].paused
    }
}

fn rejected(reason: &str) -> (CommandStatus, Option<String>, Option<Json>) {
    (CommandStatus::Rejected, Some(reason.to_owned()), None)
}

fn summary(decided: &Decided) -> (CommandStatus, Option<String>, Option<Json>) {
    (
        decided.status,
        decided.reason.clone(),
        decided.result.clone(),
    )
}

#[tokio::test]
async fn pausing_and_unpausing_change_the_dag_and_are_recorded() {
    let mut world = world().await;
    let id = world.ask(&Command::UnpauseDag { dag_id: sync() }).await;
    let applied = world.apply().await;
    assert_eq!(applied.decided[0].id, id);
    assert_eq!(
        summary(&applied.decided[0]),
        (CommandStatus::Applied, None, Some(json!({})))
    );
    assert!(!world.paused().await);
    let record = world.db.command(&id).await.unwrap().unwrap();
    assert_eq!(record.status, CommandStatus::Applied);

    world
        .ask(&Command::PauseDag {
            dag_id: sync(),
            cancel_runs: false,
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(
        applied.decided[0].result,
        Some(json!({"cancelled_runs": []}))
    );
    assert!(world.paused().await);
    assert_eq!(world.apply().await.decided, []);
}

#[tokio::test]
async fn commands_about_unknown_dags_and_runs_are_rejected() {
    let mut world = world().await;
    let ghost = DagId::new("ghost").unwrap();
    world
        .ask(&Command::UnpauseDag {
            dag_id: ghost.clone(),
        })
        .await;
    world
        .ask(&Command::PauseDag {
            dag_id: ghost.clone(),
            cancel_runs: true,
        })
        .await;
    world
        .ask(&Command::CancelRun {
            run_id: "0190a3e5-0000-7000-8000-000000000000".to_owned(),
        })
        .await;
    world
        .ask(&Command::TriggerRun {
            dag_id: ghost,
            planned_for: None,
        })
        .await;
    let decided: Vec<_> = world.apply().await.decided.iter().map(summary).collect();
    assert_eq!(
        decided,
        [
            rejected("no DAG `ghost`"),
            rejected("no DAG `ghost`"),
            rejected("no run `0190a3e5-0000-7000-8000-000000000000`"),
            rejected("DAG `ghost` can't run: it doesn't exist, was removed or is in conflict"),
        ]
    );
}

#[tokio::test]
async fn a_trigger_creates_a_manual_run_by_the_requesting_user() {
    let mut world = world().await;
    world
        .ask(&Command::TriggerRun {
            dag_id: sync(),
            planned_for: Some(ts("2026-10-15T03:00:00Z")),
        })
        .await;
    let applied = world.apply().await;
    let run_id = applied.decided[0].result.as_ref().unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        world.db.run_tasks(&run_id).await.unwrap().unwrap().state,
        RunState::Running
    );
    let Database::Sqlite(pool) = &world.db else {
        panic!("expected SQLite")
    };
    let by: String = sqlx::query_scalar(AssertSqlSafe("SELECT triggered_by FROM runs".to_owned()))
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(by, "anna");
}

#[tokio::test]
async fn a_trigger_for_the_future_is_rejected() {
    let mut world = world().await;
    world
        .ask(&Command::TriggerRun {
            dag_id: sync(),
            planned_for: Some(ts("2026-10-22T00:00:00Z")),
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(applied.decided[0].status, CommandStatus::Rejected);
    assert!(
        applied.decided[0]
            .reason
            .as_deref()
            .unwrap()
            .starts_with("`--for` must not be in the future"),
    );
}

#[tokio::test]
async fn cancelling_a_running_run_stops_its_attempts_and_a_finished_one_is_rejected() {
    let mut world = world().await;
    world
        .ask(&Command::TriggerRun {
            dag_id: sync(),
            planned_for: None,
        })
        .await;
    let run_id = world.apply().await.decided[0].result.as_ref().unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let assigned = world.dispatcher.step(ts(NOW)).await.unwrap();
    let Some(pb::master_message::Message::AssignTask(assign)) = &assigned[0].message.message else {
        panic!("expected AssignTask")
    };
    let started = pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    };
    world
        .dispatcher
        .started("w1", &started, ts(NOW))
        .await
        .unwrap();

    world
        .ask(&Command::CancelRun {
            run_id: run_id.clone(),
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(
        applied.decided[0].result,
        Some(json!({"stopping_attempts": 1}))
    );
    assert_eq!(applied.outgoing.len(), 1);
    assert_eq!(applied.outgoing[0].worker_id, "w1");

    let finished = pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Cancelled.into(),
        ..pb::Finished::default()
    };
    world
        .dispatcher
        .finished("w1", &finished, ts(NOW))
        .await
        .unwrap();
    world
        .ask(&Command::CancelRun {
            run_id: run_id.clone(),
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(
        summary(&applied.decided[0]),
        rejected(&format!("run `{run_id}` already finished (cancelled)"))
    );
}

#[tokio::test]
async fn pausing_with_cancel_cancels_the_runs_in_progress() {
    let mut world = world().await;
    for _ in 0..2 {
        world
            .ask(&Command::TriggerRun {
                dag_id: sync(),
                planned_for: None,
            })
            .await;
    }
    let runs: Vec<String> = world
        .apply()
        .await
        .decided
        .iter()
        .map(|d| {
            d.result.as_ref().unwrap()["run_id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    world
        .ask(&Command::PauseDag {
            dag_id: sync(),
            cancel_runs: true,
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(
        applied.decided[0].result,
        Some(json!({"cancelled_runs": runs}))
    );
    for run_id in &runs {
        assert_eq!(
            world.db.run_tasks(run_id).await.unwrap().unwrap().state,
            RunState::Cancelled
        );
    }
}

#[tokio::test]
async fn a_payload_that_doesnt_read_is_rejected() {
    let mut world = world().await;
    let id = world.ask(&Command::UnpauseDag { dag_id: sync() }).await;
    let Database::Sqlite(pool) = &world.db else {
        panic!("expected SQLite")
    };
    sqlx::query(AssertSqlSafe(
        "UPDATE commands SET payload = '{\"kind\": \"drop_dag\"}'".to_owned(),
    ))
    .execute(pool)
    .await
    .unwrap();
    let applied = world.apply().await;
    assert_eq!(applied.decided[0].id, id);
    assert!(
        applied.decided[0]
            .reason
            .as_deref()
            .unwrap()
            .starts_with("invalid command: "),
    );
}
