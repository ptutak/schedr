use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_model::{Dag, RunState, TaskId};
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
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC", "catchup": false, "start": "2026-10-01T00:00:00Z"},
        "initial_since": null,
        "max_active_runs": 1,
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
    let schedr_db::database::Pool::Sqlite(pool) = world.db.pool() else {
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
    let schedr_db::database::Pool::Sqlite(pool) = world.db.pool() else {
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

#[tokio::test]
async fn a_task_is_cancelled_or_retried_and_impossible_requests_are_rejected() {
    let mut world = world().await;
    let extract = TaskId::new("extract").unwrap();
    let ghost = TaskId::new("ghost").unwrap();
    let unknown_run = "0190a3e5-0000-7000-8000-000000000000".to_owned();
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
    let cancel = |task_id: &TaskId| Command::CancelTask {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
    };
    let retry = |task_id: &TaskId| Command::RetryTask {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
    };
    world.ask(&cancel(&extract)).await;
    world.ask(&cancel(&ghost)).await;
    world.ask(&retry(&ghost)).await;
    world
        .ask(&Command::RetryTask {
            run_id: unknown_run.clone(),
            task_id: extract.clone(),
        })
        .await;
    let applied = world.apply().await;
    let decided: Vec<_> = applied.decided.iter().map(summary).collect();
    assert_eq!(
        decided,
        [
            rejected(&format!(
                "run `{run_id}` hasn't started its tasks yet: cancel the run \
                 (`schedr runs cancel`), or try again once it runs"
            )),
            rejected(&format!("run `{run_id}` has no task `ghost`")),
            rejected(&format!("run `{run_id}` has no task `ghost`")),
            rejected(&format!("no run `{unknown_run}`")),
        ]
    );

    world.dispatcher.step(ts(NOW)).await.unwrap();
    world.ask(&retry(&extract)).await;
    world.ask(&cancel(&extract)).await;
    let applied = world.apply().await;
    let decided: Vec<_> = applied.decided.iter().map(summary).collect();
    assert_eq!(
        decided,
        [
            rejected(&format!(
                "task `extract` of run `{run_id}` is running, not unschedulable: only an \
                 unschedulable task is retried (a failed one is retried by its `retries`)"
            )),
            (
                CommandStatus::Applied,
                None,
                Some(json!({"state": "stopping", "stopping_attempts": 1}))
            ),
        ]
    );
    assert_eq!(applied.outgoing.len(), 1);

    // The worker rejects it meanwhile: cancelled, and the run with it.
    let Some(pb::master_message::Message::CancelAttempt(stop)) =
        &applied.outgoing[0].message.message
    else {
        panic!("expected CancelAttempt")
    };
    let busy = pb::Rejected {
        attempt_id: stop.attempt_id.clone(),
        reason: pb::RejectReason::NoFreeSlot.into(),
        detail: String::new(),
    };
    world
        .dispatcher
        .rejected("w1", &busy, ts(NOW))
        .await
        .unwrap();
    world.ask(&cancel(&extract)).await;
    world.ask(&retry(&extract)).await;
    let applied = world.apply().await;
    let decided: Vec<_> = applied.decided.iter().map(summary).collect();
    assert_eq!(
        decided,
        [
            rejected(&format!("run `{run_id}` already finished (cancelled)")),
            rejected(&format!(
                "run `{run_id}` already finished (cancelled): trigger a new run"
            )),
        ]
    );
}

#[tokio::test]
async fn a_database_failure_leaves_a_task_command_pending() {
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
    world.dispatcher.step(ts(NOW)).await.unwrap();
    let schedr_db::database::Pool::Sqlite(pool) = world.db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query(AssertSqlSafe(
        "ALTER TABLE dag_versions RENAME TO gone".to_owned(),
    ))
    .execute(pool)
    .await
    .unwrap();
    world
        .ask(&Command::CancelTask {
            run_id,
            task_id: TaskId::new("extract").unwrap(),
        })
        .await;
    let applied = world
        .commander
        .apply_pending(&world.scheduler, &mut world.dispatcher, ts(NOW))
        .await;
    assert!(applied.is_err(), "{applied:?}");
    assert_eq!(world.db.pending_commands().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_worker_is_drained_and_resumed_and_unknown_ones_are_rejected() {
    let mut world = world().await;
    world
        .db
        .register_worker(
            &schedr_db::workers::WorkerRegistration {
                id: "w1",
                incarnation: "0190a0a0-0000-7000-8000-000000000001",
                protocol_version: "0.1",
                schedr_version: "0.1.0",
                queues: &[],
                slots: 2,
                log_endpoint: "",
            },
            ts(NOW),
        )
        .await
        .unwrap();
    for command in [
        Command::ResumeWorker {
            worker_id: "w1".to_owned(),
        },
        Command::DrainWorker {
            worker_id: "w1".to_owned(),
        },
        Command::DrainWorker {
            worker_id: "w9".to_owned(),
        },
    ] {
        world.ask(&command).await;
    }
    let applied = world.apply().await;
    let decided: Vec<_> = applied.decided.iter().map(summary).collect();
    let unknown = "no worker `w9` (`schedr workers list` shows the workers the master has seen)";
    assert_eq!(
        decided,
        [
            rejected("worker `w1` isn't drained"),
            (CommandStatus::Applied, None, Some(json!({"running": 0}))),
            rejected(unknown),
        ]
    );
    assert_eq!(applied.drained, [("w1".to_owned(), true)]);
    assert!(world.dispatcher.workers().get("w1").unwrap().drained);
    assert!(world.db.worker_drained("w1").await.unwrap());

    world
        .ask(&Command::ResumeWorker {
            worker_id: "w1".to_owned(),
        })
        .await;
    world
        .ask(&Command::ResumeWorker {
            worker_id: "w9".to_owned(),
        })
        .await;
    let applied = world.apply().await;
    let decided: Vec<_> = applied.decided.iter().map(summary).collect();
    assert_eq!(
        decided,
        [
            (CommandStatus::Applied, None, Some(json!({}))),
            rejected(unknown)
        ]
    );
    assert_eq!(applied.drained, [("w1".to_owned(), false)]);
    assert!(!world.dispatcher.workers().get("w1").unwrap().drained);
}

#[tokio::test]
async fn applying_into_adds_to_what_was_applied_before() {
    let mut world = world().await;
    world.ask(&Command::UnpauseDag { dag_id: sync() }).await;
    let mut applied = world.apply().await;
    assert_eq!(applied.decided.len(), 1);
    world.ask(&Command::UnpauseDag { dag_id: sync() }).await;
    world
        .commander
        .apply_into(
            &world.scheduler,
            &mut world.dispatcher,
            &mut applied,
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(applied.decided.len(), 2);
}

#[tokio::test]
async fn a_repeated_command_is_applied_only_when_what_it_asks_for_holds() {
    // B7: applied again after its round failed, a command whose requested state holds is
    // applied; anything else wrong with it is still a rejection.
    let mut world = world().await;
    let trigger = Command::TriggerRun {
        dag_id: sync(),
        planned_for: None,
    };
    let id = world.ask(&trigger).await;
    world.db.mark_applying(&id, ts(NOW)).await.unwrap();
    // The first try made the run, then its round failed.
    world
        .scheduler
        .trigger_as(Some(&id), &sync(), None, "anna", ts(NOW))
        .await
        .unwrap();
    let missing = world
        .ask(&Command::CancelTask {
            run_id: id.clone(),
            task_id: TaskId::new("no_such_task").unwrap(),
        })
        .await;
    world.db.mark_applying(&missing, ts(NOW)).await.unwrap();
    world.apply().await;
    let triggered = world.db.command(&id).await.unwrap().unwrap();
    assert_eq!(triggered.status, CommandStatus::Applied);
    assert_eq!(triggered.result, Some(json!({"run_id": id})));
    assert_eq!(world.db.running_count(&sync()).await.unwrap(), 1);
    let rejected = world.db.command(&missing).await.unwrap().unwrap();
    assert_eq!(rejected.status, CommandStatus::Rejected, "{rejected:?}");
}
