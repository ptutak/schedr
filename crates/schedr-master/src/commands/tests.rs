use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_model::{Dag, RunState, TaskId};
use schedr_proto::pb;
use serde_json::Value as Json;
use sqlx::AssertSqlSafe;

use super::*;
use crate::dispatch::DEFAULT_CANCEL_GRACE;

mod backfill;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

fn dag() -> Dag {
    serde_json::from_value(json!({
        "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "a.py"},
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
    world_of(dag()).await
}

/// [`world`] with `dag` as DAG `sync`.
async fn world_of(dag: Dag) -> World {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "1.0.0",
            worker_id: "w1",
            parsed_at: ts(NOW),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
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
        self.db.stored_dags().await.unwrap()[0].paused == Some(true)
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
    // The DAG woken, in case it is quiet: unpaused, it may have runs to create.
    assert_eq!(
        world.scheduler.wakes().take().get(&sync()),
        Some(&crate::quiet::WakeReason::Command)
    );
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
async fn a_dag_without_a_trigger_has_nothing_to_pause() {
    // Decision 459: its manual runs start either way, and no switch is stored.
    let mut manual = dag();
    manual.trigger = schedr_model::Trigger::None;
    let mut world = world_of(manual).await;
    let refused = "DAG `sync` has no trigger: it runs only when started, so pausing changes \
                   nothing (cancel its runs with `schedr runs cancel`)";
    world.ask(&Command::UnpauseDag { dag_id: sync() }).await;
    world
        .ask(&Command::PauseDag {
            dag_id: sync(),
            cancel_runs: true,
        })
        .await;
    let applied = world.apply().await;
    let decided: Vec<_> = applied.decided.iter().map(summary).collect();
    assert_eq!(decided, [rejected(refused), rejected(refused)]);
    assert_eq!(world.db.stored_dags().await.unwrap()[0].paused, None);
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
            event: None,
            params: None,
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
            event: None,
            params: None,
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
            event: None,
            params: None,
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
            event: None,
            params: None,
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
                event: None,
                params: None,
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
            event: None,
            params: None,
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
        map_index: None,
    };
    let retry = |task_id: &TaskId| Command::RetryTask {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        map_index: None,
    };
    world.ask(&cancel(&extract)).await;
    world.ask(&cancel(&ghost)).await;
    world.ask(&retry(&ghost)).await;
    world
        .ask(&Command::RetryTask {
            run_id: unknown_run.clone(),
            task_id: extract.clone(),
            map_index: None,
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
            event: None,
            params: None,
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
            map_index: None,
        })
        .await;
    let applied = world
        .commander
        .apply_pending(&world.scheduler, &mut world.dispatcher, ts(NOW))
        .await;
    assert!(applied.is_err(), "{applied:?}");
    assert_eq!(world.db.pending_commands(100).await.unwrap().len(), 1);
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
                protocol_version: "1.0",
                schedr_version: "1.0.0",
                queues: &[],
                slots: 2,
                watcher_slots: 0,
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
        event: None,
        params: None,
        dag_id: sync(),
        planned_for: None,
    };
    let id = world.ask(&trigger).await;
    world.db.mark_applying(&id, ts(NOW)).await.unwrap();
    // The first try made the run, then its round failed.
    world
        .scheduler
        .trigger_as(Some(&id), &sync(), None, (None, None), "anna", ts(NOW))
        .await
        .unwrap();
    let missing = world
        .ask(&Command::CancelTask {
            run_id: id.clone(),
            task_id: TaskId::new("no_such_task").unwrap(),
            map_index: None,
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

#[tokio::test]
async fn a_round_applies_at_most_so_many_commands_the_rest_in_the_next() {
    // B18: a burst of commands (101 triggers) was applied in one round of about 0.85 s.
    let mut world = world().await;
    for i in 0..COMMANDS_PER_ROUND + 5 {
        let command = if i % 2 == 0 {
            Command::PauseDag {
                dag_id: sync(),
                cancel_runs: false,
            }
        } else {
            Command::UnpauseDag { dag_id: sync() }
        };
        world.ask(&command).await;
    }
    assert_eq!(
        world.apply().await.decided.len(),
        COMMANDS_PER_ROUND as usize
    );
    assert_eq!(world.apply().await.decided.len(), 5);
}

/// Roots `etl` (connections `db`, `api`, a refused `twice`) and `ml` (another `db`), both
/// parsed at `h1`; `w1` has `etl` at it, `w2` both, `w3` `etl` at an older hash.
fn with_connections(world: &mut World) {
    let registry = |names: &[&str]| {
        let mut registry = schedr_model::connection::Registry::default();
        for name in names {
            let def = serde_json::from_value(json!({"name": name, "params": {}})).unwrap();
            registry.connections.insert((*name).to_owned(), def);
            registry
                .files
                .insert((*name).to_owned(), "conns.py".to_owned());
        }
        registry
    };
    let mut etl = registry(&["db", "api"]);
    etl.refused
        .insert("twice".to_owned(), "defined in a.py and b.py".to_owned());
    world.dispatcher.set_connections(
        &std::collections::BTreeMap::from([
            ("etl".to_owned(), std::sync::Arc::new(etl)),
            ("ml".to_owned(), std::sync::Arc::new(registry(&["db"]))),
        ]),
        &std::collections::BTreeMap::from([
            ("etl".to_owned(), "h1".to_owned()),
            ("ml".to_owned(), "h1".to_owned()),
        ]),
    );
    let current = std::collections::BTreeSet::from(["h1".to_owned()]);
    world.dispatcher.set_current_code("etl", &current, None);
    world.dispatcher.set_current_code("ml", &current, None);
    world.dispatcher.report_root("w1", "etl", "h1");
}

async fn register(world: &mut World, worker: &str) {
    world
        .dispatcher
        .register(worker, "i1", &[], 2, ts(NOW))
        .await
        .unwrap();
}

fn jobs_sent(applied: &Applied) -> Vec<(String, String, String, Option<pb::ConnectionTest>)> {
    applied
        .outgoing
        .iter()
        .filter_map(|o| match &o.message.message {
            Some(master_message::Message::ConnectionJob(job)) => {
                let registry: schedr_model::connection::Registry =
                    serde_json::from_slice(&job.connections_json).unwrap();
                let names: Vec<_> = registry
                    .connections
                    .keys()
                    .chain(registry.refused.keys())
                    .cloned()
                    .collect();
                Some((
                    o.worker_id.clone(),
                    job.job_id.clone(),
                    format!(
                        "{}@{}: {}",
                        job.root.as_ref().unwrap().root,
                        job.root.as_ref().unwrap().content_hash,
                        names.join(",")
                    ),
                    job.test.clone(),
                ))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_check_goes_to_a_worker_with_each_roots_current_code() {
    let mut world = world().await;
    register(&mut world, "w2").await;
    register(&mut world, "w3").await;
    with_connections(&mut world);
    world.dispatcher.report_root("w2", "etl", "h1");
    world.dispatcher.report_root("w2", "ml", "h1");
    world.dispatcher.report_root("w3", "etl", "h0");
    let every = world
        .ask(&Command::CheckConnections {
            root: None,
            name: None,
            worker: None,
        })
        .await;
    let one = world
        .ask(&Command::CheckConnections {
            root: Some("etl".to_owned()),
            name: Some("api".to_owned()),
            worker: Some("w2".to_owned()),
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(
        jobs_sent(&applied),
        [
            // The fewest attempts, then by ID: `w1` for `etl`; only `w2` has `ml`.
            (
                "w1".to_owned(),
                format!("{every}/etl"),
                "etl@h1: api,db,twice".to_owned(),
                None
            ),
            (
                "w2".to_owned(),
                format!("{every}/ml"),
                "ml@h1: db".to_owned(),
                None
            ),
            (
                "w2".to_owned(),
                format!("{one}/etl"),
                "etl@h1: api".to_owned(),
                None
            ),
        ]
    );
    assert_eq!(
        summary(&applied.decided[0]),
        (
            CommandStatus::Applied,
            None,
            Some(json!({"jobs": [
                {"root": "etl", "worker": "w1", "state": "running"},
                {"root": "ml", "worker": "w2", "state": "running"},
            ]}))
        )
    );
    let check = std::time::Duration::from_secs(30);
    assert_eq!(
        applied
            .connection_jobs
            .iter()
            .map(|job| (job.job_id.clone(), job.worker_id.clone(), job.allowed))
            .collect::<Vec<_>>(),
        [
            (format!("{every}/etl"), "w1".to_owned(), check),
            (format!("{every}/ml"), "w2".to_owned(), check),
            (format!("{one}/etl"), "w2".to_owned(), check),
        ]
    );
}

#[tokio::test]
async fn a_test_goes_to_the_root_defining_it_with_its_timeout() {
    let mut world = world().await;
    with_connections(&mut world);
    let id = world
        .ask(&Command::TestConnection {
            name: "api".to_owned(),
            root: None,
            worker: None,
            timeout_ms: 5_000,
        })
        .await;
    let applied = world.apply().await;
    assert_eq!(
        jobs_sent(&applied),
        [(
            "w1".to_owned(),
            format!("{id}/etl"),
            "etl@h1: api".to_owned(),
            Some(pb::ConnectionTest {
                name: "api".to_owned(),
                timeout_ms: 5_000
            })
        )]
    );
}

#[tokio::test]
async fn connections_commands_no_worker_or_root_can_take_are_rejected() {
    let mut world = world().await;
    register(&mut world, "w3").await;
    with_connections(&mut world);
    world.dispatcher.report_root("w3", "etl", "h0");
    // Newer code, its parse on its way: tasks may go to it, connections jobs not (its
    // connections may differ from those sent).
    register(&mut world, "w4").await;
    let newer = std::collections::BTreeSet::from(["h1".to_owned(), "h2".to_owned()]);
    world.dispatcher.set_current_code("etl", &newer, None);
    world.dispatcher.report_root("w4", "etl", "h2");
    // Drained for maintenance: no new work, named or not.
    register(&mut world, "w5").await;
    world.dispatcher.report_root("w5", "etl", "h1");
    world.dispatcher.drain("w5");
    let test = |name: &str, root: Option<&str>, worker: Option<&str>, timeout_ms| {
        Command::TestConnection {
            name: name.to_owned(),
            root: root.map(str::to_owned),
            worker: worker.map(str::to_owned),
            timeout_ms,
        }
    };
    let check =
        |root: Option<&str>, name: Option<&str>, worker: Option<&str>| Command::CheckConnections {
            root: root.map(str::to_owned),
            name: name.map(str::to_owned),
            worker: worker.map(str::to_owned),
        };
    for command in [
        test("db", None, None, 1_000),
        test("nope", None, None, 1_000),
        test("db", Some("ml"), None, 1_000),
        test("twice", Some("etl"), None, 1_000),
        test("api", None, None, 0),
        test("api", None, Some("w3"), 1_000),
        test("api", None, Some("w9"), 1_000),
        test("api", None, Some("w4"), 1_000),
        test("api", None, Some("w5"), 1_000),
        check(Some("etl"), Some("nope"), None),
        check(Some("other"), None, None),
        check(None, None, None),
    ] {
        world.ask(&command).await;
    }
    let applied = world.apply().await;
    let reasons: Vec<_> = applied
        .decided
        .iter()
        .map(|d| {
            d.reason
                .clone()
                .unwrap_or_else(|| format!("{:?}", d.status))
        })
        .collect();
    assert_eq!(
        reasons,
        [
            "connection `db` is defined in roots etl, ml: give one (`--root`)",
            "no root defines connection `nope`",
            "no active worker has root `ml` at its current code",
            "defined in a.py and b.py",
            "a test's timeout is 1 ms to 3600 s",
            "worker `w3` doesn't have root `etl` at its current code",
            "worker `w9` isn't connected",
            "worker `w4` doesn't have root `etl` at its current code",
            "worker `w5` is draining or drained",
            "root `etl` defines no connection `nope`",
            "root `other` defines no connections",
            // `ml` has no worker: the whole check is refused, not sent in part.
            "no active worker has root `ml` at its current code",
        ]
    );
    assert_eq!(applied.outgoing.len(), 0);
}

/// [`dag`] started by a trigger whose events hold at most 16 bytes.
fn event_dag() -> Dag {
    let mut dag = serde_json::to_value(dag()).unwrap();
    dag["trigger"] = serde_json::json!({
        "kind": "event",
        "task": {
            "id": "found", "kind": "sensor", "mode": "loop", "arguments": {},
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 16
            },
            "output_type": {"type": "none"}
        }
    });
    serde_json::from_value(dag).unwrap()
}

fn run_with(event: Option<AskedEvent>) -> Command {
    Command::TriggerRun {
        event,
        params: None,
        dag_id: sync(),
        planned_for: None,
    }
}

/// What the master says to `command`: applied or why not.
async fn decided(world: &mut World, command: &Command) -> Result<(), String> {
    world.ask(command).await;
    let applied = world.apply().await;
    let decided = &applied.decided[0];
    match decided.status {
        CommandStatus::Rejected => Err(decided.reason.clone().unwrap_or_default()),
        _ => Ok(()),
    }
}

#[tokio::test]
async fn a_manual_run_of_a_dag_with_a_trigger_needs_an_event_it_could_have_found() {
    let mut world = world_of(event_dag()).await;
    let given = |key: &str, data: Json| {
        Some(AskedEvent::Given {
            key: key.to_owned(),
            data,
            at: Some(ts("2026-10-14T03:00:00Z")),
        })
    };
    let why = decided(&mut world, &run_with(None)).await.unwrap_err();
    assert!(why.contains("event"), "{why}");
    let unknown = Some(AskedEvent::Stored {
        key: "gone".to_owned(),
    });
    let why = decided(&mut world, &run_with(unknown)).await.unwrap_err();
    assert_eq!(why, "no event `gone` of DAG `sync`");
    let bad_key = given("a\nb", Json::Null);
    let why = decided(&mut world, &run_with(bad_key)).await.unwrap_err();
    assert!(why.contains("control"), "{why}");
    let big = given("big", Json::String("x".repeat(64)));
    let why = decided(&mut world, &run_with(big)).await.unwrap_err();
    assert!(
        why.contains("over its trigger's `max_output_size`"),
        "{why}"
    );
    let fits = given("fits", Json::from(7));
    assert_eq!(decided(&mut world, &run_with(fits)).await, Ok(()));
}

#[tokio::test]
async fn a_dag_without_a_trigger_takes_no_event_and_has_no_trigger_to_restart() {
    let mut world = world().await;
    let event = Some(AskedEvent::Given {
        key: "a".to_owned(),
        data: Json::Null,
        at: None,
    });
    let why = decided(&mut world, &run_with(event)).await.unwrap_err();
    assert!(why.contains("no event trigger starts its runs"), "{why}");
    let restart = Command::RestartTrigger { dag_id: sync() };
    let why = decided(&mut world, &restart).await.unwrap_err();
    assert_eq!(why, "DAG `sync` has no event trigger");
    let gone = Command::RestartTrigger {
        dag_id: DagId::new("gone").unwrap(),
    };
    assert!(decided(&mut world, &gone).await.is_err());
}

#[tokio::test]
async fn a_trigger_restarts_and_its_function_can_be_switched_on() {
    let mut world = world_of(event_dag()).await;
    let restart = Command::RestartTrigger { dag_id: sync() };
    assert_eq!(decided(&mut world, &restart).await, Ok(()));
    let on = |sensor: &str| Command::SwitchOnSensor {
        dag_id: sync(),
        sensor: TaskId::new(sensor).unwrap(),
    };
    assert_eq!(decided(&mut world, &on("found")).await, Ok(()));
    let why = decided(&mut world, &on("extract")).await.unwrap_err();
    assert_eq!(why, "no wait step `extract` in DAG `sync`");
}

#[tokio::test]
async fn a_manual_run_of_an_event_named_dag_keeps_its_data_as_given() {
    // Grok's review of M4.11e: a tag-shaped dict read back as a datetime.
    let mut dag = serde_json::to_value(dag()).unwrap();
    dag["trigger"] = serde_json::json!({"kind": "event_named", "event": {"name": "paid"}});
    let mut world = world_of(serde_json::from_value(dag).unwrap()).await;
    let data = serde_json::json!({"$datetime": "2026-01-01T00:00:00+00:00"});
    let given = Some(AskedEvent::Given {
        key: "o-9".to_owned(),
        data: data.clone(),
        at: None,
    });
    assert_eq!(decided(&mut world, &run_with(given)).await, Ok(()));
    let run = &world.db.dag_runs(&sync(), None, 5).await.unwrap()[0];
    let event = world.db.run_event(&run.id).await.unwrap().unwrap();
    assert_eq!(event.data, serde_json::json!({"$dict": data}));
}

#[tokio::test]
async fn an_item_of_a_batch_run_again_is_a_batch_of_it_alone() {
    // The side agent's note on B188's fix: each item is stored as its own event, while the
    // DAG's runs read a list of them.
    let mut dag = serde_json::to_value(event_dag()).unwrap();
    dag["trigger"]["task"]["mode"] = serde_json::json!("poke");
    dag["trigger"]["task"]["every_ms"] = serde_json::json!(1000);
    dag["trigger"]["task"]["batch_ms"] = serde_json::json!(60_000);
    let mut world = world_of(serde_json::from_value(dag).unwrap()).await;
    let active = world.db.active_dags().await.unwrap();
    let new = schedr_db::runs::NewRun {
        dag_id: &sync(),
        dag_version_id: &active[0].version_id,
        kind: schedr_model::RunKind::Event,
        state: schedr_model::RunState::Queued,
        planned_for: ts("2026-10-14T03:00:00Z"),
        since: ts("2026-10-14T03:00:00Z"),
        triggered_at: ts("2026-10-14T03:00:00Z"),
        triggered_by: "trigger",
        params: &schedr_db::runs::NO_PARAMS,
        event: None,
        parent_run_id: None,
        backfill_id: None,
    };
    let item = schedr_model::RunEvent {
        key: "a".to_owned(),
        data: Json::from(1),
        at: Some(ts("2026-10-14T02:00:00Z")),
    };
    let raw =
        serde_json::json!({"key": "a", "data": 1, "at": {"$datetime": "2026-10-14T02:00:00Z"}});
    world
        .db
        .store_batch(
            "0190a0a0-0000-7000-8000-0000000000c1",
            &new,
            "a",
            &[(item, raw.clone())],
        )
        .await
        .unwrap();
    let again = Some(AskedEvent::Stored {
        key: "a".to_owned(),
    });
    assert_eq!(decided(&mut world, &run_with(again)).await, Ok(()));
    let runs = world.db.dag_runs(&sync(), None, 5).await.unwrap();
    let manual = runs
        .iter()
        .find(|run| run.kind == schedr_model::RunKind::Manual)
        .unwrap();
    let event = world.db.run_event(&manual.id).await.unwrap().unwrap();
    assert_eq!(event.data, serde_json::json!([raw]));
}
