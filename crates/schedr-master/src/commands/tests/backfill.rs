//! Backfills through the Commander (decisions 363, 364, 400-404).

use pretty_assertions::assert_eq;
use schedr_model::commands::BackfillRequest;
use schedr_model::{RunKind, RunState};

use super::*;
use crate::faults::{Scenario, fail_each_write};

fn backfill(from: &str, to: Option<&str>) -> BackfillRequest {
    BackfillRequest {
        dag_id: sync(),
        from: ts(from),
        to: to.map(ts),
        all: false,
        params: None,
        dry_run: false,
    }
}

/// The DAG's runs, oldest first, as (`planned_for`, `since`, kind, state, backfill place).
async fn runs_of(
    db: &Database,
) -> Vec<(Timestamp, Timestamp, RunKind, RunState, Option<(u32, u32)>)> {
    let mut runs = db.dag_runs(&sync(), None, 100).await.unwrap();
    runs.sort_by_key(|r| (r.planned_for, r.backfill.is_some()));
    runs.iter()
        .map(|r| {
            (
                r.planned_for,
                r.since,
                r.kind,
                r.state,
                r.backfill.as_ref().map(|b| (b.index, b.total)),
            )
        })
        .collect()
}

async fn decide(world: &mut World, command: Command) -> Decided {
    world.ask(&command).await;
    world.apply().await.decided.remove(0)
}

#[tokio::test]
async fn a_backfill_runs_each_fire_time_with_no_successful_run_oldest_first() {
    let mut world = world().await;
    // The 16th has a manual run that succeeded; the 17th one that failed.
    for (at, state) in [
        ("2026-10-16T03:00:00Z", RunState::Success),
        ("2026-10-17T03:00:00Z", RunState::Failed),
    ] {
        let run_id = world
            .scheduler
            .trigger(&sync(), Some(ts(at)), "bob", ts(NOW))
            .await
            .unwrap()
            .run_id;
        world.db.finish_run(&run_id, state, ts(NOW)).await.unwrap();
    }
    let dry = decide(
        &mut world,
        Command::Backfill(BackfillRequest {
            dry_run: true,
            ..backfill("2026-10-15T00:00:00Z", Some("2026-10-18T12:00:00Z"))
        }),
    )
    .await;
    assert_eq!(
        dry.result,
        Some(
            json!({"backfill_id": null, "fire_times": 4, "created": 3, "skipped": 1,
                    "dry_run": true})
        )
    );
    assert_eq!(
        runs_of(&world.db).await.len(),
        2,
        "a dry run creates nothing"
    );

    let id = world
        .ask(&Command::Backfill(backfill(
            "2026-10-15T00:00:00Z",
            Some("2026-10-18T12:00:00Z"),
        )))
        .await;
    let decided = world.apply().await.decided.remove(0);
    assert_eq!(
        decided.result,
        Some(
            json!({"backfill_id": id, "fire_times": 4, "created": 3, "skipped": 1,
                    "dry_run": false})
        )
    );
    let day = |d: u8| ts(&format!("2026-10-{d}T03:00:00Z"));
    let backfilled = |d: u8, state, place| {
        (
            day(d),
            day(d - 1),
            RunKind::Backfill,
            state,
            Some((place, 3)),
        )
    };
    // The DAG is paused (as new DAGs are): its backfill starts all the same (decision 401),
    // one at a time (`max_active_runs=1`), oldest first.
    assert!(world.paused().await);
    assert_eq!(
        runs_of(&world.db).await,
        [
            backfilled(15, RunState::Running, 1),
            (day(16), day(15), RunKind::Manual, RunState::Success, None),
            (day(17), day(16), RunKind::Manual, RunState::Failed, None),
            backfilled(17, RunState::Queued, 2),
            backfilled(18, RunState::Queued, 3),
        ]
    );
    let stored = world.db.backfill(&id).await.unwrap().unwrap();
    assert_eq!(
        (stored.requested_by.as_str(), stored.all_fire_times),
        ("anna", false)
    );

    // Covered now, every one: nothing to create; `all` runs them again.
    let again = decide(
        &mut world,
        Command::Backfill(backfill(
            "2026-10-15T00:00:00Z",
            Some("2026-10-18T12:00:00Z"),
        )),
    )
    .await;
    assert_eq!(
        again.result,
        Some(
            json!({"backfill_id": null, "fire_times": 4, "created": 0, "skipped": 4,
                    "dry_run": false})
        )
    );
    let all = decide(
        &mut world,
        Command::Backfill(BackfillRequest {
            all: true,
            ..backfill("2026-10-15T00:00:00Z", Some("2026-10-18T12:00:00Z"))
        }),
    )
    .await;
    assert_eq!(all.result.unwrap()["created"], 4);
}

#[tokio::test]
async fn a_range_ending_later_than_now_ends_now() {
    let mut world = world().await;
    let decided = decide(
        &mut world,
        Command::Backfill(backfill(
            "2026-10-20T00:00:00Z",
            Some("2026-11-30T00:00:00Z"),
        )),
    )
    .await;
    assert_eq!(
        decided.result.unwrap()["fire_times"],
        2,
        "the 20th and the 21st at 03:00"
    );
    let open = decide(
        &mut world,
        Command::Backfill(backfill("2026-10-21T00:00:00Z", None)),
    )
    .await;
    assert_eq!(
        open.result.unwrap(),
        json!({"backfill_id": null, "fire_times": 1, "created": 0, "skipped": 1,
               "dry_run": false}),
        "the 21st's run, the first backfill's, is unfinished"
    );
    let stored = world.db.dag_backfills(&sync(), 10).await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].range_to, ts(NOW));
}

#[tokio::test]
async fn a_backfill_of_an_every_or_once_trigger_takes_its_fire_times() {
    let every = |dag: &mut Dag| {
        dag.trigger = serde_json::from_value(json!({"kind": "every", "interval_ms": 21_600_000,
            "catchup": false, "start": "2026-10-20T00:00:00Z", "end": null}))
        .unwrap();
    };
    let mut dag = dag();
    every(&mut dag);
    let mut world = world_of(dag).await;
    let decided = decide(
        &mut world,
        Command::Backfill(backfill("2026-10-20T00:00:00Z", None)),
    )
    .await;
    assert_eq!(
        decided.result.unwrap()["fire_times"],
        6,
        "every 6 hours from the 20th, 00:00 to the 21st, 06:00"
    );
    let mut dag = super::dag();
    dag.trigger =
        serde_json::from_value(json!({"kind": "once", "at": "2026-10-10T12:00:00Z"})).unwrap();
    let mut world = world_of(dag).await;
    let decided = decide(
        &mut world,
        Command::Backfill(backfill("2026-10-01T00:00:00Z", None)),
    )
    .await;
    assert_eq!(decided.result.unwrap()["fire_times"], 1);
    assert_eq!(runs_of(&world.db).await[0].0, ts("2026-10-10T12:00:00Z"));
}

#[tokio::test]
async fn backfills_that_cant_be_are_rejected() {
    let mut world = world().await;
    world.commander = Commander::new(world.db.clone()).with_backfill_max_runs(3);
    let cases = [
        (
            backfill("2026-10-15T00:00:00Z", Some("2026-10-19T00:00:00Z")),
            "DAG `sync`: no backfill: its range has more than 3 fire times, over \
             `[backfill] max_runs`: split it",
        ),
        (
            backfill("2026-10-15T04:00:00Z", Some("2026-10-16T02:00:00Z")),
            "DAG `sync`: no backfill: no fire time from 2026-10-15T04:00:00Z to \
             2026-10-16T02:00:00Z",
        ),
        (
            backfill("2026-10-16T00:00:00Z", Some("2026-10-15T00:00:00Z")),
            "DAG `sync`: no backfill: the range ends (2026-10-15T00:00:00Z) before it starts \
             (2026-10-16T00:00:00Z); a range ending later than now ends now",
        ),
        (
            BackfillRequest {
                params: Some(json!({"region": "us"}).as_object().unwrap().clone()),
                ..backfill("2026-10-15T00:00:00Z", Some("2026-10-16T00:00:00Z"))
            },
            "",
        ),
        (
            BackfillRequest {
                dag_id: DagId::new("ghost").unwrap(),
                ..backfill("2026-10-15T00:00:00Z", None)
            },
            "",
        ),
    ];
    for (request, reason) in cases {
        let decided = decide(&mut world, Command::Backfill(request.clone())).await;
        assert_eq!(decided.status, CommandStatus::Rejected, "{request:?}");
        if !reason.is_empty() {
            assert_eq!(decided.reason.as_deref(), Some(reason));
        }
    }
    assert_eq!(runs_of(&world.db).await, []);
    assert_eq!(world.db.dag_backfills(&sync(), 10).await.unwrap(), []);

    let mut dag = dag();
    dag.trigger = serde_json::from_value(json!({"kind": "none"})).unwrap();
    let mut world = world_of(dag).await;
    let decided = decide(
        &mut world,
        Command::Backfill(backfill("2026-10-15T00:00:00Z", None)),
    )
    .await;
    assert_eq!(
        decided.reason.as_deref(),
        Some(
            "DAG `sync`: no backfill: its trigger has no fire times (a backfill takes `Cron`, \
             `Every` or `Once`); start a run with `schedr runs trigger` instead"
        )
    );
}

#[tokio::test]
async fn cancelling_a_backfill_cancels_its_queued_and_running_runs_only() {
    let mut world = world().await;
    let id = world
        .ask(&Command::Backfill(backfill(
            "2026-10-15T00:00:00Z",
            Some("2026-10-17T12:00:00Z"),
        )))
        .await;
    world.apply().await;
    let oldest = world.db.unfinished_backfill_runs(&id).await.unwrap()[0].clone();
    world
        .db
        .finish_run(&oldest, RunState::Success, ts(NOW))
        .await
        .unwrap();
    let cancel = Command::CancelBackfill {
        backfill_id: id.clone(),
    };
    let decided = decide(&mut world, cancel.clone()).await;
    assert_eq!(decided.status, CommandStatus::Applied, "{decided:?}");
    assert_eq!(
        decided.result.unwrap()["cancelled_runs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let states: Vec<RunState> = runs_of(&world.db).await.iter().map(|r| r.3).collect();
    assert_eq!(
        states,
        [RunState::Success, RunState::Cancelled, RunState::Cancelled],
        "the finished one stays as it was"
    );
    assert_eq!(
        summary(&decide(&mut world, cancel).await),
        rejected(&format!(
            "backfill `{id}` has no queued or running run left"
        ))
    );
    let ghost = "01a1c0a0-0000-7000-8000-00000000ffff";
    assert_eq!(
        summary(
            &decide(
                &mut world,
                Command::CancelBackfill {
                    backfill_id: ghost.to_owned()
                }
            )
            .await
        ),
        rejected(&format!("no backfill `{ghost}`"))
    );
}

/// A round applying a backfill of three runs, whichever write fails, then the next round
/// (decision 98): the backfill once, its three runs, the oldest running.
struct Backfilling {
    world: World,
    command_id: String,
    run_id: String,
}

impl Scenario for Backfilling {
    async fn setup() -> Self {
        let world = world().await;
        let command_id = world
            .ask(&Command::Backfill(backfill(
                "2026-10-15T00:00:00Z",
                Some("2026-10-17T12:00:00Z"),
            )))
            .await;
        Self {
            world,
            command_id,
            run_id: String::new(),
        }
    }
    async fn run(&mut self) {
        let world = &mut self.world;
        let _ = world
            .commander
            .apply_pending(&world.scheduler, &mut world.dispatcher, ts(NOW))
            .await;
    }
    fn db(&self) -> &Database {
        &self.world.db
    }
    fn run_id(&self) -> &str {
        &self.run_id
    }
    fn dispatcher(&self) -> &Dispatcher {
        &self.world.dispatcher
    }
    async fn recover(&mut self) {
        let world = &mut self.world;
        let _ = world
            .commander
            .apply_pending(&world.scheduler, &mut world.dispatcher, ts(NOW))
            .await;
        let _ = world.scheduler.pass(ts(NOW)).await;
        let mut runs = world.db.dag_runs(&sync(), None, 10).await.unwrap();
        runs.sort_by_key(|r| r.planned_for);
        self.run_id = runs.first().map(|r| r.id.clone()).unwrap_or_default();
    }
    async fn extra(&self) -> Vec<String> {
        let mut found = Vec::new();
        let db = &self.world.db;
        let command = db.command(&self.command_id).await.unwrap().unwrap();
        if command.status != CommandStatus::Applied
            || command.result.as_ref().map(|r| r["created"].clone()) != Some(json!(3))
        {
            found.push(format!("the command is {command:?}"));
        }
        let backfills = db.dag_backfills(&sync(), 10).await.unwrap();
        if backfills.len() != 1 || backfills[0].id != self.command_id {
            found.push(format!("backfills stored: {backfills:?}"));
        }
        let runs = runs_of(db).await;
        let states: Vec<RunState> = runs.iter().map(|r| r.3).collect();
        if runs.len() != 3 || states != [RunState::Running, RunState::Queued, RunState::Queued] {
            found.push(format!("its runs: {runs:?}"));
        }
        found
    }
}

#[tokio::test]
async fn a_backfill_leaves_nothing_half_done_whichever_write_fails() {
    let (writes, found) = Box::pin(fail_each_write::<Backfilling>()).await;
    assert!(found.is_empty(), "{writes} writes:\n{}", found.join("\n"));
}
