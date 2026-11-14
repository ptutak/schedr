use pretty_assertions::assert_eq;
use schedr_model::commands::Command;
use schedr_model::{Dag, DagId, RunKind, RunState, TaskId, TaskInstanceState};
use serde_json::json;

use super::*;
use crate::dags::{DagChange, NewParse};
use crate::instances::InstanceUpdate;
use crate::runs::NewRun;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

fn dag() -> Dag {
    dag_of("sync", &json!({"kind": "none"}))
}

fn dag_of(name: &str, trigger: &serde_json::Value) -> Dag {
    let task = |name: &str| {
        json!({
            "id": name, "kind": "python",
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        })
    };
    serde_json::from_value(json!({
        "produced_by": "1.0", "id": name,
        "location": {"root": "etl", "path": format!("{name}.py")},
        "trigger": trigger, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [task("extract"), task("load")],
        "edges": [{"from": "extract", "to": "load", "kind": "data"}]
    }))
    .unwrap()
}

/// A database with DAG `sync` defined and nothing else.
async fn with_dag() -> (Database, String) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    define(&db, dag()).await;
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    (db, version_id)
}

/// Defines `dag`, not paused.
async fn define(db: &Database, dag: Dag) {
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: dag.id.as_str(),
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
}

async fn run(db: &Database, version_id: &str, state: RunState, planned_for: &str) -> String {
    db.insert_run(&NewRun {
        params: &crate::runs::NO_PARAMS,
        event: None,
        dag_id: &DagId::new("sync").unwrap(),
        dag_version_id: version_id,
        kind: RunKind::Manual,
        state,
        planned_for: ts(planned_for),
        since: ts("2026-10-20T08:00:00Z"),
        triggered_at: ts(NOW),
        triggered_by: "anna",
        parent_run_id: None,
        backfill_id: None,
    })
    .await
    .unwrap()
    .unwrap()
}

/// What each check's own read finds at `now`: whether it returns anything.
async fn reads(db: &Database, now: Timestamp) -> Vec<(Check, bool)> {
    let mut found = vec![
        (
            Check::Commands,
            !db.pending_commands(100).await.unwrap().is_empty(),
        ),
        (
            Check::QueuedRuns,
            !db.dags_with_queued_runs().await.unwrap().is_empty(),
        ),
        (
            Check::Scheduled,
            !db.scheduled_due(now).await.unwrap().is_empty(),
        ),
        (
            Check::QueuedEventRuns,
            !db.queued_event_runs().await.unwrap().is_empty(),
        ),
        (
            Check::NewlyRunning,
            !db.newly_running().await.unwrap().is_empty(),
        ),
        (
            Check::DueRetries,
            !db.due_retries(now).await.unwrap().is_empty(),
        ),
        (
            Check::ReplacedAttempts,
            !db.replaced_attempts().await.unwrap().is_empty(),
        ),
        (
            Check::Unschedulable,
            !db.unschedulable_instances().await.unwrap().is_empty(),
        ),
        (
            Check::RemovedDagsRuns,
            !db.removed_dags_runs().await.unwrap().is_empty(),
        ),
        (
            Check::ChainedAttempts,
            !db.chained_attempts().await.unwrap().is_empty(),
        ),
        (
            Check::Ready,
            !db.ready_instances(now).await.unwrap().is_empty(),
        ),
    ];
    found.sort();
    found
}

/// The superset checks' reads (any cutoff, any worker, at `now`): a check says yes wherever
/// its read finds anything, and may where it doesn't.
async fn assert_supersets(db: &Database, now: Timestamp) {
    let far = ts("2100-01-01T00:00:00Z");
    let probe = db.probe(now).await.unwrap();
    for (check, read) in [
        (
            Check::Unstarted,
            !db.unstarted_attempts(far, &[], 100)
                .await
                .unwrap()
                .is_empty(),
        ),
        (
            Check::UnstartedCleanups,
            !db.unstarted_cleanups(far, &[], 100)
                .await
                .unwrap()
                .is_empty(),
        ),
        (
            Check::WaitingCleanups,
            !db.waiting_cleanups(now).await.unwrap().is_empty(),
        ),
    ] {
        assert!(
            !read || probe.found(check),
            "{check:?}: its read found some"
        );
    }
}

/// What the probe found for the checks [`reads`] covers.
async fn probed(db: &Database, now: Timestamp) -> Vec<(Check, bool)> {
    let probe = db.probe(now).await.unwrap();
    let mut found: Vec<(Check, bool)> = Check::ALL
        .iter()
        .filter(|c| {
            !matches!(
                c,
                Check::WaitingCleanups
                    | Check::Unstarted
                    | Check::UnstartedCleanups
                    | Check::Triggers
            )
        })
        .map(|&c| (c, probe.found(c)))
        .collect();
    found.sort();
    found
}

#[tokio::test]
async fn each_check_finds_what_its_read_finds() {
    // Decision 485: a check said no only where its read finds nothing, through the states a
    // run goes through; an empty database finds nothing at all.
    let (db, version) = with_dag().await;
    let now = ts(NOW);
    assert_eq!(probed(&db, now).await, reads(&db, now).await);
    let empty = db.probe(now).await.unwrap();
    assert!(
        Check::ALL.iter().all(|&c| !empty.found(c)),
        "nothing to do: every check says no"
    );
    let running = run(&db, &version, RunState::Running, NOW).await;
    assert_eq!(probed(&db, now).await, reads(&db, now).await);
    assert!(db.probe(now).await.unwrap().found(Check::NewlyRunning));
    db.create_instances(
        &running,
        &[
            (id("extract"), -1, TaskInstanceState::Queued),
            (id("load"), -1, TaskInstanceState::Scheduled),
        ],
        now,
    )
    .await
    .unwrap();
    assert_eq!(probed(&db, now).await, reads(&db, now).await);
    assert!(db.probe(now).await.unwrap().found(Check::Ready));
    assert_supersets(&db, now).await;
    // `extract` assigned, not started yet.
    let extract = db.ready_instances(now).await.unwrap()[0]
        .instance_id
        .clone();
    db.assign_attempt(&extract, "w1", now)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probed(&db, now).await, reads(&db, now).await);
    assert!(db.probe(now).await.unwrap().found(Check::Unstarted));
    assert_supersets(&db, now).await;
    run(&db, &version, RunState::Queued, "2026-10-22T08:00:00Z").await;
    db.insert_command(
        &Command::UnpauseDag {
            dag_id: DagId::new("sync").unwrap(),
        },
        "anna",
        now,
    )
    .await
    .unwrap();
    assert_eq!(probed(&db, now).await, reads(&db, now).await);
    let probe = db.probe(now).await.unwrap();
    assert!(probe.found(Check::QueuedRuns) && probe.found(Check::Commands));
    // A retry not yet due, then due.
    db.update_instances(
        &running,
        &[InstanceUpdate {
            task_id: id("extract"),
            map_index: -1,
            state: TaskInstanceState::UpForRetry,
            ready_at: Some(ts("2026-10-21T08:05:00Z")),
            outcome: None,
            used_retry: true,
            reason: None,
            map_sizes: None,
        }],
        now,
    )
    .await
    .unwrap();
    assert_eq!(probed(&db, now).await, reads(&db, now).await);
    assert!(!db.probe(now).await.unwrap().found(Check::DueRetries));
    let later = ts("2026-10-21T08:06:00Z");
    assert_eq!(probed(&db, later).await, reads(&db, later).await);
    assert!(db.probe(later).await.unwrap().found(Check::DueRetries));
    assert_supersets(&db, later).await;
}

#[tokio::test]
async fn a_scheduled_dag_is_due_while_its_next_fire_time_is_unknown_or_has_come() {
    // Decision 485: a new schedule's next fire time isn't known (NULL) and is due; one stored
    // ahead isn't until then; pausing or unpausing clears it, and a paused DAG is never due.
    let (db, _) = with_dag().await;
    let cron = json!({"kind": "cron", "expression": "0 4 * * *", "timezone": "UTC",
        "catchup": false});
    define(&db, dag_of("nightly", &cron)).await;
    let nightly = DagId::new("nightly").unwrap();
    let now = ts(NOW);
    let due = |at: Timestamp| {
        let db = &db;
        async move {
            let found = db.probe(at).await.unwrap().found(Check::Scheduled);
            let due = db.scheduled_due(at).await.unwrap();
            assert_eq!(
                found,
                !due.is_empty(),
                "the check finds what its read finds"
            );
            due
        }
    };
    // A new trigger starts paused (decision 459).
    assert_eq!(due(now).await, []);
    db.set_paused_at(&nightly, false, now).await.unwrap();
    assert_eq!(due(now).await, [(nightly.clone(), None)]);
    let next = ts("2026-10-22T04:00:00Z");
    db.set_next_fire(&nightly, next).await.unwrap();
    assert_eq!(due(now).await, []);
    assert_eq!(due(next).await, [(nightly.clone(), Some(next))]);
    assert_eq!(
        db.next_fires().await.unwrap(),
        [(nightly.clone(), Some(next))].into(),
        "only scheduled DAGs have one"
    );
    db.set_next_fire(&nightly, crate::dags::NO_FIRE)
        .await
        .unwrap();
    assert_eq!(due(next).await, []);
    db.set_paused_at(&nightly, true, now).await.unwrap();
    assert_eq!(
        db.next_fires().await.unwrap(),
        [(nightly.clone(), None)].into()
    );
    assert_eq!(due(next).await, [], "paused: not due though not known");
    db.set_paused_at(&nightly, false, now).await.unwrap();
    assert_eq!(due(next).await, [(nightly, None)]);
}

#[tokio::test]
async fn the_trigger_check_finds_a_switched_on_trigger_the_master_runs() {
    // Decision 485: the trigger pass is skipped only while no `run_of` or event trigger is
    // switched on and none runs on a worker.
    let (db, _) = with_dag().await;
    let now = ts(NOW);
    let found = |db: &Database| {
        let db = db.clone();
        async move { db.probe(now).await.unwrap().found(Check::Triggers) }
    };
    assert!(!found(&db).await, "a DAG without a trigger");
    define(
        &db,
        dag_of(
            "after",
            &json!({"kind": "run_of", "run_of": {"dag": "sync"}}),
        ),
    )
    .await;
    assert!(
        !found(&db).await,
        "a new trigger starts paused (decision 459)"
    );
    let after = DagId::new("after").unwrap();
    db.set_paused_at(&after, false, now).await.unwrap();
    assert!(found(&db).await);
    db.set_paused_at(&after, true, now).await.unwrap();
    assert!(!found(&db).await);
}

#[tokio::test]
async fn a_round_skips_what_the_probe_found_empty_and_probes_again_after_a_write() {
    // Decision 485: inside a round a read the probe found empty returns nothing, and a write
    // in between (a run started for the round's later parts) is found by the next read.
    let (db, version) = with_dag().await;
    let now = ts(NOW);
    let found = db
        .in_round(now, std::time::Duration::ZERO, async {
            assert!(db.skipped(Check::NewlyRunning).await.unwrap());
            assert_eq!(db.newly_running().await.unwrap().len(), 0);
            run(&db, &version, RunState::Running, NOW).await;
            assert!(!db.skipped(Check::NewlyRunning).await.unwrap());
            // Found, then another write: still read, whatever a probe would say.
            run(&db, &version, RunState::Queued, "2026-10-22T08:00:00Z").await;
            assert!(!db.skipped(Check::NewlyRunning).await.unwrap());
            db.newly_running().await.unwrap().len()
        })
        .await;
    assert_eq!(found, 1);
}

#[tokio::test]
async fn outside_a_round_no_read_is_skipped() {
    // Decision 485: only a master's round skips; a worker's turn or the API reads as before.
    let (db, _) = with_dag().await;
    for check in Check::ALL {
        assert!(!db.skipped(check).await.unwrap(), "{check:?}");
    }
}

#[tokio::test]
async fn a_read_for_a_later_time_than_the_rounds_probe_takes_it_again() {
    // Decision 485 (Grok's review of M5.1f1): a round's parts run at their own, later clocks.
    // Work stored inside the round at such a time (a task queued by prepare), and a retry
    // coming due after the round's start, are found by the read asking for that time.
    let (db, version) = with_dag().await;
    let start = ts(NOW);
    let later = ts("2026-10-21T08:00:01Z");
    let running = run(&db, &version, RunState::Running, NOW).await;
    let found = db
        .in_round(start, std::time::Duration::ZERO, async {
            // The round's first probe, at its start: nothing ready.
            assert!(db.skipped(Check::Ready).await.unwrap());
            db.create_instances(
                &running,
                &[(id("extract"), -1, TaskInstanceState::Queued)],
                later,
            )
            .await
            .unwrap();
            let ready = db.ready_instances(later).await.unwrap().len();
            db.update_instances(
                &running,
                &[InstanceUpdate {
                    task_id: id("extract"),
                    map_index: -1,
                    state: TaskInstanceState::UpForRetry,
                    ready_at: Some(later),
                    outcome: None,
                    used_retry: true,
                    reason: None,
                    map_sizes: None,
                }],
                start,
            )
            .await
            .unwrap();
            (ready, db.due_retries(later).await.unwrap().len())
        })
        .await;
    assert_eq!(found, (1, 1));
}

#[tokio::test]
async fn a_rounds_probe_looking_ahead_finds_what_comes_due_before_the_next_tick() {
    // Decision 485: looking a tick ahead, the probe says yes for a retry due within it, so a
    // part a little later reads it without another probe; due later than that, it's skipped.
    let (db, version) = with_dag().await;
    let start = ts(NOW);
    let running = run(&db, &version, RunState::Running, NOW).await;
    db.create_instances(
        &running,
        &[(id("extract"), -1, TaskInstanceState::Queued)],
        start,
    )
    .await
    .unwrap();
    let retry_at = |at: &str| InstanceUpdate {
        task_id: id("extract"),
        map_index: -1,
        state: TaskInstanceState::UpForRetry,
        ready_at: Some(ts(at)),
        outcome: None,
        used_retry: true,
        reason: None,
        map_sizes: None,
    };
    db.update_instances(&running, &[retry_at("2026-10-21T08:00:00.500Z")], start)
        .await
        .unwrap();
    let tick = std::time::Duration::from_secs(1);
    let (skipped, due) = db
        .in_round(start, tick, async {
            let skipped = db.skipped(Check::DueRetries).await.unwrap();
            let due = db
                .due_retries(ts("2026-10-21T08:00:00.600Z"))
                .await
                .unwrap();
            (skipped, due.len())
        })
        .await;
    assert_eq!((skipped, due), (false, 1), "due within the tick: read");
    db.update_instances(&running, &[retry_at("2026-10-21T08:00:05Z")], start)
        .await
        .unwrap();
    let skipped = db
        .in_round(start, tick, async {
            db.skipped(Check::DueRetries).await.unwrap()
        })
        .await;
    assert!(skipped, "due after the next tick: skipped");
}
