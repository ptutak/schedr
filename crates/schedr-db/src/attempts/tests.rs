use pretty_assertions::assert_eq;
use schedr_model::{DagId, RunState};
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
        "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "none"}, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [task("extract"), task("load")],
        "edges": [{"from": "extract", "to": "load", "kind": "data"}]
    }))
    .unwrap()
}

/// A database with a running run of `sync`: `extract` queued, `load` scheduled.
async fn with_run() -> (Database, String) {
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
        dag: Box::new(dag()),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts(NOW)).await.unwrap();
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    let dag_id = DagId::new("sync").unwrap();
    let run_id = db
        .insert_run(&NewRun {
            params: &crate::runs::NO_PARAMS,
            event: None,
            dag_id: &dag_id,
            dag_version_id: &version_id,
            kind: RunKind::Manual,
            state: RunState::Running,
            planned_for: ts(NOW),
            since: ts("2026-10-20T08:00:00Z"),
            triggered_at: ts(NOW),
            triggered_by: "anna",
            parent_run_id: None,
            backfill_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    db.create_instances(
        &run_id,
        &[
            (id("extract"), -1, TaskInstanceState::Queued),
            (id("load"), -1, TaskInstanceState::Scheduled),
        ],
        ts(NOW),
    )
    .await
    .unwrap();
    (db, run_id)
}

async fn instance_state(db: &Database, run_id: &str, task: &str) -> (TaskInstanceState, u32) {
    let run = db.run_tasks(run_id).await.unwrap().unwrap();
    let instance = run
        .instances
        .iter()
        .find(|i| i.task_id == id(task))
        .unwrap();
    (instance.state, instance.try_number)
}

#[tokio::test]
async fn ready_tasks_share_their_dags_one_definition() {
    // B13: each ready task brought its run's whole DAG definition and parsed it; a wide DAG
    // made every round read it once per ready task.
    let (db, run_id) = with_run().await;
    db.update_instances(
        &run_id,
        &[crate::instances::InstanceUpdate {
            task_id: id("load"),
            map_index: -1,
            state: TaskInstanceState::Queued,
            ready_at: Some(ts(NOW)),
            outcome: None,
            used_retry: false,
            reason: None,
            map_sizes: None,
        }],
        ts(NOW),
    )
    .await
    .unwrap();
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(ready.len(), 2);
    assert!(std::sync::Arc::ptr_eq(&ready[0].dag, &ready[1].dag));
    // And the next round's too: parsed once per version.
    let again = db.ready_instances(ts(NOW)).await.unwrap();
    assert!(std::sync::Arc::ptr_eq(&ready[0].dag, &again[0].dag));
}

#[tokio::test]
async fn ready_instances_are_the_queued_tasks_of_running_runs() {
    let (db, run_id) = with_run().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    assert_eq!(ready.len(), 1);
    let extract = &ready[0];
    assert_eq!(
        (extract.run_id.as_str(), extract.task_id.as_str()),
        (run_id.as_str(), "extract")
    );
    assert_eq!((extract.try_number, extract.run_kind), (0, RunKind::Manual));
    assert_eq!(
        (extract.planned_for, extract.since),
        (ts(NOW), ts("2026-10-20T08:00:00Z"))
    );
    assert_eq!(*extract.dag, dag());
    assert_eq!(extract.content_hash, "h1");
    assert_eq!(extract.queued_reason, None);
    assert_eq!(extract.parse_version, "1.0.0");
    db.set_queued_reasons(
        &[(
            extract.instance_id.clone(),
            "no worker serves queue `large`".to_owned(),
        )],
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        db.ready_instances(ts(NOW)).await.unwrap()[0]
            .queued_reason
            .as_deref(),
        Some("no worker serves queue `large`")
    );
    assert_eq!(
        db.ready_instances(ts("2026-10-21T07:59:59Z"))
            .await
            .unwrap(),
        []
    );
    let extract_id = extract.task_id.clone();
    db.request_task_cancel(&run_id, &extract_id, ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.ready_instances(ts(NOW)).await.unwrap(),
        [],
        "a task being cancelled isn't ready"
    );
    db.request_cancel(&run_id, ts(NOW)).await.unwrap();
    assert_eq!(db.ready_instances(ts(NOW)).await.unwrap(), []);
}

#[tokio::test]
async fn an_instance_is_assigned_once_with_the_next_try_number() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, try_number) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(try_number, 1);
    assert_eq!(
        db.assign_attempt(&instance_id, "w2", ts(NOW))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        instance_state(&db, &run_id, "extract").await,
        (TaskInstanceState::Running, 1)
    );
    let attempt = db.attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(
        attempt,
        StoredAttempt {
            id: attempt_id.clone(),
            instance_id,
            run_id,
            task_id: id("extract"),
            try_number: 1,
            worker_id: "w1".to_owned(),
            status: None,
            started: false,
            map_index: -1,
            result_kept: false,
            cleanup_moves: 0,
            cleanup_holder: 0,
            moving: false,
        }
    );
    assert_eq!(
        db.attempt("0190a3e5-0000-7000-8000-000000000000")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_batch_claims_the_queued_instances_with_their_attempts_and_skips_the_others() {
    // Decision 482: as `assign_attempt`, for many at once; an instance not queued (`load` is
    // scheduled, `extract` claimed already the second time) is left out, nothing stored for it.
    let (db, run_id) = with_run().await;
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    let instance = |task: &str| {
        run.instances
            .iter()
            .find(|i| i.task_id == id(task))
            .unwrap()
            .id
            .clone()
    };
    let (extract, load) = (instance("extract"), instance("load"));
    let (first, second) = (
        "0190a3e5-0000-7000-8000-000000000001",
        "0190a3e5-0000-7000-8000-000000000002",
    );
    let claims = [
        (extract.as_str(), "w1", first),
        (load.as_str(), "w2", second),
    ];
    let tries = db.claim_attempts(&claims, ts(NOW)).await.unwrap();
    assert_eq!(tries, BTreeMap::from([(extract.clone(), 1)]));
    assert_eq!(
        instance_state(&db, &run_id, "extract").await,
        (TaskInstanceState::Running, 1)
    );
    assert_eq!(
        instance_state(&db, &run_id, "load").await,
        (TaskInstanceState::Scheduled, 0)
    );
    let attempt = db.attempt(first).await.unwrap().unwrap();
    assert_eq!(
        (attempt.instance_id.as_str(), attempt.worker_id.as_str()),
        (extract.as_str(), "w1")
    );
    assert_eq!((attempt.try_number, attempt.started), (1, false));
    assert_eq!(db.attempt(second).await.unwrap(), None);
    let again = [(
        extract.as_str(),
        "w2",
        "0190a3e5-0000-7000-8000-000000000003",
    )];
    assert!(db.claim_attempts(&again, ts(NOW)).await.unwrap().is_empty());
}

#[tokio::test]
async fn first_starts_read_each_instances_earliest_start_at_once() {
    // Decision 482: `first_start` for a round's wait steps together; never started: absent.
    let (db, run_id) = with_run().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let extract = ready[0].instance_id.clone();
    let run = db.run_tasks(&run_id).await.unwrap().unwrap();
    let load = run
        .instances
        .iter()
        .find(|i| i.task_id == id("load"))
        .unwrap()
        .id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&extract, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert!(
        db.first_starts(&[&extract, &load])
            .await
            .unwrap()
            .is_empty()
    );
    db.mark_started(&attempt_id, "spawn", ts("2026-10-21T08:00:05Z"))
        .await
        .unwrap();
    assert_eq!(
        db.first_starts(&[&extract, &load]).await.unwrap(),
        BTreeMap::from([(extract.clone(), ts("2026-10-21T08:00:05Z"))])
    );
    assert_eq!(
        db.first_start(&extract).await.unwrap(),
        Some(ts("2026-10-21T08:00:05Z"))
    );
}

#[tokio::test]
async fn a_rejected_assignment_is_taken_back_without_a_try() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let later = ts("2026-10-21T08:00:35Z");
    db.undo_assignment(
        &attempt_id,
        TaskInstanceState::Queued,
        Some(later),
        Some("waiting"),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        instance_state(&db, &run_id, "extract").await,
        (TaskInstanceState::Queued, 0)
    );
    assert_eq!(db.attempt(&attempt_id).await.unwrap(), None);
    assert_eq!(db.ready_instances(ts(NOW)).await.unwrap(), []);
    assert_eq!(db.ready_instances(later).await.unwrap().len(), 1);
}

#[tokio::test]
async fn unschedulable_instances_are_queued_again_when_asked() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    db.undo_assignment(
        &attempt_id,
        TaskInstanceState::Unschedulable,
        None,
        Some("no"),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        instance_state(&db, &run_id, "extract").await.0,
        TaskInstanceState::Unschedulable
    );
    assert_eq!(db.requeue_unschedulable(ts(NOW)).await.unwrap(), 1);
    assert_eq!(
        instance_state(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
}

/// B284/B285: only the instances named are queued again, and only `unschedulable` ones.
#[tokio::test]
async fn only_the_unschedulable_instances_named_are_queued_again() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    db.undo_assignment(
        &attempt_id,
        TaskInstanceState::Unschedulable,
        None,
        Some("no"),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(db.requeue_unschedulable_of(&[], ts(NOW)).await.unwrap(), 0);
    let other = "01920000-0000-7000-8000-000000000000".to_owned();
    assert_eq!(
        db.requeue_unschedulable_of(&[other], ts(NOW))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        instance_state(&db, &run_id, "extract").await.0,
        TaskInstanceState::Unschedulable
    );
    let named = [instance_id];
    assert_eq!(
        db.requeue_unschedulable_of(&named, ts(NOW)).await.unwrap(),
        1
    );
    assert_eq!(
        instance_state(&db, &run_id, "extract").await.0,
        TaskInstanceState::Queued
    );
    // Queued already: not touched again.
    assert_eq!(
        db.requeue_unschedulable_of(&named, ts(NOW)).await.unwrap(),
        0
    );
    // B286: of these, those still there and not final.
    let both = [
        named[0].clone(),
        "01920000-0000-7000-8000-000000000000".to_owned(),
    ];
    assert_eq!(
        db.not_final(&both).await.unwrap(),
        BTreeSet::from([named[0].clone()])
    );
}

/// B254: a worker's tasks and wait steps running are counted apart, each against its slots.
#[tokio::test]
async fn a_workers_tasks_and_wait_steps_running_are_counted_apart() {
    let (db, _run_id) = with_run().await;
    db.register_worker(
        &crate::workers::WorkerRegistration {
            id: "w1",
            incarnation: "0190a0a0-0000-7000-8000-000000000001",
            protocol_version: "2.2",
            schedr_version: "1.0.0",
            queues: &[],
            slots: 4,
            watcher_slots: 2,
            log_endpoint: "",
        },
        ts(NOW),
    )
    .await
    .unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    // Both instances ready, `load` a wait step.
    sqlx::query(
        "UPDATE task_instances SET state = 'queued', ready_at = '2026-01-01T00:00:00Z', \
         wait_step = (task_id = 'load')",
    )
    .execute(pool)
    .await
    .unwrap();
    for ready in db.ready_instances(ts(NOW)).await.unwrap() {
        db.assign_attempt(&ready.instance_id, "w1", ts(NOW))
            .await
            .unwrap()
            .unwrap();
    }
    let workers = db.workers().await.unwrap();
    let w1 = workers.iter().find(|w| w.id == "w1").unwrap();
    assert_eq!((w1.running, w1.running_wait_steps), (1, 1));
}

#[tokio::test]
async fn attempts_start_and_end_once() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.open_attempts(Some("w1")).await.unwrap().len(), 1);
    assert_eq!(db.open_attempts(Some("w2")).await.unwrap(), []);
    assert_eq!(db.open_attempts(None).await.unwrap().len(), 1);
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T08:00:10Z"), &[], 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(db.unstarted_attempts(ts(NOW), &[], 100).await.unwrap(), []);
    // Not those of workers that may come back (F14's review).
    let away = ["w1".to_owned()];
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T08:00:10Z"), &away, 100)
            .await
            .unwrap(),
        []
    );
    let others = ["w2".to_owned(), "w3".to_owned()];
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T08:00:10Z"), &others, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    // At most `limit` (B52).
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T08:00:10Z"), &[], 0)
            .await
            .unwrap(),
        []
    );

    assert!(
        db.mark_started(&attempt_id, "spawn", ts(NOW))
            .await
            .unwrap()
    );
    assert!(db.attempt(&attempt_id).await.unwrap().unwrap().started);
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T09:00:00Z"), &[], 100)
            .await
            .unwrap(),
        []
    );

    let end = AttemptEnd {
        exit_code: Some(1),
        error_kind: Some("input_mismatch".to_owned()),
        error_summary: Some("{}".to_owned()),
        code_hash: Some("sha256:x".to_owned()),
        peak_memory_bytes: Some(1024),
        signal: None,
        usage: Some(schedr_model::runner::ProcessUsage {
            user_cpu_micros: 1_500_000,
            system_cpu_micros: 250_000,
            peak_rss_bytes: None,
            major_faults: 3,
            voluntary_context_switches: Some(40),
            involuntary_context_switches: Some(7),
        }),
        core_dumped: true,
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    assert!(db.end_attempt(&attempt_id, &end, ts(NOW)).await.unwrap());
    let stored = &db.task_attempts(&run_id, &id("extract")).await.unwrap()[0];
    assert_eq!((stored.usage, stored.core_dumped), (end.usage, true));
    assert!(
        !db.end_attempt(
            &attempt_id,
            &AttemptEnd::by_master(AttemptStatus::Lost),
            ts(NOW)
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.attempt(&attempt_id).await.unwrap().unwrap().status,
        Some(AttemptStatus::Failed)
    );
    assert!(
        !db.mark_started(&attempt_id, "spawn", ts(NOW))
            .await
            .unwrap()
    );
    assert_eq!(db.open_attempts(None).await.unwrap(), []);
}

#[tokio::test]
async fn an_adopted_attempt_counts_as_started_keeping_an_earlier_start() {
    let (db, run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    db.mark_adopted(&attempt_id, ts("2026-10-21T08:00:05Z"))
        .await
        .unwrap();
    assert!(db.attempt(&attempt_id).await.unwrap().unwrap().started);
    assert_eq!(
        db.unstarted_attempts(ts("2026-10-21T09:00:00Z"), &[], 100)
            .await
            .unwrap(),
        []
    );
    // A `Started` arriving after still sets the method; the first start time stays.
    assert!(
        db.mark_started(&attempt_id, "fork", ts("2026-10-21T08:00:09Z"))
            .await
            .unwrap()
    );
    db.mark_adopted(&attempt_id, ts("2026-10-21T08:00:20Z"))
        .await
        .unwrap();
    let stored = &db.task_attempts(&run_id, &id("extract")).await.unwrap()[0];
    assert_eq!(
        (stored.started_at, stored.start_method.as_deref()),
        (Some(ts("2026-10-21T08:00:05Z")), Some("fork"))
    );
}

#[tokio::test]
async fn run_outputs_are_the_stored_outputs_by_task() {
    let (db, run_id) = with_run().await;
    assert_eq!(db.run_outputs(&run_id).await.unwrap(), BTreeMap::new());
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    db.store_output(&instance_id, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    let done = InstanceUpdate {
        task_id: id("extract"),
        map_index: -1,
        state: TaskInstanceState::Success,
        ready_at: None,
        outcome: Some("SUCCESS".to_owned()),
        used_retry: false,
        reason: None,
        map_sizes: None,
    };
    db.update_instances(&run_id, &[done], ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.run_outputs(&run_id).await.unwrap(),
        BTreeMap::from([(id("extract"), json!({"rows": 3}))])
    );
    // Only the tasks asked for: an output not stored yet is simply not there.
    assert_eq!(
        db.outputs_of(&run_id, &[&id("extract"), &id("load")])
            .await
            .unwrap(),
        BTreeMap::from([(id("extract"), json!({"rows": 3}))])
    );
    assert_eq!(
        db.outputs_of(&run_id, &[&id("load")]).await.unwrap(),
        BTreeMap::new()
    );
    assert_eq!(db.outputs_of(&run_id, &[]).await.unwrap(), BTreeMap::new());
}

#[tokio::test]
async fn upstream_of_runs_reads_the_pairs_asked_over_many_runs() {
    let (db, first) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    db.store_output(&instance_id, "SUCCESS", &json!({"rows": 3}))
        .await
        .unwrap();
    let done = InstanceUpdate {
        task_id: id("extract"),
        map_index: -1,
        state: TaskInstanceState::Success,
        ready_at: None,
        outcome: Some("BIG".to_owned()),
        used_retry: false,
        reason: None,
        map_sizes: None,
    };
    db.update_instances(&first, &[done], ts(NOW)).await.unwrap();
    // A second run: `extract` skipped, `load` succeeded without an outcome stored.
    let version_id = db.active_dags().await.unwrap()[0].version_id.clone();
    let dag_id = DagId::new("sync").unwrap();
    let second = db
        .insert_run(&NewRun {
            params: &crate::runs::NO_PARAMS,
            event: None,
            dag_id: &dag_id,
            dag_version_id: &version_id,
            kind: RunKind::Manual,
            state: RunState::Running,
            planned_for: ts("2026-10-21T09:00:00Z"),
            since: ts("2026-10-20T09:00:00Z"),
            triggered_at: ts("2026-10-21T09:00:00Z"),
            triggered_by: "anna",
            parent_run_id: None,
            backfill_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    db.create_instances(
        &second,
        &[
            (id("extract"), -1, TaskInstanceState::Skipped),
            (id("load"), -1, TaskInstanceState::Success),
        ],
        ts(NOW),
    )
    .await
    .unwrap();
    let wanted = BTreeSet::from([
        (first.clone(), id("extract")),
        (first.clone(), id("load")),
        (second.clone(), id("extract")),
    ]);
    let reads = db.upstream_of_runs(&wanted).await.unwrap();
    // `load` of the first run is still scheduled: no outcome. The second run's `load`
    // succeeded but wasn't asked for, though its run and task each were.
    assert_eq!(
        reads.outcomes,
        BTreeMap::from([
            ((first.clone(), id("extract")), Some("BIG".to_owned())),
            ((second.clone(), id("extract")), None),
        ])
    );
    assert_eq!(
        reads.outputs,
        BTreeMap::from([((first.clone(), id("extract")), json!({"rows": 3}))])
    );
    // The same as the per-run reads give.
    assert_eq!(
        db.upstream_outcomes(&first, &[&id("extract"), &id("load")])
            .await
            .unwrap(),
        BTreeMap::from([(id("extract"), Some("BIG".to_owned()))])
    );
    let load_only = BTreeSet::from([(second.clone(), id("load"))]);
    assert_eq!(
        db.upstream_of_runs(&load_only).await.unwrap().outcomes,
        BTreeMap::from([(
            (second, id("load")),
            Some(schedr_model::DEFAULT_OUTCOME.to_owned())
        )])
    );
    assert_eq!(
        db.upstream_of_runs(&BTreeSet::new()).await.unwrap(),
        crate::attempts::UpstreamReads::default()
    );
}

#[tokio::test]
async fn stored_values_that_dont_read_are_errors() {
    let (db, _run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt_id, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let crate::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE attempts SET status = 'sometimes'")
        .execute(pool)
        .await
        .unwrap();
    let error = db.attempt(&attempt_id).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in attempts.status: "),
        "{error}"
    );
    sqlx::query(
        "UPDATE task_instances SET state = 'queued', ready_at = '2026-01-01T00:00:00.000000Z', \
         task_id = 'bad id!' WHERE task_id = 'extract'",
    )
    .execute(pool)
    .await
    .unwrap();
    let error = db.ready_instances(ts(NOW)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("invalid stored data in task_instances.task_id: "),
        "{error}"
    );
}

/// SQLite's plan for `query`, one line per step.
async fn query_plan(db: &Database, query: &str) -> String {
    let crate::Pool::Sqlite(pool) = db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    let rows: Vec<(i64, i64, i64, String)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!("EXPLAIN QUERY PLAN {query}")))
            .fetch_all(pool)
            .await
            .unwrap();
    rows.into_iter()
        .map(|(_, _, _, step)| step)
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn each_rounds_reads_of_pending_work_dont_scan_the_history() {
    // B87: with no retry due and no attempt running, a round must not read every finished
    // task instance and attempt.
    let (db, _) = with_run().await;
    let unstarted = query_plan(
        &db,
        &sql(crate::backend::Backend::Sqlite, UNSTARTED_ATTEMPTS).0,
    )
    .await;
    assert!(unstarted.contains("attempts_open"), "{unstarted}");
    let open = query_plan(&db, &sql(crate::backend::Backend::Sqlite, OPEN_ATTEMPTS).0).await;
    assert!(open.contains("attempts_open"), "{open}");
    let due = query_plan(
        &db,
        "SELECT run_id, task_id, map_index FROM task_instances \
         WHERE state = 'up_for_retry' AND ready_at <= '2026-10-21' ORDER BY ready_at, id",
    )
    .await;
    assert!(due.contains("task_instances_retry_due"), "{due}");
}

#[tokio::test]
async fn attempts_are_read_and_ended_together() {
    // M4B.4: a turn's attempts read by their IDs, and ended in one statement.
    let (db, run_id) = with_run().await;
    let ready = db.ready_instances(ts(NOW)).await.unwrap();
    let (first, _) = db
        .assign_attempt(&ready[0].instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let found = db
        .attempts_by_ids(&[first.as_str(), "01a00000-0000-7000-8000-000000000000"])
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
        [first.as_str()]
    );
    let end = AttemptEnd {
        exit_code: Some(3),
        error_kind: Some("input_mismatch".to_owned()),
        error_summary: Some("{}".to_owned()),
        usage: Some(schedr_model::runner::ProcessUsage {
            user_cpu_micros: 1_500_000,
            system_cpu_micros: 250_000,
            peak_rss_bytes: None,
            major_faults: 3,
            voluntary_context_switches: Some(40),
            involuntary_context_switches: Some(7),
        }),
        core_dumped: true,
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    };
    let recorded = db
        .end_attempts(&[(first.as_str(), &end)], ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        recorded.into_iter().collect::<Vec<_>>(),
        std::slice::from_ref(&first)
    );
    let stored = db.attempt(&first).await.unwrap().unwrap();
    assert_eq!(stored.status, Some(AttemptStatus::Failed));
    let shown = &db.task_attempts(&run_id, &id("extract")).await.unwrap()[0];
    assert_eq!((shown.usage, shown.core_dumped), (end.usage, true));
    // Ended already: not recorded again.
    let lost = AttemptEnd::by_master(AttemptStatus::Lost);
    assert!(
        db.end_attempts(&[(first.as_str(), &lost)], ts(NOW))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.attempt(&first).await.unwrap().unwrap().status,
        Some(AttemptStatus::Failed)
    );
}

#[tokio::test]
async fn an_attempts_reports_keep_the_first_and_latest_statuses_and_links_by_label() {
    use crate::reports::{AttemptLink, AttemptStatus, MAX_LINKS, MAX_STATUSES};
    let (db, _run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    for n in 0..=MAX_STATUSES {
        let status = AttemptStatus {
            at: ts(NOW),
            phase: format!("phase {n}"),
            message: (n == 0).then(|| "first".to_owned()),
            progress: (n == 1).then_some(0.5),
        };
        db.add_attempt_status(&attempt, ("dummy", "1.0"), &status)
            .await
            .unwrap();
    }
    let link = |label: &str, url: &str| AttemptLink {
        label: label.to_owned(),
        url: url.to_owned(),
    };
    assert!(
        db.set_attempt_link(&attempt, &link("Pod", "https://a"))
            .await
            .unwrap()
    );
    assert!(
        db.set_attempt_link(&attempt, &link("Pod", "https://b"))
            .await
            .unwrap()
    );
    for n in 1..MAX_LINKS {
        assert!(
            db.set_attempt_link(&attempt, &link(&format!("l{n}"), "https://x"))
                .await
                .unwrap()
        );
    }
    assert!(
        !db.set_attempt_link(&attempt, &link("one more", "https://x"))
            .await
            .unwrap()
    );
    let (statuses, links) = db.attempt_reports(&attempt).await.unwrap();
    assert_eq!(statuses.len(), usize::try_from(MAX_STATUSES).unwrap());
    assert_eq!(statuses[0].phase, "phase 0");
    assert_eq!(statuses[0].message.as_deref(), Some("first"));
    assert_eq!(statuses[1].phase, "phase 2", "the second oldest went");
    assert_eq!(
        statuses.last().unwrap().phase,
        format!("phase {MAX_STATUSES}")
    );
    assert_eq!(links[0], link("Pod", "https://b"));
    assert_eq!(links.len(), usize::try_from(MAX_LINKS).unwrap());
    // DN19: its extension's page lists it with its latest phase; another extension's doesn't.
    let listed = db
        .extension_attempts("dummy", (None, None), 10)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].attempt_id, attempt);
    assert_eq!(listed[0].status.phase, format!("phase {MAX_STATUSES}"));
    assert_eq!(
        (listed[0].try_number, listed[0].ended.as_deref()),
        (1, None)
    );
    assert_eq!(
        db.extension_attempts("other", (None, None), 10)
            .await
            .unwrap(),
        []
    );
    // DN21: a worker's tab lists only what it ran (agy's review of M5.2i5).
    assert_eq!(
        db.extension_attempts("dummy", (Some("w1"), None), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.extension_attempts("dummy", (Some("w2"), None), 10)
            .await
            .unwrap(),
        []
    );
}

#[tokio::test]
async fn a_flush_of_reports_keeps_only_its_workers_attempts_and_trims_as_one_by_one() {
    use crate::reports::{
        AttemptLink, AttemptStatus, MAX_LINKS, MAX_STATUSES, ReportedLink, ReportedStatus,
    };
    let (db, _run_id) = with_run().await;
    let instance_id = db.ready_instances(ts(NOW)).await.unwrap()[0]
        .instance_id
        .clone();
    let (attempt, _) = db
        .assign_attempt(&instance_id, "w1", ts(NOW))
        .await
        .unwrap()
        .unwrap();
    let status = |worker: &str, n: i64| ReportedStatus {
        attempt_id: attempt.clone(),
        worker_id: worker.to_owned(),
        extension: "dummy".to_owned(),
        version: "1.0".to_owned(),
        status: AttemptStatus {
            at: ts(NOW),
            phase: format!("phase {n}"),
            message: None,
            progress: None,
        },
    };
    let link = |worker: &str, label: &str| ReportedLink {
        attempt_id: attempt.clone(),
        worker_id: worker.to_owned(),
        link: AttemptLink {
            label: label.to_owned(),
            url: "https://x".to_owned(),
        },
    };
    let statuses: Vec<ReportedStatus> = (0..=MAX_STATUSES).map(|n| status("w1", n)).collect();
    let links: Vec<ReportedLink> = (0..=MAX_LINKS)
        .map(|n| link("w1", &format!("l{n}")))
        .collect();
    let kept = db.add_attempt_reports(&statuses, &links).await.unwrap();
    assert_eq!(
        kept,
        (
            usize::try_from(MAX_STATUSES + 1).unwrap(),
            usize::try_from(MAX_LINKS).unwrap()
        ),
        "past MAX_LINKS, a new label isn't kept"
    );
    // Another worker's aren't kept: the attempt isn't its (decision 521).
    assert_eq!(
        db.add_attempt_reports(&[status("w2", 999)], &[link("w2", "l0")])
            .await
            .unwrap(),
        (0, 0)
    );
    // Nor those of a hold that isn't its current one (decision 535; agy's fifth review of
    // B304), its own worker's or not.
    let mut later_hold = status("w1", 998);
    later_hold.attempt_id = format!("{attempt}_1");
    let mut later_link = link("w1", "l0");
    later_link.attempt_id = format!("{attempt}_1");
    assert_eq!(
        db.add_attempt_reports(&[later_hold], &[later_link])
            .await
            .unwrap(),
        (0, 0),
        "hold 1 isn't the current hold"
    );
    let (stored, links) = db.attempt_reports(&attempt).await.unwrap();
    assert_eq!(stored.len(), usize::try_from(MAX_STATUSES).unwrap());
    assert_eq!(stored[0].phase, "phase 0");
    assert_eq!(stored[1].phase, "phase 2", "the second oldest went");
    assert_eq!(links.len(), usize::try_from(MAX_LINKS).unwrap());
    // B292: a version's page lists its own attempts only, by the version stored with them.
    let of = |version| db.extension_attempts("dummy", (None, Some(version)), 10);
    assert_eq!(of("1.0").await.unwrap().len(), 1);
    assert_eq!(of("1.1").await.unwrap(), []);
    // Ended meanwhile: its worker's last reports, sent before its end, still kept.
    let failed = AttemptEnd::by_master(schedr_model::AttemptStatus::Failed);
    db.end_attempts(&[(attempt.as_str(), &failed)], ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        db.add_attempt_reports(&[status("w1", 1000)], &[])
            .await
            .unwrap(),
        (1, 0)
    );
}
