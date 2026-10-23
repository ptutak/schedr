use pretty_assertions::assert_eq;
use schedr_model::DagId;
use schedr_model::runner::ParsedFile;
use schedr_proto::convert::parsed_file_to_proto;
use serde_json::json;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

const NOW: &str = "2026-10-21T08:00:00Z";

fn later(seconds: i64) -> Timestamp {
    ts(NOW)
        .checked_add(SignedDuration::from_secs(seconds))
        .unwrap()
}

async fn core() -> MasterCore {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    MasterCore::new(db, Settings::default())
}

fn root(hash: &str) -> pb::RootHash {
    pb::RootHash {
        kind: pb::RootKind::Dag.into(),
        root: "etl".to_owned(),
        content_hash: hash.to_owned(),
    }
}

fn register(worker: &str, incarnation: &str, journal: Vec<pb::JournalEntry>) -> pb::Register {
    pb::Register {
        worker_id: worker.to_owned(),
        incarnation_id: incarnation.to_owned(),
        protocol: Some(PROTOCOL_VERSION),
        schedr_version: "0.1.0".to_owned(),
        queues: vec![],
        slots: 2,
        log_endpoint: "127.0.0.1:8081".to_owned(),
        roots: vec![root("h1")],
        secret_storages: vec![],
        journal,
    }
}

fn dag_file() -> ParsedFile {
    serde_json::from_value(json!({
        "path": "sync.py",
        "dags": [{
            "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
            "trigger": {"kind": "none"}, "initial_since": null,
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
        }],
        "error": null
    }))
    .unwrap()
}

fn upload(job_id: &str) -> pb::ParseResult {
    pb::ParseResult {
        parse_job_id: job_id.to_owned(),
        worker_id: "w1".to_owned(),
        root: Some(root("h1")),
        schedr_version: "0.1.0".to_owned(),
        files: vec![parsed_file_to_proto(&dag_file())],
    }
}

fn message(outgoing: &Outgoing) -> &master_message::Message {
    outgoing.message.message.as_ref().unwrap()
}

/// Registers `w1`, parses its root and stores DAG `sync`; returns the core.
async fn with_dag() -> MasterCore {
    let mut core = core().await;
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    core.upload(upload(&job.parse_job_id), ts(NOW))
        .await
        .unwrap();
    core
}

async fn trigger(core: &mut MasterCore) {
    let command = schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    core.db
        .insert_command(&command, "anna", ts(NOW))
        .await
        .unwrap();
}

#[tokio::test]
async fn registering_answers_with_the_timing_and_parse_jobs_for_new_roots() {
    let mut core = core().await;
    let (registered, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    assert_eq!(
        registered.timing,
        Some(pb::Timing {
            heartbeat_interval_ms: 10_000,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 30_000,
        })
    );
    assert!(core.is_live("w1"));
    assert_eq!(core.dispatcher().workers().get("w1").unwrap().slots, 2);
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].worker_id, "w1");
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    assert_eq!(job.root, Some(root("h1")));
    assert_eq!(core.settings(), Settings::default());
}

#[tokio::test]
async fn an_uploaded_parse_stores_its_dags_and_a_known_hash_needs_no_job() {
    let mut core = with_dag().await;
    let dags = core.db.stored_dags().await.unwrap();
    assert_eq!(dags[0].dag_id.as_str(), "sync");
    let changed = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
        root: Some(root("h1")),
    });
    assert_eq!(core.handle("w1", changed, ts(NOW)).await.unwrap(), []);
    let new_code = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
        root: Some(root("h2")),
    });
    let out = core.handle("w1", new_code, ts(NOW)).await.unwrap();
    assert!(matches!(
        message(&out[0]),
        master_message::Message::AssignParse(_)
    ));
    let scripts = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
        root: Some(pb::RootHash {
            kind: pb::RootKind::Script.into(),
            ..root("h3")
        }),
    });
    assert_eq!(core.handle("w1", scripts, ts(NOW)).await.unwrap(), []);
    let empty = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged { root: None });
    assert_eq!(core.handle("w1", empty, ts(NOW)).await.unwrap(), []);
}

#[tokio::test]
async fn a_tick_applies_commands_and_assigns_the_task_which_then_runs_to_the_end() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    assert_eq!(
        (out[0].worker_id.as_str(), assign.task_id.as_str()),
        ("w1", "extract")
    );
    assert_eq!(assign.expected_code_hash, "h1");
    assert_eq!(
        core.inputs(&assign.attempt_id).await.unwrap(),
        Some(b"{}".to_vec())
    );

    let started = worker_message::Message::Started(pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    });
    assert_eq!(core.handle("w1", started, ts(NOW)).await.unwrap(), []);
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: b"null".to_vec(),
        ..pb::Finished::default()
    });
    let ack = core.handle("w1", finished, ts(NOW)).await.unwrap();
    assert!(matches!(
        message(&ack[0]),
        master_message::Message::FinishedAck(_)
    ));
    assert_eq!(core.inputs(&assign.attempt_id).await.unwrap(), None);
    let runs = core
        .db
        .dag_runs(&DagId::new("sync").unwrap(), None, 10)
        .await
        .unwrap();
    assert_eq!(runs[0].state, schedr_model::RunState::Success);
}

#[tokio::test]
async fn rejections_heartbeats_and_draining_are_handled() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let heartbeat = worker_message::Message::Heartbeat(pb::Heartbeat::default());
    assert_eq!(core.handle("w1", heartbeat, later(30)).await.unwrap(), []);
    let rejected = worker_message::Message::Rejected(pb::Rejected {
        attempt_id: assign.attempt_id.clone(),
        reason: pb::RejectReason::NoFreeSlot.into(),
        detail: String::new(),
    });
    assert_eq!(core.handle("w1", rejected, later(30)).await.unwrap(), []);
    let draining = worker_message::Message::Draining(pb::Draining {});
    core.handle("w1", draining, later(30)).await.unwrap();
    assert_eq!(core.tick(later(31)).await.unwrap(), []);
}

#[tokio::test]
async fn a_second_register_on_the_stream_is_a_bad_message() {
    let mut core = with_dag().await;
    let again = worker_message::Message::Register(register("w1", "i1", vec![]));
    let error = core.handle("w1", again, ts(NOW)).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "message from worker `w1`: `Register` is only the first message"
    );
}

#[tokio::test]
async fn incompatible_and_duplicate_registrations_are_refused() {
    let mut core = core().await;
    let old = pb::Register {
        protocol: Some(pb::ProtocolVersion { major: 9, minor: 0 }),
        ..register("w1", "i1", vec![])
    };
    let error = core.register(&old, ts(NOW)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("registration refused: worker protocol 9.0"),
        "{error}"
    );
    let nameless = register("", "i1", vec![]);
    assert_eq!(
        core.register(&nameless, ts(NOW))
            .await
            .unwrap_err()
            .to_string(),
        "registration refused: the worker sent no worker ID"
    );
    core.register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let error = core
        .register(&register("w1", "i2", vec![]), ts(NOW))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "registration refused: worker `w1` already has a live stream"
    );
    core.disconnected("w1", ts(NOW)).await.unwrap();
    assert!(!core.is_live("w1"));
    core.register(&register("w1", "i2", vec![]), ts(NOW))
        .await
        .unwrap();
    core.disconnected("ghost", ts(NOW)).await.unwrap();
}

#[tokio::test]
async fn journal_entries_are_continued_acknowledged_or_killed() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let open = assign.attempt_id.clone();
    let entry = |id: &str, state: pb::JournalState| pb::JournalEntry {
        attempt_id: id.to_owned(),
        state: state.into(),
    };
    core.disconnected("w1", ts(NOW)).await.unwrap();
    let journal = vec![
        entry(&open, pb::JournalState::Running),
        entry(
            "0190a3e5-0000-7000-8000-000000000000",
            pb::JournalState::Running,
        ),
    ];
    let (registered, _) = core
        .register(&register("w1", "i1", journal), later(5))
        .await
        .unwrap();
    let decisions: Vec<i32> = registered.decisions.iter().map(|d| d.decision).collect();
    assert_eq!(
        decisions,
        [
            i32::from(pb::AttemptDecision::Continue),
            i32::from(pb::AttemptDecision::Kill)
        ]
    );
    assert_eq!(
        core.dispatcher().workers().get("w1").unwrap().free_slots(),
        1
    );

    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: open.clone(),
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    });
    core.handle("w1", finished, later(6)).await.unwrap();
    core.disconnected("w1", ts(NOW)).await.unwrap();
    let journal = vec![
        entry(&open, pb::JournalState::Finished),
        entry(&open, pb::JournalState::Running),
    ];
    let (registered, _) = core
        .register(&register("w1", "i1", journal), later(7))
        .await
        .unwrap();
    let decisions: Vec<i32> = registered.decisions.iter().map(|d| d.decision).collect();
    assert_eq!(
        decisions,
        [
            i32::from(pb::AttemptDecision::Ack),
            i32::from(pb::AttemptDecision::Kill)
        ]
    );
    core.disconnected("w1", ts(NOW)).await.unwrap();
    let (registered, _) = core
        .register(
            &register("w2", "i1", vec![entry(&open, pb::JournalState::Running)]),
            later(8),
        )
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Kill)
    );
}

#[tokio::test]
async fn a_worker_silent_past_its_lease_is_lost_and_its_attempts_with_it() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let started = worker_message::Message::Started(pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    });
    core.handle("w1", started, ts(NOW)).await.unwrap();
    core.disconnected("w1", ts(NOW)).await.unwrap();
    core.tick(later(60)).await.unwrap();
    assert!(
        core.dispatcher().workers().get("w1").is_some(),
        "not yet past the lease"
    );
    core.tick(later(61)).await.unwrap();
    assert!(core.dispatcher().workers().get("w1").is_none());
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Lost));
}

#[tokio::test]
async fn deregistering_forgets_the_worker() {
    let mut core = with_dag().await;
    let bye = worker_message::Message::Deregister(pb::Deregister {});
    assert_eq!(core.handle("w1", bye, ts(NOW)).await.unwrap(), []);
    assert!(!core.is_live("w1"));
    assert!(core.dispatcher().workers().get("w1").is_none());
}

#[tokio::test]
async fn uploads_that_dont_read_are_bad_messages() {
    let mut core = core().await;
    let no_root = pb::ParseResult {
        root: None,
        ..upload("j")
    };
    assert_eq!(
        core.upload(no_root, ts(NOW)).await.unwrap_err().to_string(),
        "message from worker `w1`: a parse result without its root"
    );
    let mut bad = upload("j");
    bad.files[0].dag_json.push(b"{}".to_vec());
    assert!(matches!(
        core.upload(bad, ts(NOW)).await.unwrap_err(),
        CoreError::BadMessage { .. }
    ));
}

#[tokio::test]
async fn a_cancel_missed_while_away_is_sent_again_on_reconnect() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let started = worker_message::Message::Started(pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    });
    core.handle("w1", started, ts(NOW)).await.unwrap();
    core.disconnected("w1", ts(NOW)).await.unwrap();
    let runs = core
        .db
        .dag_runs(&DagId::new("sync").unwrap(), None, 1)
        .await
        .unwrap();
    let cancel = schedr_model::commands::Command::CancelRun {
        run_id: runs[0].id.clone(),
    };
    core.db
        .insert_command(&cancel, "anna", ts(NOW))
        .await
        .unwrap();
    core.tick(later(1)).await.unwrap();
    let journal = vec![pb::JournalEntry {
        attempt_id: assign.attempt_id.clone(),
        state: pb::JournalState::Running.into(),
    }];
    let (registered, out) = core
        .register(&register("w1", "i1", journal), later(2))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Continue)
    );
    let cancels: Vec<&str> = out
        .iter()
        .filter_map(|o| match message(o) {
            master_message::Message::CancelAttempt(c) => Some(c.attempt_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(cancels, [assign.attempt_id.as_str()]);
}

async fn registry_state(core: &MasterCore, worker: &str) -> (WorkerState, Option<Timestamp>) {
    let record = core
        .db
        .workers()
        .await
        .unwrap()
        .into_iter()
        .find(|w| w.id == worker)
        .unwrap();
    (record.state, record.offline_at)
}

async fn started_attempt(core: &mut MasterCore) -> String {
    trigger(core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let started = worker_message::Message::Started(pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    });
    core.handle("w1", started, ts(NOW)).await.unwrap();
    assign.attempt_id.clone()
}

#[tokio::test]
async fn the_registry_follows_a_worker_through_its_states_and_keeps_it_when_lost() {
    let mut core = with_dag().await;
    assert_eq!(
        registry_state(&core, "w1").await,
        (WorkerState::Active, None)
    );
    let draining = worker_message::Message::Draining(pb::Draining {});
    core.handle("w1", draining, later(1)).await.unwrap();
    assert_eq!(registry_state(&core, "w1").await.0, WorkerState::Draining);
    core.disconnected("w1", later(2)).await.unwrap();
    assert_eq!(
        registry_state(&core, "w1").await.0,
        WorkerState::Disconnected
    );
    core.tick(later(70)).await.unwrap();
    assert_eq!(
        registry_state(&core, "w1").await,
        (WorkerState::Lost, Some(later(70)))
    );
}

#[tokio::test]
async fn a_deregistered_worker_stays_listed_as_offline() {
    let mut core = with_dag().await;
    let bye = worker_message::Message::Deregister(pb::Deregister {});
    core.handle("w1", bye, later(3)).await.unwrap();
    assert_eq!(
        registry_state(&core, "w1").await,
        (WorkerState::Offline, Some(later(3)))
    );
}

#[tokio::test]
async fn signs_of_life_are_written_at_most_once_per_heartbeat_interval() {
    let mut core = with_dag().await;
    let heartbeat = || worker_message::Message::Heartbeat(pb::Heartbeat::default());
    core.handle("w1", heartbeat(), later(5)).await.unwrap();
    let seen = |core: &MasterCore| {
        let db = core.db.clone();
        async move { db.workers().await.unwrap()[0].last_heartbeat_at }
    };
    assert_eq!(seen(&core).await, ts(NOW));
    core.handle("w1", heartbeat(), later(10)).await.unwrap();
    assert_eq!(seen(&core).await, later(10));
}

#[tokio::test]
async fn a_new_incarnation_ends_the_started_attempts_it_doesnt_report() {
    let mut core = with_dag().await;
    let attempt = started_attempt(&mut core).await;
    core.disconnected("w1", later(1)).await.unwrap();
    core.register(&register("w1", "i2", vec![]), later(5))
        .await
        .unwrap();
    let ended = core.db.attempt(&attempt).await.unwrap().unwrap();
    assert_eq!(ended.status, Some(schedr_model::AttemptStatus::Lost));
    assert!(core.is_live("w1"));
}

#[tokio::test]
async fn a_reconnect_of_the_same_incarnation_keeps_its_attempts() {
    let mut core = with_dag().await;
    let attempt = started_attempt(&mut core).await;
    core.disconnected("w1", later(1)).await.unwrap();
    core.register(&register("w1", "i1", vec![]), later(5))
        .await
        .unwrap();
    let open = core.db.attempt(&attempt).await.unwrap().unwrap();
    assert_eq!(open.status, None);
}

#[tokio::test]
async fn a_new_incarnation_leaves_unstarted_attempts_to_expire() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    core.disconnected("w1", later(1)).await.unwrap();
    core.register(&register("w1", "i2", vec![]), later(2))
        .await
        .unwrap();
    let open = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(open.status, None);
}

#[tokio::test]
async fn a_restarted_master_restores_workers_and_counts_their_lease_from_its_start() {
    let mut first = with_dag().await;
    let attempt = started_attempt(&mut first).await;
    let db = first.db.clone();
    let mut restarted = MasterCore::new(db.clone(), Settings::default());
    assert_eq!(restarted.restore(later(100)).await.unwrap(), 1);
    assert!(!restarted.is_live("w1"));
    assert_eq!(
        registry_state(&restarted, "w1").await.0,
        WorkerState::Disconnected
    );
    restarted.tick(later(160)).await.unwrap();
    assert_eq!(db.attempt(&attempt).await.unwrap().unwrap().status, None);
    restarted.tick(later(161)).await.unwrap();
    assert_eq!(
        db.attempt(&attempt).await.unwrap().unwrap().status,
        Some(schedr_model::AttemptStatus::Lost)
    );
    assert_eq!(registry_state(&restarted, "w1").await.0, WorkerState::Lost);
    let mut again = MasterCore::new(db, Settings::default());
    assert_eq!(
        again.restore(later(200)).await.unwrap(),
        0,
        "lost workers stay lost"
    );
}

#[tokio::test]
async fn a_registration_that_fails_half_way_leaves_no_live_stream() {
    let mut core = core().await;
    let Database::Sqlite(pool) = &core.db else {
        unreachable!()
    };
    sqlx::query("DROP TABLE workers")
        .execute(pool)
        .await
        .unwrap();
    assert!(
        core.register(&register("w1", "i1", vec![]), ts(NOW))
            .await
            .is_err()
    );
    assert!(
        !core.is_live("w1"),
        "the next registration must not be refused"
    );
}

#[tokio::test]
async fn tasks_go_to_the_worker_with_the_newest_code_until_it_leaves() {
    let mut core = with_dag().await;
    let w1 = core.dispatcher().workers().get("w1").unwrap();
    assert_eq!(w1.version, Some(SchedrVersion::new(0, 1)));
    assert_eq!(w1.roots.get("etl").map(String::as_str), Some("h1"));
    let mut newer = register("w2", "i1", vec![]);
    newer.roots = vec![root("h2")];
    let (_, outgoing) = core.register(&newer, later(60)).await.unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    let parsed = pb::ParseResult {
        worker_id: "w2".to_owned(),
        root: Some(root("h2")),
        ..upload(&job.parse_job_id)
    };
    core.upload(parsed, later(60)).await.unwrap();
    trigger(&mut core).await;
    let out = core.tick(later(60)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    assert_eq!(
        (
            out[0].worker_id.as_str(),
            assign.expected_code_hash.as_str()
        ),
        ("w2", "h2")
    );
    // w2 leaves (its attempt is lost, no retries left): the next run's task goes to w1, on h1.
    let bye = worker_message::Message::Deregister(pb::Deregister {});
    core.handle("w2", bye, later(61)).await.unwrap();
    trigger(&mut core).await;
    let out = core.tick(later(62)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    assert_eq!(
        (
            out[0].worker_id.as_str(),
            assign.expected_code_hash.as_str()
        ),
        ("w1", "h1")
    );
}

#[tokio::test]
async fn a_drained_worker_stays_drained_when_it_registers_again() {
    let mut core = with_dag().await;
    core.db.set_worker_drained("w1", true).await.unwrap();
    core.disconnected("w1", later(1)).await.unwrap();
    core.register(&register("w1", "i2", vec![]), later(1))
        .await
        .unwrap();
    assert!(core.dispatcher().workers().get("w1").unwrap().drained);
    trigger(&mut core).await;
    let out = core.tick(later(2)).await.unwrap();
    assert!(
        !out.iter()
            .any(|o| matches!(message(o), master_message::Message::AssignTask(_))),
        "{out:?}"
    );
}

#[tokio::test]
async fn a_worker_saying_it_drains_hands_its_parse_jobs_over() {
    let mut core = core().await;
    core.register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    core.register(&register("w2", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let draining = worker_message::Message::Draining(pb::Draining {});
    let out = core.handle("w1", draining, ts(NOW)).await.unwrap();
    let master_message::Message::AssignParse(job) = message(&out[0]) else {
        panic!("expected AssignParse, got {out:?}")
    };
    assert_eq!(out[0].worker_id, "w2");
    assert_eq!(job.root.as_ref().unwrap().content_hash, "h1");
}

#[test]
fn arrivals_keep_the_latest_per_worker_until_forgotten() {
    let arrivals = Arrivals::default();
    let at = |s: &str| -> Timestamp { s.parse().unwrap() };
    assert_eq!(arrivals.last("w1"), None);
    arrivals.arrived("w1", at("2026-10-23T08:00:10Z"));
    // A message read later can't carry an earlier time back.
    arrivals.arrived("w1", at("2026-10-23T08:00:05Z"));
    arrivals.clone().arrived("w2", at("2026-10-23T08:00:20Z"));
    assert_eq!(arrivals.last("w1"), Some(at("2026-10-23T08:00:10Z")));
    assert_eq!(arrivals.last("w2"), Some(at("2026-10-23T08:00:20Z")));
    arrivals.forget("w1");
    assert_eq!(arrivals.last("w1"), None);
    assert_eq!(arrivals.last("w2"), Some(at("2026-10-23T08:00:20Z")));
}

#[tokio::test]
async fn a_partial_tick_returns_its_messages_with_the_result() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let (out, result) = core.tick_partial(ts(NOW)).await;
    assert!(result.is_ok(), "{result:?}");
    assert!(
        matches!(message(&out[0]), master_message::Message::AssignTask(_)),
        "{out:?}"
    );
}
