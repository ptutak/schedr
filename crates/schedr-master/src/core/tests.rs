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
            "schedule": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
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
    core.disconnected("w1");
    assert!(!core.is_live("w1"));
    core.register(&register("w1", "i2", vec![]), ts(NOW))
        .await
        .unwrap();
    core.disconnected("ghost");
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
    core.disconnected("w1");
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
    core.disconnected("w1");
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
    core.disconnected("w1");
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
    core.disconnected("w1");
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
    core.disconnected("w1");
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
