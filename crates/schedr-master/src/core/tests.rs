use pretty_assertions::assert_eq;
use schedr_model::DagId;
use schedr_model::commands::CommandStatus;
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
        running_triggers: Vec::new(),
        watcher_slots: 500,
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
        process_id: "p-1".to_owned(),
        config_json: r#"{"worker":{"slots":2}}"#.to_owned(),
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
    upload_of(job_id, &dag_file())
}

fn upload_of(job_id: &str, file: &ParsedFile) -> pb::ParseResult {
    pb::ParseResult {
        parse_job_id: job_id.to_owned(),
        worker_id: "w1".to_owned(),
        root: Some(root("h1")),
        schedr_version: "0.1.0".to_owned(),
        files: vec![parsed_file_to_proto(file)],
    }
}

/// [`dag_file`], `extract` with a `cleanup_anywhere` cleanup.
fn anywhere_file() -> ParsedFile {
    let mut file = serde_json::to_value(dag_file()).unwrap();
    file["dags"][0]["tasks"][0]["cleanup"] = json!({
        "run": "python", "retries": 0, "retry_delay_ms": 0, "anywhere": true
    });
    serde_json::from_value(file).unwrap()
}

fn message(outgoing: &Outgoing) -> &master_message::Message {
    outgoing.message.message.as_ref().unwrap()
}

/// Registers `w1`, parses its root and stores DAG `sync`; returns the core.
async fn with_dag() -> MasterCore {
    with_dag_of(&dag_file()).await
}

/// As [`with_dag`], the root parsed as `file`.
async fn with_dag_of(file: &ParsedFile) -> MasterCore {
    let mut core = core().await;
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    core.upload(upload_of(&job.parse_job_id, file), ts(NOW))
        .await
        .unwrap();
    core
}

async fn trigger(core: &mut MasterCore) {
    let command = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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
    // No queues configured: it serves `default`, and is stored so (the Workers page showed
    // none).
    let stored = core.db.workers().await.unwrap();
    assert_eq!(stored[0].queues, ["default"]);
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
    // A script root is read for its `schedr.toml` files (M4.9): a parse job of its kind.
    let out = core.handle("w1", scripts, ts(NOW)).await.unwrap();
    let master_message::Message::AssignParse(job) = message(&out[0]) else {
        panic!("expected AssignParse")
    };
    assert_eq!(job.root.as_ref().unwrap().kind(), pb::RootKind::Script);
    let unknown = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
        root: Some(pb::RootHash {
            kind: pb::RootKind::Unspecified.into(),
            ..root("h4")
        }),
    });
    assert_eq!(core.handle("w1", unknown, ts(NOW)).await.unwrap(), []);
    let empty = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged { root: None });
    assert_eq!(core.handle("w1", empty, ts(NOW)).await.unwrap(), []);
}

/// Makes the database refuse writes matching `when` (an SQLite trigger on `table`), as a
/// failing disk or a lost connection would partway through a piece of work.
async fn refuse(core: &MasterCore, table: &str, when: &str) {
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TRIGGER refuse_{table} BEFORE UPDATE ON {table} WHEN {when} \
         BEGIN SELECT RAISE(ABORT, 'refused by the test'); END"
    )))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn a_finished_message_is_stored_whole_or_not_at_all() {
    // T1 (2.10b): a worker message's writes were each their own transaction; a failure
    // partway left the first stored (the attempt ended, its run not finished).
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    let started = worker_message::Message::Started(pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    });
    core.handle("w1", started, ts(NOW)).await.unwrap();
    // The run's own write fails: the last of the `Finished`'s writes (and so does the run's
    // failure the master stores instead of a refused result, B106).
    refuse(&core, "runs", "NEW.state IN ('success', 'failed')").await;
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: b"null".to_vec(),
        ..pb::Finished::default()
    });
    assert!(core.handle("w1", finished, ts(NOW)).await.is_err());
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None, "a part of the `Finished` was stored");
    // Nor in memory (code review): the attempt still runs, so its slot is still taken.
    let w1 = core.dispatcher().workers().get("w1").unwrap();
    assert_eq!(
        w1.free_slots(),
        w1.slots - 1,
        "the slot was freed in memory"
    );
}

#[tokio::test]
async fn a_result_the_database_refuses_is_stored_as_the_attempts_failure() {
    // B106: refused for its data, it was refused again after every reconnect, the worker's
    // later results stuck behind it.
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    refuse(&core, "runs", "NEW.state = 'success'").await;
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: b"null".to_vec(),
        duration_ms: Some(1500),
        ..pb::Finished::default()
    });
    let ack = core.handle("w1", finished, ts(NOW)).await.unwrap();
    assert!(matches!(
        message(&ack[0]),
        master_message::Message::FinishedAck(_)
    ));
    let runs = core
        .db
        .dag_runs(&DagId::new("sync").unwrap(), None, 10)
        .await
        .unwrap();
    // Not retried: the same result would be refused again.
    assert_eq!(runs[0].state, schedr_model::RunState::Failed);
    let extract = schedr_model::TaskId::new("extract").unwrap();
    let attempts = core
        .db
        .instance_attempts(&runs[0].id, &extract, -1)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].status,
        Some(schedr_model::AttemptStatus::Failed)
    );
    assert_eq!(attempts[0].error_kind.as_deref(), Some("result_not_stored"));
    assert_eq!(attempts[0].duration_ms, Some(1500));
    let summary = attempts[0].error_summary.as_deref().unwrap_or_default();
    assert!(
        summary.contains("the result couldn't be stored")
            && summary.contains("refused by the test"),
        "{summary}"
    );
}

#[tokio::test]
async fn a_refused_results_failure_keeps_nothing_that_would_be_refused_again() {
    // B127: the fallback kept the worker's code hash as it was; one holding NUL (which
    // Postgres refuses, stood in for here by a trigger) refused the fallback too, and the
    // `Finished` was dropped unacknowledged, its attempt left running.
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    refuse(&core, "attempts", "instr(NEW.code_hash, char(0)) > 0").await;
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: b"null".to_vec(),
        code_hash: "sha256-ab\0cd".to_owned(),
        ..pb::Finished::default()
    });
    let ack = core.handle("w1", finished, ts(NOW)).await.unwrap();
    assert!(matches!(
        message(&ack[0]),
        master_message::Message::FinishedAck(_)
    ));
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Failed));
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
async fn what_a_worker_says_of_its_hosts_is_kept_for_people() {
    let mut core = with_dag().await;
    let host = pb::WatcherHost {
        code: "abc".to_owned(),
        wait_steps: 2,
        threads: 31,
        tied_threads: 1,
        memory_bytes: 1024,
        hosts_lost: 3,
    };
    let beat = |hosts: Vec<pb::WatcherHost>| {
        worker_message::Message::Heartbeat(pb::Heartbeat {
            watcher_hosts: hosts,
            ..pb::Heartbeat::default()
        })
    };
    core.handle("w1", beat(vec![host.clone()]), later(1))
        .await
        .unwrap();
    let stored = core.db.workers().await.unwrap();
    assert_eq!(stored[0].watcher_hosts.len(), 1);
    assert_eq!(stored[0].watcher_hosts[0].code, "abc");
    assert_eq!(stored[0].watcher_hosts[0].wait_steps, 2);
    assert_eq!(stored[0].watcher_hosts[0].threads, 31);
    assert_eq!(stored[0].watcher_hosts[0].tied_threads, 1);
    assert_eq!(stored[0].watcher_hosts[0].memory_bytes, 1024);
    assert_eq!(stored[0].watcher_hosts[0].hosts_lost, 3);
    let tied_past_its_threads = pb::WatcherHost {
        tied_threads: 99,
        ..host.clone()
    };
    core.handle("w1", beat(vec![tied_past_its_threads]), later(2))
        .await
        .unwrap();
    let stored = core.db.workers().await.unwrap();
    assert_eq!(stored[0].watcher_hosts[0].tied_threads, 31);
    core.handle("w1", beat(Vec::new()), later(3)).await.unwrap();
    assert_eq!(core.db.workers().await.unwrap()[0].watcher_hosts, []);
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
async fn a_workers_warnings_replace_the_last_ones() {
    let mut core = with_dag().await;
    let report = |messages: &[&str]| {
        worker_message::Message::Warnings(pb::Warnings {
            warnings: messages
                .iter()
                .map(|m| pb::WorkerWarning {
                    kind: "fork_server_disabled".to_owned(),
                    interpreter: "python".to_owned(),
                    message: (*m).to_owned(),
                })
                .collect(),
        })
    };
    assert_eq!(
        core.handle("w1", report(&["a", "b"]), later(1))
            .await
            .unwrap(),
        []
    );
    let stored = |core: &MasterCore| {
        let db = core.db.clone();
        async move {
            let workers = db.workers().await.unwrap();
            let w1 = workers.iter().find(|w| w.id == "w1").unwrap();
            w1.warnings
                .iter()
                .map(|w| w.message.clone())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(stored(&core).await, ["a", "b"]);
    core.handle("w1", report(&[]), later(2)).await.unwrap();
    assert_eq!(stored(&core).await, Vec::<String>::new());
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
        handles: 0,
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
async fn a_registration_goes_on_when_adopting_an_attempt_is_refused_for_its_data() {
    // B128: the database refusing an open attempt's start, for its data, refused every
    // registration, so the worker could never send that attempt's `Finished` again.
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    core.disconnected("w1", ts(NOW)).await.unwrap();
    refuse(
        &core,
        "attempts",
        "NEW.started_at IS NOT NULL AND OLD.started_at IS NULL AND NEW.status IS NULL",
    )
    .await;
    let journal = vec![pb::JournalEntry {
        handles: 0,
        attempt_id: assign.attempt_id.clone(),
        state: pb::JournalState::Finished.into(),
    }];
    let (registered, _) = core
        .register(&register("w1", "i1", journal), later(5))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Continue)
    );
    // The worker runs it still, as far as the master knows: its slot is taken.
    let w1 = core.dispatcher().workers().get("w1").unwrap();
    assert_eq!(w1.free_slots(), w1.slots - 1);
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: b"null".to_vec(),
        ..pb::Finished::default()
    });
    core.handle("w1", finished, later(6)).await.unwrap();
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Success));
}

#[tokio::test]
async fn an_attempt_adopted_unrecorded_isnt_taken_back_while_its_worker_runs_it() {
    // Codex's review of B128: the start left unrecorded, the attempt counted as never started
    // and expired while the worker ran it, and its task was assigned again (run twice).
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    core.disconnected("w1", ts(NOW)).await.unwrap();
    refuse(
        &core,
        "attempts",
        "NEW.started_at IS NOT NULL AND OLD.started_at IS NULL AND NEW.status IS NULL",
    )
    .await;
    let journal = vec![pb::JournalEntry {
        handles: 0,
        attempt_id: assign.attempt_id.clone(),
        state: pb::JournalState::Running.into(),
    }];
    core.register(&register("w1", "i1", journal), later(5))
        .await
        .unwrap();
    let out = core.tick(later(11)).await.unwrap();
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None, "{out:?}");
    assert!(
        !out.iter()
            .any(|o| matches!(message(o), master_message::Message::AssignTask(_))),
        "{out:?}"
    );
    // Once its worker is lost, it is taken back as an attempt never started.
    core.disconnected("w1", later(12)).await.unwrap();
    core.tick(later(73)).await.unwrap();
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(
        attempt.status,
        Some(schedr_model::AttemptStatus::AssignmentExpired)
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
async fn a_cleanup_that_ended_while_its_worker_was_away_counts_when_it_comes_back() {
    // Codex's review of decision 282: the journal's `Finished` of a cleanup waiting to move
    // was answered `Kill`, the worker dropped the result, and the cleanup ran again elsewhere.
    let mut core = with_dag_of(&anywhere_file()).await;
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
    core.tick(later(61)).await.unwrap();
    let waiting = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!((waiting.worker_id.as_str(), waiting.status), ("", None));
    let journal = vec![pb::JournalEntry {
        handles: 0,
        attempt_id: assign.attempt_id.clone(),
        state: pb::JournalState::Finished.into(),
    }];
    let (registered, _) = core
        .register(&register("w1", "i1", journal), later(62))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Continue),
        "sent again, not dropped"
    );
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: assign.attempt_id.clone(),
        status: pb::AttemptStatus::Lost.into(),
        cleanup: Some(pb::CleanupResult {
            status: pb::CleanupStatus::Ok.into(),
            tries: 1,
            ..pb::CleanupResult::default()
        }),
        ..pb::Finished::default()
    });
    core.handle("w1", finished, later(63)).await.unwrap();
    let ended = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(ended.status, Some(schedr_model::AttemptStatus::Lost));
    assert_eq!(
        core.db.waiting_cleanups(later(63)).await.unwrap().len(),
        0,
        "it ran: it doesn't move"
    );
}

/// Assigns `sync`'s task with an anywhere cleanup to `w1`, started; `w1` is then lost and
/// its cleanup waits to move. Returns the core and the attempt's ID.
async fn moved_away_from_w1() -> (MasterCore, String) {
    let mut core = with_dag_of(&anywhere_file()).await;
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
    core.tick(later(61)).await.unwrap();
    let waiting = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!((waiting.worker_id.as_str(), waiting.status), ("", None));
    (core, assign.attempt_id.clone())
}

fn running(attempt_id: &str, handles: u32) -> Vec<pb::JournalEntry> {
    vec![pb::JournalEntry {
        attempt_id: attempt_id.to_owned(),
        state: pb::JournalState::Running.into(),
        handles,
    }]
}

fn finished_with_cleanup(
    attempt_id: &str,
    status: pb::AttemptStatus,
    cleanup: pb::CleanupStatus,
) -> worker_message::Message {
    worker_message::Message::Finished(pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: status.into(),
        cleanup: Some(pb::CleanupResult {
            status: cleanup.into(),
            tries: 1,
            ..pb::CleanupResult::default()
        }),
        ..pb::Finished::default()
    })
}

#[tokio::test]
async fn a_waiting_cleanup_goes_back_to_its_worker_holding_handles_the_master_lacks() {
    // B169: cut off past its lease, the worker registered a handle the master never got; the
    // cleanup moved with none. Back while the move waits, it is the only one with them all.
    let (mut core, attempt_id) = moved_away_from_w1().await;
    let (registered, _) = core
        .register(&register("w1", "i1", running(&attempt_id, 1)), later(62))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Continue)
    );
    let back = core.db.attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(
        (
            back.worker_id.as_str(),
            back.cleanup_moves,
            back.result_kept
        ),
        ("w1", 0, false),
        "the move is undone, its `lost` result dropped"
    );
    assert!(
        core.db
            .waiting_cleanups(later(600))
            .await
            .unwrap()
            .is_empty(),
        "no other worker gets it"
    );
    let done = finished_with_cleanup(
        &attempt_id,
        pb::AttemptStatus::Success,
        pb::CleanupStatus::Ok,
    );
    core.handle("w1", done, later(63)).await.unwrap();
    let ended = core.db.attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(
        ended.status,
        Some(schedr_model::AttemptStatus::Success),
        "its own result counts"
    );
}

#[tokio::test]
async fn a_worker_back_with_handles_a_moved_cleanup_lacked_cleans_them_and_adds_its_result() {
    // B169: the move ran elsewhere without the handle the master never got.
    let (mut core, attempt_id) = moved_away_from_w1().await;
    let (_, out) = core
        .register(&register("w2", "i2", vec![]), later(62))
        .await
        .unwrap();
    assert!(out.is_empty(), "{out:?}");
    // Alive past its first lease, when the moved cleanup is given to it.
    let heartbeat = worker_message::Message::Heartbeat(pb::Heartbeat::default());
    core.handle("w2", heartbeat, later(100)).await.unwrap();
    let out = core.tick(later(125)).await.unwrap();
    assert!(
        out.iter()
            .any(|o| o.worker_id == "w2"
                && matches!(message(o), master_message::Message::AssignTask(_))),
        "the moved cleanup goes to w2: {out:?}"
    );
    let not_needed = finished_with_cleanup(
        &attempt_id,
        pb::AttemptStatus::Lost,
        pb::CleanupStatus::NotNeeded,
    );
    core.handle("w2", not_needed, later(126)).await.unwrap();
    let ended = core.db.attempt(&attempt_id).await.unwrap().unwrap();
    assert_eq!(ended.status, Some(schedr_model::AttemptStatus::Lost));
    // Nothing missing: nothing to clean there.
    let (registered, _) = core
        .register(&register("w1", "i1", running(&attempt_id, 0)), later(127))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Kill)
    );
    core.disconnected("w1", later(128)).await.unwrap();
    let (registered, _) = core
        .register(&register("w1", "i3", running(&attempt_id, 2)), later(128))
        .await
        .unwrap();
    assert_eq!(
        (
            registered.decisions[0].decision,
            registered.decisions[0].cleanup_from
        ),
        (i32::from(pb::AttemptDecision::CleanRest), 0)
    );
    let rest = finished_with_cleanup(
        &attempt_id,
        pb::AttemptStatus::Lost,
        pb::CleanupStatus::Failed,
    );
    core.handle("w1", rest, later(129)).await.unwrap();
    let run = core.db.run_tasks(&ended.run_id).await.unwrap().unwrap();
    let instance = run
        .instances
        .iter()
        .find(|i| i.id == ended.instance_id)
        .unwrap();
    assert_eq!(
        instance.cleanup_result,
        Some(schedr_model::CleanupResult::Failed),
        "the rest's failure is the instance's"
    );
    // Added once: sent again, or reported again in the journal, it changes nothing.
    assert!(
        !core
            .db
            .add_rest_cleanup(&attempt_id, &rest_end())
            .await
            .unwrap()
    );
    core.disconnected("w1", later(130)).await.unwrap();
    let journal = vec![pb::JournalEntry {
        attempt_id: attempt_id.clone(),
        state: pb::JournalState::Finished.into(),
        handles: 2,
    }];
    let (registered, _) = core
        .register(&register("w1", "i4", journal), later(130))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Kill)
    );
}

#[tokio::test]
async fn a_failed_rest_after_the_runs_success_makes_it_a_yellow_success() {
    // Decision 309 (Codex's review of B169): the run ended `success` before the worker it
    // moved from came back; its rest's failure doesn't change the run's state, it marks it.
    let mut core = with_dag_of(&anywhere_file()).await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let attempt_id = assign.attempt_id.clone();
    let started = worker_message::Message::Started(pb::Started {
        attempt_id: attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    });
    core.handle("w1", started, ts(NOW)).await.unwrap();
    // The task succeeded; w1 was cut off while it cleaned up.
    let ended = worker_message::Message::TaskEnded(pb::TaskEnded {
        result: Some(pb::Finished {
            attempt_id: attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        }),
    });
    core.handle("w1", ended, ts(NOW)).await.unwrap();
    core.disconnected("w1", ts(NOW)).await.unwrap();
    core.tick(later(61)).await.unwrap();
    core.register(&register("w2", "i2", vec![]), later(62))
        .await
        .unwrap();
    let heartbeat = worker_message::Message::Heartbeat(pb::Heartbeat::default());
    core.handle("w2", heartbeat, later(100)).await.unwrap();
    core.tick(later(125)).await.unwrap();
    let moved = finished_with_cleanup(
        &attempt_id,
        pb::AttemptStatus::Lost,
        pb::CleanupStatus::NotNeeded,
    );
    core.handle("w2", moved, later(126)).await.unwrap();
    core.tick(later(127)).await.unwrap();
    let attempt = core.db.attempt(&attempt_id).await.unwrap().unwrap();
    let run_id = attempt.run_id.clone();
    let db = core.db.clone();
    let state = || async {
        let run = db.run_record(&run_id).await.unwrap().unwrap();
        (run.state, run.late_failure)
    };
    assert_eq!(state().await, (schedr_model::RunState::Success, None));
    let (registered, _) = core
        .register(&register("w1", "i3", running(&attempt_id, 1)), later(128))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::CleanRest)
    );
    let mut rest = pb::Finished {
        attempt_id: attempt_id.clone(),
        status: pb::AttemptStatus::Lost.into(),
        cleanup: Some(pb::CleanupResult {
            status: pb::CleanupStatus::Failed.into(),
            tries: 1,
            rest: true,
            ..pb::CleanupResult::default()
        }),
        ..pb::Finished::default()
    };
    core.handle(
        "w1",
        worker_message::Message::Finished(rest.clone()),
        later(129),
    )
    .await
    .unwrap();
    assert_eq!(
        state().await,
        (
            schedr_model::RunState::Success,
            Some("late_cleanup_failed".to_owned())
        ),
        "still a success, marked"
    );
    // A rest said to be one never counts as the whole cleanup, even from the attempt's worker.
    rest.cleanup.as_mut().unwrap().status = pb::CleanupStatus::Ok.into();
    core.handle("w1", worker_message::Message::Finished(rest), later(130))
        .await
        .unwrap();
    assert_eq!(
        state().await.1.as_deref(),
        Some("late_cleanup_failed"),
        "added once"
    );
}

fn rest_end() -> schedr_db::attempts::CleanupEnd {
    schedr_db::attempts::CleanupEnd {
        result: schedr_model::CleanupResult::Ok,
        tries: 1,
        duration_ms: 1,
        error_kind: None,
        error_summary: None,
    }
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
        handles: 0,
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

#[tokio::test]
async fn a_task_cancelled_while_its_worker_is_away_is_cancelled_on_reconnect() {
    // B43: a `CancelTask` (not the run's) missed while away is sent again too.
    let mut core = with_dag().await;
    let attempt = started_attempt(&mut core).await;
    core.disconnected("w1", ts(NOW)).await.unwrap();
    let runs = core
        .db
        .dag_runs(&DagId::new("sync").unwrap(), None, 1)
        .await
        .unwrap();
    let cancel = schedr_model::commands::Command::CancelTask {
        run_id: runs[0].id.clone(),
        task_id: schedr_model::TaskId::new("extract").unwrap(),
        map_index: None,
    };
    core.db
        .insert_command(&cancel, "anna", ts(NOW))
        .await
        .unwrap();
    core.tick(later(1)).await.unwrap();
    let journal = vec![pb::JournalEntry {
        handles: 0,
        attempt_id: attempt.clone(),
        state: pb::JournalState::Running.into(),
    }];
    let (_, out) = core
        .register(&register("w1", "i1", journal), later(2))
        .await
        .unwrap();
    let cancels: Vec<&str> = out
        .iter()
        .filter_map(|o| match message(o) {
            master_message::Message::CancelAttempt(c) => Some(c.attempt_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(cancels, [attempt.as_str()]);
}

#[tokio::test]
async fn an_attempt_running_on_reconnect_whose_start_was_lost_is_not_assigned_again() {
    // B50: `Started` lost with the stream; the journal says it runs, so it has started.
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    core.disconnected("w1", later(1)).await.unwrap();
    let journal = vec![pb::JournalEntry {
        handles: 0,
        attempt_id: assign.attempt_id.clone(),
        state: pb::JournalState::Running.into(),
    }];
    core.register(&register("w1", "i1", journal), later(2))
        .await
        .unwrap();
    let out = core.tick(later(30)).await.unwrap();
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None, "still running, not expired");
    assert!(attempt.started);
    assert!(
        !out.iter()
            .any(|o| matches!(message(o), master_message::Message::AssignTask(_))),
        "{out:?}"
    );
}

#[tokio::test]
async fn a_start_or_result_that_couldnt_be_stored_keeps_the_attempt_from_being_taken_back() {
    // The worker runs it: a failed write of its `Started` (or `Finished`) mustn't let the
    // attempt expire as never started and run again elsewhere.
    for result in [false, true] {
        let mut core = with_dag().await;
        trigger(&mut core).await;
        let out = core.tick(ts(NOW)).await.unwrap();
        let master_message::Message::AssignTask(assign) = message(&out[0]) else {
            panic!("expected AssignTask")
        };
        let id = assign.attempt_id.clone();
        let reported = if result {
            worker_message::Message::Finished(pb::Finished {
                attempt_id: id.clone(),
                status: pb::AttemptStatus::Success.into(),
                ..pb::Finished::default()
            })
        } else {
            worker_message::Message::Started(pb::Started {
                attempt_id: id.clone(),
                start_method: pb::StartMethod::Spawn.into(),
            })
        };
        // As the stream's reader records it on arrival.
        core.arrivals().start_arrived("w1", &id);
        core.db.fail_write(1);
        assert!(core.handle("w1", reported, later(1)).await.is_err());
        core.db.count_writes();
        let out = core.tick(later(30)).await.unwrap();
        let attempt = core.db.attempt(&id).await.unwrap().unwrap();
        assert_eq!(attempt.status, None, "taken back (result: {result})");
        assert!(
            !out.iter()
                .any(|o| matches!(message(o), master_message::Message::AssignTask(_))),
            "{out:?}"
        );
    }
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
    let schedr_db::database::Pool::Sqlite(pool) = core.db.pool() else {
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

#[test]
fn arrivals_hold_an_arrived_start_until_it_is_handled() {
    let arrivals = Arrivals::default();
    assert!(!arrivals.start_waiting("a1"));
    arrivals.clone().start_arrived("w1", "a1");
    assert!(arrivals.start_waiting("a1"));
    assert!(!arrivals.start_waiting("a2"));
    arrivals.start_handled("a1");
    assert!(!arrivals.start_waiting("a1"));
    // A worker forgotten (lost, gone) takes its waiting starts with it.
    arrivals.start_arrived("w1", "a3");
    arrivals.start_arrived("w2", "a4");
    arrivals.forget("w1");
    assert!(!arrivals.start_waiting("a3"));
    assert!(arrivals.start_waiting("a4"));
    // Handled twice (a `Started`, then its `Finished`): nothing left, nothing wrong.
    arrivals.start_handled("a1");
    assert!(!arrivals.start_waiting("a1"));
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

/// The fault-injection harness's scenarios at the core's level (decision 98).
mod faults {
    use super::*;
    use crate::dispatch::Dispatcher;
    use crate::faults::{Scenario, fail_each_write};

    /// `w1` runs `extract`, then goes silent: the tick after its lease runs out forgets it
    /// (`Core::forget`: its state stored, its attempts ended, its connection dropped).
    struct LeaseRunsOut {
        core: MasterCore,
        run_id: String,
    }

    impl Scenario for LeaseRunsOut {
        async fn setup() -> Self {
            let mut core = with_dag().await;
            trigger(&mut core).await;
            let out = core.tick(ts(NOW)).await.unwrap();
            let master_message::Message::AssignTask(assign) = message(&out[0]) else {
                panic!("expected AssignTask, got {out:?}")
            };
            let started = worker_message::Message::Started(pb::Started {
                attempt_id: assign.attempt_id.clone(),
                start_method: pb::StartMethod::Spawn.into(),
            });
            core.handle("w1", started, ts(NOW)).await.unwrap();
            let run_id = assign.run.as_ref().unwrap().run_id.clone();
            Self { core, run_id }
        }
        async fn run(&mut self) {
            let lease = SignedDuration::try_from(self.core.settings.lease_timeout).unwrap();
            let after = ts(NOW)
                .checked_add(lease)
                .unwrap()
                .checked_add(SignedDuration::from_secs(1))
                .unwrap();
            let _ = self.core.tick(after).await;
        }
        /// The next tick after the lease again: a worker its failure left behind is forgotten.
        async fn recover(&mut self) {
            let lease = SignedDuration::try_from(self.core.settings.lease_timeout).unwrap();
            let later = ts(NOW)
                .checked_add(lease * 2)
                .unwrap()
                .checked_add(SignedDuration::from_secs(2))
                .unwrap();
            let _ = self.core.tick(later).await;
        }
        fn db(&self) -> &Database {
            &self.core.db
        }
        fn run_id(&self) -> &str {
            &self.run_id
        }
        fn dispatcher(&self) -> &Dispatcher {
            &self.core.dispatcher
        }
        /// After the next round, `w1` is forgotten for good: stored lost, its connection gone
        /// (B42: a failure must be retried, not left; a worker stored active keeps its
        /// connection until then, or nothing would retry it).
        async fn extra(&self) -> Vec<String> {
            let workers = self.core.db.workers().await.unwrap();
            let mut found: Vec<String> = workers
                .iter()
                .filter(|w| !matches!(w.state, WorkerState::Lost | WorkerState::Offline))
                .filter(|w| !self.core.connections.contains_key(&w.id))
                .map(|w| {
                    format!(
                        "worker `{}` stored {:?} with no connection left",
                        w.id, w.state
                    )
                })
                .collect();
            let w1 = workers.iter().find(|w| w.id == "w1").unwrap();
            let connected = self.core.connections.contains_key("w1");
            if w1.state != WorkerState::Lost || connected {
                found.push(format!(
                    "`w1` not forgotten after the next round: {:?}, connected {connected}",
                    w1.state
                ));
            }
            found
        }
    }

    #[tokio::test]
    async fn a_lease_running_out_leaves_nothing_half_done_whichever_write_fails() {
        let (writes, found) = Box::pin(fail_each_write::<LeaseRunsOut>()).await;
        assert!(found.is_empty(), "{writes} writes:\n{}", found.join("\n"));
    }
}

#[tokio::test]
async fn a_code_switch_rolled_back_is_recorded_when_the_worker_reports_it_again() {
    // B91: discovery kept a rolled-back switch in memory, so the retry (the worker registering
    // again) recorded nothing, and a restarted master undid the switch.
    let mut core = with_dag().await;
    let switch = |hash: &str| {
        worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
            root: Some(root(hash)),
        })
    };
    let out = core.handle("w1", switch("h2"), later(1)).await.unwrap();
    let master_message::Message::AssignParse(job) = message(&out[0]) else {
        panic!("expected AssignParse, got {out:?}")
    };
    let mut parsed = upload(&job.parse_job_id);
    parsed.root = Some(root("h2"));
    core.upload(parsed, later(2)).await.unwrap();
    // A revert to h1, whose second write (h1 current again) fails: rolled back whole.
    core.db.fail_write(2);
    core.handle("w1", switch("h1"), later(3)).await.unwrap_err();
    // Its link drops; it registers again, at h1.
    core.disconnected("w1", later(4)).await.unwrap();
    core.register(&register("w1", "i1", vec![]), later(5))
        .await
        .unwrap();
    let switches = core.db.root_hash_switches("etl").await.unwrap();
    assert!(switches["h1"] > switches["h2"], "{switches:?}");
}

#[tokio::test]
async fn a_revert_in_a_registration_that_failed_is_recorded_when_it_registers_again() {
    // Codex's review of B91: registration dropped the worker's earlier reports before its
    // writes; failing, it left them dropped, so the retry didn't see the revert as a switch.
    let mut core = with_dag().await;
    let out = core
        .handle(
            "w1",
            worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
                root: Some(root("h2")),
            }),
            later(1),
        )
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&out[0]) else {
        panic!("expected AssignParse, got {out:?}")
    };
    let mut parsed = upload(&job.parse_job_id);
    parsed.root = Some(root("h2"));
    core.upload(parsed, later(2)).await.unwrap();
    // w1 reverted to h1 while away; its registration's switch can't be stored this time.
    core.disconnected("w1", later(3)).await.unwrap();
    refuse(&core, "root_hashes", "NEW.content_hash = 'h1'").await;
    core.register(&register("w1", "i1", vec![]), later(4))
        .await
        .unwrap_err();
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query("DROP TRIGGER refuse_root_hashes")
        .execute(pool)
        .await
        .unwrap();
    core.disconnected("w1", later(5)).await.unwrap();
    core.register(&register("w1", "i1", vec![]), later(6))
        .await
        .unwrap();
    let switches = core.db.root_hash_switches("etl").await.unwrap();
    assert!(switches["h1"] > switches["h2"], "{switches:?}");
}

async fn rename_table(core: &MasterCore, from: &str, to: &str) {
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {from} RENAME TO {to}"
    )))
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn a_resume_whose_follow_up_failed_is_retried_in_later_rounds() {
    // B99: the resume command is applied once; its follow-up (the worker's parse jobs) must
    // be tried again until it succeeds.
    let mut core = core().await;
    core.register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let db = core.db.clone();
    let ask = |command| {
        let db = db.clone();
        async move { db.insert_command(&command, "anna", ts(NOW)).await.unwrap() }
    };
    ask(schedr_model::commands::Command::DrainWorker {
        worker_id: "w1".to_owned(),
    })
    .await;
    core.tick(later(1)).await.unwrap();
    ask(schedr_model::commands::Command::ResumeWorker {
        worker_id: "w1".to_owned(),
    })
    .await;
    rename_table(&core, "parse_results", "parse_results_away").await;
    let (out, _) = core.tick_partial(later(2)).await;
    assert!(
        !out.iter()
            .any(|o| matches!(message(o), master_message::Message::AssignParse(_))),
        "{out:?}"
    );
    rename_table(&core, "parse_results_away", "parse_results").await;
    let mut assigned = false;
    for round in 3..6 {
        let out = core.tick(later(round)).await.unwrap();
        assigned |= out.iter().any(|o| {
            o.worker_id == "w1" && matches!(message(o), master_message::Message::AssignParse(_))
        });
    }
    assert!(assigned, "the resumed worker got no parse job");
}

/// Registers `w1` and parses a root defining connection `db`; returns the core.
async fn with_connection() -> MasterCore {
    let mut core = core().await;
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    let file: ParsedFile = serde_json::from_value(json!({
        "path": "conns.py", "dags": [], "error": null,
        "connections": [{"name": "db", "params": {"url": {"literal": "postgresql://db/x"}}}],
    }))
    .unwrap();
    let mut parsed = upload(&job.parse_job_id);
    parsed.files = vec![parsed_file_to_proto(&file)];
    core.upload(parsed, ts(NOW)).await.unwrap();
    core
}

/// Asks for a check of every root's connections; the job sent, and the command's ID.
async fn check(core: &mut MasterCore) -> (pb::ConnectionJob, String) {
    let command = schedr_model::commands::Command::CheckConnections {
        root: None,
        name: None,
        worker: None,
    };
    let id = core
        .db
        .insert_command(&command, "anna", ts(NOW))
        .await
        .unwrap();
    let out = core.tick(ts(NOW)).await.unwrap();
    let jobs: Vec<_> = out
        .iter()
        .filter_map(|o| match message(o) {
            master_message::Message::ConnectionJob(job) => Some(job.clone()),
            _ => None,
        })
        .collect();
    let [job] = jobs.as_slice() else {
        panic!("expected one ConnectionJob: {out:?}")
    };
    (job.clone(), id)
}

async fn stored(core: &MasterCore, id: &str) -> serde_json::Value {
    core.db.command(id).await.unwrap().unwrap().result.unwrap()
}

#[tokio::test]
async fn a_workers_answer_to_a_connections_job_fills_in_its_command() {
    let mut core = with_connection().await;
    let (job, id) = check(&mut core).await;
    assert_eq!(job.job_id, format!("{id}/etl"));
    let registry: schedr_model::connection::Registry =
        serde_json::from_slice(&job.connections_json).unwrap();
    assert_eq!(registry.files["db"], "conns.py");
    assert_eq!(
        stored(&core, &id).await,
        json!({"jobs": [{"root": "etl", "worker": "w1", "state": "running"}]})
    );
    let report = json!({"name": "db", "ok": false, "error": "secret `etl/db#pw` missing"});
    let done = || pb::ConnectionJobDone {
        job_id: job.job_id.clone(),
        report_json: vec![serde_json::to_vec(&report).unwrap()],
    };
    // Another worker's answer to it is dropped.
    core.handle(
        "w9",
        worker_message::Message::ConnectionJobDone(done()),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(stored(&core, &id).await["jobs"][0]["state"], "running");
    core.handle(
        "w1",
        worker_message::Message::ConnectionJobDone(done()),
        ts(NOW),
    )
    .await
    .unwrap();
    assert_eq!(
        stored(&core, &id).await,
        json!({"jobs": [{"root": "etl", "worker": "w1", "state": "done",
                         "connections": [report]}]})
    );
    // A report that doesn't read is a bad message.
    let bad = pb::ConnectionJobDone {
        job_id: job.job_id.clone(),
        report_json: vec![b"{}".to_vec()],
    };
    assert!(matches!(
        core.handle(
            "w1",
            worker_message::Message::ConnectionJobDone(bad),
            ts(NOW)
        )
        .await,
        Err(CoreError::BadMessage { .. })
    ));
}

#[tokio::test]
async fn a_connections_job_is_lost_with_its_worker() {
    let mut core = with_connection().await;
    let (_, id) = check(&mut core).await;
    let bye = worker_message::Message::Deregister(pb::Deregister {});
    core.handle("w1", bye, ts(NOW)).await.unwrap();
    assert_eq!(
        stored(&core, &id).await,
        json!({"jobs": [{"root": "etl", "worker": "w1", "state": "lost",
                         "reason": "worker `w1` went away before answering"}]})
    );
}

/// Lets the database take the writes [`refuse`] made it refuse.
async fn allow(core: &MasterCore, table: &str) {
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP TRIGGER refuse_{table}")))
        .execute(pool)
        .await
        .unwrap();
}

fn connection_jobs(out: &[Outgoing]) -> Vec<pb::ConnectionJob> {
    out.iter()
        .filter_map(|o| match message(o) {
            master_message::Message::ConnectionJob(job) => Some(job.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_connections_job_goes_out_once_its_command_is_decided() {
    // Sent before its decision was stored, it went out again with the next round's.
    let mut core = with_connection().await;
    let command = schedr_model::commands::Command::CheckConnections {
        root: None,
        name: None,
        worker: None,
    };
    let id = core
        .db
        .insert_command(&command, "anna", ts(NOW))
        .await
        .unwrap();
    refuse(&core, "commands", "NEW.status = 'applied'").await;
    let (out, result) = core.tick_partial(ts(NOW)).await;
    assert!(result.is_err());
    assert_eq!(connection_jobs(&out), []);
    allow(&core, "commands").await;
    let out = core.tick(later(1)).await.unwrap();
    assert_eq!(
        connection_jobs(&out)
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>(),
        [format!("{id}/etl")]
    );
    assert_eq!(connection_jobs(&core.tick(later(2)).await.unwrap()), []);
}

#[tokio::test]
async fn a_connections_job_whose_answer_wasnt_stored_is_still_lost_with_its_worker() {
    let mut core = with_connection().await;
    let (job, id) = check(&mut core).await;
    let done = || {
        worker_message::Message::ConnectionJobDone(pb::ConnectionJobDone {
            job_id: job.job_id.clone(),
            report_json: vec![br#"{"name": "db", "ok": true}"#.to_vec()],
        })
    };
    // Another worker's answer leaves it waiting for its own.
    core.handle("w9", done(), ts(NOW)).await.unwrap();
    refuse(&core, "commands", "NEW.result LIKE '%done%'").await;
    assert!(core.handle("w1", done(), ts(NOW)).await.is_err());
    allow(&core, "commands").await;
    let bye = worker_message::Message::Deregister(pb::Deregister {});
    core.handle("w1", bye, ts(NOW)).await.unwrap();
    assert_eq!(stored(&core, &id).await["jobs"][0]["state"], "lost");
}

#[tokio::test]
async fn a_connections_job_is_lost_when_its_worker_restarts_or_doesnt_answer_in_time() {
    let mut core = with_connection().await;
    let (_, restarted) = check(&mut core).await;
    core.disconnected("w1", later(1)).await.unwrap();
    core.register(&register("w1", "i2", vec![]), later(2))
        .await
        .unwrap();
    assert_eq!(
        stored(&core, &restarted).await["jobs"][0],
        json!({"root": "etl", "worker": "w1", "state": "lost",
               "reason": "worker `w1` restarted before answering"})
    );
    let (_, silent) = check(&mut core).await;
    // A check has 30 s, and its answer 30 s more.
    core.tick(later(59)).await.unwrap();
    assert_eq!(stored(&core, &silent).await["jobs"][0]["state"], "running");
    core.tick(later(61)).await.unwrap();
    assert_eq!(
        stored(&core, &silent).await["jobs"][0],
        json!({"root": "etl", "worker": "w1", "state": "lost",
               "reason": "worker `w1` didn't answer in time"})
    );
}

#[tokio::test]
async fn a_restarted_master_calls_the_jobs_it_sent_before_lost() {
    let mut first = with_connection().await;
    let (_, id) = check(&mut first).await;
    let mut restarted = MasterCore::new(first.db.clone(), Settings::default());
    restarted.restore(later(10)).await.unwrap();
    assert_eq!(
        stored(&restarted, &id).await["jobs"][0],
        json!({"root": "etl", "worker": "w1", "state": "lost",
               "reason": "the master restarted before the answer came"})
    );
}

#[tokio::test]
async fn a_connections_job_goes_to_no_disconnected_worker() {
    let mut core = with_connection().await;
    core.disconnected("w1", later(1)).await.unwrap();
    let command = schedr_model::commands::Command::CheckConnections {
        root: None,
        name: None,
        worker: None,
    };
    let id = core
        .db
        .insert_command(&command, "anna", later(1))
        .await
        .unwrap();
    assert_eq!(connection_jobs(&core.tick(later(2)).await.unwrap()), []);
    let record = core.db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        (record.status, record.reason.as_deref()),
        (
            CommandStatus::Rejected,
            Some("no active worker has root `etl` at its current code")
        )
    );
}

#[tokio::test]
async fn a_kept_result_the_database_refuses_doesnt_hold_its_worker_for_ever() {
    // Stored as bytes it passes; written out when the worker goes, Postgres refuses the NUL.
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    let started = pb::Started {
        attempt_id: assign.attempt_id.clone(),
        start_method: pb::StartMethod::Spawn.into(),
    };
    core.handle("w1", worker_message::Message::Started(started), ts(NOW))
        .await
        .unwrap();
    refuse(&core, "attempts", "instr(NEW.code_hash, char(0)) > 0").await;
    let ended = worker_message::Message::TaskEnded(pb::TaskEnded {
        result: Some(pb::Finished {
            attempt_id: assign.attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            code_hash: "sha256-ab\0cd".to_owned(),
            ..pb::Finished::default()
        }),
    });
    core.handle("w1", ended, ts(NOW)).await.unwrap();
    let gone = worker_message::Message::Deregister(pb::Deregister::default());
    core.handle("w1", gone, ts(NOW)).await.unwrap();
    let attempt = core.db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Failed));
}

#[tokio::test]
async fn a_task_end_cut_off_at_its_commit_counts_only_if_its_result_was_kept() {
    let mut core = with_dag().await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    let id = assign.attempt_id.clone();
    let open = core.db.attempt(&id).await.unwrap();
    assert!(!result_landed(open.as_ref()));
    core.db.set_task_result(&id, b"kept").await.unwrap();
    let kept = core.db.attempt(&id).await.unwrap();
    assert!(result_landed(kept.as_ref()));
    assert!(result_landed(None));
}

/// Decisions 303 and 304: the script roots a DAG root's current parse asked about go to the
/// live workers once, and to a worker registering in `Registered`; a worker withdrawing the
/// root no longer has it.
#[tokio::test]
async fn the_script_roots_a_dag_root_uses_go_to_the_workers_and_a_root_can_be_withdrawn() {
    let mut file = dag_file();
    file.script_roots = vec!["jobs".to_owned()];
    let mut core = with_dag_of(&file).await;
    let uses_sent = |out: &[Outgoing]| -> Vec<(String, Vec<pb::RootUses>)> {
        out.iter()
            .filter_map(|o| match message(o) {
                master_message::Message::ScriptUses(uses) => {
                    Some((o.worker_id.clone(), uses.roots.clone()))
                }
                _ => None,
            })
            .collect()
    };
    let expected = vec![pb::RootUses {
        root: "etl".to_owned(),
        script_roots: vec!["jobs".to_owned()],
    }];
    let out = core.tick(later(1)).await.unwrap();
    assert_eq!(uses_sent(&out), [("w1".to_owned(), expected.clone())]);
    assert_eq!(uses_sent(&core.tick(later(2)).await.unwrap()), []);
    let (registered, _) = core
        .register(&register("w2", "i1", vec![]), later(3))
        .await
        .unwrap();
    assert_eq!(registered.script_uses, expected);
    let withdrawn = worker_message::Message::CodeVersionChanged(pb::CodeVersionChanged {
        root: Some(root("")),
    });
    core.handle("w1", withdrawn, later(4)).await.unwrap();
    let w1 = core.dispatcher().workers().get("w1").unwrap();
    assert_eq!(w1.roots.get("etl"), None);
    assert_eq!(
        core.dispatcher()
            .workers()
            .get("w2")
            .unwrap()
            .roots
            .get("etl")
            .map(String::as_str),
        Some("h1")
    );
}

#[tokio::test]
async fn a_script_root_no_worker_has_stops_withdrawing_its_dag_roots_after_the_grace() {
    // A typo (`typo`) that no worker has: after the grace, workers no longer fold it, so the
    // DAG root stays reported and a fix to it is parsed (303, the user's refinement).
    let mut file = dag_file();
    file.script_roots = vec!["jobs".to_owned(), "typo".to_owned()];
    let mut core = with_dag_of(&file).await;
    core.settings.script_root_grace = Duration::from_mins(1);
    let mut w2 = register("w2", "i1", vec![]);
    w2.roots.push(pb::RootHash {
        kind: pb::RootKind::Script.into(),
        root: "jobs".to_owned(),
        content_hash: "s1".to_owned(),
    });
    let (registered, _) = core.register(&w2, later(30)).await.unwrap();
    let uses = |roots: &[&str]| {
        vec![pb::RootUses {
            root: "etl".to_owned(),
            script_roots: roots.iter().map(|r| (*r).to_owned()).collect(),
        }]
    };
    // Within the grace from the master's start, a root not reported yet still counts.
    assert_eq!(registered.script_uses, uses(&["jobs", "typo"]));
    let sent = |out: &[Outgoing]| -> Vec<Vec<pb::RootUses>> {
        out.iter()
            .filter_map(|o| match message(o) {
                master_message::Message::ScriptUses(uses) => Some(uses.roots.clone()),
                _ => None,
            })
            .collect()
    };
    core.tick(later(31)).await.unwrap();
    // After it, `typo` is left out; `jobs`, reported by `w2`, stays.
    let out = core.tick(later(61)).await.unwrap();
    assert_eq!(sent(&out), [uses(&["jobs"]), uses(&["jobs"])]);
    // `w2` is lost (its lease ends at the tick of 123 s): `jobs` too counts as absent once
    // the grace has passed since it was last reported, not before.
    core.disconnected("w2", later(62)).await.unwrap();
    core.tick(later(123)).await.unwrap();
    assert!(!core.connections.contains_key("w2"));
    let effective = |core: &mut MasterCore, t| root_uses(&core.script_uses_at(later(t)));
    assert_eq!(effective(&mut core, 180), uses(&["jobs"]));
    assert_eq!(effective(&mut core, 184), []);
}

#[tokio::test]
async fn a_poke_that_missed_its_workers_drain_is_moved_when_it_comes_back() {
    let mut file = dag_file();
    let mut dag = serde_json::to_value(&file.dags[0]).unwrap();
    dag["tasks"] = json!([{
        "id": "wait", "kind": "sensor", "mode": "poke", "every_ms": 1000, "arguments": {},
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "any"}
    }]);
    dag["edges"] = json!([]);
    file.dags[0] = serde_json::from_value(dag).unwrap();
    let mut core = with_dag_of(&file).await;
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask")
    };
    let attempt = assign.attempt_id.clone();
    core.disconnected("w1", ts(NOW)).await.unwrap();
    // Drained while it was away: no message could reach it.
    core.db.set_worker_drained("w1", true).await.unwrap();
    let journal = vec![pb::JournalEntry {
        handles: 0,
        attempt_id: attempt.clone(),
        state: pb::JournalState::Running.into(),
    }];
    let (registered, sent) = core
        .register(&register("w1", "i1", journal), later(5))
        .await
        .unwrap();
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Continue)
    );
    let moved = sent
        .iter()
        .find_map(|outgoing| match message(outgoing) {
            master_message::Message::CancelAttempt(cancel) => Some(cancel.clone()),
            _ => None,
        })
        .expect("the poke told to move");
    assert_eq!(
        (moved.attempt_id.as_str(), moved.interrupted),
        (attempt.as_str(), true)
    );
}

/// [`dag_file`], started by a trigger.
fn event_file() -> ParsedFile {
    let mut file = serde_json::to_value(dag_file()).unwrap();
    file["dags"][0]["trigger"] = json!({
        "kind": "event",
        "task": {
            "id": "found", "kind": "sensor", "mode": "loop", "arguments": {},
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }
    });
    serde_json::from_value(file).unwrap()
}

/// With a DAG started by a trigger, unpaused, and its trigger given to `w1`.
async fn with_trigger() -> (MasterCore, pb::AssignTrigger) {
    let mut core = with_dag_of(&event_file()).await;
    core.db
        .set_paused(&DagId::new("sync").unwrap(), false)
        .await
        .unwrap();
    let assigned = assign_of(&core.tick(later(1)).await.unwrap());
    (core, assigned.expect("the trigger is given to a worker"))
}

fn assign_of(outgoing: &[Outgoing]) -> Option<pb::AssignTrigger> {
    outgoing.iter().find_map(|o| match message(o) {
        master_message::Message::AssignTrigger(assign) => Some(assign.clone()),
        _ => None,
    })
}

fn found(assign: &pb::AssignTrigger, seq: u64, key: &str) -> worker_message::Message {
    worker_message::Message::TriggerEvent(pb::TriggerEvent {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        seq,
        key: key.to_owned(),
        data_json: b"1".to_vec(),
        at: None,
    })
}

fn ack_of(outgoing: &[Outgoing]) -> Option<u64> {
    outgoing.iter().find_map(|o| match message(o) {
        master_message::Message::TriggerEventAck(ack) => Some(ack.seq),
        _ => None,
    })
}

#[tokio::test]
async fn a_new_key_queues_a_run_and_a_repeat_only_moves_the_bookmark() {
    let (mut core, assign) = with_trigger().await;
    assert_eq!(assign.after, None);
    assert_eq!(assign.failures, 0);
    assert_eq!(assign.dag_id, "sync");
    for (seq, key) in [(1, "a"), (2, "b"), (3, "a")] {
        let answer = core
            .handle("w1", found(&assign, seq, key), later(2))
            .await
            .unwrap();
        assert_eq!(ack_of(&answer), Some(seq), "acknowledged once stored");
    }
    let runs = core
        .db
        .queued_runs(&DagId::new("sync").unwrap())
        .await
        .unwrap();
    assert_eq!(runs.len(), 2, "{runs:?}");
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.bookmark.as_deref(), Some("a"));
}

#[tokio::test]
async fn what_an_old_instance_found_is_acknowledged_and_not_stored() {
    let (mut core, mut assign) = with_trigger().await;
    assign.instance_id = "someone-else".to_owned();
    let answer = core
        .handle("w1", found(&assign, 1, "a"), later(2))
        .await
        .unwrap();
    assert_eq!(ack_of(&answer), Some(1));
    let runs = core
        .db
        .queued_runs(&DagId::new("sync").unwrap())
        .await
        .unwrap();
    assert_eq!(runs.len(), 0);
}

#[tokio::test]
async fn pausing_stops_a_trigger_and_it_comes_back_from_its_bookmark() {
    let (mut core, assign) = with_trigger().await;
    core.handle("w1", found(&assign, 1, "a"), later(2))
        .await
        .unwrap();
    let dag = DagId::new("sync").unwrap();
    core.db.set_paused(&dag, true).await.unwrap();
    let stopped = core.tick(later(3)).await.unwrap();
    assert!(
        stopped.iter().any(|o| matches!(
            message(o),
            master_message::Message::CancelTrigger(cancel)
                if cancel.instance_id == assign.instance_id
        )),
        "{stopped:?}"
    );
    let ended = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        ..pb::TriggerStopped::default()
    });
    core.handle("w1", ended, later(3)).await.unwrap();
    assert_eq!(core.dispatcher.worker("w1").unwrap().wait_steps.len(), 0);
    core.db.set_paused(&dag, false).await.unwrap();
    let again = assign_of(&core.tick(later(4)).await.unwrap()).unwrap();
    assert_ne!(again.instance_id, assign.instance_id);
    assert_eq!(again.after.as_deref(), Some("a"));
}

#[tokio::test]
async fn a_failed_trigger_starts_again_only_after_its_wait() {
    let (mut core, assign) = with_trigger().await;
    let stopped = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        error_kind: "trigger_failed".to_owned(),
        message: "ValueError: no".to_owned(),
        ..pb::TriggerStopped::default()
    });
    core.handle("w1", stopped, later(2)).await.unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.failures, 1);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("trigger_failed: ValueError: no")
    );
    assert_eq!(core.dispatcher.worker("w1").unwrap().wait_steps.len(), 0);
    assert_eq!(assign_of(&core.tick(later(3)).await.unwrap()), None);
    assert!(assign_of(&core.tick(later(8)).await.unwrap()).is_some());
}

#[tokio::test]
async fn a_lost_worker_gives_up_its_trigger_and_one_it_reports_late_is_stopped() {
    let (mut core, assign) = with_trigger().await;
    core.register(&register("w2", "i2", vec![]), later(2))
        .await
        .unwrap();
    // `w1` comes back with nothing: what it had has ended.
    core.disconnected("w1", later(3)).await.unwrap();
    core.register(&register("w1", "i1", vec![]), later(3))
        .await
        .unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.instance_id, None);
    // A trigger the master doesn't count as running there.
    let beat = worker_message::Message::Heartbeat(pb::Heartbeat {
        running_triggers: vec![assign.instance_id.clone()],
        ..pb::Heartbeat::default()
    });
    let told = core.handle("w1", beat, later(4)).await.unwrap();
    assert!(
        told.iter().any(|o| matches!(
            message(o),
            master_message::Message::CancelTrigger(cancel)
                if cancel.instance_id == assign.instance_id
        )),
        "{told:?}"
    );
}

#[tokio::test]
async fn a_worker_coming_back_with_its_trigger_keeps_it() {
    let (mut core, assign) = with_trigger().await;
    core.disconnected("w1", later(2)).await.unwrap();
    let mut back = register("w1", "i1", vec![]);
    back.running_triggers = vec![assign.instance_id.clone()];
    core.register(&back, later(3)).await.unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(
        stored.instance_id.as_deref(),
        Some(assign.instance_id.as_str())
    );
    assert_eq!(assign_of(&core.tick(later(4)).await.unwrap()), None);
    assert_eq!(core.dispatcher.worker("w1").unwrap().wait_steps.len(), 1);
}

#[tokio::test]
async fn a_manual_run_of_an_event_dag_is_refused_without_an_event() {
    let (core, _) = with_trigger().await;
    let refused = core
        .scheduler
        .trigger(&DagId::new("sync").unwrap(), None, "anna", later(2))
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("needs an event"), "{refused}");
}

#[tokio::test]
async fn a_trigger_given_nowhere_keeps_no_slot() {
    let mut core = with_dag_of(&event_file()).await;
    core.db
        .set_paused(&DagId::new("sync").unwrap(), false)
        .await
        .unwrap();
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query(
        "CREATE TRIGGER refuse_triggers BEFORE INSERT ON triggers \
         BEGIN SELECT RAISE(ABORT, 'refused by the test'); END",
    )
    .execute(pool)
    .await
    .unwrap();
    assert!(core.tick(later(1)).await.is_err());
    assert_eq!(core.dispatcher.worker("w1").unwrap().wait_steps.len(), 0);
}

#[tokio::test]
async fn what_a_trigger_of_an_older_version_found_starts_nothing() {
    let (mut core, assign) = with_trigger().await;
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query("UPDATE triggers SET dag_version_id = 'older'")
        .execute(pool)
        .await
        .unwrap();
    let answer = core
        .handle("w1", found(&assign, 1, "a"), later(2))
        .await
        .unwrap();
    assert_eq!(ack_of(&answer), Some(1));
    let dag = DagId::new("sync").unwrap();
    assert_eq!(core.db.queued_runs(&dag).await.unwrap().len(), 0);
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.bookmark, None);
}

#[tokio::test]
async fn new_code_or_a_withdrawn_root_stops_a_trigger_also_under_a_new_master() {
    let (core, assign) = with_trigger().await;
    let db = core.db.clone();
    // A new master, over the same data; the worker returns with its trigger, at other code.
    let mut restarted = MasterCore::new(db, Settings::default());
    restarted.restore(later(1)).await.unwrap();
    let mut back = register("w1", "i1", vec![]);
    back.roots = vec![root("h2")];
    back.running_triggers = vec![assign.instance_id.clone()];
    restarted.register(&back, later(2)).await.unwrap();
    let told = restarted.tick(later(3)).await.unwrap();
    assert!(
        told.iter().any(|o| matches!(
            message(o),
            master_message::Message::CancelTrigger(cancel)
                if cancel.instance_id == assign.instance_id
        )),
        "{told:?}"
    );
    core.db
        .trigger("sync")
        .await
        .unwrap()
        .filter(|r| r.instance_id.is_none())
        .expect("ended");
}

#[tokio::test]
async fn only_the_worker_that_has_a_trigger_speaks_for_it() {
    let (mut core, assign) = with_trigger().await;
    core.register(&register("w2", "i2", vec![]), later(2))
        .await
        .unwrap();
    let answer = core
        .handle("w2", found(&assign, 1, "planted"), later(3))
        .await
        .unwrap();
    assert_eq!(ack_of(&answer), Some(1));
    let dag = DagId::new("sync").unwrap();
    assert_eq!(core.db.queued_runs(&dag).await.unwrap().len(), 0);
    let stopped = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        error_kind: "trigger_failed".to_owned(),
        message: "planted".to_owned(),
        ..pb::TriggerStopped::default()
    });
    core.handle("w2", stopped, later(3)).await.unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(
        stored.instance_id.as_deref(),
        Some(assign.instance_id.as_str())
    );
    assert_eq!(stored.last_error, None);
    // Nor does one free a slot that is someone else's.
    let stopped = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: "not-a-trigger".to_owned(),
        instance_id: assign.instance_id.clone(),
        error_kind: String::new(),
        message: String::new(),
        ..pb::TriggerStopped::default()
    });
    core.handle("w2", stopped, later(3)).await.unwrap();
    assert_eq!(core.dispatcher.worker("w1").unwrap().wait_steps.len(), 1);
}

#[tokio::test]
async fn what_cant_be_an_event_fails_the_trigger_and_stores_nothing() {
    let dag = DagId::new("sync").unwrap();
    for bad in [
        pb::TriggerEvent {
            key: "k".repeat(schedr_model::trigger::MAX_EVENT_KEY + 1),
            data_json: b"1".to_vec(),
            ..pb::TriggerEvent::default()
        },
        pb::TriggerEvent {
            key: "a\0b".to_owned(),
            data_json: b"1".to_vec(),
            ..pb::TriggerEvent::default()
        },
        pb::TriggerEvent {
            key: "a\nb".to_owned(),
            data_json: b"1".to_vec(),
            ..pb::TriggerEvent::default()
        },
        pb::TriggerEvent {
            key: String::new(),
            data_json: b"1".to_vec(),
            ..pb::TriggerEvent::default()
        },
        pb::TriggerEvent {
            key: "k".to_owned(),
            data_json: vec![b'1'; 2_000_000],
            ..pb::TriggerEvent::default()
        },
        pb::TriggerEvent {
            key: "k".to_owned(),
            data_json: b"{not json".to_vec(),
            ..pb::TriggerEvent::default()
        },
    ] {
        let (mut core, assign) = with_trigger().await;
        let event = pb::TriggerEvent {
            dag_id: assign.dag_id.clone(),
            instance_id: assign.instance_id.clone(),
            seq: 1,
            ..bad
        };
        let answer = core
            .handle("w1", worker_message::Message::TriggerEvent(event), later(2))
            .await
            .unwrap();
        assert_eq!(ack_of(&answer), None);
        assert!(matches!(
            message(&answer[0]),
            master_message::Message::CancelTrigger(_)
        ));
        assert_eq!(core.db.queued_runs(&dag).await.unwrap().len(), 0);
        let stored = core.db.trigger("sync").await.unwrap().unwrap();
        assert_eq!(stored.instance_id, None);
        assert!(
            stored.last_error.unwrap().starts_with("event_refused"),
            "said why"
        );
        assert_eq!(stored.bookmark, None);
    }
}

#[tokio::test]
async fn a_long_error_is_kept_short() {
    let (mut core, assign) = with_trigger().await;
    let stopped = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        error_kind: "trigger_failed".to_owned(),
        message: "x\0".repeat(1_000_000),
        ..pb::TriggerStopped::default()
    });
    core.handle("w1", stopped, later(2)).await.unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    let error = stored.last_error.unwrap();
    assert_eq!(error.chars().count(), 4096);
    assert!(!error.contains('\0'));
}

fn stopped_with(assign: &pb::AssignTrigger, kind: &str, struck: bool) -> worker_message::Message {
    worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        error_kind: kind.to_owned(),
        struck,
        code_hash: "h1".to_owned(),
        ..pb::TriggerStopped::default()
    })
}

#[tokio::test]
async fn a_generator_that_returned_stays_stopped_until_new_code() {
    let (mut core, assign) = with_trigger().await;
    core.handle(
        "w1",
        stopped_with(&assign, "trigger_returned", false),
        later(2),
    )
    .await
    .unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.stopped_why.as_deref(), Some("returned"));
    assert_eq!(assign_of(&core.tick(later(30)).await.unwrap()), None);
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query("UPDATE triggers SET stopped_on = 'older h0'")
        .execute(pool)
        .await
        .unwrap();
    let again = assign_of(&core.tick(later(31)).await.unwrap());
    assert!(again.is_some(), "a new version starts it");
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.stopped_why, None);
}

#[tokio::test]
async fn too_many_queued_runs_stop_a_trigger_until_they_have_started() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let settings = Settings {
        max_queued_events: 2,
        ..Settings::default()
    };
    let mut core = MasterCore::new(db, settings);
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    core.upload(upload_of(&job.parse_job_id, &event_file()), ts(NOW))
        .await
        .unwrap();
    let dag = DagId::new("sync").unwrap();
    core.db.set_paused(&dag, false).await.unwrap();
    let assign = assign_of(&core.tick(later(1)).await.unwrap()).unwrap();
    for (seq, key) in [(1, "a"), (2, "b")] {
        core.handle("w1", found(&assign, seq, key), later(2))
            .await
            .unwrap();
    }
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    let pool = pool.clone();
    // Held as they are, however the scheduler goes on.
    sqlx::query("UPDATE runs SET state = 'queued'")
        .execute(&pool)
        .await
        .unwrap();
    let mut told = Vec::new();
    core.trigger_pass(later(3), &mut told).await.unwrap();
    assert!(matches!(
        message(&told[0]),
        master_message::Message::CancelTrigger(_)
    ));
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.stopped_why.as_deref(), Some("back_pressure"));
    assert_eq!(stored.instance_id, None);
    sqlx::query("UPDATE runs SET state = 'running' WHERE rowid = (SELECT MIN(rowid) FROM runs)")
        .execute(&pool)
        .await
        .unwrap();
    let mut told = Vec::new();
    core.trigger_pass(later(4), &mut told).await.unwrap();
    assert_eq!(assign_of(&told), None, "not until the queue has drained");
    sqlx::query("UPDATE runs SET state = 'running'")
        .execute(&pool)
        .await
        .unwrap();
    let mut told = Vec::new();
    core.trigger_pass(later(5), &mut told).await.unwrap();
    assert_eq!(assign_of(&told).unwrap().after.as_deref(), Some("b"));
}

#[tokio::test]
async fn a_trigger_that_keeps_blocking_is_switched_off_until_new_code() {
    let (mut core, assign) = with_trigger().await;
    let mut given = assign;
    for at in [2, 100, 200] {
        core.handle(
            "w1",
            stopped_with(&given, "blocked_event_loop", true),
            later(at),
        )
        .await
        .unwrap();
        let next = assign_of(&core.tick(later(at + 50)).await.unwrap());
        if at == 200 {
            assert_eq!(next, None, "three in a row: off");
        } else {
            given = next.unwrap();
        }
    }
    let strikes = core.db.wait_step_strikes("sync", "found").await.unwrap();
    assert!(strikes.unwrap().off_at.is_some());
    core.db.clear_wait_step("sync", "found").await.unwrap();
    assert!(assign_of(&core.tick(later(1000)).await.unwrap()).is_some());
}

#[tokio::test]
async fn a_poke_report_is_kept_and_a_call_that_went_well_clears_it() {
    let (mut core, assign) = with_trigger().await;
    core.db
        .strike_wait_step(("sync", "found"), 3, "h1", later(1))
        .await
        .unwrap();
    let poked = |error: &str, failures| {
        worker_message::Message::TriggerPoked(pb::TriggerPoked {
            dag_id: assign.dag_id.clone(),
            instance_id: assign.instance_id.clone(),
            at: None,
            error: error.to_owned(),
            failures,
        })
    };
    core.handle("w1", poked("ValueError: no", 2), later(2))
        .await
        .unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.failures, 2);
    assert_eq!(stored.last_error.as_deref(), Some("ValueError: no"));
    assert_eq!(stored.last_poked_at, Some(later(2)));
    assert_eq!(
        stored.instance_id.as_deref(),
        Some(assign.instance_id.as_str()),
        "it goes on"
    );
    core.handle("w1", poked("", 0), later(3)).await.unwrap();
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.failures, 0);
    assert_eq!(stored.last_error, None);
    assert_eq!(
        core.db.wait_step_strikes("sync", "found").await.unwrap(),
        None
    );
}

#[tokio::test]
async fn a_generator_finding_nothing_for_its_idle_time_starts_again() {
    let mut file = serde_json::to_value(event_file()).unwrap();
    file["dags"][0]["trigger"]["restart_if_idle_ms"] = json!(60_000);
    let file = serde_json::from_value(file).unwrap();
    let mut core = with_dag_of(&file).await;
    core.db
        .set_paused(&DagId::new("sync").unwrap(), false)
        .await
        .unwrap();
    let first = assign_of(&core.tick(later(1)).await.unwrap()).unwrap();
    assert_eq!(assign_of(&core.tick(later(30)).await.unwrap()), None);
    core.handle("w1", found(&first, 1, "a"), later(40))
        .await
        .unwrap();
    assert_eq!(
        assign_of(&core.tick(later(80)).await.unwrap()),
        None,
        "it found one"
    );
    let told = core.tick(later(120)).await.unwrap();
    assert!(
        told.iter()
            .any(|o| matches!(message(o), master_message::Message::CancelTrigger(_)))
    );
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.last_restart_at, Some(later(120)));
    let again = assign_of(&told).expect("from its bookmark, at once");
    assert_eq!(again.after.as_deref(), Some("a"));
}

#[tokio::test]
async fn a_trigger_told_to_stop_keeps_its_slot_and_its_strike_counts() {
    let (mut core, assign) = with_trigger().await;
    let dag = DagId::new("sync").unwrap();
    core.db.set_paused(&dag, true).await.unwrap();
    core.tick(later(2)).await.unwrap();
    assert_eq!(
        core.dispatcher.worker("w1").unwrap().wait_steps.len(),
        1,
        "its worker counts it until it has ended"
    );
    let mut stopped = stopped_with(&assign, "", true);
    let worker_message::Message::TriggerStopped(one) = &mut stopped else {
        unreachable!()
    };
    one.code_hash = String::new();
    core.handle("w1", stopped, later(3)).await.unwrap();
    assert_eq!(core.dispatcher.worker("w1").unwrap().wait_steps.len(), 0);
    let strikes = core.db.wait_step_strikes("sync", "found").await.unwrap();
    assert_eq!(strikes.unwrap().strikes, 1);
}

#[tokio::test]
async fn events_past_the_queue_limit_stop_the_trigger_at_once() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let settings = Settings {
        max_queued_events: 2,
        ..Settings::default()
    };
    let mut core = MasterCore::new(db, settings);
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    core.upload(upload_of(&job.parse_job_id, &event_file()), ts(NOW))
        .await
        .unwrap();
    let dag = DagId::new("sync").unwrap();
    core.db.set_paused(&dag, false).await.unwrap();
    let assign = assign_of(&core.tick(later(1)).await.unwrap()).unwrap();
    for (seq, key) in [(1, "a"), (2, "b")] {
        let answer = core
            .handle("w1", found(&assign, seq, key), later(2))
            .await
            .unwrap();
        assert_eq!(ack_of(&answer), Some(seq));
    }
    let answer = core
        .handle("w1", found(&assign, 3, "c"), later(2))
        .await
        .unwrap();
    assert_eq!(ack_of(&answer), None);
    assert!(matches!(
        message(&answer[0]),
        master_message::Message::CancelTrigger(_)
    ));
    assert_eq!(core.db.queued_runs(&dag).await.unwrap().len(), 2);
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(stored.bookmark.as_deref(), Some("b"));
    assert_eq!(stored.stopped_why.as_deref(), Some("back_pressure"));
}

/// [`dag_file`] with `sync` started by runs of `orders` (`run_of`), and `orders` beside it.
fn run_of_file(run_of: &serde_json::Value) -> ParsedFile {
    let mut file = serde_json::to_value(dag_file()).unwrap();
    let mut orders = file["dags"][0].clone();
    orders["id"] = json!("orders");
    orders["max_active_runs"] = json!(5);
    file["dags"][0]["trigger"] = json!({"kind": "run_of", "run_of": run_of});
    file["dags"][0]["max_active_runs"] = json!(5);
    file["dags"].as_array_mut().unwrap().push(orders);
    serde_json::from_value(file).unwrap()
}

/// A run of `orders` planned for `planned_for` that succeeded at `ended`.
async fn orders_succeeded(core: &MasterCore, planned_for: Timestamp, ended: Timestamp) -> String {
    let orders = DagId::new("orders").unwrap();
    let version = core
        .db
        .stored_dag(&orders)
        .await
        .unwrap()
        .unwrap()
        .current
        .unwrap()
        .version_id;
    let run_id = core
        .db
        .insert_run(&schedr_db::runs::NewRun {
            params: &schedr_db::runs::NO_PARAMS,
            event: None,
            dag_id: &orders,
            dag_version_id: &version,
            kind: schedr_model::RunKind::Manual,
            state: schedr_model::RunState::Running,
            planned_for,
            since: planned_for,
            triggered_at: planned_for,
            triggered_by: "anna",
            parent_run_id: None,
            backfill_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    core.db
        .finish_run(&run_id, schedr_model::RunState::Success, ended)
        .await
        .unwrap();
    run_id
}

async fn sync_runs(core: &MasterCore) -> Vec<(Timestamp, schedr_model::RunKind)> {
    let sync = DagId::new("sync").unwrap();
    let mut runs: Vec<_> = core
        .db
        .dag_runs(&sync, None, 50)
        .await
        .unwrap()
        .into_iter()
        .map(|run| (run.planned_for, run.kind))
        .collect();
    runs.sort_by_key(|(at, _)| *at);
    runs
}

#[tokio::test]
async fn a_dag_started_by_runs_of_another_starts_once_for_each_that_succeeds_while_live() {
    let mut core = with_dag_of(&run_of_file(&json!({"dag": "orders", "lag_ms": 3_600_000}))).await;
    let sync = DagId::new("sync").unwrap();
    // Before it is live: not caught up.
    orders_succeeded(&core, ts("2026-10-20T00:00:00Z"), ts(NOW)).await;
    for dag in ["sync", "orders"] {
        core.db
            .set_paused_at(&DagId::new(dag).unwrap(), false, later(1))
            .await
            .unwrap();
    }
    core.tick(later(1)).await.unwrap();
    core.tick(later(2)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 0);
    let theirs = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(3)).await;
    core.tick(later(4)).await.unwrap();
    core.tick(later(5)).await.unwrap();
    let runs = sync_runs(&core).await;
    assert_eq!(runs.len(), 1, "once, however many passes see it");
    // The run that started it is its parent, linked both ways.
    let started = core.db.dag_runs(&sync, None, 1).await.unwrap().remove(0);
    assert_eq!(started.parent_run_id.as_deref(), Some(theirs.as_str()));
    assert_eq!(
        runs[0].0,
        ts("2026-10-21T01:00:00Z"),
        "planned for its own plus lag"
    );
    // Paused, what ends meanwhile is never caught up.
    core.db.set_paused_at(&sync, true, later(6)).await.unwrap();
    orders_succeeded(&core, ts("2026-10-22T00:00:00Z"), later(7)).await;
    core.tick(later(8)).await.unwrap();
    core.db.set_paused_at(&sync, false, later(9)).await.unwrap();
    core.tick(later(9)).await.unwrap();
    core.tick(later(10)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 1);
}

/// [`dag_file`] with `sync` started by events named `paid` (decision 354).
fn event_named_file() -> ParsedFile {
    let mut file = serde_json::to_value(dag_file()).unwrap();
    file["dags"][0]["trigger"] = json!({"kind": "event_named", "event": {"name": "paid"}});
    file["dags"][0]["max_active_runs"] = json!(5);
    serde_json::from_value(file).unwrap()
}

async fn post(core: &MasterCore, key: &str, at: Timestamp, posted_at: Timestamp) {
    let event = schedr_db::named_events::NewNamedEvent {
        name: "paid",
        key,
        data: &json!({"amount": 40}),
        at,
    };
    core.db
        .post_named_event(&event, "anna", None, posted_at)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_dag_started_by_named_events_starts_once_per_key_posted_while_live() {
    let mut core = with_dag_of(&event_named_file()).await;
    let sync = DagId::new("sync").unwrap();
    // Before it is live: not caught up.
    post(&core, "o-0", ts(NOW), ts(NOW)).await;
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    core.tick(later(1)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 0);
    post(&core, "o-1", ts("2026-10-21T00:00:00Z"), later(2)).await;
    // The same key again (a new event, decision 354), and another one: one run more.
    post(&core, "o-1", ts("2026-10-21T00:00:00Z"), later(3)).await;
    post(&core, "o-2", ts("2026-10-22T00:00:00Z"), later(3)).await;
    core.tick(later(4)).await.unwrap();
    core.tick(later(5)).await.unwrap();
    let runs = sync_runs(&core).await;
    assert_eq!(
        runs.iter().map(|(at, _)| *at).collect::<Vec<_>>(),
        [ts("2026-10-21T00:00:00Z"), ts("2026-10-22T00:00:00Z")],
        "one per key, planned for when it happened"
    );
    let event = core.db.stored_event("sync", "o-1").await.unwrap().unwrap();
    assert_eq!(event.data, json!({"amount": 40}));
    // Paused, what is posted meanwhile is never caught up.
    core.db.set_paused_at(&sync, true, later(6)).await.unwrap();
    post(&core, "o-3", later(7), later(7)).await;
    core.tick(later(8)).await.unwrap();
    core.db.set_paused_at(&sync, false, later(9)).await.unwrap();
    core.tick(later(9)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 2);
    post(&core, "o-4", later(10), later(10)).await;
    core.tick(later(11)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 3);
}

#[tokio::test]
async fn a_dag_started_by_runs_of_another_queues_no_more_than_the_limit_in_one_pass() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let settings = Settings {
        max_queued_events: 2,
        ..Settings::default()
    };
    let mut core = MasterCore::new(db, settings);
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    let file = run_of_file(&json!({"dag": "orders"}));
    core.upload(upload_of(&job.parse_job_id, &file), ts(NOW))
        .await
        .unwrap();
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), false, later(1))
        .await
        .unwrap();
    for day in 21..24 {
        let planned_for = ts(&format!("2026-10-{day}T00:00:00Z"));
        orders_succeeded(&core, planned_for, later(2)).await;
    }
    core.tick(later(3)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 2, "the limit, not the three");
}

#[tokio::test]
async fn a_run_of_a_dag_started_by_another_can_be_run_again_by_hand_with_its_event() {
    let mut core = with_dag_of(&run_of_file(&json!({"dag": "orders"}))).await;
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), false, later(1))
        .await
        .unwrap();
    let other = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    core.tick(later(3)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 1);
    let command = schedr_model::commands::Command::TriggerRun {
        event: Some(schedr_model::commands::AskedEvent::Stored { key: other }),
        params: None,
        dag_id: DagId::new("sync").unwrap(),
        planned_for: Some(ts("2026-10-20T00:00:00Z")),
    };
    let id = core
        .db
        .insert_command(&command, "anna", later(4))
        .await
        .unwrap();
    core.tick(later(5)).await.unwrap();
    let record = core.db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        (record.status, record.reason),
        (CommandStatus::Applied, None)
    );
    assert_eq!(sync_runs(&core).await.len(), 2);
}

#[tokio::test]
async fn a_dag_started_by_runs_of_another_starts_once_even_when_the_events_are_gone() {
    let mut core = with_dag_of(&run_of_file(&json!({"dag": "orders"}))).await;
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), false, later(1))
        .await
        .unwrap();
    orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    orders_succeeded(&core, ts("2026-10-20T00:00:00Z"), later(2)).await;
    core.tick(later(3)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 2);
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    let pool = pool.clone();
    sqlx::query("DELETE FROM events")
        .execute(&pool)
        .await
        .unwrap();
    core.tick(later(4)).await.unwrap();
    core.tick(later(5)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 2, "kept events or not");
}

#[tokio::test]
async fn what_a_worker_reports_for_people_is_kept_within_limits() {
    // B183: a worker that lies could store megabytes of hosts or warnings on each heartbeat.
    let mut core = with_dag().await;
    let host = pb::WatcherHost {
        code: "é".repeat(5000),
        wait_steps: 1,
        threads: 1,
        tied_threads: 0,
        memory_bytes: 0,
        hosts_lost: 0,
    };
    let beat = worker_message::Message::Heartbeat(pb::Heartbeat {
        watcher_hosts: vec![host; 300],
        ..pb::Heartbeat::default()
    });
    core.handle("w1", beat, later(1)).await.unwrap();
    let warning = pb::WorkerWarning {
        kind: "fork_server_disabled".to_owned(),
        interpreter: "python".to_owned(),
        message: "x".repeat(10_000),
    };
    let report = worker_message::Message::Warnings(pb::Warnings {
        warnings: vec![warning; 300],
    });
    core.handle("w1", report, later(2)).await.unwrap();
    let workers = core.db.workers().await.unwrap();
    let w1 = workers.iter().find(|w| w.id == "w1").unwrap();
    assert_eq!(w1.watcher_hosts.len(), 256);
    assert_eq!(
        w1.watcher_hosts[0].code.chars().count(),
        4096,
        "cut at a character"
    );
    assert_eq!(w1.warnings.len(), 256);
    assert_eq!(w1.warnings[0].message.len(), 4096);
    assert_eq!(w1.warnings[0].kind, "fork_server_disabled");
    let cached = &core.connections["w1"].watcher_hosts;
    assert_eq!(
        (cached.len(), cached[0].code.chars().count()),
        (256, 4096),
        "nor kept whole in memory"
    );
}

#[tokio::test]
async fn a_registration_over_the_limits_is_refused_not_stored() {
    // B183: what a worker registers with is kept and matched on, so it isn't cut but refused.
    let mut core = core().await;
    let long = "q".repeat(4097);
    let cases = [
        pb::Register {
            schedr_version: long.clone(),
            ..register("w1", "i1", vec![])
        },
        pb::Register {
            queues: vec![long.clone()],
            ..register("w1", "i1", vec![])
        },
        pb::Register {
            queues: (0..257).map(|n| format!("q{n}")).collect(),
            ..register("w1", "i1", vec![])
        },
        pb::Register {
            log_endpoint: long.clone(),
            ..register("w1", "i1", vec![])
        },
        pb::Register {
            config_json: format!("\"{}\"", "c".repeat(256 * 4096)),
            ..register("w1", "i1", vec![])
        },
    ];
    for case in cases {
        let refused = core.register(&case, ts(NOW)).await.unwrap_err();
        assert!(matches!(refused, CoreError::Refused(_)), "{refused}");
    }
    assert_eq!(core.db.workers().await.unwrap(), []);
    assert!(
        core.register(&register("w1", "i1", vec![]), ts(NOW))
            .await
            .is_ok()
    );
}

/// [`run_of_file`] with `sync` started by an `@trigger(wait_for=RunOf("orders"))` instead.
fn waiting_trigger_file(run_retries: Option<u32>) -> ParsedFile {
    let mut file = serde_json::to_value(run_of_file(&json!({"dag": "orders"}))).unwrap();
    let mut trigger = json!({
        "kind": "event",
        "task": {
            "id": "on_orders", "kind": "sensor", "mode": "loop",
            "arguments": {"least": {"literal": 5}},
            "wait_for": {"run_of": {"dag": "orders"}, "param": "run"},
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                "queue": "default", "max_output_size": 1_048_576
            },
            "output_type": {"type": "none"}
        }
    });
    if let Some(retries) = run_retries {
        trigger["run_retries"] = json!(retries);
    }
    file["dags"][0]["trigger"] = trigger;
    serde_json::from_value(file).unwrap()
}

/// A waiting trigger's call that returned: done with its run.
fn returned(assign: &pb::AssignTrigger) -> worker_message::Message {
    worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        done: true,
        ..pb::TriggerStopped::default()
    })
}

/// The run an `AssignTrigger` hands its function, by its ID.
fn handed_run(assign: &pb::AssignTrigger) -> String {
    let task: serde_json::Value = serde_json::from_slice(&assign.task_json).unwrap();
    task["arguments"]["run"]["literal"]["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no run handed: {task}"))
        .to_owned()
}

/// `sync` and `orders` live from `later(1)`, with a waiting trigger.
async fn with_waiting_trigger(run_retries: Option<u32>) -> MasterCore {
    let core = with_dag_of(&waiting_trigger_file(run_retries)).await;
    for dag in ["sync", "orders"] {
        core.db
            .set_paused_at(&DagId::new(dag).unwrap(), false, later(1))
            .await
            .unwrap();
    }
    core
}

#[tokio::test]
async fn a_trigger_waiting_for_runs_is_called_once_for_each_that_ends_with_it() {
    let mut core = with_waiting_trigger(None).await;
    assert!(
        assign_of(&core.tick(later(2)).await.unwrap()).is_none(),
        "nothing ended: not called"
    );
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(3)).await;
    let second = orders_succeeded(&core, ts("2026-10-22T00:00:00Z"), later(4)).await;
    let assign = assign_of(&core.tick(later(5)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&assign), first, "oldest first");
    let task: serde_json::Value = serde_json::from_slice(&assign.task_json).unwrap();
    assert_eq!(task["arguments"]["least"], json!({"literal": 5}));
    assert!(
        assign_of(&core.tick(later(6)).await.unwrap()).is_none(),
        "one call at a time"
    );
    // Its event, keyed by the run, then a clean end: the next call gets the next run.
    let answer = core
        .handle("w1", found(&assign, 1, &first), later(6))
        .await
        .unwrap();
    assert_eq!(ack_of(&answer), Some(1));
    core.handle("w1", returned(&assign), later(6))
        .await
        .unwrap();
    let next = assign_of(&core.tick(later(7)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&next), second);
    core.handle("w1", returned(&next), later(7)).await.unwrap();
    assert!(assign_of(&core.tick(later(8)).await.unwrap()).is_none());
    assert_eq!(
        sync_runs(&core).await.len(),
        1,
        "the second call started none"
    );
    let record = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(record.failures, 0);
}

#[tokio::test]
async fn a_trigger_waiting_for_runs_gets_the_same_run_after_failing_until_its_retries_are_used() {
    let mut core = with_waiting_trigger(Some(1)).await;
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let second = orders_succeeded(&core, ts("2026-10-22T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&assign), first);
    core.handle(
        "w1",
        stopped_with(&assign, "trigger_failed", false),
        later(3),
    )
    .await
    .unwrap();
    assert!(
        assign_of(&core.tick(later(4)).await.unwrap()).is_none(),
        "after its wait"
    );
    let again = assign_of(&core.tick(later(60)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&again), first, "the same run, tried again");
    core.handle(
        "w1",
        stopped_with(&again, "trigger_failed", false),
        later(60),
    )
    .await
    .unwrap();
    let record = core.db.trigger("sync").await.unwrap().unwrap();
    let error = record.last_error.unwrap();
    assert!(
        error.contains(&format!("passed run {first}")) && error.contains("failed 2 times"),
        "{error}"
    );
    assert_eq!(record.failures, 0);
    let next = assign_of(&core.tick(later(61)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&next), second, "passed: the next at once");
}

#[tokio::test]
async fn skip_passes_the_run_a_trigger_keeps_failing_on_but_not_while_it_is_called() {
    let mut core = with_waiting_trigger(None).await;
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let second = orders_succeeded(&core, ts("2026-10-22T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    let skip = schedr_model::commands::Command::SkipTriggerRun {
        dag_id: DagId::new("sync").unwrap(),
    };
    let refused = core
        .db
        .insert_command(&skip, "anna", later(4))
        .await
        .unwrap();
    core.tick(later(4)).await.unwrap();
    let record = core.db.command(&refused).await.unwrap().unwrap();
    assert_eq!(record.status, CommandStatus::Rejected);
    assert!(
        record
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("being called"),
        "{:?}",
        record.reason
    );
    core.handle(
        "w1",
        stopped_with(&assign, "trigger_failed", false),
        later(4),
    )
    .await
    .unwrap();
    let mut calls = 0;
    for at in [10, 25, 50] {
        let call = assign_of(&core.tick(later(at)).await.unwrap()).unwrap();
        assert_eq!(handed_run(&call), first, "never passed by itself");
        core.handle(
            "w1",
            stopped_with(&call, "trigger_failed", false),
            later(at),
        )
        .await
        .unwrap();
        calls += 1;
    }
    assert_eq!(calls, 3);
    let skipped = core
        .db
        .insert_command(&skip, "anna", later(55))
        .await
        .unwrap();
    // Applied first in the round, which then calls it for the next run.
    let next = assign_of(&core.tick(later(55)).await.unwrap()).unwrap();
    let record = core.db.command(&skipped).await.unwrap().unwrap();
    assert_eq!(
        (record.status, record.result),
        (CommandStatus::Applied, Some(json!({"run_id": first})))
    );
    let stored = core.db.trigger("sync").await.unwrap().unwrap();
    assert!(stored.last_error.unwrap().starts_with(&format!(
        "passed run {first}: skipped by anna (command {skipped})"
    )));
    assert_eq!(handed_run(&next), second);
}

#[tokio::test]
async fn a_waiting_trigger_interrupted_or_cancelled_keeps_its_run() {
    let mut core = with_waiting_trigger(None).await;
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    // Asked to stop by its worker (draining): no error, but not done.
    core.handle("w1", stopped_with(&assign, "", false), later(3))
        .await
        .unwrap();
    let again = assign_of(&core.tick(later(4)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&again), first, "not passed");
}

#[tokio::test]
async fn a_waiting_trigger_s_call_ends_with_its_run_passed_or_not_at_all() {
    let mut core = with_waiting_trigger(None).await;
    orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let second = orders_succeeded(&core, ts("2026-10-22T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    core.db.fail_write(1);
    assert!(
        core.handle("w1", returned(&assign), later(3))
            .await
            .is_err()
    );
    let record = core.db.trigger("sync").await.unwrap().unwrap();
    assert_eq!(
        record.instance_id.as_deref(),
        Some(assign.instance_id.as_str()),
        "the call is still held, so its end can come again"
    );
    core.handle("w1", returned(&assign), later(4))
        .await
        .unwrap();
    let next = assign_of(&core.tick(later(5)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&next), second);
}

#[tokio::test]
async fn a_new_run_after_a_pause_has_its_own_retries_and_a_return_clears_strikes() {
    let mut core = with_waiting_trigger(Some(1)).await;
    orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    core.handle(
        "w1",
        stopped_with(&assign, "trigger_failed", true),
        later(3),
    )
    .await
    .unwrap();
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, true, later(4)).await.unwrap();
    core.tick(later(4)).await.unwrap();
    core.db.set_paused_at(&sync, false, later(5)).await.unwrap();
    let fresh = orders_succeeded(&core, ts("2026-10-22T00:00:00Z"), later(6)).await;
    let call = assign_of(&core.tick(later(10)).await.unwrap()).unwrap();
    assert_eq!(
        handed_run(&call),
        fresh,
        "the run from before the pause is dropped"
    );
    assert_eq!(call.failures, 0, "the dropped run's failures aren't its");
    core.handle(
        "w1",
        stopped_with(&call, "trigger_failed", false),
        later(10),
    )
    .await
    .unwrap();
    let retried = assign_of(&core.tick(later(20)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&retried), fresh, "its one retry");
    core.handle("w1", returned(&retried), later(20))
        .await
        .unwrap();
    let strikes = core
        .db
        .wait_step_strikes("sync", "on_orders")
        .await
        .unwrap();
    assert!(
        strikes.as_ref().is_none_or(|s| s.strikes == 0),
        "a call that returned clears its strikes: {strikes:?}"
    );
}

#[tokio::test]
async fn a_skip_applied_before_its_decision_was_stored_is_applied_again() {
    let mut core = with_waiting_trigger(None).await;
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    core.handle(
        "w1",
        stopped_with(&assign, "trigger_failed", false),
        later(3),
    )
    .await
    .unwrap();
    let skip = schedr_model::commands::Command::SkipTriggerRun {
        dag_id: DagId::new("sync").unwrap(),
    };
    let id = core
        .db
        .insert_command(&skip, "anna", later(4))
        .await
        .unwrap();
    // The round passed the run and was cut before storing the decision.
    core.db.mark_applying(&id, later(4)).await.unwrap();
    let active = core
        .db
        .active_dags_of(&[&DagId::new("sync").unwrap()])
        .await
        .unwrap();
    let why = format!("skipped by anna (command {id})");
    crate::core::triggers::pass_handed_run(&core.db, &active[0].dag, &why, None)
        .await
        .unwrap()
        .unwrap();
    core.tick(later(5)).await.unwrap();
    let record = core.db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        (record.status, record.result),
        (CommandStatus::Applied, Some(json!({"run_id": first})))
    );
}

#[tokio::test]
async fn a_call_being_stopped_is_not_made_again_until_it_has_ended() {
    let mut core = with_waiting_trigger(None).await;
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    core.register(&register("w2", "i2", vec![]), later(3))
        .await
        .unwrap();
    let drain = schedr_model::commands::Command::DrainWorker {
        worker_id: "w1".to_owned(),
    };
    core.db
        .insert_command(&drain, "anna", later(4))
        .await
        .unwrap();
    let told = core.tick(later(4)).await.unwrap();
    assert!(
        told.iter().any(|o| matches!(
            message(o),
            master_message::Message::CancelTrigger(cancel)
                if cancel.instance_id == assign.instance_id
        )),
        "{told:?}"
    );
    assert!(
        assign_of(&told).is_none(),
        "the run isn't called twice: {told:?}"
    );
    assert!(assign_of(&core.tick(later(5)).await.unwrap()).is_none());
    // Its end, cancelled: the run, kept, is called on the other worker.
    core.handle("w1", stopped_with(&assign, "", false), later(5))
        .await
        .unwrap();
    let again = core.tick(later(6)).await.unwrap();
    let call = assign_of(&again).unwrap();
    assert_eq!(handed_run(&call), first);
    assert!(again.iter().any(|o| o.worker_id == "w2"));
}

#[tokio::test]
async fn a_worker_being_stopped_cant_pass_another_dags_run() {
    let mut file = serde_json::to_value(waiting_trigger_file(None)).unwrap();
    let mut report = file["dags"][0].clone();
    report["id"] = json!("report");
    file["dags"].as_array_mut().unwrap().push(report);
    let file: ParsedFile = serde_json::from_value(file).unwrap();
    let mut core = with_dag_of(&file).await;
    for dag in ["sync", "orders", "report"] {
        core.db
            .set_paused_at(&DagId::new(dag).unwrap(), false, later(1))
            .await
            .unwrap();
    }
    orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let calls = core.tick(later(3)).await.unwrap();
    let calls: Vec<pb::AssignTrigger> = calls
        .iter()
        .filter_map(|o| match message(o) {
            master_message::Message::AssignTrigger(a) => Some(a.clone()),
            _ => None,
        })
        .collect();
    let of = |dag: &str| calls.iter().find(|a| a.dag_id == dag).unwrap().clone();
    let (sync, report) = (of("sync"), of("report"));
    // `report`'s call fails: between calls, its run handed.
    core.handle(
        "w1",
        stopped_with(&report, "trigger_failed", false),
        later(3),
    )
    .await
    .unwrap();
    let handed = core.db.trigger_run_handed("report").await.unwrap();
    assert!(handed.is_some());
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), true, later(4))
        .await
        .unwrap();
    core.tick(later(4)).await.unwrap();
    // Told to stop `sync`'s call, the worker names `report`.
    let lie = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: "report".to_owned(),
        instance_id: sync.instance_id.clone(),
        done: true,
        ..pb::TriggerStopped::default()
    });
    core.handle("w1", lie, later(5)).await.unwrap();
    assert_eq!(
        core.db.trigger_run_handed("report").await.unwrap(),
        handed,
        "another DAG's run stays"
    );
}

#[tokio::test]
async fn a_run_too_big_to_send_is_a_failed_call_that_run_retries_pass() {
    for (retries, passed) in [(None, false), (Some(0), true)] {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.migrate().await.unwrap();
        let settings = Settings {
            // Room for results (64 KiB), less than the assignment below.
            max_message_size: 70 * 1024,
            ..Settings::default()
        };
        let mut core = MasterCore::new(db, settings);
        let (_, outgoing) = core
            .register(&register("w1", "i1", vec![]), ts(NOW))
            .await
            .unwrap();
        let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
            panic!("expected AssignParse")
        };
        // Outputs that fit the small limit, or the DAGs aren't taken.
        let mut file = serde_json::to_value(waiting_trigger_file(retries)).unwrap();
        for dag in file["dags"].as_array_mut().unwrap() {
            for task in dag["tasks"].as_array_mut().unwrap() {
                task["settings"]["max_output_size"] = json!(10);
            }
            if dag["trigger"]["kind"] == "event" {
                dag["trigger"]["task"]["settings"]["max_output_size"] = json!(10);
                // With the run, the whole assignment is over the limit.
                dag["trigger"]["task"]["arguments"]["least"] =
                    json!({"literal": "x".repeat(80 * 1024)});
            }
        }
        let file: ParsedFile = serde_json::from_value(file).unwrap();
        core.upload(upload_of(&job.parse_job_id, &file), ts(NOW))
            .await
            .unwrap();
        for dag in ["sync", "orders"] {
            core.db
                .set_paused_at(&DagId::new(dag).unwrap(), false, later(1))
                .await
                .unwrap();
        }
        let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
        let told = core.tick(later(3)).await.unwrap();
        assert!(
            assign_of(&told).is_none(),
            "never sent: it would end the stream"
        );
        let record = core.db.trigger("sync").await.unwrap().unwrap();
        let error = record.last_error.unwrap_or_default();
        assert!(error.contains("a worker takes"), "{error}");
        let handed = core.db.trigger_run_handed("sync").await.unwrap();
        if passed {
            assert_eq!(handed, None, "run_retries=0: passed at once");
            assert!(error.starts_with(&format!("passed run {first}")), "{error}");
        } else {
            assert_eq!(handed.map(|h| h.run), Some(first), "kept, for a skip");
            assert_eq!(record.failures, 1);
        }
    }
}

#[tokio::test]
async fn a_call_a_restarted_master_doesnt_know_holds_back_waiting_triggers_until_it_ends() {
    let mut core = with_waiting_trigger(None).await;
    let first = orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    // A worker coming back with a call this master never gave (its DAG unknown here).
    let mut back = register("w2", "i2", vec![]);
    back.running_triggers = vec!["from-before".to_owned()];
    let (_, told) = core.register(&back, later(3)).await.unwrap();
    assert!(
        told.iter().any(|o| matches!(
            message(o),
            master_message::Message::CancelTrigger(cancel) if cancel.instance_id == "from-before"
        )),
        "{told:?}"
    );
    assert!(
        assign_of(&core.tick(later(3)).await.unwrap()).is_none(),
        "it may be this trigger's call, with this run"
    );
    let ended = worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: "sync".to_owned(),
        instance_id: "from-before".to_owned(),
        ..pb::TriggerStopped::default()
    });
    core.handle("w2", ended, later(4)).await.unwrap();
    let call = assign_of(&core.tick(later(5)).await.unwrap()).unwrap();
    assert_eq!(handed_run(&call), first);
}

#[tokio::test]
async fn an_event_stored_behind_the_newest_one_seen_still_starts_its_run() {
    // Codex's review: a post's time is taken before its commit, so one can land behind.
    let mut core = with_dag_of(&event_named_file()).await;
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    post(&core, "o-2", later(5), later(5)).await;
    core.tick(later(6)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 1);
    post(&core, "o-1", later(3), later(3)).await;
    core.tick(later(7)).await.unwrap();
    core.tick(later(8)).await.unwrap();
    assert_eq!(sync_runs(&core).await.len(), 2, "the late one too, once");
}

#[tokio::test]
async fn a_repeated_key_leaves_the_next_run_s_window_as_it_was() {
    // Codex's review: a repeat moved the next run's `since`, depending on the passes.
    let mut core = with_dag_of(&event_named_file()).await;
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    post(&core, "o-1", later(2), later(2)).await;
    core.tick(later(3)).await.unwrap();
    post(&core, "o-1", later(4), later(4)).await;
    post(&core, "o-2", later(6), later(6)).await;
    core.tick(later(7)).await.unwrap();
    let runs = core.db.dag_runs(&sync, None, 50).await.unwrap();
    let second = runs.iter().find(|r| r.planned_for == later(6)).unwrap();
    assert_eq!(
        second.since,
        later(2),
        "from the first run's moment, not the repeat's"
    );
}

#[tokio::test]
async fn an_event_s_data_shaped_like_a_tag_reaches_its_run_as_posted() {
    let mut core = with_dag_of(&event_named_file()).await;
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), false, later(1))
        .await
        .unwrap();
    let event = schedr_db::named_events::NewNamedEvent {
        name: "paid",
        key: "o-1",
        data: &json!({"$dict": {"amount": 40}}),
        at: later(2),
    };
    core.db
        .post_named_event(&event, "anna", None, later(2))
        .await
        .unwrap();
    core.tick(later(3)).await.unwrap();
    let stored = core.db.stored_event("sync", "o-1").await.unwrap().unwrap();
    assert_eq!(
        stored.data,
        json!({"$dict": {"$dict": {"amount": 40}}}),
        "escaped"
    );
}

#[tokio::test]
async fn an_event_for_a_moment_before_the_last_one_s_has_an_empty_window_not_an_inverted_one() {
    // Grok's review of M4.11e: `since` came after `planned_for`.
    let mut core = with_dag_of(&event_named_file()).await;
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    post(&core, "o-1", later(100), later(2)).await;
    post(&core, "o-2", later(5), later(3)).await;
    core.tick(later(4)).await.unwrap();
    for run in core.db.dag_runs(&sync, None, 50).await.unwrap() {
        assert!(
            run.since <= run.planned_for,
            "{} > {}",
            run.since,
            run.planned_for
        );
    }
}

/// [`waiting_trigger_file`] waiting for the events named `paid` instead (decision 354).
fn event_waiting_trigger_file() -> ParsedFile {
    let mut file = serde_json::to_value(waiting_trigger_file(None)).unwrap();
    file["dags"][0]["trigger"]["task"]["wait_for"] =
        json!({"event": {"name": "paid"}, "param": "run"});
    serde_json::from_value(file).unwrap()
}

/// A call's end that says it returned an event (`returned_event`).
fn returned_with_event(assign: &pb::AssignTrigger) -> worker_message::Message {
    worker_message::Message::TriggerStopped(pb::TriggerStopped {
        dag_id: assign.dag_id.clone(),
        instance_id: assign.instance_id.clone(),
        done: true,
        returned_event: true,
        ..pb::TriggerStopped::default()
    })
}

#[tokio::test]
async fn a_call_s_event_the_master_didn_t_store_hands_its_item_again() {
    // Codex's review of part 2: stopped while its call ran, the event it returned was dropped
    // and its end passed the item for good.
    let mut core = with_dag_of(&event_waiting_trigger_file()).await;
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    post(&core, "o-1", later(2), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    assert_eq!(handed_event(&assign)["key"], json!("o-1"));
    // Its DAG changed while the call ran (as a deploy does): the event is of the old version.
    let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
        unreachable!("the tests' database is SQLite")
    };
    sqlx::query("UPDATE triggers SET dag_version_id = 'before'")
        .execute(pool)
        .await
        .unwrap();
    let answer = core
        .handle("w1", found(&assign, 1, "o-1"), later(4))
        .await
        .unwrap();
    assert_eq!(ack_of(&answer), Some(1), "acknowledged, not stored");
    core.handle("w1", returned_with_event(&assign), later(4))
        .await
        .unwrap();
    assert_eq!(
        core.db.queued_runs(&sync).await.unwrap().len(),
        0,
        "dropped"
    );
    let again = assign_of(&core.tick(later(5)).await.unwrap()).expect("handed again");
    assert_eq!(handed_event(&again)["key"], json!("o-1"));
    core.handle("w1", found(&again, 1, "o-1"), later(6))
        .await
        .unwrap();
    core.handle("w1", returned_with_event(&again), later(6))
        .await
        .unwrap();
    assert_eq!(core.db.queued_runs(&sync).await.unwrap().len(), 1, "stored");
    assert!(
        assign_of(&core.tick(later(7)).await.unwrap()).is_none(),
        "passed once stored"
    );
}

fn handed_event(assign: &pb::AssignTrigger) -> serde_json::Value {
    let task: serde_json::Value = serde_json::from_slice(&assign.task_json).unwrap();
    task["arguments"]["run"]["literal"].clone()
}

#[tokio::test]
async fn a_trigger_waiting_for_events_is_called_once_for_each_posted_while_live() {
    let mut core = with_dag_of(&event_waiting_trigger_file()).await;
    post(&core, "o-0", ts(NOW), ts(NOW)).await;
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), false, later(1))
        .await
        .unwrap();
    assert!(
        assign_of(&core.tick(later(2)).await.unwrap()).is_none(),
        "one posted before it was live: not called"
    );
    post(&core, "o-1", ts("2026-10-21T00:00:00Z"), later(3)).await;
    post(&core, "o-1", ts("2026-10-21T00:00:00Z"), later(4)).await;
    let assign = assign_of(&core.tick(later(5)).await.unwrap()).unwrap();
    let event = handed_event(&assign);
    assert_eq!(
        (&event["key"], &event["data"]["amount"]),
        (&json!("o-1"), &json!(40))
    );
    core.handle("w1", returned(&assign), later(5))
        .await
        .unwrap();
    let next = assign_of(&core.tick(later(6)).await.unwrap()).unwrap();
    assert_eq!(
        handed_event(&next)["key"],
        json!("o-1"),
        "each post is an event"
    );
    assert_ne!(next.instance_id, assign.instance_id);
    core.handle("w1", returned(&next), later(6)).await.unwrap();
    assert!(assign_of(&core.tick(later(7)).await.unwrap()).is_none());
}

#[tokio::test]
async fn a_trigger_waiting_for_events_is_called_for_one_stored_behind_the_last_once() {
    // Codex's review of part 1: a post's time is taken before its commit.
    let mut core = with_dag_of(&event_waiting_trigger_file()).await;
    core.db
        .set_paused_at(&DagId::new("sync").unwrap(), false, later(1))
        .await
        .unwrap();
    post(&core, "o-2", later(5), later(5)).await;
    let assign = assign_of(&core.tick(later(6)).await.unwrap()).unwrap();
    assert_eq!(handed_event(&assign)["key"], json!("o-2"));
    core.handle("w1", returned(&assign), later(6))
        .await
        .unwrap();
    post(&core, "o-1", later(3), later(3)).await;
    let late = assign_of(&core.tick(later(7)).await.unwrap()).unwrap();
    assert_eq!(handed_event(&late)["key"], json!("o-1"), "the late one too");
    core.handle("w1", returned(&late), later(7)).await.unwrap();
    assert!(
        assign_of(&core.tick(later(8)).await.unwrap()).is_none(),
        "and each once"
    );
}

#[tokio::test]
async fn only_named_events_are_kept_as_passed_and_a_removed_dag_s_are_forgotten() {
    // B186: `RunOf` items were kept and never read; nothing was ever deleted.
    let count = async |core: &MasterCore| -> i64 {
        let schedr_db::Pool::Sqlite(pool) = core.db.pool() else {
            unreachable!("the tests' database is SQLite")
        };
        sqlx::query_scalar("SELECT COUNT(*) FROM trigger_passed")
            .fetch_one(pool)
            .await
            .unwrap()
    };
    let mut core = with_waiting_trigger(None).await;
    orders_succeeded(&core, ts("2026-10-21T00:00:00Z"), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    core.handle("w1", returned(&assign), later(3))
        .await
        .unwrap();
    assert_eq!(count(&core).await, 0, "a run of another DAG isn't kept");

    let mut core = with_dag_of(&event_waiting_trigger_file()).await;
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    post(&core, "o-1", later(2), later(2)).await;
    let assign = assign_of(&core.tick(later(3)).await.unwrap()).unwrap();
    core.handle("w1", returned(&assign), later(3))
        .await
        .unwrap();
    assert_eq!(count(&core).await, 1, "a named event is");
    core.db
        .apply_dag_changes(
            &[schedr_db::dags::DagChange::Remove {
                dag_id: sync.clone(),
            }],
            later(4),
        )
        .await
        .unwrap();
    assert_eq!(count(&core).await, 0, "forgotten with its DAG");
}

#[tokio::test]
async fn an_event_passed_isn_t_handed_again_after_a_late_one_and_the_pruning() {
    // Codex's review of B186: passing a late event moved the cursor back, into a stretch whose
    // passed rows were forgotten, and an event passed long ago came back.
    let mut core = with_dag_of(&event_waiting_trigger_file()).await;
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    let call = async |core: &mut MasterCore, at: i64| -> Option<serde_json::Value> {
        let assign = assign_of(&core.tick(later(at)).await.unwrap())?;
        core.handle("w1", returned(&assign), later(at))
            .await
            .unwrap();
        Some(handed_event(&assign)["key"].clone())
    };
    // Ticks a second apart (the worker's lease), the posts' times as they would be.
    post(&core, "old", later(59), later(59)).await;
    assert_eq!(call(&mut core, 2).await, Some(json!("old")));
    post(&core, "latest", later(120), later(120)).await;
    assert_eq!(call(&mut core, 3).await, Some(json!("latest")));
    // Committed late: its time was taken before the one before it was stored.
    post(&core, "late", later(118), later(118)).await;
    assert_eq!(call(&mut core, 4).await, Some(json!("late")));
    assert_eq!(call(&mut core, 5).await, None, "`old` stays passed");
}

#[tokio::test]
async fn a_lookback_raised_after_the_pruning_hands_nothing_passed_again() {
    // Grok's review of B186: a wider window reached what was passed and forgotten.
    let mut core = with_dag_of(&event_waiting_trigger_file()).await;
    core.settings.event_lookback = Duration::from_mins(30);
    let sync = DagId::new("sync").unwrap();
    core.db.set_paused_at(&sync, false, later(1)).await.unwrap();
    let call = async |core: &mut MasterCore, at: i64| -> Option<serde_json::Value> {
        let assign = assign_of(&core.tick(later(at)).await.unwrap())?;
        core.handle("w1", returned(&assign), later(at))
            .await
            .unwrap();
        Some(handed_event(&assign)["key"].clone())
    };
    post(&core, "early", later(10), later(10)).await;
    assert_eq!(call(&mut core, 2).await, Some(json!("early")));
    post(&core, "an_hour_on", later(3610), later(3610)).await;
    assert_eq!(call(&mut core, 3).await, Some(json!("an_hour_on")));
    post(&core, "after", later(3620), later(3620)).await;
    assert_eq!(
        call(&mut core, 4).await,
        Some(json!("after")),
        "`early` forgotten here"
    );
    core.settings.event_lookback = Duration::from_hours(2);
    assert_eq!(call(&mut core, 5).await, None, "`early` stays passed");
}

/// What a task's API calls reach: the request, as the router got it.
#[derive(Debug, Default)]
struct RecordedApi(std::sync::Mutex<Vec<task_api::TaskApiRequest>>);

impl task_api::TaskApi for RecordedApi {
    fn call(
        &self,
        request: task_api::TaskApiRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = task_api::TaskApiResponse> + Send + '_>>
    {
        self.0.lock().unwrap().push(request);
        Box::pin(async {
            task_api::TaskApiResponse {
                status: 201,
                body: b"{}".to_vec(),
            }
        })
    }
}

/// M4.11h: a task's call reaches the API as the task, from the worker holding its attempt open
/// and on a listed route; anything else is refused (403) and reaches nothing.
#[tokio::test]
async fn a_tasks_api_call_runs_as_the_task_only_from_its_worker_on_a_listed_route() {
    let api = std::sync::Arc::new(RecordedApi::default());
    let mut core = with_dag().await.with_task_api(api.clone());
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    let call = |attempt: &str, method: &str, path: &str| {
        worker_message::Message::ApiCall(pb::ApiCall {
            request_id: 7,
            attempt_id: attempt.to_owned(),
            method: method.to_owned(),
            path: path.to_owned(),
            body_json: br#"{"name": "paid", "key": "o-1"}"#.to_vec(),
            idempotency_key: "k".to_owned(),
            ..pb::ApiCall::default()
        })
    };
    let status = |out: &[Outgoing]| {
        let master_message::Message::ApiAnswer(answer) = message(&out[0]) else {
            panic!("expected ApiAnswer, got {out:?}")
        };
        assert_eq!(answer.request_id, 7);
        answer.status
    };
    let attempt = assign.attempt_id.as_str();
    let posted = core
        .handle("w1", call(attempt, "post", "/api/v1/events"), ts(NOW))
        .await
        .unwrap();
    assert_eq!(status(&posted), 201);
    let off_list = core
        .handle(
            "w1",
            call(attempt, "POST", "/api/v1/dags/sync/pause"),
            ts(NOW),
        )
        .await
        .unwrap();
    assert_eq!(status(&off_list), 403);
    let another_worker = core
        .handle("w2", call(attempt, "POST", "/api/v1/events"), ts(NOW))
        .await
        .unwrap();
    assert_eq!(status(&another_worker), 403);
    let unknown = core
        .handle("w1", call("nope", "POST", "/api/v1/events"), ts(NOW))
        .await
        .unwrap();
    assert_eq!(status(&unknown), 403);
    let run = &core
        .db
        .dag_runs(&DagId::new("sync").unwrap(), None, 1)
        .await
        .unwrap()[0];
    let reached = api.0.lock().unwrap();
    assert_eq!(reached.len(), 1, "{reached:?}");
    assert_eq!(reached[0].caller, format!("task:sync/{}/extract", run.id));
    assert_eq!(
        (
            reached[0].method.as_str(),
            reached[0].idempotency_key.as_deref()
        ),
        ("POST", Some("k"))
    );
}

#[tokio::test]
async fn a_poke_trigger_crossing_failing_after_tells_its_channels_once() {
    let mut section = schedr_config::config::NotificationsSection::default();
    section.channels.insert(
        "ops".to_owned(),
        schedr_config::config::ChannelConfig {
            kind: schedr_config::config::ChannelKind::Webhook,
            url: Some("https://hooks.example.com/ops".to_owned()),
            headers_env: BTreeMap::new(),
            body: None,
            to: Vec::new(),
        },
    );
    let settings =
        crate::notifications::Notifications::from_config(&section, &BTreeMap::new()).unwrap();
    let mut file = serde_json::to_value(event_file()).unwrap();
    file["dags"][0]["notify"] =
        json!([{"channel": {"kind": "server", "name": "ops"}, "events": ["TRIGGER_FAILING"]}]);
    let file: ParsedFile = serde_json::from_value(file).unwrap();
    let mut core = core().await.with_notifications(Arc::new(settings));
    let (_, outgoing) = core
        .register(&register("w1", "i1", vec![]), ts(NOW))
        .await
        .unwrap();
    let master_message::Message::AssignParse(job) = message(&outgoing[0]) else {
        panic!("expected AssignParse")
    };
    core.upload(upload_of(&job.parse_job_id, &file), ts(NOW))
        .await
        .unwrap();
    core.db
        .set_paused(&DagId::new("sync").unwrap(), false)
        .await
        .unwrap();
    let assign = assign_of(&core.tick(later(1)).await.unwrap()).expect("the trigger is given");
    let poked = |failures| {
        worker_message::Message::TriggerPoked(pb::TriggerPoked {
            dag_id: assign.dag_id.clone(),
            instance_id: assign.instance_id.clone(),
            at: None,
            error: "ConnectionError: refused".to_owned(),
            failures,
        })
    };
    let queued = |core: &MasterCore| {
        let db = core.db.clone();
        async move { db.due_notifications(later(10_000), 10).await.unwrap() }
    };
    for (failures, expected) in [(4, 0), (5, 1), (6, 1)] {
        core.handle("w1", poked(failures), later(2)).await.unwrap();
        assert_eq!(
            queued(&core).await.len(),
            expected,
            "after {failures} failures"
        );
    }
    let sent = &queued(&core).await[0];
    assert_eq!(sent.event, "TRIGGER_FAILING");
    assert!(
        sent.body["text"]
            .as_str()
            .unwrap()
            .contains("its trigger is failing: ConnectionError: refused"),
        "{}",
        sent.body
    );
}

/// M4.13a (decisions 421, 422): a task's own notification is queued once per call, a call sent
/// again (same key) answered without queueing it twice, and the quota refuses past it, again on
/// a resend.
#[tokio::test]
async fn a_tasks_notification_is_queued_once_per_call_within_its_quota() {
    let mut section = schedr_config::config::NotificationsSection {
        max_per_attempt: 1,
        ..Default::default()
    };
    section.channels.insert(
        "ops".to_owned(),
        schedr_config::config::ChannelConfig {
            kind: schedr_config::config::ChannelKind::Webhook,
            url: Some("https://hooks.example.com/ops".to_owned()),
            headers_env: BTreeMap::new(),
            body: Some(json!({"text": "{task}: {text} ({data[rows]})"})),
            to: Vec::new(),
        },
    );
    let settings =
        crate::notifications::Notifications::from_config(&section, &BTreeMap::new()).unwrap();
    let mut core = with_dag().await.with_notifications(Arc::new(settings));
    trigger(&mut core).await;
    let out = core.tick(ts(NOW)).await.unwrap();
    let master_message::Message::AssignTask(assign) = message(&out[0]) else {
        panic!("expected AssignTask, got {out:?}")
    };
    let call = |key: &str, body: &str| {
        worker_message::Message::ApiCall(pb::ApiCall {
            request_id: 7,
            attempt_id: assign.attempt_id.clone(),
            method: "POST".to_owned(),
            path: "/api/v1/notifications".to_owned(),
            body_json: body.as_bytes().to_vec(),
            idempotency_key: key.to_owned(),
            ..pb::ApiCall::default()
        })
    };
    let answer = |out: &[Outgoing]| {
        let master_message::Message::ApiAnswer(answer) = message(&out[0]) else {
            panic!("expected ApiAnswer, got {out:?}")
        };
        (
            answer.status,
            String::from_utf8_lossy(&answer.body_json).into_owned(),
        )
    };
    let ok = r#"{"channel": "ops", "text": "loaded", "data": {"rows": 12}}"#;
    let mut statuses = Vec::new();
    for (worker, key, body) in [
        ("w1", "a", ok),
        ("w1", "a", ok),
        ("w1", "b", ok),
        ("w1", "b", ok),
        ("w1", "c", r#"{"channel": "nope"}"#),
        ("w1", "d", r#"{"channel": "ops", "data": {}}"#),
        ("w1", "e", r#"{"channel": "ops", "extra": 1}"#),
        ("w2", "f", ok),
    ] {
        let out = core.handle(worker, call(key, body), ts(NOW)).await.unwrap();
        statuses.push(answer(&out));
    }
    let codes: Vec<u32> = statuses.iter().map(|(code, _)| *code).collect();
    assert_eq!(
        codes,
        [202, 202, 429, 429, 400, 400, 400, 403],
        "{statuses:#?}"
    );
    assert!(
        statuses[4].1.contains("no channel `nope`"),
        "{}",
        statuses[4].1
    );
    assert!(
        statuses[5].1.contains("data[\\\"rows\\\"]"),
        "{}",
        statuses[5].1
    );
    let queued = core.db.due_notifications(later(10), 10).await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].event, "MESSAGE");
    assert_eq!(queued[0].body, json!({"text": "extract: loaded (12)"}));
}
