use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_model::DagId;
use schedr_model::runner::ParsedFile;
use schedr_proto::PROTOCOL_VERSION;
use schedr_proto::convert::parsed_file_to_proto;
use schedr_proto::pb::master_client::MasterClient;
use serde_json::json;
use tonic::transport::{Channel, Endpoint, Server};

use super::*;
use crate::core::Settings;

fn fixed_now() -> Timestamp {
    "2026-10-21T08:00:00Z".parse().unwrap()
}

/// A hub over a fresh database, served over an in-memory connection, and a client to it.
async fn connected() -> (Hub, Database, MasterClient<Channel>) {
    connected_with(fixed_now).await
}

/// As [`connected`], with `clock`.
async fn connected_with(clock: fn() -> Timestamp) -> (Hub, Database, MasterClient<Channel>) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    connected_on(db, clock).await
}

/// One statement's limit for the lease tests below. Same budget as the lease unit tests: long
/// enough that a loaded machine still finishes one statement. A period is three of these (the
/// production ratio); a round is a quarter of one.
const QUERY_LIMIT: Duration = Duration::from_secs(2);

/// As [`connected`], with queries limited to [`QUERY_LIMIT`]: a lease renewal then leads for
/// its period less that.
async fn connected_limited() -> (Hub, Database, MasterClient<Channel>) {
    let db = Database::connect_with_timeout("sqlite::memory:", QUERY_LIMIT)
        .await
        .unwrap();
    connected_on(db, fixed_now).await
}

/// As [`connected`], on `db`, with `clock`.
async fn connected_on(
    db: Database,
    clock: fn() -> Timestamp,
) -> (Hub, Database, MasterClient<Channel>) {
    connected_on_with(db, clock, |hub| hub).await
}

/// As [`connected_on`], the hub changed by `with` before it is served.
async fn connected_on_with(
    db: Database,
    clock: fn() -> Timestamp,
    with: impl FnOnce(Hub) -> Hub,
) -> (Hub, Database, MasterClient<Channel>) {
    db.migrate().await.unwrap();
    let hub = with(Hub::with_clock(
        MasterCore::new(db.clone(), Settings::default()),
        clock,
    ));
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let service = hub.service();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
    });
    let mut client_io = Some(client_io);
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_| {
            let io = client_io.take();
            async move {
                io.map(hyper_util::rt::TokioIo::new)
                    .ok_or_else(|| std::io::Error::other("connected once"))
            }
        }))
        .await
        .unwrap();
    (hub, db, MasterClient::new(channel))
}

fn root() -> pb::RootHash {
    pb::RootHash {
        kind: pb::RootKind::Dag.into(),
        root: "etl".to_owned(),
        content_hash: "h1".to_owned(),
    }
}

fn register(protocol: pb::ProtocolVersion) -> pb::WorkerMessage {
    pb::WorkerMessage {
        message: Some(worker_message::Message::Register(pb::Register {
            worker_id: "w1".to_owned(),
            incarnation_id: "i1".to_owned(),
            protocol: Some(protocol),
            schedr_version: "0.1.0".to_owned(),
            slots: 2,
            roots: vec![root()],
            ..pb::Register::default()
        })),
    }
}

fn wrap(message: worker_message::Message) -> pb::WorkerMessage {
    pb::WorkerMessage {
        message: Some(message),
    }
}

async fn next(stream: &mut Streaming<pb::MasterMessage>) -> master_message::Message {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("a message in time")
        .unwrap()
        .unwrap()
        .message
        .unwrap()
}

fn parse_result() -> pb::ParseResult {
    let file: ParsedFile = serde_json::from_value(json!({
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
    .unwrap();
    pb::ParseResult {
        parse_job_id: "j".to_owned(),
        worker_id: "w1".to_owned(),
        root: Some(root()),
        schedr_version: "0.1.0".to_owned(),
        files: vec![parsed_file_to_proto(&file)],
    }
}

#[tokio::test]
async fn a_worker_registers_parses_runs_a_task_and_is_acknowledged() {
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));

    client.upload_parse_result(parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    db.insert_command(&trigger, "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };

    let mut inputs = client
        .fetch_inputs(pb::FetchInputsRequest {
            attempt_id: assign.attempt_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let chunk = inputs.message().await.unwrap().unwrap();
    assert_eq!(chunk.data, b"{}");

    to_master
        .send(wrap(worker_message::Message::Started(pb::Started {
            attempt_id: assign.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        })))
        .await
        .unwrap();
    to_master
        .send(wrap(worker_message::Message::Finished(pb::Finished {
            attempt_id: assign.attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        })))
        .await
        .unwrap();
    let master_message::Message::FinishedAck(ack) = next(&mut stream).await else {
        panic!("expected FinishedAck")
    };
    assert_eq!(ack.attempt_id, assign.attempt_id);

    let status = client
        .fetch_inputs(pb::FetchInputsRequest {
            attempt_id: assign.attempt_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);

    drop(to_master);
    for _ in 0..100 {
        if !hub.core().lock().await.is_live("w1") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the worker should be disconnected when its stream ends");
}

/// A parse upload past gRPC's default 4 MiB receive limit: a file's import error padded to
/// 5 MiB beside the `sync` DAG.
fn large_parse_result() -> pb::ParseResult {
    let mut result = parse_result();
    let big: ParsedFile = serde_json::from_value(json!({
        "path": "big.py", "dags": [],
        "error": {"exception": "ImportError", "message": "x".repeat(5 << 20), "traceback": ""}
    }))
    .unwrap();
    result.files.push(parsed_file_to_proto(&big));
    result
}

#[tokio::test]
async fn a_parse_upload_past_grpcs_default_limit_is_stored() {
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));

    client
        .upload_parse_result(large_parse_result())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let dags = db.stored_dags().await.unwrap();
    assert_eq!(
        dags.iter().map(|d| d.dag_id.as_str()).collect::<Vec<_>>(),
        ["sync"]
    );
}

#[tokio::test]
async fn a_parse_upload_past_the_configured_limit_is_refused() {
    let (_hub, _db, mut client) = connected_on_with(
        Database::connect("sqlite::memory:").await.unwrap(),
        fixed_now,
        |hub| hub.with_max_message_size(4 << 20),
    )
    .await;
    let status = client
        .upload_parse_result(large_parse_result())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::OutOfRange, "{status}");
}

#[tokio::test]
async fn a_stream_must_start_with_a_compatible_register() {
    let (_hub, _db, mut client) = connected().await;
    let heartbeat = wrap(worker_message::Message::Heartbeat(pb::Heartbeat::default()));
    let status = client
        .control(tokio_stream::iter(vec![heartbeat]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let status = client
        .control(tokio_stream::iter(Vec::<pb::WorkerMessage>::new()))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let old = register(pb::ProtocolVersion { major: 9, minor: 0 });
    let status = client
        .control(tokio_stream::iter(vec![old]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn a_bad_upload_is_an_invalid_argument() {
    let (_hub, _db, mut client) = connected().await;
    let status = client
        .upload_parse_result(pb::ParseResult {
            root: None,
            ..parse_result()
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn the_loop_ticks_until_stopped() {
    let (hub, db, _client) = connected().await;
    let pause = schedr_model::commands::Command::UnpauseDag {
        dag_id: DagId::new("ghost").unwrap(),
    };
    let id = db
        .insert_command(&pause, "anna", fixed_now())
        .await
        .unwrap();
    let mut registry = prometheus_client::registry::Registry::default();
    hub.metrics.register(&mut registry);
    assert_eq!(hub.beat().since(std::time::Instant::now()), None);
    let stop = tokio::time::sleep(Duration::from_millis(100));
    hub.run(Duration::from_millis(10), None, stop).await;
    let decided = db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        decided.status,
        schedr_model::commands::CommandStatus::Rejected
    );
    // The loop beat, timed its ticks, counted the decision and refreshed the gauges.
    assert!(hub.beat().since(std::time::Instant::now()).unwrap() < Duration::from_secs(1));
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(text.contains("tick_duration_seconds_count "), "{text}");
    assert!(
        text.contains("commands_total{kind=\"unpause_dag\",decision=\"rejected\"} 1"),
        "{text}"
    );
    assert!(text.contains("command_latency_seconds_count 1"), "{text}");
}

#[tokio::test]
async fn restoring_counts_from_the_hubs_clock() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let hub = Hub::with_clock(MasterCore::new(db, Settings::default()), || {
        "2026-10-22T08:00:00Z".parse().unwrap()
    });
    assert_eq!(hub.restore().await.unwrap(), 0);
}

#[tokio::test]
async fn a_refresh_past_its_limit_is_abandoned_and_the_loop_goes_on() {
    // A file: an abandoned query closes its connection, and a new in-memory one would be an
    // empty database.
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::connect(&format!("sqlite://{}", dir.path().join("s.db").display()))
        .await
        .unwrap();
    db.migrate().await.unwrap();
    let hub = Hub::with_clock(MasterCore::new(db.clone(), Settings::default()), fixed_now)
        .with_refresh_limit(Duration::ZERO);
    let pause = schedr_model::commands::Command::UnpauseDag {
        dag_id: DagId::new("ghost").unwrap(),
    };
    let id = db
        .insert_command(&pause, "anna", fixed_now())
        .await
        .unwrap();
    hub.refresh_gauges().await;
    hub.run(
        Duration::from_millis(10),
        None,
        tokio::time::sleep(Duration::from_millis(100)),
    )
    .await;
    // Ticks are never abandoned: the command was decided.
    let decided = db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        decided.status,
        schedr_model::commands::CommandStatus::Rejected
    );
    assert_eq!(DEFAULT_REFRESH_LIMIT, Duration::from_secs(30));
}

/// Seconds after [`fixed_now`] that [`moving_now`] reads: one clock for the whole test binary,
/// so the tests that move it each set it before use (nextest runs each test in a process of
/// its own; `cargo test` threads would share it).
static MOVED: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn moving_now() -> Timestamp {
    let moved = MOVED.load(std::sync::atomic::Ordering::SeqCst);
    fixed_now() + jiff::SignedDuration::from_secs(moved)
}

fn move_to(seconds: i64) {
    MOVED.store(seconds, std::sync::atomic::Ordering::SeqCst);
}

#[tokio::test]
async fn a_stalled_master_doesnt_declare_a_worker_lost_whose_heartbeats_kept_arriving() {
    // B6: the lease counted from the last message the master *handled*, so heartbeats that
    // arrived while it was stuck in a slow write (its lock held) didn't count, and the round
    // waiting for the lock declared a live worker lost.
    move_to(0);
    let (hub, db, mut client) = connected_with(moving_now).await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    // The master is stuck: something holds its lock, and a round waits for it.
    let stuck = hub.core().lock_owned().await;
    let round = tokio::spawn({
        let hub = hub.clone();
        async move { hub.tick().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Meanwhile the worker's heartbeats arrive, well within the 60 s lease of each other.
    let heartbeat = || wrap(worker_message::Message::Heartbeat(pb::Heartbeat::default()));
    for at in [30, 59] {
        move_to(at);
        to_master.send(heartbeat()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The stall ends 61 s after the worker registered; the round runs first.
    move_to(61);
    drop(stuck);
    round.await.unwrap().unwrap();
    assert!(
        hub.core().lock().await.is_live("w1"),
        "the worker was declared lost"
    );
    let workers = db.workers().await.unwrap();
    assert_eq!(workers[0].state, schedr_model::WorkerState::Active);
}

#[tokio::test]
async fn a_stalled_master_counts_a_reconnecting_workers_register_when_it_arrives() {
    // B6, the reconnect: a worker whose connection dropped registers again within its lease
    // while the master is stuck; its `Register` waits for the lock behind a round, which must
    // count it as a sign of life rather than declare the worker lost.
    move_to(0);
    let (hub, db, mut client) = connected_with(moving_now).await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    // The worker runs an attempt…
    client.upload_parse_result(parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    db.insert_command(&trigger, "anna", moving_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    to_master
        .send(wrap(worker_message::Message::Started(pb::Started {
            attempt_id: assign.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        })))
        .await
        .unwrap();
    for _ in 0..100 {
        if db
            .attempt(&assign.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .started
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // …and its connection drops.
    drop(to_master);
    drop(stream);
    for _ in 0..100 {
        if !hub.core().lock().await.is_live("w1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The master is stuck, a round waits for it, and the worker comes back 50 s later with
    // its attempt still running.
    let stuck = hub.core().lock_owned().await;
    let round = tokio::spawn({
        let hub = hub.clone();
        async move { hub.tick().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    move_to(50);
    let mut again = register(PROTOCOL_VERSION);
    if let Some(worker_message::Message::Register(register)) = again.message.as_mut() {
        register.journal = vec![pb::JournalEntry {
            attempt_id: assign.attempt_id.clone(),
            state: pb::JournalState::Running.into(),
        }];
    }
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(again).await.unwrap();
    let reconnect = tokio::spawn({
        let mut client = client.clone();
        async move { client.control(ReceiverStream::new(outbound)).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The stall ends 61 s after the worker was last heard before it reconnected.
    move_to(61);
    drop(stuck);
    round.await.unwrap().unwrap();
    reconnect.await.unwrap().unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(
        attempt.status, None,
        "the attempt was ended: the worker was declared lost"
    );
}

/// A worker registered over a stream, its root parsed: its messages' sender and the
/// master's stream to it.
async fn parsed_worker(
    client: &mut MasterClient<Channel>,
) -> (
    mpsc::Sender<pb::WorkerMessage>,
    Streaming<pb::MasterMessage>,
) {
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    client.upload_parse_result(parse_result()).await.unwrap();
    (to_master, stream)
}

fn trigger_sync() -> schedr_model::commands::Command {
    schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    }
}

/// Makes the SQLite database refuse `event` on `table` (e.g. `UPDATE OF status ON commands`)
/// until [`heal`].
async fn break_db(db: &Database, event_on_table: &str) {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    // Fixed text from the tests themselves.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TRIGGER injected_failure BEFORE {event_on_table} \
         BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
    )))
    .execute(pool)
    .await
    .unwrap();
}

async fn heal(db: &Database) {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query("DROP TRIGGER injected_failure")
        .execute(pool)
        .await
        .unwrap();
}

async fn count_runs(db: &Database) -> i64 {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query_scalar("SELECT count(*) FROM runs")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_command_whose_decision_couldnt_be_stored_is_not_applied_again() {
    // B7: applying a command and recording its decision were separate writes; when the second
    // failed, the command stayed pending and the next round applied it again.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream) = parsed_worker(&mut client).await;
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    break_db(&db, "UPDATE OF status ON commands").await;
    assert!(hub.tick().await.is_err(), "the round should fail");
    heal(&db).await;
    hub.tick().await.unwrap();
    assert_eq!(count_runs(&db).await, 1, "the trigger was applied twice");
}

#[tokio::test]
async fn a_cancel_reaches_the_worker_when_a_later_step_of_its_round_fails() {
    // B7: a round that failed after applying a cancel dropped the `CancelAttempt` it had
    // built; the cancel was decided, so it was sent again only when the worker reconnected.
    let (hub, db, mut client) = connected().await;
    let (to_master, mut stream) = parsed_worker(&mut client).await;
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    to_master
        .send(wrap(worker_message::Message::Started(pb::Started {
            attempt_id: assign.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        })))
        .await
        .unwrap();
    for _ in 0..100 {
        if db
            .attempt(&assign.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .started
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let run_id = db
        .attempt(&assign.attempt_id)
        .await
        .unwrap()
        .unwrap()
        .run_id;
    // The task is cancelled; a command after it fails, and the round with it.
    let cancel = schedr_model::commands::Command::CancelTask {
        run_id,
        task_id: schedr_model::TaskId::new("extract").unwrap(),
    };
    db.insert_command(&cancel, "anna", fixed_now())
        .await
        .unwrap();
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    break_db(&db, "INSERT ON runs").await;
    assert!(hub.tick().await.is_err(), "the round should fail");
    heal(&db).await;
    hub.tick().await.unwrap();
    // The worker stays connected all along: the cancel must still reach it.
    let cancelled = tokio::time::timeout(Duration::from_secs(2), async {
        while let Ok(Some(message)) = stream.message().await {
            if let Some(master_message::Message::CancelAttempt(cancel)) = message.message {
                return cancel.attempt_id;
            }
        }
        String::new()
    })
    .await;
    assert_eq!(
        cancelled.ok(),
        Some(assign.attempt_id),
        "the cancel never reached the worker"
    );
}

#[tokio::test]
async fn a_command_applied_before_its_decision_failed_is_decided_applied_on_the_repeat() {
    // B7: a repeat after a failed decision found its own work done and rejected the command
    // ("isn't drained" for a resume that had resumed the worker).
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream) = parsed_worker(&mut client).await;
    let drain = schedr_model::commands::Command::DrainWorker {
        worker_id: "w1".to_owned(),
    };
    db.insert_command(&drain, "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let resume = schedr_model::commands::Command::ResumeWorker {
        worker_id: "w1".to_owned(),
    };
    let id = db
        .insert_command(&resume, "anna", fixed_now())
        .await
        .unwrap();
    break_db(&db, "UPDATE OF status ON commands").await;
    assert!(hub.tick().await.is_err(), "the round should fail");
    heal(&db).await;
    hub.tick().await.unwrap();
    let decided = db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        (decided.status, decided.reason),
        (schedr_model::commands::CommandStatus::Applied, None)
    );
}

#[tokio::test]
async fn a_master_whose_lease_isnt_renewed_pauses_and_resumes_once_it_is() {
    let (hub, db, _client) = connected_limited().await;
    let period = QUERY_LIMIT * 3;
    let tick = QUERY_LIMIT / 4;
    let leadership =
        crate::lease::Leadership::take(db.clone(), "m1".to_owned(), period, || {}).await;
    // Renewals fail from now on. The lease leads for the period less the query limit; waiting
    // out the whole period passes that. The stop covers this wait, a quiet stretch, and a
    // period more so a renewal after the heal still lands.
    break_db(&db, "UPDATE ON master_lease").await;
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run_leading(tick, None, leadership, tokio::time::sleep(period * 3))
                .await
        }
    });
    tokio::time::sleep(period).await;
    assert_eq!(hub.metrics.leading.get(), 0, "paused");
    let unpause = schedr_model::commands::Command::UnpauseDag {
        dag_id: DagId::new("ghost").unwrap(),
    };
    let id = db
        .insert_command(&unpause, "anna", fixed_now())
        .await
        .unwrap();
    tokio::time::sleep(QUERY_LIMIT).await;
    let waiting = db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        waiting.status,
        schedr_model::commands::CommandStatus::Pending,
        "a paused master applies nothing"
    );
    heal(&db).await;
    let resumed = tokio::time::timeout(period, async {
        loop {
            let decided = db.command(&id).await.unwrap().unwrap();
            if decided.status != schedr_model::commands::CommandStatus::Pending {
                return;
            }
            tokio::time::sleep(tick).await;
        }
    })
    .await;
    assert!(
        resumed.is_ok(),
        "the renewed master didn't apply the command"
    );
    assert_eq!(hub.metrics.leading.get(), 1);
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn a_master_whose_lease_was_taken_over_stops() {
    let (hub, db, _client) = connected_limited().await;
    let period = QUERY_LIMIT * 3;
    let tick = QUERY_LIMIT / 4;
    let leadership =
        crate::lease::Leadership::take(db.clone(), "m1".to_owned(), period, || {}).await;
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run_leading(tick, None, leadership, std::future::pending())
                .await
        }
    });
    // Another master takes it over (as after this one's lease expired).
    db.release_lease("m1").await.unwrap();
    assert_eq!(db.take_lease("m2", period).await.unwrap(), Some(2));
    // A renewal is every third of the period; several periods covers a late one.
    let ended = tokio::time::timeout(period * 3, running)
        .await
        .expect("the master stops")
        .unwrap();
    assert_eq!(ended, Err(LeaseLost));
    assert_eq!(hub.metrics.leading.get(), 0);
}

#[tokio::test]
async fn a_stopped_master_gives_its_lease_up() {
    let (hub, db, _client) = connected_limited().await;
    let period = QUERY_LIMIT * 3;
    let tick = QUERY_LIMIT / 4;
    let leadership =
        crate::lease::Leadership::take(db.clone(), "m1".to_owned(), period, || {}).await;
    let ended = hub
        .run_leading(tick, None, leadership, tokio::time::sleep(tick * 2))
        .await;
    assert_eq!(ended, Ok(()));
    // Taken at once by the next master, not after the period.
    assert_eq!(db.take_lease("m2", period).await.unwrap(), Some(2));
}

#[tokio::test]
async fn a_long_round_doesnt_hold_the_leases_renewals_up() {
    // The round holds the master's core past the lease's period: renewed meanwhile, the lease
    // stays this master's (not taken over by a standby while the round goes on).
    let (hub, db, _client) = connected_limited().await;
    let period = QUERY_LIMIT * 3;
    let tick = QUERY_LIMIT / 4;
    // Longer than the lease, so only a renewal during the round keeps it.
    let hold = period + QUERY_LIMIT;
    let leadership =
        crate::lease::Leadership::take(db.clone(), "m1".to_owned(), period, || {}).await;
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run_leading(tick, None, leadership, tokio::time::sleep(hold + period))
                .await
        }
    });
    // Leading from the start, before its first renewal (a third of the period).
    tokio::time::sleep(tick).await;
    assert_eq!(hub.metrics.leading.get(), 1);
    let round = hub.core.lock().await;
    tokio::time::sleep(hold).await;
    assert_eq!(db.take_lease("m2", period).await.unwrap(), None);
    drop(round);
    assert_eq!(running.await.unwrap(), Ok(()));
}

#[tokio::test]
async fn a_tasks_inputs_are_served_while_the_master_is_busy() {
    // B11: a task fetches its inputs before it starts; behind the master's lock (a round, or
    // messages queued), its `Started` came too late and the assignment expired.
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    client.upload_parse_result(parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    db.insert_command(&trigger, "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    // A long round holds the master.
    let core = hub.core();
    let busy = core.lock().await;
    let fetched = tokio::time::timeout(
        Duration::from_secs(2),
        client.fetch_inputs(pb::FetchInputsRequest {
            attempt_id: assign.attempt_id.clone(),
        }),
    )
    .await
    .expect("inputs served without waiting for the master's lock")
    .unwrap();
    let chunk = fetched.into_inner().message().await.unwrap().unwrap();
    assert_eq!(chunk.data, b"{}");
    drop(busy);
}

#[tokio::test]
async fn a_task_whose_start_arrived_isnt_taken_back_while_the_master_is_busy() {
    // B11: under load the master handled `Started` after the 10 s start timeout, though the
    // worker sent it in time; the task was taken back and assigned again, which loaded the
    // master more (624 expired attempts for 11 successes in stress scenario 13).
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    client.upload_parse_result(parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    db.insert_command(&trigger, "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    // The master is busy (a long round); the worker's `Started` arrives and waits.
    let core = hub.core();
    let mut busy = core.lock().await;
    to_master
        .send(wrap(worker_message::Message::Started(pb::Started {
            attempt_id: assign.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        })))
        .await
        .unwrap();
    // The stream's reader records it at once (an in-memory connection: well within this).
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The next round comes past the start timeout, the `Started` still unhandled.
    busy.tick(fixed_now() + Duration::from_secs(11))
        .await
        .unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None, "a task that started was taken back");
    drop(busy);
}

#[tokio::test]
async fn a_workers_queued_messages_are_handled_in_one_turn_of_the_lock() {
    // B12: one message per turn of the master's lock left a worker's messages behind every
    // round in between (1 task a second in stress scenario 13).
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    client.upload_parse_result(parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    db.insert_command(&trigger, "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    // While the master is busy, the task starts and finishes: two messages wait.
    let core = hub.core();
    let busy = core.lock().await;
    for message in [
        worker_message::Message::Started(pb::Started {
            attempt_id: assign.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        }),
        worker_message::Message::Finished(pb::Finished {
            attempt_id: assign.attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        }),
    ] {
        to_master.send(wrap(message)).await.unwrap();
    }
    // The worker's handler is waiting for the lock by then (an in-memory connection).
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(busy);
    // Next in line after the handler (the lock is first come, first served): both messages
    // were handled in its one turn.
    let _round = core.lock().await;
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Success));
}

#[tokio::test]
async fn a_finished_task_starts_its_downstream_at_once_not_at_the_next_tick() {
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    // `extract` then `load`.
    let mut result = parse_result();
    let tasks: Vec<_> = ["extract", "load"]
        .map(|id| {
            json!({
                "id": id, "kind": "python",
                "settings": {
                    "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                    "queue": "default", "max_output_size": 1_048_576
                },
                "output_type": {"type": "none"}
            })
        })
        .into();
    let file: ParsedFile = serde_json::from_value(json!({
        "path": "sync.py",
        "dags": [{
            "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
            "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
            "tasks": tasks,
            "edges": [{"from": "extract", "to": "load", "kind": "order"}]
        }],
        "error": null
    }))
    .unwrap();
    result.files = vec![parsed_file_to_proto(&file)];
    client.upload_parse_result(result).await.unwrap();
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    // Rounds a minute apart: the first one at once triggers the run and dispatches `extract`.
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run(Duration::from_mins(1), None, async {
                let _ = stopped.await;
            })
            .await;
        }
    });
    let master_message::Message::AssignTask(extract) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    assert_eq!(extract.task_id, "extract");
    for message in [
        worker_message::Message::Started(pb::Started {
            attempt_id: extract.attempt_id.clone(),
            start_method: pb::StartMethod::Spawn.into(),
        }),
        worker_message::Message::Finished(pb::Finished {
            attempt_id: extract.attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        }),
    ] {
        to_master.send(wrap(message)).await.unwrap();
    }
    let finished = std::time::Instant::now();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::FinishedAck(_)
    ));
    // Within moments (`next` waits 5 s at most), not a minute later at the next tick.
    let master_message::Message::AssignTask(load) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    assert_eq!(load.task_id, "load");
    assert!(finished.elapsed() < Duration::from_secs(2));
    let _ = stop.send(());
    running.await.unwrap();
}

#[tokio::test]
async fn a_stream_of_finishes_runs_rounds_at_most_every_round_gap() {
    let (hub, _db, mut client) = connected().await;
    let mut registry = prometheus_client::registry::Registry::default();
    hub.metrics.register(&mut registry);
    let (to_master, outbound) = mpsc::channel(64);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run(Duration::from_mins(1), None, async {
                let _ = stopped.await;
            })
            .await;
        }
    });
    // 30 finishes 10 ms apart (of attempts the master doesn't know: only their arrival
    // matters), over 300 ms: one round each without the gap.
    let started = std::time::Instant::now();
    for n in 0..30 {
        let finished = worker_message::Message::Finished(pb::Finished {
            attempt_id: format!("unknown-{n}"),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        });
        to_master.send(wrap(finished)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(ROUND_GAP * 2).await;
    let elapsed = started.elapsed();
    let _ = stop.send(());
    running.await.unwrap();
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    let rounds: u128 = text
        .lines()
        .find_map(|line| line.strip_prefix("master_tick_duration_seconds_count "))
        .expect("the rounds counted")
        .parse()
        .unwrap();
    // The first round at once, then at most one every ROUND_GAP (and one more at the end).
    let most = 2 + elapsed.as_millis() / ROUND_GAP.as_millis();
    assert!(rounds >= 3, "the finishes woke rounds: {rounds}");
    assert!(
        rounds <= most,
        "{rounds} rounds in {elapsed:?}, at most {most}"
    );
}
