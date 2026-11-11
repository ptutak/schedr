use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_model::DagId;
use schedr_model::runner::ParsedFile;
use schedr_proto::PROTOCOL_VERSION;
use schedr_proto::convert::{parse_upload_parts, parsed_file_to_proto};
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
    connected_on_with(db, clock, &Settings::default()).await
}

/// As [`connected_on`], the master with `settings`.
async fn connected_on_with(
    db: Database,
    clock: fn() -> Timestamp,
    settings: &Settings,
) -> (Hub, Database, MasterClient<Channel>) {
    db.migrate().await.unwrap();
    let hub = Hub::with_clock(MasterCore::new(db.clone(), *settings), clock).with_epoch(7);
    let client = served(&hub).await;
    (hub, db, client)
}

/// A client to `hub`, served over an in-memory connection.
async fn served(hub: &Hub) -> MasterClient<Channel> {
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
    MasterClient::new(channel)
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

/// Uploads `result` as the agent does: its header, then a file a message.
async fn upload(
    client: &mut MasterClient<Channel>,
    result: pb::ParseResult,
) -> Result<tonic::Response<pb::ParseResultAck>, tonic::Status> {
    client
        .upload_parse_result(tokio_stream::iter(parse_upload_parts(result)))
        .await
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

    upload(&mut client, parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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

    upload(&mut client, large_parse_result()).await.unwrap();
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
        &Settings {
            max_message_size: 4 << 20,
            ..Settings::default()
        },
    )
    .await;
    let status = upload(&mut client, large_parse_result()).await.unwrap_err();
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
    let status = upload(
        &mut client,
        pb::ParseResult {
            root: None,
            ..parse_result()
        },
    )
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
async fn every_message_carries_the_masters_clock_and_an_assignment_its_deadline() {
    // B225: the clock on every message names this master's timeline, master time and the
    // latest heartbeat heard on the stream; an assignment is due by its start timeout (the
    // lease) in master time from when it was stored.
    let (hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let clock_of = |message: &pb::MasterMessage| message.clock.expect("a clock");
    let registered = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first = clock_of(&registered);
    assert_eq!(first.timeline, crate::clock::timeline());
    assert_eq!(first.master_ms, fixed_now().as_millisecond());
    assert_eq!(first.heard_sequence, 0);
    assert_eq!(first.wall_ms, fixed_now().as_millisecond());
    assert_eq!(first.lease_epoch, 7, "its lease's epoch (decision 444)");
    let beat = pb::Heartbeat {
        sequence: 3,
        ..pb::Heartbeat::default()
    };
    to_master
        .send(wrap(worker_message::Message::Heartbeat(beat)))
        .await
        .unwrap();
    let mut parse_seen = false;
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match &message.message {
            Some(master_message::Message::AssignParse(_)) => parse_seen = true,
            Some(master_message::Message::HeartbeatAck(_)) => {
                assert_eq!(clock_of(&message).heard_sequence, 3);
                assert_eq!(clock_of(&message).lease_epoch, 7);
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(parse_seen);
    upload(&mut client, parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
        dag_id: DagId::new("sync").unwrap(),
        planned_for: None,
    };
    db.insert_command(&trigger, "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(master_message::Message::AssignTask(assign)) = &message.message else {
        panic!("expected AssignTask")
    };
    let lease = jiff::SignedDuration::try_from(Settings::default().lease_timeout).unwrap();
    assert_eq!(
        assign.start_by_ms,
        Some((fixed_now() + lease).as_millisecond())
    );
    assert_eq!(clock_of(&message).heard_sequence, 3);
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
/// so a test that moves it holds it for its whole run ([`own_clock`]): nextest runs each test
/// in a process of its own, but `cargo test` threads share it (one test's `move_to(0)` landed
/// between another's `move_to(11)` and its tick).
static MOVED: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// Held by the test moving [`MOVED`].
static CLOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The moving clock, at 0, for this test alone until the guard is dropped.
async fn own_clock() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = CLOCK.lock().await;
    move_to(0);
    guard
}

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
    let _clock = own_clock().await;
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
async fn a_heartbeat_is_answered_as_it_arrives_while_the_master_leads() {
    let (hub, _db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let mut next = async || stream.message().await.unwrap().unwrap().message;
    while !matches!(next().await, Some(master_message::Message::Registered(_))) {}
    let heartbeat = || wrap(worker_message::Message::Heartbeat(pb::Heartbeat::default()));
    // A master stuck behind its lock still answers: busy isn't lost.
    let stuck = hub.core().lock_owned().await;
    to_master.send(heartbeat()).await.unwrap();
    loop {
        if matches!(next().await, Some(master_message::Message::HeartbeatAck(_))) {
            break;
        }
    }
    drop(stuck);
    // A paused one doesn't: another may lead, and count the worker lost.
    hub.leading.send_replace(false);
    to_master.send(heartbeat()).await.unwrap();
    let silence = tokio::time::timeout(Duration::from_millis(200), next()).await;
    assert!(silence.is_err(), "{silence:?}");
}

#[tokio::test]
async fn a_master_that_isnt_leading_refuses_a_stream_at_once() {
    // B165: a paused master (or one taken over) doesn't hold a worker's stream until it
    // leads again; the worker is told at once, to connect to the master that leads.
    let (hub, _db, mut client) = connected().await;
    hub.leading.send_replace(false);
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        client.control(ReceiverStream::new(outbound)),
    )
    .await
    .expect("refused at once, not held");
    let status = refused.expect_err("a stream to a master that doesn't lead");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert!(status.message().contains("isn't leading"), "{status:?}");
    // Leading again, it takes streams again.
    hub.leading.send_replace(true);
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
}

#[tokio::test]
async fn a_worker_that_gives_up_during_its_registration_can_register_again() {
    // B171: a worker giving up resets its stream, which drops the server's call. Dropped inside
    // the registration, the worker stayed live with no stream behind it, nothing to end it, and
    // each try refused ("already has a live stream"), each refusal also renewing its lease.
    let (_hub, db, mut client) = connected().await;
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("sqlite")
    };
    // The only connection: the registration stops at its first write, the worker marked live.
    let held = pool.acquire().await.unwrap();
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut giving_up = client.clone();
    let opening =
        tokio::spawn(async move { giving_up.control(ReceiverStream::new(outbound)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!opening.is_finished());
    opening.abort();
    drop(to_master);
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(held);
    // Refused at most while the abandoned registration ends.
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        let (to_master, outbound) = mpsc::channel(16);
        to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
        match client.control(ReceiverStream::new(outbound)).await {
            Ok(response) => break response.into_inner(),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
                assert!(
                    tokio::time::Instant::now() < until,
                    "still refused: {status:?}"
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
}

#[tokio::test]
async fn a_parse_upload_the_worker_gave_up_on_is_still_stored() {
    // As B171: the upload arrived whole, so the worker's reset doesn't stop it half-way.
    let (_hub, db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let _stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("sqlite")
    };
    let held = pool.acquire().await.unwrap();
    let mut giving_up = client.clone();
    let uploading = tokio::spawn(async move { upload(&mut giving_up, parse_result()).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!uploading.is_finished());
    uploading.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(held);
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while db
        .stored_dag(&DagId::new("sync").unwrap())
        .await
        .unwrap()
        .is_none()
    {
        assert!(tokio::time::Instant::now() < until, "not stored");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_master_paused_while_a_register_waits_for_the_lock_refuses_it() {
    // Codex's review of B165: the check came before the wait for the lock, behind a round
    // during which the master paused.
    let (hub, _db, mut client) = connected().await;
    let round = hub.core().lock_owned().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let opening = tokio::spawn(async move { client.control(ReceiverStream::new(outbound)).await });
    // Past the first check, waiting for the lock.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!opening.is_finished());
    hub.leading.send_replace(false);
    drop(round);
    let refused = tokio::time::timeout(Duration::from_secs(5), opening)
        .await
        .expect("answered once the lock is free")
        .unwrap();
    let status = refused.expect_err("a stream to a master that paused meanwhile");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
}

#[tokio::test]
async fn a_stalled_master_counts_a_reconnecting_workers_register_when_it_arrives() {
    // B6, the reconnect: a worker whose connection dropped registers again within its lease
    // while the master is stuck; its `Register` waits for the lock behind a round, which must
    // count it as a sign of life rather than declare the worker lost.
    let _clock = own_clock().await;
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
    upload(&mut client, parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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
            handles: 0,
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
    upload(client, parse_result()).await.unwrap();
    (to_master, stream)
}

fn trigger_sync() -> schedr_model::commands::Command {
    schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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
        map_index: None,
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
    upload(&mut client, parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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
    upload(&mut client, parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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
    upload(&mut client, parse_result()).await.unwrap();
    let trigger = schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
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
    upload(&mut client, result).await.unwrap();
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
async fn a_stop_doesnt_wait_for_the_gap_after_a_round() {
    let settings = Settings {
        timings: crate::core::MasterTimings {
            round_gap: Duration::from_mins(1),
            ..crate::core::MasterTimings::default()
        },
        ..Settings::default()
    };
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let (hub, _db, mut client) = connected_on_with(db, fixed_now, &settings).await;
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
            hub.run(Duration::from_mins(10), None, async {
                let _ = stopped.await;
            })
            .await;
        }
    });
    // Its first round ends, then a finish asks for the next one, a minute from it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let finished = worker_message::Message::Finished(pb::Finished {
        attempt_id: "unknown".to_owned(),
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    });
    to_master.send(wrap(finished)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("stopped during the gap")
        .unwrap();
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

/// B35: a root bigger than the limit, its files each under it, is uploaded a file a message
/// and stored, instead of being refused and handed out again every ten minutes.
#[tokio::test]
async fn a_root_past_the_limit_is_stored_when_each_file_fits() {
    let (hub, db, mut client) = connected_on_with(
        Database::connect("sqlite::memory:").await.unwrap(),
        fixed_now,
        &Settings {
            max_message_size: 4 << 20,
            ..Settings::default()
        },
    )
    .await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let master_message::Message::Registered(registered) = next(&mut stream).await else {
        panic!("expected Registered")
    };
    assert_eq!(registered.max_message_size, 4 << 20);
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::AssignParse(_)
    ));
    let mut result = parse_result();
    for name in ["a.py", "b.py"] {
        let big: ParsedFile = serde_json::from_value(json!({
            "path": name, "dags": [],
            "error": {"exception": "ImportError", "message": "x".repeat(3 << 20), "traceback": ""}
        }))
        .unwrap();
        result.files.push(parsed_file_to_proto(&big));
    }
    upload(&mut client, result).await.unwrap();
    hub.tick().await.unwrap();
    let dags = db.stored_dags().await.unwrap();
    assert_eq!(
        dags.iter().map(|d| d.dag_id.as_str()).collect::<Vec<_>>(),
        ["sync"]
    );
}

#[tokio::test]
async fn an_old_stream_of_a_worker_neither_changes_nor_disconnects_its_new_one() {
    // B44: an old stream whose transport stays open after the worker was taken for gone
    // (here: marked disconnected, as a lost lease does) and registered again.
    let (hub, db, client) = connected().await;
    let open = |message: pb::WorkerMessage| {
        let mut client = client.clone();
        async move {
            let (to_master, outbound) = mpsc::channel(16);
            to_master.send(message).await.unwrap();
            let stream = client
                .control(ReceiverStream::new(outbound))
                .await
                .unwrap()
                .into_inner();
            (to_master, stream)
        }
    };
    let (old, mut old_stream) = open(register(PROTOCOL_VERSION)).await;
    assert!(matches!(
        next(&mut old_stream).await,
        master_message::Message::Registered(_)
    ));
    hub.core()
        .lock()
        .await
        .disconnected("w1", fixed_now())
        .await
        .unwrap();
    let mut again = register(PROTOCOL_VERSION);
    if let Some(worker_message::Message::Register(register)) = again.message.as_mut() {
        register.incarnation_id = "i2".to_owned();
    }
    let (_new, mut new_stream) = open(again).await;
    assert!(matches!(
        next(&mut new_stream).await,
        master_message::Message::Registered(_)
    ));
    let state = || {
        let db = db.clone();
        async move { db.workers().await.unwrap()[0].state }
    };
    // The old stream's message is dropped, and its end changes nothing (given time to be
    // handled: without the fix this fails well within it).
    old.send(wrap(worker_message::Message::Draining(pb::Draining {})))
        .await
        .unwrap();
    drop(old);
    drop(old_stream);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(state().await, schedr_model::WorkerState::Active);
    assert!(hub.core().lock().await.is_live("w1"));
}

#[tokio::test]
async fn a_replaced_stream_gets_no_more_heartbeat_answers() {
    // Decision 443 (B230): a new incarnation's registration is the master's last word to the
    // old one, so the old process's lease runs out from it; the new stream is answered.
    let (hub, _db, client) = connected().await;
    let open = |message: pb::WorkerMessage| {
        let mut client = client.clone();
        async move {
            let (to_master, outbound) = mpsc::channel(16);
            to_master.send(message).await.unwrap();
            let stream = client
                .control(ReceiverStream::new(outbound))
                .await
                .unwrap()
                .into_inner();
            (to_master, stream)
        }
    };
    let (old, mut old_stream) = open(register(PROTOCOL_VERSION)).await;
    assert!(matches!(
        next(&mut old_stream).await,
        master_message::Message::Registered(_)
    ));
    // Its transport still open, the old stream taken for gone (a lease run out, B44's case).
    hub.core()
        .lock()
        .await
        .disconnected("w1", fixed_now())
        .await
        .unwrap();
    let mut again = register(PROTOCOL_VERSION);
    if let Some(worker_message::Message::Register(register)) = again.message.as_mut() {
        register.incarnation_id = "i2".to_owned();
    }
    let (new, mut new_stream) = open(again).await;
    assert!(matches!(
        next(&mut new_stream).await,
        master_message::Message::Registered(_)
    ));
    let beat = |sequence| {
        wrap(worker_message::Message::Heartbeat(pb::Heartbeat {
            sequence,
            ..pb::Heartbeat::default()
        }))
    };
    // Whether an answer came, or else whether the stream ended (not merely went quiet).
    let acked = async |stream: &mut tonic::Streaming<pb::MasterMessage>| {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            match tokio::time::timeout_at(deadline, stream.message()).await {
                Ok(Ok(Some(message))) => {
                    if matches!(
                        message.message,
                        Some(master_message::Message::HeartbeatAck(_))
                    ) {
                        return (true, false);
                    }
                }
                Ok(_) => return (false, true),
                Err(_) => return (false, false),
            }
        }
    };
    old.send(beat(1)).await.unwrap();
    // Unanswered, and let go at that heartbeat (Grok's review: a quiet 500 ms passed too).
    assert_eq!(
        acked(&mut old_stream).await,
        (false, true),
        "the old stream"
    );
    new.send(beat(1)).await.unwrap();
    assert_eq!(
        acked(&mut new_stream).await,
        (true, false),
        "the new stream"
    );
}

#[tokio::test]
async fn a_reconnect_waiting_behind_a_round_keeps_its_running_attempts() {
    // B50, through a stall: the round that would take the attempt back as never started runs
    // before the registration reporting it running is handled.
    let _clock = own_clock().await;
    let (hub, db, mut client) = connected_with(moving_now).await;
    let (to_master, stream) = parsed_worker(&mut client).await;
    db.insert_command(&trigger_sync(), "anna", moving_now())
        .await
        .unwrap();
    let mut stream = stream;
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    // Its `Started` is lost with the connection.
    drop(to_master);
    drop(stream);
    for _ in 0..100 {
        if !hub.core().lock().await.is_live("w1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let stuck = hub.core().lock_owned().await;
    move_to(5);
    let round = tokio::spawn({
        let hub = hub.clone();
        async move { hub.tick().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut again = register(PROTOCOL_VERSION);
    if let Some(worker_message::Message::Register(register)) = again.message.as_mut() {
        register.journal = vec![pb::JournalEntry {
            handles: 0,
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
    // The round goes first, past the start timeout.
    move_to(11);
    drop(stuck);
    round.await.unwrap().unwrap();
    let mut stream = reconnect.await.unwrap().unwrap().into_inner();
    let master_message::Message::Registered(registered) = next(&mut stream).await else {
        panic!("expected Registered")
    };
    assert_eq!(
        registered.decisions[0].decision,
        i32::from(pb::AttemptDecision::Continue)
    );
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None);
    assert!(attempt.started);
    drop(to_master);
}

#[tokio::test]
async fn an_open_stream_of_a_worker_taken_for_lost_changes_nothing_and_ends() {
    let _clock = own_clock().await;
    let (hub, db, client) = connected_with(moving_now).await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .clone()
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    // Silent past its lease (its transport open).
    move_to(61);
    hub.tick().await.unwrap();
    let state = || {
        let db = db.clone();
        async move { db.workers().await.unwrap()[0].state }
    };
    assert_eq!(state().await, schedr_model::WorkerState::Lost);
    to_master
        .send(wrap(worker_message::Message::Draining(pb::Draining {})))
        .await
        .unwrap();
    // The stream ends: the worker registers again and its journal is reconciled.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.message().await {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return,
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the stream stayed open");
    assert_eq!(state().await, schedr_model::WorkerState::Lost);
}

#[tokio::test]
async fn a_worker_that_stops_reading_holds_up_no_delivery() {
    // Deliveries never wait on a worker: one that stops reading backs up only its own
    // messages, not the master's rounds, registrations or other workers' messages.
    let (hub, _db, _client) = connected().await;
    let (slow, _unread) = mpsc::channel(STREAM_BUFFER);
    let (other, mut read) = mpsc::channel(STREAM_BUFFER);
    {
        let mut streams = hub.streams.lock().await;
        streams.insert("slow".to_owned(), slow);
        streams.insert("other".to_owned(), other);
    }
    let to = |worker: &str| Outgoing {
        worker_id: worker.to_owned(),
        message: pb::MasterMessage::default(),
    };
    let outgoing: Vec<Outgoing> = (0..5000).map(|_| to("slow")).chain([to("other")]).collect();
    let delivered = tokio::time::timeout(Duration::from_secs(5), hub.deliver(outgoing)).await;
    assert!(delivered.is_ok(), "a delivery waited on a worker");
    assert!(read.try_recv().is_ok(), "the other worker got its message");
    // Its messages don't pile up: the stream of the one that stopped reading is dropped.
    assert!(!hub.streams.lock().await.contains_key("slow"));
}

#[tokio::test]
async fn a_refused_registration_keeps_no_start_of_its_journal() {
    let (hub, _db, mut client) = connected().await;
    // `a0`'s start known before (a `Started` that couldn't be stored): it stays.
    hub.arrivals.start_arrived("w1", "a0");
    let mut old = register(pb::ProtocolVersion { major: 9, minor: 0 });
    if let Some(worker_message::Message::Register(register)) = old.message.as_mut() {
        register.journal = ["a0", "a1"]
            .map(|id| pb::JournalEntry {
                handles: 0,
                attempt_id: id.to_owned(),
                state: pb::JournalState::Running.into(),
            })
            .to_vec();
    }
    let status = client
        .control(tokio_stream::iter(vec![old]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(!hub.arrivals.start_waiting("a1"));
    assert!(hub.arrivals.start_waiting("a0"));
}

#[tokio::test]
async fn a_start_known_but_not_stored_goes_once_the_worker_registers_without_it() {
    // A `Started` that couldn't be stored keeps its attempt from expiring; once the worker
    // registers again (restarted: the process gone) without it in its journal, it expires.
    let _clock = own_clock().await;
    let (hub, db, mut client) = connected_with(moving_now).await;
    let (to_master, mut stream) = parsed_worker(&mut client).await;
    db.insert_command(&trigger_sync(), "anna", moving_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    hub.arrivals.start_arrived("w1", &assign.attempt_id);
    drop(to_master);
    drop(stream);
    for _ in 0..100 {
        if !hub.core().lock().await.is_live("w1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut again = register(PROTOCOL_VERSION);
    if let Some(worker_message::Message::Register(register)) = again.message.as_mut() {
        register.incarnation_id = "i2".to_owned();
    }
    let (_to_master, mut stream) = {
        let (to_master, outbound) = mpsc::channel(16);
        to_master.send(again).await.unwrap();
        let stream = client
            .control(ReceiverStream::new(outbound))
            .await
            .unwrap()
            .into_inner();
        (to_master, stream)
    };
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    // Not at the old 10 s: the start timeout is the worker lease (decision 281, B163).
    move_to(11);
    hub.tick().await.unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None);
    move_to(61);
    hub.tick().await.unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(
        attempt.status,
        Some(schedr_model::AttemptStatus::AssignmentExpired)
    );
}

#[tokio::test]
async fn a_worker_that_stops_reading_is_disconnected() {
    let (hub, _db, mut client) = connected().await;
    let (_to_master, _unread) = parsed_worker(&mut client).await;
    let core = hub.core();
    for _ in 0..100 {
        let flood: Vec<Outgoing> = (0..STREAM_BUFFER)
            .map(|_| Outgoing {
                worker_id: "w1".to_owned(),
                message: pb::MasterMessage::default(),
            })
            .collect();
        hub.deliver(flood).await;
        if !hub.streams.lock().await.contains_key("w1") {
            break;
        }
    }
    assert!(!hub.streams.lock().await.contains_key("w1"), "never full");
    for _ in 0..100 {
        if !core.lock().await.is_live("w1") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the worker stayed live");
}

#[tokio::test]
async fn a_registration_with_more_new_roots_than_a_stream_holds_gets_every_parse() {
    // B45: a registration's own messages (`Registered`, then a parse for each new root) go
    // into its stream before the worker can read any; more than the stream holds once took
    // the worker for one that stopped reading.
    let (hub, _db, mut client) = connected().await;
    let roots = STREAM_BUFFER + 10;
    let mut message = register(PROTOCOL_VERSION);
    let Some(worker_message::Message::Register(registration)) = &mut message.message else {
        unreachable!()
    };
    registration.roots = (0..roots)
        .map(|i| pb::RootHash {
            root: format!("etl{i}"),
            ..root()
        })
        .collect();
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(message).await.unwrap();
    let mut stream = tokio::time::timeout(
        // About 4 s alone; only an upper bound, for a loaded machine.
        Duration::from_secs(60),
        client.control(ReceiverStream::new(outbound)),
    )
    .await
    .expect("registered in time")
    .unwrap()
    .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    for _ in 0..roots {
        assert!(matches!(
            next(&mut stream).await,
            master_message::Message::AssignParse(_)
        ));
    }
    assert!(hub.core().lock().await.is_live("w1"));
    assert!(hub.streams.lock().await.contains_key("w1"));
    drop(to_master);
}

#[tokio::test]
async fn a_message_that_cant_be_handled_ends_the_stream() {
    // B101: a report the master failed to handle isn't sent again on the same stream (the
    // agent reports a hash once); ending the stream makes the worker register again,
    // reporting its roots afresh.
    let (_hub, db, client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .clone()
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    // A failure of the database's own (not of the data): may pass when sent again.
    db.fail_write(1);
    to_master.send(code_version("h2")).await.unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match stream.message().await {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => return,
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the stream stayed open");
    let state = db.workers().await.unwrap()[0].state;
    assert_eq!(state, schedr_model::WorkerState::Disconnected);
}

#[tokio::test]
async fn a_message_the_database_refuses_for_its_data_is_dropped_and_the_stream_goes_on() {
    // B106: ended, the stream came back with the same message after every reconnect, refused
    // again, the worker's later messages behind it.
    let (_hub, db, client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .clone()
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    // Only h2 refused: messages are handled in order, so h3's answer comes after h2's drop.
    break_db(&db, "INSERT ON root_hashes WHEN NEW.content_hash = 'h2'").await;
    to_master.send(code_version("h2")).await.unwrap();
    to_master.send(code_version("h3")).await.unwrap();
    loop {
        // The registration's own parse (h1) first.
        if let master_message::Message::AssignParse(job) = next(&mut stream).await
            && job.root.as_ref().unwrap().content_hash != "h1"
        {
            assert_eq!(job.root.unwrap().content_hash, "h3");
            break;
        }
    }
    let state = db.workers().await.unwrap()[0].state;
    assert_eq!(state, schedr_model::WorkerState::Active);
    drop(to_master);
}

fn code_version(hash: &str) -> pb::WorkerMessage {
    wrap(worker_message::Message::CodeVersionChanged(
        pb::CodeVersionChanged {
            root: Some(pb::RootHash {
                content_hash: hash.to_owned(),
                ..root()
            }),
        },
    ))
}

#[tokio::test(start_paused = true)]
async fn the_pause_after_a_notifications_failure_ends_by_itself() {
    // Codex's review of the Deployments batch: the failure's problem was cleared only by the
    // next command notification, so a listener that reconnected by itself, with no command
    // since, reported it for good. The pause's end clears it.
    let problems = Problems::default();
    problems.set(ProblemKind::Notifications, false, "failed".to_owned());
    let started = tokio::time::Instant::now();
    let mut paused_until = Some(started + NOTIFICATION_RETRY);
    let waited = tokio::time::timeout(
        NOTIFICATION_RETRY + Duration::from_secs(1),
        next_command(None, &mut paused_until, &problems),
    )
    .await;
    // Without notifications nothing comes; the pause ended meanwhile.
    assert!(waited.is_err());
    assert_eq!(paused_until, None);
    assert_eq!(problems.now(), []);
    // Cut short during the pause: still paused, still reported.
    problems.set(ProblemKind::Notifications, false, "failed".to_owned());
    let until = tokio::time::Instant::now() + NOTIFICATION_RETRY;
    let mut paused_until = Some(until);
    let cut = tokio::time::timeout(
        Duration::from_secs(1),
        next_command(None, &mut paused_until, &problems),
    )
    .await;
    assert!(cut.is_err());
    assert_eq!(paused_until, Some(until));
    assert_eq!(problems.now().len(), 1);
}

/// B200's review: once the master closes its streams, nothing holds a worker stream's sender
/// any more, so the stream ends even when it was too full to take the stopping status
/// (its server used to keep a sender while it waited for the worker's messages).
#[tokio::test]
async fn closing_the_streams_lets_every_sender_of_a_stream_go() {
    let (hub, _db, client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .clone()
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    let weak = hub
        .streams
        .lock()
        .await
        .values()
        .next()
        .unwrap()
        .downgrade();
    // The worker's side stays open: its server would wait for its messages for ever.
    hub.close_streams().await;
    let released = tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(released.is_ok(), "a sender of the stream is still held");
    drop(to_master);
}

/// B219: a stream that hasn't sent `Register` yet ends too when the master closes its streams
/// (its call held tonic's graceful shutdown).
#[tokio::test]
async fn closing_the_streams_ends_one_waiting_for_its_register() {
    let (hub, _db, client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    let call = tokio::spawn(async move {
        let mut client = client.clone();
        client.control(ReceiverStream::new(outbound)).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    hub.close_streams().await;
    let ended = tokio::time::timeout(Duration::from_secs(5), call).await;
    let status = ended
        .expect("the call is still waiting")
        .unwrap()
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    drop(to_master);
}

/// B219's review: an upload of a parse result left unfinished ends too when the master
/// closes its streams.
#[tokio::test]
async fn closing_the_streams_ends_an_unfinished_upload() {
    let (hub, _db, client) = connected().await;
    let (to_master, parts) = mpsc::channel::<pb::ParseResultPart>(16);
    let call = tokio::spawn(async move {
        let mut client = client.clone();
        client.upload_parse_result(ReceiverStream::new(parts)).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    hub.close_streams().await;
    let ended = tokio::time::timeout(Duration::from_secs(5), call).await;
    let status = ended
        .expect("the upload is still waiting")
        .unwrap()
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    drop(to_master);
}

/// B219's second review: an upload or a registration waiting for this master to lead again
/// stops waiting once it closes its streams.
#[tokio::test]
async fn a_wait_for_leadership_ends_when_the_streams_close() {
    let (hub, _db, _client) = connected().await;
    hub.set_leading(false);
    let waiting = tokio::spawn({
        let hub = hub.clone();
        async move { hub.leading_unless_closing().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiting.is_finished(),
        "it waits while the master doesn't lead"
    );
    hub.close_streams().await;
    let ended = tokio::time::timeout(Duration::from_secs(5), waiting).await;
    assert!(!ended.expect("still waiting").unwrap());
}
