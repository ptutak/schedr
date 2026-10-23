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
    db.migrate().await.unwrap();
    let hub = Hub::with_clock(MasterCore::new(db.clone(), Settings::default()), clock);
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
