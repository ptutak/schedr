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
            schedr_version: "1.0.0".to_owned(),
            slots: 2,
            roots: vec![root()],
            ..pb::Register::default()
        })),
        clock: Some(pb::WorkerClock::default()),
    }
}

/// `message` unstamped (an older worker's, or one whose numbers don't matter here).
fn wrap(message: worker_message::Message) -> pb::WorkerMessage {
    pb::WorkerMessage {
        message: Some(message),
        clock: None,
    }
}

/// `message` numbered `sequence` on its stream, naming the master's messages up to `heard`
/// as handled (B240).
fn numbered(message: worker_message::Message, sequence: u64, heard: u64) -> pb::WorkerMessage {
    pb::WorkerMessage {
        message: Some(message),
        clock: Some(pb::WorkerClock {
            sequence,
            heard_sequence: heard,
        }),
    }
}

/// The round's results part once a turn has queued something for it (M5.2cB): what the loop
/// does, these tests running no loop.
async fn results_handled(hub: &Hub) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hub.queued_results().is_empty() {
        assert!(std::time::Instant::now() < deadline, "a result queued");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
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
            "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
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
        schedr_version: "1.0.0".to_owned(),
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
            ..pb::Started::default()
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
    results_handled(&hub).await;
    let master_message::Message::FinishedAck(ack) = next(&mut stream).await else {
        panic!("expected FinishedAck")
    };
    assert_eq!(ack.attempt_id, assign.attempt_id);
    // M5.1a: the lock's waits and the parts are recorded where they happen; the `Started` and
    // `Finished` in the round's results part, their turn taking no lock (M5.2cB).
    let mut registry = prometheus_client::registry::Registry::with_prefix("schedr");
    hub.metrics.register(&mut registry);
    let mut scraped = String::new();
    prometheus_client::encoding::text::encode(&mut scraped, &registry).unwrap();
    for series in [
        "schedr_master_core_lock_wait_seconds_count{by=\"round\"}",
        "schedr_master_core_lock_wait_seconds_count{by=\"upload\"}",
        "schedr_master_round_part_seconds_count{part=\"results\"}",
    ] {
        let count = scraped
            .lines()
            .find_map(|line| line.strip_prefix(series))
            .and_then(|rest| rest.trim().parse::<u64>().ok());
        assert!(count.is_some_and(|n| n >= 1), "{series}\n{scraped}");
    }

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

/// Defines `id`, started by `trigger`, unpaused (a new trigger starts paused, decision 459).
async fn define_unpaused(db: &Database, id: &str, trigger: serde_json::Value) {
    let dag: schedr_model::Dag = serde_json::from_value(json!({
        "produced_by": "1.0", "id": id, "location": {"root": "etl", "path": format!("{id}.py")},
        "trigger": trigger, "initial_since": null, "max_active_runs": 5,
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
    .unwrap();
    let parse_id = db
        .insert_parse(&schedr_db::dags::NewParse {
            root: "etl",
            content_hash: id,
            schedr_version: "1.0.0",
            worker_id: "w1",
            parsed_at: fixed_now(),
            files: &[],
        })
        .await
        .unwrap();
    let change = schedr_db::dags::DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], fixed_now()).await.unwrap();
    db.set_paused(&DagId::new(id).unwrap(), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_posted_event_starts_its_run_from_the_trigger_loop_without_a_tick() {
    // Decisions 485 and 486: the trigger pass is the trigger loop's, woken by a write (on
    // Postgres a notification too), and the run it queues wakes the round: with a tick of ten
    // minutes, the event posted after the first round still has its run running within moments.
    let (hub, db, _client) = connected_with(Timestamp::now).await;
    define_unpaused(
        &db,
        "paid",
        json!({"kind": "event_named", "event": {"name": "paid"}}),
    )
    .await;
    // Running: the round the loop woke (as a `Finished` does) started it.
    let runs_of_paid = |db: Database| async move {
        let filter = schedr_db::reads::RunFilter {
            dag_id: Some(DagId::new("paid").unwrap()),
            state: Some(schedr_model::RunState::Running),
            limit: 10,
            ..Default::default()
        };
        db.runs(&filter).await.unwrap().len()
    };
    let posted = {
        let db = db.clone();
        async move {
            // After the loop's first round and pass.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let event = schedr_db::named_events::NewNamedEvent {
                name: "paid",
                key: "order-1",
                data: &json!({}),
                at: Timestamp::now(),
            };
            db.post_named_event(&event, "anna", None, Timestamp::now())
                .await
                .unwrap();
            let posted = std::time::Instant::now();
            loop {
                if runs_of_paid(db.clone()).await == 1 {
                    return posted.elapsed();
                }
                assert!(
                    posted.elapsed() < Duration::from_secs(5),
                    "no run of `paid`"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    let (took, ()) = tokio::join!(
        posted,
        hub.run(
            Duration::from_mins(10),
            None,
            tokio::time::sleep(Duration::from_secs(6))
        )
    );
    assert!(
        took < Duration::from_secs(1),
        "the event's run took {took:?}"
    );
}

#[tokio::test]
async fn a_failed_trigger_pass_is_tried_again_at_the_next_tick() {
    // Grok's review of M5.1f2: a pass that failed had no wake left (its write consumed); the
    // next tick wakes the loop again, as a failed round is tried at the next.
    let (hub, _db, _client) = connected().await;
    hub.triggers_failed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    hub.tick().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), hub.triggers_due.notified())
        .await
        .expect("the trigger loop woken");
    assert!(
        !hub.triggers_failed
            .load(std::sync::atomic::Ordering::SeqCst),
        "once"
    );
}

#[tokio::test]
async fn every_message_carries_the_masters_clock_and_its_number() {
    // B225: the clock on every message names this master's timeline, master time and the
    // latest worker message heard on the stream; B240: and its own number on the stream (an
    // assignment has no start deadline).
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
    to_master
        .send(numbered(
            worker_message::Message::Heartbeat(pb::Heartbeat::default()),
            3,
            1,
        ))
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
    let Some(master_message::Message::AssignTask(_)) = &message.message else {
        panic!("expected AssignTask")
    };
    assert!(
        clock_of(&message).sequence > 1,
        "numbered on its stream, after `Registered` (B240)"
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
    let (hub, db, mut client) = connected().await;
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
    assert!(
        status.message().contains("no master holds the lease"),
        "{status:?}"
    );
    // Decision 452: the master that leads is named, with when its lease runs out.
    db.create_lease_table().await.unwrap();
    db.take_lease("master-2:7", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let status = client
        .control(ReceiverStream::new(outbound))
        .await
        .expect_err("a stream to a master that doesn't lead");
    assert!(
        status
            .message()
            .contains("the lease is held by `master-2:7` until 20"),
        "{status:?}"
    );
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
async fn a_worker_that_left_while_a_round_held_its_registration_isnt_registered() {
    // agy's review of B297: registered all the same, it was live with no one there, and its next
    // try, in line right behind, was refused ("already has a live stream").
    let (hub, _db, mut client) = connected().await;
    let round = hub.core_for("a slow round").await;
    let (left, outbound) = mpsc::channel(16);
    left.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut leaving = client.clone();
    let first = tokio::spawn(async move { leaving.control(ReceiverStream::new(outbound)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    first.abort();
    drop(left);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let again = tokio::spawn(async move { client.control(ReceiverStream::new(outbound)).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(round);
    let mut stream = again.await.unwrap().unwrap().into_inner();
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
            ..pb::Started::default()
        })))
        .await
        .unwrap();
    // Stored by the round (M5.2cB).
    results_handled(&hub).await;
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
        // Its end waits behind what it queued, if anything: the round's (M5.2cB).
        Box::pin(hub.round_results(moving_now())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!hub.core().lock().await.is_live("w1"), "disconnected");
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

#[tokio::test]
async fn a_refused_register_doesnt_keep_the_old_stream_of_its_worker_alive() {
    // B267: each `Register` counted as a sign of life as it arrived, refused or not: a worker
    // whose old stream stayed live (its end not handled) renewed that stream's lease with each
    // try, and was refused forever.
    let _clock = own_clock().await;
    let (hub, _db, mut client) = connected_with(moving_now).await;
    let (old, outbound) = mpsc::channel(16);
    old.send(register(PROTOCOL_VERSION)).await.unwrap();
    let _old_stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    for at in (10..=70).step_by(10) {
        move_to(at);
        let (to_master, outbound) = mpsc::channel(16);
        to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
        let status = client
            .control(ReceiverStream::new(outbound))
            .await
            .expect_err("its old stream is live");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
    }
    // 70 s since it was last heard on its old stream, past the lease.
    hub.tick().await.unwrap();
    assert!(
        !hub.core().lock().await.is_live("w1"),
        "the refused registrations kept the old stream's lease"
    );
}

#[tokio::test]
async fn a_worker_still_live_whose_stream_is_gone_registers_again_at_once() {
    // B267: a stream's end waiting (behind what its worker queued for the round, or for the
    // lock) left the worker live with no stream: its registrations were refused until then.
    let (hub, _db, mut client) = connected().await;
    let (old, outbound) = mpsc::channel(16);
    old.send(register(PROTOCOL_VERSION)).await.unwrap();
    let old_stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    // The stream gone from the hub, its end not handled yet.
    hub.streams.lock().await.remove("w1");
    assert!(hub.core().lock().await.is_live("w1"));
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .expect("registered in place of the gone stream")
        .into_inner();
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::Registered(_)
    ));
    // The old stream's end, handled now, leaves the new one as it is (B44).
    drop(old);
    drop(old_stream);
    for _ in 0..20 {
        Box::pin(hub.round_results(fixed_now())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        hub.core().lock().await.is_live("w1"),
        "the new stream disconnected"
    );
    assert!(hub.streams.lock().await.contains_key("w1"));
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
            ..pb::Started::default()
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
            ..pb::Started::default()
        })))
        .await
        .unwrap();
    // The stream's reader records it at once (an in-memory connection: well within this).
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The next round comes 11 s later, the `Started` still unhandled.
    busy.tick(fixed_now() + Duration::from_secs(11))
        .await
        .unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None, "a task that started was taken back");
    drop(busy);
}

#[tokio::test]
async fn a_turn_queues_its_starts_and_results_without_waiting_for_the_lock() {
    // M5.2cB (decision 505; decision 480 before it): a turn queues its `Started`s and results
    // for the round without the master's lock, so a round (here the test) holding it doesn't
    // hold them up; the round's results part stores them together, in their order.
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
            ..pb::Started::default()
        }),
        worker_message::Message::Finished(pb::Finished {
            attempt_id: assign.attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        }),
    ] {
        to_master.send(wrap(message)).await.unwrap();
    }
    // Queued meanwhile, the lock still held: nothing stored yet.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hub.queued_results().len() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "queued under a held lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!stored(&db, &assign.attempt_id, false).await);
    drop(busy);
    results_handled(&hub).await;
    assert!(stored(&db, &assign.attempt_id, true).await);
}

#[tokio::test]
async fn a_round_checks_a_queued_messages_stream_as_a_turn_did() {
    // Decision 480, in the round since M5.2cB: between a turn queueing its `Started` and
    // `Finished` and the round handling them, the worker is taken for lost; they change
    // nothing (its stream is let go, B44), as a turn whose stream wasn't served any more.
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    let core = hub.core();
    let busy = core.lock().await;
    to_master.send(wrap(started_msg(id))).await.unwrap();
    to_master.send(wrap(finished_msg(id))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(busy);
    // Between the turn queueing them and the round.
    let mut between = core.lock().await;
    between.disconnected("w1", fixed_now()).await.unwrap();
    drop(between);
    results_handled(&hub).await;
    // The round found its stream no longer served and let it go (Grok's review: a sleep alone
    // would pass on a `Finished` stored late).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hub.streams.lock().await.contains_key("w1") {
        assert!(std::time::Instant::now() < deadline, "the stream is let go");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let attempt = db.attempt(id).await.unwrap().unwrap();
    assert_eq!(
        attempt.status, None,
        "the `Finished` of a stream no longer served"
    );
    assert!(!attempt.started, "nor its `Started`");
    assert_eq!(successes(&hub), 0);
}

#[tokio::test]
async fn a_rounds_commands_each_take_the_lock_so_others_get_in_between() {
    // Decision 483: the round takes the master's lock for each command on its own, so whoever
    // waits for it (a worker's turn; here the test) gets in between two commands.
    let (hub, db, _client) = connected().await;
    let unknown = |dag: &str| schedr_model::commands::Command::TriggerRun {
        event: None,
        params: None,
        dag_id: DagId::new(dag).unwrap(),
        planned_for: None,
    };
    for dag in ["first", "second"] {
        db.insert_command(&unknown(dag), "anna", fixed_now())
            .await
            .unwrap();
    }
    let core = hub.core();
    let busy = core.lock().await;
    let round = tokio::spawn({
        let hub = hub.clone();
        async move { hub.tick().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pending = async || db.pending_commands(10).await.unwrap().len();
    drop(busy);
    // Next in line after the round's reading of its commands: neither applied yet.
    let between = core.lock().await;
    assert_eq!(pending().await, 2);
    drop(between);
    // After its first command: one decided, the other still waiting for the lock.
    let between = core.lock().await;
    assert_eq!(pending().await, 1);
    drop(between);
    round.await.unwrap().unwrap();
    assert_eq!(pending().await, 0);
}

/// A worker with one task assigned, its stream and the assignment.
async fn assigned_one(
    hub: &Hub,
    db: &Database,
    client: &mut MasterClient<Channel>,
) -> (
    mpsc::Sender<pb::WorkerMessage>,
    Streaming<pb::MasterMessage>,
    pb::AssignTask,
) {
    let (to_master, mut stream) = parsed_worker(client).await;
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    (to_master, stream, assign)
}

fn started_msg(attempt_id: &str) -> worker_message::Message {
    worker_message::Message::Started(pb::Started {
        attempt_id: attempt_id.to_owned(),
        start_method: pb::StartMethod::Spawn.into(),
        ..pb::Started::default()
    })
}

fn finished_msg(attempt_id: &str) -> worker_message::Message {
    worker_message::Message::Finished(pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    })
}

/// Sends `messages` while the master's lock is held, so they wait and are handled in one turn;
/// the commits that turn made.
async fn in_one_turn(
    hub: &Hub,
    db: &Database,
    to_master: &mpsc::Sender<pb::WorkerMessage>,
    messages: Vec<worker_message::Message>,
    before_turn: impl FnOnce(),
) -> u64 {
    let core = hub.core();
    let busy = core.lock().await;
    for message in messages {
        to_master.send(wrap(message)).await.unwrap();
    }
    // The worker's handler is waiting for the lock by then (an in-memory connection).
    tokio::time::sleep(Duration::from_millis(200)).await;
    before_turn();
    let before = db.commits();
    drop(busy);
    // The turn takes the lock for each of its transactions (decision 480): done once the lock
    // is free and no commit has come for a while (nothing else writes in these tests).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last = (db.commits(), std::time::Instant::now());
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let commits = db.commits();
        if commits != last.0 {
            last = (commits, std::time::Instant::now());
        } else if core.try_lock().is_ok() && last.1.elapsed() > Duration::from_millis(300) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the turn ends");
    }
    // Its results, queued for the round (M5.2cB): that round's results part.
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    db.commits() - before
}

/// Whether the attempt `id` is stored started (`ended`: and ended).
async fn stored(db: &Database, id: &str, ended: bool) -> bool {
    let attempt = db.attempt(id).await.unwrap().unwrap();
    attempt.started && (!ended || attempt.status.is_some())
}

fn successes(hub: &Hub) -> u64 {
    hub.metrics
        .attempts_ended
        .get_or_create(&crate::metrics::StatusLabels {
            status: "success".to_owned(),
        })
        .get()
}

#[tokio::test]
async fn a_turns_starts_and_finishes_are_one_transaction_of_the_round() {
    // M4B.3 (decision 397), in the round since M5.2cB: five messages, the `Started`s stored
    // before the `Finished`s in one transaction, the attempt ended once.
    let (hub, db, mut client) = connected().await;
    let (to_master, mut stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    let turn = vec![
        started_msg(id),
        started_msg(id),
        finished_msg(id),
        started_msg(id),
        finished_msg(id),
    ];
    let commits = in_one_turn(&hub, &db, &to_master, turn, || {}).await;
    assert_eq!(commits, 1);
    let attempt = db.attempt(id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Success));
    assert_eq!(successes(&hub), 1);
    // Each result acknowledged.
    for _ in 0..2 {
        assert!(matches!(
            next(&mut stream).await,
            master_message::Message::FinishedAck(_)
        ));
    }
}

#[tokio::test]
async fn a_failed_batch_is_redone_as_its_workers_group_and_counted_once() {
    // M4B.3, in the round since M5.2cB: the batch fails at its first write; rolled back, the
    // worker's stretch is redone as one group: stored, the end counted once.
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    let turn = vec![started_msg(id), started_msg(id), finished_msg(id)];
    let commits = in_one_turn(&hub, &db, &to_master, turn, || db.fail_write(1)).await;
    // The batch failed (no commit), then the worker's group, `Started`s before the `Finished`.
    assert_eq!(commits, 1);
    assert!(stored(&db, id, true).await);
    assert_eq!(successes(&hub), 1);
}

#[tokio::test]
async fn a_worker_deregistering_after_its_result_is_forgotten_only_after_the_result_is_stored() {
    // M5.2cB: the `Finished` waits for the round; the `Deregister` after it in the same turn
    // waits behind it, so the worker isn't forgotten (its attempt ended `lost`) first.
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    let turn = vec![
        started_msg(id),
        finished_msg(id),
        worker_message::Message::Deregister(pb::Deregister {}),
    ];
    in_one_turn(&hub, &db, &to_master, turn, || {}).await;
    let attempt = db.attempt(id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Success));
    assert_eq!(successes(&hub), 1);
    assert!(!hub.core().lock().await.is_live("w1"), "then deregistered");
}

#[tokio::test]
async fn what_a_worker_said_before_its_stream_ended_still_counts_and_then_it_is_offline() {
    // M5.2cB (make check's draining test): the worker sends its result and `Deregister`, then
    // closes its stream, all before the round; the stream's end waits behind them, so the
    // result is stored and the worker ends offline, not disconnected (then lost).
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.clone();
    let core = hub.core();
    let busy = core.lock().await;
    for message in [
        started_msg(&id),
        finished_msg(&id),
        worker_message::Message::Deregister(pb::Deregister {}),
    ] {
        to_master.send(wrap(message)).await.unwrap();
    }
    drop(to_master);
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(busy);
    // The stream's end is queued behind them.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !hub.queued_results().iter().any(|r| r.message.is_none()) {
        assert!(
            std::time::Instant::now() < deadline,
            "the stream's end queued"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    let attempt = db.attempt(&id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Success));
    assert_eq!(
        db.workers().await.unwrap()[0].state,
        schedr_model::WorkerState::Offline
    );
}

/// B263, the turn's path (its case in `make test-integration`): a worker's `Draining`, handled
/// in its turn, whose statement runs past `query_timeout` (a master frozen in it) is tried once
/// more instead of ending the stream: the worker is draining, still connected.
#[tokio::test]
async fn a_turns_message_whose_statement_ran_past_its_limit_is_tried_once_more() {
    let (hub, db, mut client) = connected_limited().await;
    let (to_master, _stream, _assign) = assigned_one(&hub, &db, &mut client).await;
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("sqlite")
    };
    let held = pool.acquire().await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(QUERY_LIMIT + Duration::from_millis(500)).await;
        drop(held);
    });
    to_master
        .send(wrap(worker_message::Message::Draining(pb::Draining {})))
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        // Its own read waits for the connection held too: not yet.
        let Ok(workers) = db.workers().await else {
            continue;
        };
        let state = workers[0].state;
        if state == schedr_model::WorkerState::Draining {
            break;
        }
        assert_ne!(
            state,
            schedr_model::WorkerState::Disconnected,
            "its stream ended on the timeout"
        );
        assert!(std::time::Instant::now() < deadline, "still {state:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(hub.core().lock().await.is_live("w1"), "still connected");
}

/// B263, the round's path: a `Deregister` whose statement runs past `query_timeout` (a master
/// frozen in it, its time counted on the thaw; here the only connection held past the limit)
/// is tried once more instead of being dropped: the worker ends offline, not disconnected.
#[tokio::test]
async fn a_rounds_message_whose_statement_ran_past_its_limit_is_tried_once_more() {
    let (hub, db, mut client) = connected_limited().await;
    let (to_master, _stream, _assign) = assigned_one(&hub, &db, &mut client).await;
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("sqlite")
    };
    let core = hub.core();
    let busy = core.lock().await;
    to_master
        .send(wrap(worker_message::Message::Deregister(pb::Deregister {})))
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hub.queued_results().is_empty() {
        assert!(std::time::Instant::now() < deadline, "queued");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The only connection held a little past the limit, as a stall a thaw ends: the round's
    // statement for it times out once, the try again gets the connection.
    let held = pool.acquire().await.unwrap();
    tokio::spawn(async move {
        tokio::time::sleep(QUERY_LIMIT + Duration::from_millis(500)).await;
        drop(held);
    });
    drop(busy);
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        // Its own read waits for the connection held too: not yet.
        let Ok(workers) = db.workers().await else {
            continue;
        };
        let state = workers[0].state;
        if state == schedr_model::WorkerState::Offline {
            break;
        }
        assert_ne!(
            state,
            schedr_model::WorkerState::Disconnected,
            "its stream ended on the timeout"
        );
        assert!(std::time::Instant::now() < deadline, "still {state:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_queued_message_that_cant_be_handled_ends_the_stream_and_drops_what_came_after() {
    // M5.2cB, as a turn's end did (B101): the stream let go, the worker disconnected (so it
    // registers again), and its `Deregister` after the bad message dropped, not forgetting the
    // worker before what it sent is sorted out.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream, _assign) = assigned_one(&hub, &db, &mut client).await;
    let outbound = hub.streams.lock().await.get("w1").unwrap().clone();
    let mut messages = vec![
        worker_message::Message::Rejected(pb::Rejected {
            attempt_id: "01a00000-0000-7000-8000-000000000001".to_owned(),
            reason: 999,
            ..pb::Rejected::default()
        }),
        worker_message::Message::Deregister(pb::Deregister {}),
    ];
    hub.queue_results("w1", &outbound, &mut messages);
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    assert!(
        !hub.streams.lock().await.contains_key("w1"),
        "the stream let go"
    );
    assert_eq!(
        db.workers().await.unwrap()[0].state,
        schedr_model::WorkerState::Disconnected,
        "disconnected, not forgotten by the `Deregister` after it"
    );
}

#[tokio::test]
async fn a_read_before_the_lock_that_fails_doesnt_stop_the_round() {
    // Codex's review of M5.2c: a queued message the round's read before the lock fails on (here
    // an attempt's row that can't be read; on Postgres a malformed ID) must not fail every
    // round with the message left queued: it is handled under the lock, the worker's stream
    // ended over it, and the queue moves on.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    let mut connection = pool.acquire().await.unwrap();
    for statement in [
        "PRAGMA ignore_check_constraints = ON",
        "UPDATE attempts SET status = 'bogus' WHERE id = ?",
        "PRAGMA ignore_check_constraints = OFF",
    ] {
        sqlx::query(statement)
            .bind(&assign.attempt_id)
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    drop(connection);
    let outbound = hub.streams.lock().await.get("w1").unwrap().clone();
    hub.queue_results("w1", &outbound, &mut vec![finished_msg(&assign.attempt_id)]);
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    assert!(hub.queued_results().is_empty(), "the queue moved on");
}

#[tokio::test]
async fn nothing_of_a_worker_counts_in_the_round_after_its_message_ended_its_stream() {
    // Codex's review of M5.2c: a malformed `Finished` fails the batch, then alone ends the
    // worker's stream; its `Deregister` after it must not then forget the worker.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let outbound = hub.streams.lock().await.get("w1").unwrap().clone();
    let mut messages = vec![
        worker_message::Message::Finished(pb::Finished {
            attempt_id: assign.attempt_id.clone(),
            status: 999,
            ..pb::Finished::default()
        }),
        worker_message::Message::Deregister(pb::Deregister {}),
    ];
    hub.queue_results("w1", &outbound, &mut messages);
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    assert_eq!(
        db.workers().await.unwrap()[0].state,
        schedr_model::WorkerState::Disconnected,
        "disconnected, not forgotten by the `Deregister` after it"
    );
}

#[tokio::test]
async fn a_queued_message_of_a_replaced_stream_is_dropped_once_the_newer_one_ended_too() {
    // Codex's review of M5.2c: the worker registered again (a newer stream), and that one
    // ended before the round, so no stream is in place; the old stream's queued `Deregister`
    // must not count (it would forget the worker the newer stream belongs to).
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream, _assign) = assigned_one(&hub, &db, &mut client).await;
    let old = hub.streams.lock().await.get("w1").unwrap().clone();
    // A newer registration's stream, ended since.
    let _newer = hub.arrivals.new_stream("w1");
    hub.streams.lock().await.remove("w1");
    let mut messages = vec![worker_message::Message::Deregister(pb::Deregister {})];
    hub.queue_results("w1", &old, &mut messages);
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    assert!(hub.core().lock().await.is_live("w1"), "not forgotten");
}

#[tokio::test]
async fn a_closing_master_queues_nothing_more_for_the_round() {
    // Codex's review of M5.2c: a turn queueing after `close_streams` emptied the queue would
    // keep its stream's sender, and so the stream, open with no round left to take it.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let outbound = hub.streams.lock().await.get("w1").unwrap().clone();
    hub.close_streams().await;
    hub.queue_results("w1", &outbound, &mut vec![finished_msg(&assign.attempt_id)]);
    assert!(hub.queued_results().is_empty());
}

#[tokio::test]
async fn a_result_counted_in_a_failed_group_isnt_counted_again_when_redone() {
    // M4B.3: the `Finished`s' group fails after its attempt's end was written (its second
    // write); redone alone, the end is counted once, not once for the rolled-back try too.
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    in_one_turn(&hub, &db, &to_master, vec![started_msg(id)], || {}).await;
    in_one_turn(&hub, &db, &to_master, vec![finished_msg(id)], || {
        db.fail_write(2);
    })
    .await;
    let attempt = db.attempt(id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Success));
    assert_eq!(
        db.writes(),
        2 + 4,
        "failed after its first write, then redone whole"
    );
    assert_eq!(successes(&hub), 1);
}

#[tokio::test]
async fn a_group_whose_commit_answer_was_lost_is_read_back_once_and_counted_once() {
    // M4B.3: the `Finished`s' commit stores but its answer is lost; read back by its last
    // message, it stands: acknowledged, counted once, not redone.
    let (hub, db, mut client) = connected().await;
    let (to_master, mut stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    in_one_turn(&hub, &db, &to_master, vec![started_msg(id)], || {}).await;
    let commits = in_one_turn(&hub, &db, &to_master, vec![finished_msg(id)], || {
        db.lose_commit_answer();
    })
    .await;
    assert_eq!(commits, 1, "not redone");
    assert_eq!(successes(&hub), 1);
    assert!(matches!(
        next(&mut stream).await,
        master_message::Message::FinishedAck(_)
    ));
}

/// As [`assigned_one`], its attempt started and ended in a turn of its own, and the next run's
/// assigned and started: the stream, the ended attempt and the started one.
async fn one_ended_one_started(
    hub: &Hub,
    db: &Database,
    client: &mut MasterClient<Channel>,
) -> (
    mpsc::Sender<pb::WorkerMessage>,
    Streaming<pb::MasterMessage>,
    String,
    String,
) {
    let (to_master, mut stream, assign) = assigned_one(hub, db, client).await;
    let ended = assign.attempt_id;
    in_one_turn(
        hub,
        db,
        &to_master,
        vec![started_msg(&ended), finished_msg(&ended)],
        || {},
    )
    .await;
    // `sync` has one task: the next attempt is its next run's.
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let load = loop {
        if let master_message::Message::AssignTask(assign) = next(&mut stream).await {
            break assign.attempt_id;
        }
    };
    in_one_turn(hub, db, &to_master, vec![started_msg(&load)], || {}).await;
    (to_master, stream, ended, load)
}

#[tokio::test]
async fn a_group_cut_off_unstored_isnt_taken_as_stored_by_a_message_that_changed_nothing() {
    // Grok's review of M4B.3: read back by its last message, already ended before, a group
    // whose commit never landed was taken as stored: the fresh result acknowledged and lost.
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, ended, load) = one_ended_one_started(&hub, &db, &mut client).await;
    let turn = vec![finished_msg(&load), finished_msg(&ended)];
    in_one_turn(&hub, &db, &to_master, turn, || db.lose_commit_unstored()).await;
    let attempt = db.attempt(&load).await.unwrap().unwrap();
    assert_eq!(
        attempt.status,
        Some(schedr_model::AttemptStatus::Success),
        "redone"
    );
    assert_eq!(successes(&hub), 2);
}

#[tokio::test]
async fn a_group_that_landed_is_counted_though_its_last_message_changed_nothing() {
    // Grok's review of M4B.3: the commit stored, its answer lost, and its last message (an
    // attempt the master doesn't know) changed nothing: it was taken as not stored, and the
    // stored end was never counted.
    let (hub, db, mut client) = connected().await;
    let (to_master, _stream, _ended, load) = one_ended_one_started(&hub, &db, &mut client).await;
    let unknown = "01a00000-0000-7000-8000-000000000000";
    let turn = vec![finished_msg(&load), finished_msg(unknown)];
    let commits = in_one_turn(&hub, &db, &to_master, turn, || db.lose_commit_answer()).await;
    assert_eq!(commits, 1, "read back as stored, not redone");
    assert_eq!(successes(&hub), 2);
}

#[tokio::test]
async fn a_turn_lets_the_stream_go_when_the_master_stops() {
    // Grok's review of M4B.3: a failed group's eleventh message, carried to the next turn,
    // waited for the master to lead again, deaf to its stopping: tonic's graceful shutdown
    // waited on the stream. Since M5.2cB a turn queues them for a round, holding the stream's
    // sender, and a master not leading runs no round: closing lets them go, the stream ends.
    let (hub, db, mut client) = connected().await;
    // What holds the core before the worker's stream: its serving task holds it too.
    let before = Arc::strong_count(&hub.core);
    let (to_master, mut stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let id = assign.attempt_id.as_str();
    let turn: Vec<_> = (0..11).map(|_| started_msg(id)).collect();
    let core = hub.core();
    let busy = core.lock().await;
    for message in turn {
        to_master.send(wrap(message)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    db.fail_write(1);
    hub.set_leading(false);
    drop(busy);
    drop(core);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!hub.queued_results().is_empty(), "queued for a round");
    hub.close_streams().await;
    // Their senders let go: a worker that stopped reading (its buffer full, the closing status
    // not sent) has its stream end only once no sender of it is left.
    assert!(hub.queued_results().is_empty(), "let go with the streams");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while Arc::strong_count(&hub.core) > before {
        assert!(
            std::time::Instant::now() < deadline,
            "the stream's task still waits after close_streams"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The worker's stream ends: no sender of it is left.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Ok(Some(_)) = stream.message().await {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the worker's stream ends after close_streams"
    );
}

#[tokio::test]
async fn a_landed_group_whose_answer_was_lost_still_sends_its_cancels() {
    // Grok's review of M4B.3: read back as stored, a `Started` group sent nothing, dropping
    // the cancel of a `Started` for an attempt that isn't the worker's to run.
    let (hub, db, mut client) = connected().await;
    let (to_master, mut stream, assign) = assigned_one(&hub, &db, &mut client).await;
    let unknown = "01a00000-0000-7000-8000-000000000000";
    let turn = vec![started_msg(&assign.attempt_id), started_msg(unknown)];
    let commits = in_one_turn(&hub, &db, &to_master, turn, || db.lose_commit_answer()).await;
    assert_eq!(commits, 1, "read back as stored, not redone");
    let cancelled = loop {
        if let master_message::Message::CancelAttempt(cancel) = next(&mut stream).await {
            break cancel.attempt_id;
        }
    };
    assert_eq!(cancelled, unknown);
}

#[tokio::test]
async fn a_round_handles_at_most_ten_messages_alone_and_leaves_the_rest_in_order_for_the_next() {
    // M5.2cB (as Grok's review of M4B.3 for a turn): messages handled one by one, each its own
    // transaction, hold the lock; past ten a round, the rest wait at the queue's front.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream, _assign) = assigned_one(&hub, &db, &mut client).await;
    let outbound = hub.streams.lock().await.get("w1").unwrap().clone();
    let rejected = |n: u32| {
        worker_message::Message::Rejected(pb::Rejected {
            attempt_id: format!("01a00000-0000-7000-8000-0000000000{n:02}"),
            reason: pb::RejectReason::NoFreeSlot.into(),
            ..pb::Rejected::default()
        })
    };
    let mut messages: Vec<_> = (0..12).map(rejected).collect();
    hub.queue_results("w1", &outbound, &mut messages);
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    let left: Vec<String> = hub
        .queued_results()
        .iter()
        .map(|r| match &r.message {
            Some(worker_message::Message::Rejected(r)) => r.attempt_id.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        left,
        [
            "01a00000-0000-7000-8000-000000000010",
            "01a00000-0000-7000-8000-000000000011"
        ]
    );
    Box::pin(hub.round_results((hub.clock)())).await.unwrap();
    assert!(hub.queued_results().is_empty(), "the next round's");
}

#[tokio::test]
async fn a_run_created_in_a_round_is_assigned_by_the_round_it_wakes_not_the_next_tick() {
    // M5.2b: the round's probe is taken once. Its commands part creates the run, after which
    // the prepare and assign parts' reads are skipped on that probe; the round notes it and
    // the loop runs another (and that one another) after the round gap, not a tick later.
    let (hub, db, mut client) = connected().await;
    let (_to_master, mut stream) = parsed_worker(&mut client).await;
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let started = std::time::Instant::now();
    let running = tokio::spawn({
        let hub = hub.clone();
        async move {
            hub.run(Duration::from_mins(10), None, async {
                let _ = stopped.await;
            })
            .await;
        }
    });
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    assert_eq!(assign.task_id, "extract");
    // Rounds a few gaps apart, not ten minutes.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    let _ = stop.send(());
    running.await.unwrap();
}

#[tokio::test]
async fn a_cleanup_at_its_barrier_may_start_only_while_its_attempt_is_open_and_this_workers() {
    // B255, decision 504: the master's answer to `CleanupStart`.
    let (hub, db, mut client) = connected().await;
    let (to_master, mut stream) = parsed_worker(&mut client).await;
    db.insert_command(&trigger_sync(), "anna", fixed_now())
        .await
        .unwrap();
    hub.tick().await.unwrap();
    let master_message::Message::AssignTask(assign) = next(&mut stream).await else {
        panic!("expected AssignTask")
    };
    let mut ask = async |attempt_id: &str| {
        to_master
            .send(wrap(worker_message::Message::CleanupStart(
                pb::CleanupStart {
                    attempt_id: attempt_id.to_owned(),
                    rest: false,
                },
            )))
            .await
            .unwrap();
        loop {
            if let master_message::Message::CleanupGo(go) = next(&mut stream).await {
                assert_eq!(go.attempt_id, attempt_id);
                return go.go;
            }
        }
    };
    assert!(ask(&assign.attempt_id).await, "its own open attempt");
    assert!(
        !ask("018f0000-0000-7000-8000-000000000000").await,
        "an attempt it doesn't know"
    );
    to_master
        .send(wrap(worker_message::Message::Finished(pb::Finished {
            attempt_id: assign.attempt_id.clone(),
            status: pb::AttemptStatus::Success.into(),
            ..pb::Finished::default()
        })))
        .await
        .unwrap();
    // Handled by a round (M5.2cB), once the turn has queued it.
    for _ in 0..100 {
        hub.tick().await.unwrap();
        let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
        if attempt.status.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!ask(&assign.attempt_id).await, "an attempt ended");
}

#[tokio::test]
async fn idle_rounds_settle_writing_nothing_and_asking_for_no_follow_up() {
    // Decision 502: any write ending after a round's probe makes a read it skipped wake
    // another round. A round that wrote for itself while idle would so run every round gap
    // for ever (Codex's review found one in the scheduler); a heartbeat between rounds costs
    // nothing, the next round's probe being taken after it.
    let (hub, db, mut client) = connected().await;
    let (_to_master, _stream) = parsed_worker(&mut client).await;
    let mut settled = false;
    for _ in 0..10 {
        if !hub.round(Duration::ZERO, true).await.unwrap() {
            settled = true;
            break;
        }
    }
    assert!(settled, "the first rounds never settled");
    for n in 0..3 {
        let writes = db.writes_ended();
        assert!(
            !hub.round(Duration::ZERO, true).await.unwrap(),
            "idle round {n} asked for another"
        );
        assert_eq!(db.writes_ended(), writes, "idle round {n} wrote");
        db.worker_seen("w1", fixed_now()).await.unwrap();
    }
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
            "produced_by": "1.0", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
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
            ..pb::Started::default()
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
            task_round_gap: Duration::from_mins(1),
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

/// How many times the round's `part` ran, from the master's metrics.
fn part_runs(hub: &Hub, part: &str) -> u64 {
    let mut registry = prometheus_client::registry::Registry::default();
    hub.metrics.register(&mut registry);
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    let prefix = format!("master_round_part_seconds_count{{part=\"{part}\"}} ");
    text.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .map_or(0, |n| n.parse().unwrap())
}

#[tokio::test]
async fn the_slow_parts_run_once_an_interval_and_commands_also_when_announced() {
    // M5.2cB4 (the user's due flags): a round the loop runs skips Commands, Expire and Rest
    // until the tick interval has passed since they ran; Commands also run once announced.
    let (hub, _db, _client) = connected().await;
    let every = Duration::from_millis(300);
    let first = (part_runs(&hub, "expire"), part_runs(&hub, "rest"));
    hub.round(every, false).await.unwrap();
    assert_eq!(
        (part_runs(&hub, "expire"), part_runs(&hub, "rest")),
        (first.0 + 1, first.1 + 1),
        "the first round runs every part"
    );
    hub.round(every, false).await.unwrap();
    assert_eq!(
        part_runs(&hub, "expire"),
        first.0 + 1,
        "skipped within the interval"
    );
    let commands = part_runs(&hub, "commands");
    hub.commands_due.store(true, Ordering::SeqCst);
    hub.round(every, false).await.unwrap();
    assert_eq!(
        part_runs(&hub, "commands"),
        commands + 1,
        "announced: run at once"
    );
    assert_eq!(
        part_runs(&hub, "expire"),
        first.0 + 1,
        "the others still not"
    );
    tokio::time::sleep(every).await;
    hub.round(every, false).await.unwrap();
    assert_eq!(
        part_runs(&hub, "expire"),
        first.0 + 2,
        "due again after the interval"
    );
}

#[tokio::test]
async fn a_stream_of_finishes_runs_rounds_at_most_every_task_round_gap() {
    // M5.2cB4: rounds finishes wake are paced by the task rounds' gap.
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
    tokio::time::sleep(TASK_ROUND_GAP * 2).await;
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
    // The first round at once, then at most one every TASK_ROUND_GAP (and one more at the end).
    let most = 2 + elapsed.as_millis() / TASK_ROUND_GAP.as_millis();
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
        numbered(
            worker_message::Message::Heartbeat(pb::Heartbeat::default()),
            sequence,
            0,
        )
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
    // The round goes first, 11 s later.
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
        streams.insert("slow".to_owned(), Outbound::new(slow, 1));
        streams.insert("other".to_owned(), Outbound::new(other, 2));
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
    // registers again (restarted: the process gone) without it in its journal, it may have
    // run in the old process: held for its lease, then `lost`, as a started one (B240,
    // decision 443).
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
    // Held for the old incarnation's lease (60 s).
    move_to(11);
    hub.tick().await.unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, None);
    move_to(61);
    hub.tick().await.unwrap();
    let attempt = db.attempt(&assign.attempt_id).await.unwrap().unwrap();
    assert_eq!(attempt.status, Some(schedr_model::AttemptStatus::Lost));
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
        next_command(
            None,
            &mut paused_until,
            &problems,
            &tokio::sync::Notify::new(),
        ),
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
        next_command(
            None,
            &mut paused_until,
            &problems,
            &tokio::sync::Notify::new(),
        ),
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
        .sender
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

/// The first `HeartbeatAck` on `stream`, other messages skipped.
async fn next_ack(stream: &mut tonic::Streaming<pb::MasterMessage>) -> pb::HeartbeatAck {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(master_message::Message::HeartbeatAck(ack)) = message.message {
            return ack;
        }
    }
}

/// Decision 509: a worker with a queue counts its head's age in the slow-equals its acks
/// carry (the median times 3, at least 1 s); one without a queue gets it and counts nothing.
#[rstest::rstest]
#[case(4, 6000)]
#[case(0, 1000)]
#[tokio::test]
async fn a_heartbeat_ack_carries_the_clusters_slow_equals(
    #[case] queue_depth: u32,
    #[case] expected_ms: u64,
) {
    let (_hub, _db, mut client) = connected().await;
    let (to_master, outbound) = mpsc::channel(16);
    let mut first = register(PROTOCOL_VERSION);
    if let Some(worker_message::Message::Register(r)) = &mut first.message {
        r.queue_depth = queue_depth;
    }
    to_master.send(first).await.unwrap();
    let mut stream = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let beat = pb::Heartbeat {
        queue_head_age_ms: 2000,
        ..pb::Heartbeat::default()
    };
    to_master
        .send(numbered(worker_message::Message::Heartbeat(beat), 1, 0))
        .await
        .unwrap();
    assert_eq!(next_ack(&mut stream).await.slow_equals_ms, expected_ms);
}

/// M5.2f: trigger messages are timed as their own part of a turn, the rest not.
#[test]
fn trigger_messages_are_told_apart() {
    use worker_message::Message;
    assert!(is_trigger_message(&Message::TriggerEvent(
        pb::TriggerEvent::default()
    )));
    assert!(is_trigger_message(&Message::TriggerPoked(
        pb::TriggerPoked::default()
    )));
    assert!(is_trigger_message(&Message::TriggerStopped(
        pb::TriggerStopped::default()
    )));
    assert!(!is_trigger_message(&Message::Heartbeat(
        pb::Heartbeat::default()
    )));
}

/// Decision 520 (agy's review of M5.2i14): with a worker identity check, `Register`, a parse
/// upload and an attempt's inputs are each refused when the call's certificate doesn't name the
/// worker they are for. The check stands for the certificate here: it names `named`.
#[tokio::test]
async fn with_a_worker_identity_each_call_is_checked_against_the_certificate() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let named = Arc::new(std::sync::Mutex::new("w2".to_owned()));
    let certificate = named.clone();
    let check =
        crate::service::WorkerIdentity(Arc::new(move |_: &tonic::Extensions, worker_id: &str| {
            let name = certificate.lock().unwrap().clone();
            if name == worker_id {
                Ok(())
            } else {
                Err(format!("its certificate names `{name}`"))
            }
        }));
    let hub = Hub::with_clock(MasterCore::new(db.clone(), Settings::default()), fixed_now)
        .with_epoch(7)
        .with_worker_identity(Some(check));
    let mut client = served(&hub).await;
    // w1's registration with w2's certificate: refused.
    let (to_master, outbound) = mpsc::channel(16);
    to_master.send(register(PROTOCOL_VERSION)).await.unwrap();
    let refused = client
        .control(ReceiverStream::new(outbound))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::PermissionDenied, "{refused}");
    // With its own: registered, its parse uploaded, a task assigned.
    *named.lock().unwrap() = "w1".to_owned();
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
    // w2's certificate: neither w1's inputs nor an upload as w1.
    *named.lock().unwrap() = "w2".to_owned();
    let inputs = client
        .fetch_inputs(pb::FetchInputsRequest {
            attempt_id: assign.attempt_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(inputs.code(), tonic::Code::PermissionDenied, "{inputs}");
    let uploaded = upload(&mut client, parse_result()).await.unwrap_err();
    assert_eq!(uploaded.code(), tonic::Code::PermissionDenied, "{uploaded}");
    // w1's own again: its inputs.
    *named.lock().unwrap() = "w1".to_owned();
    let mut inputs = client
        .fetch_inputs(pb::FetchInputsRequest {
            attempt_id: assign.attempt_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inputs.message().await.unwrap().unwrap().data, b"{}");
    drop(to_master);
}
