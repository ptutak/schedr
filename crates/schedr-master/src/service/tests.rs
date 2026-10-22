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
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let hub = Hub::with_clock(MasterCore::new(db.clone(), Settings::default()), fixed_now);
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
    let stop = tokio::time::sleep(Duration::from_millis(100));
    hub.run(Duration::from_millis(10), None, stop).await;
    let decided = db.command(&id).await.unwrap().unwrap();
    assert_eq!(
        decided.status,
        schedr_model::commands::CommandStatus::Rejected
    );
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
