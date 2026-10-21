use pretty_assertions::assert_eq;
use prost::Message;

use super::pb;

#[test]
fn messages_encode_and_decode() {
    let message = pb::WorkerMessage {
        message: Some(pb::worker_message::Message::Register(pb::Register {
            worker_id: "worker-1".into(),
            incarnation_id: "0192c4e1-7a3b-7c00-8000-000000000000".into(),
            protocol: Some(super::PROTOCOL_VERSION),
            schedr_version: "0.1.0".into(),
            queues: vec!["default".into()],
            slots: 2,
            log_endpoint: "10.0.0.7:8081".into(),
            roots: vec![pb::RootHash {
                kind: pb::RootKind::Dag.into(),
                root: "etl-repo".into(),
                content_hash: "h2".into(),
            }],
            secret_storages: vec!["vault".into()],
            journal: vec![],
        })),
    };
    let bytes = message.encode_to_vec();
    assert_eq!(
        pb::WorkerMessage::decode(bytes.as_slice()).unwrap(),
        message
    );
}

#[test]
fn optional_fields_distinguish_absent_from_zero() {
    let finished = pb::Finished {
        exit_code: Some(0),
        ..Default::default()
    };
    let decoded = pb::Finished::decode(finished.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.exit_code, Some(0));
    assert_eq!(decoded.signal, None);
}

#[test]
fn the_master_service_has_a_client_and_a_server() {
    let _client: Option<pb::master_client::MasterClient<tonic::transport::Channel>> = None;
    assert_eq!(
        <pb::master_server::MasterServer<()> as tonic::server::NamedService>::NAME,
        "schedr.protocol.v1.Master"
    );
}
