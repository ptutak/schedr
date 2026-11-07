use pretty_assertions::assert_eq;
use prost::Message;

use super::pb;

#[test]
fn messages_encode_and_decode() {
    let message = pb::WorkerMessage {
        message: Some(pb::worker_message::Message::Register(pb::Register {
            running_triggers: Vec::new(),
            watcher_slots: 500,
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
            process_id: "0192c4e1-7a3b-7c00-8000-000000000001".into(),
            config_json: r#"{"worker":{"slots":2}}"#.into(),
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

/// Every name of `master.proto`'s control messages and rpcs, as the protocol page must have them.
fn protocol_names(proto: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_oneof = false;
    for line in proto.lines().map(str::trim) {
        if let Some(rest) = line.strip_prefix("rpc ") {
            names.extend(rest.split('(').next().map(str::to_owned));
        } else if line.starts_with("oneof message") {
            in_oneof = true;
        } else if in_oneof && line == "}" {
            in_oneof = false;
        } else if in_oneof {
            names.extend(line.split_whitespace().next().map(str::to_owned));
        }
    }
    names
}

#[test]
fn the_protocol_page_names_every_message_and_rpc() {
    let proto = include_str!("../proto/schedr/protocol/v1/master.proto");
    let page = include_str!("../../../docs/03-guide/15-protocol.md");
    let names = protocol_names(proto);
    assert!(names.len() >= 20, "{names:?}");
    let missing: Vec<&String> = names
        .iter()
        .filter(|name| !page.contains(&format!("`{name}`")))
        .collect();
    assert_eq!(missing, Vec::<&String>::new());
}
