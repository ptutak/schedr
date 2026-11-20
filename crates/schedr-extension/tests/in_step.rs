//! The crate's messages read as the worker's own types write them, and the other way (decision
//! note 17): a protocol change the crate misses fails here.

use pretty_assertions::assert_eq;
use schedr_extension::{PluginMessage, WorkerMessage};
use schedr_model::runner::{AgentMessage, RunnerMessage};
use serde_json::{Value, json};

/// `value` as the worker's own type `M` writes it, read by the crate's type `C` and written back:
/// unchanged, and read by `M` again: the same message.
fn same<M, C>(value: &Value)
where
    M: serde::de::DeserializeOwned + serde::Serialize + PartialEq + std::fmt::Debug,
    C: serde::de::DeserializeOwned + serde::Serialize,
{
    let worker: M =
        serde_json::from_value(value.clone()).unwrap_or_else(|e| panic!("{value}: {e}"));
    let written = serde_json::to_value(&worker).unwrap();
    let crate_side: C =
        serde_json::from_value(written.clone()).unwrap_or_else(|e| panic!("{written}: {e}"));
    let back = serde_json::to_value(&crate_side).unwrap();
    assert_eq!(back, written, "{value}");
    let again: M = serde_json::from_value(back).unwrap();
    assert_eq!(again, worker);
}

#[test]
fn what_a_plugin_sends_the_worker_reads_and_writes_back_the_same() {
    for message in [
        json!({"type": "hello", "schedr_version": "1.0.0", "mode": "extension", "protocol": 1}),
        json!({"type": "secret_get", "secret": {"path": "etl/api", "key": "token"}}),
        json!({"type": "secret_get", "secret": {"storage": "vault", "path": "etl/api"}}),
        json!({"type": "connection_get", "name": "warehouse"}),
        json!({"type": "handle_add", "handle": {"handle_type": "job", "value": {"id": 7}}}),
        json!({"type": "api_call", "method": "POST", "path": "/api/v1/events", "body": {"a": 1},
               "idempotency_key": "k"}),
        json!({"type": "api_call", "method": "GET", "path": "/api/v1/dags"}),
        json!({"type": "task_succeeded", "output": {"rows": 3}, "outcome": "SUCCESS"}),
        json!({"type": "task_failed", "error": {"exception": "E", "message": "m", "traceback": ""},
               "kind": null}),
    ] {
        same::<RunnerMessage, PluginMessage>(&message);
    }
}

#[test]
fn what_the_worker_sends_a_plugin_reads_and_writes_back_the_same() {
    let task = json!({
        "type": "extension_task",
        "dag_file": {"root": "etl", "root_dir": "/dags/etl", "path": "x.py"},
        "dag_id": "extended",
        "task": {
            "id": "job", "kind": "extension", "name": "spark", "version": "1.0",
            "payload": {"go": true},
            "settings": {
                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null, "queue": "default",
                "max_output_size": 1024
            },
            "output_type": {"type": "any"},
            "cleanup": {"run": "extension", "retries": 3, "retry_delay_ms": 30000}
        },
        "inputs": {}, "outcomes": {},
        "run": {
            "dag_id": "extended", "run_id": "01", "kind": "manual",
            "planned_for": "2026-11-20T00:00:00Z", "since": "2026-11-19T00:00:00Z",
            "try_number": 1
        },
        "sys_path": ["/dags/etl"],
        "secrets": [{"name": {"path": "spark/api", "key": "token"}, "value": "s"}],
        "cleanup": [{"handle_type": "job", "value": {"id": 7}}],
    });
    for message in [
        json!({"type": "welcome"}),
        json!({"type": "refused", "reason": "protocol 2"}),
        task,
        json!({"type": "secret", "value": {"user": "u", "password": "p"}}),
        json!({"type": "secret_failed", "kind": "secret_not_found", "message": "no"}),
        json!({"type": "connection", "values": {"url": "pg://x"}}),
        json!({"type": "connection_failed", "kind": "connection_denied", "message": "no"}),
        json!({"type": "handle_added"}),
        json!({"type": "handle_refused", "message": "no cleanup"}),
        json!({"type": "api_answer", "status": 200, "body": [], "note": "masked"}),
    ] {
        same::<AgentMessage, WorkerMessage>(&message);
    }
}

#[test]
fn the_crates_protocol_is_the_workers() {
    assert_eq!(
        schedr_extension::PROTOCOL,
        schedr_model::runner::EXTENSION_PROTOCOL
    );
    assert_eq!(
        schedr_extension::MAX_MESSAGE_SIZE,
        schedr_model::runner::MAX_MESSAGE_SIZE
    );
}
