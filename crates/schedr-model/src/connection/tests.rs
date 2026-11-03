use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn def(params: &serde_json::Value) -> ConnectionDef {
    serde_json::from_value(json!({"name": "mongo", "params": params})).unwrap()
}

#[test]
fn a_connection_names_its_secrets_once_and_never_a_value() {
    let mongo = def(&json!({
        "uri": {"ref": {"kind": "secret", "path": "mongo/default", "key": "uri"}},
        "label": {"template": [
            {"text": "for "},
            {"ref": {"kind": "secret", "path": "mongo/default", "key": "uri"}},
            {"ref": {"kind": "secret", "path": "app", "key": "name", "storage": "vault"}},
        ]},
        "database": {"literal": "orders"},
    }));
    mongo.check().unwrap();
    let names: Vec<String> = mongo.secrets().iter().map(ToString::to_string).collect();
    assert_eq!(
        names,
        [
            "secrets['mongo/default']['uri']",
            "secrets.vault['app']['name']"
        ]
    );
}

#[test]
fn a_connection_refuses_a_bad_name_and_references_other_than_secrets() {
    let mut bad = def(&json!({}));
    bad.name = "not a name".to_owned();
    assert!(bad.check().unwrap_err().contains("connection `not a name`"));
    let run = def(&json!({"when": {"ref": {"kind": "dag_ctx", "field": "dag_id"}}}));
    assert_eq!(
        run.check().unwrap_err(),
        "connection `mongo`, `when`: only secrets can be referenced"
    );
}

fn file(path: &str, connections: Vec<ConnectionDef>) -> crate::runner::ParsedFile {
    crate::runner::ParsedFile {
        path: path.to_owned(),
        dags: Vec::new(),
        connections,
        error: None,
        warnings: Vec::new(),
    }
}

#[test]
fn a_roots_registry_refuses_a_name_defined_twice_or_invalid() {
    let mongo = def(&json!({"database": {"literal": "orders"}}));
    let mut other = mongo.clone();
    other.name = "other".to_owned();
    let mut bad = mongo.clone();
    bad.name = "bad".to_owned();
    bad.params.insert(
        "when".to_owned(),
        serde_json::from_value(json!({"ref": {"kind": "dag_ctx", "field": "dag_id"}})).unwrap(),
    );
    let registry = Registry::of(&[
        file("b.py", vec![mongo.clone()]),
        file("a.py", vec![mongo.clone(), other.clone(), bad]),
    ]);
    assert_eq!(registry.get("other"), Ok(&other));
    assert_eq!(
        registry.get("mongo").unwrap_err(),
        "connection `mongo` is defined in more than one file (a.py, b.py): rename one"
    );
    assert!(registry.get("bad").unwrap_err().contains("only secrets"));
    assert!(
        registry
            .get("nowhere")
            .unwrap_err()
            .contains("isn't defined in the DAG folder")
    );
    // As the master sends it and the agent reads it.
    let json = serde_json::to_value(&registry).unwrap();
    assert_eq!(serde_json::from_value::<Registry>(json).unwrap(), registry);
}

#[test]
fn a_file_the_master_refused_keeps_its_connections() {
    // One invalid DAG (refused after it imported) doesn't fail the tasks of other files
    // using its connections; the master clears those of a file that failed to import.
    let mut refused = file("bad.py", vec![named("hidden")]);
    refused.error = Some(crate::runner::ErrorSummary {
        exception: "InvalidDag".to_owned(),
        message: "max_active_runs: 0".to_owned(),
        traceback: String::new(),
    });
    let registry = Registry::of(&[refused, file("good.py", vec![named("warehouse")])]);
    assert_eq!(
        registry.connections.keys().collect::<Vec<_>>(),
        ["hidden", "warehouse"]
    );
}

fn named(name: &str) -> ConnectionDef {
    serde_json::from_value(json!({"name": name, "params": {}})).unwrap()
}
