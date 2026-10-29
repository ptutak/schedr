use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;
use crate::task::{CommandLine, TaskKind, TaskSettings};
use crate::types::TypeDesc;
use crate::value::Template;

fn id(s: &str) -> TaskId {
    TaskId::new(s).unwrap()
}

fn empty_task(name: &str) -> Task {
    Task {
        id: id(name),
        kind: TaskKind::Empty,
        settings: TaskSettings::default(),
        output_type: TypeDesc::None,
        outcomes: crate::default_outcomes(),
        group: None,
        expand: None,
    }
}

fn edge(from: &str, to: &str, kind: EdgeKind) -> Edge {
    Edge {
        on: Vec::new(),
        from: id(from),
        to: id(to),
        kind,
    }
}

fn dag() -> Dag {
    Dag {
        produced_by: SchedrVersion::new(0, 1),
        id: DagId::new("sync").unwrap(),
        location: DagLocation {
            root: "etl-repo".into(),
            path: "team_a/sync.py".into(),
        },
        description: None,
        tags: vec![],
        trigger: Trigger::Cron {
            expression: "0 3 * * *".into(),
            timezone: "UTC".into(),
            catchup: false,
            start: Some("2026-01-01T00:00:00Z".parse().unwrap()),
            end: None,
        },
        initial_since: None,
        max_active_runs: Some(1),
        tasks: vec![empty_task("a"), empty_task("b"), empty_task("c")],
        edges: vec![
            edge("a", "c", EdgeKind::Data),
            edge("b", "c", EdgeKind::Order),
            edge("a", "c", EdgeKind::Order),
            edge("a", "b", EdgeKind::Order),
        ],
        groups: Vec::new(),
    }
}

#[test]
fn serializes_the_documented_shape() {
    let json = serde_json::to_value(dag()).unwrap();
    assert_eq!(json["produced_by"], "0.1");
    assert_eq!(json["id"], "sync");
    assert_eq!(
        json["location"],
        json!({"root": "etl-repo", "path": "team_a/sync.py"})
    );
    assert_eq!(json["trigger"]["kind"], "cron");
    assert_eq!(json["trigger"]["start"], "2026-01-01T00:00:00Z");
    assert_eq!(json["trigger"]["end"], json!(null));
    assert!(json.get("start").is_none());
    assert_eq!(json["max_active_runs"], 1);
    assert_eq!(
        json["edges"][0],
        json!({"from": "a", "to": "c", "kind": "data"})
    );
    assert!(json.get("description").is_none());
    assert!(json.get("tags").is_none());
}

#[test]
fn round_trips_through_json() {
    let mut original = dag();
    original.description = Some("Daily sync".into());
    original.tags = vec!["sync".into()];
    original.tasks.push(Task {
        id: id("notify"),
        kind: TaskKind::Command {
            command: CommandLine::Shell(Template::text("echo done")),
            env: std::collections::BTreeMap::new(),
        },
        settings: TaskSettings::default(),
        output_type: TypeDesc::ProcessResult,
        outcomes: crate::default_outcomes(),
        group: None,
        expand: None,
    });
    let json = serde_json::to_string(&original).unwrap();
    assert_eq!(serde_json::from_str::<Dag>(&json).unwrap(), original);
}

#[test]
fn rejects_unknown_top_level_fields() {
    let mut json = serde_json::to_value(dag()).unwrap();
    json["owner"] = json!("me");
    let err = serde_json::from_value::<Dag>(json).unwrap_err();
    assert!(err.to_string().contains("unknown field `owner`"), "{err}");
}

#[test]
fn rejects_unknown_fields_in_location_and_edges() {
    assert!(
        serde_json::from_value::<DagLocation>(json!({"root": "r", "path": "p", "x": 1})).is_err()
    );
    assert!(
        serde_json::from_value::<Edge>(json!({"from": "a", "to": "b", "kind": "data", "x": 1}))
            .is_err()
    );
}

#[test]
fn finds_tasks_by_id() {
    let dag = dag();
    assert_eq!(dag.task(&id("b")).map(|t| &t.id), Some(&id("b")));
    assert_eq!(dag.task(&id("missing")), None);
}

#[test]
fn upstream_of_lists_direct_upstream_tasks_once() {
    let dag = dag();
    assert_eq!(dag.upstream_of(&id("c")), vec![&id("a"), &id("b")]);
    assert_eq!(dag.upstream_of(&id("b")), vec![&id("a")]);
    assert_eq!(dag.upstream_of(&id("a")), Vec::<&TaskId>::new());
}

#[test]
fn downstream_of_lists_direct_downstream_tasks_once() {
    let dag = dag();
    assert_eq!(dag.downstream_of(&id("a")), vec![&id("c"), &id("b")]);
    assert_eq!(dag.downstream_of(&id("b")), vec![&id("c")]);
    assert_eq!(dag.downstream_of(&id("c")), Vec::<&TaskId>::new());
}
