use pretty_assertions::assert_eq;

use super::*;

#[test]
fn dag_schema_describes_a_dag_with_its_definitions() {
    let schema = serde_json::to_value(dag_schema()).unwrap();
    assert_eq!(schema["title"], "Dag");
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["additionalProperties"], false);
    let defs = schema["$defs"].as_object().unwrap();
    for name in [
        "Task",
        "TypeDesc",
        "Value",
        "Reference",
        "Schedule",
        "TaskId",
        "DagId",
    ] {
        assert!(defs.contains_key(name), "missing definition {name}");
    }
}

#[test]
fn runner_schema_covers_both_directions() {
    let schema = serde_json::to_value(runner_schema()).unwrap();
    assert_eq!(schema["title"], "RunnerChannelMessage");
    let defs = schema["$defs"].as_object().unwrap();
    for name in ["RunnerMessage", "AgentMessage", "ErrorSummary", "Dag"] {
        assert!(defs.contains_key(name), "missing definition {name}");
    }
}

#[test]
fn dag_schema_requires_the_identity_and_schedule() {
    let schema = serde_json::to_value(dag_schema()).unwrap();
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    for field in [
        "produced_by",
        "id",
        "location",
        "schedule",
        "catchup",
        "tasks",
        "edges",
    ] {
        assert!(required.contains(&field), "{field} not required");
    }
}
