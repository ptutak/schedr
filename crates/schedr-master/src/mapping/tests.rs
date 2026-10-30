use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

pub(crate) fn dag(tasks: &Json, edges: &Json) -> Dag {
    let mut tasks = tasks.clone();
    for task in tasks.as_array_mut().unwrap() {
        task["kind"] = json!("python");
        task["module"] = json!("m");
        task["function"] = json!("f");
        task["settings"] = json!({"retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1024});
        task["output_type"] = json!({"type": "int"});
        if task.get("arguments").is_none() {
            task["arguments"] = json!({});
        }
    }
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "d", "location": {"root": "r", "path": "d.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "tasks": tasks, "edges": edges
    }))
    .unwrap()
}

fn output(task: &str) -> Json {
    json!({"ref": {"kind": "output", "task": task}})
}

fn data(from: &str, to: &str) -> Json {
    json!({"from": from, "to": to, "kind": "data"})
}

#[test]
fn only_an_element_wise_use_of_a_mapped_task_is_a_lane() {
    let dag = dag(
        &json!([
            {"id": "files", "expand": {"product": [{"name": "n", "value": {"literal": [1]}}]}},
            {"id": "lane", "expand": {"product": [{"name": "n", "value": output("files")}]}},
            {"id": "whole", "arguments": {"all": output("files")},
             "expand": {"product": [{"name": "n", "value": output("files")}]}},
            {"id": "plain", "expand": {"product": [{"name": "n", "value": output("one")}]}},
            {"id": "one"}
        ]),
        &json!([data("files", "lane"), data("files", "whole"), data("one", "plain"),
                {"from": "files", "to": "lane", "kind": "order"}]),
    );
    let lanes: Vec<bool> = dag.edges.iter().map(|e| is_lane(&dag, e)).collect();
    // The data edge of `lane` only: `whole` also takes the list, `one` isn't mapped, and an
    // order edge counts its upstream as a whole (70.1).
    assert_eq!(lanes, [true, false, false, false]);
    let lane = dag.task(&TaskId::new("lane").unwrap()).unwrap();
    assert_eq!(
        dims(&dag, lane),
        [(Some("n"), Dim::Lane(&TaskId::new("files").unwrap()))]
    );
}

#[test]
fn coordinates_follow_the_written_order_the_first_varying_slowest() {
    let sizes = [2, 3];
    let all: Vec<Vec<usize>> = (0..6).map(|i| coordinates(i, &sizes)).collect();
    assert_eq!(all, [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]]);
    assert_eq!(count(&sizes), Some(6));
    assert_eq!(count(&[]), Some(1));
    assert_eq!(count(&[usize::MAX, 2]), None);
}

#[test]
fn values_resolve_as_the_runner_resolves_them() {
    let mut sources = Sources::default();
    let t = |id: &str| TaskId::new(id).unwrap();
    sources.outputs.insert(
        t("cfg"),
        json!({"files": ["a", "b"], "pair": {"$tuple": [1, 2]}}),
    );
    sources
        .outcomes
        .insert(t("cfg"), Some("SUCCESS".to_owned()));
    sources.outcomes.insert(t("gone"), None);
    let value = |json: Json| -> Value { serde_json::from_value(json).unwrap() };
    let path = |p: Json| value(json!({"ref": {"kind": "output", "task": "cfg", "path": p}}));
    assert_eq!(
        resolve(&path(json!(["files"])), &sources),
        Ok(json!(["a", "b"]))
    );
    assert_eq!(resolve(&path(json!(["pair", 1])), &sources), Ok(json!(2)));
    assert_eq!(
        resolve(
            &value(json!({"list": [{"literal": 1}, output("cfg")]})),
            &sources
        )
        .unwrap()[0],
        json!(1)
    );
    // A skipped upstream, or another outcome than narrowed to, stands for null.
    assert_eq!(resolve(&value(output("gone")), &sources), Ok(Json::Null));
    let narrowed = value(json!({"ref": {"kind": "output", "task": "cfg", "on": ["Rows"]}}));
    assert_eq!(resolve(&narrowed, &sources), Ok(Json::Null));
    assert!(resolve(&path(json!(["nope"])), &sources).is_err());
}
