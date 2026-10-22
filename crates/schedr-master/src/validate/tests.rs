use pretty_assertions::assert_eq;
use serde_json::{Value as Json, json};

use super::*;

const MASTER: SchedrVersion = SchedrVersion::new(0, 1);

fn task(id: &str, arguments: &Json) -> Json {
    json!({
        "id": id, "kind": "python", "arguments": arguments,
        "settings": {
            "retries": 0, "retry_delay_ms": 300_000, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        },
        "output_type": {"type": "int"}
    })
}

fn output_of(task: &str) -> Json {
    json!({"ref": {"kind": "output", "task": task}})
}

fn edge(from: &str, to: &str, kind: &str) -> Json {
    json!({"from": from, "to": to, "kind": kind})
}

fn dag_with(changes: &Json) -> Dag {
    let mut dag = json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
        "trigger": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
        "max_active_runs": 1,
        "tasks": [task("extract", &json!({})), task("load", &json!({"rows": output_of("extract")}))],
        "edges": [edge("extract", "load", "data")]
    });
    for (key, value) in changes.as_object().unwrap() {
        dag[key] = value.clone();
    }
    serde_json::from_value(dag).unwrap()
}

fn problems(changes: &Json) -> Vec<String> {
    dag_problems(&dag_with(changes), "etl", "sync.py", MASTER)
}

#[test]
fn a_valid_dag_has_no_problems() {
    assert_eq!(problems(&json!({})), Vec::<String>::new());
}

#[test]
fn dag_json_from_a_newer_or_other_major_version_cant_be_read() {
    assert_eq!(
        problems(&json!({"produced_by": "0.2"})),
        ["produced by schedr 0.2, which this master (schedr 0.1) can't read"]
    );
    assert_eq!(problems(&json!({"produced_by": "1.0"})).len(), 1);
    let older = dag_with(&json!({"produced_by": "0.1"}));
    assert_eq!(
        dag_problems(&older, "etl", "sync.py", SchedrVersion::new(0, 3)),
        Vec::<String>::new()
    );
}

#[test]
fn the_location_must_be_the_file_parsed() {
    assert_eq!(
        problems(&json!({"location": {"root": "etl", "path": "other.py"}})),
        ["located at etl:other.py in its DAG JSON, but parsed from etl:sync.py"]
    );
}

#[test]
fn run_limits_and_the_window_are_checked() {
    assert_eq!(
        problems(&json!({"max_active_runs": 0})),
        ["max_active_runs must be at least 1"]
    );
    assert_eq!(
        problems(&json!({"start": "2026-02-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"})),
        ["start (2026-02-01T00:00:00Z) is after end (2026-01-01T00:00:00Z)"]
    );
    assert_eq!(
        problems(&json!({"start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"})),
        Vec::<String>::new()
    );
}

#[test]
fn the_schedule_must_be_usable() {
    assert_eq!(
        problems(
            &json!({"trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "Mars/Base", "catchup": false}})
        ),
        ["trigger: unknown time zone `Mars/Base`"]
    );
    assert_eq!(
        problems(&json!({"trigger": {"kind": "every", "interval_ms": 0, "catchup": false}})),
        ["trigger: the interval of an Every trigger must be positive"]
    );
}

#[test]
fn task_ids_are_unique() {
    assert_eq!(
        problems(&json!({"tasks": [task("a", &json!({})), task("a", &json!({}))], "edges": []})),
        ["task `a` is defined more than once"]
    );
}

#[test]
fn edges_join_tasks_of_the_dag() {
    assert_eq!(
        problems(
            &json!({"edges": [edge("extract", "load", "data"), edge("extract", "ghost", "order")]})
        ),
        ["edge `extract` -> `ghost`: no task `ghost`"]
    );
    assert_eq!(
        problems(
            &json!({"edges": [edge("extract", "load", "data"), edge("load", "load", "order")]})
        ),
        [
            "task `load` depends on itself",
            "the tasks `load` depend on each other in a cycle"
        ]
    );
}

#[test]
fn cycles_name_the_tasks_left_over() {
    let tasks = json!([
        task("a", &json!({})),
        task("b", &json!({})),
        task("c", &json!({})),
        task("d", &json!({}))
    ]);
    let edges = json!([
        edge("a", "b", "order"),
        edge("b", "c", "order"),
        edge("c", "b", "order"),
        edge("c", "d", "order")
    ]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": edges})),
        ["the tasks `b`, `c`, `d` depend on each other in a cycle"]
    );
}

#[test]
fn outputs_are_used_only_along_data_edges() {
    assert_eq!(
        problems(&json!({"edges": [edge("extract", "load", "order")]})),
        ["task `load` uses the output of `extract` without a data edge from it"]
    );
    let tasks = json!([
        task("extract", &json!({})),
        task(
            "load",
            &json!({"a": output_of("ghost"), "b": output_of("ghost")})
        )
    ]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": []})),
        ["task `load` uses the output of `ghost`, which isn't a task of the DAG"]
    );
    let run_info = json!({"ref": {"kind": "dag_ctx", "field": "planned_for"}});
    let tasks = json!([task("extract", &json!({"when": run_info}))]);
    assert_eq!(
        problems(&json!({"tasks": tasks, "edges": []})),
        Vec::<String>::new()
    );
}

fn file(path: &str, dags: Vec<Dag>) -> ParsedFile {
    ParsedFile {
        path: path.to_owned(),
        dags,
        error: None,
    }
}

#[test]
fn files_with_invalid_dags_become_import_errors() {
    let bad = dag_with(&json!({"max_active_runs": 0, "produced_by": "0.9"}));
    let files = validate_files(
        "etl",
        vec![file("sync.py", vec![dag_with(&json!({})), bad])],
        MASTER,
    );
    assert_eq!(files[0].dags, []);
    let error = files[0].error.as_ref().unwrap();
    assert_eq!(error.exception, INVALID_DAG_EXCEPTION);
    assert_eq!(
        error.message,
        "DAG `sync`: produced by schedr 0.9, which this master (schedr 0.1) can't read\n\
         DAG `sync`: max_active_runs must be at least 1"
    );
}

#[test]
fn valid_files_and_import_errors_are_kept() {
    let error = ErrorSummary {
        exception: "ImportError".to_owned(),
        message: "no module".to_owned(),
        traceback: String::new(),
    };
    let failed = ParsedFile {
        path: "bad.py".to_owned(),
        dags: vec![dag_with(&json!({}))],
        error: Some(error.clone()),
    };
    let good = file("sync.py", vec![dag_with(&json!({}))]);
    let files = validate_files("etl", vec![good.clone(), failed], MASTER);
    assert_eq!(files[0], good);
    assert_eq!(files[1].dags, []);
    assert_eq!(files[1].error, Some(error));
}

#[test]
fn paths_must_stay_inside_the_root() {
    for path in ["", "/etc/x.py", "../x.py", "a/../x.py", "./x.py", "a\\x.py"] {
        let files = validate_files("etl", vec![file(path, vec![])], MASTER);
        let error = files[0].error.as_ref().unwrap();
        assert_eq!(error.exception, INVALID_PATH_EXCEPTION, "{path}");
        assert_eq!(
            error.message,
            format!("`{path}` isn't a relative path inside the root")
        );
    }
    let files = validate_files("etl", vec![file("team/a.py", vec![])], MASTER);
    assert_eq!(files[0].error, None);
}
