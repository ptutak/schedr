use pretty_assertions::assert_eq;
use rstest::rstest;
use serde_json::{Value as Json, json};

use super::*;

/// A DAG `sync` with tasks `a` and `b`, with `limits` (`max_active_tasks`) and each task's
/// `settings` merged in.
fn dag(max_active_tasks: Option<u32>, a: &Json, b: &Json) -> Dag {
    let task = |id: &str, extra: &Json| {
        let mut settings = json!({
            "retries": 0, "retry_delay_ms": 300_000, "timeout_ms": null,
            "queue": "default", "max_output_size": 1_048_576
        });
        for (key, value) in extra.as_object().unwrap() {
            settings[key] = value.clone();
        }
        json!({
            "id": id, "kind": "python", "arguments": {}, "settings": settings,
            "output_type": {"type": "int"}
        })
    };
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "sync.py"},
        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
        "max_active_tasks": max_active_tasks,
        "tasks": [task("a", a), task("b", b)], "edges": []
    }))
    .unwrap()
}

fn id(text: &str) -> TaskId {
    TaskId::new(text).unwrap()
}

fn dag_id(text: &str) -> DagId {
    DagId::new(text).unwrap()
}

fn open(attempt: &str, dag: &str, run: &str, task: &str) -> ActiveAttempt {
    ActiveAttempt {
        attempt_id: attempt.to_owned(),
        dag_id: dag_id(dag),
        run_id: run.to_owned(),
        task_id: id(task),
    }
}

#[test]
fn counts_follow_attempts_added_and_removed() {
    let mut active = Active::new();
    assert!(!active.is_loaded());
    active.load(vec![
        open("1", "sync", "r1", "a"),
        open("2", "sync", "r1", "a"),
        open("3", "sync", "r2", "a"),
        open("4", "other", "r3", "a"),
    ]);
    assert!(active.is_loaded());
    assert_eq!(active.of_dag(&dag_id("sync")), 3);
    assert_eq!(active.of_task(&dag_id("sync"), &id("a")), 3);
    assert_eq!(active.of_run_task("r1", &id("a")), 2);
    // Added twice, counted once.
    active.add("5", dag_id("sync"), "r1".into(), id("b"));
    active.add("5", dag_id("sync"), "r1".into(), id("b"));
    assert_eq!(active.of_dag(&dag_id("sync")), 4);
    assert_eq!(active.of_task(&dag_id("sync"), &id("b")), 1);
    // Removed twice, or never counted: once.
    active.remove("1");
    active.remove("1");
    active.remove("unknown");
    assert_eq!(active.of_run_task("r1", &id("a")), 1);
    assert_eq!(active.of_dag(&dag_id("sync")), 3);
    for attempt in ["2", "3", "4", "5"] {
        active.remove(attempt);
    }
    // Nothing left behind: equal to a fresh load of nothing.
    let mut empty = Active::new();
    empty.load(Vec::new());
    assert_eq!(active, empty);
}

#[test]
fn a_load_replaces_what_was_counted() {
    let mut active = Active::new();
    active.add("1", dag_id("sync"), "r1".into(), id("a"));
    active.load(vec![open("2", "sync", "r1", "b")]);
    assert_eq!(active.of_task(&dag_id("sync"), &id("a")), 0);
    assert_eq!(active.of_task(&dag_id("sync"), &id("b")), 1);
}

#[rstest]
#[case::no_limits(None, json!({}), None)]
#[case::dag_limit_reached(Some(3), json!({}), Some(("max_active_tasks", 3)))]
#[case::dag_limit_not_reached(Some(4), json!({}), None)]
#[case::task_limit_reached(None, json!({"max_active": 2}), Some(("max_active", 2)))]
#[case::task_limit_not_reached(None, json!({"max_active": 4}), None)]
#[case::per_run_reached(None, json!({"max_active_per_run": 2}), Some(("max_active_per_run", 2)))]
#[case::per_run_not_reached(None, json!({"max_active_per_run": 3}), None)]
#[case::dag_limit_first(Some(1), json!({"max_active": 1}), Some(("max_active_tasks", 1)))]
fn a_limit_is_reached_at_its_count(
    #[case] max_active_tasks: Option<u32>,
    #[case] a: Json,
    #[case] expected: Option<(&str, u32)>,
) {
    // `a` has 2 open in run r1 and 1 in r2 (3 across runs).
    let mut active = Active::new();
    active.load(vec![
        open("1", "sync", "r1", "a"),
        open("2", "sync", "r1", "a"),
        open("3", "sync", "r2", "a"),
    ]);
    let dag = dag(max_active_tasks, &a, &json!({}));
    let reached = active.reached((&dag, &dag), "r1", &id("a"));
    assert_eq!(reached.map(|r| (r.name, r.value)), expected);
}

#[test]
fn limits_across_runs_are_the_current_versions_and_per_run_the_runs_own() {
    let mut active = Active::new();
    active.load(vec![open("1", "sync", "r1", "a")]);
    let own = dag(Some(1), &json!({"max_active": 1}), &json!({}));
    // The current version raised the limits across runs: they don't hold any more.
    let current = dag(
        Some(10),
        &json!({"max_active": 10, "max_active_per_run": 1}),
        &json!({}),
    );
    assert_eq!(active.reached((&current, &own), "r1", &id("a")), None);
    // The run's own version's `max_active_per_run` holds, not the current one's.
    let own = dag(None, &json!({"max_active_per_run": 1}), &json!({}));
    let current = dag(None, &json!({}), &json!({}));
    assert_eq!(
        active.reached((&current, &own), "r1", &id("a")),
        Some(Reached {
            name: "max_active_per_run",
            value: 1
        })
    );
}

#[test]
fn a_task_the_current_version_dropped_keeps_its_own_max_active() {
    let mut active = Active::new();
    active.load(vec![open("1", "sync", "r1", "b")]);
    let own = dag(None, &json!({}), &json!({"max_active": 1}));
    let mut current = dag(None, &json!({}), &json!({}));
    current.tasks.retain(|t| t.id != id("b"));
    assert_eq!(
        active.reached((&current, &own), "r2", &id("b")),
        Some(Reached {
            name: "max_active",
            value: 1
        })
    );
}

#[test]
fn the_reason_names_the_limit_and_its_value() {
    let reached = Reached {
        name: "max_active_per_run",
        value: 10,
    };
    assert_eq!(
        reached.reason(),
        "waiting: `max_active_per_run` (10) reached"
    );
}
