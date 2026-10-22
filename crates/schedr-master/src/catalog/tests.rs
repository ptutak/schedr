use pretty_assertions::assert_eq;
use schedr_db::dags::CurrentVersion;
use schedr_model::runner::ErrorSummary;
use serde_json::json;

use super::*;

fn dag(id: &str, root: &str, path: &str) -> Dag {
    serde_json::from_value(json!({
        "produced_by": "0.1", "id": id, "location": {"root": root, "path": path},
        "trigger": {"kind": "none"}, "start": null, "end": null, "initial_since": null,
        "max_active_runs": 1, "tasks": [], "edges": []
    }))
    .unwrap()
}

fn id(text: &str) -> DagId {
    DagId::new(text).unwrap()
}

fn place(root: &str, path: &str) -> DagLocation {
    DagLocation {
        root: root.to_owned(),
        path: path.to_owned(),
    }
}

fn ok(path: &str, dags: Vec<Dag>) -> ParsedFile {
    ParsedFile {
        path: path.to_owned(),
        dags,
        error: None,
    }
}

fn failed(path: &str) -> ParsedFile {
    ParsedFile {
        path: path.to_owned(),
        dags: vec![],
        error: Some(ErrorSummary {
            exception: "SyntaxError".to_owned(),
            message: "invalid syntax".to_owned(),
            traceback: String::new(),
        }),
    }
}

fn source(root: &str, parse_id: &str, files: Vec<ParsedFile>) -> Source {
    Source {
        root: root.to_owned(),
        parse_id: parse_id.to_owned(),
        files,
    }
}

fn stored(dag: Dag, parse_id: &str) -> StoredDag {
    StoredDag {
        dag_id: dag.id.clone(),
        paused: false,
        removed: false,
        conflict: None,
        current: Some(CurrentVersion {
            version_id: format!("v-{parse_id}"),
            parse_id: parse_id.to_owned(),
            dag,
        }),
    }
}

fn define(dag: Dag, parse_id: &str, pause: bool) -> DagChange {
    DagChange::Define {
        dag: Box::new(dag),
        parse_id: parse_id.to_owned(),
        pause,
    }
}

#[test]
fn new_dags_are_defined() {
    let sources = [source(
        "etl",
        "p1",
        vec![ok("a.py", vec![dag("a", "etl", "a.py")])],
    )];
    assert_eq!(
        plan(&sources, &[]),
        [define(dag("a", "etl", "a.py"), "p1", false)]
    );
}

#[test]
fn a_dag_of_the_current_parse_is_unchanged() {
    let a = dag("a", "etl", "a.py");
    let sources = [source("etl", "p1", vec![ok("a.py", vec![a.clone()])])];
    assert_eq!(plan(&sources, &[stored(a, "p1")]), []);
}

#[test]
fn a_new_parse_gives_a_new_version() {
    let a = dag("a", "etl", "a.py");
    let sources = [source("etl", "p2", vec![ok("a.py", vec![a.clone()])])];
    assert_eq!(
        plan(&sources, &[stored(a.clone(), "p1")]),
        [define(a, "p2", false)]
    );
}

#[test]
fn a_removed_dag_that_comes_back_is_paused() {
    let a = dag("a", "etl", "a.py");
    let mut gone = stored(a.clone(), "p1");
    gone.removed = true;
    let sources = [source("etl", "p1", vec![ok("a.py", vec![a.clone()])])];
    assert_eq!(plan(&sources, &[gone]), [define(a, "p1", true)]);
}

#[test]
fn a_dag_no_longer_defined_in_its_root_is_removed() {
    let sources = [source("etl", "p2", vec![ok("b.py", vec![])])];
    let kept_elsewhere = stored(dag("x", "other", "x.py"), "p0");
    let mut already_removed = stored(dag("r", "etl", "r.py"), "p1");
    already_removed.removed = true;
    assert_eq!(
        plan(
            &sources,
            &[
                stored(dag("a", "etl", "a.py"), "p1"),
                kept_elsewhere,
                already_removed
            ]
        ),
        [DagChange::Remove { dag_id: id("a") }]
    );
}

#[test]
fn a_dag_that_moved_to_another_file_gets_the_new_place() {
    let moved = dag("a", "etl", "new.py");
    let sources = [source("etl", "p2", vec![ok("new.py", vec![moved.clone()])])];
    assert_eq!(
        plan(&sources, &[stored(dag("a", "etl", "a.py"), "p1")]),
        [define(moved, "p2", false)]
    );
}

#[test]
fn a_file_that_fails_keeps_its_dags() {
    let sources = [source("etl", "p2", vec![failed("a.py")])];
    assert_eq!(plan(&sources, &[stored(dag("a", "etl", "a.py"), "p1")]), []);
}

#[test]
fn the_same_id_in_two_places_is_a_conflict() {
    let sources = [
        source(
            "etl",
            "p1",
            vec![ok("a.py", vec![dag("dup", "etl", "a.py")])],
        ),
        source("ml", "p2", vec![ok("b.py", vec![dag("dup", "ml", "b.py")])]),
    ];
    let conflict = DagChange::Conflict {
        dag_id: id("dup"),
        places: vec![place("etl", "a.py"), place("ml", "b.py")],
        pause: true,
    };
    assert_eq!(plan(&sources, &[]), [conflict]);

    let mut known = stored(dag("dup", "etl", "a.py"), "p0");
    assert_eq!(
        plan(&sources, std::slice::from_ref(&known)),
        [DagChange::Conflict {
            dag_id: id("dup"),
            places: vec![place("etl", "a.py"), place("ml", "b.py")],
            pause: false,
        }]
    );
    known.conflict = Some(vec![place("etl", "a.py"), place("ml", "b.py")]);
    assert_eq!(plan(&sources, &[known]), []);
}

#[test]
fn a_file_defining_an_id_twice_is_a_conflict() {
    let twice = vec![dag("dup", "etl", "a.py"), dag("dup", "etl", "a.py")];
    let sources = [source("etl", "p1", vec![ok("a.py", twice)])];
    assert_eq!(
        plan(&sources, &[]),
        [DagChange::Conflict {
            dag_id: id("dup"),
            places: vec![place("etl", "a.py"), place("etl", "a.py")],
            pause: true,
        }]
    );
}

#[test]
fn a_failing_file_still_counts_for_a_conflict() {
    let mut known = stored(dag("dup", "etl", "a.py"), "p1");
    known.current = None;
    known.conflict = Some(vec![place("etl", "a.py"), place("etl", "b.py")]);
    let sources = [source(
        "etl",
        "p2",
        vec![failed("a.py"), ok("b.py", vec![dag("dup", "etl", "b.py")])],
    )];
    assert_eq!(plan(&sources, &[known]), []);
}

#[test]
fn a_resolved_conflict_defines_the_remaining_place() {
    let mut known = stored(dag("dup", "etl", "a.py"), "p1");
    known.conflict = Some(vec![place("etl", "a.py"), place("etl", "b.py")]);
    let b = dag("dup", "etl", "b.py");
    let sources = [source("etl", "p2", vec![ok("b.py", vec![b.clone()])])];
    assert_eq!(plan(&sources, &[known]), [define(b, "p2", false)]);
}

#[test]
fn a_resolved_conflict_with_a_failing_file_keeps_the_stored_version() {
    let a = dag("dup", "etl", "a.py");
    let mut known = stored(a.clone(), "p1");
    known.conflict = Some(vec![place("etl", "a.py"), place("etl", "b.py")]);
    let sources = [source("etl", "p2", vec![failed("a.py")])];
    assert_eq!(plan(&sources, &[known.clone()]), [define(a, "p1", false)]);
    known.current = None;
    assert_eq!(plan(&sources, &[known]), []);
}

#[test]
fn a_conflict_that_disappears_is_removed() {
    let mut known = stored(dag("dup", "etl", "a.py"), "p1");
    known.current = None;
    known.conflict = Some(vec![place("etl", "a.py"), place("etl", "b.py")]);
    let sources = [source("etl", "p2", vec![])];
    assert_eq!(
        plan(&sources, &[known]),
        [DagChange::Remove { dag_id: id("dup") }]
    );
}
