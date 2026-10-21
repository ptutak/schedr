use pretty_assertions::assert_eq;
use rstest::rstest;
use schemars::schema_for;

use super::*;

#[rstest]
#[case("sync_customers")]
#[case("etl-repo:team_a/schedr.toml#report_daily")]
#[case("a.b-c_D9")]
fn dag_id_accepts_valid_ids(#[case] id: &str) {
    assert_eq!(DagId::new(id).unwrap().as_str(), id);
}

#[test]
fn dag_id_rejects_empty() {
    assert_eq!(DagId::new(""), Err(IdError::Empty { kind: "DAG ID" }));
}

#[test]
fn dag_id_rejects_too_long() {
    let id = "a".repeat(MAX_ID_LEN + 1);
    assert_eq!(
        DagId::new(id.clone()),
        Err(IdError::TooLong { kind: "DAG ID", id })
    );
}

#[test]
fn dag_id_accepts_max_length() {
    assert!(DagId::new("a".repeat(MAX_ID_LEN)).is_ok());
}

#[rstest]
#[case("with space", ' ')]
#[case("ünicode", 'ü')]
#[case("semi;colon", ';')]
fn dag_id_rejects_invalid_chars(#[case] id: &str, #[case] ch: char) {
    assert_eq!(
        DagId::new(id),
        Err(IdError::InvalidChar {
            kind: "DAG ID",
            id: id.to_owned(),
            ch,
            allowed: DAG_ID_ALLOWED
        })
    );
}

#[rstest]
#[case("extract")]
#[case("group.extract")]
#[case("outer.inner.load-2")]
fn task_id_accepts_valid_ids(#[case] id: &str) {
    assert_eq!(TaskId::new(id).unwrap().as_str(), id);
}

#[rstest]
#[case(".extract")]
#[case("extract.")]
#[case("group..extract")]
fn task_id_rejects_empty_parts(#[case] id: &str) {
    assert_eq!(
        TaskId::new(id),
        Err(IdError::EmptyPart { id: id.to_owned() })
    );
}

#[rstest]
#[case("a:b", ':')]
#[case("a/b", '/')]
#[case("a#b", '#')]
fn task_id_rejects_dag_only_chars(#[case] id: &str, #[case] ch: char) {
    assert_eq!(
        TaskId::new(id),
        Err(IdError::InvalidChar {
            kind: "task ID",
            id: id.to_owned(),
            ch,
            allowed: TASK_ID_ALLOWED
        })
    );
}

#[test]
fn task_id_rejects_empty() {
    assert_eq!(TaskId::new(""), Err(IdError::Empty { kind: "task ID" }));
}

#[test]
fn display_from_str_and_string_round_trip() {
    let dag: DagId = "sync".parse().unwrap();
    let task: TaskId = "g.load".parse().unwrap();
    assert_eq!(dag.to_string(), "sync");
    assert_eq!(task.to_string(), "g.load");
    assert_eq!(String::from(dag), "sync");
    assert_eq!(String::from(task), "g.load");
    assert_eq!(DagId::try_from("x".to_owned()).unwrap().as_str(), "x");
    assert_eq!(TaskId::try_from("y".to_owned()).unwrap().as_str(), "y");
    assert!("bad id".parse::<TaskId>().is_err());
}

#[test]
fn ids_serialize_as_plain_strings() {
    let task = TaskId::new("g.load").unwrap();
    assert_eq!(
        serde_json::to_value(&task).unwrap(),
        serde_json::json!("g.load")
    );
    let back: TaskId = serde_json::from_value(serde_json::json!("g.load")).unwrap();
    assert_eq!(back, task);
}

#[test]
fn deserializing_an_invalid_id_fails_with_the_validation_message() {
    let err = serde_json::from_value::<DagId>(serde_json::json!("bad id")).unwrap_err();
    assert!(
        err.to_string().contains("DAG ID `bad id` contains ` `"),
        "{err}"
    );
}

#[test]
fn error_messages_are_readable() {
    assert_eq!(
        IdError::Empty { kind: "task ID" }.to_string(),
        "task ID is empty"
    );
    assert_eq!(
        IdError::EmptyPart { id: "a..b".into() }.to_string(),
        "task ID `a..b` has an empty group or task name around `.`"
    );
    assert_eq!(
        IdError::TooLong {
            kind: "DAG ID",
            id: "x".into()
        }
        .to_string(),
        format!("DAG ID `x` is longer than {MAX_ID_LEN} bytes")
    );
}

#[test]
fn schemas_describe_strings_with_the_validation_pattern() {
    let dag = serde_json::to_value(schema_for!(DagId)).unwrap();
    let task = serde_json::to_value(schema_for!(TaskId)).unwrap();
    assert_eq!(dag["type"], "string");
    assert_eq!(dag["maxLength"], MAX_ID_LEN);
    assert_eq!(dag["pattern"], r"^[A-Za-z0-9_.:/#-]+$");
    assert_eq!(task["pattern"], r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$");
    assert_eq!(dag["title"], "DagId");
}
