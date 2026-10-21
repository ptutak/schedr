use std::collections::HashMap;

use pretty_assertions::assert_eq;

use super::*;

fn file(path: &str, contents: &str) -> GeneratedFile {
    GeneratedFile {
        path: PathBuf::from(path),
        contents: contents.to_owned(),
    }
}

#[test]
fn render_json_is_pretty_with_a_trailing_newline() {
    let text = render_json(&serde_json::json!({"a": [1]}));
    assert_eq!(text, "{\n  \"a\": [\n    1\n  ]\n}\n");
}

#[test]
fn schema_files_contain_the_dag_schema() {
    let files = schema_files();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, Path::new("schema").join("dag.schema.json"));
    let parsed: serde_json::Value = serde_json::from_str(&files[0].contents).unwrap();
    assert_eq!(
        parsed,
        serde_json::to_value(schedr_model::schema::dag_schema()).unwrap()
    );
    assert!(files[0].contents.ends_with("}\n"));
}

#[test]
fn stale_files_lists_missing_and_changed_files_only() {
    let files = vec![
        file("same.json", "x\n"),
        file("changed.json", "new\n"),
        file("missing.json", "y\n"),
    ];
    let committed: HashMap<PathBuf, String> = HashMap::from([
        (PathBuf::from("same.json"), "x\n".to_owned()),
        (PathBuf::from("changed.json"), "old\n".to_owned()),
    ]);
    let stale = stale_files(&files, |p| committed.get(p).cloned());
    assert_eq!(
        stale,
        vec![Path::new("changed.json"), Path::new("missing.json")]
    );
}

#[test]
fn stale_files_is_empty_when_everything_matches() {
    let files = vec![file("a.json", "1\n")];
    assert_eq!(
        stale_files(&files, |_| Some("1\n".to_owned())),
        Vec::<&Path>::new()
    );
}

#[test]
fn stale_message_lists_the_files_and_the_fix() {
    let message = stale_message(&[Path::new("schema/dag.schema.json")]);
    assert_eq!(
        message,
        "generated files are out of date:\n  schema/dag.schema.json\nrun `make schema` and commit the result"
    );
}

#[test]
fn parse_args_accepts_schema_with_optional_check() {
    assert_eq!(parse_args(&["schema"]), Ok(Command::WriteSchemas));
    assert_eq!(
        parse_args(&["schema", "--check"]),
        Ok(Command::CheckSchemas)
    );
}

#[test]
fn parse_args_rejects_anything_else() {
    let usage = Err("usage: cargo xtask schema [--check]".to_owned());
    assert_eq!(parse_args::<&str>(&[]), usage);
    assert_eq!(parse_args(&["other"]), usage);
    assert_eq!(parse_args(&["schema", "--force"]), usage);
}
