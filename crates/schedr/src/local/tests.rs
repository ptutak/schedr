use pretty_assertions::assert_eq;
use rstest::rstest;
use tempfile::TempDir;

use super::*;

fn test_command(changes: impl FnOnce(&mut DagsCommand)) -> DagsCommand {
    let mut command = DagsCommand::Test {
        dag: "sync".to_owned(),
        planned_for: None,
        params: vec![],
        secrets: vec![],
        task: None,
        path: None,
    };
    changes(&mut command);
    command
}

#[test]
fn a_directory_is_a_root_of_its_python_files() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("etl");
    std::fs::create_dir_all(root.join("team")).unwrap();
    std::fs::write(root.join("team/sync.py"), "").unwrap();
    std::fs::write(root.join("notes.md"), "").unwrap();
    std::fs::write(root.join("old.py"), "").unwrap();
    std::fs::write(root.join(".schedrignore"), "old.py\n").unwrap();
    let found = LocalRoot::at(&root).unwrap();
    assert_eq!(found.name, "etl");
    assert_eq!(found.dir, root.canonicalize().unwrap());
    assert_eq!(found.files, ["team/sync.py"]);
}

#[test]
fn a_file_is_a_root_of_one_file_in_its_directory() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("sync.py"), "").unwrap();
    let found = LocalRoot::at(&dir.path().join("sync.py")).unwrap();
    assert_eq!(found.dir, dir.path().canonicalize().unwrap());
    assert_eq!(found.files, ["sync.py"]);
}

#[test]
fn a_missing_path_is_a_bad_argument() {
    let dir = TempDir::new().unwrap();
    let error = LocalRoot::at(&dir.path().join("nope")).unwrap_err();
    assert!(error.to_string().contains("nope"), "{error}");
}

#[rstest]
#[case(test_command(|c| if let DagsCommand::Test { params, .. } = c { params.push("a=1".to_owned()); }), "dags test -p", "M4")]
#[case(test_command(|c| if let DagsCommand::Test { secrets, .. } = c { secrets.push("p#k=v".to_owned()); }), "dags test --secret", "M4")]
#[case(test_command(|c| if let DagsCommand::Test { task, .. } = c { *task = Some("load".to_owned()); }), "dags test --task", "M4")]
#[case(DagsCommand::Check { path: ".".into(), against: Some("http://x".to_owned()) }, "dags check --against", "M2")]
#[tokio::test]
async fn later_features_say_when_they_come(
    #[case] command: DagsCommand,
    #[case] what: &str,
    #[case] milestone: &str,
) {
    let error = run_local(
        &command,
        &Config::default(),
        &BTreeMap::new(),
        false,
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("`schedr {what}` isn't implemented yet (milestone {milestone})")
    );
}

#[tokio::test]
async fn a_bad_for_and_an_undefined_interpreter_are_bad_arguments() {
    let bad_for = test_command(|c| {
        if let DagsCommand::Test { planned_for, .. } = c {
            *planned_for = Some("someday".to_owned());
        }
    });
    let error = run_local(
        &bad_for,
        &Config::default(),
        &BTreeMap::new(),
        false,
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("isn't a date or a date-time"),
        "{error}"
    );

    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("sync.py"), "").unwrap();
    let check = DagsCommand::Check {
        path: dir.path().to_path_buf(),
        against: None,
    };
    let error = run_local(
        &check,
        &Config::default(),
        &BTreeMap::new(),
        false,
        &mut Vec::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "the DAG interpreter `python` isn't defined in [interpreters]"
    );
}

#[test]
fn durations_come_from_the_last_attempt() {
    let task = json!({"attempts": [
        {"started_at": "2026-10-21T08:00:00Z", "ended_at": "2026-10-21T08:00:05Z"},
        {"started_at": "2026-10-21T08:01:00Z", "ended_at": "2026-10-21T08:01:01.5Z"}
    ]});
    assert_eq!(duration(&task), "1.5s");
    let assigned_only = json!({"attempts": [
        {"started_at": null, "assigned_at": "2026-10-21T08:00:00Z", "ended_at": "2026-10-21T08:00:00.2Z"}
    ]});
    assert_eq!(duration(&assigned_only), "0.2s");
    assert_eq!(duration(&json!({"attempts": []})), "-");
    assert_eq!(
        duration(&json!({"attempts": [{"started_at": "2026-10-21T08:00:00Z", "ended_at": null}]})),
        "-"
    );
}
