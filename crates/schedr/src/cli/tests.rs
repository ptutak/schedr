use clap::CommandFactory;
use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("schedr").chain(args.iter().copied()))
}

#[test]
fn the_command_tree_is_consistent() {
    Cli::command().debug_assert();
}

#[rstest]
#[case(&["master"], "master")]
#[case(&["worker"], "worker")]
#[case(&["webserver"], "webserver")]
#[case(&["standalone"], "standalone")]
#[case(&["db", "migrate"], "db")]
#[case(&["secrets", "set", "etl/db", "password=-"], "secrets")]
#[case(&["dags", "list"], "dags")]
#[case(&["runs", "list", "sync"], "runs")]
#[case(&["tasks", "retry", "r1", "load"], "tasks")]
#[case(&["workers", "drain", "worker-1"], "workers")]
#[case(&["task-ctx", "secret", "get", "ci/deploy#token"], "task-ctx")]
#[case(&["dag-ctx", "run", "get"], "dag-ctx")]
fn every_top_level_command_parses(#[case] args: &[&str], #[case] name: &str) {
    assert_eq!(parse(args).unwrap().command.name(), name);
}

#[test]
fn config_is_a_global_flag() {
    let cli = parse(&["db", "status", "--config", "/etc/x.toml"]).unwrap();
    assert_eq!(cli.config, Some(PathBuf::from("/etc/x.toml")));
    assert!(matches!(
        cli.command,
        Command::Db {
            command: DbCommand::Status
        }
    ));
}

#[test]
fn runs_trigger_takes_for_and_repeated_params() {
    let cli = parse(&[
        "runs",
        "trigger",
        "report",
        "--for",
        "2026-10-15T03:00",
        "-p",
        "region=us",
        "-p",
        "full:=true",
    ])
    .unwrap();
    let Command::Runs { command, client } = cli.command else {
        panic!("not runs")
    };
    assert_eq!(
        command,
        RunsCommand::Trigger {
            event_key: None,
            event_data: None,
            event_at: None,
            event: None,
            dag: "report".into(),
            planned_for: Some("2026-10-15T03:00".into()),
            params: vec!["region=us".into(), "full:=true".into()],
            params_json: None,
        }
    );
    assert_eq!(client.server, "http://localhost:8080");
    assert!(!client.json);
}

#[test]
fn runs_trigger_rejects_params_and_params_json_together() {
    assert!(parse(&["runs", "trigger", "r", "-p", "a=1", "--params", "{}"]).is_err());
}

#[test]
fn runs_cancel_needs_a_run_id_or_a_backfill() {
    assert!(parse(&["runs", "cancel"]).is_err());
    assert!(parse(&["runs", "cancel", "r1", "--backfill", "b1"]).is_err());
    let cli = parse(&["runs", "cancel", "--backfill", "b1"]).unwrap();
    assert!(matches!(
        cli.command,
        Command::Runs {
            command: RunsCommand::Cancel {
                run_id: None,
                backfill: Some(_)
            },
            ..
        }
    ));
}

#[test]
fn runs_backfill_requires_from() {
    assert!(parse(&["runs", "backfill", "sync"]).is_err());
    let cli = parse(&["runs", "backfill", "sync", "--from", "2026-10-01", "--all"]).unwrap();
    assert!(matches!(
        cli.command,
        Command::Runs {
            command: RunsCommand::Backfill {
                all: true,
                to: None,
                ..
            },
            ..
        }
    ));
}

#[test]
fn client_flags_are_accepted_after_the_subcommand() {
    let cli = parse(&[
        "dags",
        "list",
        "--server",
        "https://schedr.example",
        "--json",
    ])
    .unwrap();
    let Command::Dags { client, .. } = cli.command else {
        panic!("not dags")
    };
    assert_eq!(client.server, "https://schedr.example");
    assert!(client.json);
}

#[test]
fn tasks_take_a_run_a_task_and_an_optional_map_index() {
    let cli = parse(&["tasks", "cancel", "r1", "process", "--map-index", "3"]).unwrap();
    let Command::Tasks { command, .. } = cli.command else {
        panic!("not tasks")
    };
    assert_eq!(
        command,
        TasksCommand::Cancel(TaskTarget {
            run_id: "r1".into(),
            task_id: "process".into(),
            map_index: Some(3)
        })
    );
}

#[test]
fn tasks_logs_take_a_try_and_follow() {
    let cli = parse(&["tasks", "logs", "r1", "load", "--try", "2", "-f"]).unwrap();
    let Command::Tasks { command, .. } = cli.command else {
        panic!("not tasks")
    };
    assert_eq!(
        command,
        TasksCommand::Logs(LogsArgs {
            target: TaskTarget {
                run_id: "r1".into(),
                task_id: "load".into(),
                map_index: None
            },
            try_number: Some(2),
            follow: true,
        })
    );
    let cli = parse(&["tasks", "logs", "r1", "load"]).unwrap();
    let Command::Tasks {
        command: TasksCommand::Logs(args),
        ..
    } = cli.command
    else {
        panic!("not tasks logs")
    };
    assert_eq!((args.try_number, args.follow), (None, false));
}

#[test]
fn task_ctx_cleanup_add_takes_a_type_and_fields() {
    let cli = parse(&[
        "task-ctx",
        "cleanup",
        "add",
        "ExportJob",
        "job_id=j-1",
        "size:=8",
    ])
    .unwrap();
    let Command::TaskCtx { command } = cli.command else {
        panic!("not task-ctx")
    };
    assert_eq!(
        command,
        TaskCtxCommand::Cleanup {
            command: CleanupCommand::Add {
                handle_type: "ExportJob".into(),
                fields: vec!["job_id=j-1".into(), "size:=8".into()],
                json: None,
            }
        }
    );
}

#[test]
fn dags_test_takes_secrets_and_a_task() {
    let cli = parse(&[
        "dags",
        "test",
        "sync",
        "--secret",
        "etl/db#password=x",
        "--task",
        "load",
    ])
    .unwrap();
    let Command::Dags { command, .. } = cli.command else {
        panic!("not dags")
    };
    assert!(
        matches!(command, DagsCommand::Test { task: Some(_), ref secrets, .. } if secrets.len() == 1)
    );
}

#[test]
fn standalone_help_describes_one_pod_in_production() {
    let mut command = Cli::command();
    let standalone = command.find_subcommand_mut("standalone").unwrap();
    assert_eq!(
        standalone.get_about().unwrap().to_string(),
        "Run every role in one process (SQLite or Postgres): local development, or one pod in production"
    );
    let help = standalone.render_long_help().to_string();
    assert!(
        help.contains("SQLite or Postgres") && help.contains("one pod in production"),
        "{help}"
    );
}

#[test]
fn dags_check_against_says_it_fails_for_an_older_worker() {
    let mut command = Cli::command();
    let check = command
        .find_subcommand_mut("dags")
        .unwrap()
        .find_subcommand_mut("check")
        .unwrap();
    let against = check
        .get_arguments()
        .find(|arg| arg.get_id() == "against")
        .unwrap();
    assert_eq!(
        against.get_help().unwrap().to_string(),
        "Fail if an active worker at this server runs an older schedr (major.minor) than this one"
    );
}

#[test]
fn version_and_help_are_reported_as_clap_display_errors() {
    assert_eq!(
        parse(&["--version"]).unwrap_err().kind(),
        clap::error::ErrorKind::DisplayVersion
    );
    assert_eq!(
        parse(&["--help"]).unwrap_err().kind(),
        clap::error::ErrorKind::DisplayHelp
    );
}

#[rstest]
#[case(&["db", "migrate"], None)]
#[case(&["standalone"], None)]
#[case(&["standalone", "--listen", "0.0.0.0"], None)]
#[case(&["dags", "list"], None)]
#[case(&["runs", "list", "sync"], None)]
#[case(&["dag-ctx", "run", "get"], None)]
#[case(&["master"], None)]
#[case(&["worker"], None)]
#[case(&["webserver"], None)]
#[case(&["tasks", "retry", "r", "t"], None)]
#[case(&["workers", "resume", "w"], None)]
#[case(&["workers", "list"], None)]
#[case(&["task-ctx", "resource", "get"], None)]
#[case(&["dag-ctx", "params", "get", "--field", "region"], None)]
fn milestones_say_when_a_command_starts_working(
    #[case] args: &[&str],
    #[case] expected: Option<&str>,
) {
    assert_eq!(parse(args).unwrap().command.milestone(), expected);
}

#[test]
fn a_connection_test_without_a_timeout_leaves_it_to_the_webserver() {
    let timeout_of = |args: &[&str]| match parse(args).unwrap().command {
        Command::Connections {
            command: ConnectionsCommand::Test { timeout, .. },
            ..
        } => timeout,
        other => panic!("expected a connection test, got {other:?}"),
    };
    assert_eq!(timeout_of(&["connections", "test", "db"]), None);
    assert_eq!(
        timeout_of(&["connections", "test", "db", "--timeout", "2m"]),
        Some(std::time::Duration::from_mins(2))
    );
}

#[rstest]
#[case(".", "/home/me/project")]
#[case("./", "/home/me/project")]
#[case("./dags/", "dags")]
#[case("code/etl", "code/etl")]
#[case("../shared", "../shared")]
#[case("/srv/dags", "/srv/dags")]
fn a_root_path_is_kept_as_given_unless_it_has_no_name(#[case] given: &str, #[case] root: &str) {
    assert_eq!(
        root_path(Path::new("/home/me/project"), Path::new(given)),
        PathBuf::from(root)
    );
}

#[test]
fn a_root_path_ending_in_dot_dot_is_the_folder_on_the_filesystem() {
    // `live` links to `releases/v1`: `live/..` is `releases`, not the folder holding `live`.
    let dir = tempfile::TempDir::new().unwrap();
    let base = std::fs::canonicalize(dir.path()).unwrap();
    std::fs::create_dir_all(base.join("releases/v1")).unwrap();
    std::fs::create_dir(base.join("caller")).unwrap();
    std::os::unix::fs::symlink(base.join("releases/v1"), base.join("caller/live")).unwrap();
    assert_eq!(
        root_path(&base, Path::new("caller/live/..")),
        base.join("releases")
    );
    // One in the middle is the filesystem's to resolve: kept.
    assert_eq!(
        root_path(&base, Path::new("caller/live/../dags")),
        PathBuf::from("caller/live/../dags")
    );
    assert_eq!(
        root_path(&base, Path::new("missing/..")),
        base.join("missing/.."),
        "left as it is, refused for having no name"
    );
}

#[test]
fn a_root_path_that_isnt_utf8_is_refused() {
    use std::os::unix::ffi::OsStrExt;
    let folder = std::ffi::OsStr::from_bytes(b"root-\xff");
    let cli = Cli::try_parse_from([
        std::ffi::OsStr::new("schedr"),
        std::ffi::OsStr::new("standalone"),
        std::ffi::OsStr::new("--script-path"),
        folder,
    ])
    .unwrap();
    let err = cli.command.overrides(Path::new("/work")).unwrap_err();
    assert!(err.starts_with("--script-path: `root-"), "{err}");
    assert!(err.ends_with("isn't UTF-8; rename the folder"), "{err}");
}

#[test]
fn standalones_dag_and_script_paths_are_lists_over_the_configuration() {
    let cli = parse(&[
        "standalone",
        "--dag-path",
        ".",
        "--dag-path",
        "/srv/a:b,c",
        "--script-path",
        "scripts",
    ])
    .unwrap();
    assert_eq!(
        cli.command.overrides(Path::new("/work")).unwrap(),
        [
            Override {
                key: "dags.path".into(),
                value: OverrideValue::List(vec!["/work".into(), "/srv/a:b,c".into()]),
                flag: "--dag-path".into(),
            },
            Override {
                key: "scripts.path".into(),
                value: OverrideValue::List(vec!["scripts".into()]),
                flag: "--script-path".into(),
            },
        ]
    );
    let none = parse(&["standalone", "--migrate"]).unwrap();
    assert_eq!(none.command.overrides(Path::new("/work")).unwrap(), []);
    assert!(
        parse(&["worker", "--dag-path", "."]).is_err(),
        "standalone only"
    );
}
