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
#[case(&["dag-ctx", "run", "get"], Some("M4"))]
#[case(&["master"], None)]
#[case(&["worker"], None)]
#[case(&["webserver"], None)]
#[case(&["tasks", "retry", "r", "t"], Some("M2"))]
#[case(&["workers", "resume", "w"], Some("M2"))]
#[case(&["workers", "list"], None)]
#[case(&["task-ctx", "resource", "get"], Some("M4"))]
#[case(&["dag-ctx", "params", "get", "--field", "region"], Some("M4"))]
fn milestones_say_when_a_command_starts_working(
    #[case] args: &[&str],
    #[case] expected: Option<&str>,
) {
    assert_eq!(parse(args).unwrap().command.milestone(), expected);
}
