use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;
use clap::Parser;

use crate::cli::Cli;

fn env(vars: &[(&str, &str)]) -> BTreeMap<String, String> {
    vars.iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn not_found(_: &Path) -> io::Result<String> {
    Err(io::Error::from(io::ErrorKind::NotFound))
}

fn memory_config() -> Config {
    load_config(
        None,
        &env(&[("SCHEDR_CONFIG__DATABASE__URL", "sqlite::memory:")]),
        not_found,
    )
    .unwrap()
}

#[test]
fn a_missing_default_file_gives_defaults_and_environment() {
    let config = load_config(
        None,
        &env(&[("SCHEDR_CONFIG__OPS__PORT", "9100")]),
        not_found,
    )
    .unwrap();
    assert_eq!(config.ops.port, 9100);
}

#[test]
fn the_default_file_is_read_when_present() {
    let read = |path: &Path| {
        assert_eq!(path, Path::new("/etc/schedr/config.toml"));
        Ok("[ops]\nport = 9200\n".to_owned())
    };
    assert_eq!(load_config(None, &env(&[]), read).unwrap().ops.port, 9200);
}

#[test]
fn an_explicit_file_must_exist() {
    let err = load_config(Some(Path::new("/x.toml")), &env(&[]), not_found).unwrap_err();
    assert!(
        matches!(&err, AppError::ReadConfig { path, .. } if path == "/x.toml"),
        "{err:?}"
    );
}

#[test]
fn other_read_errors_are_reported_even_for_the_default_file() {
    let denied = |_: &Path| Err(io::Error::from(io::ErrorKind::PermissionDenied));
    let err = load_config(None, &env(&[]), denied).unwrap_err();
    assert!(
        err.to_string()
            .starts_with("can't read the configuration file `/etc/schedr/config.toml`: ")
    );
}

#[test]
fn invalid_configuration_is_a_config_error() {
    let err = load_config(None, &env(&[("SCHEDR_CONFIG__OPS__PORT", "x")]), not_found).unwrap_err();
    assert!(matches!(err, AppError::Config(_)), "{err:?}");
}

#[rstest]
#[case(SchemaStatus::UpToDate, "up to date")]
#[case(SchemaStatus::Uninitialized, "uninitialized: run `schedr db migrate`")]
#[case(SchemaStatus::Outdated { missing: vec![2, 3] }, "outdated: 2 migration(s) missing; run `schedr db migrate`")]
#[case(SchemaStatus::Newer { unknown: vec![9] }, "newer than this schedr (1 unknown migration(s)); usable within the same major version")]
fn describes_every_schema_status(#[case] status: SchemaStatus, #[case] expected: &str) {
    assert_eq!(describe_status(&status), expected);
}

#[tokio::test]
async fn run_db_migrate_reports_the_redacted_url() {
    let mut out = Vec::new();
    run_db(DbCommand::Migrate, &memory_config(), &mut out)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "migrated sqlite::memory:: schema up to date\n"
    );
}

#[tokio::test]
async fn run_db_status_reports_the_status() {
    let mut out = Vec::new();
    run_db(DbCommand::Status, &memory_config(), &mut out)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "sqlite::memory:: uninitialized: run `schedr db migrate`\n"
    );
}

#[tokio::test]
async fn run_db_needs_a_database_url() {
    let config = load_config(None, &env(&[]), not_found).unwrap();
    let err = run_db(DbCommand::Status, &config, &mut Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::NoDatabase));
}

#[tokio::test]
async fn run_db_reports_connection_errors() {
    let config = load_config(
        None,
        &env(&[("SCHEDR_CONFIG__DATABASE__URL", "mysql://h/db")]),
        not_found,
    )
    .unwrap();
    let err = run_db(DbCommand::Status, &config, &mut Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Db(_)), "{err:?}");
}

/// A writer that always fails.
struct Broken;

impl io::Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("broken pipe"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn output_errors_are_reported() {
    let err = run_db(DbCommand::Migrate, &memory_config(), &mut Broken)
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Output(_)), "{err:?}");
    assert_eq!(err.to_string(), "can't write output: broken pipe");
    assert!(Broken.flush().is_ok());
}

#[tokio::test]
async fn execute_dispatches_db_commands() {
    let cli = Cli::try_parse_from(["schedr", "db", "migrate"]).unwrap();
    let mut out = Vec::new();
    execute(&cli.command, &memory_config(), &mut out)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "migrated sqlite::memory:: schema up to date\n"
    );
}

#[rstest]
#[case(&["schedr", "standalone"], "standalone", "M1")]
#[case(&["schedr", "workers", "drain", "w1"], "workers", "M2")]
#[case(&["schedr", "task-ctx", "cleanup", "list"], "task-ctx", "M4")]
#[tokio::test]
async fn execute_reports_later_milestones(
    #[case] args: &[&str],
    #[case] command: &str,
    #[case] milestone: &str,
) {
    let cli = Cli::try_parse_from(args).unwrap();
    let err = execute(&cli.command, &memory_config(), &mut Vec::new())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, AppError::NotImplemented { command: c, milestone: m } if *c == command && *m == milestone)
    );
    assert_eq!(
        err.to_string(),
        format!("`schedr {command}` isn't implemented yet (milestone {milestone})")
    );
}

#[test]
fn every_error_exits_with_1() {
    assert_eq!(AppError::NoDatabase.exit_code(), 1);
    let not_implemented = AppError::NotImplemented {
        command: "x",
        milestone: "M1",
    };
    assert_eq!(not_implemented.exit_code(), 1);
}

#[test]
fn no_database_message_names_the_variable() {
    assert_eq!(
        AppError::NoDatabase.to_string(),
        "no database configured: set `SCHEDR_CONFIG__DATABASE__URL` (or [database] url)"
    );
}
