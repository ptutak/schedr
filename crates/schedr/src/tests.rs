use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use pretty_assertions::assert_eq;

use super::*;

/// The result of one `run`: exit code, stdout, stderr.
struct Outcome {
    code: u8,
    out: String,
    err: String,
}

fn schedr(args: &[&str], env: &[(&str, &str)], files: &[(&str, &str)]) -> Outcome {
    let env: BTreeMap<String, String> = env
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    let files: BTreeMap<String, String> = files
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    let read = |path: &Path| {
        files
            .get(&path.display().to_string())
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    };
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(
        std::iter::once("schedr").chain(args.iter().copied()),
        &env,
        read,
        false,
        &mut out,
        &mut err,
    );
    Outcome {
        code,
        out: String::from_utf8(out).unwrap(),
        err: String::from_utf8(err).unwrap(),
    }
}

const MEMORY_DB: (&str, &str) = ("SCHEDR_CONFIG__DATABASE__URL", "sqlite::memory:");

#[test]
fn version_is_the_package_version() {
    assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
}

#[test]
fn version_flag_prints_the_version_and_succeeds() {
    let result = schedr(&["--version"], &[], &[]);
    assert_eq!(result.code, 0);
    assert_eq!(result.out, format!("schedr {VERSION}\n"));
}

#[test]
fn help_succeeds_and_lists_the_commands() {
    let result = schedr(&["--help"], &[], &[]);
    assert_eq!(result.code, 0);
    for command in [
        "master",
        "worker",
        "standalone",
        "db",
        "dags",
        "runs",
        "task-ctx",
    ] {
        assert!(result.out.contains(command), "{command} missing from help");
    }
}

#[test]
fn wrong_usage_exits_with_2() {
    let result = schedr(&["frobnicate"], &[], &[]);
    assert_eq!(result.code, EXIT_USAGE);
    assert!(result.err.contains("frobnicate"), "{}", result.err);
}

#[test]
fn no_arguments_is_wrong_usage() {
    assert_eq!(schedr(&[], &[], &[]).code, EXIT_USAGE);
}

#[test]
fn db_migrate_and_status_work_against_the_configured_database() {
    let result = schedr(&["db", "migrate"], &[MEMORY_DB], &[]);
    assert_eq!((result.code, result.err.as_str()), (0, ""));
    assert_eq!(result.out, "migrated sqlite::memory:: schema up to date\n");
    // A fresh in-memory database for `status`: not migrated yet.
    let result = schedr(&["db", "status"], &[MEMORY_DB], &[]);
    assert_eq!(result.code, 0);
    assert_eq!(
        result.out,
        "sqlite::memory:: uninitialized: run `schedr db migrate`\n"
    );
}

#[test]
fn db_without_a_database_url_fails_clearly() {
    let result = schedr(&["db", "migrate"], &[], &[]);
    assert_eq!(result.code, 1);
    assert!(
        result.err.contains("no database configured"),
        "{}",
        result.err
    );
}

#[test]
fn the_configuration_file_is_read_from_the_flag() {
    let file = "[database]\nurl = \"sqlite::memory:\"\n";
    let result = schedr(
        &["--config", "/etc/x.toml", "db", "migrate"],
        &[],
        &[("/etc/x.toml", file)],
    );
    assert_eq!(result.code, 0, "{}", result.err);
}

#[test]
fn a_missing_explicit_configuration_file_is_an_error() {
    let result = schedr(
        &["db", "status"],
        &[("SCHEDR_CONFIG_FILE", "/missing.toml")],
        &[],
    );
    assert_eq!(result.code, 1);
    assert!(
        result
            .err
            .contains("can't read the configuration file `/missing.toml`"),
        "{}",
        result.err
    );
}

#[test]
fn an_invalid_configuration_is_reported() {
    let result = schedr(
        &["db", "status"],
        &[("SCHEDR_CONFIG__OPS__PORT", "high")],
        &[],
    );
    assert_eq!(result.code, 1);
    assert!(result.err.starts_with("error: "), "{}", result.err);
    assert!(
        result.err.contains("SCHEDR_CONFIG__OPS__PORT"),
        "{}",
        result.err
    );
}

#[test]
fn an_invalid_log_level_is_reported() {
    let result = schedr(
        &["db", "status"],
        &[("SCHEDR_CONFIG__LOG__LEVEL", "a=b=c")],
        &[],
    );
    assert_eq!(result.code, 1);
    assert!(result.err.contains("[log] level"), "{}", result.err);
}

#[test]
fn commands_of_later_milestones_say_when_they_arrive() {
    let result = schedr(&["dag-ctx", "run", "get"], &[], &[]);
    assert_eq!(result.code, 1);
    assert_eq!(
        result.err,
        "error: `schedr dag-ctx` isn't implemented yet (milestone M4)\n"
    );
}

#[test]
fn the_master_without_a_database_says_how_to_set_one() {
    let result = schedr(
        &["master"],
        &[
            ("SCHEDR_CONFIG__TLS__MODE", "mesh"),
            ("SCHEDR_CONFIG__OPS__PORT", "0"),
        ],
        &[],
    );
    assert_eq!(result.code, 1);
    assert!(
        result.err.contains("SCHEDR_CONFIG__DATABASE__URL"),
        "{}",
        result.err
    );
}
