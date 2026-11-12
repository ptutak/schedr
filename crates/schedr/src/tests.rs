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
fn a_secrets_usage_error_doesnt_show_the_arguments() {
    // Grok's finding in M4.6c's security review: clap quoted `password=...` on stderr.
    let result = schedr(&["secrets", "get", "etl/db", "password=hunter22"], &[], &[]);
    assert_eq!(result.code, 2);
    assert!(!result.err.contains("hunter22"), "{}", result.err);
    assert!(
        result.err.contains("schedr secrets --help"),
        "{}",
        result.err
    );
    // Help still prints.
    let help = schedr(&["secrets", "--help"], &[], &[]);
    assert_eq!(help.code, 0);
    assert!(help.out.contains("export"), "{}", help.out);
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

/// B236: an earlier schedr's database: `db migrate` says a fresh one is needed, not sqlx's
/// "previously applied but has been modified", and `db status` says why.
#[test]
fn db_migrate_and_status_name_an_earlier_schedrs_database() {
    let dir = tempfile::TempDir::new().unwrap();
    let url = format!("sqlite://{}", dir.path().join("schedr.db").display());
    let env = [("SCHEDR_CONFIG__DATABASE__URL", url.as_str())];
    assert_eq!(schedr(&["db", "migrate"], &env, &[]).code, 0);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        sqlx::raw_sql("UPDATE _sqlx_migrations SET checksum = x'00'")
            .execute(&pool)
            .await
            .unwrap();
    });
    let result = schedr(&["db", "migrate"], &env, &[]);
    assert_eq!(result.code, 1, "{}", result.out);
    assert!(
        result.err.contains("needs a fresh database"),
        "{}",
        result.err
    );
    let result = schedr(&["db", "status"], &env, &[]);
    assert!(
        result.out.contains("created by an earlier schedr"),
        "{}",
        result.out
    );
}

#[test]
fn db_migrate_refuses_while_a_master_holds_the_lease() {
    // Migrating under a running master (or a second migration) is ruled out by the master's
    // lease, which works through any pooler (sqlx's own migration lock doesn't).
    let dir = tempfile::TempDir::new().unwrap();
    let url = format!("sqlite://{}", dir.path().join("schedr.db").display());
    let env = [("SCHEDR_CONFIG__DATABASE__URL", url.as_str())];
    assert_eq!(schedr(&["db", "migrate"], &env, &[]).code, 0);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = schedr_db::Database::connect(&url).await.unwrap();
        let period = std::time::Duration::from_secs(3600);
        assert!(db.take_lease("master", period).await.unwrap().is_some());
    });
    let result = schedr(&["db", "migrate"], &env, &[]);
    assert_eq!(result.code, 1, "{}", result.out);
    assert!(
        result.err.contains("a master holds the database's lease"),
        "{}",
        result.err
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
fn task_ctx_outside_a_task_says_where_it_works() {
    let result = schedr(&["task-ctx", "secret", "get", "x#k"], &[], &[]);
    assert_eq!(result.code, 1);
    assert!(
        result
            .err
            .starts_with("error: `schedr task-ctx` must be run inside a schedr task"),
        "{}",
        result.err
    );
}

#[test]
fn dag_ctx_outside_a_task_says_where_it_works() {
    // Even with a configuration that doesn't load: inside a task, none is read.
    let result = schedr(
        &["dag-ctx", "params", "get"],
        &[("SCHEDR_CONFIG__LOG__LEVEL", "a=b=c")],
        &[],
    );
    assert_eq!(result.code, 1);
    assert!(
        result
            .err
            .starts_with("error: `schedr dag-ctx` must be run inside a schedr task"),
        "{}",
        result.err
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
