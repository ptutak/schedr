use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pretty_assertions::assert_eq;

use super::*;
use crate::roots::Root;

const FILE: &str = "/etc/schedr/config.toml";

fn env(vars: &[(&str, &str)]) -> BTreeMap<String, String> {
    vars.iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn file(contents: &str) -> FileSource<'_> {
    FileSource {
        path: Path::new(FILE),
        contents,
    }
}

fn flag(key: &str, value: &str) -> Override {
    Override {
        key: key.into(),
        value: value.into(),
        flag: format!("--{key}"),
    }
}

#[test]
fn no_sources_gives_the_defaults_with_the_builtin_python() {
    let config = load(None, &env(&[]), &[]).unwrap();
    assert_eq!(config.dags.interpreter, "python");
    assert_eq!(config.interpreters["python"], Interpreter::path("python3"));
    assert_eq!(config.dag_roots, Vec::<Root>::new());
    assert_eq!(config.script_roots, Vec::<Root>::new());
    assert_eq!(config.ops.port, 8090);
}

#[test]
fn precedence_is_flags_then_environment_then_file() {
    let contents = "[ops]\nport = 9001\nloop_stall_timeout = \"1m\"\n[log]\nlevel = \"warn\"\n";
    let vars = env(&[
        ("SCHEDR_CONFIG__OPS__PORT", "9002"),
        ("SCHEDR_CONFIG__LOG__LEVEL", "debug"),
    ]);
    let config = load(Some(file(contents)), &vars, &[flag("ops.port", "9003")]).unwrap();
    assert_eq!(config.ops.port, 9003);
    assert_eq!(config.log.level, "debug");
    assert_eq!(config.ops.loop_stall_timeout, Duration::from_secs(60));
}

#[test]
fn resolves_dag_and_script_roots() {
    let contents = "[dags.roots]\netl = \"/git/dags/etl-repo\"\n";
    let vars = env(&[
        ("SCHEDR_DAG_PATH", "/git/dags/etl-repo:/git/dags/ml"),
        ("SCHEDR_SCRIPT_PATH", "/git/scripts/ops"),
    ]);
    let config = load(Some(file(contents)), &vars, &[]).unwrap();
    assert_eq!(
        config.dag_roots,
        vec![
            Root {
                name: "etl".into(),
                path: "/git/dags/etl-repo".into()
            },
            Root {
                name: "ml".into(),
                path: "/git/dags/ml".into()
            },
        ]
    );
    assert_eq!(
        config.script_roots,
        vec![Root {
            name: "ops".into(),
            path: "/git/scripts/ops".into()
        }]
    );
}

#[test]
fn a_configured_python_replaces_the_builtin_one() {
    let config = load(
        Some(file("[interpreters]\npython = \"/usr/bin/python3.14\"\n")),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(
        config.interpreters["python"],
        Interpreter::path("/usr/bin/python3.14")
    );
}

#[test]
fn rejects_an_undefined_dag_interpreter() {
    let contents = "[dags]\ninterpreter = \"etl-venv\"\n[interpreters]\nbash = \"/bin/bash\"\n";
    let err = load(Some(file(contents)), &env(&[]), &[]).unwrap_err();
    assert_eq!(
        err,
        ConfigError::UnknownInterpreter {
            name: "etl-venv".into(),
            defined: "bash, python".into()
        }
    );
    assert_eq!(
        err.to_string(),
        "[dags] interpreter `etl-venv` is not defined in [interpreters] (defined: bash, python)"
    );
}

#[test]
fn rejects_invalid_roots() {
    let err = load(None, &env(&[("SCHEDR_DAG_PATH", "/git:/git/dags")]), &[]).unwrap_err();
    assert!(
        matches!(err, ConfigError::Roots(RootError::Nested { .. })),
        "{err:?}"
    );
    let err = load(None, &env(&[("SCHEDR_SCRIPT_PATH", "/a/x:/b/x")]), &[]).unwrap_err();
    assert!(
        matches!(err, ConfigError::Roots(RootError::DuplicateName { .. })),
        "{err:?}"
    );
}

#[test]
fn rejects_environment_errors() {
    let vars = env(&[
        ("SCHEDR_DAG_PATH", "/a"),
        ("SCHEDR_CONFIG__DAGS__PATH", "/b"),
    ]);
    let err = load(None, &vars, &[]).unwrap_err();
    assert!(
        matches!(err, ConfigError::Source(SourceError::AliasConflict { .. })),
        "{err:?}"
    );
}

#[test]
fn invalid_values_name_their_origin() {
    let err = load(Some(file("[ops]\nport = \"high\"\n")), &env(&[]), &[])
        .unwrap_err()
        .to_string();
    assert!(err.contains("configuration file"), "{err}");
    assert!(err.contains(FILE), "{err}");

    let err = load(
        None,
        &env(&[("SCHEDR_CONFIG__DAGS__SETTLE_TIME", "soon")]),
        &[],
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("SCHEDR_CONFIG__DAGS__SETTLE_TIME"), "{err}");
    assert!(err.contains("is not a duration"), "{err}");
}

#[test]
fn rejects_unknown_keys_and_bad_toml() {
    let err = load(Some(file("[dags]\npoll = \"1m\"\n")), &env(&[]), &[]).unwrap_err();
    assert!(
        matches!(&err, ConfigError::Invalid(m) if m.contains("unknown field")),
        "{err:?}"
    );
    let err = load(Some(file("[dags\n")), &env(&[]), &[]).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid(_)), "{err:?}");
}

#[test]
fn every_figment_error_is_kept_in_the_message() {
    let err = load(
        Some(file("[ops]\nport = \"a\"\nloop_stall_timeout = 5\n")),
        &env(&[]),
        &[],
    )
    .unwrap_err();
    let ConfigError::Invalid(message) = err else {
        panic!("expected Invalid, got {err:?}")
    };
    assert!(
        message.contains("port") || message.contains("loop_stall_timeout"),
        "{message}"
    );
}

#[test]
fn paths_in_roots_are_kept_as_given() {
    let config = load(None, &env(&[("SCHEDR_DAG_PATH", "dags/local")]), &[]).unwrap();
    assert_eq!(config.dag_roots[0].path, PathBuf::from("dags/local"));
}

#[test]
fn a_database_url_with_a_password_is_refused_from_the_file() {
    let contents = "[database]\nurl = \"postgres://schedr:secret@db/schedr\"\n";
    let err = load(Some(file(contents)), &env(&[]), &[]).unwrap_err();
    assert_eq!(
        err,
        ConfigError::SecretInFile {
            key: "database.url",
            env: "SCHEDR_CONFIG__DATABASE__URL"
        }
    );
    assert!(
        err.to_string().contains("never in the configuration file"),
        "{err}"
    );
}

#[test]
fn a_database_url_with_a_password_is_accepted_from_the_environment() {
    let vars = env(&[(
        "SCHEDR_CONFIG__DATABASE__URL",
        "postgres://schedr:secret@db/schedr",
    )]);
    let config = load(Some(file("")), &vars, &[]).unwrap();
    assert_eq!(
        config.database.url.as_deref(),
        Some("postgres://schedr:secret@db/schedr")
    );
}

#[test]
fn a_database_url_without_a_password_may_be_in_the_file() {
    let contents = "[database]\nurl = \"postgres://schedr@db/schedr\"\n";
    let config = load(Some(file(contents)), &env(&[]), &[]).unwrap();
    assert_eq!(
        config.database.url.as_deref(),
        Some("postgres://schedr@db/schedr")
    );
}
