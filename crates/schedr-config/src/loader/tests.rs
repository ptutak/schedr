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
    assert_eq!(config.builtin_python.as_deref(), Some("built-in default"));
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
fn a_list_comes_from_the_environment_as_an_array() {
    // The Helm chart's `passEnv` sets `[worker] pass_env` this way.
    let vars = env(&[(
        "SCHEDR_CONFIG__WORKER__PASS_ENV",
        r#"["MONGO_URI", "API_TOKEN"]"#,
    )]);
    let config = load(None, &vars, &[]).unwrap();
    assert_eq!(config.worker.pass_env, ["MONGO_URI", "API_TOKEN"]);
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
    assert_eq!(config.builtin_python, None);
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
fn a_notifications_url_with_a_password_is_refused_from_the_file() {
    let contents = "[database]\nnotifications_url = \"postgres://schedr:secret@pg/schedr\"\n";
    let err = load(Some(file(contents)), &env(&[]), &[]).unwrap_err();
    assert_eq!(
        err,
        ConfigError::SecretInFile {
            key: "database.notifications_url",
            env: "SCHEDR_CONFIG__DATABASE__NOTIFICATIONS_URL"
        }
    );
    let vars = env(&[(
        "SCHEDR_CONFIG__DATABASE__NOTIFICATIONS_URL",
        "postgres://schedr:secret@pg/schedr",
    )]);
    let config = load(Some(file("")), &vars, &[]).unwrap();
    assert_eq!(
        config.database.notifications_url.as_deref(),
        Some("postgres://schedr:secret@pg/schedr")
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

#[test]
fn roles_take_bind_addresses_and_the_masters_url() {
    let defaults = load(None, &env(&[]), &[]).unwrap();
    assert_eq!(defaults.master.bind.to_string(), "0.0.0.0");
    assert_eq!(defaults.webserver.bind.to_string(), "0.0.0.0");
    assert_eq!(defaults.worker.master_url, None);
    let vars = env(&[
        ("SCHEDR_CONFIG__MASTER__BIND", "127.0.0.1"),
        ("SCHEDR_CONFIG__WEBSERVER__BIND", "::1"),
        (
            "SCHEDR_CONFIG__WORKER__MASTER_URL",
            "http://schedr-master:8070",
        ),
    ]);
    let config = load(None, &vars, &[]).unwrap();
    assert_eq!(config.master.bind.to_string(), "127.0.0.1");
    assert_eq!(config.webserver.bind.to_string(), "::1");
    assert_eq!(
        config.worker.master_url.as_deref(),
        Some("http://schedr-master:8070")
    );
    let bad = load(
        None,
        &env(&[("SCHEDR_CONFIG__MASTER__BIND", "nowhere")]),
        &[],
    )
    .unwrap_err();
    assert!(
        bad.to_string()
            .contains("invalid IP address syntax for key \"SCHEDR_CONFIG__MASTER__BIND\""),
        "{bad}"
    );
}

#[test]
fn the_drain_times_must_fit_in_the_termination_grace_period() {
    // Exactly the grace period: the tasks get no time of their own, but it is allowed.
    let fits = load(
        Some(file(
            "[worker]\ntermination_grace_period = \"10s\"\n\
             drain_terminate_period = \"6s\"\ndrain_report_period = \"4s\"\n",
        )),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(fits.worker.drain_terminate_period, Duration::from_secs(6));
    assert_eq!(fits.worker.drain_report_period, Duration::from_secs(4));
    let error = load(
        Some(file("[worker]\ntermination_grace_period = \"4s\"\n")),
        &env(&[]),
        &[],
    )
    .unwrap_err();
    assert_eq!(
        error,
        ConfigError::DrainPeriods {
            terminate_period: "3s".to_owned(),
            report_period: "2s".to_owned(),
            grace: "4s".to_owned(),
        }
    );
    assert!(
        error.to_string().starts_with(
            "[worker] drain_terminate_period (3s) + drain_report_period (2s) must not exceed"
        ),
        "{error}"
    );
}

#[test]
fn the_charts_fork_server_variables_make_an_interpreter_table() {
    // What the chart sets for its workers (`worker.forkServer`).
    let vars = env(&[
        (
            "SCHEDR_CONFIG__INTERPRETERS__PYTHON__PATH",
            "/usr/local/bin/python3",
        ),
        ("SCHEDR_CONFIG__INTERPRETERS__PYTHON__FORK_SERVER", "true"),
        (
            "SCHEDR_CONFIG__INTERPRETERS__PYTHON__PRELOAD",
            r#"["requests","pymongo"]"#,
        ),
    ]);
    let config = load(None, &vars, &[]).unwrap();
    let python = &config.interpreters["python"];
    assert_eq!(
        python.path,
        std::path::PathBuf::from("/usr/local/bin/python3")
    );
    assert!(python.fork_server);
    assert_eq!(python.preload, ["requests", "pymongo"]);
}

#[test]
fn the_grpc_message_size_is_read_from_the_file_and_env_within_its_range() {
    let from_file = load(
        Some(file("[master]\ngrpc_max_message_size = \"16MiB\"\n")),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(from_file.master.grpc_max_message_size, 16 * 1024 * 1024);
    let from_env = load(
        None,
        &env(&[("SCHEDR_CONFIG__MASTER__GRPC_MAX_MESSAGE_SIZE", "1GiB")]),
        &[],
    )
    .unwrap();
    assert_eq!(from_env.master.grpc_max_message_size, 1024 * 1024 * 1024);
    for (size, shown) in [("4194303B", "4194303B"), ("2GiB", "2GiB")] {
        let error = load(
            None,
            &env(&[("SCHEDR_CONFIG__MASTER__GRPC_MAX_MESSAGE_SIZE", size)]),
            &[],
        )
        .unwrap_err();
        assert_eq!(
            error,
            ConfigError::MessageSize {
                size: shown.to_owned()
            }
        );
        assert!(
            error
                .to_string()
                .starts_with("[master] grpc_max_message_size (")
        );
    }
}

#[test]
fn mapping_max_instances_is_read_and_must_be_positive() {
    assert_eq!(
        load(None, &env(&[]), &[]).unwrap().mapping.max_instances,
        1024
    );
    let set = load(
        Some(file("[mapping]\nmax_instances = 50\n")),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(set.mapping.max_instances, 50);
    let zero = load(
        None,
        &env(&[("SCHEDR_CONFIG__MAPPING__MAX_INSTANCES", "0")]),
        &[],
    )
    .unwrap_err();
    assert_eq!(zero, ConfigError::NoInstances);
}
