use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use pretty_assertions::assert_eq;
use rstest::rstest;

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
    let contents = "[ops]\nport = 9001\n[master]\ncancel_grace = \"1m\"\n[log]\nlevel = \"warn\"\n";
    let vars = env(&[
        ("SCHEDR_CONFIG__OPS__PORT", "9002"),
        ("SCHEDR_CONFIG__LOG__LEVEL", "debug"),
    ]);
    let config = load(Some(file(contents)), &vars, &[flag("ops.port", "9003")]).unwrap();
    assert_eq!(config.ops.port, 9003);
    assert_eq!(config.log.level, "debug");
    assert_eq!(config.master.cancel_grace, Duration::from_secs(60));
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
                path: "/git/dags/etl-repo".into(),
                only: None
            },
            Root {
                name: "ml".into(),
                path: "/git/dags/ml".into(),
                only: None
            },
        ]
    );
    assert_eq!(
        config.script_roots,
        vec![Root {
            name: "ops".into(),
            path: "/git/scripts/ops".into(),
            only: None
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
fn a_script_root_named_as_a_dag_root_is_an_error_until_renamed() {
    // Decision 299.
    let vars = env(&[
        ("SCHEDR_DAG_PATH", "/git/dags/etl"),
        ("SCHEDR_SCRIPT_PATH", "/git/scripts/etl"),
    ]);
    let err = load(None, &vars, &[]).unwrap_err();
    assert_eq!(
        err.to_string(),
        "SCHEDR_SCRIPT_PATH: `/git/scripts/etl` is named `etl`, as the DAG root `/git/dags/etl`; \
         rename one in [scripts.roots] or [dags.roots]"
    );
    let renamed = load(
        Some(file(
            "[scripts.roots]\netl-scripts = \"/git/scripts/etl\"\n",
        )),
        &vars,
        &[],
    )
    .unwrap();
    assert_eq!(renamed.script_roots[0].name, "etl-scripts");
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
        Some(file("[ops]\nport = \"a\"\n[master]\ncancel_grace = 5\n")),
        &env(&[]),
        &[],
    )
    .unwrap_err();
    let ConfigError::Invalid(message) = err else {
        panic!("expected Invalid, got {err:?}")
    };
    assert!(
        message.contains("port") || message.contains("cancel_grace"),
        "{message}"
    );
}

#[test]
fn a_relative_path_in_roots_is_made_absolute() {
    // B60: kept as given, a relative root broke parsing and task imports.
    let config = load(None, &env(&[("SCHEDR_DAG_PATH", "dags/local")]), &[]).unwrap();
    let here = std::env::current_dir().unwrap();
    assert_eq!(config.dag_roots[0].path, here.join("dags/local"));
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
             drain_terminate_period = \"6s\"\n",
        )),
        &env(&[("SCHEDR_HIDDEN__WORKER__DRAIN_REPORT_PERIOD", "4s")]),
        &[],
    )
    .unwrap();
    assert_eq!(fits.worker.drain_terminate_period, Duration::from_secs(6));
    assert_eq!(fits.timings.drain_report_period, Duration::from_secs(4));
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

#[test]
fn secret_storages_are_tables_by_name() {
    let toml = r#"
[secret_storage]
default = "main"
min_mask_length = 8

[secret_storage.main]
type = "db"
cache_ttl = "5s"
unmasked_prefixes = ["variables/"]

[secret_storage.other-db]
type = "db"
"#;
    let config = load(
        Some(file(toml)),
        &env(&[("SCHEDR_CONFIG__SECRET_STORAGE__MAIN__CACHE_TTL", "0s")]),
        &[],
    )
    .unwrap();
    let section = &config.secret_storage;
    assert_eq!(section.default.as_deref(), Some("main"));
    assert_eq!(section.min_mask_length, 8);
    assert_eq!(section.storages["main"].cache_ttl, Duration::ZERO);
    assert_eq!(section.storages["main"].unmasked_prefixes, ["variables/"]);
    assert_eq!(
        section.storages["other-db"].cache_ttl,
        Duration::from_secs(60)
    );
    let defaults = load(None, &env(&[]), &[]).unwrap().secret_storage;
    assert_eq!((defaults.default, defaults.min_mask_length), (None, 5));
    // `db` is standalone's own: a default without configuring it.
    let db = load(
        Some(file("[secret_storage]\ndefault = \"db\"\n")),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(db.secret_storage.default.as_deref(), Some("db"));
}

#[test]
fn a_vault_storage_takes_its_token_from_the_environment() {
    let toml = r#"
[secret_storage.vault]
type = "vault"
address = "https://vault.example.com:8200"
mount = "team/kv"
namespace = "team-x"
auth = { method = "token" }

[secret_storage.k8s]
type = "vault"
address = "https://vault.example.com:8200"
auth = { method = "kubernetes", role = "schedr-worker", mount = "k8s-prod" }

[secret_storage]
default = "vault"
"#;
    let config = load(
        Some(file(toml)),
        &env(&[("SCHEDR_CONFIG__SECRET_STORAGE__VAULT__TOKEN", "hvs.dev")]),
        &[],
    )
    .unwrap();
    let vault = &config.secret_storage.storages["vault"];
    assert_eq!(vault.token.as_deref(), Some("hvs.dev"));
    // Never printed, not even for debugging (the whole config's `Debug` holds it).
    assert!(!format!("{config:?}").contains("hvs.dev"), "{vault:?}");
    assert!(
        format!("{vault:?}").contains("token: Some(\"****\")"),
        "{vault:?}"
    );
    assert_eq!(vault.mount.as_deref(), Some("team/kv"));
    let k8s = config.secret_storage.storages["k8s"].auth.clone().unwrap();
    assert_eq!(k8s.role.as_deref(), Some("schedr-worker"));
    assert_eq!(k8s.mount.as_deref(), Some("k8s-prod"));
}

#[rstest::rstest]
#[case(
    "[secret_storage]\ndefault = \"vault\"\n",
    "default `vault` isn't a configured storage (configured: none"
)]
#[case("[secret_storage.\"a b\"]\ntype = \"db\"\n", "storage name `a b`")]
#[case(
    "[secret_storage.my__vault]\ntype = \"db\"\n",
    "storage name `my__vault`"
)]
#[case(
    "[secret_storage.default]\ntype = \"db\"\n",
    "expected a string for key \"default.secret_storage.default\""
)]
#[case(
    "[secret_storage.x]\ntype = \"file\"\n",
    "unknown variant: found `file`, expected ``db` or `vault``"
)]
#[case(
    "[secret_storage.x]\ntype = \"db\"\naddress = \"h\"\n",
    "[secret_storage.x] address: only a `type = \"vault\"` storage has it"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\nauth = { method = \"kubernetes\", role = \"r\" }\n",
    "[secret_storage.v] needs `address`"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\naddress = \"vault:8200\"\nauth = { method = \"kubernetes\", role = \"r\" }\n",
    "address `vault:8200`: an http:// or https:// URL"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\naddress = \"https://v\"\n",
    "[secret_storage.v] needs `auth`"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\naddress = \"https://v\"\nauth = { method = \"kubernetes\" }\n",
    "auth method `kubernetes` needs `role`"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\naddress = \"https://v\"\nauth = { method = \"token\" }\n",
    "auth method `token` needs the token in SCHEDR_CONFIG__SECRET_STORAGE__V__TOKEN"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\naddress = \"https://v\"\nmount = \"a//b\"\nauth = { method = \"kubernetes\", role = \"r\" }\n",
    "mount `a//b`"
)]
#[case(
    "[secret_storage.v]\ntype = \"vault\"\naddress = \"https://v\"\ntoken = \"hvs.x\"\nauth = { method = \"token\" }\n",
    "a token isn't written in the file; set SCHEDR_CONFIG__SECRET_STORAGE__V__TOKEN"
)]
#[case(
    "[secret_storage.x]\ntype = \"db\"\nunmasked_prefixes = [\"etl\"]\n",
    "unmasked_prefixes: `etl` isn't a path prefix ending with `/`"
)]
#[case(
    "[secret_storage.x]\ntype = \"db\"\nunmasked_prefixes = [\"\"]\n",
    "unmasked_prefixes: `` isn't a path prefix"
)]
#[case(
    "[secret_storage.a]\ntype = \"db\"\n[secret_storage.b]\ntype = \"db\"\n",
    "several storages (a, b) and no `default`"
)]
fn a_bad_secret_storage_is_refused(#[case] toml: &str, #[case] message: &str) {
    let error = load(Some(file(toml)), &env(&[]), &[])
        .unwrap_err()
        .to_string();
    assert!(error.contains(message), "{error}");
}

#[test]
fn a_hidden_variable_sets_its_timing_and_is_remembered() {
    let vars = env(&[
        ("SCHEDR_HIDDEN__MASTER__TICK_INTERVAL", "100ms"),
        ("SCHEDR_HIDDEN__DATABASE__QUERY_TIMEOUT", "2s"),
        ("SCHEDR_HIDDEN__TLS__HANDSHAKE_TIMEOUT", "1m"),
    ]);
    let timings = load(None, &vars, &[]).unwrap().timings;
    assert_eq!(timings.tick_interval, Duration::from_millis(100));
    assert_eq!(timings.query_timeout, Duration::from_secs(2));
    assert_eq!(timings.handshake_timeout, Duration::from_mins(1));
    assert_eq!(
        timings.overridden,
        [
            "SCHEDR_HIDDEN__DATABASE__QUERY_TIMEOUT",
            "SCHEDR_HIDDEN__MASTER__TICK_INTERVAL",
            "SCHEDR_HIDDEN__TLS__HANDSHAKE_TIMEOUT",
        ]
    );
}

#[test]
fn without_a_hidden_variable_the_timings_are_the_fixed_ones() {
    assert_eq!(
        load(None, &env(&[]), &[]).unwrap().timings,
        Timings::default()
    );
}

#[rstest]
#[case("SCHEDR_HIDDEN__MASTER__TICK", "1s", "no such timing")]
#[case("SCHEDR_HIDDEN__MASTER__CANCEL_GRACE", "1s", "no such timing")]
#[case(
    "SCHEDR_HIDDEN__MASTER__TICK_INTERVAL",
    "0s",
    "must be longer than zero"
)]
#[case(
    "SCHEDR_HIDDEN__WORKER__MEMORY_CHECK_INTERVAL",
    "0ms",
    "must be longer than zero"
)]
fn a_hidden_variable_that_cant_be_used_is_refused(
    #[case] name: &str,
    #[case] value: &str,
    #[case] reason: &str,
) {
    assert_eq!(
        load(None, &env(&[(name, value)]), &[]).unwrap_err(),
        ConfigError::BadHiddenTiming {
            name: name.to_owned(),
            reason: reason.to_owned(),
        }
    );
}

#[test]
fn a_hidden_variable_that_isnt_a_duration_is_refused() {
    let name = "SCHEDR_HIDDEN__MASTER__TICK_INTERVAL";
    let error = load(None, &env(&[(name, "soon")]), &[]).unwrap_err();
    assert!(
        matches!(&error, ConfigError::BadHiddenTiming { name: said, .. } if said == name),
        "{error:?}"
    );
}

#[rstest]
#[case("[master]\ntick_interval = \"100ms\"\n", "tick_interval")]
#[case("[master]\nread_back_window = \"1s\"\n", "read_back_window")]
#[case("[database]\nquery_timeout = \"1s\"\n", "query_timeout")]
#[case("[ops]\nloop_stall_timeout = \"1m\"\n", "loop_stall_timeout")]
#[case("[tls]\nhandshake_timeout = \"1s\"\n", "handshake_timeout")]
fn a_file_cant_set_a_fixed_timing(#[case] contents: &str, #[case] key: &str) {
    let error = load(Some(file(contents)), &env(&[]), &[]).unwrap_err();
    assert!(error.to_string().contains(key), "{error}");
}

#[test]
fn the_normal_variables_cant_set_a_fixed_timing() {
    let vars = env(&[("SCHEDR_CONFIG__MASTER__TICK_INTERVAL", "100ms")]);
    let error = load(None, &vars, &[]).unwrap_err();
    assert!(error.to_string().contains("tick_interval"), "{error}");
}

#[test]
fn the_new_timeouts_are_read_from_a_file() {
    let config = load(
        Some(file(
            "[master]\nparse_job_timeout = \"1m\"\n\
             [webserver]\nworker_request_timeout = \"2m\"\nconnection_test_timeout = \"3m\"\n\
             [worker]\nrunner_start_timeout = \"4m\"\nfork_server_preload_timeout = \"5m\"\n\
             cleanup_timeout = \"6m\"\nresource_cleanup_timeout = \"7m\"\n",
        )),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(config.master.parse_job_timeout, Duration::from_mins(1));
    assert_eq!(
        config.webserver.worker_request_timeout,
        Duration::from_mins(2)
    );
    assert_eq!(
        config.webserver.connection_test_timeout,
        Duration::from_mins(3)
    );
    assert_eq!(config.worker.runner_start_timeout, Duration::from_mins(4));
    assert_eq!(
        config.worker.fork_server_preload_timeout,
        Duration::from_mins(5)
    );
    assert_eq!(config.worker.cleanup_timeout, Duration::from_mins(6));
    assert_eq!(
        config.worker.resource_cleanup_timeout,
        Duration::from_mins(7)
    );
}

#[test]
fn no_files_parsed_at_once_is_refused() {
    assert_eq!(
        load(
            Some(file("[dags]\nparse_concurrency = 0\n")),
            &env(&[]),
            &[]
        )
        .unwrap_err(),
        ConfigError::NoParseConcurrency
    );
}

#[rstest]
#[case("[dags]\npoll_interval = \"0s\"\n", "[dags] poll_interval")]
#[case("[dags]\nparse_file_timeout = \"0s\"\n", "[dags] parse_file_timeout")]
#[case("[master]\nparse_job_timeout = \"0s\"\n", "[master] parse_job_timeout")]
#[case(
    "[webserver]\nworker_request_timeout = \"0s\"\n",
    "[webserver] worker_request_timeout"
)]
#[case("[worker]\nlog_retention = \"0s\"\n", "[worker] log_retention")]
#[case(
    "[worker]\nrunner_start_timeout = \"0s\"\n",
    "[worker] runner_start_timeout"
)]
#[case(
    "[worker]\nfork_server_preload_timeout = \"0s\"\n",
    "[worker] fork_server_preload_timeout"
)]
#[case("[worker]\ncleanup_timeout = \"0s\"\n", "[worker] cleanup_timeout")]
#[case(
    "[worker]\nresource_cleanup_timeout = \"0s\"\n",
    "[worker] resource_cleanup_timeout"
)]
fn a_timing_that_cant_work_at_zero_is_refused(#[case] contents: &str, #[case] key: &str) {
    assert_eq!(
        load(Some(file(contents)), &env(&[]), &[]).unwrap_err(),
        ConfigError::ZeroTiming {
            key: key.to_owned()
        }
    );
}

#[test]
fn a_zero_stays_allowed_where_it_means_something() {
    let config = load(
        Some(file(
            "[dags]\nsettle_time = \"0s\"\n[master]\ncancel_grace = \"0s\"\ncode_wait = \"0s\"\n\
             [worker]\ndrain_terminate_period = \"0s\"\nfork_server_import_retry = \"0s\"\n",
        )),
        &env(&[]),
        &[],
    )
    .unwrap();
    assert_eq!(config.master.cancel_grace, Duration::ZERO);
}

#[test]
fn the_guide_names_every_fixed_timing_as_the_loader_knows_it() {
    let page = include_str!("../../../../docs/03-guide/14-timings.md");
    let names: Vec<&str> = page
        .split('`')
        .filter(|part| part.starts_with(HIDDEN_PREFIX) && !part.ends_with('*'))
        .collect();
    assert_eq!(names.len(), 67, "{names:?}");
    for name in names {
        let loaded = load(None, &env(&[(name, "1s")]), &[]);
        if HIDDEN_LIMITS.iter().any(|limit| name.ends_with(limit)) {
            let limits = load(None, &env(&[(name, "1")]), &[]).unwrap().limits;
            assert_eq!(limits.overridden, [name]);
            assert!(loaded.is_err(), "a limit is a number, not a duration");
        } else {
            assert_eq!(loaded.unwrap().timings.overridden, [name]);
        }
    }
}

#[rstest]
#[case("0600", Some(0o600))]
#[case("0640", Some(0o640))]
#[case("644", Some(0o644))]
#[case("0o640", Some(0o640))]
#[case("0400", None)]
#[case("0660", None)]
#[case("0700", None)]
#[case("0606", None)]
#[case("rw", None)]
#[case("", None)]
#[case("00600", None)]
fn a_log_mode_lets_the_owner_read_and_write_and_others_at_most_read(
    #[case] text: &str,
    #[case] mode: Option<u32>,
) {
    assert_eq!(crate::config::log_mode(text), mode);
}

#[test]
fn a_log_mode_logs_cant_have_is_refused() {
    assert_eq!(
        load(
            Some(file("[worker]\nlog_mode = \"0666\"\n")),
            &env(&[]),
            &[]
        )
        .unwrap_err(),
        ConfigError::LogMode {
            mode: "0666".to_owned()
        }
    );
    let config = load(None, &env(&[]), &[]).unwrap();
    assert_eq!(config.worker.log_mode, "0600");
}

#[test]
fn hidden_limits_are_whole_numbers_above_zero() {
    let loaded = load(
        None,
        &env(&[
            ("SCHEDR_HIDDEN__MASTER__WORKER_REPORT_RECORDS", "8"),
            ("SCHEDR_HIDDEN__MASTER__WORKER_REPORT_TEXT", "64"),
        ]),
        &[],
    )
    .unwrap();
    assert_eq!(
        (
            loaded.limits.worker_report_records,
            loaded.limits.worker_report_text
        ),
        (8, 64)
    );
    assert_eq!(loaded.limits.overridden.len(), 2);
    assert_eq!(loaded.timings.overridden, Vec::<String>::new());
    let name = "SCHEDR_HIDDEN__MASTER__WORKER_REPORT_TEXT";
    for bad in ["0", "-1", "many"] {
        assert_eq!(
            load(None, &env(&[(name, bad)]), &[]).unwrap_err(),
            ConfigError::BadHiddenTiming {
                name: name.to_owned(),
                reason: format!("must be a whole number above zero, got `{bad}`"),
            }
        );
    }
    let defaults = load(None, &env(&[]), &[]).unwrap().limits;
    assert_eq!(
        (defaults.worker_report_records, defaults.worker_report_text),
        (256, 4096)
    );
}

#[rstest]
#[case("0600", "0600")]
#[case("0640", "0640")]
#[case("644", "0644")]
#[case("0o640", "0o640")]
fn a_log_mode_from_the_environment_is_read_as_octal_whatever_it_looks_like(
    #[case] given: &str,
    #[case] kept: &str,
) {
    // The environment gives `0640` as the number 640: its digits are the mode's.
    let loaded = load(
        None,
        &env(&[("SCHEDR_CONFIG__WORKER__LOG_MODE", given)]),
        &[],
    )
    .unwrap();
    assert_eq!(loaded.worker.log_mode, kept);
    assert!(crate::config::log_mode(&loaded.worker.log_mode).is_some());
}

#[test]
fn a_log_mode_number_that_isnt_a_mode_is_refused() {
    let error = load(
        None,
        &env(&[("SCHEDR_CONFIG__WORKER__LOG_MODE", "666")]),
        &[],
    )
    .unwrap_err();
    assert_eq!(
        error,
        ConfigError::LogMode {
            mode: "0666".to_owned()
        }
    );
}
