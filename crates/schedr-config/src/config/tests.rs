use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use pretty_assertions::assert_eq;
use rstest::rstest;

use super::*;

fn parse(toml: &str) -> Result<Config, Box<figment::Error>> {
    Figment::from(Toml::string(toml))
        .extract()
        .map_err(Box::new)
}

#[test]
fn defaults_match_the_documented_values() {
    let config = Config::default();
    assert_eq!(config.dags.path, PathList::default());
    assert_eq!(config.dags.interpreter, "python");
    assert_eq!(config.dags.poll_interval, Duration::from_secs(30));
    assert_eq!(config.dags.settle_time, Duration::from_secs(5));
    assert_eq!(config.scripts, ScriptsSection::default());
    assert_eq!(config.interpreters, BTreeMap::new());
    assert_eq!(
        config.log,
        LogSection {
            format: LogFormat::Auto,
            level: "info".into()
        }
    );
    assert_eq!(
        config.ops,
        OpsSection {
            bind: std::net::IpAddr::from([0, 0, 0, 0]),
            port: 8090,
            loop_stall_timeout: Duration::from_secs(120)
        }
    );
}

#[test]
fn an_empty_file_gives_the_defaults() {
    assert_eq!(parse("").unwrap(), Config::default());
}

#[test]
fn reads_every_section() {
    let config = parse(
        r#"
        [dags]
        path = ["/git/dags/etl-repo"]
        interpreter = "etl-venv"
        poll_interval = "1m"
        settle_time = "10s"

        [dags.roots]
        etl = "/git/dags/etl-repo"

        [scripts]
        path = "/git/scripts/a:/git/scripts/b"

        [interpreters]
        python = "/usr/bin/python3"

        [interpreters.etl-venv]
        path = "/opt/venvs/etl/bin/python"
        fork_server = true
        preload = ["pandas"]

        [log]
        format = "json"
        level = "info,schedr::master=debug"

        [ops]
        port = 9100
        loop_stall_timeout = "30s"
        "#,
    )
    .unwrap();
    assert_eq!(
        config.dags.path,
        PathList(vec!["/git/dags/etl-repo".into()])
    );
    assert_eq!(
        config.dags.roots["etl"],
        PathBuf::from("/git/dags/etl-repo")
    );
    assert_eq!(config.dags.interpreter, "etl-venv");
    assert_eq!(config.dags.poll_interval, Duration::from_secs(60));
    assert_eq!(config.dags.settle_time, Duration::from_secs(10));
    assert_eq!(
        config.scripts.path,
        PathList(vec!["/git/scripts/a".into(), "/git/scripts/b".into()])
    );
    assert_eq!(
        config.interpreters["python"],
        Interpreter::path("/usr/bin/python3")
    );
    assert_eq!(
        config.interpreters["etl-venv"],
        Interpreter {
            path: "/opt/venvs/etl/bin/python".into(),
            fork_server: true,
            preload: vec!["pandas".into()],
        }
    );
    assert_eq!(config.log.format, LogFormat::Json);
    assert_eq!(config.log.level, "info,schedr::master=debug");
    assert_eq!(config.ops.port, 9100);
    assert_eq!(config.ops.loop_stall_timeout, Duration::from_secs(30));
}

#[test]
fn interpreter_table_defaults_to_spawn_without_preloading() {
    let config = parse("[interpreters.venv]\npath = \"/v/bin/python\"\n").unwrap();
    assert_eq!(
        config.interpreters["venv"],
        Interpreter::path("/v/bin/python")
    );
}

#[rstest]
#[case("[dags]\npoll = \"30s\"\n", "unknown field")]
#[case("[database]\nhost = \"x\"\n", "unknown field")]
#[case(
    "[interpreters.venv]\npath = \"/p\"\nthreads = 2\n",
    "data did not match"
)]
#[case("[log]\nformat = \"xml\"\n", "unknown variant")]
#[case("[dags]\nsettle_time = \"soon\"\n", "is not a duration")]
#[case("[ops]\nport = 70000\n", "port")]
#[case("[tls]\nmode = \"off\"\n", "unknown variant")]
#[case("[worker]\nlog_max_size = \"10GB\"\n", "is not a size")]
#[case("[worker]\nlog_max_size = 10\n", "a size string")]
fn rejects_invalid_values(#[case] toml: &str, #[case] expected: &str) {
    let err = parse(toml).unwrap_err().to_string();
    assert!(err.contains(expected), "{err}");
}

#[test]
fn interpreter_path_builder() {
    let interpreter = Interpreter::path("/bin/bash");
    assert_eq!(interpreter.path, PathBuf::from("/bin/bash"));
    assert!(!interpreter.fork_server);
    assert_eq!(interpreter.preload, Vec::<String>::new());
}

#[test]
fn reads_the_database_url() {
    let config = parse("[database]\nurl = \"sqlite:///var/lib/schedr/schedr.db\"\n").unwrap();
    assert_eq!(
        config.database.url.as_deref(),
        Some("sqlite:///var/lib/schedr/schedr.db")
    );
    assert_eq!(
        Config::default().database,
        DatabaseSection {
            url: None,
            query_timeout: Duration::from_secs(30)
        }
    );
    let config =
        parse("[database]\nquery_timeout = \"10s\"\n[ops]\nbind = \"127.0.0.1\"\n").unwrap();
    assert_eq!(config.database.query_timeout, Duration::from_secs(10));
    assert_eq!(config.ops.bind, std::net::IpAddr::from([127, 0, 0, 1]));
}

#[rstest]
#[case("postgres://schedr:secret@db/schedr", true)]
#[case("postgres://schedr:p@ss@db/schedr", true)]
#[case("postgres://schedr@db/schedr", false)]
#[case("postgres://db/schedr", false)]
#[case("postgres://db/path:with@colon", false)]
#[case("sqlite::memory:", false)]
#[case("sqlite:///x.db", false)]
fn url_has_password_looks_at_the_user_info_only(#[case] url: &str, #[case] expected: bool) {
    assert_eq!(url_has_password(url), expected);
}

#[test]
fn master_webserver_and_worker_defaults() {
    let config = Config::default();
    assert_eq!(config.dags.parse_file_timeout, Duration::from_secs(60));
    assert_eq!(
        config.master,
        MasterSection {
            bind: std::net::IpAddr::from([0, 0, 0, 0]),
            grpc_port: 8070,
            tick_interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(10),
            lease_timeout: Duration::from_secs(60),
            cancel_grace: Duration::from_secs(30),
            code_wait: Duration::from_mins(10),
        }
    );
    assert_eq!(config.webserver.port, 8080);
    assert_eq!(config.webserver.auth.identity_header, None);
    assert_eq!(
        config.worker,
        WorkerSection {
            id: None,
            slots: 2,
            queues: vec![],
            data_dir: None,
            pass_env: vec![],
            master_url: None,
            termination_grace_period: Duration::from_secs(30),
            log_bind: std::net::IpAddr::from([0, 0, 0, 0]),
            log_port: 8081,
            log_address: None,
            log_retention: Duration::from_hours(720),
            log_max_size: None,
            memory_budget: None,
            memory_check_interval: Duration::from_secs(1),
        }
    );
    assert_eq!(
        config.tls,
        TlsSection {
            mode: TlsMode::App,
            ca: None,
            cert: None,
            key: None,
            worker_name: "schedr-worker".into(),
            reload_interval: Duration::from_secs(30),
        }
    );
}

#[test]
fn reads_the_master_webserver_and_worker_sections() {
    let config = parse(
        r#"
        [dags]
        parse_file_timeout = "2m"

        [master]
        grpc_port = 9070
        tick_interval = "500ms"
        heartbeat_interval = "5s"
        lease_timeout = "30s"
        cancel_grace = "10s"
        code_wait = "3m"

        [webserver]
        port = 9080

        [webserver.auth]
        identity_header = "x-forwarded-user"

        [worker]
        id = "worker-1"
        slots = 4
        queues = ["default", "large"]
        data_dir = "/var/lib/schedr"
        pass_env = ["HTTPS_PROXY"]
        termination_grace_period = "10m"
        log_bind = "127.0.0.1"
        log_port = 9081
        log_address = "w-0.schedr-worker.etl.svc.cluster.local"
        log_retention = "7d"
        log_max_size = "10GiB"
        memory_budget = "2GiB"
        memory_check_interval = "500ms"
        "#,
    )
    .unwrap();
    assert_eq!(config.dags.parse_file_timeout, Duration::from_secs(120));
    assert_eq!(config.master.grpc_port, 9070);
    assert_eq!(config.master.tick_interval, Duration::from_millis(500));
    assert_eq!(config.master.heartbeat_interval, Duration::from_secs(5));
    assert_eq!(config.master.lease_timeout, Duration::from_secs(30));
    assert_eq!(config.master.cancel_grace, Duration::from_secs(10));
    assert_eq!(config.master.code_wait, Duration::from_mins(3));
    assert_eq!(
        config.worker.termination_grace_period,
        Duration::from_mins(10)
    );
    assert_eq!(config.webserver.port, 9080);
    assert_eq!(
        config.webserver.auth.identity_header.as_deref(),
        Some("x-forwarded-user")
    );
    assert_eq!(config.worker.id.as_deref(), Some("worker-1"));
    assert_eq!(config.worker.slots, 4);
    assert_eq!(config.worker.queues, ["default", "large"]);
    assert_eq!(
        config.worker.data_dir,
        Some(PathBuf::from("/var/lib/schedr"))
    );
    assert_eq!(config.worker.pass_env, ["HTTPS_PROXY"]);
    assert_eq!(
        config.worker.log_bind,
        std::net::IpAddr::from([127, 0, 0, 1])
    );
    assert_eq!(config.worker.log_port, 9081);
    assert_eq!(
        config.worker.log_address.as_deref(),
        Some("w-0.schedr-worker.etl.svc.cluster.local")
    );
    assert_eq!(config.worker.log_retention, Duration::from_hours(168));
    assert_eq!(config.worker.log_max_size, Some(10 << 30));
    assert_eq!(config.worker.memory_budget, Some(2 << 30));
    assert_eq!(
        config.worker.memory_check_interval,
        Duration::from_millis(500)
    );
}

#[test]
fn reads_the_tls_section() {
    let config = parse(
        r#"
        [tls]
        mode = "mesh"
        ca = "/etc/schedr/tls/ca.crt"
        cert = "/etc/schedr/tls/tls.crt"
        key = "/etc/schedr/tls/tls.key"
        worker_name = "worker.schedr"
        reload_interval = "1m"
        "#,
    )
    .unwrap();
    assert_eq!(
        config.tls,
        TlsSection {
            mode: TlsMode::Mesh,
            ca: Some("/etc/schedr/tls/ca.crt".into()),
            cert: Some("/etc/schedr/tls/tls.crt".into()),
            key: Some("/etc/schedr/tls/tls.key".into()),
            worker_name: "worker.schedr".into(),
            reload_interval: Duration::from_mins(1),
        }
    );
}

#[test]
fn the_defaults_serialize_to_what_reads_back_as_the_defaults() {
    let written = serde_json::to_value(Config::default()).unwrap();
    assert_eq!(written["master"]["tick_interval"], "1s");
    assert_eq!(written["worker"]["log_retention"], "30d");
    // Unset options are left out, as in a file that doesn't set them.
    assert_eq!(written["database"].get("url"), None);
    let read: Config = serde_json::from_value(written).unwrap();
    assert_eq!(read, Config::default());
}

#[test]
fn a_full_configuration_reads_back_as_itself() {
    let config = parse(
        r#"
        [interpreters]
        python = "/usr/bin/python3"
        etl = { path = "/opt/etl/bin/python", fork_server = true, preload = ["pandas"] }
        [worker]
        id = "w1"
        memory_budget = "512MiB"
        log_max_size = "10GiB"
        [tls]
        mode = "mesh"
        "#,
    )
    .unwrap();
    let written = serde_json::to_value(&config).unwrap();
    assert_eq!(written["interpreters"]["python"], "/usr/bin/python3");
    assert_eq!(written["interpreters"]["etl"]["fork_server"], true);
    assert_eq!(written["worker"]["memory_budget"], "512MiB");
    let read: Config = serde_json::from_value(written).unwrap();
    assert_eq!(read, config);
}

#[test]
fn the_schema_documents_every_key_with_its_default() {
    let schema = serde_json::to_value(config_schema()).unwrap();
    let ops = &schema["$defs"]["OpsSection"]["properties"];
    assert_eq!(ops["port"]["default"], 8090);
    assert_eq!(ops["loop_stall_timeout"]["default"], "2m");
    assert_eq!(ops["loop_stall_timeout"]["$ref"], "#/$defs/Duration");
    assert!(
        ops["bind"]["description"]
            .as_str()
            .unwrap()
            .starts_with("The address the ops port listens on"),
        "{ops}"
    );
    // Fields filled in when loading aren't keys.
    assert_eq!(schema["properties"].get("dag_roots"), None);
    assert_eq!(
        schema["$defs"]["PathList"]["anyOf"][1]["type"],
        serde_json::json!("string")
    );
}
