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
#[case("[database]\nurl = \"x\"\n", "unknown field")]
#[case(
    "[interpreters.venv]\npath = \"/p\"\nthreads = 2\n",
    "data did not match"
)]
#[case("[log]\nformat = \"xml\"\n", "unknown variant")]
#[case("[dags]\nsettle_time = \"soon\"\n", "is not a duration")]
#[case("[ops]\nport = 70000\n", "port")]
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
