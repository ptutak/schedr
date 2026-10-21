use std::collections::BTreeMap;
use std::path::Path;

use figment::Figment;
use pretty_assertions::assert_eq;
use rstest::rstest;
use serde::Deserialize;

use super::*;

fn env(vars: &[(&str, &str)]) -> BTreeMap<String, String> {
    vars.iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[derive(Debug, Deserialize, PartialEq)]
struct Ops {
    port: u16,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Dags {
    path: String,
    poll_interval: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Sample {
    ops: Option<Ops>,
    dags: Option<Dags>,
}

#[test]
fn config_file_prefers_the_flag_then_the_variable_then_the_default() {
    let vars = env(&[("SCHEDR_CONFIG_FILE", "/from/env.toml")]);
    assert_eq!(
        config_file(Some(Path::new("/from/flag.toml")), &vars),
        ConfigFile::Explicit("/from/flag.toml".into())
    );
    assert_eq!(
        config_file(None, &vars),
        ConfigFile::Explicit("/from/env.toml".into())
    );
    assert_eq!(
        config_file(None, &env(&[])),
        ConfigFile::Default(DEFAULT_CONFIG_FILE.into())
    );
    assert_eq!(
        config_file(None, &env(&[("SCHEDR_CONFIG_FILE", "")])),
        ConfigFile::Default(DEFAULT_CONFIG_FILE.into())
    );
}

#[test]
fn config_file_path_and_requirement() {
    let explicit = ConfigFile::Explicit("/a.toml".into());
    let default = ConfigFile::Default("/b.toml".into());
    assert_eq!(explicit.path(), Path::new("/a.toml"));
    assert_eq!(default.path(), Path::new("/b.toml"));
    assert!(explicit.is_required());
    assert!(!default.is_required());
}

#[rstest]
#[case("SCHEDR_CONFIG__OPS__PORT", Some("ops.port"))]
#[case("SCHEDR_CONFIG__DAGS__POLL_INTERVAL", Some("dags.poll_interval"))]
#[case(
    "SCHEDR_CONFIG__SECRET_STORAGE__VAULT__TOKEN",
    Some("secret_storage.vault.token")
)]
#[case("SCHEDR_CONFIG_FILE", None)]
#[case("HOME", None)]
fn env_key_translates_prefixed_variables(#[case] var: &str, #[case] expected: Option<&str>) {
    assert_eq!(env_key(var), Ok(expected.map(str::to_owned)));
}

#[rstest]
#[case("SCHEDR_CONFIG__")]
#[case("SCHEDR_CONFIG__DAGS____PATH")]
#[case("SCHEDR_CONFIG__DAGS__")]
fn env_key_rejects_empty_parts(#[case] var: &str) {
    assert_eq!(
        env_key(var),
        Err(SourceError::InvalidEnvKey(var.to_owned()))
    );
}

#[test]
fn env_provider_nests_values_and_parses_numbers() {
    let provider = env_provider(&env(&[
        ("SCHEDR_CONFIG__OPS__PORT", "9100"),
        ("SCHEDR_CONFIG__DAGS__PATH", "/a:/b"),
        ("SCHEDR_CONFIG__DAGS__POLL_INTERVAL", "1m"),
        ("HOME", "/root"),
    ]))
    .unwrap();
    let sample: Sample = Figment::from(provider).extract().unwrap();
    assert_eq!(
        sample,
        Sample {
            ops: Some(Ops { port: 9100 }),
            dags: Some(Dags {
                path: "/a:/b".into(),
                poll_interval: Some("1m".into())
            }),
        }
    );
}

#[test]
fn env_provider_maps_aliases_to_their_keys() {
    let provider = env_provider(&env(&[("SCHEDR_DAG_PATH", "/git/dags/a")])).unwrap();
    let sample: Sample = Figment::from(provider).extract().unwrap();
    assert_eq!(sample.dags.unwrap().path, "/git/dags/a");
}

#[test]
fn env_provider_rejects_an_alias_together_with_its_full_variable() {
    let err = env_provider(&env(&[
        ("SCHEDR_DAG_PATH", "/a"),
        ("SCHEDR_CONFIG__DAGS__PATH", "/b"),
    ]))
    .unwrap_err();
    assert_eq!(
        err,
        SourceError::AliasConflict {
            alias: "SCHEDR_DAG_PATH",
            var: "SCHEDR_CONFIG__DAGS__PATH".into(),
            key: "dags.path",
        }
    );
    assert_eq!(
        err.to_string(),
        "`SCHEDR_DAG_PATH` and `SCHEDR_CONFIG__DAGS__PATH` set the same key `dags.path`; set only one"
    );
}

#[test]
fn env_provider_rejects_malformed_variables() {
    assert_eq!(
        env_provider(&env(&[("SCHEDR_CONFIG__X____Y", "1")])).unwrap_err(),
        SourceError::InvalidEnvKey("SCHEDR_CONFIG__X____Y".into())
    );
}

#[test]
fn errors_name_the_variable_a_bad_value_came_from() {
    let provider = env_provider(&env(&[("SCHEDR_CONFIG__OPS__PORT", "high")])).unwrap();
    let err = Figment::from(provider)
        .extract::<Sample>()
        .unwrap_err()
        .to_string();
    assert!(err.contains("SCHEDR_CONFIG__OPS__PORT"), "{err}");
    assert!(err.contains("environment variable"), "{err}");
}

#[test]
fn errors_name_the_alias_a_bad_value_came_from() {
    let provider = env_provider(&env(&[("SCHEDR_DAG_PATH", "5")])).unwrap();
    let err = Figment::from(provider)
        .extract::<Sample>()
        .unwrap_err()
        .to_string();
    assert!(err.contains("SCHEDR_DAG_PATH"), "{err}");
}

#[test]
fn override_provider_names_the_flag() {
    let provider = override_provider(&[Override {
        key: "ops.port".into(),
        value: "x".into(),
        flag: "--ops-port".into(),
    }]);
    let err = Figment::from(provider)
        .extract::<Sample>()
        .unwrap_err()
        .to_string();
    assert!(err.contains("--ops-port"), "{err}");
    assert!(err.contains("command-line flag"), "{err}");
}

#[test]
fn later_entries_merge_into_earlier_ones() {
    let provider = override_provider(&[
        Override {
            key: "dags.path".into(),
            value: "/a".into(),
            flag: "--dag-path".into(),
        },
        Override {
            key: "dags.poll_interval".into(),
            value: "2m".into(),
            flag: "--poll".into(),
        },
        Override {
            key: "dags.path".into(),
            value: "/b".into(),
            flag: "--dag-path".into(),
        },
    ]);
    let sample: Sample = Figment::from(provider).extract().unwrap();
    assert_eq!(
        sample.dags,
        Some(Dags {
            path: "/b".into(),
            poll_interval: Some("2m".into())
        })
    );
}

#[test]
fn file_metadata_names_the_file() {
    let metadata = file_metadata(Path::new("/etc/schedr/config.toml"));
    assert_eq!(metadata.name, "configuration file");
    assert_eq!(
        metadata.source.as_ref().and_then(|s| s.file_path()),
        Some(Path::new("/etc/schedr/config.toml"))
    );
}

#[test]
fn source_error_messages() {
    assert_eq!(
        SourceError::InvalidEnvKey("SCHEDR_CONFIG__".into()).to_string(),
        "environment variable `SCHEDR_CONFIG__` is not a configuration key (empty part between `__`)"
    );
}
