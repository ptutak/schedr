use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_model::commands::{ConnectionReport, SecretReport, SecretState};
use schedr_model::connection::Registry;
use schedr_model::secret::SecretName;
use schedr_proto::pb;
use serde_json::json;

use super::*;
use crate::secrets::{MemoryStorage, ReadFuture, SecretError, SecretStorage, SecretStores};

fn stores() -> Arc<SecretStores> {
    let name = |path: &str, key: &str| SecretName {
        storage: None,
        path: path.to_owned(),
        key: Some(key.to_owned()),
        default: None,
    };
    Arc::new(SecretStores::for_test(MemoryStorage::new([
        (name("etl/db", "password"), "hunter22".to_owned()),
        (name("etl/db", "user"), "etl".to_owned()),
    ])))
}

fn secret(path: &str, key: &str) -> serde_json::Value {
    json!({"ref": {"kind": "secret", "path": path, "key": key}})
}

/// `db` with its secrets there, `api` missing one, `opt` falling back to its default, and a
/// refused `twice`.
fn registry() -> Arc<Registry> {
    let def = |name: &str, params: serde_json::Value| {
        serde_json::from_value(json!({"name": name, "params": params})).unwrap()
    };
    let with_default = json!({"ref": {"kind": "secret", "path": "etl/opt", "key": "k",
                                      "default": "none"}});
    Arc::new(Registry {
        connections: BTreeMap::from([
            (
                "db".to_owned(),
                def(
                    "db",
                    json!({"user": secret("etl/db", "user"),
                           "password": secret("etl/db", "password")}),
                ),
            ),
            (
                "api".to_owned(),
                def(
                    "api",
                    json!({"token": secret("etl/api", "token"),
                           "user": secret("etl/db", "user")}),
                ),
            ),
            ("opt".to_owned(), def("opt", json!({"k": with_default}))),
        ]),
        refused: BTreeMap::from([("twice".to_owned(), "defined in a.py and b.py".to_owned())]),
        files: BTreeMap::from([("db".to_owned(), "conns.py".to_owned())]),
    })
}

fn report(
    name: &str,
    ok: bool,
    secrets: &[(&str, SecretState)],
    error: Option<&str>,
) -> ConnectionReport {
    ConnectionReport {
        name: name.to_owned(),
        ok,
        secrets: secrets
            .iter()
            .map(|(secret, state)| SecretReport {
                secret: (*secret).to_owned(),
                state: *state,
                message: None,
            })
            .collect(),
        error: error.map(str::to_owned),
        duration_ms: None,
    }
}

#[tokio::test]
async fn a_check_says_of_each_secret_whether_it_is_there_never_its_value() {
    let reports = check(&stores(), &registry()).await;
    assert_eq!(
        reports,
        [
            report("twice", false, &[], Some("defined in a.py and b.py")),
            report(
                "api",
                false,
                &[
                    ("secrets['etl/api']['token']", SecretState::Missing),
                    ("secrets['etl/db']['user']", SecretState::Present)
                ],
                Some("connection `api`: `secrets['etl/api']['token']`: no such secret")
            ),
            report(
                "db",
                true,
                &[
                    ("secrets['etl/db']['password']", SecretState::Present),
                    ("secrets['etl/db']['user']", SecretState::Present)
                ],
                None
            ),
            report(
                "opt",
                true,
                &[("secrets['etl/opt'].get('k', 'none')", SecretState::Default)],
                None
            ),
        ]
    );
    let text = serde_json::to_string(&reports).unwrap();
    assert!(!text.contains("hunter22"), "{text}");
}

fn job() -> ConnectionJob {
    ConnectionJob {
        root: "etl".to_owned(),
        code_dir: None,
        registry: registry(),
        test: None,
        cancel_grace: CANCEL_GRACE,
        attempt_id: "t0".to_owned(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(60),
    }
}

#[test]
fn a_test_runs_the_connections_file_as_a_one_off_attempt() {
    let spec = test_spec(&job(), "db", Duration::from_secs(5), "t1".to_owned()).unwrap();
    assert_eq!(
        (
            spec.attempt_id.as_str(),
            spec.dag_root.as_str(),
            spec.dag_path.as_str(),
            spec.connection_test.as_deref(),
            spec.task.connections.clone(),
            spec.task.settings.timeout_ms,
        ),
        (
            "t1",
            "etl",
            "conns.py",
            Some("db"),
            vec!["db".to_owned()],
            Some(5_000)
        )
    );
    let refused = test_spec(&job(), "twice", Duration::from_secs(5), "t2".to_owned());
    assert_eq!(refused.err().as_deref(), Some("defined in a.py and b.py"));
    let mut outside = job();
    let mut registry = (*outside.registry).clone();
    registry
        .files
        .insert("db".to_owned(), "/tmp/evil.py".to_owned());
    outside.registry = Arc::new(registry);
    let escaped = test_spec(&outside, "db", Duration::from_secs(5), "t4".to_owned());
    assert_eq!(
        escaped.err().as_deref(),
        Some("connection `db`: `/tmp/evil.py` isn't a path inside root `etl`")
    );
    let no_file = test_spec(&job(), "api", Duration::from_secs(5), "t3".to_owned());
    assert_eq!(
        no_file.err().as_deref(),
        Some("connection `api`: no file of root `etl` defines it")
    );
}

#[test]
fn a_tests_report_says_how_its_attempt_ended() {
    let finished = |status: pb::AttemptStatus, error: serde_json::Value| pb::Finished {
        status: status.into(),
        error_summary_json: if error.is_null() {
            Vec::new()
        } else {
            serde_json::to_vec(&error).unwrap()
        },
        ..pb::Finished::default()
    };
    let took = Duration::from_millis(1_234);
    let ok = test_report(
        "db",
        &finished(pb::AttemptStatus::Success, json!(null)),
        took,
    );
    assert_eq!((ok.ok, ok.error, ok.duration_ms), (true, None, Some(1_234)));
    let failed = test_report(
        "db",
        &finished(
            pb::AttemptStatus::Failed,
            json!({"exception": "OperationalError", "message": "password authentication failed for user \"etl\"", "traceback": ""}),
        ),
        took,
    );
    assert_eq!(
        (failed.ok, failed.error.as_deref()),
        (
            false,
            Some("OperationalError: password authentication failed for user \"etl\"")
        )
    );
    let timed_out = test_report(
        "db",
        &finished(pb::AttemptStatus::TimedOut, json!(null)),
        took,
    );
    assert_eq!(
        timed_out.error.as_deref(),
        Some("stopped after its timeout (1.2 s)")
    );
}

/// A secret missing at its first read, there at the next (fixed meanwhile).
#[derive(Debug, Default)]
struct FixedMeanwhile {
    reads: std::sync::atomic::AtomicUsize,
}

impl SecretStorage for FixedMeanwhile {
    fn read<'a>(&'a self, _path: &'a str) -> ReadFuture<'a> {
        let first = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                Err(SecretError::NotFound("no such secret".to_owned()))
            } else {
                Ok(BTreeMap::from([(
                    "password".to_owned(),
                    "hunter22".to_owned(),
                )]))
            }
        })
    }
}

#[tokio::test]
async fn a_check_saying_a_secret_is_missing_never_passes() {
    // Its states and its rendering read the secret apart: it passed, a secret "missing".
    let stores = Arc::new(SecretStores::for_test(FixedMeanwhile::default()));
    let registry = Arc::new(Registry {
        connections: BTreeMap::from([(
            "db".to_owned(),
            serde_json::from_value(json!({"name": "db",
                "params": {"password": secret("etl/db", "password")}}))
            .unwrap(),
        )]),
        ..Registry::default()
    });
    let reports = check(&stores, &registry).await;
    assert_eq!(
        (reports[0].ok, reports[0].secrets[0].state),
        (false, SecretState::Missing)
    );
}
