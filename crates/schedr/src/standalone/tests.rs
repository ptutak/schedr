use pretty_assertions::assert_eq;
use schedr_config::Root;
use schedr_config::config::Interpreter;
use tempfile::TempDir;

use super::*;

fn config() -> Config {
    let mut config = Config::default();
    config
        .interpreters
        .insert("python".to_owned(), Interpreter::path("/usr/bin/python3"));
    config
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn the_data_directory_comes_from_the_config_xdg_or_home() {
    let mut configured = config();
    configured.worker.data_dir = Some(PathBuf::from("/srv/schedr"));
    let setup = plan(&configured, &env(&[("HOME", "/home/anna")])).unwrap();
    assert_eq!(setup.data_dir, PathBuf::from("/srv/schedr"));
    assert_eq!(setup.database_url, "sqlite:///srv/schedr/schedr.db");
    assert!(
        setup.agent.setup.log_dir.starts_with("/srv/schedr/logs/"),
        "{}",
        setup.agent.setup.log_dir.display()
    );

    let xdg = plan(
        &config(),
        &env(&[("XDG_DATA_HOME", "/x"), ("HOME", "/home/anna")]),
    )
    .unwrap();
    assert_eq!(xdg.data_dir, PathBuf::from("/x/schedr"));
    let home = plan(&config(), &env(&[("HOME", "/home/anna")])).unwrap();
    assert_eq!(
        home.data_dir,
        PathBuf::from("/home/anna/.local/share/schedr")
    );
    let error = plan(&config(), &BTreeMap::new()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "no data directory: set [worker] data_dir (or HOME)"
    );
}

#[test]
fn a_configured_database_wins() {
    let mut configured = config();
    configured.database.url = Some("postgres://db/schedr".to_owned());
    let setup = plan(&configured, &env(&[("HOME", "/h")])).unwrap();
    assert_eq!(setup.database_url, "postgres://db/schedr");
}

#[test]
fn everything_listens_on_localhost_with_the_configured_settings() {
    let mut configured = config();
    configured.master.grpc_port = 9070;
    configured.webserver.port = 9080;
    configured.webserver.auth.identity_header = Some("x-user".to_owned());
    configured.worker.slots = 4;
    configured.worker.queues = vec!["large".to_owned()];
    configured.dag_roots = vec![Root {
        name: "etl".to_owned(),
        path: PathBuf::from("/dags/etl"),
        only: None,
    }];
    let setup = plan(&configured, &env(&[("HOME", "/h"), ("HOSTNAME", "pod-1")])).unwrap();
    assert_eq!(setup.grpc.to_string(), "127.0.0.1:9070");
    assert_eq!(setup.http.to_string(), "127.0.0.1:9080");
    assert_eq!(setup.auth.identity_header.as_deref(), Some("x-user"));
    assert_eq!(setup.tick, Duration::from_secs(1));
    assert_eq!(setup.master.lease_timeout, Duration::from_secs(60));
    assert_eq!(setup.agent.worker_id, "pod-1");
    assert_eq!(
        (setup.agent.slots, setup.agent.queues.clone()),
        (4, vec!["large".to_owned()])
    );
    assert_eq!(setup.agent.setup.roots["etl"], PathBuf::from("/dags/etl"));
    assert_eq!(setup.agent.setup.python, PathBuf::from("/usr/bin/python3"));
    assert_eq!(setup.parse.per_file, Duration::from_secs(60));
    configured.worker.id = Some("worker-1".to_owned());
    assert_eq!(
        plan(&configured, &env(&[("HOME", "/h")]))
            .unwrap()
            .agent
            .worker_id,
        "worker-1"
    );
    configured.worker.id = None;
    assert_eq!(
        plan(&configured, &env(&[("HOME", "/h")]))
            .unwrap()
            .agent
            .worker_id,
        "localhost"
    );
}

#[test]
fn an_undefined_interpreter_is_an_error() {
    let mut configured = config();
    configured.dags.interpreter = "etl-venv".to_owned();
    let error = plan(&configured, &env(&[("HOME", "/h")])).unwrap_err();
    assert_eq!(
        error.to_string(),
        "the DAG interpreter `etl-venv` isn't defined in [interpreters]"
    );
}

#[tokio::test]
async fn standalone_serves_the_api_and_stops_when_asked() {
    let dir = TempDir::new().unwrap();
    let mut configured = config();
    configured.worker.data_dir = Some(dir.path().join("data"));
    configured.master.grpc_port = 0;
    configured.ops.port = 0;
    configured.webserver.port = 0;
    // Every port the system's choice: a standalone running here holds the defaults.
    configured.worker.log_port = 0;
    configured.timings.tick_interval = Duration::from_millis(50);
    let mut setup = plan(&configured, &env(&[("HOME", "/h")])).unwrap();
    assert!(!setup.migrate, "migrating is asked for (`--migrate`)");
    setup.migrate = true;
    let (stop, stopped) = tokio::sync::watch::channel(0);
    let (bound, addresses) = tokio::sync::oneshot::channel();
    let shown = setup.clone();
    let server = tokio::spawn(run(
        setup,
        move |listening| {
            let _ = bound.send(listening);
        },
        stopped,
    ));
    let Ok(listening) = addresses.await else {
        panic!("standalone ended before listening: {:?}", server.await);
    };
    assert!(banner(listening, &shown).starts_with("schedr standalone: API on http://127.0.0.1:"));
    let body: serde_json::Value = reqwest::get(format!("http://{}/api/v1/dags", listening.http))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body, serde_json::json!({"dags": []}));
    assert!(dir.path().join("data/schedr.db").is_file());
    stop.send(1).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("standalone stops in time")
        .unwrap()
        .unwrap();
}

#[test]
fn one_standalone_per_data_directory() {
    let tmp = TempDir::new().unwrap();
    let held = lock_data_dir(tmp.path()).unwrap();
    assert!(tmp.path().join(LOCK_FILE).is_file());
    let error = lock_data_dir(tmp.path()).unwrap_err().to_string();
    assert!(
        error.starts_with(&format!(
            "another schedr standalone is using {}",
            tmp.path().display()
        )),
        "{error}"
    );
    assert!(error.contains("one replica"), "{error}");
    drop(held);
    lock_data_dir(tmp.path()).unwrap();
    let error = lock_data_dir(&tmp.path().join("missing/dir"))
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("can't open "), "{error}");
}

#[test]
fn an_unwritable_data_directory_says_how_to_fix_it() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = TempDir::new().unwrap();
    std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let result = lock_data_dir(tmp.path());
    std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    if rustix::process::geteuid().is_root() {
        // Root writes anyway: there is no permission to miss.
        assert!(result.is_ok());
    } else {
        let error = result.unwrap_err().to_string();
        assert!(error.contains("give the pod an fsGroup"), "{error}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_worker_stuck_after_the_masters_failure_is_given_up_after_the_bound() {
    // B151: once the master failed, a worker that doesn't stop (stuck on its way out) no
    // longer keeps standalone running with no master.
    let (failed, watching) = watch::channel(false);
    let started = tokio::time::Instant::now();
    let stuck = async {
        failed.send(true).unwrap();
        std::future::pending::<()>().await;
    };
    assert!(!ended_within(stuck, watching, Duration::from_secs(10)).await);
    assert_eq!(started.elapsed(), Duration::from_secs(10));

    // Without a failure, nothing is cut short; a worker that stops in time ended.
    let (_failed, watching) = watch::channel(false);
    let slow = tokio::time::sleep(Duration::from_secs(3600));
    assert!(ended_within(slow, watching, Duration::from_secs(10)).await);
    let (failed, watching) = watch::channel(false);
    let prompt = async {
        failed.send(true).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    assert!(ended_within(prompt, watching, Duration::from_secs(10)).await);
    // The sender dropped without a failure: not a failure.
    let (failed, watching) = watch::channel(false);
    drop(failed);
    let slow = tokio::time::sleep(Duration::from_secs(3600));
    assert!(ended_within(slow, watching, Duration::from_secs(10)).await);
}

/// B206's review: `Debug` of a standalone hides its database URLs' passwords.
#[test]
fn debug_hides_the_database_urls_passwords() {
    let mut configured = config();
    configured.database.url = Some("postgres://u:db_s3cret@db/schedr".to_owned());
    configured.database.notifications_url = Some("postgres://u:listen_s3cret@db/schedr".to_owned());
    let setup = plan(&configured, &env(&[("HOME", "/h")])).unwrap();
    let shown = format!("{setup:?}");
    assert!(
        !shown.contains("db_s3cret") && !shown.contains("listen_s3cret"),
        "{shown}"
    );
    assert!(shown.contains("postgres://u:***@db/schedr"), "{shown}");
}
