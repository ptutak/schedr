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
    assert_eq!(setup.agent.setup.log_dir, PathBuf::from("/srv/schedr/logs"));

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
    }];
    let setup = plan(&configured, &env(&[("HOME", "/h"), ("HOSTNAME", "pod-1")])).unwrap();
    assert_eq!(setup.grpc.to_string(), "127.0.0.1:9070");
    assert_eq!(setup.http.to_string(), "127.0.0.1:9080");
    assert_eq!(setup.identity_header.as_deref(), Some("x-user"));
    assert_eq!(setup.tick, Duration::from_secs(1));
    assert_eq!(setup.master.lease_timeout, Duration::from_secs(60));
    assert_eq!(setup.agent.worker_id, "pod-1");
    assert_eq!(
        (setup.agent.slots, setup.agent.queues.clone()),
        (4, vec!["large".to_owned()])
    );
    assert_eq!(setup.agent.setup.roots["etl"], PathBuf::from("/dags/etl"));
    assert_eq!(setup.agent.setup.python, PathBuf::from("/usr/bin/python3"));
    assert_eq!(setup.parse_file_timeout, Duration::from_secs(60));
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
    configured.webserver.port = 0;
    configured.master.tick_interval = Duration::from_millis(50);
    let setup = plan(&configured, &env(&[("HOME", "/h")])).unwrap();
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
    let listening = addresses.await.unwrap();
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
