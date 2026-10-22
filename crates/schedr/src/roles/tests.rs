use std::net::Ipv4Addr;

use pretty_assertions::assert_eq;
use schedr_config::config::Interpreter;
use schedr_db::DbError;
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

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

#[test]
fn the_master_settings_come_from_the_config() {
    let mut config = config();
    config.master.lease_timeout = Duration::from_secs(90);
    let settings = master_settings(&config);
    assert_eq!(settings.lease_timeout, Duration::from_secs(90));
    assert_eq!(settings.heartbeat_interval, Duration::from_secs(10));
    assert_eq!(settings.parse_job_timeout, PARSE_JOB_TIMEOUT);
}

#[test]
fn the_data_directory_is_configured_or_under_xdg_or_home() {
    let mut configured = config();
    configured.worker.data_dir = Some(PathBuf::from("/srv/schedr"));
    assert_eq!(
        data_dir(&configured, &env(&[])).unwrap(),
        PathBuf::from("/srv/schedr")
    );
    assert_eq!(
        data_dir(&config(), &env(&[("XDG_DATA_HOME", "/x"), ("HOME", "/h")])).unwrap(),
        PathBuf::from("/x/schedr")
    );
    assert_eq!(
        data_dir(&config(), &env(&[("HOME", "/h")])).unwrap(),
        PathBuf::from("/h/.local/share/schedr")
    );
    let error = data_dir(&config(), &env(&[])).unwrap_err();
    assert!(error.to_string().contains("no data directory"), "{error}");
}

#[test]
fn agent_settings_name_the_worker_and_its_interpreter() {
    let mut config = config();
    config.worker.slots = 4;
    let agent = agent_settings(&config, &env(&[("HOSTNAME", "pod-1")]), Path::new("/d")).unwrap();
    assert_eq!(agent.worker_id, "pod-1");
    assert_eq!(agent.slots, 4);
    assert_eq!(agent.setup.python, PathBuf::from("/usr/bin/python3"));
    assert_eq!(agent.setup.log_dir, PathBuf::from("/d/logs"));
    assert_eq!(
        agent_settings(&config, &env(&[]), Path::new("/d"))
            .unwrap()
            .worker_id,
        "localhost"
    );
    config.dags.interpreter = "etl-venv".to_owned();
    let error = agent_settings(&config, &env(&[]), Path::new("/d")).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the DAG interpreter `etl-venv` isn't defined"),
        "{error}"
    );
}

#[tokio::test]
async fn listen_binds_or_says_why_not() {
    let listener = listen(loopback()).await.unwrap();
    let taken = listener.local_addr().unwrap();
    let error = listen(taken).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with(&format!("can't listen on {taken}: ")),
        "{error}"
    );
}

async fn memory_db() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    db
}

#[tokio::test]
async fn the_master_and_the_api_serve_until_stopped() {
    let (stop, stopped) = watch::channel(false);
    let master = serve_master(
        memory_db().await,
        master_settings(&config()),
        Duration::from_millis(10),
        listen(loopback()).await.unwrap(),
        stopped.clone(),
    );
    let api = serve_api(
        memory_db().await,
        None,
        listen(loopback()).await.unwrap(),
        stopped,
    );
    let later = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = stop.send(true);
    };
    let (master, api, ()) = tokio::join!(master, api, later);
    master.unwrap();
    api.unwrap();
}

#[tokio::test]
async fn a_worker_without_a_reachable_master_stops_when_asked() {
    let tmp = TempDir::new().unwrap();
    let agent = agent_settings(&config(), &env(&[]), tmp.path()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let worker = run_worker(
        agent,
        Duration::from_secs(1),
        "http://127.0.0.1:1".to_owned(),
        stopped,
    );
    let later = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = stop.send(true);
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(worker, later);
    })
    .await
    .expect("the worker stops");
}

#[tokio::test]
async fn a_role_needs_a_database_with_a_usable_schema() {
    assert!(matches!(
        open_database(&config(), "master").await,
        Err(AppError::NoDatabase)
    ));
    let tmp = TempDir::new().unwrap();
    let url = format!("sqlite://{}", tmp.path().join("schedr.db").display());
    let mut config = config();
    config.database.url = Some(url.clone());
    let error = open_database(&config, "master").await.unwrap_err();
    assert!(
        matches!(error, AppError::Db(DbError::SchemaNotUsable { .. })),
        "{error}"
    );
    Database::connect(&url)
        .await
        .unwrap()
        .migrate()
        .await
        .unwrap();
    let db = open_database(&config, "webserver").await.unwrap();
    assert_eq!(db.backend(), Backend::Sqlite);
}

#[test]
fn plain_text_is_fine_on_loopback_only() {
    for host in ["localhost", "LocalHost", "127.0.0.1", "::1", "[::1]"] {
        assert!(is_loopback(host), "{host}");
        assert_eq!(plain_text_warning(host), None, "{host}");
    }
    for host in ["0.0.0.0", "10.0.0.5", "schedr-master"] {
        assert!(!is_loopback(host), "{host}");
        let warning = plain_text_warning(host).unwrap();
        assert!(
            warning.starts_with(&format!("gRPC to {host} is plain text until mTLS")),
            "{warning}"
        );
    }
}

#[test]
fn the_master_url_is_an_http_url_with_a_host() {
    assert_eq!(
        master_host("http://schedr-master:8070").unwrap(),
        "schedr-master"
    );
    assert_eq!(
        master_host("http://user:pw@127.0.0.1:8070/").unwrap(),
        "127.0.0.1"
    );
    assert_eq!(master_host("http://[::1]:8070").unwrap(), "[::1]");
    for (url, why) in [
        ("schedr-master:8070", "needs the http:// scheme"),
        (
            "https://schedr-master:8070",
            "uses https, which comes with mTLS",
        ),
        ("http://", "isn't a URL"),
        ("not a url", "isn't a URL"),
    ] {
        let error = master_host(url).unwrap_err().to_string();
        assert!(
            error.starts_with(&format!("[worker] master_url `{url}` {why}")),
            "{error}"
        );
    }
}

#[tokio::test]
async fn the_roles_say_what_they_miss() {
    assert!(matches!(master(&config()).await, Err(AppError::NoDatabase)));
    assert!(matches!(
        webserver(&config()).await,
        Err(AppError::NoDatabase)
    ));
    let error = worker(&config(), &env(&[("HOME", "/h")]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no master to connect to: set [worker] master_url"),
        "{error}"
    );
    let mut bad_url = config();
    bad_url.worker.master_url = Some("schedr-master:8070".to_owned());
    let error = worker(&bad_url, &env(&[("HOME", "/h")])).await.unwrap_err();
    assert!(
        error.to_string().contains("needs the http:// scheme"),
        "{error}"
    );
}

#[tokio::test]
async fn until_completes_once_asked_to_stop() {
    let (stop, stopped) = watch::channel(false);
    let waiting = tokio::spawn(until(stopped));
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_signal_stop_starts_unstopped() {
    // The signal itself is exercised by the integration tests (a real SIGTERM).
    assert!(!*stop_on_signal().borrow());
}

#[test]
fn an_open_api_is_warned_about_with_its_address() {
    assert_eq!(no_auth_warning(loopback(), "x"), None);
    let open = SocketAddr::from(([0, 0, 0, 0], 8080));
    let warning = no_auth_warning(open, "bind 127.0.0.1").unwrap();
    assert!(
        warning.starts_with("the REST API has no authentication of its own"),
        "{warning}"
    );
    assert!(
        warning.ends_with("listens on 0.0.0.0:8080: keep it behind an authenticating proxy or mesh, or bind 127.0.0.1"),
        "{warning}"
    );
}
