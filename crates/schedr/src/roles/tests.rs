use std::net::Ipv4Addr;

use pretty_assertions::assert_eq;
use schedr_agent::memory::STANDALONE_RESERVE;
use schedr_config::config::Interpreter;
use schedr_db::DbError;
use tempfile::TempDir;

use super::*;

fn config() -> Config {
    let mut config = Config::default();
    // Roles started here bind their ops port: a free one.
    config.ops.port = 0;
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
    let agent = agent_settings(
        &config,
        &env(&[("HOSTNAME", "pod-1")]),
        Path::new("/d"),
        WORKER_RESERVE,
    )
    .unwrap();
    assert_eq!(agent.worker_id, "pod-1");
    assert_eq!(agent.slots, 4);
    assert_eq!(agent.setup.python, PathBuf::from("/usr/bin/python3"));
    assert_eq!(agent.setup.log_dir, PathBuf::from("/d/logs/pod-1"));
    assert_eq!(agent.journal_dir, PathBuf::from("/d/journal/pod-1"));
    for (id, dir) in [
        ("../etl/w 1", "%2E.%2Fetl%2Fw%201"),
        ("pod:1", "pod%3A1"),
        ("pod_1", "pod_1"),
        ("pod%3A1", "pod%253A1"),
        ("", "%"),
    ] {
        config.worker.id = Some(id.to_owned());
        let agent = agent_settings(&config, &env(&[]), Path::new("/d"), WORKER_RESERVE).unwrap();
        assert_eq!(agent.journal_dir, Path::new("/d/journal").join(dir), "{id}");
    }
    config.worker.id = None;
    assert_eq!(
        agent_settings(&config, &env(&[]), Path::new("/d"), WORKER_RESERVE)
            .unwrap()
            .worker_id,
        "localhost"
    );
    config.dags.interpreter = "etl-venv".to_owned();
    let error = agent_settings(&config, &env(&[]), Path::new("/d"), WORKER_RESERVE).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the DAG interpreter `etl-venv` isn't defined"),
        "{error}"
    );
}

#[test]
fn one_agent_holds_a_journal() {
    let tmp = TempDir::new().unwrap();
    let agent = agent_settings(&config(), &env(&[]), tmp.path(), WORKER_RESERVE).unwrap();
    let held = lock_journal(&agent).unwrap();
    let error = lock_journal(&agent).unwrap_err().to_string();
    assert!(
        error.starts_with("another schedr worker `localhost` is using "),
        "{error}"
    );
    drop(held);
    std::fs::remove_dir_all(tmp.path().join("journal")).unwrap();
    std::fs::write(tmp.path().join("journal"), "a file").unwrap();
    let error = lock_journal(&agent).unwrap_err().to_string();
    assert!(error.starts_with("can't lock the journal in "), "{error}");
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
    let (stop, stopped) = watch::channel(0);
    let health = Health::new(Duration::from_secs(120));
    let db = memory_db().await;
    let master = serve_master(
        db.clone(),
        master_settings(&config()),
        Duration::from_millis(10),
        listen(loopback()).await.unwrap(),
        None,
        take_lease(db, Duration::from_secs(15), "master"),
        Observed {
            metrics: MasterMetrics::default(),
            health: &health,
            ready: health.starting("master"),
        },
        stopped.clone(),
    );
    let api = serve_api(
        memory_db().await,
        None,
        listen(loopback()).await.unwrap(),
        None,
        Observed {
            metrics: ApiMetrics::default(),
            health: &health,
            ready: health.starting("webserver"),
        },
        stopped,
    );
    let later = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = stop.send(1);
    };
    let (master, api, ()) = tokio::join!(master, api, later);
    master.unwrap();
    api.unwrap();
    // Both said they were ready; the master's loop beat.
    assert_eq!(health.not_ready(), Vec::<&str>::new());
    assert_eq!(health.stalled(std::time::Instant::now()), None);
}

#[tokio::test]
async fn a_worker_without_a_reachable_master_stops_when_asked() {
    let tmp = TempDir::new().unwrap();
    let agent = agent_settings(&config(), &env(&[]), tmp.path(), WORKER_RESERVE).unwrap();
    let (stop, stopped) = watch::channel(0);
    // Boxed, as in standalone: the worker's future is large.
    let links = WorkerLinks {
        master_url: "http://127.0.0.1:1".to_owned(),
        tls: None,
        log_listener: listen(loopback()).await.unwrap(),
        retention: log_retention(&config()),
    };
    let health = Health::new(Duration::from_secs(120));
    let observed = Observed {
        metrics: AgentMetrics::default(),
        health: &health,
        ready: health.starting("worker"),
    };
    let worker = Box::pin(run_worker(
        agent,
        Duration::from_secs(1),
        links,
        observed,
        stopped,
    ));
    let later = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = stop.send(1);
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
fn the_master_url_has_the_scheme_of_the_tls_mode_and_a_host() {
    assert_eq!(
        master_host("https://schedr-master:8070", TlsMode::App).unwrap(),
        "schedr-master"
    );
    assert_eq!(
        master_host("http://user:pw@127.0.0.1:8070/", TlsMode::Mesh).unwrap(),
        "127.0.0.1"
    );
    assert_eq!(
        master_host("http://[::1]:8070", TlsMode::Mesh).unwrap(),
        "[::1]"
    );
    for (url, mode, why, example) in [
        (
            "schedr-master:8070",
            TlsMode::Mesh,
            "needs the http:// scheme",
            "http://schedr-master:8070",
        ),
        (
            "http://schedr-master:8070",
            TlsMode::App,
            "needs the https:// scheme with [tls] mode `app`",
            "https://schedr-master:8070",
        ),
        (
            "https://schedr-master:8070",
            TlsMode::Mesh,
            "needs the http:// scheme with [tls] mode `mesh`",
            "http://schedr-master:8070",
        ),
        ("http://", TlsMode::Mesh, "isn't a URL", "http://"),
        ("not a url", TlsMode::App, "isn't a URL", "https://"),
    ] {
        let error = master_host(url, mode).unwrap_err().to_string();
        assert!(
            error.starts_with(&format!("[worker] master_url `{url}` {why}: ")),
            "{error}"
        );
        assert!(error.contains(example), "{error}");
    }
}

#[test]
fn the_log_endpoint_is_the_log_address_or_the_host_name() {
    let mut config = config();
    let env = env(&[("HOSTNAME", "pod-1")]);
    assert_eq!(
        log_endpoint(&config, &env, true, 8081),
        "https://pod-1:8081"
    );
    assert_eq!(
        log_endpoint(&config, &BTreeMap::new(), false, 8081),
        "http://localhost:8081"
    );
    config.worker.log_address = Some("w-0.schedr-worker.etl.svc.cluster.local".to_owned());
    assert_eq!(
        log_endpoint(&config, &env, true, 9000),
        "https://w-0.schedr-worker.etl.svc.cluster.local:9000"
    );
    config.worker.log_address = Some("fd00::7".to_owned());
    assert_eq!(
        log_endpoint(&config, &env, false, 8081),
        "http://[fd00::7]:8081"
    );
}

#[test]
fn log_retention_comes_from_the_config() {
    let mut config = config();
    config.worker.log_max_size = Some(1 << 30);
    assert_eq!(
        log_retention(&config),
        LogRetention {
            max_age: Duration::from_hours(720),
            max_size: Some(1 << 30),
        }
    );
}

#[test]
fn app_mode_needs_the_three_files_and_mesh_mode_none() {
    let mut config = config();
    let error = role_tls(&config).unwrap_err().to_string();
    assert!(
        error.starts_with(
            "[tls] mode is `app` (the default), which needs [tls] ca (SCHEDR_CONFIG__TLS__CA); \
             or set [tls] mode = \"mesh\""
        ),
        "{error}"
    );
    let dir = TempDir::new().unwrap();
    let files = schedr_tls::testing::TestCa::new("ca").write(dir.path(), "w", &["schedr-worker"]);
    config.tls.ca = Some(files.ca.clone());
    config.tls.cert = Some(files.cert.clone());
    let error = role_tls(&config).unwrap_err().to_string();
    assert!(
        error.contains("needs [tls] key (SCHEDR_CONFIG__TLS__KEY)"),
        "{error}"
    );
    config.tls.key = Some(dir.path().join("missing.key"));
    let error = role_tls(&config).unwrap_err().to_string();
    assert!(error.starts_with("can't read [tls] key"), "{error}");
    config.tls.key = Some(files.key);
    assert!(role_tls(&config).unwrap().is_some());
    config.tls.reload_interval = Duration::ZERO;
    assert_eq!(
        role_tls(&config).unwrap_err().to_string(),
        "[tls] reload_interval must be longer than 0s (e.g. \"30s\")"
    );
    config.tls.mode = TlsMode::Mesh;
    assert!(role_tls(&config).unwrap().is_none());
}

#[tokio::test]
async fn the_roles_say_what_they_miss() {
    for error in [
        Box::pin(master(&config())).await,
        webserver(&config()).await,
    ] {
        let error = error.unwrap_err().to_string();
        assert!(error.contains("needs [tls] ca"), "{error}");
    }
    let mut mesh = config();
    mesh.tls.mode = TlsMode::Mesh;
    assert!(matches!(
        Box::pin(master(&mesh)).await,
        Err(AppError::NoDatabase)
    ));
    assert!(matches!(webserver(&mesh).await, Err(AppError::NoDatabase)));
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
        error.to_string().contains("needs the https:// scheme"),
        "{error}"
    );
    let mut no_certificates = config();
    no_certificates.worker.master_url = Some("https://schedr-master:8070".to_owned());
    let error = worker(&no_certificates, &env(&[("HOME", "/h")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("needs [tls] ca"), "{error}");
}

#[tokio::test]
async fn until_completes_once_asked_to_stop() {
    let (stop, stopped) = watch::channel(0);
    let waiting = tokio::spawn(until(stopped));
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    stop.send(1).unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_signal_stop_starts_unstopped() {
    // The signal itself is exercised by the integration tests (a real SIGTERM).
    assert_eq!(*stop_on_signal().borrow(), 0);
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

#[test]
fn the_memory_watch_takes_the_configured_budget_or_shares_the_limit() {
    let mut config = config();
    let watch = memory_watch(&config, Some(4 << 30), WORKER_RESERVE);
    let limit: u64 = 4 << 30;
    assert_eq!(watch.budget, Some((limit - limit / 100 * 10) / 2));
    assert_eq!(watch.interval, Duration::from_secs(1));
    assert_eq!(memory_watch(&config, None, WORKER_RESERVE).budget, None);
    // Standalone keeps a quarter for its master and API; `memory_reserve` overrides either.
    let standalone = memory_watch(&config, Some(limit), STANDALONE_RESERVE);
    assert_eq!(standalone.budget, Some((limit - limit / 100 * 25) / 2));
    config.worker.memory_reserve = Some(MemoryReserve::Size(1 << 30));
    let reserved = memory_watch(&config, Some(limit), STANDALONE_RESERVE);
    assert_eq!(reserved.budget, Some((limit - (1 << 30)) / 2));
    config.worker.memory_reserve = Some(MemoryReserve::Percent(50));
    let half = memory_watch(&config, Some(limit), WORKER_RESERVE);
    assert_eq!(half.budget, Some((limit - limit / 100 * 50) / 2));
    config.worker.memory_reserve = None;
    config.worker.memory_budget = Some(100 << 20);
    config.worker.memory_check_interval = Duration::from_millis(200);
    let watch = memory_watch(&config, None, WORKER_RESERVE);
    assert_eq!(
        (watch.budget, watch.interval),
        (Some(100 << 20), Duration::from_millis(200))
    );
    assert_eq!(memory_text(&watch), "memory budget 100 MiB per task");
    assert_eq!(
        memory_text(&MemoryWatch::default()),
        "no memory limit found: task memory is measured, never limited"
    );
}

#[test]
fn a_zero_check_interval_or_a_tiny_budget_is_refused() {
    let mut config = config();
    config.worker.memory_check_interval = Duration::ZERO;
    let error = agent_settings(&config, &env(&[]), Path::new("/d"), WORKER_RESERVE)
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "[worker] memory_check_interval must be longer than 0s (e.g. \"1s\")"
    );
    let mut config = self::config();
    config.worker.memory_budget = Some(1 << 10);
    let error = agent_settings(&config, &env(&[]), Path::new("/d"), WORKER_RESERVE)
        .unwrap_err()
        .to_string();
    assert_eq!(
        error,
        "[worker] memory_budget 1 KiB is too small for any task: give at least 16 MiB"
    );
}

#[test]
fn the_query_timeout_must_be_shorter_than_the_loop_stall_timeout() {
    let mut config = config();
    assert!(check_timeouts(&config).is_ok());
    config.database.query_timeout = Duration::from_mins(3);
    assert_eq!(
        check_timeouts(&config).unwrap_err().to_string(),
        "[database] query_timeout (180 s) must be at least 1 ms and shorter than [ops] \
         loop_stall_timeout (120 s)"
    );
    config.database.query_timeout = Duration::ZERO;
    assert!(check_timeouts(&config).is_err());
    config.database.query_timeout = Duration::from_micros(500);
    assert!(check_timeouts(&config).is_err());
}

#[test]
fn the_lease_period_must_be_shorter_than_the_loop_stall_timeout() {
    let mut config = config();
    config.master.lease_period = Duration::from_mins(2);
    assert_eq!(
        check_timeouts(&config).unwrap_err().to_string(),
        "[master] lease_period (120 s) must be at least three [database] query_timeout (10 s) \
         and shorter than [ops] loop_stall_timeout (120 s)"
    );
    config.master.lease_period = Duration::from_secs(30);
    assert!(check_timeouts(&config).is_ok());
}

#[test]
fn the_lease_period_must_fit_three_query_timeouts() {
    // A renewal, limited by the query limit, must fit a third of the period.
    let mut config = config();
    config.master.lease_period = Duration::from_secs(29);
    assert!(check_timeouts(&config).is_err());
    config.database.query_timeout = Duration::from_millis(300);
    config.master.lease_period = Duration::from_millis(900);
    assert!(check_timeouts(&config).is_ok());
}

#[tokio::test]
async fn the_ops_port_is_bound_and_logged() {
    let mut config = config();
    config.ops.bind = std::net::IpAddr::from([127, 0, 0, 1]);
    config.ops.port = 0;
    let (stop, stopped) = watch::channel(0);
    let ops = crate::ops::Ops::new(
        prometheus_client::registry::Registry::default(),
        Health::new(Duration::from_secs(120)),
    );
    let bound = start_ops(&config, "master", ops, &stopped).await.unwrap();
    assert_ne!(bound.port(), 0);
    stop.send(1).unwrap();
}
