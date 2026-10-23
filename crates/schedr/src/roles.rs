//! The server roles as processes of their own (M2.1): `schedr master`, `schedr webserver` and
//! `schedr worker`, and the pieces `schedr standalone` runs together in one process.
//!
//! - **master:** the gRPC service workers connect to and the master's loop, on the database
//!   (Postgres, or SQLite on one host); it refuses an outdated schema (`schedr db migrate`
//!   runs first) and picks commands up at once on Postgres (`LISTEN/NOTIFY`), else by polling.
//! - **webserver:** the REST API on the same database.
//! - **worker:** the agent, connected to `[worker] master_url`, and its log endpoint (the
//!   webserver reads task logs from it), with log retention; no database.
//!
//! The internal links (worker → master, webserver → worker logs) use mTLS in `[tls] mode =
//! "app"` (the default: each role needs `[tls] ca`, `cert` and `key`, read again when they
//! change) and plain traffic in `mesh` mode, where a service mesh encrypts. SIGTERM and Ctrl-C
//! stop a role (a worker drains first). See PROJECT.md, Architecture: Components and roles.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::ops::{Health, Ops, Starting, ops_line, serve_ops};
use prometheus_client::registry::Registry;
use schedr_agent::agent::{Agent, AgentSettings, ProcessExecutor};
use schedr_agent::attempt::WorkerSetup;
use schedr_agent::drain::DrainPeriods;
use schedr_agent::journal::Journal;
use schedr_agent::logs::LogEndpoint;
use schedr_agent::memory::{MIN_BUDGET, MemoryWatch, budget, cgroup_limit, show_size};
use schedr_agent::metrics::AgentMetrics;
use schedr_agent::retention::{LogRetention, sweep_logs};
use schedr_api::logs::HttpLogSource;
use schedr_api::metrics::ApiMetrics;
use schedr_api::{ApiState, router};
use schedr_config::{Config, TlsMode};
use schedr_db::{Backend, Database, redact_password};
use schedr_master::core::{CoreError, MasterCore, Settings};
use schedr_master::metrics::MasterMetrics;
use schedr_master::parse_jobs::PARSE_JOB_TIMEOUT;
use schedr_master::service::Hub;
use schedr_proto::pb::master_client::MasterClient;
use schedr_tls::{Protocol, Tls, TlsFiles, incoming};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::app::{AppError, role_config};

#[cfg(test)]
mod tests;

/// Completes once `stop` turns `true`: how every part of a role learns to stop.
pub async fn until(mut stop: watch::Receiver<u32>) {
    let _ = stop.wait_for(|requests| *requests > 0).await;
}

/// The master's timing, from the configuration.
#[must_use]
pub fn master_settings(config: &Config) -> Settings {
    Settings {
        heartbeat_interval: config.master.heartbeat_interval,
        lease_timeout: config.master.lease_timeout,
        cancel_grace: config.master.cancel_grace,
        parse_job_timeout: PARSE_JOB_TIMEOUT,
        code_wait: config.master.code_wait,
    }
}

/// Where task logs (and standalone's SQLite file) go: `[worker] data_dir`, else
/// `$XDG_DATA_HOME/schedr`, else `~/.local/share/schedr`.
///
/// # Errors
///
/// [`AppError::BadArgument`] when none of them is set.
pub fn data_dir(config: &Config, env: &BTreeMap<String, String>) -> Result<PathBuf, AppError> {
    config
        .worker
        .data_dir
        .clone()
        .or_else(|| {
            env.get("XDG_DATA_HOME")
                .map(|d| PathBuf::from(d).join("schedr"))
        })
        .or_else(|| {
            env.get("HOME")
                .map(|h| PathBuf::from(h).join(".local/share/schedr"))
        })
        .ok_or_else(|| {
            AppError::BadArgument("no data directory: set [worker] data_dir (or HOME)".to_owned())
        })
}

/// The worker agent's settings, from the configuration; its journal is
/// `<data_dir>/journal/<worker ID>` and its logs `<data_dir>/logs/<worker ID>` (other bytes
/// than letters, digits, `-`, `_` and `.`, and a leading `.`, as `%XX`), so workers sharing a
/// data directory never see each other's (retention, the log endpoint).
///
/// # Errors
///
/// [`AppError::BadArgument`] if the DAG interpreter isn't defined, the memory check interval
/// is 0 or the memory budget below [`MIN_BUDGET`].
pub fn agent_settings(
    config: &Config,
    env: &BTreeMap<String, String>,
    data_dir: &Path,
) -> Result<AgentSettings, AppError> {
    let python = config
        .interpreters
        .get(&config.dags.interpreter)
        .map(|i| i.path.clone())
        .ok_or_else(|| {
            AppError::BadArgument(format!(
                "the DAG interpreter `{}` isn't defined in [interpreters]",
                config.dags.interpreter
            ))
        })?;
    let worker_id = config
        .worker
        .id
        .clone()
        .or_else(|| env.get("HOSTNAME").cloned())
        .unwrap_or_else(|| "localhost".to_owned());
    if config.worker.memory_check_interval.is_zero() {
        return Err(AppError::BadArgument(
            "[worker] memory_check_interval must be longer than 0s (e.g. \"1s\")".to_owned(),
        ));
    }
    if let Some(budget) = config.worker.memory_budget
        && budget < MIN_BUDGET
    {
        return Err(AppError::BadArgument(format!(
            "[worker] memory_budget {} is too small for any task: give at least {}",
            show_size(budget),
            show_size(MIN_BUDGET)
        )));
    }
    Ok(AgentSettings {
        queues: config.worker.queues.clone(),
        slots: config.worker.slots,
        setup: WorkerSetup {
            roots: config
                .dag_roots
                .iter()
                .map(|r| (r.name.clone(), r.path.clone()))
                .collect(),
            python,
            agent_env: env.clone(),
            pass_env: config.worker.pass_env.clone(),
            log_dir: data_dir.join("logs").join(file_name(&worker_id)),
            memory: memory_watch(
                config,
                cgroup_limit(Path::new("/proc"), Path::new("/sys/fs/cgroup"))
                    .ok()
                    .flatten(),
            ),
        },
        poll_interval: config.dags.poll_interval,
        settle_time: config.dags.settle_time,
        journal_dir: data_dir.join("journal").join(file_name(&worker_id)),
        drain: DrainPeriods {
            grace: config.worker.termination_grace_period,
            terminate_period: config.worker.drain_terminate_period,
            report_period: config.worker.drain_report_period,
        },
        // Known once the log endpoint listens (see [`log_endpoint`]).
        log_endpoint: String::new(),
        worker_id,
    })
}

/// How the memory watchdog watches tasks: `[worker] memory_budget`, else (`limit` − 10 %) ÷
/// slots; without either it only measures.
#[must_use]
pub fn memory_watch(config: &Config, limit: Option<u64>) -> MemoryWatch {
    MemoryWatch {
        budget: budget(config.worker.memory_budget, limit, config.worker.slots),
        interval: config.worker.memory_check_interval,
        ..MemoryWatch::default()
    }
}

/// What the watchdog does, for the worker's startup line.
#[must_use]
pub fn memory_text(watch: &MemoryWatch) -> String {
    match watch.budget {
        Some(budget) => format!("memory budget {} per task", show_size(budget)),
        None => "no memory limit found: task memory is measured, never limited".to_owned(),
    }
}

/// The worker's log retention, from the configuration.
#[must_use]
pub const fn log_retention(config: &Config) -> LogRetention {
    LogRetention {
        max_age: config.worker.log_retention,
        max_size: config.worker.log_max_size,
    }
}

/// Where the webserver reaches this worker's log endpoint listening on `port`: `[worker]
/// log_address` (else the host name), over https with TLS.
#[must_use]
pub fn log_endpoint(
    config: &Config,
    env: &BTreeMap<String, String>,
    tls: bool,
    port: u16,
) -> String {
    let host = config
        .worker
        .log_address
        .clone()
        .or_else(|| env.get("HOSTNAME").cloned())
        .unwrap_or_else(|| "localhost".to_owned());
    // An IPv6 address goes in brackets.
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host
    };
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{host}:{port}")
}

/// The role's TLS material by `[tls]`: none in `mesh` mode; in `app` mode the files, which
/// must be configured and usable.
///
/// # Errors
///
/// [`AppError::BadArgument`] when a file isn't configured, or can't be read or used.
pub fn role_tls(config: &Config) -> Result<Option<Tls>, AppError> {
    if config.tls.mode == TlsMode::Mesh {
        return Ok(None);
    }
    if config.tls.reload_interval.is_zero() {
        return Err(AppError::BadArgument(
            "[tls] reload_interval must be longer than 0s (e.g. \"30s\")".to_owned(),
        ));
    }
    let file = |path: &Option<PathBuf>, key: &str| {
        path.clone().ok_or_else(|| {
            AppError::BadArgument(format!(
                "[tls] mode is `app` (the default), which needs [tls] {key} \
                 (SCHEDR_CONFIG__TLS__{}); or set [tls] mode = \"mesh\" behind a service mesh \
                 that encrypts the traffic between pods",
                key.to_uppercase()
            ))
        })
    };
    let files = TlsFiles {
        ca: file(&config.tls.ca, "ca")?,
        cert: file(&config.tls.cert, "cert")?,
        key: file(&config.tls.key, "key")?,
    };
    Tls::load(files, &config.tls.worker_name)
        .map(Some)
        .map_err(|e| AppError::BadArgument(e.to_string()))
}

/// Reads the role's certificates again every `[tls] reload_interval` until `stop`.
fn watch_tls(tls: Option<&Tls>, config: &Config, stop: &watch::Receiver<u32>) {
    if let Some(tls) = tls {
        tokio::spawn(
            tls.clone()
                .watch(config.tls.reload_interval, until(stop.clone())),
        );
    }
}

/// `name` as one safe path component, different for different names: bytes other than
/// letters, digits, `-`, `_` and `.` become `%XX`, and so does a leading `.`; an empty name is
/// `%`.
fn file_name(name: &str) -> String {
    use std::fmt::Write as _;
    if name.is_empty() {
        return "%".to_owned();
    }
    let mut safe = String::new();
    for (i, b) in name.bytes().enumerate() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || (b == b'.' && i > 0) {
            safe.push(char::from(b));
        } else {
            let _ = write!(safe, "%{b:02X}");
        }
    }
    safe
}

/// Takes the worker's journal lock for as long as the returned file is open: one agent per
/// worker ID and data directory.
///
/// # Errors
///
/// [`AppError::BadArgument`] if another agent holds it, or it can't be created.
pub fn lock_journal(agent: &AgentSettings) -> Result<std::fs::File, AppError> {
    let dir = &agent.journal_dir;
    Journal::new(dir.clone()).lock().map_err(|e| {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            AppError::BadArgument(format!(
                "another schedr worker `{}` is using {}: give each worker on this host its own \
                 [worker] id or data_dir",
                agent.worker_id,
                dir.display()
            ))
        } else {
            AppError::BadArgument(format!(
                "can't lock the journal in {}: {e} (the data directory must be writable)",
                dir.display()
            ))
        }
    })
}

/// Binds a TCP listener.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the address can't be bound.
pub async fn listen(address: SocketAddr) -> Result<TcpListener, AppError> {
    TcpListener::bind(address)
        .await
        .map_err(|e| AppError::BadArgument(format!("can't listen on {address}: {e}")))
}

/// Serves the master: its gRPC service on `listener` (over mTLS with `tls`) and its loop,
/// until `stop`.
///
/// # Errors
///
/// [`AppError`] if the gRPC server fails.
pub async fn serve_master(
    db: Database,
    settings: Settings,
    tick: Duration,
    listener: TcpListener,
    tls: Option<Tls>,
    observed: Observed<'_, MasterMetrics>,
    stop: watch::Receiver<u32>,
) -> Result<(), AppError> {
    let connections = incoming(listener, tls, Protocol::Grpc)
        .map_err(|e| AppError::BadArgument(format!("gRPC server: {e}")))?;
    let notifications = match db.command_notifications().await {
        Ok(notifications) => notifications,
        Err(error) => {
            tracing::warn!(%error, "can't listen for commands; polling every tick");
            None
        }
    };
    let hub = Hub::new(MasterCore::new(db, settings).with_metrics(observed.metrics))
        .with_refresh_limit(observed.health.stall() / 2);
    observed.health.watch_loop("scheduling loop", hub.beat());
    let restored = hub.restore().await.map_err(|e| match e {
        CoreError::Db(db) => AppError::Db(db),
        other => AppError::BadArgument(format!("can't read the worker registry: {other}")),
    })?;
    if restored > 0 {
        tracing::info!(
            restored,
            "workers restored as disconnected; their leases start now"
        );
    }
    observed.ready.ready();
    let grpc = tonic::transport::Server::builder()
        .add_service(hub.service())
        .serve_with_incoming_shutdown(connections, until(stop.clone()));
    let master_loop = hub.run(tick, notifications, until(stop));
    tokio::pin!(grpc, master_loop);
    // The server ends first only by failing (both end together on `stop`): the role must
    // then end too, not go on with a loop no worker can reach.
    tokio::select! {
        result = &mut grpc => {
            result.map_err(|e| AppError::BadArgument(format!("gRPC server: {e}")))
        }
        () = &mut master_loop => {
            grpc.await.map_err(|e| AppError::BadArgument(format!("gRPC server: {e}")))
        }
    }
}

/// Serves the REST API on `listener` until `stop`, reading task logs from workers (over mTLS
/// with `tls`).
///
/// # Errors
///
/// [`AppError::BadArgument`] if the HTTP server fails.
pub async fn serve_api(
    db: Database,
    identity_header: Option<String>,
    listener: TcpListener,
    tls: Option<Tls>,
    observed: Observed<'_, ApiMetrics>,
    stop: watch::Receiver<u32>,
) -> Result<(), AppError> {
    let state = ApiState::new(db, identity_header)
        .with_logs(std::sync::Arc::new(HttpLogSource::new(tls)))
        .with_metrics(observed.metrics);
    observed.ready.ready();
    axum::serve(listener, router(state))
        .with_graceful_shutdown(until(stop))
        .await
        .map_err(|e| AppError::BadArgument(format!("HTTP server: {e}")))
}

/// How a part of a role is observed: its metrics (registered by the caller), the health it
/// reports its loops to, and the readiness it sets once started.
#[derive(Debug)]
pub struct Observed<'a, M> {
    /// The part's metrics.
    pub metrics: M,
    /// The role's health.
    pub health: &'a Health,
    /// The part's readiness.
    pub ready: Starting,
}

/// How a worker reaches the master and serves its logs.
#[derive(Debug)]
pub struct WorkerLinks {
    /// The master's gRPC URL.
    pub master_url: String,
    /// The TLS material (`app` mode), for both.
    pub tls: Option<Tls>,
    /// Where the log endpoint listens (`agent.log_endpoint` says how it is reached).
    pub log_listener: TcpListener,
    /// What logs to keep.
    pub retention: LogRetention,
}

/// Runs a worker agent connected to the master until it has drained after a stop request
/// (`stop` counts them; see [`schedr_agent::drain`]), with its log endpoint and log
/// retention beside it until then.
pub async fn run_worker(
    agent: AgentSettings,
    parse_file_timeout: Duration,
    links: WorkerLinks,
    observed: Observed<'_, AgentMetrics>,
    stop: watch::Receiver<u32>,
) {
    let WorkerLinks {
        master_url,
        tls,
        log_listener,
        retention,
    } = links;
    let endpoint = LogEndpoint {
        worker_id: agent.worker_id.clone(),
        log_dir: agent.setup.log_dir.clone(),
        journal: Journal::new(agent.journal_dir.clone()),
    };
    let (done, finished) = watch::channel(0);
    let logs = match incoming(log_listener, tls.clone(), Protocol::Http) {
        Ok(connections) => Some(tokio::spawn(
            axum::serve(connections, schedr_agent::logs::router(endpoint.clone()))
                .with_graceful_shutdown(until(finished.clone()))
                .into_future(),
        )),
        Err(error) => {
            tracing::error!(%error, "can't serve task logs");
            None
        }
    };
    let sweeping = tokio::spawn(sweep_logs(
        endpoint.log_dir,
        endpoint.journal,
        retention,
        observed.metrics.clone(),
        until(finished),
    ));
    let executor = ProcessExecutor {
        setup: agent.setup.clone(),
        parse_file_timeout,
        metrics: observed.metrics.clone(),
        drain_terminate_period: agent.drain.terminate_period,
    };
    let mut agent = Agent::new(agent, executor, uuid::Uuid::now_v7().to_string())
        .with_metrics(observed.metrics);
    observed.health.watch_loop("worker agent", agent.beat());
    observed.ready.ready();
    agent
        .run(
            move || {
                let url = master_url.clone();
                let tls = tls.clone();
                async move {
                    schedr_tls::channel(&url, tls.as_ref())
                        .await
                        .map(MasterClient::new)
                }
            },
            stop,
        )
        .await;
    let _ = done.send(1);
    let _ = sweeping.await;
    if let Some(Ok(Err(error))) = match logs {
        Some(logs) => Some(logs.await),
        None => None,
    } {
        tracing::warn!(%error, "the log endpoint failed");
    }
}

/// Opens the database of the master or the webserver: `[database] url` is required, SQLite is
/// for one host (it warns), and the schema must be usable by this binary.
///
/// # Errors
///
/// [`AppError::NoDatabase`] without a URL; [`AppError::Db`] if it can't be reached or its
/// schema is outdated.
pub async fn open_database(config: &Config, role: &str) -> Result<Database, AppError> {
    let url = config.database.url.as_deref().ok_or(AppError::NoDatabase)?;
    let db = Database::connect_with_timeout(url, config.database.query_timeout).await?;
    if db.backend() == Backend::Sqlite {
        tracing::warn!(
            "schedr {role} on SQLite: every role must run on this host, on a local disk; \
             use Postgres for anything distributed"
        );
    }
    db.require_usable_schema().await?;
    tracing::info!(database = %redact_password(url), "schedr {role}: database ready");
    Ok(db)
}

/// The warning for the REST API listening on `address` beyond loopback: it has no
/// authentication of its own until the UI (M3). `alternative` says how to keep it local.
#[must_use]
pub fn no_auth_warning(address: SocketAddr, alternative: &str) -> Option<String> {
    (!address.ip().is_loopback()).then(|| {
        format!(
            "the REST API has no authentication of its own (it comes with the UI, milestone \
             M3) and listens on {address}: keep it behind an authenticating proxy or mesh, or \
             {alternative}"
        )
    })
}

/// Checks `[worker] master_url` and returns its host: an `https://` URL with a host in `app`
/// mode (the master's certificate must name the host), an `http://` one in `mesh` mode.
///
/// # Errors
///
/// [`AppError::BadArgument`] for anything else, saying what is expected.
pub fn master_host(url: &str, mode: TlsMode) -> Result<String, AppError> {
    let (scheme, example, mode_name) = match mode {
        TlsMode::App => ("https", "https://schedr-master:8070", "app"),
        TlsMode::Mesh => ("http", "http://schedr-master:8070", "mesh"),
    };
    let bad = |why: &str| {
        AppError::BadArgument(format!(
            "[worker] master_url `{url}` {why}: give the master's gRPC URL, e.g. {example}"
        ))
    };
    let uri = url
        .parse::<tonic::transport::Uri>()
        .map_err(|_| bad("isn't a URL"))?;
    match uri.scheme_str() {
        Some(given) if given == scheme => {}
        Some("http" | "https") => {
            return Err(bad(&format!(
                "needs the {scheme}:// scheme with [tls] mode `{mode_name}`"
            )));
        }
        _ => return Err(bad(&format!("needs the {scheme}:// scheme"))),
    }
    uri.host()
        .map(str::to_owned)
        .ok_or_else(|| bad("has no host"))
}

/// A count of stop requests (Ctrl-C or SIGTERM): the first stops a role (a worker drains), a
/// second makes a draining worker interrupt its tasks at once. The SIGTERM handler is
/// registered before this returns, so a SIGTERM right after start still stops the role
/// gracefully.
#[must_use]
pub fn stop_on_signal() -> watch::Receiver<u32> {
    let (stop, stopped) = watch::channel(0);
    let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    tokio::spawn(async move {
        let mut terminate = terminate
            .map_err(|error| tracing::warn!(%error, "can't watch SIGTERM; Ctrl-C only"))
            .ok();
        let mut ctrl_c = true;
        loop {
            let sigterm = async {
                match &mut terminate {
                    Some(signal) => signal.recv().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                watched = tokio::signal::ctrl_c(), if ctrl_c => {
                    if let Err(error) = watched {
                        tracing::warn!(%error, "can't watch Ctrl-C; SIGTERM only");
                        ctrl_c = false;
                        continue;
                    }
                }
                Some(()) = sigterm => {}
            }
            stop.send_modify(|requests| *requests += 1);
        }
    });
    stopped
}

/// `schedr master`: the gRPC service and the loop, until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// As [`open_database`], [`listen`] and [`serve_master`].
pub async fn master(config: &Config) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let tls = role_tls(config)?;
    check_timeouts(config)?;
    let health = Health::new(config.ops.loop_stall_timeout);
    let ready = health.starting("master");
    let metrics = MasterMetrics::default();
    let mut registry = Registry::default();
    metrics.register(registry.sub_registry_with_prefix("schedr"));
    start_ops(config, "master", Ops::new(registry, health.clone()), &stop).await?;
    let db = open_database(config, "master").await?;
    let address = SocketAddr::new(config.master.bind, config.master.grpc_port);
    let listener = listen(address).await?;
    let bound = listener.local_addr().unwrap_or(address);
    tracing::info!(
        "schedr master: gRPC on {bound} ({})",
        links_text(tls.as_ref())
    );
    watch_tls(tls.as_ref(), config, &stop);
    serve_master(
        db,
        master_settings(config),
        config.master.tick_interval,
        listener,
        tls,
        Observed {
            metrics,
            health: &health,
            ready,
        },
        stop,
    )
    .await
}

/// Checks that `[database] query_timeout` is shorter than `[ops] loop_stall_timeout`, so one
/// hung query doesn't look like a stuck loop.
///
/// # Errors
///
/// [`AppError::BadArgument`] when it isn't.
pub fn check_timeouts(config: &Config) -> Result<(), AppError> {
    // Below 1 ms it would be 0 for Postgres: no limit at all.
    if config.database.query_timeout >= config.ops.loop_stall_timeout
        || config.database.query_timeout.as_millis() == 0
    {
        return Err(AppError::BadArgument(format!(
            "[database] query_timeout ({} s) must be at least 1 ms and shorter than [ops] \
             loop_stall_timeout ({} s)",
            config.database.query_timeout.as_secs_f64(),
            config.ops.loop_stall_timeout.as_secs_f64()
        )));
    }
    Ok(())
}

/// Binds the ops port (`[ops] bind` : `port`) and serves `ops` there until `stop`, logging
/// the address as `schedr <role>: ops on http://<address>`.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the address can't be bound.
pub async fn start_ops(
    config: &Config,
    role: &str,
    ops: Ops,
    stop: &watch::Receiver<u32>,
) -> Result<SocketAddr, AppError> {
    let address = SocketAddr::new(config.ops.bind, config.ops.port);
    let listener = listen(address).await?;
    let bound = listener.local_addr().unwrap_or(address);
    tracing::info!("schedr {role}: {}", ops_line(bound));
    let stop = stop.clone();
    tokio::spawn(async move {
        if let Err(error) = serve_ops(listener, ops, stop).await {
            tracing::error!(%error, "the ops port failed");
        }
    });
    Ok(bound)
}

/// How a role's internal links are protected, for its startup line.
fn links_text(tls: Option<&Tls>) -> &'static str {
    if tls.is_some() {
        "mTLS"
    } else {
        "plain: tls mode `mesh`"
    }
}

/// `schedr webserver`: the REST API, until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// As [`open_database`], [`listen`] and [`serve_api`].
pub async fn webserver(config: &Config) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let tls = role_tls(config)?;
    check_timeouts(config)?;
    let health = Health::new(config.ops.loop_stall_timeout);
    let ready = health.starting("webserver");
    let metrics = ApiMetrics::default();
    let mut registry = Registry::default();
    metrics.register(registry.sub_registry_with_prefix("schedr"));
    start_ops(
        config,
        "webserver",
        Ops::new(registry, health.clone()),
        &stop,
    )
    .await?;
    let db = open_database(config, "webserver").await?;
    let address = SocketAddr::new(config.webserver.bind, config.webserver.port);
    let listener = listen(address).await?;
    let bound = listener.local_addr().unwrap_or(address);
    if let Some(warning) = no_auth_warning(bound, "set [webserver] bind = \"127.0.0.1\"") {
        tracing::warn!("{warning}");
    }
    tracing::info!(
        "schedr webserver: API on http://{bound} (worker logs: {})",
        links_text(tls.as_ref())
    );
    watch_tls(tls.as_ref(), config, &stop);
    serve_api(
        db,
        config.webserver.auth.identity_header.clone(),
        listener,
        tls,
        Observed {
            metrics,
            health: &health,
            ready,
        },
        stop,
    )
    .await
}

/// `schedr worker`: the agent connected to `[worker] master_url`, until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// [`AppError::BadArgument`] without a master URL, a data directory or a usable DAG
/// interpreter.
pub async fn worker(config: &Config, env: &BTreeMap<String, String>) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let master_url = config.worker.master_url.clone().ok_or_else(|| {
        AppError::BadArgument(
            "no master to connect to: set [worker] master_url \
             (SCHEDR_CONFIG__WORKER__MASTER_URL), e.g. https://schedr-master:8070"
                .to_owned(),
        )
    })?;
    master_host(&master_url, config.tls.mode)?;
    let tls = role_tls(config)?;
    let health = Health::new(config.ops.loop_stall_timeout);
    let ready = health.starting("worker");
    let metrics = AgentMetrics::default();
    let mut registry = Registry::default();
    metrics.register(registry.sub_registry_with_prefix("schedr"));
    start_ops(config, "worker", Ops::new(registry, health.clone()), &stop).await?;
    let config = role_config(config, env, false)?;
    let data_dir = data_dir(&config, env)?;
    tokio::fs::create_dir_all(&data_dir)
        .await
        .map_err(|e| AppError::BadArgument(format!("can't create {}: {e}", data_dir.display())))?;
    let mut agent = agent_settings(&config, env, &data_dir)?;
    let _lock = lock_journal(&agent)?;
    let address = SocketAddr::new(config.worker.log_bind, config.worker.log_port);
    let log_listener = listen(address).await?;
    let port = log_listener
        .local_addr()
        .map_or(address.port(), |a| a.port());
    agent.log_endpoint = log_endpoint(&config, env, tls.is_some(), port);
    tracing::info!(
        "schedr worker {}: master {master_url} ({}), {}, data in {}, logs on {}",
        agent.worker_id,
        links_text(tls.as_ref()),
        memory_text(&agent.setup.memory),
        data_dir.display(),
        agent.log_endpoint
    );
    watch_tls(tls.as_ref(), &config, &stop);
    let links = WorkerLinks {
        master_url,
        tls,
        log_listener,
        retention: log_retention(&config),
    };
    let observed = Observed {
        metrics,
        health: &health,
        ready,
    };
    run_worker(agent, config.dags.parse_file_timeout, links, observed, stop).await;
    Ok(())
}
