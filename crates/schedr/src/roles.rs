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
use schedr_agent::memory::{
    MIN_BUDGET, MemoryWatch, Reserve, WORKER_RESERVE, budget, cgroup_limit, show_size,
};
use schedr_agent::metrics::AgentMetrics;
use schedr_agent::retention::{LogRetention, sweep_logs};
use schedr_api::logs::HttpLogSource;
use schedr_api::metrics::ApiMetrics;
use schedr_api::{ApiState, router};
use schedr_config::{Config, MemoryReserve, TlsMode};
use schedr_db::{Backend, Database, redact_password};
use schedr_master::core::{CoreError, MasterCore, Settings};
use schedr_master::lease::{Leadership, Renewal};
use schedr_master::metrics::MasterMetrics;
use schedr_master::parse_jobs::PARSE_JOB_TIMEOUT;
use schedr_master::service::{Hub, LeaseLost};
use schedr_proto::pb::master_client::MasterClient;
use schedr_tls::{Protocol, Tls, TlsFiles, incoming};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::app::{AppError, describe_status, role_config};

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
        set_aside: config.master.set_aside,
        read_back: schedr_master::dispatch::ReadBack {
            window: config.master.read_back_window,
            interval: config.master.read_back_interval,
        },
        listen_retry: config.master.listen_retry,
        quiet: schedr_master::quiet::QuietSettings {
            enabled: config.master.quiet_dags,
            after: config.master.quiet_dag_after,
            wake_ahead: config.master.quiet_dag_wake_ahead,
            full_pass_interval: config.master.quiet_dag_full_pass_interval,
        },
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
/// data directory never see each other's (retention, the log endpoint). Without `[worker]
/// memory_reserve`, the task budgets leave `default_reserve` of the pod's limit out.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the DAG interpreter isn't defined, the memory check interval
/// is 0, or the memory budget (set, or what the reserve leaves) below [`MIN_BUDGET`].
pub fn agent_settings(
    config: &Config,
    env: &BTreeMap<String, String>,
    data_dir: &Path,
    default_reserve: Reserve,
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
    let limit = cgroup_limit(Path::new("/proc"), Path::new("/sys/fs/cgroup"))
        .ok()
        .flatten();
    let memory = memory_watch(config, limit, default_reserve);
    if config.worker.memory_budget.is_none()
        && let Some(budget) = memory.budget
        && budget < MIN_BUDGET
    {
        return Err(AppError::BadArgument(format!(
            "[worker] memory_reserve leaves {} per task of the pod's {} limit: give at least {} \
             (a smaller reserve, fewer slots, or more memory)",
            show_size(budget),
            show_size(limit.unwrap_or_default()),
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
            memory,
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

/// How the memory watchdog watches tasks: `[worker] memory_budget`, else (`limit` − the
/// reserve) ÷ slots, the reserve `[worker] memory_reserve` or `default_reserve`; without a
/// budget or a limit it only measures.
#[must_use]
pub fn memory_watch(config: &Config, limit: Option<u64>, default_reserve: Reserve) -> MemoryWatch {
    let reserve = config
        .worker
        .memory_reserve
        .map_or(default_reserve, |reserve| match reserve {
            MemoryReserve::Percent(percent) => Reserve::Percent(percent),
            MemoryReserve::Size(bytes) => Reserve::Bytes(bytes),
        });
    MemoryWatch {
        budget: budget(
            config.worker.memory_budget,
            limit,
            config.worker.slots,
            reserve,
        ),
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
/// until `stop`, once `leadership` has taken the lease (a standby waits there).
///
/// # Errors
///
/// [`AppError`] if the gRPC server fails, the lease can't be taken, or another master took it
/// over.
#[expect(clippy::too_many_arguments, reason = "the parts of one role, named")]
pub async fn serve_master(
    db: Database,
    settings: Settings,
    tick: Duration,
    listener: TcpListener,
    tls: Option<Tls>,
    leadership: impl Future<Output = Leadership>,
    observed: Observed<'_, MasterMetrics>,
    stop: watch::Receiver<u32>,
) -> Result<(), AppError> {
    // Nothing of the master's before the lease: a standby waits here, its readiness
    // "starting".
    let leadership = tokio::select! {
        taken = leadership => taken,
        () = until(stop.clone()) => return Ok(()),
    };
    tracing::info!(
        epoch = leadership.epoch(),
        holder = leadership.holder(),
        "schedr master: leading (the lease is this master's)"
    );
    let connections = incoming(listener, tls, Protocol::Grpc)
        .map_err(|e| AppError::BadArgument(format!("gRPC server: {e}")))?;
    let listen_retry = settings.listen_retry;
    let (notifications, retry) = match db.command_notifications().await {
        Ok(notifications) => (notifications, false),
        Err(error) => {
            tracing::warn!(
                %error,
                "can't listen for commands; polling every tick, trying again in {} min",
                listen_retry.as_secs() / 60
            );
            (None, true)
        }
    };
    // Its writes through the lease: refused once another master took it over.
    let mut hub =
        Hub::new(MasterCore::new(leadership.fenced_db(), settings).with_metrics(observed.metrics))
            .with_refresh_limit(observed.health.stall() / 2);
    if retry {
        hub = hub.with_listen_retry(listen_retry);
    }
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
    let master_loop = hub.run_leading(tick, notifications, leadership, until(stop));
    tokio::pin!(grpc, master_loop);
    // The server ends first only by failing (both end together on `stop`): the role must
    // then end too, not go on with a loop no worker can reach.
    tokio::select! {
        result = &mut grpc => {
            result.map_err(|e| AppError::BadArgument(format!("gRPC server: {e}")))
        }
        ended = &mut master_loop => {
            ended.map_err(|lost| AppError::BadArgument(lost.to_string()))?;
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

/// Connects to the database of the master or the webserver, whatever its schema:
/// `[database] url` is required, SQLite is for one host (it warns).
///
/// # Errors
///
/// [`AppError::NoDatabase`] without a URL; [`AppError::Db`] if it can't be reached.
pub async fn connect_database(config: &Config, role: &str) -> Result<Database, AppError> {
    let url = config.database.url.as_deref().ok_or(AppError::NoDatabase)?;
    let db = Database::connect_with_timeout(url, config.database.query_timeout)
        .await?
        .with_notifications(
            config.database.notifications,
            config.database.notifications_url.clone(),
        );
    if db.backend() == Backend::Sqlite {
        tracing::warn!(
            "schedr {role} on SQLite: every role must run on this host, on a local disk; \
             use Postgres for anything distributed"
        );
    }
    tracing::info!(database = %redact_password(url), "schedr {role}: database ready");
    Ok(db)
}

/// How often a webserver checks again for a schema it can use.
pub const SCHEMA_RETRY: Duration = Duration::from_secs(5);

/// Waits until `db`'s schema is usable by this binary, checking every `retry` and saying what
/// it waits for whenever that changes (the database unreachable, then a schema to migrate, …;
/// the master, or `schedr db migrate`, migrates it), so a webserver started before the
/// migration serves once it is done instead of failing.
pub async fn wait_for_schema(db: &Database, role: &str, retry: Duration) {
    let mut said = String::new();
    loop {
        let (waiting, error) = match db.schema_status().await {
            Ok(status) if status.is_usable() => return,
            Ok(status) => (describe_status(&status), false),
            Err(error) => (format!("unreadable ({error})"), true),
        };
        if waiting != said {
            let line = format!(
                "schedr {role}: the database schema is {waiting}; waiting for it (checked every \
                 {} s)",
                retry.as_secs()
            );
            if error {
                tracing::warn!("{line}");
            } else {
                tracing::info!("{line}");
            }
            said = waiting;
        }
        tokio::time::sleep(retry).await;
    }
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

/// Waits for the master's lease (see [`Leadership::take`]) as a new incarnation of the master
/// of `role`, logging once that it waits as a standby.
pub async fn take_lease(db: Database, period: Duration, role: &'static str) -> Leadership {
    let holder = uuid::Uuid::now_v7().to_string();
    Leadership::take(db, holder, period, move || {
        tracing::info!("schedr {role}: another master holds the lease; waiting as a standby");
    })
    .await
}

/// The master's lease, being taken (see [`take_lease`]).
pub type LeaseFuture = std::pin::Pin<Box<dyn Future<Output = Leadership> + Send>>;

/// The master's lease for `role` (`master` or `standalone`), being taken while the role's
/// other parts start when the schema is usable; else, with `--migrate`, taken now (a standby
/// waits) and the schema migrated by its holder. `None` when a shutdown is asked for while
/// waiting.
///
/// # Errors
///
/// [`AppError::BadArgument`] for a schema to migrate without `--migrate`, as
/// [`migrate_holding`], or [`AppError::Db`] if the schema can't be read.
pub async fn lead(
    db: &Database,
    url: &str,
    (period, migrate): (Duration, bool),
    shutdown: watch::Receiver<u32>,
    role: &'static str,
) -> Result<Option<LeaseFuture>, AppError> {
    let status = db.schema_status().await?;
    if status.is_usable() {
        return Ok(Some(Box::pin(take_lease(db.clone(), period, role))));
    }
    if !migrate {
        return Err(AppError::BadArgument(format!(
            "the database schema is {}, or start {role} with `--migrate`",
            describe_status(&status)
        )));
    }
    let lease = take_lease(db.clone(), period, role);
    let Some(lead) = until_stopped(lease, shutdown).await else {
        return Ok(None);
    };
    let lead = migrate_holding(lead, &unlimited(url, db).await?).await?;
    Ok(Some(Box::pin(std::future::ready(lead))))
}

/// `work`'s output, or `None` once a shutdown is asked for.
pub async fn until_stopped<T>(
    work: impl Future<Output = T>,
    stop: watch::Receiver<u32>,
) -> Option<T> {
    tokio::select! {
        done = work => Some(done),
        () = until(stop) => None,
    }
}

/// `schedr db migrate`: migrates the database at `url` holding the master's lease, so no master
/// runs on the schema meanwhile and no other migration runs at once (sqlx's own lock is a
/// session lock, which a pooler in transaction mode doesn't keep). Refused while a master
/// holds it, unless `wait`.
///
/// # Errors
///
/// [`AppError::BadArgument`] when a master holds the lease (and not `wait`), else as
/// [`migrate_holding`].
pub async fn migrate_leased(
    url: &str,
    query_timeout: Duration,
    period: Duration,
    wait: bool,
) -> Result<(), AppError> {
    let db = Database::connect_with_timeout(url, query_timeout).await?;
    let unlimited = unlimited(url, &db).await?;
    let holder = format!("migrate-{}", uuid::Uuid::now_v7());
    let lead = if wait {
        Leadership::take(db, holder, period, || {
            tracing::info!("a master holds the database's lease; waiting for it");
        })
        .await
    } else {
        Leadership::try_take(db, holder, period)
            .await?
            .ok_or_else(|| {
                AppError::BadArgument(
                    "a master holds the database's lease: stop it first, or wait for it with \
                 `schedr db migrate --wait`"
                        .to_owned(),
                )
            })?
    };
    let lead = migrate_holding(lead, &unlimited).await?;
    lead.release().await;
    Ok(())
}

/// The database at `url` without a query limit, for a migration (a long one must finish);
/// `limited` itself in memory, where another connection would be another database.
///
/// # Errors
///
/// [`AppError::Db`] if it can't be connected to.
pub async fn unlimited(url: &str, limited: &Database) -> Result<Database, AppError> {
    if url.contains(":memory:") {
        Ok(limited.clone())
    } else {
        Ok(Database::connect(url).await?)
    }
}

/// Migrates `db` (see [`unlimited`]) while holding `lead`, renewing it as the migration runs.
///
/// # Errors
///
/// [`AppError::Db`] if the migration fails, [`AppError::BadArgument`] if another master took
/// the lease over meanwhile.
pub async fn migrate_holding(mut lead: Leadership, db: &Database) -> Result<Leadership, AppError> {
    let migration = db.migrate_as_lease_holder();
    tokio::pin!(migration);
    loop {
        tokio::select! {
            migrated = &mut migration => {
                migrated?;
                return Ok(lead);
            }
            () = tokio::time::sleep(lead.period() / 3) => {
                if lead.renew(std::time::Instant::now()).await == Renewal::TakenOver {
                    return Err(AppError::BadArgument(LeaseLost.to_string()));
                }
            }
        }
    }
}

/// `schedr master`: the gRPC service and the loop, until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// As [`connect_database`], [`lead`], [`listen`] and [`serve_master`].
pub async fn master(config: &Config, migrate: bool) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let tls = role_tls(config)?;
    check_timeouts(config)?;
    let health = Health::new(config.ops.loop_stall_timeout);
    let ready = health.starting("master");
    let metrics = MasterMetrics::default();
    let mut registry = Registry::default();
    metrics.register(registry.sub_registry_with_prefix("schedr"));
    start_ops(config, "master", Ops::new(registry, health.clone()), &stop).await?;
    let db = connect_database(config, "master").await?;
    let url = config.database.url.as_deref().unwrap_or_default();
    let period = (config.master.lease_period, migrate);
    let Some(leadership) = lead(&db, url, period, stop.clone(), "master").await? else {
        return Ok(());
    };
    let address = SocketAddr::new(config.master.bind, config.master.grpc_port);
    let listener = listen(address).await?;
    let bound = listener.local_addr().unwrap_or(address);
    tracing::info!(
        "schedr master: gRPC on {bound} ({})",
        links_text(tls.as_ref())
    );
    watch_tls(tls.as_ref(), config, &stop);
    // Boxed: the role's future is large.
    Box::pin(serve_master(
        db,
        master_settings(config),
        config.master.tick_interval,
        listener,
        tls,
        leadership,
        Observed {
            metrics,
            health: &health,
            ready,
        },
        stop,
    ))
    .await
}

/// Checks that `[database] query_timeout` and `[master] lease_period` are shorter than `[ops]
/// loop_stall_timeout`, so one hung query doesn't look like a stuck loop, and a stuck master
/// doesn't lead past its restart.
///
/// # Errors
///
/// [`AppError::BadArgument`] when it isn't.
pub fn check_timeouts(config: &Config) -> Result<(), AppError> {
    // Below 1 ms every query would be cut off at once.
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
    // A paused master keeps its loop beating, but a lease longer than the stall limit would
    // leave a stuck one leading past its restart. A renewal (limited by the query limit) must
    // fit a third of it, and leading stops a query limit before it ends.
    if config.master.lease_period >= config.ops.loop_stall_timeout
        || config.master.lease_period < config.database.query_timeout * 3
    {
        return Err(AppError::BadArgument(format!(
            "[master] lease_period ({} s) must be at least three [database] query_timeout \
             ({} s) and shorter than [ops] loop_stall_timeout ({} s)",
            config.master.lease_period.as_secs_f64(),
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
/// As [`connect_database`], [`listen`] and [`serve_api`]. A schema it can't use yet is waited
/// for (see [`wait_for_schema`]), not ready meanwhile.
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
    let db = connect_database(config, "webserver").await?;
    let waiting = wait_for_schema(&db, "webserver", SCHEMA_RETRY);
    if until_stopped(waiting, stop.clone()).await.is_none() {
        return Ok(());
    }
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
    let mut agent = agent_settings(&config, env, &data_dir, WORKER_RESERVE)?;
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
