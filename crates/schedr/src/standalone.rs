//! `schedr standalone`: every role in one process, on localhost, with SQLite by default (see
//! PROJECT.md, Master–worker protocol: standalone uses the same gRPC protocol).
//!
//! The master's gRPC service, the REST API, the master's loop and one worker agent with its log
//! endpoint run side by side; the agent talks to the master over gRPC on localhost like any
//! worker, and the API reads logs from the endpoint (no TLS: neither listens beyond
//! localhost, whatever `[tls]` says). `--migrate` migrates the database at start (a laptop);
//! without it, a schema that isn't up to date is refused (production migrates as a step of its
//! own, `schedr db migrate`). When the master fails (another took the lease over), the whole
//! process ends, its tasks interrupted: restarted, it waits as a standby.
//!
//! In one pod (for testing on a cluster, see PROJECT.md): `--listen 0.0.0.0` opens the REST
//! API to a Service, SIGTERM stops it cleanly, and a lock in the data directory refuses a
//! second standalone on the same data (two pods on one volume would schedule every run twice).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use prometheus_client::registry::Registry;
use schedr_agent::agent::AgentSettings;
use schedr_agent::memory::STANDALONE_RESERVE;
use schedr_agent::metrics::AgentMetrics;
use schedr_agent::retention::LogRetention;
use schedr_api::metrics::ApiMetrics;
use schedr_config::Config;
use schedr_db::Database;
use schedr_master::core::Settings;
use schedr_master::lease::Leadership;
use schedr_master::metrics::MasterMetrics;
use tokio::sync::watch;

use crate::app::{AppError, describe_status};
use crate::ops::{Health, Ops, ops_line, serve_ops};
use crate::roles::{
    Observed, WorkerLinks, agent_settings, check_timeouts, data_dir, listen, lock_journal,
    log_retention, master_settings, memory_text, migrate_holding, no_auth_warning, run_worker,
    serve_api, serve_master, stop_on_signal, take_lease, unlimited, until,
};

#[cfg(test)]
mod tests;

/// Everything `schedr standalone` runs, read from the configuration.
#[derive(Debug, Clone)]
pub struct Standalone {
    /// Where logs, the worker's journal and the SQLite database go.
    pub data_dir: PathBuf,
    /// The database (`[database] url`, else SQLite in the data directory).
    pub database_url: String,
    /// The master's gRPC address.
    pub grpc: SocketAddr,
    /// The REST API's address.
    pub http: SocketAddr,
    /// The worker's log endpoint's address.
    pub logs: SocketAddr,
    /// The ops port's address.
    pub ops: SocketAddr,
    /// How long a loop may go without turning before liveness fails.
    pub loop_stall_timeout: Duration,
    /// The longest a database query may take.
    pub query_timeout: Duration,
    /// `[database] notifications`: whether new commands are announced and listened for.
    pub notifications: bool,
    /// `[database] notifications_url`: what the master listens through, if not the database's
    /// own connection.
    pub notifications_url: Option<String>,
    /// The master's lease (one master per database).
    pub lease_period: Duration,
    /// What logs the worker keeps.
    pub retention: LogRetention,
    /// The master's timing.
    pub master: Settings,
    /// How often the master's loop runs.
    pub tick: Duration,
    /// The worker agent.
    pub agent: AgentSettings,
    /// How long one DAG file may take to import.
    pub parse_file_timeout: Duration,
    /// The header naming the user on commands.
    pub identity_header: Option<String>,
    /// Whether to migrate the database at start (`--migrate`).
    pub migrate: bool,
}

/// The addresses standalone listens on, once it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Listening {
    /// The master's gRPC address.
    pub grpc: SocketAddr,
    /// The REST API's address.
    pub http: SocketAddr,
    /// The worker's log endpoint's address.
    pub logs: SocketAddr,
    /// The ops port's address.
    pub ops: SocketAddr,
}

/// Reads the standalone setup from the configuration and the environment (`env` gives the
/// data directory's default, the host name and what tasks may inherit).
///
/// # Errors
///
/// [`AppError::BadArgument`] when there is no data directory to use, or the DAG interpreter
/// isn't defined.
pub fn plan(config: &Config, env: &BTreeMap<String, String>) -> Result<Standalone, AppError> {
    check_timeouts(config)?;
    let data_dir = data_dir(config, env)?;
    let database_url = config
        .database
        .url
        .clone()
        .unwrap_or_else(|| format!("sqlite://{}", data_dir.join("schedr.db").display()));
    let localhost = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    Ok(Standalone {
        grpc: localhost(config.master.grpc_port),
        http: localhost(config.webserver.port),
        logs: localhost(config.worker.log_port),
        ops: localhost(config.ops.port),
        loop_stall_timeout: config.ops.loop_stall_timeout,
        query_timeout: config.database.query_timeout,
        notifications: config.database.notifications,
        notifications_url: config.database.notifications_url.clone(),
        lease_period: config.master.lease_period,
        retention: log_retention(config),
        master: master_settings(config),
        tick: config.master.tick_interval,
        agent: agent_settings(config, env, &data_dir, STANDALONE_RESERVE)?,
        parse_file_timeout: config.dags.parse_file_timeout,
        identity_header: config.webserver.auth.identity_header.clone(),
        migrate: false,
        database_url,
        data_dir,
    })
}

/// Runs standalone until asked to stop (`shutdown` counts the requests): its worker drains
/// first (a second request interrupts its tasks at once), then the master and the API stop,
/// so interrupted tasks are recorded; `listening` gets the addresses once bound. A failing
/// master ends it too, its worker's tasks interrupted at once (nothing can record them).
///
/// # Errors
///
/// [`AppError`] if the data directory, the database or a port can't be set up, the schema
/// isn't up to date (without `migrate`), or the master fails.
pub async fn run(
    setup: Standalone,
    listening: impl FnOnce(Listening),
    shutdown: watch::Receiver<u32>,
) -> Result<(), AppError> {
    let io = |what: &str, e: std::io::Error| AppError::BadArgument(format!("{what}: {e}"));
    tokio::fs::create_dir_all(&setup.data_dir)
        .await
        .map_err(|e| io(&format!("can't create {}", setup.data_dir.display()), e))?;
    let _lock = lock_data_dir(&setup.data_dir)?;
    let _journal_lock = lock_journal(&setup.agent)?;
    let grpc = listen(setup.grpc).await?;
    let http = listen(setup.http).await?;
    let logs = listen(setup.logs).await?;
    let ops = listen(setup.ops).await?;
    let addresses = Listening {
        ops: ops.local_addr().map_err(|e| io("ops address", e))?,
        grpc: grpc.local_addr().map_err(|e| io("gRPC address", e))?,
        http: http.local_addr().map_err(|e| io("HTTP address", e))?,
        logs: logs
            .local_addr()
            .map_err(|e| io("log endpoint address", e))?,
    };
    listening(addresses);
    let mut agent = setup.agent;
    agent.log_endpoint = format!("http://{}", addresses.logs);

    // The master and the API stop once the worker has drained.
    let (stop, stopped) = watch::channel(0);
    // One ops port for every part.
    let health = Health::new(setup.loop_stall_timeout);
    let (registry, master_observed, api_observed, agent_observed) = observed(&health);

    let ops_server = tokio::spawn(serve_ops(
        ops,
        Ops::new(registry, health.clone()),
        stopped.clone(),
    ));
    // The ops port answers (`/readyz`: starting) meanwhile. One master per database: an
    // up-to-date schema lets the API and the worker start while the master waits for the
    // lease (a standby); with `--migrate`, one to migrate is migrated by the lease's holder.
    let db = Database::connect_with_timeout(&setup.database_url, setup.query_timeout)
        .await?
        .with_notifications(setup.notifications, setup.notifications_url.clone());
    let leading = lead(
        &db,
        &setup.database_url,
        (setup.lease_period, setup.migrate),
        shutdown.clone(),
    );
    let Some(leadership) = leading.await? else {
        return Ok(());
    };
    // The worker's stop requests: the signals', and the master's failure (two at once: its
    // tasks interrupted, nothing could record them).
    let (request, requests) = watch::channel(0);
    let forwarding = tokio::spawn(forward_requests(shutdown, request.clone()));
    // Boxed: the roles' futures are large.
    let master_db = db.clone();
    let master = Box::pin(async {
        let served = Box::pin(serve_master(
            master_db,
            setup.master,
            setup.tick,
            grpc,
            None,
            leadership,
            master_observed,
            stopped.clone(),
        ))
        .await;
        if let Err(error) = &served {
            tracing::error!(%error, "the master failed: standalone stops");
            request.send_modify(|requests| *requests = (*requests).max(2));
        }
        served
    });
    let api = Box::pin(serve_api(
        db,
        setup.identity_header.clone(),
        http,
        None,
        api_observed,
        stopped.clone(),
    ));
    let links = WorkerLinks {
        master_url: format!("http://{}", addresses.grpc),
        tls: None,
        log_listener: logs,
        retention: setup.retention,
    };
    let worker = Box::pin(async move {
        run_worker(
            agent,
            setup.parse_file_timeout,
            links,
            agent_observed,
            requests,
        )
        .await;
        let _ = stop.send(1);
    });
    let (master, api, ()) = tokio::join!(master, api, worker);
    forwarding.abort();
    if let Ok(Err(error)) = ops_server.await {
        tracing::warn!(%error, "the ops port failed");
    }
    master?;
    api
}

/// The ops port's registry with every part's metrics, and what each part reports to it.
fn observed(
    health: &Health,
) -> (
    Registry,
    Observed<'_, MasterMetrics>,
    Observed<'_, ApiMetrics>,
    Observed<'_, AgentMetrics>,
) {
    let (master_metrics, api_metrics, agent_metrics) = (
        MasterMetrics::default(),
        ApiMetrics::default(),
        AgentMetrics::default(),
    );
    let mut registry = Registry::default();
    let schedr = registry.sub_registry_with_prefix("schedr");
    master_metrics.register(schedr);
    api_metrics.register(schedr);
    agent_metrics.register(schedr);
    let part = |name| health.starting(name);
    (
        registry,
        Observed {
            metrics: master_metrics,
            health,
            ready: part("master"),
        },
        Observed {
            metrics: api_metrics,
            health,
            ready: part("webserver"),
        },
        Observed {
            metrics: agent_metrics,
            health,
            ready: part("worker"),
        },
    )
}

/// The master's lease, being taken (see [`take_lease`]).
type LeaseFuture = std::pin::Pin<Box<dyn Future<Output = Leadership> + Send>>;

/// The master's lease, being taken while the API and the worker start when the schema is
/// usable; else, with `--migrate`, taken now (a standby waits) and the schema migrated by its
/// holder. `None` when a shutdown is asked for while waiting.
///
/// # Errors
///
/// [`AppError::BadArgument`] for a schema to migrate without `--migrate`, as
/// [`migrate_holding`], or [`AppError::Db`] if the schema can't be read.
async fn lead(
    db: &Database,
    url: &str,
    (period, migrate): (Duration, bool),
    shutdown: watch::Receiver<u32>,
) -> Result<Option<LeaseFuture>, AppError> {
    let status = db.schema_status().await?;
    if status.is_usable() {
        return Ok(Some(Box::pin(take_lease(db.clone(), period, "standalone"))));
    }
    if !migrate {
        return Err(AppError::BadArgument(format!(
            "the database schema is {}, or start standalone with `--migrate`",
            describe_status(&status)
        )));
    }
    let lease = take_lease(db.clone(), period, "standalone");
    let Some(lead) = until_stopped(lease, shutdown).await else {
        return Ok(None);
    };
    let lead = migrate_holding(lead, &unlimited(url, db).await?).await?;
    Ok(Some(Box::pin(std::future::ready(lead))))
}

/// `work`'s output, or `None` once a shutdown is asked for.
async fn until_stopped<T>(work: impl Future<Output = T>, stop: watch::Receiver<u32>) -> Option<T> {
    tokio::select! {
        done = work => Some(done),
        () = until(stop) => None,
    }
}

/// Passes the stop requests counted by `from` on to `to` (never lowering its count).
async fn forward_requests(mut from: watch::Receiver<u32>, to: watch::Sender<u32>) {
    loop {
        let requests = *from.borrow_and_update();
        to.send_modify(|count| *count = (*count).max(requests));
        if from.changed().await.is_err() {
            return;
        }
    }
}

/// The name of the lock file in the data directory.
pub const LOCK_FILE: &str = "standalone.lock";

/// Takes the data directory's lock for as long as the returned file is open: one standalone
/// per data directory (and its SQLite database).
///
/// # Errors
///
/// [`AppError::BadArgument`] if another process holds it, or the file can't be opened.
pub fn lock_data_dir(data_dir: &Path) -> Result<std::fs::File, AppError> {
    let path = data_dir.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| {
            let hint = if e.kind() == std::io::ErrorKind::PermissionDenied {
                ": the data directory must be writable by this user (on Kubernetes, give the \
                 pod an fsGroup the user belongs to)"
            } else {
                ""
            };
            AppError::BadArgument(format!("can't open {}: {e}{hint}", path.display()))
        })?;
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
        |errno| {
            if errno == rustix::io::Errno::WOULDBLOCK {
                AppError::BadArgument(format!(
                    "another schedr standalone is using {} (it holds {}): one standalone per \
                     data directory; on Kubernetes run one replica (a StatefulSet, or a \
                     Deployment with strategy Recreate)",
                    data_dir.display(),
                    path.display()
                ))
            } else {
                AppError::BadArgument(format!(
                    "can't lock {}: {errno} (a network filesystem? the data directory needs a \
                     local disk or block storage)",
                    path.display()
                ))
            }
        },
    )?;
    Ok(file)
}

/// The line standalone prints once it listens.
#[must_use]
pub fn banner(listening: Listening, setup: &Standalone) -> String {
    format!(
        "schedr standalone: API on http://{}, master on {}, {}, data in {}",
        listening.http,
        listening.grpc,
        ops_line(listening.ops),
        setup.data_dir.display()
    )
}

/// Runs `schedr standalone` until Ctrl-C or SIGTERM, the REST API on `listen` (127.0.0.1 when
/// not given); the banner goes to the log once it listens.
///
/// # Errors
///
/// As [`plan`] and [`run`].
pub async fn command(
    config: &Config,
    env: &BTreeMap<String, String>,
    listen: Option<IpAddr>,
    migrate: bool,
) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let mut setup = plan(config, env)?;
    setup.migrate = migrate;
    if let Some(address) = listen {
        // Probes and scrapes come from outside the pod too.
        setup.http.set_ip(address);
        setup.ops.set_ip(address);
    }
    let shown = setup.clone();
    run(
        setup,
        |listening| {
            if let Some(warning) =
                no_auth_warning(listening.http, "reach it with kubectl port-forward")
            {
                tracing::warn!("{warning}");
            }
            tracing::info!("{}", banner(listening, &shown));
            tracing::info!("{}", memory_text(&shown.agent.setup.memory));
        },
        stop,
    )
    .await
}
