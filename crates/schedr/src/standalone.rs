//! `schedr standalone`: every role in one process, on localhost, with SQLite by default (see
//! PROJECT.md, Master–worker protocol: standalone uses the same gRPC protocol).
//!
//! The master's gRPC service, the REST API, the master's loop and one worker agent with its log
//! endpoint run side by side; the agent talks to the master over gRPC on localhost like any
//! worker, and the API reads logs from the endpoint (no TLS: neither listens beyond
//! localhost, whatever `[tls]` says). The database is migrated at start: standalone is for a
//! laptop, where there is no separate `schedr db migrate` step.
//!
//! In one pod (for testing on a cluster, see PROJECT.md): `--listen 0.0.0.0` opens the REST
//! API to a Service, SIGTERM stops it cleanly, and a lock in the data directory refuses a
//! second standalone on the same data (two pods on one volume would schedule every run twice).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use schedr_agent::agent::AgentSettings;
use schedr_agent::retention::LogRetention;
use schedr_config::Config;
use schedr_db::Database;
use schedr_master::core::Settings;
use tokio::sync::watch;

use crate::app::AppError;
use crate::roles::{
    WorkerLinks, agent_settings, data_dir, listen, lock_journal, log_retention, master_settings,
    memory_text, no_auth_warning, run_worker, serve_api, serve_master, stop_on_signal,
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
}

/// Reads the standalone setup from the configuration and the environment (`env` gives the
/// data directory's default, the host name and what tasks may inherit).
///
/// # Errors
///
/// [`AppError::BadArgument`] when there is no data directory to use, or the DAG interpreter
/// isn't defined.
pub fn plan(config: &Config, env: &BTreeMap<String, String>) -> Result<Standalone, AppError> {
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
        retention: log_retention(config),
        master: master_settings(config),
        tick: config.master.tick_interval,
        agent: agent_settings(config, env, &data_dir)?,
        parse_file_timeout: config.dags.parse_file_timeout,
        identity_header: config.webserver.auth.identity_header.clone(),
        database_url,
        data_dir,
    })
}

/// Runs standalone until asked to stop (`shutdown` counts the requests): its worker drains
/// first (a second request interrupts its tasks at once), then the master and the API stop,
/// so interrupted tasks are recorded; `listening` gets the addresses once bound.
///
/// # Errors
///
/// [`AppError`] if the data directory, the database or a port can't be set up.
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
    let db = Database::connect(&setup.database_url).await?;
    db.migrate().await?;

    let grpc = listen(setup.grpc).await?;
    let http = listen(setup.http).await?;
    let logs = listen(setup.logs).await?;
    let addresses = Listening {
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
    // Boxed: the roles' futures are large.
    let master = Box::pin(serve_master(
        db.clone(),
        setup.master,
        setup.tick,
        grpc,
        None,
        stopped.clone(),
    ));
    let api = Box::pin(serve_api(
        db,
        setup.identity_header.clone(),
        http,
        None,
        stopped.clone(),
    ));
    let links = WorkerLinks {
        master_url: format!("http://{}", addresses.grpc),
        tls: None,
        log_listener: logs,
        retention: setup.retention,
    };
    let worker = Box::pin(async move {
        run_worker(agent, setup.parse_file_timeout, links, shutdown).await;
        let _ = stop.send(1);
    });
    let (master, api, ()) = tokio::join!(master, api, worker);
    master?;
    api
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
        "schedr standalone: API on http://{}, master on {}, data in {}",
        listening.http,
        listening.grpc,
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
) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let mut setup = plan(config, env)?;
    if let Some(address) = listen {
        setup.http.set_ip(address);
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
