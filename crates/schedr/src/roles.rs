//! The server roles as processes of their own (M2.1): `schedr master`, `schedr webserver` and
//! `schedr worker`, and the pieces `schedr standalone` runs together in one process.
//!
//! - **master:** the gRPC service workers connect to and the master's loop, on the database
//!   (Postgres, or SQLite on one host); it refuses an outdated schema (`schedr db migrate`
//!   runs first) and picks commands up at once on Postgres (`LISTEN/NOTIFY`), else by polling.
//! - **webserver:** the REST API on the same database.
//! - **worker:** the agent, connected to `[worker] master_url`; no database.
//!
//! gRPC is plain text until mTLS (M2.7): a master listening beyond loopback, or a worker
//! connecting beyond it, says so at startup. SIGTERM and Ctrl-C stop a role (draining comes
//! with M2.6). See PROJECT.md, Architecture: Components and roles.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use schedr_agent::agent::{Agent, AgentSettings, ProcessExecutor};
use schedr_agent::attempt::WorkerSetup;
use schedr_agent::journal::Journal;
use schedr_api::{ApiState, router};
use schedr_config::Config;
use schedr_db::{Backend, Database, redact_password};
use schedr_master::core::{CoreError, MasterCore, Settings};
use schedr_master::parse_jobs::PARSE_JOB_TIMEOUT;
use schedr_master::service::Hub;
use schedr_proto::pb::master_client::MasterClient;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;

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
/// `<data_dir>/journal/<worker ID>` (other bytes than letters, digits, `-`, `_` and `.`, and a
/// leading `.`, as `%XX`).
///
/// # Errors
///
/// [`AppError::BadArgument`] if the DAG interpreter isn't defined.
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
            log_dir: data_dir.join("logs"),
        },
        poll_interval: config.dags.poll_interval,
        settle_time: config.dags.settle_time,
        journal_dir: data_dir.join("journal").join(file_name(&worker_id)),
        termination_grace_period: config.worker.termination_grace_period,
        worker_id,
    })
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

/// Serves the master: its gRPC service on `listener` and its loop, until `stop`.
///
/// # Errors
///
/// [`AppError`] if the gRPC server fails.
pub async fn serve_master(
    db: Database,
    settings: Settings,
    tick: Duration,
    listener: TcpListener,
    stop: watch::Receiver<u32>,
) -> Result<(), AppError> {
    let notifications = match db.command_notifications().await {
        Ok(notifications) => notifications,
        Err(error) => {
            tracing::warn!(%error, "can't listen for commands; polling every tick");
            None
        }
    };
    let hub = Hub::new(MasterCore::new(db, settings));
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
    let grpc = tonic::transport::Server::builder()
        .add_service(hub.service())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), until(stop.clone()));
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

/// Serves the REST API on `listener` until `stop`.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the HTTP server fails.
pub async fn serve_api(
    db: Database,
    identity_header: Option<String>,
    listener: TcpListener,
    stop: watch::Receiver<u32>,
) -> Result<(), AppError> {
    axum::serve(listener, router(ApiState::new(db, identity_header)))
        .with_graceful_shutdown(until(stop))
        .await
        .map_err(|e| AppError::BadArgument(format!("HTTP server: {e}")))
}

/// Runs a worker agent connected to the master at `master_url` until it has drained after a
/// stop request (`stop` counts them; see [`schedr_agent::drain`]).
pub async fn run_worker(
    agent: AgentSettings,
    parse_file_timeout: Duration,
    master_url: String,
    stop: watch::Receiver<u32>,
) {
    let executor = ProcessExecutor {
        setup: agent.setup.clone(),
        parse_file_timeout,
    };
    let mut agent = Agent::new(agent, executor, uuid::Uuid::now_v7().to_string());
    agent
        .run(
            move || {
                let url = master_url.clone();
                async move { MasterClient::connect(url).await.map_err(|e| e.to_string()) }
            },
            stop,
        )
        .await;
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
    let db = Database::connect(url).await?;
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

/// Whether `host` (a name, an IP address, an IPv6 address in brackets) is this machine only.
#[must_use]
pub fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The warning for plain-text gRPC to or from `host`, unless it is a loopback address.
#[must_use]
pub fn plain_text_warning(host: &str) -> Option<String> {
    (!is_loopback(host)).then(|| {
        format!(
            "gRPC to {host} is plain text until mTLS (milestone M2): keep it on a trusted \
             network"
        )
    })
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

/// Checks `[worker] master_url` and returns its host: an `http://` URL with a host (plain
/// text until mTLS, M2.7).
///
/// # Errors
///
/// [`AppError::BadArgument`] for anything else, saying what is expected.
pub fn master_host(url: &str) -> Result<String, AppError> {
    let bad = |why: &str| {
        AppError::BadArgument(format!(
            "[worker] master_url `{url}` {why}: give the master's gRPC URL, e.g. \
             http://schedr-master:8070"
        ))
    };
    let uri = url
        .parse::<tonic::transport::Uri>()
        .map_err(|_| bad("isn't a URL"))?;
    match uri.scheme_str() {
        Some("http") => {}
        Some("https") => return Err(bad("uses https, which comes with mTLS (milestone M2)")),
        _ => return Err(bad("needs the http:// scheme")),
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
    let db = open_database(config, "master").await?;
    let address = SocketAddr::new(config.master.bind, config.master.grpc_port);
    let listener = listen(address).await?;
    if let Some(warning) = plain_text_warning(&config.master.bind.to_string()) {
        tracing::warn!("{warning}");
    }
    let bound = listener.local_addr().unwrap_or(address);
    tracing::info!("schedr master: gRPC on {bound}");
    serve_master(
        db,
        master_settings(config),
        config.master.tick_interval,
        listener,
        stop,
    )
    .await
}

/// `schedr webserver`: the REST API, until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// As [`open_database`], [`listen`] and [`serve_api`].
pub async fn webserver(config: &Config) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let db = open_database(config, "webserver").await?;
    let address = SocketAddr::new(config.webserver.bind, config.webserver.port);
    let listener = listen(address).await?;
    let bound = listener.local_addr().unwrap_or(address);
    if let Some(warning) = no_auth_warning(bound, "set [webserver] bind = \"127.0.0.1\"") {
        tracing::warn!("{warning}");
    }
    tracing::info!("schedr webserver: API on http://{bound}");
    serve_api(
        db,
        config.webserver.auth.identity_header.clone(),
        listener,
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
             (SCHEDR_CONFIG__WORKER__MASTER_URL), e.g. http://schedr-master:8070"
                .to_owned(),
        )
    })?;
    let host = master_host(&master_url)?;
    let config = role_config(config, env, false)?;
    let data_dir = data_dir(&config, env)?;
    tokio::fs::create_dir_all(&data_dir)
        .await
        .map_err(|e| AppError::BadArgument(format!("can't create {}: {e}", data_dir.display())))?;
    let agent = agent_settings(&config, env, &data_dir)?;
    if let Some(warning) = plain_text_warning(&host) {
        tracing::warn!("{warning}");
    }
    tracing::info!(
        "schedr worker {}: master {master_url}, data in {}",
        agent.worker_id,
        data_dir.display()
    );
    let _lock = lock_journal(&agent)?;
    run_worker(agent, config.dags.parse_file_timeout, master_url, stop).await;
    Ok(())
}
