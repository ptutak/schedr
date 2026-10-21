//! `schedr standalone`: every role in one process, on localhost, with SQLite by default (see
//! PROJECT.md, Master–worker protocol: standalone uses the same gRPC protocol).
//!
//! The master's gRPC service, the REST API, the master's loop and one worker agent run side by
//! side; the agent talks to the master over gRPC on localhost like any worker (no TLS: nothing
//! listens beyond localhost). The database is migrated at start: standalone is for a laptop,
//! where there is no separate `schedr db migrate` step.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use schedr_agent::agent::{Agent, AgentSettings, ProcessExecutor};
use schedr_agent::attempt::WorkerSetup;
use schedr_api::{ApiState, router};
use schedr_config::Config;
use schedr_db::Database;
use schedr_master::core::{MasterCore, Settings};
use schedr_master::parse_jobs::PARSE_JOB_TIMEOUT;
use schedr_master::service::Hub;
use schedr_proto::pb::master_client::MasterClient;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;

use crate::app::AppError;

#[cfg(test)]
mod tests;

/// Everything `schedr standalone` runs, read from the configuration.
#[derive(Debug, Clone)]
pub struct Standalone {
    /// Where logs, runner sockets and the SQLite database go.
    pub data_dir: PathBuf,
    /// The database (`[database] url`, else SQLite in the data directory).
    pub database_url: String,
    /// The master's gRPC address.
    pub grpc: SocketAddr,
    /// The REST API's address.
    pub http: SocketAddr,
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
}

/// Reads the standalone setup from the configuration and the environment (`env` gives the
/// data directory's default, the host name and what tasks may inherit).
///
/// # Errors
///
/// [`AppError::BadArgument`] when there is no data directory to use, or the DAG interpreter
/// isn't defined.
pub fn plan(config: &Config, env: &BTreeMap<String, String>) -> Result<Standalone, AppError> {
    let data_dir = config
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
        })?;
    let database_url = config
        .database
        .url
        .clone()
        .unwrap_or_else(|| format!("sqlite://{}", data_dir.join("schedr.db").display()));
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
    let localhost = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    Ok(Standalone {
        grpc: localhost(config.master.grpc_port),
        http: localhost(config.webserver.port),
        master: Settings {
            heartbeat_interval: config.master.heartbeat_interval,
            lease_timeout: config.master.lease_timeout,
            cancel_grace: config.master.cancel_grace,
            parse_job_timeout: PARSE_JOB_TIMEOUT,
        },
        tick: config.master.tick_interval,
        agent: AgentSettings {
            worker_id,
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
        },
        parse_file_timeout: config.dags.parse_file_timeout,
        identity_header: config.webserver.auth.identity_header.clone(),
        database_url,
        data_dir,
    })
}

/// Runs standalone until `shutdown` completes; `listening` gets the addresses once bound.
///
/// # Errors
///
/// [`AppError`] if the data directory, the database or a port can't be set up.
pub async fn run(
    setup: Standalone,
    listening: impl FnOnce(Listening),
    shutdown: impl Future<Output = ()>,
) -> Result<(), AppError> {
    let io = |what: &str, e: std::io::Error| AppError::BadArgument(format!("{what}: {e}"));
    tokio::fs::create_dir_all(&setup.data_dir)
        .await
        .map_err(|e| io(&format!("can't create {}", setup.data_dir.display()), e))?;
    let db = Database::connect(&setup.database_url).await?;
    db.migrate().await?;

    let grpc = TcpListener::bind(setup.grpc)
        .await
        .map_err(|e| io(&format!("can't listen on {}", setup.grpc), e))?;
    let http = TcpListener::bind(setup.http)
        .await
        .map_err(|e| io(&format!("can't listen on {}", setup.http), e))?;
    let addresses = Listening {
        grpc: grpc.local_addr().map_err(|e| io("gRPC address", e))?,
        http: http.local_addr().map_err(|e| io("HTTP address", e))?,
    };
    listening(addresses);

    let (stop, stopped) = watch::channel(false);
    let until = move |mut rx: watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|stop| *stop).await;
    };

    let hub = Hub::new(MasterCore::new(db.clone(), setup.master));
    let grpc_server = tonic::transport::Server::builder()
        .add_service(hub.service())
        .serve_with_incoming_shutdown(TcpListenerStream::new(grpc), until(stopped.clone()));
    let api = router(ApiState::new(db, setup.identity_header.clone()));
    let http_server = axum::serve(http, api).with_graceful_shutdown(until(stopped.clone()));
    let master_loop = hub.run(setup.tick, until(stopped.clone()));

    let executor = ProcessExecutor {
        setup: setup.agent.setup.clone(),
        parse_file_timeout: setup.parse_file_timeout,
    };
    let mut agent = Agent::new(setup.agent, executor, uuid::Uuid::now_v7().to_string());
    let master_url = format!("http://{}", addresses.grpc);
    let agent_loop = agent.run(
        move || {
            let url = master_url.clone();
            async move { MasterClient::connect(url).await.map_err(|e| e.to_string()) }
        },
        until(stopped),
    );

    let signal = async move {
        shutdown.await;
        let _ = stop.send(true);
    };
    let (grpc_result, http_result, (), (), ()) =
        tokio::join!(grpc_server, http_server, master_loop, agent_loop, signal);
    grpc_result.map_err(|e| AppError::BadArgument(format!("gRPC server: {e}")))?;
    http_result.map_err(|e| io("HTTP server", e))?;
    Ok(())
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

/// Runs `schedr standalone` until Ctrl-C; the banner goes to the log once it listens.
///
/// # Errors
///
/// As [`plan`] and [`run`].
pub async fn command(config: &Config, env: &BTreeMap<String, String>) -> Result<(), AppError> {
    let setup = plan(config, env)?;
    let shown = setup.clone();
    run(
        setup,
        |listening| tracing::info!("{}", banner(listening, &shown)),
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await
}
