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

use schedr_agent::agent::AgentSettings;
use schedr_config::Config;
use schedr_db::Database;
use schedr_master::core::Settings;
use tokio::sync::watch;

use crate::app::AppError;
use crate::roles::{
    agent_settings, data_dir, listen, master_settings, run_worker, serve_api, serve_master,
};

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
        master: master_settings(config),
        tick: config.master.tick_interval,
        agent: agent_settings(config, env, &data_dir)?,
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

    let grpc = listen(setup.grpc).await?;
    let http = listen(setup.http).await?;
    let addresses = Listening {
        grpc: grpc.local_addr().map_err(|e| io("gRPC address", e))?,
        http: http.local_addr().map_err(|e| io("HTTP address", e))?,
    };
    listening(addresses);

    let (stop, stopped) = watch::channel(false);
    // Boxed: the roles' futures are large.
    let master = Box::pin(serve_master(
        db.clone(),
        setup.master,
        setup.tick,
        grpc,
        stopped.clone(),
    ));
    let api = Box::pin(serve_api(
        db,
        setup.identity_header.clone(),
        http,
        stopped.clone(),
    ));
    let worker = Box::pin(run_worker(
        setup.agent,
        setup.parse_file_timeout,
        format!("http://{}", addresses.grpc),
        stopped,
    ));
    let signal = async move {
        shutdown.await;
        let _ = stop.send(true);
    };
    let (master, api, (), ()) = tokio::join!(master, api, worker, signal);
    master?;
    api
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
