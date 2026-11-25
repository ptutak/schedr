//! `schedr standalone`: every role in one process, on localhost, with SQLite by default (see
//! archive/PROJECT.md, Master–worker protocol: standalone uses the same gRPC protocol).
//!
//! The master's gRPC service, the REST API, the master's loop and one worker agent with its log
//! endpoint run side by side; the agent talks to the master over gRPC on localhost like any
//! worker, and the API reads logs from the endpoint, both over mTLS whatever `[tls]` says: a
//! CA and a certificate made in memory at start, renewed every 12 hours, never on disk
//! (decision 520), so another process on the machine can't connect to either. `--migrate`
//! migrates the database at start (a laptop);
//! without it, a schema that isn't up to date is refused (production migrates as a step of its
//! own, `schedr db migrate`). When the master fails (another took the lease over), the whole
//! process ends, its tasks interrupted: restarted, it waits as a standby.
//!
//! In one pod (for testing on a cluster, see archive/PROJECT.md): `--listen 0.0.0.0` opens the REST
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
use schedr_config::{AuthMode, AuthSection, Config};
use schedr_db::Database;
use schedr_master::core::Settings;
use schedr_master::metrics::MasterMetrics;
use schedr_master::problems::Problems;
use tokio::sync::watch;

use crate::app::AppError;
use crate::node::{MasterIdentity, heartbeat_master, new_master_id, webserver_node};
use crate::ops::{Health, Ops, ops_line, serve_ops};
use crate::roles::{
    LocalUi, MasterOutbox, Observed, WorkerLinks, agent_settings, check_timeouts, data_dir,
    docs_dir, lead, listen, lock_journal, log_retention, master_outbox, master_settings,
    memory_text, no_auth_warning, role_auth, run_worker, serve_api, serve_master, stop_on_signal,
    until,
};
use schedr_config::masked::masked;

#[cfg(test)]
mod tests;

impl std::fmt::Debug for Standalone {
    /// Its addresses and folders, the database URLs' passwords hidden (B206's review); the
    /// configuration masked as its own `Debug` masks it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let url = |url: &str| schedr_db::redact_password(url);
        f.debug_struct("Standalone")
            .field("data_dir", &self.data_dir)
            .field("database_url", &url(&self.database_url))
            .field(
                "notifications_url",
                &self.notifications_url.as_deref().map(url),
            )
            .field("grpc", &self.grpc)
            .field("http", &self.http)
            .field("ops", &self.ops)
            .field("migrate", &self.migrate)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// How a standalone connects to its database.
#[derive(Debug, Clone, Copy)]
pub struct Connecting {
    /// The longest a database query may take.
    pub query_timeout: Duration,
    /// `[database] prepared_statements`: whether queries stay prepared on their connections.
    pub prepared_statements: bool,
}

/// Everything `schedr standalone` runs, read from the configuration.
#[derive(Clone)]
pub struct Standalone {
    /// How long its worker waits for its master's address (`master_connect_timeout`).
    pub master_connect_timeout: std::time::Duration,
    /// The secret storages, made once the database is connected (`db` among them).
    pub secret_storage: crate::secret_storage::SecretPlan,
    /// Where logs, the worker's journal and the SQLite database go.
    pub data_dir: PathBuf,
    /// The mode of its worker's logs (`[worker] log_mode`, B182).
    pub log_mode: u32,
    /// Whether structured log lines are stored readable (`[worker] log_format`, decision 514).
    pub log_format: schedr_config::config::TaskLogFormat,
    /// The database (`[database] url`, else SQLite in the data directory).
    pub database_url: String,
    /// The master's gRPC address.
    pub grpc: SocketAddr,
    /// How long a handshake on its links may take, and how many run at once
    /// (`[timings] handshake_timeout`, `SCHEDR_HIDDEN__LIMITS__TLS_HANDSHAKES_AT_ONCE`).
    pub handshakes: (Duration, usize),
    /// The REST API's address.
    pub http: SocketAddr,
    /// The worker's log endpoint's address.
    pub logs: SocketAddr,
    /// The ops port's address.
    pub ops: SocketAddr,
    /// How the users and the UI's token are followed.
    pub auth_timings: schedr_api::auth::AuthTimings,
    /// How often its row of the deployment's nodes is written.
    pub node_heartbeat: Duration,
    /// How long a gone master's row stays (`SCHEDR_HIDDEN__MASTER__NODE_ROW_KEPT`).
    pub node_row_kept: Duration,
    /// How often it tries again to reach a database it can't reach at its start
    /// (`SCHEDR_HIDDEN__WEBSERVER__SCHEMA_RETRY`).
    pub schema_retry: Duration,
    /// The DAG versions kept parsed (`SCHEDR_HIDDEN__DATABASE__DAG_VERSIONS_CACHED`).
    pub dag_versions_cached: usize,
    /// How long the worker gets to stop once the master failed: its tasks are interrupted at
    /// once (nothing could record them), so this is only for a worker stuck on its way out
    /// (B151).
    pub worker_stop_after_failure: Duration,
    /// How long a loop may go without turning before liveness fails.
    pub loop_stall_timeout: Duration,
    /// How the database is connected.
    pub connecting: Connecting,
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
    /// `[notifications]`, checked, and its sender's timings.
    pub outbox: MasterOutbox,
    /// How often the master's loop runs.
    pub tick: Duration,
    /// The worker agent.
    pub agent: AgentSettings,
    /// How long one DAG file may take to import, and how many are parsed at once.
    pub parse: schedr_agent::parse::ParseLimits,
    /// Who the user is (`[webserver.auth]`).
    pub auth: AuthSection,
    /// The token a UI elsewhere presents to pass users' names on (`[webserver] ui_token_file`:
    /// a federation of standalones, decision 36).
    pub ui_token_file: Option<PathBuf>,
    /// How long the webserver waits, and how often live pages refresh when the database's
    /// changes can't be followed.
    pub api_timings: crate::roles::ApiTimings,
    /// Whether the writes announce their changes (applied with `--migrate`).
    pub change_notifications: bool,
    /// The web UI it serves next to its API; none with `--no-ui`.
    pub ui: Option<LocalUi>,
    /// Whether to migrate the database at start (`--migrate`).
    pub migrate: bool,
    /// Its configuration, masked: shown on the Deployments page for its master and worker.
    pub config: serde_json::Value,
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

/// Standalone's SQLite file in the data directory, without `[database] url`.
#[must_use]
pub fn sqlite_file(data_dir: &Path) -> PathBuf {
    data_dir.join("schedr.db")
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
        .unwrap_or_else(|| format!("sqlite://{}", sqlite_file(&data_dir).display()));
    let localhost = |port| SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    Ok(Standalone {
        grpc: localhost(config.master.grpc_port),
        handshakes: (
            config.timings.handshake_timeout,
            config.limits.tls_handshakes_at_once,
        ),
        http: localhost(config.webserver.port),
        logs: localhost(config.worker.log_port),
        ops: localhost(config.ops.port),
        loop_stall_timeout: config.timings.loop_stall_timeout,
        master_connect_timeout: config.timings.master_connect_timeout,
        connecting: Connecting {
            query_timeout: config.timings.query_timeout,
            prepared_statements: config.database.prepared_statements,
        },
        notifications: config.database.notifications,
        notifications_url: config.database.notifications_url.clone(),
        lease_period: config.timings.lease_period,
        retention: log_retention(config),
        log_mode: crate::roles::log_mode(config),
        log_format: config.worker.log_format,
        master: schedr_master::core::Settings {
            // Its webserver is this process: sampled with the master (decision 464).
            with_webserver: true,
            ..master_settings(config)
        },
        outbox: master_outbox(config, env)?,
        tick: config.timings.tick_interval,
        agent: agent_settings(config, env, &data_dir, STANDALONE_RESERVE)?,
        secret_storage: crate::secret_storage::SecretPlan::new(&config.secret_storage, env)
            .renewing(&config.timings),
        parse: schedr_agent::parse::ParseLimits::configured(
            config.dags.parse_file_timeout,
            config.dags.parse_concurrency,
        ),
        auth: config.webserver.auth.clone(),
        ui_token_file: config.webserver.ui_token_file.clone(),
        api_timings: crate::roles::ApiTimings::of(config),
        change_notifications: config.database.change_notifications,
        auth_timings: crate::roles::auth_timings(&config.timings, &config.limits),
        node_heartbeat: config.timings.node_heartbeat,
        node_row_kept: config.timings.node_row_kept,
        schema_retry: config.timings.schema_retry,
        dag_versions_cached: config.limits.dag_versions_cached,
        worker_stop_after_failure: config.timings.worker_stop_after_failure,
        ui: Some(LocalUi {
            timings: crate::roles::ui_timings(&config.timings, &config.limits),
            docs_dir: docs_dir(config),
        }),
        migrate: false,
        config: masked(config),
        database_url,
        data_dir,
    })
}

impl Standalone {
    /// How it takes the master's lease ([`lead`]): its period, `--migrate`, and the change
    /// notifications to apply with a migration.
    const fn lease(&self) -> (Duration, bool, bool) {
        (self.lease_period, self.migrate, self.change_notifications)
    }
}

/// Standalone's listeners (gRPC, HTTP, logs, ops) and where they listen.
async fn bind(setup: &Standalone) -> Result<([tokio::net::TcpListener; 4], Listening), AppError> {
    let io = |what: &str, e: std::io::Error| AppError::BadArgument(format!("{what}: {e}"));
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
    Ok(([grpc, http, logs, ops], addresses))
}

impl Standalone {
    /// Its database, connected, with its notifications and DAG cache as configured.
    async fn database(&self) -> Result<Database, AppError> {
        Ok(Database::connect_prepared(
            &self.database_url,
            self.connecting.query_timeout,
            self.connecting.prepared_statements,
        )
        .await?
        .with_notifications(self.notifications, self.notifications_url.clone())
        .with_dag_versions_cached(self.dag_versions_cached))
    }

    /// Its master's row: the configuration shown, how often it is written and how long it
    /// stays once gone.
    const fn node_beat(&self) -> (&serde_json::Value, (Duration, Duration)) {
        (&self.config, (self.node_heartbeat, self.node_row_kept))
    }

    /// What the master's loop runs with: its tick, the events' limit and the notifications.
    fn master_loop(
        &self,
    ) -> (
        Duration,
        (usize, schedr_api::state::ApiLimits),
        MasterOutbox,
    ) {
        (
            self.tick,
            (self.api_timings.max_event_size, self.api_timings.limits),
            self.outbox.clone(),
        )
    }
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
#[expect(
    clippy::too_many_lines,
    reason = "the start in order: data, database, servers, roles"
)]
pub async fn run(
    setup: Standalone,
    listening: impl FnOnce(Listening),
    shutdown: watch::Receiver<u32>,
) -> Result<(), AppError> {
    let lease = setup.lease();
    let io = |what: &str, e: std::io::Error| AppError::BadArgument(format!("{what}: {e}"));
    // Its user's alone (decision 520): created so, warned of when open to others.
    let warned = crate::private_files::data_dir(&setup.data_dir)
        .map_err(|e| io(&format!("can't create {}", setup.data_dir.display()), e))?;
    let database = crate::private_files::database(&setup.database_url)
        .map_err(|e| io("can't create the SQLite file", e))?;
    for warning in warned.into_iter().chain(database) {
        tracing::warn!("{warning}");
    }
    let _lock = lock_data_dir(&setup.data_dir)?;
    let _journal_lock = lock_journal(&setup.agent)?;
    // After the lock: a second standalone on the data directory stops before touching it.
    schedr_agent::connection_env::prepare(&setup.agent.setup.connection_dir)
        .map_err(AppError::BadArgument)?;
    crate::roles::secure_logs(setup.log_mode, &setup.agent.setup.log_dir)?;
    schedr_agent::process::set_log_plain(
        setup.log_format == schedr_config::config::TaskLogFormat::Plain,
    );
    let ([grpc, http, logs, ops], addresses) = bind(&setup).await?;
    let auth = standalone_auth(&setup, addresses.http)?;
    listening(addresses);
    let (tick, max_event_size, outbox) = setup.master_loop();

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
    let connected = crate::roles::once_reached(
        ("standalone", &setup.database_url),
        setup.schema_retry,
        shutdown.clone(),
        || setup.database(),
    );
    let Some(db) = connected.await? else {
        return Ok(());
    };
    let secrets = setup.secret_storage.for_role(Some(&db))?;
    let (master_id, problems, heartbeat) = heartbeat(&db, setup.node_beat(), &stopped);
    let (tls, ephemeral) = links_tls(setup.handshakes)?;
    tokio::spawn(ephemeral.renew(
        tls.clone(),
        schedr_tls::ephemeral::RENEW_AFTER,
        until(stopped.clone()),
    ));
    let mut agent = setup.agent;
    agent.log_endpoint = format!("https://{}", addresses.logs);
    agent.secrets = secrets;
    let leading = lead(
        &db,
        &setup.database_url,
        lease,
        (shutdown.clone(), setup.schema_retry),
        ("standalone", master_id),
    );
    let Some(leadership) = leading.await? else {
        return Ok(());
    };
    // The worker's stop requests: the signals', and the master's failure (two at once: its
    // tasks interrupted, nothing could record them).
    let (request, requests) = watch::channel(0);
    // Once the master failed, the worker gets `worker_stop_after_failure` to stop (B151).
    let (master_failed, failed) = watch::channel(false);
    let forwarding = tokio::spawn(forward_requests(shutdown, request.clone()));
    // Boxed: the roles' futures are large.
    let master_db = db.clone();
    let master_tls = tls.clone();
    let master = Box::pin(async {
        let served = Box::pin(serve_master(
            master_db,
            setup.master,
            (tick, problems, max_event_size, outbox),
            grpc,
            (Some(master_tls), None),
            leadership,
            master_observed,
            stopped.clone(),
        ))
        .await;
        if let Err(error) = &served {
            tracing::error!(%error, "the master failed: standalone stops");
            request.send_modify(|requests| *requests = (*requests).max(2));
            let _ = master_failed.send(true);
        }
        served
    });
    let node = webserver_node(setup.config.clone());
    let api = Box::pin(serve_api(
        db,
        (auth, setup.api_timings, node),
        (http, None),
        Some(tls.clone()),
        setup.ui.clone(),
        api_observed,
        stopped.clone(),
    ));
    let links = WorkerLinks {
        master_url: format!("https://{}", addresses.grpc),
        tls: Some(tls),
        log_listener: logs,
        retention: setup.retention,
        max_message_size: setup.master.max_message_size,
        connect_timeout: setup.master_connect_timeout,
    };
    let worker = Box::pin(run_worker(
        agent,
        setup.parse,
        links,
        agent_observed,
        requests,
    ));
    let given = setup.worker_stop_after_failure;
    let worker = worker_then_stop(worker, failed, stop, given);
    let (master, api, ()) = tokio::join!(master, api, worker);
    forwarding.abort();
    heartbeat.abort();
    if let Ok(Err(error)) = ops_server.await {
        tracing::warn!(%error, "the ops port failed");
    }
    master?;
    api
}

/// The warning for standalone's API signing no one in on a loopback address (decision 520,
/// Grok's review of M5.2i14): every user of the machine reaches it, and through it runs and
/// task logs, though its internal links are under mTLS.
#[must_use]
pub fn local_users_warning(address: SocketAddr) -> String {
    format!(
        "the REST API and its pages sign no one in (auth mode `none`) on {address}: every user \
         of this machine can reach it, and read runs and task logs through it; on a machine \
         shared with other users set [webserver.auth] mode = \"basic\" or \"proxy_header\""
    )
}

/// The name standalone's certificate gives its worker (checked as `[tls] worker_name` is).
const WORKER_NAME: &str = "schedr-worker";

/// Standalone's links' material, made in memory (decision 520): its certificate names
/// 127.0.0.1 (the master's and the log endpoint's address) and [`WORKER_NAME`]; and its CA, to
/// renew it.
fn links_tls(
    (timeout, at_once): (Duration, usize),
) -> Result<(schedr_tls::Tls, schedr_tls::ephemeral::Ephemeral), AppError> {
    let made = |e: schedr_tls::TlsError| AppError::BadArgument(e.to_string());
    let ephemeral =
        schedr_tls::ephemeral::Ephemeral::new(&["127.0.0.1", WORKER_NAME]).map_err(made)?;
    let tls = ephemeral
        .tls(WORKER_NAME)
        .map_err(made)?
        .with_handshake_timeout(timeout)
        .with_handshakes_at_once(at_once);
    Ok((tls, ephemeral))
}

/// Who a standalone's requests are from, by `setup`, on `http`.
fn standalone_auth(
    setup: &Standalone,
    http: SocketAddr,
) -> Result<schedr_api::auth::Auth, AppError> {
    role_auth(
        "webserver",
        &setup.auth,
        http,
        setup.ui_token_file.as_deref(),
        setup.auth_timings,
    )
}

/// Runs the worker (given `given` once `failed`), then stops the master
/// and the API.
async fn worker_then_stop(
    worker: impl Future<Output = ()>,
    failed: watch::Receiver<bool>,
    stop: watch::Sender<u32>,
    given: Duration,
) {
    if !ended_within(worker, failed, given).await {
        tracing::error!(
            "the worker didn't stop within {}s of the master's failure: stopping anyway",
            given.as_secs()
        );
    }
    let _ = stop.send(1);
}

/// Runs `work` to its end, or until `bound` after `failed` turned true: whether it ended.
/// `failed` closing without turning true bounds nothing.
pub async fn ended_within(
    work: impl Future<Output = ()>,
    mut failed: watch::Receiver<bool>,
    bound: Duration,
) -> bool {
    let cut_short = async {
        if failed.wait_for(|failed| *failed).await.is_err() {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(bound).await;
    };
    tokio::select! {
        () = work => true,
        () = cut_short => false,
    }
}

/// Its master's row on the Deployments page, written from now on until `stopped`: the
/// master's ID (to take the lease as), its problems (the master reports them there) and the
/// writing task.
fn heartbeat(
    db: &Database,
    (config, (beat, row_kept)): (&serde_json::Value, (Duration, Duration)),
    stopped: &watch::Receiver<u32>,
) -> (String, Problems, tokio::task::JoinHandle<()>) {
    let problems = Problems::default();
    let identity = MasterIdentity {
        master_id: new_master_id(),
        config: config.clone(),
        problems: problems.clone(),
        heartbeat: beat,
        row_kept,
    };
    let master_id = identity.master_id.clone();
    let task = tokio::spawn(heartbeat_master(db.clone(), identity, stopped.clone()));
    (master_id, problems, task)
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
    (migrate, no_ui): (bool, bool),
) -> Result<(), AppError> {
    let stop = stop_on_signal();
    let mut setup = plan(config, env)?;
    crate::roles::discover_plugins(&mut setup.agent, config, env).await;
    crate::roles::warn_without_dag_roots(config, true);
    setup.migrate = migrate;
    if no_ui {
        setup.ui = None;
    }
    if let Some(address) = listen {
        // Probes and scrapes come from outside the pod too.
        setup.http.set_ip(address);
        setup.ops.set_ip(address);
    }
    let secrets = crate::private_files::secret_files(&[
        (
            "[webserver.auth] users_file",
            config.webserver.auth.users_file.as_deref(),
        ),
        (
            "[webserver] ui_token_file",
            config.webserver.ui_token_file.as_deref(),
        ),
    ]);
    for warning in secrets {
        tracing::warn!("{warning}");
    }
    let shown = setup.clone();
    Box::pin(run(
        setup,
        |listening| {
            if shown.auth.mode == AuthMode::None {
                let warning = no_auth_warning(listening.http, "reach it with kubectl port-forward")
                    .unwrap_or_else(|| local_users_warning(listening.http));
                tracing::warn!("{warning}");
            }
            tracing::info!("{}", banner(listening, &shown));
            tracing::info!("{}", memory_text(&shown.agent.setup.memory));
        },
        stop,
    ))
    .await
}
