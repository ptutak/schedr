//! The ops port of every role (see PROJECT.md, Observability): plain HTTP on `[ops] bind` :
//! `port` with only `/healthz`, `/readyz` and `/metrics`, never data or actions, so probes and
//! Prometheus need no certificate or credentials. It is started first, so `/readyz` answers
//! while a role starts.
//!
//! - **`/healthz`** (liveness): 503 once a loop the role watches (the master's scheduling
//!   loop, the worker agent's event loop) hasn't turned for `[ops] loop_stall_timeout`; it
//!   never asks another component (a database or master outage mustn't restart the role).
//! - **`/readyz`**: 503 until every part of the role has started.
//! - **`/metrics`**: the role's metrics (`schedr_…`), its process's (`process_…`, from
//!   `/proc/self`) and `schedr_build_info`, in the `OpenMetrics` text format. A scrape reads
//!   them only: nothing touches the database.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::info::Info;
use prometheus_client::registry::Registry;
use schedr_model::beat::Beat;
use schedr_model::metric_doc::{MetricDoc, MetricKind};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::VERSION;
use crate::app::AppError;
use crate::roles::until;

#[cfg(test)]
mod tests;

/// The content type of `/metrics`.
pub const METRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

const RESIDENT_MEMORY: MetricDoc = MetricDoc {
    name: "process_resident_memory_bytes",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "Resident memory of this process",
};
const CPU: MetricDoc = MetricDoc {
    name: "process_cpu_seconds",
    kind: MetricKind::Counter,
    labels: &[],
    help: "CPU time this process used, user and system",
};
const OPEN_FDS: MetricDoc = MetricDoc {
    name: "process_open_fds",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "File descriptors this process has open",
};
const START_TIME: MetricDoc = MetricDoc {
    name: "process_start_time_seconds",
    kind: MetricKind::Gauge,
    labels: &[],
    help: "When this process started, in seconds since the Unix epoch",
};
const BUILD: MetricDoc = MetricDoc {
    name: "schedr_build",
    kind: MetricKind::Info,
    labels: &["version"],
    help: "The schedr version running",
};

/// The metrics every role serves besides its own, as registered (without a prefix): the
/// process's and the build's.
pub const PROCESS_METRICS: &[MetricDoc] = &[RESIDENT_MEMORY, CPU, OPEN_FDS, START_TIME, BUILD];

/// What the health endpoints check (cheap to clone; clones share it).
#[derive(Debug, Clone)]
pub struct Health {
    inner: Arc<Mutex<Checks>>,
}

#[derive(Debug)]
struct Checks {
    stall: Duration,
    loops: Vec<(&'static str, Beat)>,
    parts: Vec<(&'static str, Arc<AtomicBool>)>,
}

/// A part of a role that is starting; ready once told.
#[derive(Debug, Clone)]
pub struct Starting(Arc<AtomicBool>);

impl Starting {
    /// The part has started.
    pub fn ready(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Health {
    /// Health with loops failing liveness after `stall` without turning.
    #[must_use]
    pub fn new(stall: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Checks {
                stall,
                loops: Vec::new(),
                parts: Vec::new(),
            })),
        }
    }

    fn checks(&self) -> std::sync::MutexGuard<'_, Checks> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How long a loop may go without turning.
    #[must_use]
    pub fn stall(&self) -> Duration {
        self.checks().stall
    }

    /// Liveness watches the loop `name` through `beat` (a loop that hasn't turned yet is
    /// starting, so alive).
    pub fn watch_loop(&self, name: &'static str, beat: Beat) {
        self.checks().loops.push((name, beat));
    }

    /// Readiness waits for the part `name` until it says it is ready.
    #[must_use]
    pub fn starting(&self, name: &'static str) -> Starting {
        let flag = Arc::new(AtomicBool::new(false));
        self.checks().parts.push((name, flag.clone()));
        Starting(flag)
    }

    /// Why the role isn't alive at `now`, if it isn't.
    #[must_use]
    pub fn stalled(&self, now: Instant) -> Option<String> {
        let checks = self.checks();
        checks.loops.iter().find_map(|(name, beat)| {
            let since = beat.since(now)?;
            (since > checks.stall).then(|| {
                format!(
                    "the {name} hasn't turned for {} s (limit {} s)",
                    since.as_secs(),
                    checks.stall.as_secs()
                )
            })
        })
    }

    /// The parts still starting.
    #[must_use]
    pub fn not_ready(&self) -> Vec<&'static str> {
        self.checks()
            .parts
            .iter()
            .filter(|(_, ready)| !ready.load(Ordering::Relaxed))
            .map(|(name, _)| *name)
            .collect()
    }
}

/// What the ops endpoints serve.
#[derive(Debug, Clone)]
pub struct Ops {
    registry: Arc<Registry>,
    health: Health,
}

impl Ops {
    /// The ops endpoints over `registry` (complete: nothing is registered later) and
    /// `health`; the process's metrics and the build's are added to the registry.
    #[must_use]
    pub fn new(mut registry: Registry, health: Health) -> Self {
        registry.register_collector(Box::new(ProcessMetrics));
        registry.register(
            BUILD.name,
            BUILD.help,
            Info::new(vec![("version".to_owned(), VERSION.to_owned())]),
        );
        Self {
            registry: Arc::new(registry),
            health,
        }
    }

    /// The registry's text, as `/metrics` serves it.
    #[must_use]
    pub fn metrics_text(&self) -> String {
        let mut text = String::new();
        // Writing into a String can't fail.
        let _ = prometheus_client::encoding::text::encode(&mut text, &self.registry);
        text
    }
}

/// The ops routes.
pub fn router(ops: Ops) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(ops)
}

async fn healthz(State(ops): State<Ops>) -> impl IntoResponse {
    match ops.health.stalled(Instant::now()) {
        None => (StatusCode::OK, "ok\n".to_owned()),
        Some(why) => (StatusCode::SERVICE_UNAVAILABLE, format!("{why}\n")),
    }
}

async fn readyz(State(ops): State<Ops>) -> impl IntoResponse {
    let starting = ops.health.not_ready();
    if starting.is_empty() {
        (StatusCode::OK, "ok\n".to_owned())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("starting: {}\n", starting.join(", ")),
        )
    }
}

async fn metrics(State(ops): State<Ops>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)],
        ops.metrics_text(),
    )
}

/// Serves the ops endpoints on `listener` until `stop`.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the HTTP server fails.
pub async fn serve_ops(
    listener: TcpListener,
    ops: Ops,
    stop: watch::Receiver<u32>,
) -> Result<(), AppError> {
    axum::serve(listener, router(ops))
        .with_graceful_shutdown(until(stop))
        .await
        .map_err(|e| AppError::BadArgument(format!("ops server: {e}")))
}

/// The process's own metrics, read from `/proc/self` at each scrape (absent without
/// `/proc`).
#[derive(Debug, Default)]
pub struct ProcessMetrics;

/// What `/proc/self` says about this process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessStats {
    /// Resident memory, in bytes.
    pub resident_bytes: u64,
    /// CPU time used (user and system), in seconds.
    pub cpu_seconds: f64,
    /// Open file descriptors.
    pub open_fds: u64,
    /// When the process started, in seconds since the Unix epoch.
    pub start_time_seconds: f64,
}

/// Reads this process's stats from `/proc`; `None` where there is no `/proc`.
#[must_use]
pub fn process_stats() -> Option<ProcessStats> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let boot = std::fs::read_to_string("/proc/stat").ok()?;
    // The listing's own descriptor is among the entries.
    let open_fds = std::fs::read_dir("/proc/self/fd")
        .ok()?
        .count()
        .saturating_sub(1);
    parse_stats(
        &stat,
        &statm,
        &boot,
        u64::try_from(open_fds).unwrap_or(0),
        u64::try_from(rustix::param::page_size()).unwrap_or(4096),
        rustix::param::clock_ticks_per_second(),
    )
}

/// The stats in `/proc/self/stat`, `/proc/self/statm` and `/proc/stat` (for the boot time).
#[must_use]
pub fn parse_stats(
    stat: &str,
    statm: &str,
    boot: &str,
    open_fds: u64,
    page: u64,
    ticks: u64,
) -> Option<ProcessStats> {
    // Fields after the command (field 2, in parentheses): 3 is the first.
    let fields: Vec<&str> = stat[stat.rfind(')')? + 1..].split_whitespace().collect();
    let field = |n: usize| -> Option<u64> { fields.get(n - 3)?.parse().ok() };
    #[expect(clippy::cast_precision_loss, reason = "seconds from clock ticks")]
    let seconds = |t: u64| t as f64 / ticks.max(1) as f64;
    let boot_time: u64 = boot
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?
        .trim()
        .parse()
        .ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    #[expect(clippy::cast_precision_loss, reason = "a Unix time in seconds")]
    let boot_seconds = boot_time as f64;
    Some(ProcessStats {
        resident_bytes: resident_pages * page,
        cpu_seconds: seconds(field(14)? + field(15)?),
        open_fds,
        start_time_seconds: boot_seconds + seconds(field(22)?),
    })
}

/// Help text ending as `Registry::register` ends it.
fn help(doc: MetricDoc) -> String {
    format!("{}.", doc.help)
}

impl Collector for ProcessMetrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let Some(stats) = process_stats() else {
            return Ok(());
        };
        let resident = ConstGauge::new(i64::try_from(stats.resident_bytes).unwrap_or(i64::MAX));
        resident.encode(encoder.encode_descriptor(
            RESIDENT_MEMORY.name,
            &help(RESIDENT_MEMORY),
            None,
            resident.metric_type(),
        )?)?;
        let cpu = ConstCounter::new(stats.cpu_seconds);
        cpu.encode(encoder.encode_descriptor(CPU.name, &help(CPU), None, cpu.metric_type())?)?;
        let fds = ConstGauge::new(i64::try_from(stats.open_fds).unwrap_or(i64::MAX));
        fds.encode(encoder.encode_descriptor(
            OPEN_FDS.name,
            &help(OPEN_FDS),
            None,
            fds.metric_type(),
        )?)?;
        let start = ConstGauge::new(stats.start_time_seconds);
        start.encode(encoder.encode_descriptor(
            START_TIME.name,
            &help(START_TIME),
            None,
            start.metric_type(),
        )?)?;
        Ok(())
    }
}

/// The ops address's line for a role's log.
#[must_use]
pub fn ops_line(address: std::net::SocketAddr) -> String {
    format!("ops on http://{address}")
}
