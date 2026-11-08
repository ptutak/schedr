//! The watcher hosts: a worker's wait steps, as coroutines in one long-lived process per code
//! set (decision 320; see PROJECT.md, Sensors and triggers).
//!
//! A host is started for the first wait step of an interpreter and set of root copies, and
//! stopped when its last one ends, so new code gets a new host while the old one finishes. All
//! a host's watchers share its socket: one task reads it and passes each message to its
//! attempt, one writes, so no attempt being dropped can leave half a message in it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, Instant};

use schedr_model::runner::{
    AgentMessage, ErrorSummary, OutputStream, RunnerMessage, RunnerMode, TaskRequest,
    WatchersRequest,
};
use schedr_model::task::{SensorMode, TaskKind};
use schedr_proto::pb;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{Mutex, Notify, mpsc, oneshot, watch};

use crate::attempt::{Ran, WorkerSetup};
use crate::channel::{read_message, write_message};
use crate::handles::{AttemptHandles, HandleError};
use crate::launch::{
    LaunchError, PythonTask, connection_answer, secret_answer, start_runner, unless_stopped,
};
use crate::mask::{Mask, MaskSet, MaskStream};
use crate::metrics::{CodeLabels, WatcherMetrics};
use crate::process::{OsProcess, ProcessSpec};
use crate::secrets::AttemptSecrets;
use crate::status::{StopReason, Termination};
use crate::supervise::{Limits, Process, Signal, Stops, Supervised};

/// The exit code a failed watcher stands for: as a task's process that reports a failure.
const FAILED_EXIT_CODE: i32 = 1;

/// The signal number a cancelled watcher stands for: as a task's process that took it.
const TERMINATED: i32 = 15;

/// The signal number of a host that had to be killed.
const KILLED: i32 = 9;

#[cfg(test)]
mod tests;

/// What a trigger found, on its way to the master.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundEvent {
    /// Its key.
    pub key: String,
    /// Its data, encoded as an output is.
    pub data: serde_json::Value,
    /// When it happened, if it said.
    pub at: Option<jiff::Timestamp>,
}

/// Takes what a trigger says.
#[derive(Clone)]
pub struct EventSink {
    /// Whether it batches (`@trigger.poke(batch=...)`): its data is a list of events.
    pub batched: bool,
    /// What it found; the answer comes once the master has it.
    pub found: Arc<dyn Fn(FoundEvent) -> oneshot::Receiver<()> + Send + Sync>,
    /// How a poke's call ended: why it failed, if it did.
    pub poked: Arc<dyn Fn(Option<String>) + Send + Sync>,
}

impl std::fmt::Debug for EventSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EventSink")
    }
}

impl PartialEq for EventSink {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.found, &other.found)
    }
}

/// The hosts of a worker, by code set (cheap to clone; clones share them).
#[derive(Debug, Clone, Default)]
pub struct WatcherHosts {
    hosts: Arc<Mutex<BTreeMap<Vec<String>, Arc<Host>>>>,
    next_watcher: Arc<AtomicU64>,
    /// The code sets hosts run, read by the agent's copy collection, which can't wait.
    code_sets: Arc<StdMutex<BTreeSet<Vec<String>>>>,
    /// The copy of each DAG root the worker runs now.
    code: Arc<StdMutex<Option<watch::Sender<Copies>>>>,
    /// Whether they are being looked at (their memory, and what the heartbeat tells).
    watching: Arc<AtomicBool>,
    metrics: WatcherMetrics,
    /// Hosts lost, by code set, since the agent started (the code sets still current).
    lost: Arc<StdMutex<BTreeMap<Vec<String>, u64>>>,
    /// As last looked at, for the heartbeat, which can't wait.
    reports: Arc<StdMutex<Vec<pb::WatcherHost>>>,
}

/// What a host last said of itself.
#[derive(Debug, Default, Clone, Copy)]
struct HostStats {
    threads: u32,
    tied_threads: u32,
    thread_stops: u64,
}

impl HostStats {
    fn heard(
        stats: &StdMutex<Self>,
        metrics: &WatcherMetrics,
        (threads, tied_threads, thread_stops, loop_lag_ms): (u32, u32, u64, u64),
    ) {
        let mut stats = stats.lock().unwrap_or_else(PoisonError::into_inner);
        metrics
            .thread_stops
            .inc_by(thread_stops.saturating_sub(stats.thread_stops));
        *stats = Self {
            threads,
            // It is the host's word, and the count is of two moments.
            tied_threads: tied_threads.min(threads),
            thread_stops: thread_stops.max(stats.thread_stops),
        };
        #[allow(clippy::cast_precision_loss)] // A delay in milliseconds.
        metrics.loop_lag.observe(loop_lag_ms as f64 / 1000.0);
    }
}

/// The code a host runs: its folders' names, the copies' hashes.
fn code_of(sys_path: &[String]) -> String {
    let names: Vec<String> = sys_path
        .iter()
        .filter_map(|dir| {
            let name = std::path::Path::new(dir).file_name()?;
            Some(name.to_string_lossy().into_owned())
        })
        .collect();
    names.join(",")
}

/// The wait step a host is killed for: the others on it start again.
#[derive(Debug, Clone)]
struct Blamed {
    watcher: u64,
    /// Where it held the event loop, if that is why.
    stack: Option<String>,
}

/// Why a host has to go.
#[derive(Debug, Default)]
struct Going {
    blamed: Option<Blamed>,
    /// With no wait step to blame.
    why: Option<String>,
    /// Its process has ended: nothing done now can be why.
    dead: bool,
    /// The wait steps on it, by watcher: a lost attempt's error names them (decision 342).
    names: BTreeMap<u64, String>,
}

type Copies = BTreeMap<String, PathBuf>;

type Routes = Arc<StdMutex<HashMap<u64, mpsc::Sender<RunnerMessage>>>>;

/// The messages of a watcher waiting for its attempt: past them its host's socket waits too, so a
/// wait step printing faster than its log is written slows down, and the agent's memory holds at
/// most this many (each up to `_LOG_CHUNK`, 4 MiB, for text).
const ROUTE_MESSAGES: usize = 16;

/// A running host.
#[derive(Debug)]
struct Host {
    pid: i32,
    outbox: mpsc::UnboundedSender<AgentMessage>,
    routes: Routes,
    stop: Arc<Notify>,
    ended: watch::Receiver<Option<Termination>>,
    /// What its own log masks: every value of every watcher it has had (decision 334: output
    /// of a thread or C code can't be told apart, and may come after its watcher ended).
    mask: Arc<MaskSet>,
    going: Arc<StdMutex<Going>>,
    stats: Arc<StdMutex<HostStats>>,
}

impl Host {
    /// Starts one, ready for its watchers.
    async fn start(
        setup: &WorkerSetup,
        sys_path: &[String],
        hosts: &WatcherHosts,
    ) -> Result<Self, LaunchError> {
        // Each value was accepted by the attempt's own set, under its `min_mask_length`.
        let mask = Arc::new(MaskSet::new(0));
        let spec = host_spec(setup, sys_path, Mask(Some(Arc::clone(&mask))));
        let runner = start_runner(&spec, RunnerMode::Watchers, |_| {}).await?;
        let pid = runner.process.id();
        let (reader, mut writer) = runner.stream.into_split();
        let (outbox, mut outgoing) = mpsc::unbounded_channel::<AgentMessage>();
        let _ = outbox.send(AgentMessage::Watchers(WatchersRequest {
            sys_path: sys_path.to_vec(),
            block_limit_ms: millis(setup.timeouts.watcher_block_limit),
            import_limit_ms: millis(setup.timeouts.watcher_import_limit),
        }));
        let stop = Arc::new(Notify::new());
        let routes = Routes::default();
        let writing = tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                if write_message(&mut writer, &message).await.is_err() {
                    return;
                }
            }
        });
        let going = Arc::new(StdMutex::new(Going::default()));
        let stats = Arc::new(StdMutex::new(HostStats::default()));
        // Its watchdog says it is there; twice the limit, so that it can say whom to blame first.
        let silence = setup
            .timeouts
            .watcher_block_limit
            .max(setup.timeouts.watcher_import_limit)
            * 2;
        let reading = tokio::spawn(read_host(
            reader,
            Arc::clone(&routes),
            Arc::clone(&stop),
            Arc::clone(&going),
            (silence, pid, Arc::clone(&mask)),
            (Arc::clone(&stats), hosts.metrics.clone()),
        ));
        let (ended_sender, ended) = watch::channel(None);
        let watch_over = HostEnd {
            stopping: Arc::clone(&stop),
            routes: Arc::clone(&routes),
            ended: ended_sender,
            drain: spec.output_drain,
            going: Arc::clone(&going),
            sys_path: sys_path.to_vec(),
            hosts: hosts.clone(),
        };
        tokio::spawn(watch_over.run(runner.process, reading, writing));
        Ok(Self {
            pid,
            outbox,
            routes,
            stop,
            ended,
            mask,
            going,
            stats,
        })
    }

    fn alive(&self) -> bool {
        self.ended.borrow().is_none()
    }

    /// Kills it for a wait step that wouldn't end.
    fn kill_for(&self, watcher: u64) {
        let dead = self
            .going
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .dead;
        if !dead {
            blame(&self.going, watcher, None);
        }
        self.stop.notify_one();
    }

    /// Remembers what a watcher on it runs.
    fn named(&self, watcher: u64, request: &TaskRequest) {
        self.going
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .names
            .insert(watcher, format!("{}.{}", request.dag_id, request.task.id));
    }

    /// Forgets one that ended; those on it when it ended stay, for the errors they are named in.
    fn unnamed(&self, watcher: u64) {
        if self.alive() {
            self.going
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .names
                .remove(&watcher);
        }
    }

    /// Those on it, for a lost attempt's error.
    fn names(&self) -> String {
        let going = self.going.lock().unwrap_or_else(PoisonError::into_inner);
        let names: BTreeSet<&str> = going.names.values().map(String::as_str).collect();
        names.into_iter().collect::<Vec<_>>().join(", ")
    }

    fn going(&self) -> (Option<Blamed>, Option<String>) {
        let going = self.going.lock().unwrap_or_else(PoisonError::into_inner);
        (going.blamed.clone(), going.why.clone())
    }
}

/// Reads a host's messages until it is to go: each watcher's goes to it, and what it says of
/// itself is taken here.
async fn read_host(
    mut reader: tokio::net::unix::OwnedReadHalf,
    routes: Routes,
    stop: Arc<Notify>,
    going: Arc<StdMutex<Going>>,
    (silence, pid, mask): (Duration, i32, Arc<MaskSet>),
    (stats, metrics): (Arc<StdMutex<HostStats>>, WatcherMetrics),
) {
    loop {
        let read = read_message::<_, RunnerMessage>(&mut reader);
        let Ok(read) = tokio::time::timeout(silence, read).await else {
            tracing::warn!(pid, "a watcher host stopped answering: killing it");
            set_why(&going, format!("its host didn't answer for {silence:?}"));
            stop.notify_one();
            return;
        };
        match read {
            Ok(RunnerMessage::WatcherMessage { watcher, message }) => {
                if matches!(*message, RunnerMessage::WatcherStuck) {
                    if is_on_it(&routes, watcher) {
                        blame(&going, watcher, None);
                    }
                    continue;
                }
                let route = routes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&watcher)
                    .cloned();
                // Past it the host's own sends wait too, its watchdog's among them: this is
                // what its silence would be.
                if let Some(route) = route
                    && tokio::time::timeout(silence, route.send(*message))
                        .await
                        .is_err()
                {
                    tracing::warn!(pid, watcher, "a wait step's log isn't being written");
                    set_why(
                        &going,
                        format!("a log on its host wasn't written for {silence:?}"),
                    );
                    stop.notify_one();
                    return;
                }
            }
            Ok(RunnerMessage::HostAlive {
                threads,
                tied_threads,
                thread_stops,
                loop_lag_ms,
            }) => HostStats::heard(
                &stats,
                &metrics,
                (threads, tied_threads, thread_stops, loop_lag_ms),
            ),
            Ok(RunnerMessage::LoopBlocked { watcher, stack }) => {
                tracing::warn!(pid, ?watcher, "a watcher blocked its host's loop");
                // What every wait step on it read is in this process, and may be in its stack.
                let stack = mask.mask_text(&stack);
                // Only one on it can be blamed: anyone else would leave all to start again.
                match watcher.filter(|watcher| is_on_it(&routes, *watcher)) {
                    Some(watcher) => blame(&going, watcher, Some(stack)),
                    None => set_why(
                        &going,
                        format!("its host's event loop was held by no wait step:\n{stack}"),
                    ),
                }
                // What was sent before it is still read.
                stop.notify_one();
            }
            Ok(other) => {
                tracing::warn!(?other, "a watcher host sent something unexpected");
            }
            Err(error) => {
                tracing::warn!(%error, pid, "a watcher host's channel ended");
                stop.notify_one();
                return;
            }
        }
    }
}

fn is_on_it(routes: &Routes, watcher: u64) -> bool {
    routes
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains_key(&watcher)
}

/// The first to blame stays: what comes after is a result of it.
fn blame(going: &StdMutex<Going>, watcher: u64, stack: Option<String>) {
    going
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .blamed
        .get_or_insert(Blamed { watcher, stack });
}

fn set_why(going: &StdMutex<Going>, why: String) {
    going
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .why
        .get_or_insert(why);
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Waits for a host's end, or stops it, and tells its watchers.
struct HostEnd {
    stopping: Arc<Notify>,
    routes: Routes,
    ended: watch::Sender<Option<Termination>>,
    drain: Duration,
    going: Arc<StdMutex<Going>>,
    sys_path: Vec<String>,
    hosts: WatcherHosts,
}

impl HostEnd {
    /// Counts how it ended: for one wait step, or with those on it and none to blame. One
    /// without any was stopped for that.
    fn count(&self) {
        let blamed = self
            .going
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .blamed
            .is_some();
        let on_it = !self
            .routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty();
        if blamed {
            self.hosts.metrics.blocked_kills.inc();
        } else if on_it {
            self.hosts.host_lost(&self.sys_path);
        }
    }

    async fn run(
        self,
        mut process: OsProcess,
        reading: tokio::task::JoinHandle<()>,
        writing: tokio::task::JoinHandle<()>,
    ) {
        let termination = tokio::select! {
            biased;
            () = self.stopping.notified() => {
                let _ = process.signal(Signal::Kill);
                process.wait().await
            }
            termination = process.wait() => termination,
        };
        self.going
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .dead = true;
        // What it started goes with it.
        let _ = process.signal(Signal::Kill);
        let _ = tokio::time::timeout(self.drain, process.output()).await;
        // What it sent before it ended counts: a result, and whom to blame.
        let mut reading = reading;
        if tokio::time::timeout(self.drain, &mut reading)
            .await
            .is_err()
        {
            reading.abort();
        }
        writing.abort();
        self.count();
        let _ = self
            .ended
            .send(Some(termination.unwrap_or(Termination::Unknown)));
        self.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// The host's process: its environment and folder are no run's, so they are the worker's; what
/// threads and C code print goes to its own log.
fn host_spec(setup: &WorkerSetup, sys_path: &[String], mask: Mask) -> ProcessSpec {
    let mut hasher = DefaultHasher::new();
    sys_path.hash(&mut hasher);
    ProcessSpec {
        program: setup.python.clone(),
        args: vec![
            // Not the working directory first: a root's file can't shadow what the runner imports (B178).
            "-P".to_owned(),
            // A thread a wait step starts stays its: what it prints goes to its log, and it can't
            // start a process either (decision 336).
            "-X".to_owned(),
            "thread_inherit_context=1".to_owned(),
            "-m".to_owned(),
            "schedr.runner".to_owned(),
            "watchers".to_owned(),
        ],
        env: setup
            .agent_env
            .iter()
            .filter(|(name, _)| crate::env::is_allowed(name) || setup.pass_env.contains(name))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        working_dir: setup.log_dir.clone(),
        log_file: setup
            .log_dir
            .join(format!("watchers-{:016x}.log", hasher.finish())),
        max_output_size: 64 * 1024,
        mask,
        start_timeout: setup.timeouts.runner_start,
        output_drain: setup.timeouts.output_drain,
    }
}

/// A watcher's place in its host, given up when dropped (a dropped attempt included).
struct Entered {
    hosts: WatcherHosts,
    host: Arc<Host>,
    watcher: u64,
    messages: mpsc::Receiver<RunnerMessage>,
    /// How long a cancel of it may take when no attempt waits for it any more.
    grace: Duration,
    left: bool,
}

impl WatcherHosts {
    /// The hosts of a worker, counted in `metrics`.
    #[must_use]
    pub fn with_metrics(metrics: WatcherMetrics) -> Self {
        Self {
            metrics,
            ..Self::default()
        }
    }

    /// A place in the code set's host, started if it has none.
    async fn enter(
        &self,
        setup: &WorkerSetup,
        sys_path: &[String],
        grace: Duration,
    ) -> Result<Entered, LaunchError> {
        self.watch_hosts(setup);
        let mut hosts = self.hosts.lock().await;
        let running = hosts.get(sys_path).filter(|host| host.alive()).cloned();
        let host = if let Some(host) = running {
            host
        } else {
            // Held from before it starts: nothing removes its copies under it.
            self.code_set(sys_path, true);
            let host = match Host::start(setup, sys_path, self).await {
                Ok(host) => Arc::new(host),
                Err(error) => {
                    self.code_set(sys_path, false);
                    return Err(error);
                }
            };
            hosts.insert(sys_path.to_vec(), Arc::clone(&host));
            host
        };
        let watcher = self.next_watcher.fetch_add(1, Ordering::Relaxed);
        let (route, messages) = mpsc::channel(ROUTE_MESSAGES);
        host.routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(watcher, route);
        Ok(Entered {
            hosts: self.clone(),
            host,
            watcher,
            messages,
            grace,
            left: false,
        })
    }

    /// From the first wait step on, looks at the hosts: what the heartbeat tells and the metrics
    /// come from here, and all of them together are kept under `[watchers] memory_budget`. Past
    /// it the largest is killed: no wait step can be blamed, as memory isn't told apart.
    fn watch_hosts(&self, setup: &WorkerSetup) {
        if self.watching.swap(true, Ordering::SeqCst) {
            return;
        }
        let hosts = self.clone();
        let watch = setup.memory.clone();
        let budget = setup.timeouts.watcher_memory_budget;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(watch.interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let running: Vec<(Vec<String>, Arc<Host>)> = hosts
                    .hosts
                    .lock()
                    .await
                    .iter()
                    .filter(|(_, host)| host.alive())
                    .map(|(code, host)| (code.clone(), Arc::clone(host)))
                    .collect();
                let root = watch.proc_root.clone();
                let pids: Vec<i32> = running.iter().map(|(_, host)| host.pid).collect();
                let Ok(sizes) = tokio::task::spawn_blocking(move || {
                    pids.iter()
                        .map(|pid| crate::memory::group_memory(&root, *pid, true).unwrap_or(0))
                        .collect::<Vec<u64>>()
                })
                .await
                else {
                    continue;
                };
                hosts.looked_at(&running, &sizes);
                let used: u64 = sizes.iter().sum();
                let largest = sizes.iter().enumerate().max_by_key(|(_, size)| **size);
                if used > budget
                    && let Some((index, _)) = largest
                {
                    let host = &running[index].1;
                    tracing::warn!(pid = host.pid, used, budget, "killing a watcher host");
                    set_why(
                        &host.going,
                        format!(
                            "its host was killed: wait step hosts use {}, over `[watchers] \
                             memory_budget` of {}",
                            crate::memory::show_size(used),
                            crate::memory::show_size(budget),
                        ),
                    );
                    host.stop.notify_one();
                }
            }
        });
    }

    /// Keeps what the hosts look like now, for the heartbeat and the metrics; code sets that
    /// lost hosts stay after them, as what kills their hosts may kill them again.
    fn looked_at(&self, running: &[(Vec<String>, Arc<Host>)], sizes: &[u64]) {
        let mut lost = self
            .lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let (mut tied, mut untied) = (0_u64, 0_u64);
        let mut reports = Vec::new();
        for ((code, host), memory) in running.iter().zip(sizes) {
            let stats = *host.stats.lock().unwrap_or_else(PoisonError::into_inner);
            tied += u64::from(stats.tied_threads);
            untied += u64::from(stats.threads - stats.tied_threads);
            let wait_steps = host
                .routes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len();
            reports.push(pb::WatcherHost {
                code: code_of(code),
                wait_steps: u32::try_from(wait_steps).unwrap_or(u32::MAX),
                threads: stats.threads,
                tied_threads: stats.tied_threads,
                memory_bytes: *memory,
                hosts_lost: lost.remove(code).unwrap_or(0),
            });
        }
        reports.extend(lost.iter().map(|(code, count)| pb::WatcherHost {
            code: code_of(code),
            hosts_lost: *count,
            ..pb::WatcherHost::default()
        }));
        self.metrics.set_threads(tied, untied);
        self.metrics
            .hosts
            .set(i64::try_from(running.len()).unwrap_or(i64::MAX));
        self.metrics
            .memory
            .set(i64::try_from(sizes.iter().sum::<u64>()).unwrap_or(i64::MAX));
        *self.reports.lock().unwrap_or_else(PoisonError::into_inner) = reports;
    }

    /// The hosts as last looked at, for the heartbeat.
    #[must_use]
    pub fn reports(&self) -> Vec<pb::WatcherHost> {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn host_lost(&self, sys_path: &[String]) {
        self.metrics
            .hosts_lost
            .get_or_create(&CodeLabels {
                code: code_of(sys_path),
            })
            .inc();
        *self
            .lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(sys_path.to_vec())
            .or_default() += 1;
    }

    /// Takes a watcher out; a host left without any stops.
    async fn leave(&self, host: &Arc<Host>, watcher: u64) {
        host.unnamed(watcher);
        let mut hosts = self.hosts.lock().await;
        let empty = {
            let mut routes = host.routes.lock().unwrap_or_else(PoisonError::into_inner);
            routes.remove(&watcher);
            routes.is_empty()
        };
        if empty {
            host.stop.notify_one();
            let mut gone = None;
            hosts.retain(|key, known| {
                let keep = !Arc::ptr_eq(known, host);
                if !keep {
                    gone = Some(key.clone());
                }
                keep
            });
            if let Some(key) = gone {
                self.code_set(&key, false);
            }
        }
    }

    fn code_set(&self, sys_path: &[String], running: bool) {
        let mut sets = self
            .code_sets
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if running {
            sets.insert(sys_path.to_vec());
        } else {
            sets.remove(sys_path);
        }
    }

    /// The folders of the code copies hosts run: they outlive the attempt that started them,
    /// when a poke moved.
    #[must_use]
    pub fn code_in_use(&self) -> Vec<String> {
        let sets = self
            .code_sets
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        sets.iter()
            .flatten()
            .filter_map(|dir| {
                let name = std::path::Path::new(dir).file_name()?;
                Some(name.to_string_lossy().into_owned())
            })
            .collect()
    }

    /// The worker's newest copy of each DAG root: running pokes move to it (decision 335).
    pub fn code_changed(&self, copies: &Copies) {
        self.code_channel().send_if_modified(|now| {
            let changed = now != copies;
            if changed {
                // Code no longer current has its lost hosts told by the metric from here on.
                let old: Vec<&PathBuf> = now
                    .values()
                    .filter(|copy| !copies.values().any(|current| current == *copy))
                    .collect();
                self.lost
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .retain(|code, _| {
                        !code
                            .iter()
                            .any(|dir| old.iter().any(|copy| copy.as_os_str() == dir.as_str()))
                    });
                now.clone_from(copies);
            }
            changed
        });
    }

    fn code_channel(&self) -> watch::Sender<Copies> {
        self.code
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| watch::channel(Copies::new()).0)
            .clone()
    }

    /// How many hosts run, for tests and metrics.
    pub async fn running(&self) -> usize {
        self.hosts.lock().await.len()
    }
}

impl Entered {
    async fn leave(mut self) {
        self.left = true;
        self.hosts.leave(&self.host, self.watcher).await;
    }
}

impl Drop for Entered {
    fn drop(&mut self) {
        if self.left {
            return;
        }
        // No attempt waits for it any more (it ended first, or its agent is going away): it
        // is still cancelled and held to its grace, as Python can't cancel what doesn't yield.
        let _ = self.host.outbox.send(AgentMessage::CancelWatcher {
            watcher: self.watcher,
        });
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let hosts = self.hosts.clone();
        let host = Arc::clone(&self.host);
        let watcher = self.watcher;
        let grace = self.grace;
        let mut messages = std::mem::replace(&mut self.messages, mpsc::channel(1).1);
        runtime.spawn(async move {
            let ended = async {
                while let Some(message) = messages.recv().await {
                    if ends_a_watcher(&message) {
                        return;
                    }
                }
            };
            if tokio::time::timeout(grace, ended).await.is_err() {
                host.kill_for(watcher);
            }
            hosts.leave(&host, watcher).await;
        });
    }
}

impl Entered {
    /// Its grace, for its host to hold what it left running to.
    fn cancel_grace_ms(&self) -> u64 {
        millis(self.grace)
    }
}

/// Whether a watcher says nothing after this.
const fn ends_a_watcher(message: &RunnerMessage) -> bool {
    matches!(
        message,
        RunnerMessage::TaskSucceeded { .. }
            | RunnerMessage::TaskFailed { .. }
            | RunnerMessage::WatcherCancelled
            | RunnerMessage::WatcherMoved
    )
}

/// What a watcher printed, masked, in its attempt's log.
struct AttemptLog<'a> {
    mask: &'a Mask,
    file: tokio::fs::File,
    stdout: Option<MaskStream<'a>>,
    stderr: Option<MaskStream<'a>>,
}

impl<'a> AttemptLog<'a> {
    async fn open(spec: &'a ProcessSpec) -> std::io::Result<Self> {
        let file = crate::process::open_log_file(&spec.log_file).await?;
        let stream = || spec.mask.0.as_deref().map(MaskSet::stream);
        Ok(Self {
            mask: &spec.mask,
            file,
            stdout: stream(),
            stderr: stream(),
        })
    }

    async fn write(&mut self, stream: OutputStream, text: &str) {
        let masking = match stream {
            OutputStream::Stdout => &mut self.stdout,
            OutputStream::Stderr => &mut self.stderr,
        };
        let masked = match masking {
            Some(masking) => masking.push(text.as_bytes()),
            None => text.as_bytes().to_vec(),
        };
        let _ = self.file.write_all(&masked).await;
    }

    /// A line of the agent's, masked: it may quote what new code raised.
    async fn note(&mut self, text: &str) {
        let masked = match &self.mask.0 {
            Some(mask) => mask.mask(text.as_bytes()),
            None => text.as_bytes().to_vec(),
        };
        let _ = self.file.write_all(&masked).await;
    }

    async fn finish(mut self) {
        for masking in [&mut self.stdout, &mut self.stderr].into_iter().flatten() {
            let rest = masking.finish();
            let _ = self.file.write_all(&rest).await;
        }
        let _ = self.file.flush().await;
    }
}

/// Answers what watchers ask. Answers wait on a secret storage, so none holds up the rest, a
/// cancel's end above all; they stop when it is dropped.
struct Answers {
    secrets: Option<Arc<AttemptSecrets>>,
    handles: Option<Arc<AttemptHandles>>,
    events: Option<EventSink>,
    pending: tokio::task::JoinSet<()>,
}

impl Answers {
    fn new(
        secrets: Option<Arc<AttemptSecrets>>,
        handles: Option<Arc<AttemptHandles>>,
        events: Option<EventSink>,
    ) -> Self {
        Self {
            secrets,
            handles,
            events,
            pending: tokio::task::JoinSet::new(),
        }
    }

    /// Why an event can't be sent, if it can't: see [`secret_key`].
    fn secret_key(&self, key: &str, data: &serde_json::Value, batched: bool) -> Option<String> {
        secret_key(&self.secrets.as_ref()?.mask, key, data, batched)
    }

    /// Takes what a watcher sent; gives back what isn't for it, which is the caller's.
    async fn serve(
        &mut self,
        message: RunnerMessage,
        host: &Arc<Host>,
        watcher: u64,
        log: &mut AttemptLog<'_>,
    ) -> Option<RunnerMessage> {
        if let RunnerMessage::WatcherLog { stream, text } = &message {
            log.write(*stream, text).await;
            return None;
        }
        if let RunnerMessage::TriggerPoked { error } = message {
            if let Some(events) = &self.events {
                let error = match self.secrets.as_ref().map(|s| &s.mask) {
                    Some(mask) if !mask.is_empty() => error.map(|why| mask.mask_text(&why)),
                    _ => error,
                };
                (events.poked)(error);
            }
            return None;
        }
        if let RunnerMessage::TriggerEvent { key, data, at } = message {
            let Some(events) = &self.events else {
                return None;
            };
            if let Some(why) = self.secret_key(&key, &data, events.batched) {
                let _ = host.outbox.send(AgentMessage::ToWatcher {
                    watcher,
                    message: Box::new(AgentMessage::EventRefused { message: why }),
                });
                return None;
            }
            // What a run reads of it, and people see, as they would of an output.
            let (key, data) = match self.secrets.as_ref().map(|s| &s.mask) {
                Some(mask) if !mask.is_empty() => (mask.mask_text(&key), mask.mask_json(&data)),
                _ => (key, data),
            };
            let stored = (events.found)(FoundEvent { key, data, at });
            let host = Arc::clone(host);
            self.pending.spawn(async move {
                // Dropped: it stopped, and nothing waits for this any more.
                if stored.await.is_ok() {
                    let _ = host.outbox.send(AgentMessage::ToWatcher {
                        watcher,
                        message: Box::new(AgentMessage::EventStored),
                    });
                }
            });
            return None;
        }
        if !matches!(
            message,
            RunnerMessage::SecretGet { .. }
                | RunnerMessage::HandleAdd { .. }
                | RunnerMessage::ConnectionGet { .. }
        ) {
            return Some(message);
        }
        let secrets = self.secrets.clone();
        let handles = self.handles.clone();
        let host = Arc::clone(host);
        self.pending.spawn(async move {
            let answer = answer_to(message, secrets.as_deref(), handles.as_deref()).await;
            let _ = host.outbox.send(AgentMessage::ToWatcher {
                watcher,
                message: Box::new(answer),
            });
        });
        None
    }
}

async fn answer_to(
    message: RunnerMessage,
    secrets: Option<&AttemptSecrets>,
    handles: Option<&AttemptHandles>,
) -> AgentMessage {
    match message {
        RunnerMessage::SecretGet { secret } => secret_answer(secrets, &secret).await,
        RunnerMessage::ConnectionGet { name } => connection_answer(secrets, &name).await,
        RunnerMessage::HandleAdd { handle } => {
            let added = match handles {
                Some(handles) => handles.add(handle).await,
                None => Err(HandleError::NoCleanup),
            };
            added.map_or_else(
                |error| AgentMessage::HandleRefused {
                    message: error.to_string(),
                },
                |()| AgentMessage::HandleAdded,
            )
        }
        other => AgentMessage::Refused {
            reason: format!("not a question: {}", crate::channel::describe(&other)),
        },
    }
}

/// A poke's move to new code (decision 335), under way.
enum Stage {
    /// Its new host is being found or started: nothing waits for that.
    Entering(tokio::task::JoinHandle<Result<Entered, LaunchError>>),
    /// Its place there, making no call until told; `stopping`: it could start, and the old
    /// one was told to stop.
    Entered { to: Entered, stopping: bool },
}

struct Moving {
    stage: Stage,
    /// What it runs there.
    request: TaskRequest,
    /// The new code's hash.
    code_hash: Option<String>,
}

/// A poke moving to new code when there is some; what never moves has none of these.
struct Poke<'a> {
    setup: &'a WorkerSetup,
    hosts: &'a WatcherHosts,
    mask: Mask,
    grace: Duration,
    /// As it runs now.
    request: TaskRequest,
    code: watch::Receiver<Copies>,
    /// The code last tried: new code that can't start it isn't tried again.
    tried: Vec<String>,
    max_wait: Option<Duration>,
    started: Instant,
    moving: Option<Moving>,
}

impl Poke<'_> {
    /// Starts a move, if there is new code it hasn't tried.
    fn begin_move(&mut self) {
        if self.moving.is_some() {
            return;
        }
        let copies = self.code.borrow_and_update().clone();
        let Some(root_dir) = copies.get(&self.request.dag_file.root) else {
            return;
        };
        let sys_path = self.setup.sys_path_at(&copies);
        if sys_path == self.tried {
            return;
        }
        self.tried.clone_from(&sys_path);
        // A copy's folder is its hash (`crate::scan`).
        let code_hash = root_dir
            .file_name()
            .map(|name| name.to_string_lossy().replacen('-', ":", 1));
        let mut request = self.request.clone();
        request.dag_file.root_dir = root_dir.to_string_lossy().into_owned();
        request.sys_path.clone_from(&sys_path);
        let (hosts, setup, grace) = (self.hosts.clone(), self.setup.clone(), self.grace);
        let entering = tokio::spawn(async move { hosts.enter(&setup, &sys_path, grace).await });
        self.moving = Some(Moving {
            stage: Stage::Entering(entering),
            request,
            code_hash,
        });
    }

    /// Its place in the new code's host came, or didn't: it starts there, not calling yet.
    async fn entered_new_host(
        &mut self,
        result: Result<Result<Entered, LaunchError>, tokio::task::JoinError>,
        log: &mut AttemptLog<'_>,
    ) {
        let Some(moving) = self.moving.as_mut() else {
            return;
        };
        match result {
            Ok(Ok(to)) => {
                // Before it can print anything there.
                if let Some(mask) = &self.mask.0 {
                    mask.also_in(Arc::clone(&to.host.mask));
                }
                let _ = to.host.outbox.send(AgentMessage::StartWatcher {
                    watcher: to.watcher,
                    task: Box::new(moving.request.clone()),
                    cancel_grace_ms: to.cancel_grace_ms(),
                    moved_in: true,
                });
                moving.stage = Stage::Entered {
                    to,
                    stopping: false,
                };
            }
            Ok(Err(error)) => self.stays(&error.to_string(), log).await,
            Err(error) => self.stays(&error.to_string(), log).await,
        }
    }

    /// New code can't start it: it stays on its code.
    async fn stays(&mut self, why: &str, log: &mut AttemptLog<'_>) {
        log.note(&format!(
            "new code can't start this wait step, it stays on its code: {why}\n"
        ))
        .await;
        if let Some(Moving {
            stage: Stage::Entered { to, .. },
            ..
        }) = self.moving.take()
        {
            to.leave().await;
        }
    }

    /// A message of the new code's host, about a move under way.
    async fn on_new_code_message(
        &mut self,
        message: Option<RunnerMessage>,
        old: &Entered,
        answers: &mut Answers,
        log: &mut AttemptLog<'_>,
    ) {
        let Some(Moving {
            stage: Stage::Entered { to, stopping },
            ..
        }) = self.moving.as_mut()
        else {
            return;
        };
        let why = match message {
            Some(RunnerMessage::TaskImported) if !*stopping => {
                *stopping = true;
                let _ = old.host.outbox.send(AgentMessage::MoveWatcher {
                    watcher: old.watcher,
                });
                return;
            }
            Some(RunnerMessage::TaskFailed { error, .. }) => {
                format!("{}: {}", error.exception, error.message)
            }
            None => "its host ended".to_owned(),
            Some(other) => {
                let (host, watcher) = (Arc::clone(&to.host), to.watcher);
                let _ = answers.serve(other, &host, watcher, log).await;
                return;
            }
        };
        self.stays(&why, log).await;
    }

    fn left_of_max_wait(&self) -> Option<u64> {
        self.max_wait.map(|wait| {
            u64::try_from(wait.saturating_sub(self.started.elapsed()).as_millis())
                .unwrap_or(u64::MAX)
        })
    }
}

/// Where a watcher is now, for what stops it from outside.
struct Place {
    host: StdMutex<Arc<Host>>,
    watcher: AtomicU64,
    /// It is being cancelled: it doesn't move any more.
    cancelling: AtomicBool,
    /// The hash of the code it moved to, if it moved.
    moved_to: StdMutex<Option<String>>,
    /// What it runs now: a move changes it.
    request: StdMutex<TaskRequest>,
    /// Started again: its first call waits to be told what is left of its time since this.
    going_when_imported: StdMutex<Option<Instant>>,
}

impl Place {
    /// Its place, having told the host to start it; `running_since`: it started again, and its
    /// first call waits for its import, which takes none of its time left.
    fn started(entered: &Entered, request: &TaskRequest, running_since: Option<Instant>) -> Self {
        let _ = entered.host.outbox.send(AgentMessage::StartWatcher {
            watcher: entered.watcher,
            task: Box::new(request.clone()),
            cancel_grace_ms: entered.cancel_grace_ms(),
            moved_in: running_since.is_some(),
        });
        entered.host.named(entered.watcher, request);
        Self {
            host: StdMutex::new(Arc::clone(&entered.host)),
            watcher: AtomicU64::new(entered.watcher),
            cancelling: AtomicBool::new(false),
            moved_to: StdMutex::new(None),
            request: StdMutex::new(request.clone()),
            going_when_imported: StdMutex::new(running_since),
        }
    }

    /// Lets one started again make its first call, with what is left of its time.
    fn go_when_imported(&self, entered: &Entered) {
        let since = self
            .going_when_imported
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(since) = since else {
            return;
        };
        let request = self.request.lock().unwrap_or_else(PoisonError::into_inner);
        let max_wait_ms = match request.task.kind {
            TaskKind::Sensor { max_wait_ms, .. } => max_wait_ms,
            _ => None,
        };
        let _ = entered.host.outbox.send(AgentMessage::GoWatcher {
            watcher: entered.watcher,
            max_wait_ms: max_wait_ms.map(|max| max.saturating_sub(millis(since.elapsed()))),
        });
    }

    fn host(&self) -> Arc<Host> {
        Arc::clone(&self.host.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn moved_to(&self) -> Option<String> {
        self.moved_to
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn cancel(&self) {
        self.cancelling.store(true, Ordering::SeqCst);
        let _ = self.host().outbox.send(AgentMessage::CancelWatcher {
            watcher: self.watcher.load(Ordering::SeqCst),
        });
    }
}

/// Serves a watcher until its result, as a task's runner is served, moving a poke to new code
/// on the way; `None` if its host ended. Leaves its host on any end but being dropped.
async fn conversation(
    mut entered: Entered,
    mut poke: Option<Poke<'_>>,
    place: &Place,
    mut answers: Answers,
    log: &mut AttemptLog<'_>,
) -> Option<RunnerMessage> {
    let ended = loop {
        let still = !place.cancelling.load(Ordering::SeqCst);
        if let (Some(poke), true) = (&mut poke, still) {
            poke.begin_move();
        }
        let (code, moving) = match &mut poke {
            Some(poke) => (Some(&mut poke.code), poke.moving.as_mut()),
            None => (None, None),
        };
        let new_code = async {
            match code {
                Some(code) if still => {
                    if code.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
                _ => std::future::pending().await,
            }
        };
        let from_new = async {
            match moving.map(|moving| &mut moving.stage) {
                Some(Stage::Entering(entering)) => Happened::Entered(entering.await),
                Some(Stage::Entered { to, .. }) => Happened::New(to.messages.recv().await),
                None => std::future::pending().await,
            }
        };
        let happened = tokio::select! {
            biased;
            message = entered.messages.recv() => Happened::Old(message),
            happened = from_new => happened,
            () = new_code => Happened::NewCode,
            // A finished answer needs no one's attention.
            Some(_) = answers.pending.join_next() => Happened::NewCode,
        };
        match happened {
            Happened::NewCode => {}
            Happened::Old(Some(RunnerMessage::TaskImported)) => place.go_when_imported(&entered),
            Happened::Old(None) => break None,
            Happened::Old(Some(RunnerMessage::WatcherMoved)) => {
                if place.cancelling.load(Ordering::SeqCst) {
                    break Some(RunnerMessage::WatcherCancelled);
                }
                let Some(poke) = poke.as_mut() else {
                    break None;
                };
                match moved(poke, log).await {
                    Ok(to) => {
                        place
                            .request
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .clone_from(&poke.request);
                        *place.host.lock().unwrap_or_else(PoisonError::into_inner) =
                            Arc::clone(&to.0.host);
                        place.watcher.store(to.0.watcher, Ordering::SeqCst);
                        if to.1.is_some() {
                            *place
                                .moved_to
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner) = to.1;
                        }
                        let old = std::mem::replace(&mut entered, to.0);
                        old.leave().await;
                    }
                    Err(why) => {
                        log.note(&format!("this wait step can't go on: {why}\n"))
                            .await;
                        break None;
                    }
                }
            }
            Happened::Old(Some(message)) => {
                let host = Arc::clone(&entered.host);
                if let Some(message) = answers.serve(message, &host, entered.watcher, log).await {
                    break Some(message);
                }
            }
            Happened::Entered(result) => {
                if let Some(poke) = poke.as_mut() {
                    poke.entered_new_host(result, log).await;
                }
            }
            Happened::New(message) => {
                if let Some(poke) = poke.as_mut() {
                    poke.on_new_code_message(message, &entered, &mut answers, log)
                        .await;
                }
            }
        }
    };
    drop(poke);
    entered.leave().await;
    ended
}

/// The old host stopped its poke between calls: it goes on where it was to move to, or, the new
/// host gone by then, on its own code again; its deadline is kept either way. Gives its new
/// place, and the hash of the code it moved to.
async fn moved(
    poke: &mut Poke<'_>,
    log: &mut AttemptLog<'_>,
) -> Result<(Entered, Option<String>), String> {
    let left = poke.left_of_max_wait();
    if let Some(Moving {
        stage: Stage::Entered { to, .. },
        request,
        code_hash,
    }) = poke.moving.take()
    {
        to.host.named(to.watcher, &request);
        let _ = to.host.outbox.send(AgentMessage::GoWatcher {
            watcher: to.watcher,
            max_wait_ms: left,
        });
        log.note("moved to new code\n").await;
        poke.request = request;
        return Ok((to, code_hash));
    }
    let to = poke
        .hosts
        .enter(poke.setup, &poke.request.sys_path, poke.grace)
        .await
        .map_err(|error| error.to_string())?;
    let _ = to.host.outbox.send(AgentMessage::StartWatcher {
        watcher: to.watcher,
        task: Box::new(poke.request.clone()),
        cancel_grace_ms: to.cancel_grace_ms(),
        moved_in: true,
    });
    to.host.named(to.watcher, &poke.request);
    let _ = to.host.outbox.send(AgentMessage::GoWatcher {
        watcher: to.watcher,
        max_wait_ms: left,
    });
    Ok((to, None))
}

enum Happened {
    Old(Option<RunnerMessage>),
    New(Option<RunnerMessage>),
    Entered(Result<Result<Entered, LaunchError>, tokio::task::JoinError>),
    NewCode,
}

/// Runs a wait step in its code set's host: `spawned` gets the host's process ID. Cancelling,
/// a timeout and a drain cancel its coroutine; one that doesn't end in its grace time has its
/// whole host killed, as Python can't cancel what doesn't yield.
///
/// # Errors
///
/// [`LaunchError`] if the host can't be started or its log can't be opened.
pub async fn run_watcher(
    hosts: &WatcherHosts,
    setup: &WorkerSetup,
    process: &ProcessSpec,
    task: PythonTask,
    limits: Limits,
    mut stop: Stops,
    spawned: impl FnOnce(i32),
) -> Result<Ran, LaunchError> {
    let PythonTask {
        mut request,
        secrets,
        handles,
        events,
    } = task;
    let started = Instant::now();
    let deadline = limits
        .timeout
        .map(|timeout| tokio::time::Instant::from_std(started) + timeout);
    let ran = |supervised, runner_result| {
        Ran::with_no_process(supervised, runner_result, started.elapsed())
    };
    let mut log = AttemptLog::open(process)
        .await
        .map_err(|source| LaunchError::Start {
            what: process.log_file.display().to_string(),
            source,
        })?;
    let mut spawned = Some(spawned);
    let mut moved_to = None;
    let mut again = false;
    loop {
        let sys_path = request.sys_path.clone();
        let entering = hosts.enter(setup, &sys_path, limits.grace);
        let entered = match unless_stopped(entering, Some((&mut stop, deadline))).await {
            Ok(entered) => entered?,
            Err(reason) => {
                let supervised = Supervised {
                    termination: Termination::Unknown,
                    stopped: Some(reason),
                    killed: false,
                };
                return Ok(ran(supervised, None));
            }
        };
        if let Some(spawned) = spawned.take() {
            spawned(entered.host.pid);
        }
        if let Some(mask) = &process.mask.0 {
            mask.also_in(Arc::clone(&entered.host.mask));
        }
        let poke = poke_of(
            hosts,
            setup,
            &request,
            started,
            (&process.mask, limits.grace),
        );
        // Its time starts when it does; starting again, it runs on.
        let starts = again.then_some(started);
        let place = Place::started(&entered, &request, starts);
        let (message, reason, killed) = {
            let answers = Answers::new(secrets.clone(), handles.clone(), events.clone());
            let talking = conversation(entered, poke, &place, answers, &mut log);
            let talking = Box::pin(talking);
            until_ended(talking, &mut stop, deadline, &place, limits).await
        };
        moved_to = place.moved_to().or(moved_to);
        let (message, host_lost) = if message.is_none() && reason.is_none() {
            let trigger = place_is_trigger(&place);
            match after_host_ended(&place, setup, trigger).await {
                AfterHost::Fails(message) => (Some(message), None),
                AfterHost::Lost(why) => (None, Some(why)),
                AfterHost::StartsAgain => {
                    // Not its doing: its time runs on, and it uses no retry.
                    log.note(STARTS_AGAIN).await;
                    hosts.metrics.restarts.inc();
                    request = place
                        .request
                        .into_inner()
                        .unwrap_or_else(PoisonError::into_inner);
                    again = true;
                    continue;
                }
            }
        } else {
            (message, None)
        };
        log.finish().await;
        let struck = place
            .host()
            .going()
            .0
            .is_some_and(|blamed| blamed.watcher == place.watcher.load(Ordering::SeqCst));
        let supervised = Supervised {
            termination: termination_of(message.as_ref(), killed, &place).await,
            stopped: reason,
            killed,
        };
        let result = message.filter(|message| !matches!(message, RunnerMessage::WatcherCancelled));
        return Ok(Ran {
            moved_to,
            host_lost,
            struck,
            ..ran(supervised, result)
        });
    }
}

const STARTS_AGAIN: &str =
    "another wait step had its host killed: this one starts again on a new host\n";

/// What a wait step's host's end, with no result from it, means for it.
enum AfterHost {
    /// It is to blame.
    Fails(RunnerMessage),
    /// Another was.
    StartsAgain,
    /// None was: why it ended.
    Lost(String),
}

async fn after_host_ended(place: &Place, setup: &WorkerSetup, trigger: bool) -> AfterHost {
    let host = place.host();
    let termination = ended_host(&host).await;
    match host.going() {
        (Some(blamed), _) if blamed.watcher == place.watcher.load(Ordering::SeqCst) => {
            AfterHost::Fails(blocked(setup, blamed.stack))
        }
        // The master knows its bookmark: from there, rather than from what it started with.
        (Some(_), _) if trigger => {
            AfterHost::Lost("its host was killed for another watcher on it".to_owned())
        }
        (Some(_), _) => AfterHost::StartsAgain,
        (None, why) => {
            let why = why.unwrap_or_else(|| format!("its host ended: {termination:?}"));
            let names = host.names();
            AfterHost::Lost(format!(
                "{why}\nNo wait step can be blamed. On its host: {names}"
            ))
        }
    }
}

fn place_is_trigger(place: &Place) -> bool {
    place
        .request
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .trigger
        .is_some()
}

/// The end of a wait step that held its host's event loop.
fn blocked(setup: &WorkerSetup, stack: Option<String>) -> RunnerMessage {
    let limit = setup.timeouts.watcher_block_limit;
    RunnerMessage::TaskFailed {
        error: ErrorSummary {
            exception: "BlockedEventLoop".to_owned(),
            message: stack.as_ref().map_or_else(
                || "it wouldn't end in its grace time, so its host was killed".to_owned(),
                |_| {
                    format!(
                        "it didn't reach an `await` within `[watchers] block_limit` ({} s), \
                         holding up every wait step on its host. Use an async client or \
                         `asyncio.to_thread`",
                        limit.as_secs_f64()
                    )
                },
            ),
            traceback: stack.unwrap_or_default(),
        },
        kind: Some(schedr_model::runner::BLOCKED_EVENT_LOOP.to_owned()),
    }
}

/// Waits for a watcher's end, cancelling it at the first reason to stop. Gives its last message,
/// the reason, and whether its host had to be killed.
async fn until_ended(
    mut talking: impl Future<Output = Option<RunnerMessage>> + Unpin,
    stop: &mut Stops,
    deadline: Option<tokio::time::Instant>,
    place: &Place,
    limits: Limits,
) -> (Option<RunnerMessage>, Option<StopReason>, bool) {
    let until = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    };
    let reason = tokio::select! {
        biased;
        reason = stop.next_reason() => reason,
        () = until => StopReason::TimedOut,
        message = &mut talking => return (message, None, false),
    };
    place.cancel();
    let (message, killed) = cancelled(place, &mut talking, stop, limits, reason).await;
    (message, Some(reason), killed)
}

/// What a watcher's end stands for, as a process's would.
async fn termination_of(
    message: Option<&RunnerMessage>,
    killed: bool,
    place: &Place,
) -> Termination {
    match message {
        Some(RunnerMessage::TaskSucceeded { .. }) => Termination::Exited(0),
        Some(RunnerMessage::TaskFailed { .. }) => Termination::Exited(FAILED_EXIT_CODE),
        Some(RunnerMessage::WatcherCancelled) => Termination::Signaled(TERMINATED),
        Some(_) => Termination::Unknown,
        None if killed => Termination::Signaled(KILLED),
        None => ended_host(&place.host()).await,
    }
}

/// What moves a `@sensor.poke` to new code; a `@sensor` holds its progress, and never moves.
fn poke_of<'a>(
    hosts: &'a WatcherHosts,
    setup: &'a WorkerSetup,
    request: &TaskRequest,
    started: Instant,
    (mask, grace): (&Mask, Duration),
) -> Option<Poke<'a>> {
    // New code starts a trigger again from its bookmark, as the master tells it.
    if request.trigger.is_some() {
        return None;
    }
    let TaskKind::Sensor {
        mode: SensorMode::Poke,
        max_wait_ms,
        ..
    } = request.task.kind
    else {
        return None;
    };
    Some(Poke {
        setup,
        hosts,
        mask: mask.clone(),
        grace,
        request: request.clone(),
        code: hosts.code_channel().subscribe(),
        tried: request.sys_path.clone(),
        max_wait: max_wait_ms.map(Duration::from_millis),
        started,
        moving: None,
    })
}

/// How the host's end was told; its watchers are told after it.
async fn ended_host(host: &Host) -> Termination {
    let mut ended = host.ended.clone();
    match ended.wait_for(Option::is_some).await {
        Ok(termination) => (*termination).unwrap_or(Termination::Unknown),
        Err(_) => Termination::Unknown,
    }
}

/// Waits for a cancelled watcher to end, for its grace time: a later reason can only hurry it,
/// as for a process. Past it the host is killed. Returns its last message and whether it was.
async fn cancelled(
    place: &Place,
    talking: &mut (impl Future<Output = Option<RunnerMessage>> + Unpin),
    stop: &mut Stops,
    limits: Limits,
    reason: StopReason,
) -> (Option<RunnerMessage>, bool) {
    let grace = if reason == StopReason::Interrupted {
        limits.interrupt_grace
    } else {
        limits.grace
    };
    let mut kill_at = tokio::time::Instant::now() + grace;
    loop {
        tokio::select! {
            biased;
            later = stop.next_reason() => {
                if later == StopReason::Interrupted {
                    kill_at = kill_at.min(tokio::time::Instant::now() + limits.interrupt_grace);
                }
            }
            message = &mut *talking => return (message, false),
            () = tokio::time::sleep_until(kill_at) => {
                place.host().kill_for(place.watcher.load(Ordering::SeqCst));
                return (None, true);
            }
        }
    }
}

/// Why an event can't be sent: its key, or a batch item's, holds a secret the trigger read.
/// Keys are stored and shown masked, so two would be one (Grok's review of M4.11f).
fn secret_key(
    mask: &MaskSet,
    key: &str,
    data: &serde_json::Value,
    batched: bool,
) -> Option<String> {
    if mask.is_empty() {
        return None;
    }
    let items = data.as_array().filter(|_| batched).into_iter().flatten();
    let mut keys = std::iter::once(key).chain(items.filter_map(|item| item["key"].as_str()));
    keys.any(|key| mask.mask_text(key) != key).then(|| {
        "an event's key holds a secret the trigger read: keys are stored and shown masked, so \
         keys differing only in it would be one event; keep secrets out of keys"
            .to_owned()
    })
}
