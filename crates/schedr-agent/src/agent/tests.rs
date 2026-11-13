use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_master::core::{MasterCore, Settings};
use schedr_master::service::Hub;
use schedr_model::commands::Command;
use schedr_model::{DagId, RunState};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Endpoint, Server};

use schedr_model::runner::DAG_FILE_MISSING;

use super::*;

/// Each copy a cleanup was given, and whether it was there when it ran.
type CopiesSeen = Vec<(PathBuf, bool)>;

/// Parses each file into a DAG named after it, with one Python task; runs tasks at once,
/// except task `wait`, which waits until it is stopped. Each attempt's "process" is one that
/// doesn't exist.
#[derive(Clone, Default)]
struct Fake {
    parsed: Arc<Mutex<Vec<Vec<String>>>>,
    /// The directory of each parse, and each Python task's working directory.
    dirs: Arc<Mutex<Vec<PathBuf>>>,
    /// The handles of each cleanup left by an earlier agent.
    cleanups: Arc<Mutex<Vec<Vec<CleanupHandle>>>>,
    /// Why each task named `wait` was stopped.
    stops: Arc<Mutex<Vec<StopReason>>>,
    /// Each Python task's `sys.path`.
    sys_paths: Arc<Mutex<Vec<Vec<String>>>>,
    /// Each cleanup's other roots' copies, and whether each was there when it ran.
    cleanup_copies: Arc<Mutex<Vec<CopiesSeen>>>,
}

impl Executor for Fake {
    async fn run(
        &self,
        spec: AttemptSpec,
        plan: Plan,
        mut stop: Stops,
        spawned: Spawned,
        ended: TaskEnded,
    ) -> pb::Finished {
        if let Plan::Python { process, request } = &plan {
            self.dirs.lock().unwrap().push(process.working_dir.clone());
            self.sys_paths
                .lock()
                .unwrap()
                .push(request.sys_path.clone());
        }
        // A task named `cleaning` ends, and stays in its cleanup.
        if spec.task.id.as_str() == "cleaning" {
            spawned(NO_PROCESS, false);
            ended(pb::Finished {
                attempt_id: spec.attempt_id.clone(),
                status: pb::AttemptStatus::Success.into(),
                output_json: b"7".to_vec(),
                ..pb::Finished::default()
            });
            spawned(NO_PROCESS, false);
            std::future::pending::<()>().await;
        }
        // A trigger says what it found, from what it was told, and ends as its name says.
        if let Some((start, sink)) = &spec.trigger {
            let after = start.after.clone().unwrap_or_default();
            drop((sink.found)(crate::watchers::FoundEvent {
                key: format!("after-{after}"),
                data: json!(1),
                at: Some(jiff::Timestamp::UNIX_EPOCH),
            }));
            let (status, kind) = if spec.task.id.as_str() == "breaks" {
                (pb::AttemptStatus::Failed, "trigger_failed")
            } else {
                let reason = stop.next_reason().await;
                self.stops.lock().unwrap().push(reason);
                (pb::AttemptStatus::Cancelled, "")
            };
            (sink.poked)(Some("ValueError: not now".to_owned()));
            return pb::Finished {
                attempt_id: spec.attempt_id,
                status: status.into(),
                error_kind: kind.to_owned(),
                error_summary_json: if kind.is_empty() {
                    Vec::new()
                } else {
                    br#"{"exception": "ValueError", "message": "no", "traceback": ""}"#.to_vec()
                },
                ..pb::Finished::default()
            };
        }
        // A task named `forked` stands for one forked from a fork server.
        spawned(NO_PROCESS, spec.task.id.as_str() == "forked");
        let status = if spec.task.id.as_str() == "wait" {
            let reason = stop.next_reason().await;
            self.stops.lock().unwrap().push(reason);
            match reason {
                StopReason::Interrupted => pb::AttemptStatus::Interrupted,
                _ => pb::AttemptStatus::Cancelled,
            }
        } else {
            pb::AttemptStatus::Success
        };
        pb::Finished {
            attempt_id: spec.attempt_id,
            status: status.into(),
            output_json: b"null".to_vec(),
            ..pb::Finished::default()
        }
    }

    async fn cleanup(
        &self,
        spec: AttemptSpec,
        mut ended: pb::Finished,
        handles: Vec<CleanupHandle>,
        _spawned: Spawned,
    ) -> pb::Finished {
        tokio::task::yield_now().await; // a real cleanup starts a process
        let copies = spec
            .root_copies
            .values()
            .chain(&spec.script_dir)
            .map(|dir| (dir.clone(), dir.is_dir()))
            .collect();
        self.cleanup_copies.lock().unwrap().push(copies);
        self.cleanups.lock().unwrap().push(handles);
        ended.cleanup = Some(pb::CleanupResult {
            status: pb::CleanupStatus::Ok.into(),
            tries: 1,
            ..pb::CleanupResult::default()
        });
        ended
    }

    async fn parse(
        &self,
        root: String,
        dir: PathBuf,
        files: Vec<String>,
        _code: ParseCode,
    ) -> Result<Vec<ParsedFile>, String> {
        tokio::task::yield_now().await; // a real parse starts a runner
        self.dirs.lock().unwrap().push(dir.clone());
        self.parsed.lock().unwrap().push(files.clone());
        Ok(files
            .iter()
            .map(|path| {
                let stem = Path::new(path)
                    .file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                serde_json::from_value(json!({
                    "path": path,
                    "dags": [{
                        "produced_by": "1.0", "id": stem,
                        "location": {"root": root, "path": path},
                        "trigger": {"kind": "none"}, "initial_since": null, "max_active_runs": 1,
                        "tasks": [{
                            "id": stem, "kind": "python",
                            "settings": {
                                "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
                                "queue": "default", "max_output_size": 1_048_576
                            },
                            "output_type": {"type": "none"}
                        }],
                        "edges": []
                    }],
                    "error": null
                }))
                .unwrap()
            })
            .collect())
    }
}

const NO_PROCESS: ProcessIdentity = ProcessIdentity {
    pgid: i32::MAX,
    start_time: 1,
};

struct World {
    _dir: TempDir,
    root: PathBuf,
    db: Database,
    hub: Hub,
    fake: Fake,
    stop: watch::Sender<u32>,
    agent: tokio::task::JoinHandle<()>,
    metrics: AgentMetrics,
}

/// A master over SQLite, and an agent with a fake executor connected to it in process,
/// watching root `etl` (files `sync.py` and `wait.py`).
async fn world() -> World {
    world_in(|dir| {
        let root = dir.join("etl");
        std::fs::create_dir_all(&root).unwrap();
        root
    })
    .await
}

/// As [`world`], with root `etl` where `make_root` puts it in the test's directory (it
/// returns the configured path, e.g. a symbolic link to a revision as git-sync makes).
async fn world_in(make_root: impl FnOnce(&Path) -> PathBuf) -> World {
    world_timed(make_root, &crate::attempt::WorkerTimeouts::default()).await
}

/// As [`world_in`], the agent's timings `timeouts`.
async fn world_timed(
    make_root: impl FnOnce(&Path) -> PathBuf,
    timeouts: &crate::attempt::WorkerTimeouts,
) -> World {
    let dir = TempDir::new().unwrap();
    let root = make_root(dir.path());
    std::fs::write(root.join("sync.py"), "").unwrap();
    std::fs::write(root.join("wait.py"), "").unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let hub = Hub::new(MasterCore::new(db.clone(), Settings::default()));
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let service = hub.service();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
    });
    let mut client_io = Some(client_io);
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_| {
            let io = client_io.take();
            async move {
                io.map(hyper_util::rt::TokioIo::new)
                    .ok_or_else(|| std::io::Error::other("connected once"))
            }
        }))
        .await
        .unwrap();
    let client = Arc::new(Mutex::new(Some(MasterClient::new(channel))));
    let settings = AgentSettings {
        watcher_slots: 2,
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 2,
        setup: WorkerSetup {
            roots: BTreeMap::from([("etl".to_owned(), root.clone())]),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
            connection_dir: dir.path().join("connections"),
            memory: crate::memory::MemoryWatch::default(),
            timeouts: *timeouts,
            fork_server: None,
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.path().join("journal"),
        code_dir: dir.path().join("code"),
        drain: DrainPeriods {
            grace: Duration::from_secs(30),
            ..DrainPeriods::default()
        },
        log_endpoint: String::new(),
        node: NodeIdentity::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let fake = Fake::default();
    let metrics = AgentMetrics::default();
    let mut agent =
        Agent::new(settings, fake.clone(), "i1".to_owned()).with_metrics(metrics.clone());
    let (stop, stopped) = watch::channel(0);
    let agent = tokio::spawn(async move {
        let connect = move || {
            let client = client.lock().unwrap().take();
            async move { client.ok_or_else(|| "connected once".to_owned()) }
        };
        agent.run(connect, stopped).await;
    });
    World {
        _dir: dir,
        root,
        db,
        hub,
        fake,
        stop,
        agent,
        metrics,
    }
}

impl Drop for World {
    fn drop(&mut self) {
        // The code copies are read-only: made removable, for the temporary directory.
        let Some(dir) = self.root.parent() else {
            return;
        };
        let Ok(stores) = std::fs::read_dir(dir.join("code")) else {
            return;
        };
        for store in stores.flatten() {
            for copy in std::fs::read_dir(store.path())
                .into_iter()
                .flatten()
                .flatten()
            {
                let _ = crate::scan::remove_capture(&copy.path());
            }
        }
    }
}

/// Ticks the master until `check` holds (or fails after 10 s).
async fn eventually<F, Fut>(world: &World, what: &str, check: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..500 {
        world.hub.tick().await.unwrap();
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn dag_ids(db: &Database) -> Vec<String> {
    let mut ids: Vec<String> = db
        .stored_dags()
        .await
        .unwrap()
        .iter()
        .filter(|d| !d.removed)
        .map(|d| d.dag_id.to_string())
        .collect();
    ids.sort();
    ids
}

async fn trigger(db: &Database, dag: &str) -> String {
    let command = Command::TriggerRun {
        event: None,
        params: None,
        dag_id: DagId::new(dag).unwrap(),
        planned_for: None,
    };
    db.insert_command(&command, "anna", jiff::Timestamp::now())
        .await
        .unwrap()
}

async fn run_state(db: &Database, dag: &str) -> Option<RunState> {
    let runs = db
        .dag_runs(&DagId::new(dag).unwrap(), None, 1)
        .await
        .unwrap();
    runs.first().map(|r| r.state)
}

/// Asks the agent to stop and waits until it has drained.
async fn shut_down(mut world: World) {
    let _ = world.stop.send(1);
    let agent = std::mem::replace(&mut world.agent, tokio::spawn(async {}));
    tokio::time::timeout(Duration::from_secs(20), agent)
        .await
        .expect("the agent drains")
        .unwrap();
}

#[tokio::test]
async fn the_agent_registers_parses_its_root_and_runs_a_task_to_the_end() {
    let world = world().await;
    let db = world.db.clone();
    eventually(&world, "the DAGs to be parsed", || {
        let db = db.clone();
        async move { dag_ids(&db).await == ["sync", "wait"] }
    })
    .await;
    assert_eq!(world.fake.parsed.lock().unwrap()[0], ["sync.py", "wait.py"]);

    trigger(&db, "sync").await;
    eventually(&world, "the run to succeed", || {
        let db = db.clone();
        async move { run_state(&db, "sync").await == Some(RunState::Success) }
    })
    .await;
    shut_down(world).await;
}

#[tokio::test]
async fn a_start_shows_in_the_slots_gauge_and_its_stages_at_once() {
    // An alive tick an hour apart: only the start itself can move the gauge (M5.1a: stress
    // 23 read it for its starts and waited up to an alive interval for it).
    let timeouts = crate::attempt::WorkerTimeouts {
        alive: Duration::from_hours(1),
        ..crate::attempt::WorkerTimeouts::default()
    };
    let world = world_timed(
        |dir| {
            let root = dir.join("etl");
            std::fs::create_dir_all(&root).unwrap();
            root
        },
        &timeouts,
    )
    .await;
    let db = world.db.clone();
    eventually(&world, "the DAGs to be parsed", || {
        let db = db.clone();
        async move { dag_ids(&db).await.len() == 2 }
    })
    .await;
    assert_eq!(world.metrics.slots_used.get(), 0);
    trigger(&db, "wait").await;
    let metrics = world.metrics.clone();
    eventually(&world, "the slot to show as used", || {
        let metrics = metrics.clone();
        async move { metrics.slots_used.get() == 1 }
    })
    .await;
    let mut out = String::new();
    let mut registry = prometheus_client::registry::Registry::with_prefix("schedr");
    world.metrics.register(&mut registry);
    prometheus_client::encoding::text::encode(&mut out, &registry).unwrap();
    for line in [
        "schedr_worker_assign_to_started_seconds_count 1",
        "schedr_worker_journal_write_seconds_count{record=\"start\"} 1",
    ] {
        assert!(out.contains(line), "{line}\n{out}");
    }
    // Not drained: a drain goes by the alive tick, an hour away here.
    world.agent.abort();
}

#[tokio::test]
async fn a_cancel_reaches_the_running_attempt() {
    let world = world().await;
    let db = world.db.clone();
    eventually(&world, "the DAGs to be parsed", || {
        let db = db.clone();
        async move { dag_ids(&db).await.len() == 2 }
    })
    .await;
    trigger(&db, "wait").await;
    eventually(&world, "the attempt to start", || {
        let db = db.clone();
        async move { !db.open_attempts(Some("w1")).await.unwrap().is_empty() }
    })
    .await;
    let runs = db
        .dag_runs(&DagId::new("wait").unwrap(), None, 1)
        .await
        .unwrap();
    let cancel = Command::CancelRun {
        run_id: runs[0].id.clone(),
    };
    db.insert_command(&cancel, "anna", jiff::Timestamp::now())
        .await
        .unwrap();
    eventually(&world, "the run to be cancelled", || {
        let db = db.clone();
        async move { run_state(&db, "wait").await == Some(RunState::Cancelled) }
    })
    .await;
    shut_down(world).await;
}

#[tokio::test]
async fn a_new_file_gives_a_new_hash_and_a_new_parse() {
    let world = world().await;
    let db = world.db.clone();
    eventually(&world, "the DAGs to be parsed", || {
        let db = db.clone();
        async move { dag_ids(&db).await.len() == 2 }
    })
    .await;
    std::fs::write(world.root.join("load.py"), "").unwrap();
    eventually(&world, "the new DAG", || {
        let db = db.clone();
        async move { dag_ids(&db).await == ["load", "sync", "wait"] }
    })
    .await;
    assert_eq!(world.fake.parsed.lock().unwrap().len(), 2);
    shut_down(world).await;
}

/// A master that answers `Register` with prepared messages and records what the agent sends.
#[derive(Clone, Default)]
struct Scripted {
    script: Vec<pb::MasterMessage>,
    received: Arc<Mutex<Vec<pb::WorkerMessage>>>,
    /// Also assign a parse of each root `Register` reports, at the hash it reports.
    parse_registered: bool,
    /// Each of those parses assigned twice, as to a worker connecting again (B235).
    parse_twice: bool,
    /// How long an upload takes to answer.
    upload_delay: Duration,
    /// The heartbeats that arrived while each upload waited.
    during_uploads: Arc<Mutex<Vec<usize>>>,
    /// Each parse result uploaded, put back together.
    uploads: Arc<Mutex<Vec<pb::ParseResult>>>,
    /// `FetchInputs` never answers.
    inputs_stuck: bool,
    /// How long `FetchInputs` takes to send its chunk.
    inputs_after: Duration,
    /// How long the stream takes to open.
    answer_after: Duration,
    /// Messages sent after the script, this long after it (held on their way, B163).
    late: Option<(Duration, Vec<pb::MasterMessage>)>,
    /// Each heartbeat answered with a `HeartbeatAck`, as the master does, once this long has
    /// passed since the stream opened (before it, none: a paused master).
    ack_heartbeats: Option<Duration>,
    /// The lease epoch on its stamps (decision 444).
    epoch: i64,
    /// How far its wall clock runs ahead of the worker's, in milliseconds (decision 449).
    wall_ahead_ms: i64,
}

#[tonic::async_trait]
impl pb::master_server::Master for Scripted {
    type ControlStream = ReceiverStream<Result<pb::MasterMessage, tonic::Status>>;
    type FetchInputsStream = ReceiverStream<Result<pb::InputChunk, tonic::Status>>;

    async fn control(
        &self,
        request: tonic::Request<tonic::Streaming<pb::WorkerMessage>>,
    ) -> Result<tonic::Response<Self::ControlStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        tokio::time::sleep(self.answer_after).await;
        let (sender, stream) = mpsc::channel(64);
        // Master time: milliseconds since the stream opened, on one timeline; each message
        // names the latest heartbeat heard (B225), as the master's do.
        let opened = Instant::now();
        let heard = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let epoch = self.epoch;
        let wall_ahead_ms = self.wall_ahead_ms;
        let stamp = {
            let heard = Arc::clone(&heard);
            move |mut message: pb::MasterMessage| {
                message.clock = Some(pb::MasterClock {
                    timeline: 7,
                    master_ms: i64::try_from(opened.elapsed().as_millis()).unwrap(),
                    heard_sequence: heard.load(std::sync::atomic::Ordering::SeqCst),
                    wall_ms: i64::try_from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    )
                    .unwrap()
                        + wall_ahead_ms,
                    lease_epoch: epoch,
                    sequence: 0,
                });
                message
            }
        };
        for message in self.script.clone() {
            sender.send(Ok(stamp(message))).await.unwrap();
        }
        if self.parse_registered {
            let first = inbound.message().await?.expect("a register first");
            if let Some(worker_message::Message::Register(register)) = &first.message {
                for root in &register.roots {
                    for _ in 0..if self.parse_twice { 2 } else { 1 } {
                        let job = master_message::Message::AssignParse(pb::AssignParse {
                            parse_job_id: format!("job-{}", root.root),
                            root: Some(root.clone()),
                        });
                        sender.send(Ok(stamp(to_worker(job)))).await.unwrap();
                    }
                }
            }
            self.received.lock().unwrap().push(first);
        }
        let received = Arc::clone(&self.received);
        if let Some((after, late)) = self.late.clone() {
            let sender = sender.clone();
            let stamp = stamp.clone();
            tokio::spawn(async move {
                tokio::time::sleep(after).await;
                for message in late {
                    let _ = sender.send(Ok(stamp(message))).await;
                }
            });
        }
        let ack_heartbeats = self.ack_heartbeats;
        tokio::spawn(async move {
            while let Ok(Some(message)) = inbound.message().await {
                let beat = match &message.message {
                    Some(worker_message::Message::Heartbeat(_)) => {
                        message.clock.as_ref().map(|clock| clock.sequence)
                    }
                    _ => None,
                };
                received.lock().unwrap().push(message);
                let acking = ack_heartbeats.is_some_and(|after| opened.elapsed() >= after);
                if let Some(sequence) = beat.filter(|_| acking) {
                    heard.fetch_max(sequence, std::sync::atomic::Ordering::SeqCst);
                    let ack = master_message::Message::HeartbeatAck(pb::HeartbeatAck {});
                    let _ = sender.send(Ok(stamp(to_worker(ack)))).await;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(stream)))
    }

    async fn upload_parse_result(
        &self,
        request: tonic::Request<tonic::Streaming<pb::ParseResultPart>>,
    ) -> Result<tonic::Response<pb::ParseResultAck>, tonic::Status> {
        let mut parts = request.into_inner();
        let mut upload = schedr_proto::convert::ParseUpload::default();
        while let Some(part) = parts.message().await? {
            upload.add(part).unwrap();
        }
        self.uploads.lock().unwrap().push(upload.finish().unwrap());
        if !self.upload_delay.is_zero() {
            let before = heartbeats(&self.received.lock().unwrap());
            tokio::time::sleep(self.upload_delay).await;
            let after = heartbeats(&self.received.lock().unwrap());
            self.during_uploads.lock().unwrap().push(after - before);
        }
        Ok(tonic::Response::new(pb::ParseResultAck {}))
    }

    async fn fetch_inputs(
        &self,
        _request: tonic::Request<pb::FetchInputsRequest>,
    ) -> Result<tonic::Response<Self::FetchInputsStream>, tonic::Status> {
        if self.inputs_stuck {
            std::future::pending::<()>().await;
        }
        let (sender, receiver) = mpsc::channel(1);
        tokio::time::sleep(self.inputs_after).await;
        sender
            .send(Ok(pb::InputChunk {
                data: br#"{"extract": 3}"#.to_vec(),
            }))
            .await
            .unwrap();
        Ok(tonic::Response::new(ReceiverStream::new(receiver)))
    }
}

fn to_worker(message: master_message::Message) -> pb::MasterMessage {
    pb::MasterMessage {
        message: Some(message),
        clock: None,
    }
}

fn assign(
    attempt: &str,
    task: &serde_json::Value,
    path: &str,
    external: bool,
) -> pb::MasterMessage {
    to_worker(master_message::Message::AssignTask(pb::AssignTask {
        attempt_id: attempt.to_owned(),
        task_id: "t".to_owned(),
        map_index: -1,
        try_number: 1,
        run: Some(pb::RunInfo {
            event_json: Vec::new(),
            params_json: Vec::new(),
            run_id: "r1".to_owned(),
            kind: "manual".to_owned(),
            planned_for: Some(prost_types::Timestamp::default()),
            since: Some(prost_types::Timestamp::default()),
            dag_id: "sync".to_owned(),
        }),
        task_json: serde_json::to_vec(task).unwrap(),
        dag_file: Some(pb::DagFileRef {
            root: "etl".to_owned(),
            path: path.to_owned(),
        }),
        inputs_external: external,
        ..pb::AssignTask::default()
    }))
}

fn task_json(kind: &serde_json::Value) -> serde_json::Value {
    let mut task = json!({
        "id": "t",
        "settings": {
            "retries": 0, "retry_delay_ms": 0, "timeout_ms": null,
            "queue": "default", "max_output_size": 1024
        },
        "output_type": {"type": "any"}
    });
    for (key, value) in kind.as_object().unwrap() {
        task[key] = value.clone();
    }
    task
}

/// A task with a cleanup of `timeout_ms`.
fn task_with_cleanup(timeout_ms: u64) -> serde_json::Value {
    let mut task = task_json(&json!({"kind": "python"}));
    task["cleanup"] = json!({
        "run": "python", "retries": 0, "retry_delay_ms": 0, "timeout_ms": timeout_ms,
    });
    task
}

/// What the journal keeps of an attempt of [`task_with_cleanup`].
fn kept(attempt_id: &str) -> KeptAttempt {
    let task = task_with_cleanup(1000);
    let message = assign(attempt_id, &task, "sync.py", false);
    let Some(master_message::Message::AssignTask(assign)) = message.message else {
        panic!("expected an assignment")
    };
    KeptAttempt {
        assign,
        code_dir: None,
        code_hash: None,
        root_copies: BTreeMap::new(),
        script_dir: None,
    }
}

fn one_handle() -> CleanupHandle {
    CleanupHandle {
        handle_type: "Job".to_owned(),
        value: json!({"id": "j-1"}),
    }
}

/// The journal an agent starts with.
enum Start {
    /// These records (none: a new worker).
    With(Vec<(&'static str, JournalRecord)>),
    /// These records, each of an attempt of a task with a cleanup, and its one handle.
    Kept(Vec<(&'static str, JournalRecord)>),
    /// A moved cleanup left by the previous agent: its handles in its assignment alone.
    Moved(&'static str),
    /// A journal that can't be written.
    Unwritable,
}

/// Runs an agent with `slots` and journal `start` against a master playing `script`; returns
/// what the agent sent once `done` holds for it, and the attempts left in its journal.
async fn against_script(
    slots: u32,
    start: Start,
    script: Vec<pb::MasterMessage>,
    done: impl Fn(&[pb::WorkerMessage]) -> bool,
) -> (Vec<pb::WorkerMessage>, Vec<String>) {
    against_script_with(slots, start, script, done, |_| {}).await
}

/// [`against_script`], with `tweak` applied to the agent first.
async fn against_script_with(
    slots: u32,
    start: Start,
    script: Vec<pb::MasterMessage>,
    done: impl Fn(&[pb::WorkerMessage]) -> bool,
    tweak: impl FnOnce(&mut Agent<Fake>),
) -> (Vec<pb::WorkerMessage>, Vec<String>) {
    let master = Scripted {
        script,
        ..Scripted::default()
    };
    against_master(slots, start, master, done, tweak).await
}

/// Leaves in the journal what the agent starts with.
fn seed(journal: &Journal, start: Start) {
    match start {
        Start::With(records) => {
            for (attempt_id, record) in records {
                journal.write(attempt_id, &record).unwrap();
            }
        }
        Start::Kept(records) => {
            for (attempt_id, record) in records {
                journal.write(attempt_id, &record).unwrap();
                journal.keep(attempt_id, &kept(attempt_id)).unwrap();
                journal.add_handle(attempt_id, &one_handle()).unwrap();
            }
        }
        Start::Moved(attempt_id) => {
            let mut left = kept(attempt_id);
            left.assign.cleanup_only = Some(pb::CleanupOnly {
                handle_json: vec![serde_json::to_vec(&one_handle()).unwrap()],
            });
            journal
                .write(attempt_id, &JournalRecord::Running(None))
                .unwrap();
            journal.keep(attempt_id, &left).unwrap();
        }
        Start::Unwritable => std::fs::write(journal.dir(), "a file").unwrap(),
    }
}

/// [`against_script_with`], against a prepared master.
async fn against_master(
    slots: u32,
    start: Start,
    master: Scripted,
    done: impl Fn(&[pb::WorkerMessage]) -> bool,
    tweak: impl FnOnce(&mut Agent<Fake>),
) -> (Vec<pb::WorkerMessage>, Vec<String>) {
    let dir = TempDir::new().unwrap();
    let journal = Journal::new(dir.path().join("journal"));
    seed(&journal, start);
    let root = dir.path().join("etl");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("sync.py"), "").unwrap();
    let received = Arc::clone(&master.received);
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::master_server::MasterServer::new(master))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_io)))
            .await
    });
    let mut client_io = Some(client_io);
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_| {
            let io = client_io.take();
            async move {
                io.map(hyper_util::rt::TokioIo::new)
                    .ok_or_else(|| std::io::Error::other("connected once"))
            }
        }))
        .await
        .unwrap();
    let settings = AgentSettings {
        watcher_slots: 2,
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots,
        setup: WorkerSetup {
            roots: BTreeMap::from([("etl".to_owned(), root)]),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
            connection_dir: dir.path().join("connections"),
            memory: crate::memory::MemoryWatch::default(),
            timeouts: crate::attempt::WorkerTimeouts::default(),
            fork_server: None,
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: journal.dir().to_path_buf(),
        code_dir: journal.dir().parent().unwrap().join("code"),
        drain: DrainPeriods {
            grace: Duration::from_secs(30),
            ..DrainPeriods::default()
        },
        log_endpoint: "http://w1:8081".to_owned(),
        node: NodeIdentity {
            process_id: "p-1".to_owned(),
            config_json: r#"{"worker":{"slots":2}}"#.to_owned(),
        },
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    assert_eq!(agent.running(), Vec::<String>::new());
    tweak(&mut agent);
    let session = tokio::spawn(async move {
        agent.recover().await;
        agent.session(MasterClient::new(channel)).await
    });
    for _ in 0..1000 {
        let seen = received.lock().unwrap().clone();
        if done(&seen) {
            session.abort();
            let _ = session.await;
            let left = journal.records().map_or_else(
                |_| Vec::new(),
                |records| records.into_iter().map(|(id, _)| id).collect(),
            );
            return (seen, left);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "the agent didn't send what was expected: {:?}",
        received.lock().unwrap()
    );
}

/// What the agent sent, by kind; heartbeats, warnings and results sent again (B41: until
/// acknowledged, which these scripted masters seldom do) left out.
fn kinds(messages: &[pb::WorkerMessage]) -> Vec<&'static str> {
    let mut results = BTreeSet::new();
    messages
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Register(_) => Some("register"),
            worker_message::Message::Heartbeat(_) | worker_message::Message::Warnings(_) => None,
            worker_message::Message::TriggerEvent(_) => Some("trigger event"),
            worker_message::Message::TriggerStopped(_) => Some("trigger stopped"),
            worker_message::Message::TriggerPoked(_) => Some("trigger poked"),
            worker_message::Message::CodeVersionChanged(_) => Some("code"),
            worker_message::Message::Started(_) => Some("started"),
            worker_message::Message::TaskEnded(_) => Some("task ended"),
            worker_message::Message::HandlesAdded(_) => Some("handles"),
            worker_message::Message::ApiCall(_) => Some("api call"),
            worker_message::Message::Rejected(r) => {
                Some(match pb::RejectReason::try_from(r.reason) {
                    Ok(pb::RejectReason::NoFreeSlot) => "rejected: no slot",
                    Ok(pb::RejectReason::InvalidSpecification) => "rejected: invalid",
                    Ok(pb::RejectReason::DagFileMissing) => "rejected: missing",
                    Ok(pb::RejectReason::DiskNotWritable) => "rejected: disk",
                    Ok(pb::RejectReason::CodeMismatch) => "rejected: code",
                    Ok(pb::RejectReason::Draining) => "rejected: draining",
                    _ => "rejected",
                })
            }
            worker_message::Message::Finished(f) if !results.insert(&f.attempt_id) => None,
            worker_message::Message::Finished(f) => {
                Some(match pb::AttemptStatus::try_from(f.status) {
                    Ok(pb::AttemptStatus::Success) => "finished: success",
                    Ok(pb::AttemptStatus::Interrupted) => "finished: interrupted",
                    _ => "finished: failed",
                })
            }
            worker_message::Message::Draining(_) => Some("draining"),
            worker_message::Message::Deregister(_) => Some("leaving"),
            worker_message::Message::ConnectionJobDone(_) => Some("connections"),
        })
        .collect()
}

/// The one `Rejected` among `seen`.
fn rejection(seen: &[pb::WorkerMessage]) -> &pb::Rejected {
    seen.iter()
        .find_map(|m| match m.message.as_ref()? {
            worker_message::Message::Rejected(rejected) => Some(rejected),
            _ => None,
        })
        .expect("expected Rejected")
}

#[tokio::test]
async fn without_a_free_slot_an_assignment_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let (seen, _) = against_script(0, Start::With(vec![]), script, |m| kinds(m).len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: no slot"]);
    let worker_message::Message::Register(register) = seen[0].message.as_ref().unwrap() else {
        panic!("expected Register")
    };
    assert_eq!((register.worker_id.as_str(), register.slots), ("w1", 0));
    assert_eq!(register.log_endpoint, "http://w1:8081");
    assert_eq!(register.process_id, "p-1");
    assert_eq!(register.config_json, r#"{"worker":{"slots":2}}"#);
    assert_eq!(register.roots[0].root, "etl");
    assert!(register.roots[0].content_hash.starts_with("sha256:"));
}

#[tokio::test]
async fn bad_and_impossible_assignments_are_rejected_or_failed_and_good_ones_run() {
    let timing = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![
            pb::JournalDecision {
                cleanup_from: 0,
                attempt_id: "old-1".to_owned(),
                decision: pb::AttemptDecision::Ack.into(),
            },
            pb::JournalDecision {
                cleanup_from: 0,
                attempt_id: "old-2".to_owned(),
                decision: pb::AttemptDecision::Kill.into(),
            },
            pb::JournalDecision {
                cleanup_from: 0,
                attempt_id: "old-3".to_owned(),
                decision: pb::AttemptDecision::Continue.into(),
            },
        ],
    }));
    let mut invalid = assign("a1", &json!({}), "sync.py", false);
    if let Some(master_message::Message::AssignTask(a)) = invalid.message.as_mut() {
        a.task_json = b"not json".to_vec();
    }
    let python = task_json(&json!({"kind": "python"}));
    let ghost_ref = json!({"kind": "command", "command": {"shell": [{"ref": {"kind": "output", "task": "ghost"}}]}});
    let echo = json!({"kind": "command", "command": {"argv": [[{"text": "true"}]]}});
    let script = vec![
        timing,
        invalid,
        assign("a2", &python, "gone.py", false),
        assign("a3", &task_json(&ghost_ref), "sync.py", false),
        assign("a4", &task_json(&echo), "sync.py", true),
        to_worker(master_message::Message::AssignParse(pb::AssignParse {
            parse_job_id: "j1".to_owned(),
            root: Some(pb::RootHash {
                kind: pb::RootKind::Dag.into(),
                root: "ml".to_owned(),
                content_hash: "h".to_owned(),
            }),
        })),
        to_worker(master_message::Message::AssignParse(pb::AssignParse {
            parse_job_id: "j2".to_owned(),
            root: Some(pb::RootHash {
                kind: pb::RootKind::Dag.into(),
                root: "etl".to_owned(),
                content_hash: "sha256:old".to_owned(),
            }),
        })),
        to_worker(master_message::Message::AssignParse(pb::AssignParse {
            parse_job_id: "j3".to_owned(),
            root: None,
        })),
        to_worker(master_message::Message::CancelAttempt(pb::CancelAttempt {
            interrupted: false,
            attempt_id: "nobody".to_owned(),
        })),
        to_worker(master_message::Message::FinishedAck(pb::FinishedAck {
            attempt_id: "a4".to_owned(),
        })),
    ];
    let done = |m: &[pb::WorkerMessage]| {
        let k = kinds(m);
        k.iter().filter(|k| k.starts_with("finished")).count() == 2
            && m.iter()
                .any(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_))))
    };
    let (seen, left) = against_script(2, Start::With(vec![]), script, done).await;
    // Results arrive from background work, so only the order of what is sent at once holds.
    let mut sent = kinds(&seen);
    sent.sort_unstable();
    assert_eq!(
        sent,
        [
            "finished: failed",
            "finished: success",
            "register",
            "rejected: invalid",
            "rejected: missing",
            "started",
            "started",
        ]
    );
    assert_eq!(
        kinds(&seen)[..3],
        ["register", "rejected: invalid", "rejected: missing"]
    );
    assert!(
        left.contains(&"a3".to_owned()),
        "a result is kept until it is acknowledged: {left:?}"
    );
    assert_results_name_their_code(&seen, 2);
}

fn heartbeats(messages: &[pb::WorkerMessage]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_))))
        .count()
}

fn success(attempt_id: &str) -> JournalRecord {
    JournalRecord::Finished(pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    })
}

#[tokio::test]
async fn a_restarted_agent_reports_its_results_again_and_forgets_what_was_running() {
    let start = Start::With(vec![
        ("old-1", success("old-1")),
        ("old-2", success("old-2")),
        ("old-3", JournalRecord::Running(Some(NO_PROCESS))),
        ("old-4", JournalRecord::Running(None)),
    ]);
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![
            pb::JournalDecision {
                cleanup_from: 0,
                attempt_id: "old-1".to_owned(),
                decision: pb::AttemptDecision::Ack.into(),
            },
            pb::JournalDecision {
                cleanup_from: 0,
                attempt_id: "old-2".to_owned(),
                decision: pb::AttemptDecision::Continue.into(),
            },
        ],
    }));
    // A second heartbeat comes after the decisions were followed (the new heartbeat interval
    // ticks at once).
    let done = |m: &[pb::WorkerMessage]| kinds(m).len() >= 2 && heartbeats(m) >= 2;
    let (seen, left) = against_script(2, start, vec![registered], done).await;
    assert_eq!(kinds(&seen), ["register", "finished: success"]);
    let worker_message::Message::Register(register) = seen[0].message.as_ref().unwrap() else {
        panic!("expected Register")
    };
    let finished = i32::from(pb::JournalState::Finished);
    let journal: Vec<(&str, i32)> = register
        .journal
        .iter()
        .map(|e| (e.attempt_id.as_str(), e.state))
        .collect();
    assert_eq!(journal, [("old-1", finished), ("old-2", finished)]);
    assert_eq!(left, ["old-2"]);
}

#[tokio::test]
async fn a_result_sent_again_fits_the_limit_the_master_has_now() {
    // B47: a result journalled under a larger limit, reported to a master accepting less:
    // sent as stored, it would end the stream on every send.
    let big = JournalRecord::Finished(pb::Finished {
        attempt_id: "old-1".to_owned(),
        status: pb::AttemptStatus::Success.into(),
        output_json: vec![b'1'; 4096],
        ..pb::Finished::default()
    });
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 1,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![pb::JournalDecision {
            cleanup_from: 0,
            attempt_id: "old-1".to_owned(),
            decision: pb::AttemptDecision::Continue.into(),
        }],
    }));
    let finished = |m: &[pb::WorkerMessage]| -> Vec<pb::Finished> {
        m.iter()
            .filter_map(|m| match &m.message {
                Some(worker_message::Message::Finished(f)) => Some(f.clone()),
                _ => None,
            })
            .collect()
    };
    // Its `Continue`, then a resend.
    let done = |m: &[pb::WorkerMessage]| finished(m).len() >= 2;
    let start = Start::With(vec![("old-1", big)]);
    let (seen, left) = against_script(2, start, vec![registered], done).await;
    for sent in finished(&seen) {
        assert_eq!(sent.status, i32::from(pb::AttemptStatus::Failed));
        assert_eq!(sent.error_kind, "output_too_large");
        assert_eq!(sent.output_json, Vec::<u8>::new());
    }
    assert_eq!(left, ["old-1"]);
}

#[tokio::test]
async fn a_result_not_acknowledged_is_sent_again_while_the_link_stays_up() {
    // B41: the master failed to store it (a database stall) and keeps the stream; nothing
    // but a reconnect sent it again.
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![pb::JournalDecision {
            cleanup_from: 0,
            attempt_id: "old-1".to_owned(),
            decision: pb::AttemptDecision::Continue.into(),
        }],
    }));
    let finished = |m: &[pb::WorkerMessage]| {
        m.iter()
            .filter(|m| matches!(m.message, Some(worker_message::Message::Finished(_))))
            .count()
    };
    let start = Start::With(vec![("old-1", success("old-1"))]);
    let done = |m: &[pb::WorkerMessage]| finished(m) >= 3 || heartbeats(m) >= 20;
    let (seen, left) = against_script(1, start, vec![registered], done).await;
    assert!(finished(&seen) >= 3, "{:?}", kinds(&seen));
    // Not with every heartbeat: one interval at least between two sends.
    assert!(heartbeats(&seen) >= 2, "{:?}", kinds(&seen));
    assert_eq!(left, ["old-1"]);
}

/// Assignments of a Python task `wait` (it runs until stopped), one per ID.
fn waits(ids: &[&str]) -> Vec<pb::MasterMessage> {
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("wait");
    ids.iter()
        .map(|id| assign(id, &task, "sync.py", false))
        .collect()
}

/// How many times the journal kept a group of start records.
fn start_groups(metrics: &AgentMetrics) -> u64 {
    let mut registry = prometheus_client::registry::Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    let mut out = String::new();
    prometheus_client::encoding::text::encode(&mut out, &registry).unwrap();
    out.lines()
        .find_map(|line| {
            line.strip_prefix("schedr_worker_journal_write_seconds_count{record=\"start\"} ")
        })
        .map_or(0, |count| count.trim().parse().unwrap())
}

#[tokio::test]
async fn a_burst_of_assignments_is_journaled_as_one_group_then_started() {
    // M5.1b: the records of assignments already waiting are written together, each `Started`
    // only after the group is on disk.
    let metrics = AgentMetrics::default();
    let given = metrics.clone();
    let started = |m: &[pb::WorkerMessage]| kinds(m).iter().filter(|k| **k == "started").count();
    let (seen, left) = against_script_with(
        3,
        Start::With(vec![]),
        waits(&["a1", "a2", "a3"]),
        |m| started(m) >= 3,
        |agent| agent.metrics = given,
    )
    .await;
    assert_eq!(kinds(&seen), ["register", "started", "started", "started"]);
    assert_eq!(start_groups(&metrics), 1, "one group of three records");
    assert_eq!(left, ["a1", "a2", "a3"]);
}

#[tokio::test]
async fn a_burst_takes_no_more_slots_than_are_free() {
    // M5.1b: a group's assignments hold their slots before they are started.
    let (seen, left) = against_script(2, Start::With(vec![]), waits(&["a1", "a2", "a3"]), |m| {
        kinds(m).len() >= 4
    })
    .await;
    // The refusal goes as the group is gathered, before the group's `Started`s: each
    // attempt's own messages keep their order, which is what the master reads.
    assert_eq!(
        kinds(&seen),
        ["register", "rejected: no slot", "started", "started"]
    );
    assert_eq!(left, ["a1", "a2"]);
}

#[tokio::test]
async fn a_burst_the_journal_cant_record_starts_nothing() {
    // M5.1b: no `Started` for a record not on disk, each assignment refused on its own.
    let (seen, left) = against_script(3, Start::Unwritable, waits(&["a1", "a2"]), |m| {
        kinds(m).len() >= 3
    })
    .await;
    assert_eq!(
        kinds(&seen),
        ["register", "rejected: disk", "rejected: disk"]
    );
    assert!(left.is_empty(), "{left:?}");
}

#[tokio::test]
async fn an_attempt_the_journal_cant_record_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let (seen, _) = against_script(2, Start::Unwritable, script, |m| kinds(m).len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: disk"]);
    let rejected = rejection(&seen);
    assert!(
        rejected.detail.starts_with("can't write the journal in `"),
        "{}",
        rejected.detail
    );
}

#[test]
fn backoffs_have_jitter_between_half_and_all() {
    let backoff = Duration::from_secs(10);
    assert_eq!(jittered(backoff, 0), Duration::from_secs(5));
    assert_eq!(jittered(backoff, u32::MAX), backoff);
    let middle = jittered(backoff, u32::MAX / 2);
    assert!(middle > Duration::from_millis(7_400) && middle < Duration::from_millis(7_600));
    assert!(FIRST_BACKOFF < MAX_BACKOFF);
}

#[tokio::test]
async fn the_workers_warnings_go_after_register_and_on_every_change() {
    let warning = |message: &str| pb::WorkerWarning {
        kind: "fork_server_disabled".to_owned(),
        interpreter: "python".to_owned(),
        message: message.to_owned(),
    };
    let (report, warnings) = watch::channel(vec![warning("at the start")]);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        report.send_replace(vec![]);
        // Kept: a dropped sender would only stop the reports.
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let reported = |m: &[pb::WorkerMessage]| -> Vec<Vec<String>> {
        m.iter()
            .filter_map(|m| match m.message.as_ref()? {
                worker_message::Message::Warnings(w) => {
                    Some(w.warnings.iter().map(|w| w.message.clone()).collect())
                }
                _ => None,
            })
            .collect()
    };
    let (seen, _) = against_script_with(
        1,
        Start::With(vec![]),
        vec![],
        |m| reported(m).len() >= 2,
        |agent| agent.warnings = warnings,
    )
    .await;
    assert_eq!(kinds(&seen)[0], "register");
    assert_eq!(
        reported(&seen),
        [vec!["at the start".to_owned()], Vec::<String>::new()]
    );
}

#[tokio::test]
async fn a_master_wall_clock_far_off_is_warned_of_once_a_session() {
    // Decision 449 (B232): 30 s ahead, past the 2 s threshold: one `clock_skew` warning in the
    // report, however many heartbeats are answered.
    let mut script = silent_master_leased(false, 5000);
    script.truncate(1);
    let master = Scripted {
        script,
        ack_heartbeats: Some(Duration::ZERO),
        wall_ahead_ms: 30_000,
        ..Scripted::default()
    };
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(800);
    let (seen, _) = against_master(1, Start::With(vec![]), master, done, |_| {}).await;
    let reports: Vec<Vec<String>> = seen
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Warnings(w) => {
                Some(w.warnings.iter().map(|w| w.kind.clone()).collect())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        reports,
        [vec![], vec!["clock_skew".to_owned()]],
        "{:?}",
        kinds(&seen)
    );
    let beats = seen
        .iter()
        .filter(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_))))
        .count();
    assert!(beats >= 3, "answered more than once: {:?}", kinds(&seen));
}

#[tokio::test]
async fn a_forked_attempt_sends_started_again_with_its_method() {
    let timing = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![],
    }));
    let mut forked =
        task_json(&json!({"kind": "command", "command": {"argv": [[{"text": "true"}]]}}));
    forked["id"] = json!("forked");
    let script = vec![timing, assign("a1", &forked, "sync.py", false)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).len() >= 4;
    let (seen, _) = against_script(2, Start::With(vec![]), script, done).await;
    assert_eq!(
        kinds(&seen),
        ["register", "started", "started", "finished: success"]
    );
    let methods: Vec<_> = seen
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Started(s) => Some(s.start_method),
            _ => None,
        })
        .collect();
    assert_eq!(
        methods,
        [
            i32::from(pb::StartMethod::Spawn),
            i32::from(pb::StartMethod::Fork)
        ]
    );
}

#[tokio::test]
async fn an_attempt_assigned_again_is_ignored() {
    let timing = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![],
    }));
    let echo = task_json(&json!({"kind": "command", "command": {"argv": [[{"text": "true"}]]}}));
    let script = vec![
        timing,
        assign("a1", &echo, "sync.py", false),
        assign("a1", &echo, "sync.py", false),
    ];
    let done = |m: &[pb::WorkerMessage]| kinds(m).len() >= 3 && heartbeats(m) >= 4;
    let (seen, left) = against_script(2, Start::With(vec![]), script, done).await;
    assert_eq!(kinds(&seen), ["register", "started", "finished: success"]);
    assert_eq!(left, ["a1"]);
}

#[tokio::test]
async fn what_ends_while_the_master_is_away_is_journalled() {
    let dir = TempDir::new().unwrap();
    let settings = AgentSettings {
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 1,
        watcher_slots: 2,
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
            connection_dir: dir.path().join("connections"),
            memory: crate::memory::MemoryWatch::default(),
            timeouts: crate::attempt::WorkerTimeouts::default(),
            fork_server: None,
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.path().join("journal"),
        code_dir: dir.path().join("code"),
        drain: DrainPeriods {
            grace: Duration::from_secs(30),
            ..DrainPeriods::default()
        },
        log_endpoint: String::new(),
        node: NodeIdentity::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let journal = Journal::new(settings.journal_dir.clone());
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    let done = agent.done_tx.clone();
    done.send(Done::Spawned {
        attempt_id: "a1".to_owned(),
        identity: NO_PROCESS,
        forked: false,
    })
    .unwrap();
    done.send(Done::Spawned {
        attempt_id: "a2".to_owned(),
        identity: NO_PROCESS,
        forked: true,
    })
    .unwrap();
    let JournalRecord::Finished(finished) = success("a2") else {
        unreachable!()
    };
    done.send(Done::Parse {
        job_id: "j1".to_owned(),
        root: pb::RootHash {
            kind: pb::RootKind::Dag.into(),
            root: "etl".to_owned(),
            content_hash: "sha256:h".to_owned(),
        },
        copies: vec!["sha256:h".to_owned()],
        files: Ok(vec![]),
    })
    .unwrap();
    done.send(Done::Attempt(finished)).unwrap();
    let expected = [
        ("a1".to_owned(), JournalRecord::Running(Some(NO_PROCESS))),
        ("a2".to_owned(), success("a2")),
    ];
    let recorded = async {
        for _ in 0..500 {
            if journal.records().unwrap_or_default() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("not journalled: {:?}", journal.records());
    };
    agent.offline(recorded).await;
    let finished = i32::from(pb::JournalState::Finished);
    assert_eq!(
        agent.journal_entries(),
        [pb::JournalEntry {
            attempt_id: "a2".to_owned(),
            state: finished,
            handles: 0,
        }],
        "reported in the next `Register`"
    );
    // B169 (Codex's review): a handle journaled before the loop hears of it is counted.
    agent.running.insert("a3".to_owned(), Stops::new().0);
    journal.add_handle("a3", &one_handle()).unwrap();
    let a3 = agent
        .journal_entries()
        .into_iter()
        .find(|e| e.attempt_id == "a3")
        .unwrap();
    assert_eq!(a3.handles, 1);
}

#[tokio::test]
async fn an_assignment_for_other_code_than_the_workers_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let mut stale = assign("a1", &python, "sync.py", false);
    if let Some(master_message::Message::AssignTask(a)) = stale.message.as_mut() {
        a.expected_code_hash = "sha256:newer".to_owned();
    }
    let (seen, _) =
        against_script(2, Start::With(vec![]), vec![stale], |m| kinds(m).len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: code"]);
    let rejected = rejection(&seen);
    assert!(
        rejected.detail.starts_with("root `etl` here is at sha256:")
            && rejected.detail.ends_with(", the task expects sha256:newer"),
        "{}",
        rejected.detail
    );
}

/// Each of the `count` results sent names the code it ran with (one per attempt: a result
/// is sent again until acknowledged).
fn assert_results_name_their_code(seen: &[pb::WorkerMessage], count: usize) {
    let code_hashes: Vec<&str> = seen
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Finished(f) => Some((&f.attempt_id, f.code_hash.as_str())),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    assert_eq!(code_hashes.len(), count);
    assert!(
        code_hashes.iter().all(|h| h.starts_with("sha256:")),
        "{code_hashes:?}"
    );
}

#[tokio::test]
async fn a_dag_file_missing_from_the_expected_code_fails_the_attempt_at_once() {
    // The content hash depends only on the root's files: a copy of the root has the same.
    let copy = TempDir::new().unwrap();
    std::fs::write(copy.path().join("sync.py"), "").unwrap();
    let code = TempDir::new().unwrap();
    let mut watcher = RootWatcher::new(copy.path(), code.path(), Duration::ZERO);
    let hash = loop {
        watcher.poll(Instant::now()).unwrap();
        if let Some(hash) = watcher.hash() {
            break hash.to_owned();
        }
    };
    let python = task_json(&json!({"kind": "python"}));
    let mut gone = assign("a1", &python, "gone.py", false);
    if let Some(master_message::Message::AssignTask(a)) = gone.message.as_mut() {
        a.expected_code_hash = hash;
    }
    let done = |m: &[pb::WorkerMessage]| kinds(m).len() >= 3;
    let (seen, _) = against_script(2, Start::With(vec![]), vec![gone], done).await;
    assert_eq!(kinds(&seen), ["register", "started", "finished: failed"]);
    let finished = seen
        .iter()
        .find_map(|m| match m.message.as_ref()? {
            worker_message::Message::Finished(f) => Some(f),
            _ => None,
        })
        .unwrap();
    assert_eq!(finished.error_kind, DAG_FILE_MISSING);
}

#[tokio::test]
async fn a_shutdown_drains_interrupts_what_still_runs_and_deregisters() {
    let mut waiting = task_json(&json!({"kind": "python"}));
    waiting["id"] = json!("wait");
    let script = vec![assign("a1", &waiting, "sync.py", false)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"leaving");
    let (seen, left) = against_script_with(2, Start::With(vec![]), script, done, |agent| {
        // No grace: the running attempt is interrupted as soon as the drain starts.
        agent.settings.drain.grace = Duration::ZERO;
        let (requests, shutdown) = watch::channel(0);
        agent.shutdown = Some(shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = requests.send(1);
            // Kept open until the test ends.
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
    })
    .await;
    assert_eq!(
        kinds(&seen),
        [
            "register",
            "started",
            "draining",
            "finished: interrupted",
            "leaving"
        ]
    );
    assert_eq!(left, ["a1"], "unacknowledged, it stays in the journal");
}

#[tokio::test]
async fn a_moved_cleanup_given_while_cleaning_its_rest_is_refused() {
    // Codex's review of B169: ignored, the whole cleanup waited on a worker cleaning only the
    // handles the master didn't have, whose result then counted as the whole.
    let python = task_json(&json!({"kind": "python", "cleanup": {"run": "python"}}));
    let master_message::Message::AssignTask(mut whole) =
        assign("a1", &python, "sync.py", false).message.unwrap()
    else {
        unreachable!()
    };
    whole.cleanup_only = Some(pb::CleanupOnly::default());
    let script = vec![to_worker(master_message::Message::AssignTask(whole))];
    let (seen, _) = against_script_with(
        2,
        Start::With(vec![]),
        script,
        |m| kinds(m).len() >= 2,
        |agent| {
            agent.rest_cleaning.insert("a1".to_owned());
        },
    )
    .await;
    assert_eq!(kinds(&seen), ["register", "rejected: no slot"]);
}

#[tokio::test]
async fn a_draining_agent_rejects_assignments_and_says_so_again_after_a_reconnect() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let (seen, _) = against_script_with(
        2,
        Start::With(vec![]),
        script,
        |m| kinds(m).len() >= 3,
        |agent| {
            agent.drain = Some(Drain::new(
                Instant::now(),
                DrainPeriods {
                    grace: Duration::from_secs(600),
                    ..DrainPeriods::default()
                },
            ));
            // Something still runs: the drain isn't over.
            agent.running.insert("other".to_owned(), Stops::new().0);
        },
    )
    .await;
    assert_eq!(kinds(&seen), ["register", "draining", "rejected: draining"]);
    let heartbeat = seen.iter().find_map(|m| match m.message.as_ref()? {
        worker_message::Message::Heartbeat(h) => Some(h.free_slots),
        _ => None,
    });
    assert!(heartbeat.is_none_or(|free| free == 0), "{heartbeat:?}");
}

#[test]
fn a_second_shutdown_request_interrupts_at_once() {
    let settings_dir = TempDir::new().unwrap();
    let settings = AgentSettings {
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 1,
        watcher_slots: 2,
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: settings_dir.path().join("logs"),
            connection_dir: settings_dir.path().join("connections"),
            memory: crate::memory::MemoryWatch::default(),
            timeouts: crate::attempt::WorkerTimeouts::default(),
            fork_server: None,
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: settings_dir.path().join("journal"),
        code_dir: settings_dir.path().join("code"),
        drain: DrainPeriods {
            grace: Duration::from_secs(600),
            ..DrainPeriods::default()
        },
        log_endpoint: String::new(),
        node: NodeIdentity::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    assert!(!agent.on_shutdown(0));
    let (stop, mut stopped) = Stops::new();
    agent.running.insert("a1".to_owned(), stop);
    assert!(agent.on_shutdown(1));
    assert_eq!(agent.drain_step(true), DrainStep::Wait, "600 s of grace");
    assert!(!agent.on_shutdown(2));
    assert_eq!(agent.drain_step(true), DrainStep::Interrupt);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert_eq!(
        runtime.block_on(stopped.next_reason()),
        StopReason::Interrupted
    );
}

#[tokio::test]
async fn a_cancelled_attempt_keeps_its_slot_until_its_process_has_ended() {
    let dir = TempDir::new().unwrap();
    let settings = AgentSettings {
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 1,
        watcher_slots: 2,
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
            connection_dir: dir.path().join("connections"),
            memory: crate::memory::MemoryWatch::default(),
            timeouts: crate::attempt::WorkerTimeouts::default(),
            fork_server: None,
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.path().join("journal"),
        code_dir: dir.path().join("code"),
        drain: DrainPeriods {
            grace: Duration::from_secs(30),
            ..DrainPeriods::default()
        },
        log_endpoint: String::new(),
        node: NodeIdentity::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    let (stop, mut stopped) = Stops::new();
    agent.running.insert("a1".to_owned(), stop);
    let (sender, _outbound) = mpsc::unbounded_channel();
    let to_master = ToMaster::new(sender, SharedOutbox::default());
    let kill = pb::JournalDecision {
        cleanup_from: 0,
        attempt_id: "a1".to_owned(),
        decision: pb::AttemptDecision::Kill.into(),
    };
    agent.decide(&kill, &to_master).await;
    assert_eq!(stopped.next_reason().await, StopReason::Cancelled);
    assert_eq!(agent.running(), ["a1"], "still running until it reports");
    assert_eq!(agent.free_slots(), 0);
}

#[test]
fn with_memory_adds_the_peak_and_says_why_a_task_was_killed() {
    let mut killed = pb::Finished {
        attempt_id: "a1".to_owned(),
        status: pb::AttemptStatus::OomKilled.into(),
        ..pb::Finished::default()
    };
    let seen = crate::memory::MemorySeen {
        peak: 130 << 20,
        over: Some(130 << 20),
    };
    with_memory(&mut killed, seen, Some(100 << 20));
    assert_eq!(killed.peak_memory_bytes, 130 << 20);
    let summary: serde_json::Value = serde_json::from_slice(&killed.error_summary_json).unwrap();
    assert_eq!(summary["exception"], MEMORY_EXCEPTION);
    assert_eq!(
        summary["message"],
        "exceeded memory budget 100 MiB (130 MiB used)"
    );

    let mut fine = pb::Finished {
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    };
    with_memory(
        &mut fine,
        crate::memory::MemorySeen {
            peak: 5 << 20,
            over: None,
        },
        Some(100 << 20),
    );
    assert_eq!(fine.peak_memory_bytes, 5 << 20);
    assert_eq!(fine.error_summary_json, Vec::<u8>::new());
}

#[test]
fn a_missing_proc_is_warned_once_and_other_identity_errors_every_time() {
    let said = AtomicBool::new(false);
    assert_eq!(
        identity_warning(false, &said),
        Some(IdentityWarning::NoProc)
    );
    assert_eq!(
        identity_warning(false, &said),
        None,
        "said once per process"
    );
    assert!(said.load(Ordering::Relaxed));

    // A present `/proc` is some other failure (the process has gone, or the stat line
    // doesn't parse). It warns every time and doesn't consume the once-warning.
    let said = AtomicBool::new(false);
    assert_eq!(
        identity_warning(true, &said),
        Some(IdentityWarning::ThisTask)
    );
    assert_eq!(
        identity_warning(true, &said),
        Some(IdentityWarning::ThisTask)
    );
    assert!(!said.load(Ordering::Relaxed));
    assert_eq!(
        identity_warning(false, &said),
        Some(IdentityWarning::NoProc)
    );
    assert_eq!(
        NO_PROC_WARNING,
        "no /proc: orphaned tasks aren't stopped after a crash"
    );
}

#[tokio::test]
async fn heartbeats_go_on_while_a_parse_result_is_uploaded() {
    let timing = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![],
    }));
    let master = Scripted {
        script: vec![timing],
        parse_registered: true,
        upload_delay: Duration::from_millis(500),
        ..Scripted::default()
    };
    let during = Arc::clone(&master.during_uploads);
    let uploaded = Arc::clone(&master.during_uploads);
    against_master(
        1,
        Start::With(vec![]),
        master,
        move |_| !uploaded.lock().unwrap().is_empty(),
        |_| {},
    )
    .await;
    // A 500 ms upload at a 20 ms heartbeat: about 25 expected, at least a few under load.
    let counts = during.lock().unwrap().clone();
    assert!(counts[0] >= 5, "heartbeats while uploading: {counts:?}");
}

/// B235: the master gives a worker connecting again the parse jobs it held; one still running
/// answers, rather than a second parse of the same code.
#[tokio::test]
async fn a_parse_job_sent_again_while_it_runs_is_parsed_once() {
    let master = Scripted {
        parse_registered: true,
        parse_twice: true,
        ..Scripted::default()
    };
    let uploads = Arc::clone(&master.uploads);
    let seen = Arc::clone(&master.uploads);
    let since = Arc::new(Mutex::new(None::<Instant>));
    against_master(
        1,
        Start::With(vec![]),
        master,
        // A second parse would upload well within a second of the first.
        move |_| {
            if seen.lock().unwrap().is_empty() {
                return false;
            }
            let mut since = since.lock().unwrap();
            since.get_or_insert_with(Instant::now).elapsed() > Duration::from_secs(1)
        },
        |_| {},
    )
    .await;
    assert_eq!(uploads.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_parsed_file_past_the_masters_limit_is_uploaded_as_its_import_error() {
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 1,
        timing: None,
        decisions: vec![],
    }));
    let master = Scripted {
        script: vec![registered],
        parse_registered: true,
        ..Scripted::default()
    };
    let uploads = Arc::clone(&master.uploads);
    let seen = Arc::clone(&master.uploads);
    against_master(
        1,
        Start::With(vec![]),
        master,
        move |_| !seen.lock().unwrap().is_empty(),
        |_| {},
    )
    .await;
    let uploaded = uploads.lock().unwrap().clone();
    let file = &uploaded[0].files[0];
    assert_eq!(file.path, "sync.py");
    assert_eq!(file.dag_json, Vec::<Vec<u8>>::new());
    let error = file.import_error.as_ref().expect("an import error");
    assert_eq!(error.exception, "ParseResultTooLarge");
    assert!(
        error.message.ends_with(
            "more than the master accepts (1 bytes, [grpc] max_message_size): raise the \
             setting, or split the file"
        ),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn a_result_past_the_masters_limit_is_sent_as_output_too_large() {
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 1,
        timing: None,
        decisions: vec![],
    }));
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![registered, assign("a1", &python, "sync.py", false)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).iter().any(|k| k.starts_with("finished"));
    let (seen, _) = against_script(1, Start::With(vec![]), script, done).await;
    let finished = seen
        .iter()
        .find_map(|m| match &m.message {
            Some(worker_message::Message::Finished(f)) => Some(f),
            _ => None,
        })
        .unwrap();
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(finished.error_kind, "output_too_large");
    assert_eq!(finished.output_json, Vec::<u8>::new());
    let summary = String::from_utf8(finished.error_summary_json.clone()).unwrap();
    assert!(summary.contains("ResultTooLarge"), "{summary}");
    assert!(summary.contains("[grpc] max_message_size"), "{summary}");
}

/// Root `etl` as git-sync lays it out: a symbolic link to the revision `rev-a`.
fn git_sync_root(dir: &Path) -> PathBuf {
    let revision = dir.join("rev-a");
    std::fs::create_dir_all(&revision).unwrap();
    let root = dir.join("etl");
    std::os::unix::fs::symlink(&revision, &root).unwrap();
    root
}

#[tokio::test]
async fn parses_and_tasks_import_the_copy_of_the_code_hashed() {
    // B58 (decision 102): the agent's own copy of the hashed code, not the root's link (nor
    // the checkout behind it, which git-sync may delete or move on from).
    let world = world_in(git_sync_root).await;
    let checkout = world.root.canonicalize().unwrap();
    eventually(&world, "the DAGs", || async {
        dag_ids(&world.db).await == ["sync", "wait"]
    })
    .await;
    // git-sync deletes the checkout the hash is of: the code is in the copy.
    std::fs::remove_dir_all(&checkout).unwrap();
    trigger(&world.db, "sync").await;
    eventually(&world, "the run", || async {
        run_state(&world.db, "sync").await == Some(RunState::Success)
    })
    .await;
    let dirs = world.fake.dirs.lock().unwrap().clone();
    assert!(dirs.len() >= 2, "a parse and a task: {dirs:?}");
    let code = world.root.parent().unwrap().join("code");
    assert!(
        dirs.iter().all(|d| d == &dirs[0] && d.starts_with(&code)),
        "{dirs:?}"
    );
    assert!(dirs[0].join("sync.py").is_file());
    shut_down(world).await;
}

#[tokio::test]
async fn an_old_copy_of_the_code_goes_once_nothing_runs_from_it() {
    let world = world().await;
    eventually(&world, "the DAGs", || async {
        dag_ids(&world.db).await == ["sync", "wait"]
    })
    .await;
    let store = world
        .root
        .parent()
        .unwrap()
        .join("code")
        .join(code_folder("etl"));
    let copies = || {
        let mut names: Vec<String> = std::fs::read_dir(&store)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };
    let first = copies();
    assert_eq!(first.len(), 1, "{first:?}");
    // A task running from the first copy keeps it while the root changes.
    trigger(&world.db, "wait").await;
    eventually(&world, "the task running", || async {
        world.fake.dirs.lock().unwrap().len() >= 2
    })
    .await;
    std::fs::write(world.root.join("new.py"), "").unwrap();
    eventually(&world, "the new code", || async { copies().len() == 2 }).await;
    assert!(copies().contains(&first[0]));
    // Once the task ends, only the new copy is left.
    let run = world
        .db
        .dag_runs(&DagId::new("wait").unwrap(), None, 1)
        .await
        .unwrap();
    let cancel = Command::CancelRun {
        run_id: run[0].id.clone(),
    };
    world
        .db
        .insert_command(&cancel, "anna", jiff::Timestamp::now())
        .await
        .unwrap();
    eventually(&world, "the old copy removed", || async {
        let left = copies();
        left.len() == 1 && left[0] != first[0]
    })
    .await;
    shut_down(world).await;
}

#[test]
fn an_unacknowledged_result_is_sent_again_ever_less_often() {
    let mut resends = Resends::default();
    let (a, b) = ("a".to_owned(), "b".to_owned());
    let mut sent_at = Vec::new();
    for beat in 1..=150_u64 {
        // `b` is acknowledged at the 5th heartbeat.
        let unacked: Vec<&String> = if beat < 5 { vec![&a, &b] } else { vec![&a] };
        let due = resends.due(unacked.into_iter()).unwrap();
        if beat == 4 {
            assert_eq!(due, ["a", "b"]);
        }
        assert!(!due.contains(&b) || beat < 5);
        if due.contains(&a) {
            sent_at.push(beat);
        }
    }
    // A heartbeat after the first that found it, then gaps of 2, 4, ... up to the cap.
    assert_eq!(sent_at[..7], [2, 4, 8, 16, 32, 64, 96]);
    assert!(
        sent_at
            .windows(2)
            .all(|w| w[1] - w[0] <= u64::from(MAX_RESEND_GAP))
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeats_missed_while_the_loop_was_stuck_are_not_caught_up() {
    let mut interval = heartbeat_timer(Duration::from_secs(10));
    interval.tick().await;
    tokio::time::advance(Duration::from_secs(300)).await;
    interval.tick().await;
    // The next is a whole period later, not at once.
    let next = tokio::time::timeout(Duration::from_secs(9), interval.tick()).await;
    assert!(next.is_err(), "missed ticks came in a burst");
}

#[test]
fn a_result_sent_again_too_often_gives_the_session_up() {
    let mut resends = Resends::default();
    let a = "a".to_owned();
    let mut sends = 0;
    let mut beat = 0;
    while let Some(due) = resends.due([&a].into_iter()) {
        beat += 1;
        sends += due.len();
        assert!(beat < 1000, "never given up");
    }
    assert_eq!(sends, usize::try_from(MAX_RESENDS).unwrap());
    // Due a ninth time at the 160th heartbeat (sent at 2, 4, 8, ... 96, 128).
    assert_eq!(beat + 1, 160);
}

#[tokio::test]
async fn an_ended_attempt_is_masked_and_takes_a_failed_reads_kind() {
    use crate::secrets::{AttemptSecrets, MemoryStorage, SecretStores};
    use schedr_model::{SecretDecl, SecretName};
    let password = SecretName {
        storage: None,
        path: "etl/db".into(),
        key: Some("password".into()),
        default: None,
    };
    let stores = Arc::new(SecretStores::for_test(MemoryStorage::new([(
        password.clone(),
        "hunter22".to_owned(),
    )])));
    let declared = vec![SecretDecl::Secret(password.clone())];
    let secrets = AttemptSecrets::fetch(stores, std::slice::from_ref(&password), declared)
        .await
        .unwrap();
    let mut finished = pb::Finished {
        status: pb::AttemptStatus::Success.into(),
        output_json: br#"{"dsn": "u:hunter22@h"}"#.to_vec(),
        ..pb::Finished::default()
    };
    // A read refused, then the task succeeded anyway: no kind (decision 168).
    let undeclared = SecretName {
        key: Some("user".into()),
        ..password.clone()
    };
    assert!(secrets.read(&undeclared).await.is_err());
    super::ended_with_secrets(&mut finished, &secrets, true);
    assert_eq!(finished.output_json, br#"{"dsn":"u:****@h"}"#);
    assert_eq!(finished.error_kind, "");
    finished.status = pb::AttemptStatus::Failed.into();
    super::ended_with_secrets(&mut finished, &secrets, false);
    assert_eq!(finished.error_kind, "secret_denied");
    // A kind of its own is kept.
    "timeout".clone_into(&mut finished.error_kind);
    super::ended_with_secrets(&mut finished, &secrets, false);
    assert_eq!(finished.error_kind, "timeout");
    assert!(
        super::masked_lines(&secrets).contains("`secrets['etl/db']['password']` was masked once")
    );
}

#[tokio::test]
async fn the_masking_note_masks_a_value_it_would_repeat() {
    // From Grok's security review of M4.6b: a value that is a word of the note, or its own
    // path or key, was written in the clear by the note itself.
    use crate::secrets::{AttemptSecrets, MemoryStorage, SecretStores};
    use schedr_model::{SecretDecl, SecretName};
    let word = SecretName {
        storage: None,
        path: "etl/db".into(),
        key: Some("k".into()),
        default: None,
    };
    let stores = Arc::new(SecretStores::for_test(MemoryStorage::new([(
        word.clone(),
        "masked".to_owned(),
    )])));
    let declared = vec![SecretDecl::Secret(word.clone())];
    let secrets = AttemptSecrets::fetch(stores, std::slice::from_ref(&word), declared)
        .await
        .unwrap();
    assert_eq!(secrets.mask.mask_text("x masked"), "x ****");
    let note = super::masked_lines(&secrets);
    assert!(
        note.contains("`secrets['etl/db']['k']` was **** once"),
        "{note}"
    );
    assert!(!note.contains("masked"), "{note}");
}

/// Never answers in time: a storage that is down without saying so.
#[derive(Debug)]
struct Hanging;

impl crate::secrets::SecretStorage for Hanging {
    fn read<'a>(&'a self, _path: &'a str) -> crate::secrets::ReadFuture<'a> {
        Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Err(crate::secrets::SecretError::Unavailable("too late".into()))
        })
    }
}

/// Answers slowly, but successfully: its read must count against the process's time.
#[derive(Debug)]
struct Delayed;

impl crate::secrets::SecretStorage for Delayed {
    fn read<'a>(&'a self, _path: &'a str) -> crate::secrets::ReadFuture<'a> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(BTreeMap::from([(
                "password".to_owned(),
                "hunter22".to_owned(),
            )]))
        })
    }
}

fn command_needing_secret(
    storage: Arc<dyn crate::secrets::SecretStorage>,
    timeout_ms: &serde_json::Value,
    command: &[&str],
) -> (ProcessExecutor, AttemptSpec, Plan, TempDir) {
    use crate::secrets::{SecretStores, Storage};
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("etl")).unwrap();
    std::fs::write(dir.path().join("etl/sync.py"), "").unwrap();
    let setup = WorkerSetup {
        roots: std::collections::BTreeMap::from([("etl".to_owned(), dir.path().join("etl"))]),
        parse_only: std::collections::BTreeMap::new(),
        script_roots: std::collections::BTreeMap::new(),
        interpreters: BTreeMap::default(),
        python: std::path::PathBuf::from("/usr/bin/python3"),
        agent_env: std::collections::BTreeMap::from([("PATH".to_owned(), "/usr/bin".to_owned())]),
        pass_env: vec![],
        log_dir: dir.path().join("logs"),
        connection_dir: dir.path().join("connections"),
        memory: crate::memory::MemoryWatch::default(),
        timeouts: crate::attempt::WorkerTimeouts::default(),
        fork_server: None,
    };
    let stores = SecretStores::new(
        std::collections::BTreeMap::from([("main".to_owned(), Storage::new(storage))]),
        Some("main".to_owned()),
        5,
    )
    .unwrap();
    let executor = ProcessExecutor {
        setup: setup.clone(),
        parse: crate::parse::ParseLimits::one_at_a_time(std::time::Duration::from_secs(5)),
        metrics: AgentMetrics::default(),
        drain_terminate_period: std::time::Duration::from_secs(1),
        forks: None,
        watchers: crate::watchers::WatcherHosts::default(),
        secrets: Arc::new(stores),
    };
    let task = json!({
        "id": "load",
        "kind": "command",
        "command": {"argv": command.iter().map(|arg| json!([{"text": arg}])).collect::<Vec<_>>()},
        "connections": ["db"],
        "connection_env": {"DB": "db"},
        "env": {"PW": [{"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}]},
        "settings": {"retries": 0, "retry_delay_ms": 0, "timeout_ms": timeout_ms,
                     "queue": "default", "max_output_size": 1024, "preload": true,
                     "fresh_process": false},
        "output_type": {"type": "process_result"},
    });
    let connections = json!({"connections": {"db": {"name": "db", "type": "sql", "params": {
        "url": {"literal": "postgresql://etl@db.example.com/etl"},
        "password": {"template": [{"ref": {"kind": "secret", "path": "etl/db", "key": "password"}}]},
    }}}});
    let assign = pb::AssignTask {
        attempt_id: "a1".to_owned(),
        task_id: "load".to_owned(),
        map_index: -1,
        try_number: 1,
        run: Some(pb::RunInfo {
            event_json: Vec::new(),
            params_json: Vec::new(),
            run_id: "r1".to_owned(),
            kind: "manual".to_owned(),
            planned_for: Some(prost_types::Timestamp {
                seconds: 1_792_036_800,
                nanos: 0,
            }),
            since: Some(prost_types::Timestamp {
                seconds: 1_791_950_400,
                nanos: 0,
            }),
            dag_id: "sync".to_owned(),
        }),
        task_json: serde_json::to_vec(&task).unwrap(),
        dag_file: Some(pb::DagFileRef {
            root: "etl".to_owned(),
            path: "sync.py".to_owned(),
        }),
        expected_code_hash: String::new(),
        inputs_json: b"{}".to_vec(),
        inputs_external: false,
        cancel_grace_ms: 1_000,
        memory_budget_bytes: 0,
        setrlimit: false,
        upstream_outcomes: std::collections::HashMap::new(),
        connections_json: serde_json::to_vec(&connections).unwrap(),
        cleanup_only: None,
        resource_cleanup: false,
        first_started: None,
    };
    let mut spec = AttemptSpec::from_assign(&assign, None).unwrap();
    spec.stand_in_secrets();
    let plan = crate::attempt::plan(&spec, &setup).unwrap();
    (executor, spec, plan, dir)
}

#[tokio::test]
async fn a_cancel_or_the_timeout_ends_an_attempt_waiting_for_its_secrets() {
    for (timeout_ms, cancel, status) in [
        (json!(null), true, pb::AttemptStatus::Cancelled),
        (json!(200), false, pb::AttemptStatus::TimedOut),
    ] {
        let (executor, spec, plan, _dir) =
            command_needing_secret(Arc::new(Hanging), &timeout_ms, &["true"]);
        let (sender, stops) = Stops::new();
        if cancel {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let _ = sender.send(StopReason::Cancelled);
            });
        }
        let started = std::time::Instant::now();
        let finished = executor
            .run(spec, plan, stops, Arc::new(|_, _| {}), Box::new(|_| {}))
            .await;
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(finished.status, i32::from(status), "{finished:?}");
    }
}

#[tokio::test]
async fn the_timeout_covers_the_secret_read_and_the_command_together() {
    for (timeout_ms, status) in [
        (10_000, pb::AttemptStatus::Success),
        (1_000, pb::AttemptStatus::TimedOut),
    ] {
        let (executor, spec, plan, dir) =
            command_needing_secret(Arc::new(Delayed), &json!(timeout_ms), &["sleep", "0.8"]);
        let connection_dir = dir.path().join("connections/a1");
        let files = connection_dir.clone();
        let (_sender, stops) = Stops::new();
        let finished = executor
            .run(
                spec,
                plan,
                stops,
                Arc::new(move |_, _| assert!(files.join("db/DB/pgpass").is_file())),
                Box::new(|_| {}),
            )
            .await;
        assert_eq!(finished.status, i32::from(status), "{finished:?}");
        assert!(!connection_dir.exists());
    }
}

/// Returns without yielding after the deadline, so fetching wins the select even though
/// its budget is spent. The executor must check the time again before starting a process.
#[derive(Debug)]
struct CompletesLate;

impl crate::secrets::SecretStorage for CompletesLate {
    fn read<'a>(&'a self, _path: &'a str) -> crate::secrets::ReadFuture<'a> {
        Box::pin(async {
            std::thread::sleep(Duration::from_millis(20));
            Ok(BTreeMap::from([(
                "password".to_owned(),
                "hunter22".to_owned(),
            )]))
        })
    }
}

#[tokio::test]
async fn a_fetch_finishing_with_no_time_left_does_not_start_and_removes_connection_files() {
    let (executor, spec, plan, dir) =
        command_needing_secret(Arc::new(CompletesLate), &json!(1), &["true"]);
    let spawned = Arc::new(AtomicBool::new(false));
    let saw_spawn = Arc::clone(&spawned);
    let (_sender, stops) = Stops::new();
    let finished = executor
        .run(
            spec,
            plan,
            stops,
            Arc::new(move |_, _| saw_spawn.store(true, Ordering::SeqCst)),
            Box::new(|_| {}),
        )
        .await;
    assert_eq!(
        finished.status,
        i32::from(pb::AttemptStatus::TimedOut),
        "{finished:?}"
    );
    assert!(!spawned.load(Ordering::SeqCst));
    // The fetch created connection files in its final poll, and run cleaned them up.
    assert!(dir.path().join("connections").is_dir());
    assert!(!dir.path().join("connections/a1").exists());
}

/// Answers at once, leaving the runtime's only blocking thread busy: starting the process,
/// which opens its log there, then waits past the attempt's timeout.
#[derive(Debug)]
struct BusiesBlockingThread;

impl crate::secrets::SecretStorage for BusiesBlockingThread {
    fn read<'a>(&'a self, _path: &'a str) -> crate::secrets::ReadFuture<'a> {
        Box::pin(async {
            drop(tokio::task::spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(1200));
            }));
            Ok(BTreeMap::from([(
                "password".to_owned(),
                "hunter22".to_owned(),
            )]))
        })
    }
}

#[test]
fn the_timeout_covers_a_commands_slow_start() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (executor, spec, plan, _dir) = command_needing_secret(
            Arc::new(BusiesBlockingThread),
            &json!(500),
            &["sleep", "0.2"],
        );
        let spawned = Arc::new(AtomicBool::new(false));
        let saw_spawn = Arc::clone(&spawned);
        let (_sender, stops) = Stops::new();
        let finished = executor
            .run(
                spec,
                plan,
                stops,
                Arc::new(move |_, _| saw_spawn.store(true, Ordering::SeqCst)),
                Box::new(|_| {}),
            )
            .await;
        assert!(spawned.load(Ordering::SeqCst), "{finished:?}");
        assert_eq!(
            finished.status,
            i32::from(pb::AttemptStatus::TimedOut),
            "{finished:?}"
        );
    });
}

/// A command given the connection `db` (`url`, `password`) as `DB`, with `env`, run by a
/// real executor: what it finished with, and what it wrote in `out`.
async fn command_with_connection(
    script: &str,
    env: serde_json::Value,
) -> (pb::Finished, String, TempDir) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("etl")).unwrap();
    std::fs::write(dir.path().join("etl/sync.py"), "").unwrap();
    let setup = WorkerSetup {
        roots: BTreeMap::from([("etl".to_owned(), dir.path().join("etl"))]),
        parse_only: BTreeMap::new(),
        script_roots: BTreeMap::new(),
        interpreters: BTreeMap::default(),
        python: PathBuf::from("/usr/bin/python3"),
        agent_env: BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
        pass_env: vec![],
        log_dir: dir.path().join("logs"),
        connection_dir: dir.path().join("connections"),
        memory: crate::memory::MemoryWatch::default(),
        timeouts: crate::attempt::WorkerTimeouts::default(),
        fork_server: None,
    };
    let executor = ProcessExecutor {
        setup: setup.clone(),
        parse: crate::parse::ParseLimits::one_at_a_time(Duration::from_secs(5)),
        metrics: AgentMetrics::default(),
        drain_terminate_period: Duration::from_secs(1),
        forks: None,
        watchers: crate::watchers::WatcherHosts::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let task = json!({
        "id": "load",
        "kind": "command",
        "command": {"argv": [[{"text": "sh"}], [{"text": "-c"}], [{"text": script}]]},
        "env": env,
        "connection_env": {"DB": "db"},
        "connections": ["db"],
        "settings": {"retries": 0, "retry_delay_ms": 0, "timeout_ms": 20_000,
                     "queue": "default", "max_output_size": 1024, "preload": true,
                     "fresh_process": false},
        "output_type": {"type": "process_result"},
    });
    let connections = json!({"connections": {"db": {"name": "db", "type": "sql", "params": {
        "url": {"literal": "postgresql://etl@db.example.com/etl"},
        "password": {"literal": "p:w"},
    }}}});
    let assign = pb::AssignTask {
        attempt_id: "a1".to_owned(),
        task_id: "load".to_owned(),
        map_index: -1,
        try_number: 1,
        run: Some(pb::RunInfo {
            event_json: Vec::new(),
            params_json: Vec::new(),
            run_id: "r1".to_owned(),
            kind: "manual".to_owned(),
            planned_for: Some(prost_types::Timestamp {
                seconds: 1_792_036_800,
                nanos: 0,
            }),
            since: Some(prost_types::Timestamp {
                seconds: 1_791_950_400,
                nanos: 0,
            }),
            dag_id: "sync".to_owned(),
        }),
        task_json: serde_json::to_vec(&task).unwrap(),
        dag_file: Some(pb::DagFileRef {
            root: "etl".to_owned(),
            path: "sync.py".to_owned(),
        }),
        expected_code_hash: String::new(),
        inputs_json: b"{}".to_vec(),
        inputs_external: false,
        cancel_grace_ms: 1_000,
        memory_budget_bytes: 0,
        setrlimit: false,
        upstream_outcomes: std::collections::HashMap::new(),
        connections_json: serde_json::to_vec(&connections).unwrap(),
        cleanup_only: None,
        resource_cleanup: false,
        first_started: None,
    };
    let mut spec = crate::attempt::AttemptSpec::from_assign(&assign, None).unwrap();
    spec.stand_in_secrets();
    let plan = crate::attempt::plan(&spec, &setup).unwrap();
    let (_sender, stops) = Stops::new();
    let finished = executor
        .run(spec, plan, stops, Arc::new(|_, _| {}), Box::new(|_| {}))
        .await;
    let out = std::fs::read_to_string(dir.path().join("etl/out")).unwrap_or_default();
    (finished, out, dir)
}

#[tokio::test]
async fn a_command_gets_a_connection_as_variables_and_files_removed_after_it() {
    let (finished, out, dir) = command_with_connection(
        r#"{ printf '%s\n' "$DB_URL" "$DB_PASSWORD" "$KEPT"; cat "$DB_PGPASSFILE";
            stat -c %a "$(dirname "$DB_PGPASSFILE")" "$DB_PGPASSFILE"; } > out"#,
        json!({"KEPT": [{"text": "yes"}]}),
    )
    .await;
    assert_eq!(
        finished.status,
        i32::from(pb::AttemptStatus::Success),
        "{finished:?}"
    );
    assert_eq!(
        out,
        format!(
            "postgresql://etl@db.example.com/etl?sslmode=verify-full&sslrootcert=system&passfile=\
         {}\np:w\nyes\n\
         *:*:*:*:p\\:w\n700\n600\n",
            schedr_model::Encoding::Url.apply(
                &dir.path()
                    .join("connections/a1/db/DB/pgpass")
                    .to_string_lossy()
            )
        )
    );
    assert!(
        !dir.path().join("connections/a1").exists(),
        "the attempt's connection files are left"
    );
}

#[tokio::test]
async fn a_connections_variable_also_in_env_fails_the_attempt() {
    let (finished, out, dir) =
        command_with_connection("echo ran > out", json!({"DB_URL": [{"text": "mine"}]})).await;
    assert_eq!(
        finished.status,
        i32::from(pb::AttemptStatus::Failed),
        "{finished:?}"
    );
    assert_eq!(finished.error_kind, crate::attempt::RENDER_FAILED);
    assert!(
        String::from_utf8_lossy(&finished.error_summary_json)
            .contains("`DB_URL` is in the task's `env=`"),
        "{finished:?}"
    );
    assert_eq!(out, "", "the command ran");
    assert!(!dir.path().join("connections/a1").exists());
}

#[tokio::test]
async fn a_start_sweeps_connection_files_but_those_of_attempts_still_running() {
    let dir = TempDir::new().unwrap();
    for attempt in ["gone", "alive"] {
        std::fs::create_dir_all(dir.path().join(attempt).join("DB")).unwrap();
        std::fs::write(dir.path().join(attempt).join("DB/pgpass"), "pw").unwrap();
    }
    super::sweep_connection_files(dir.path(), &BTreeSet::from(["alive".to_owned()])).await;
    assert!(!dir.path().join("gone").exists());
    assert!(dir.path().join("alive/DB/pgpass").exists());
    // No folder yet: nothing to sweep, nothing failing.
    super::sweep_connection_files(&dir.path().join("none"), &BTreeSet::new()).await;
}

fn offline_agent(dir: &Path) -> Agent<Fake> {
    let settings = AgentSettings {
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 1,
        watcher_slots: 2,
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.join("logs"),
            connection_dir: dir.join("connections"),
            memory: crate::memory::MemoryWatch::default(),
            timeouts: crate::attempt::WorkerTimeouts::default(),
            fork_server: None,
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.join("journal"),
        code_dir: dir.join("code"),
        drain: DrainPeriods::default(),
        log_endpoint: String::new(),
        node: NodeIdentity::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    Agent::new(settings, Fake::default(), "i1".to_owned())
}

/// Waits (the agent handling what ends meanwhile) until `ready` says so.
async fn offline_until(agent: &mut Agent<Fake>, ready: impl Fn(&Agent<Fake>) -> bool) {
    for _ in 0..500 {
        if ready(agent) {
            return;
        }
        agent
            .offline(tokio::time::sleep(Duration::from_millis(10)))
            .await;
    }
    panic!("never ready");
}

#[tokio::test]
async fn a_connection_tests_process_is_journalled_and_its_answer_kept_while_offline() {
    // A worker killed mid-test left its process running past its timeout; an answer made
    // while the master was away was dropped, the job running for good.
    let dir = TempDir::new().unwrap();
    let journal = Journal::new(dir.path().join("journal"));
    let mut agent = offline_agent(dir.path());
    let done = agent.done_tx.clone();
    done.send(Done::Spawned {
        attempt_id: "connection-test-1".to_owned(),
        identity: NO_PROCESS,
        forked: false,
    })
    .unwrap();
    let record = [(
        "connection-test-1".to_owned(),
        JournalRecord::Running(Some(NO_PROCESS)),
    )];
    offline_until(&mut agent, |_| {
        journal.records().unwrap_or_default() == record
    })
    .await;
    done.send(Done::Connections {
        job_id: "c1/etl".to_owned(),
        hash: None,
        attempt_id: Some("connection-test-1".to_owned()),
        reports: vec![crate::connection_jobs::failed("db", "no".to_owned())],
    })
    .unwrap();
    offline_until(&mut agent, |agent| agent.unsent_answers.len() == 1).await;
    assert_eq!(journal.records().unwrap(), []);
    let answer = agent.unsent_answers[0].clone();
    // Sent once registered again.
    let registered = to_worker(master_message::Message::Registered(
        pb::Registered::default(),
    ));
    let (seen, _) = against_script_with(
        1,
        Start::With(vec![]),
        vec![registered],
        |m| kinds(m).contains(&"connections"),
        |agent| agent.unsent_answers.push_back(answer.clone()),
    )
    .await;
    let sent = seen.iter().find_map(|m| match m.message.as_ref()? {
        worker_message::Message::ConnectionJobDone(done) => Some(done.clone()),
        _ => None,
    });
    assert_eq!(sent, Some(answer));
}

#[tokio::test(start_paused = true)]
async fn a_connections_job_waiting_past_its_time_for_its_turn_says_so() {
    let dir = TempDir::new().unwrap();
    let mut agent = offline_agent(dir.path());
    let turn = Arc::clone(&agent.connection_jobs);
    let _other = turn.lock().await;
    let registry = json!({"connections": {"db": {"name": "db", "params": {}}}});
    agent.connection_job(pb::ConnectionJob {
        job_id: "c1/etl".to_owned(),
        root: Some(pb::RootHash {
            kind: pb::RootKind::Dag.into(),
            root: "etl".to_owned(),
            content_hash: "sha256:h".to_owned(),
        }),
        connections_json: serde_json::to_vec(&registry).unwrap(),
        test: None,
    });
    // A check has 30 s, its wait included (the clock paused, so this takes no time).
    for _ in 0..40 {
        if !agent.unsent_answers.is_empty() {
            break;
        }
        agent
            .offline(tokio::time::sleep(Duration::from_secs(1)))
            .await;
    }
    let report: serde_json::Value =
        serde_json::from_slice(&agent.unsent_answers[0].report_json[0]).unwrap();
    assert_eq!(
        report["error"],
        "its time was up during its wait for its turn (one connections job at a time per worker)"
    );
}

/// A real executor and a command attempt with a cleanup; both commands get `OUT`, a file in
/// the returned directory, in their environment.
fn attempt_with_cleanup(
    script: &str,
    cleanup: &str,
    retries: u32,
) -> (ProcessExecutor, AttemptSpec, Plan, TempDir) {
    attempt_with_timed_cleanup(script, cleanup, retries, &serde_json::json!(5_000))
}

/// The same, one try of the cleanup limited to `timeout_ms`.
fn attempt_with_timed_cleanup(
    script: &str,
    cleanup: &str,
    retries: u32,
    timeout_ms: &serde_json::Value,
) -> (ProcessExecutor, AttemptSpec, Plan, TempDir) {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("etl")).unwrap();
    std::fs::write(dir.path().join("etl/sync.py"), "").unwrap();
    let setup = WorkerSetup {
        roots: BTreeMap::from([("etl".to_owned(), dir.path().join("etl"))]),
        parse_only: BTreeMap::new(),
        script_roots: BTreeMap::new(),
        interpreters: BTreeMap::default(),
        python: PathBuf::from("/usr/bin/python3"),
        agent_env: BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
        pass_env: vec![],
        log_dir: dir.path().join("logs"),
        connection_dir: dir.path().join("connections"),
        memory: crate::memory::MemoryWatch::default(),
        timeouts: crate::attempt::WorkerTimeouts::default(),
        fork_server: None,
    };
    let executor = ProcessExecutor {
        setup: setup.clone(),
        parse: crate::parse::ParseLimits::one_at_a_time(Duration::from_secs(5)),
        metrics: AgentMetrics::default(),
        drain_terminate_period: Duration::from_secs(1),
        forks: None,
        watchers: crate::watchers::WatcherHosts::default(),
        secrets: Arc::new(crate::secrets::SecretStores::none()),
    };
    let line = |text: &str| json!({"argv": [[{"text": "sh"}], [{"text": "-c"}], [{"text": text}]]});
    let out = dir.path().join("out").display().to_string();
    let task = json!({
        "id": "load",
        "kind": "command",
        "command": line(script),
        "env": {"OUT": [{"text": out}]},
        "settings": {"retries": 0, "retry_delay_ms": 0, "timeout_ms": 20_000,
                     "queue": "default", "max_output_size": 1024, "preload": true,
                     "fresh_process": false},
        "output_type": {"type": "process_result"},
        "cleanup": {"run": {"command": line(cleanup)}, "timeout_ms": timeout_ms,
                    "retries": retries, "retry_delay_ms": 1},
    });
    let assign = pb::AssignTask {
        attempt_id: "a1".to_owned(),
        task_id: "load".to_owned(),
        map_index: -1,
        try_number: 1,
        run: Some(pb::RunInfo {
            event_json: Vec::new(),
            params_json: Vec::new(),
            run_id: "r1".to_owned(),
            kind: "manual".to_owned(),
            planned_for: Some(prost_types::Timestamp {
                seconds: 1_792_036_800,
                nanos: 0,
            }),
            since: Some(prost_types::Timestamp {
                seconds: 1_791_950_400,
                nanos: 0,
            }),
            dag_id: "sync".to_owned(),
        }),
        task_json: serde_json::to_vec(&task).unwrap(),
        dag_file: Some(pb::DagFileRef {
            root: "etl".to_owned(),
            path: "sync.py".to_owned(),
        }),
        expected_code_hash: String::new(),
        inputs_json: b"{}".to_vec(),
        inputs_external: false,
        cancel_grace_ms: 1_000,
        memory_budget_bytes: 0,
        setrlimit: false,
        upstream_outcomes: std::collections::HashMap::new(),
        connections_json: Vec::new(),
        cleanup_only: None,
        resource_cleanup: false,
        first_started: None,
    };
    let mut spec = AttemptSpec::from_assign(&assign, None).unwrap();
    spec.stand_in_secrets();
    let plan = crate::attempt::plan(&spec, &setup).unwrap();
    (executor, spec, plan, dir)
}

/// Runs an attempt to its end: what it finished with, what `ended` was told, and `OUT`.
async fn run_with_cleanup(
    (executor, spec, plan, dir): (ProcessExecutor, AttemptSpec, Plan, TempDir),
    stops: Stops,
) -> (pb::Finished, Option<pb::Finished>, String) {
    let told = Arc::new(std::sync::Mutex::new(None));
    let telling = Arc::clone(&told);
    let finished = executor
        .run(
            spec,
            plan,
            stops,
            Arc::new(|_, _| {}),
            Box::new(move |ended| *telling.lock().unwrap() = Some(ended)),
        )
        .await;
    let out = std::fs::read_to_string(dir.path().join("out")).unwrap_or_default();
    let ended = told.lock().unwrap().take();
    (finished, ended, out)
}

#[tokio::test]
async fn a_cleanup_runs_after_a_failed_task_and_leaves_its_result() {
    let attempt = attempt_with_cleanup("echo task >> $OUT; exit 3", "echo cleanup >> $OUT", 0);
    let (_sender, stops) = Stops::new();
    let (finished, ended, out) = run_with_cleanup(attempt, stops).await;
    assert_eq!(out, "task\ncleanup\n");
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Failed));
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Ok));
    assert_eq!(cleanup.tries, 1);
    let ended = ended.expect("the task's end is reported before its cleanup");
    assert_eq!(ended.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(ended.cleanup, None);
}

#[tokio::test]
async fn a_cleanup_without_a_timeout_gets_the_workers() {
    let mut attempt = attempt_with_timed_cleanup("exit 0", "sleep 30", 0, &serde_json::Value::Null);
    attempt.0.setup.timeouts.cleanup = std::time::Duration::from_millis(200);
    let (_sender, stops) = Stops::new();
    let started = std::time::Instant::now();
    let (finished, _, _) = run_with_cleanup(attempt, stops).await;
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Failed));
    assert_eq!(cleanup.tries, 1);
}

#[tokio::test]
async fn a_failed_cleanup_is_retried_until_it_succeeds() {
    let attempt = attempt_with_cleanup("true", "echo try >> $OUT; [ $(wc -l < $OUT) -ge 3 ]", 5);
    let (_sender, stops) = Stops::new();
    let (finished, _, out) = run_with_cleanup(attempt, stops).await;
    assert_eq!(out, "try\ntry\ntry\n");
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Success));
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Ok));
    assert_eq!(cleanup.tries, 3);
    assert_eq!(cleanup.error_kind, "");
}

#[tokio::test]
async fn a_cleanup_out_of_retries_fails_but_the_task_keeps_its_success() {
    let attempt = attempt_with_cleanup("true", "echo try >> $OUT; exit 1", 1);
    let (_sender, stops) = Stops::new();
    let (finished, _, out) = run_with_cleanup(attempt, stops).await;
    assert_eq!(out, "try\ntry\n");
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Success));
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Failed));
    assert_eq!(cleanup.tries, 2);
}

#[tokio::test]
async fn a_cancel_stops_the_task_but_not_its_cleanup() {
    let attempt = attempt_with_cleanup("sleep 20", "sleep 0.3; echo cleaned >> $OUT", 0);
    let (sender, stops) = Stops::new();
    let cancelling = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = sender.send(StopReason::Cancelled);
        // Kept open past the cleanup: a stop still in force mustn't reach it.
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(sender);
    });
    let (finished, _, out) = run_with_cleanup(attempt, stops).await;
    cancelling.abort();
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Cancelled));
    assert_eq!(out, "cleaned\n");
    assert_eq!(
        finished.cleanup.unwrap().status,
        i32::from(pb::CleanupStatus::Ok)
    );
}

#[tokio::test]
async fn an_attempt_that_never_started_needs_no_cleanup() {
    let attempt = attempt_with_cleanup("true", "echo cleaned >> $OUT", 0);
    // Its working directory gone: the process can't be spawned.
    std::fs::remove_dir_all(attempt.3.path().join("etl")).unwrap();
    let (_sender, stops) = Stops::new();
    let (finished, ended, out) = run_with_cleanup(attempt, stops).await;
    assert_eq!(out, "");
    assert_eq!(ended.map(|e| e.status), None);
    assert_eq!(
        finished.cleanup.unwrap().status,
        i32::from(pb::CleanupStatus::NotNeeded)
    );
}

#[tokio::test]
async fn a_cleanup_of_another_kind_never_runs_the_task_again() {
    let (executor, mut spec, plan, dir) = attempt_with_cleanup("echo task >> $OUT", "true", 2);
    spec.task.cleanup.as_mut().unwrap().run = schedr_model::task::CleanupRun::Python;
    let (_sender, stops) = Stops::new();
    let (finished, _, out) = run_with_cleanup((executor, spec, plan, dir), stops).await;
    assert_eq!(out, "task\n");
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Failed));
    assert_eq!(cleanup.error_kind, "invalid_cleanup");
}

fn finished_with(output: usize, cleanup_error: usize) -> pb::Finished {
    pb::Finished {
        status: pb::AttemptStatus::Success.into(),
        output_json: vec![b'x'; output],
        cleanup: Some(pb::CleanupResult {
            status: pb::CleanupStatus::Failed.into(),
            error_summary_json: vec![b'y'; cleanup_error],
            ..pb::CleanupResult::default()
        }),
        ..pb::Finished::default()
    }
}

#[test]
fn a_large_cleanup_error_gives_way_to_the_tasks_result() {
    let limit = 4 << 20;
    let mut finished = finished_with(2 << 20, 3 << 20);
    fit_result(&mut finished, limit);
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Success));
    assert_eq!(finished.output_json.len(), 2 << 20);
    let cleanup = finished.cleanup.as_ref().unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Failed));
    assert!(prost::Message::encoded_len(&finished) <= limit);
}

#[test]
fn a_result_too_large_by_itself_fails_and_fits_with_its_cleanup_error() {
    let limit = 4 << 20;
    let mut finished = finished_with(5 << 20, 5 << 20);
    fit_result(&mut finished, limit);
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Failed));
    assert_eq!(finished.error_kind, "output_too_large");
    assert!(prost::Message::encoded_len(&finished) <= limit);
}

#[tokio::test]
async fn a_cleanup_over_the_memory_budget_is_killed_and_fails_alone() {
    let cleanup = r#"python3 -c "
import time
held = b'x' * (400 << 20)
time.sleep(30)
"; echo survived >> $OUT"#;
    let (mut executor, spec, plan, dir) = attempt_with_cleanup("true", cleanup, 0);
    executor.setup.memory = crate::memory::MemoryWatch {
        budget: Some(100 << 20),
        ceiling: None,
        task_exec: None,
        interval: Duration::from_millis(50),
        proc_root: PathBuf::from("/proc"),
    };
    let (_sender, stops) = Stops::new();
    let started = std::time::Instant::now();
    let (finished, _, _) = run_with_cleanup((executor, spec, plan, dir), stops).await;
    assert!(started.elapsed() < Duration::from_secs(4), "not killed");
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Success));
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Failed));
    let summary = String::from_utf8(cleanup.error_summary_json).unwrap();
    assert!(summary.contains(MEMORY_EXCEPTION), "{summary}");
}

/// The answer to `Register`: `decision` for `attempt_id`.
fn decided(attempt_id: &str, decision: pb::AttemptDecision) -> pb::MasterMessage {
    to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![pb::JournalDecision {
            cleanup_from: 0,
            attempt_id: attempt_id.to_owned(),
            decision: decision.into(),
        }],
    }))
}

/// The `Finished` among `seen`.
fn result_in(seen: &[pb::WorkerMessage]) -> &pb::Finished {
    seen.iter()
        .find_map(|m| match m.message.as_ref()? {
            worker_message::Message::Finished(finished) => Some(finished),
            _ => None,
        })
        .expect("a result")
}

#[tokio::test]
async fn a_task_cut_off_by_a_restart_is_lost_and_its_cleanup_runs_on_continue() {
    let start = Start::Kept(vec![("old-1", JournalRecord::Running(None))]);
    let script = vec![decided("old-1", pb::AttemptDecision::Continue)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: failed");
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let seen_cleanups = Arc::clone(&cleanups);
    let (seen, left) = against_script_with(2, start, script, done, |agent| {
        agent.executor.cleanups = seen_cleanups;
    })
    .await;
    assert_eq!(
        kinds(&seen),
        ["register", "handles", "task ended", "finished: failed"]
    );
    let finished = result_in(&seen);
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Lost));
    let cleanup = finished.cleanup.as_ref().unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Ok));
    assert_eq!(*cleanups.lock().unwrap(), [vec![one_handle()]]);
    assert_eq!(left, ["old-1"], "unacknowledged, it stays in the journal");
}

#[tokio::test]
async fn a_cleanup_cut_off_by_a_restart_runs_again_and_keeps_the_tasks_result() {
    let ended = pb::Finished {
        attempt_id: "old-1".to_owned(),
        status: pb::AttemptStatus::Success.into(),
        output_json: b"7".to_vec(),
        ..pb::Finished::default()
    };
    let start = Start::Kept(vec![(
        "old-1",
        JournalRecord::Cleaning(ended, Some(NO_PROCESS)),
    )]);
    let script = vec![decided("old-1", pb::AttemptDecision::Continue)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: success");
    let (seen, _) = against_script_with(2, start, script, done, |_| {}).await;
    let finished = result_in(&seen);
    assert_eq!(finished.output_json, b"7");
    assert!(finished.cleanup.is_some());
}

#[tokio::test]
async fn a_cleanup_the_master_ended_isnt_run_after_a_restart() {
    // Decision 248: it may have been moved, or its attempt ended: its slot goes too.
    let start = Start::Kept(vec![("old-1", JournalRecord::Running(None))]);
    let script = vec![decided("old-1", pb::AttemptDecision::Kill)];
    let free = |m: &pb::WorkerMessage| {
        matches!(
            m.message.as_ref(),
            Some(worker_message::Message::Heartbeat(pb::Heartbeat {
                free_slots: 2,
                ..
            }))
        )
    };
    let done = |m: &[pb::WorkerMessage]| m.iter().any(free);
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let seen_cleanups = Arc::clone(&cleanups);
    let (seen, left) = against_script_with(2, start, script, done, |agent| {
        agent.executor.cleanups = seen_cleanups;
    })
    .await;
    assert_eq!(kinds(&seen), ["register"]);
    assert_eq!(cleanups.lock().unwrap().len(), 0);
    assert_eq!(left, Vec::<String>::new());
}

#[tokio::test]
async fn a_cleanup_left_by_an_agent_waits_for_the_masters_word_in_a_slot() {
    let start = Start::Kept(vec![("old-1", JournalRecord::Running(None))]);
    let done = |m: &[pb::WorkerMessage]| heartbeats(m) >= 2;
    let registered = decided("other", pb::AttemptDecision::Ack);
    let (seen, left) = against_script(2, start, vec![registered], done).await;
    assert_eq!(kinds(&seen), ["register"]);
    let beat = seen
        .iter()
        .find_map(|m| match m.message.as_ref()? {
            worker_message::Message::Heartbeat(beat) => Some(beat),
            _ => None,
        })
        .unwrap();
    assert_eq!(beat.free_slots, 1);
    assert_eq!(beat.running_attempt_ids, ["old-1"]);
    assert_eq!(left, ["old-1"]);
}

#[tokio::test]
async fn a_drain_stops_a_task_early_enough_for_one_try_of_its_cleanup() {
    // A grace of 30 s and a cleanup of 60 s: stopped at once. The other task has until 25 s.
    let mut with_cleanup = task_with_cleanup(60_000);
    with_cleanup["id"] = json!("wait");
    let mut plain = task_json(&json!({"kind": "python"}));
    plain["id"] = json!("wait");
    let script = vec![
        assign("a1", &with_cleanup, "sync.py", false),
        assign("a2", &plain, "sync.py", false),
    ];
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: interrupted");
    let (seen, _) = against_script_with(2, Start::With(vec![]), script, done, |agent| {
        let (requests, shutdown) = watch::channel(0);
        agent.shutdown = Some(shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = requests.send(1);
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
    })
    .await;
    assert_eq!(result_in(&seen).attempt_id, "a1");
}

#[tokio::test]
async fn a_task_in_its_cleanup_is_journaled_with_its_result_and_the_cleanups_process() {
    let mut task = task_with_cleanup(1000);
    task["id"] = json!("cleaning");
    let script = vec![assign("a1", &task, "sync.py", false)];
    let journal = Arc::new(Mutex::new(None));
    let seen_journal = Arc::clone(&journal);
    let done = |m: &[pb::WorkerMessage]| {
        let journal = journal.lock().unwrap();
        let journal: &Journal = journal.as_ref().unwrap();
        kinds(m).contains(&"task ended")
            && journal.kept("a1").unwrap().is_some()
            && matches!(
                journal.record("a1").unwrap(),
                Some(JournalRecord::Cleaning(ended, Some(_))) if ended.output_json == b"7"
            )
    };
    let (seen, _) = against_script_with(2, Start::With(vec![]), script, done, |agent| {
        *seen_journal.lock().unwrap() = Some(agent.journal.clone());
    })
    .await;
    assert_eq!(kinds(&seen), ["register", "started", "task ended"]);
}

#[tokio::test]
async fn a_cleanup_after_a_restart_runs_with_its_handles_and_fetches_its_secrets_again() {
    let attempt = attempt_with_cleanup("exit 0", "echo cleanup >> $OUT", 0);
    let (executor, spec, _plan, dir) = attempt;
    let ended = pb::Finished {
        attempt_id: spec.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    };
    let finished = executor
        .cleanup(spec, ended, vec![one_handle()], Arc::new(|_, _| {}))
        .await;
    assert_eq!(finished.status, i32::from(pb::AttemptStatus::Success));
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(
        cleanup.status,
        i32::from(pb::CleanupStatus::Ok),
        "{cleanup:?}"
    );
    let out = std::fs::read_to_string(dir.path().join("out")).unwrap();
    assert_eq!(out, "cleanup\n");
}

#[tokio::test]
async fn a_cleanup_whose_code_copy_is_gone_fails() {
    let attempt = attempt_with_cleanup("exit 0", "echo cleanup >> $OUT", 0);
    let (executor, mut spec, _plan, dir) = attempt;
    spec.code_dir = Some(dir.path().join("gone"));
    let ended = pb::Finished {
        attempt_id: spec.attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        ..pb::Finished::default()
    };
    let finished = executor
        .cleanup(spec, ended, Vec::new(), Arc::new(|_, _| {}))
        .await;
    let cleanup = finished.cleanup.unwrap();
    assert_eq!(cleanup.status, i32::from(pb::CleanupStatus::Failed));
    assert_eq!(cleanup.tries, 0);
    assert!(!dir.path().join("out").exists());
}

#[tokio::test]
async fn a_task_end_sent_again_fits_the_limit_the_master_has_now() {
    // As B47: a result kept under a larger limit, to a master accepting less, would end the
    // stream on every reconnect.
    let big = pb::Finished {
        attempt_id: "a1".to_owned(),
        status: pb::AttemptStatus::Success.into(),
        output_json: vec![b'7'; 1 << 20],
        ..pb::Finished::default()
    };
    let limit = 64 << 10;
    let mut registered = decided("a1", pb::AttemptDecision::Continue);
    if let Some(master_message::Message::Registered(answer)) = &mut registered.message {
        answer.max_message_size = limit;
    }
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"task ended");
    let (seen, _) = against_script_with(2, Start::With(vec![]), vec![registered], done, |agent| {
        agent.cleaning.insert("a1".to_owned(), big);
    })
    .await;
    let sent = seen
        .iter()
        .find_map(|m| match m.message.as_ref()? {
            worker_message::Message::TaskEnded(ended) => ended.result.as_ref(),
            _ => None,
        })
        .unwrap();
    assert!(prost::Message::encoded_len(sent) <= usize::try_from(limit).unwrap());
}

#[tokio::test]
async fn a_moved_cleanup_runs_alone_with_the_handles_it_came_with() {
    // Decision 243: the task isn't run; the master keeps its result.
    let mut message = assign("a1", &task_with_cleanup(1000), "sync.py", false);
    let Some(master_message::Message::AssignTask(assigned)) = &mut message.message else {
        panic!("expected an assignment")
    };
    assigned.cleanup_only = Some(pb::CleanupOnly {
        handle_json: vec![serde_json::to_vec(&one_handle()).unwrap()],
    });
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: failed");
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let seen_cleanups = Arc::clone(&cleanups);
    let (seen, _) = against_script_with(2, Start::With(vec![]), vec![message], done, |agent| {
        agent.executor.cleanups = seen_cleanups;
    })
    .await;
    assert_eq!(
        kinds(&seen),
        ["register", "started", "task ended", "finished: failed"]
    );
    assert_eq!(*cleanups.lock().unwrap(), [vec![one_handle()]]);
    assert!(result_in(&seen).cleanup.is_some());
}

#[tokio::test]
async fn a_moved_cleanup_counts_as_a_start_in_the_assignment_timer() {
    // Grok's review of M5.1a: a moved cleanup sends `Started` by its own path, which recorded
    // no `assign_to_started`.
    let mut message = assign("a1", &task_with_cleanup(1000), "sync.py", false);
    let Some(master_message::Message::AssignTask(assigned)) = &mut message.message else {
        panic!("expected an assignment")
    };
    assigned.cleanup_only = Some(pb::CleanupOnly {
        handle_json: vec![serde_json::to_vec(&one_handle()).unwrap()],
    });
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: failed");
    let metrics = AgentMetrics::default();
    let given = metrics.clone();
    let (seen, _) = against_script_with(2, Start::With(vec![]), vec![message], done, |agent| {
        agent.metrics = given;
    })
    .await;
    assert!(kinds(&seen).contains(&"started"));
    let mut registry = prometheus_client::registry::Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    let mut out = String::new();
    prometheus_client::encoding::text::encode(&mut out, &registry).unwrap();
    assert!(
        out.contains("schedr_worker_assign_to_started_seconds_count 1"),
        "{out}"
    );
}

#[tokio::test]
async fn the_journal_gauge_follows_finishes_and_acknowledgements() {
    // Grok's review of M5.1a: a finish set the gauge before its result was kept (one short),
    // and an acknowledgement never set it again.
    let dir = TempDir::new().unwrap();
    let mut agent = offline_agent(dir.path());
    for id in ["a1", "a2"] {
        let (stop, _) = Stops::new();
        agent.running.insert(id.to_owned(), stop);
    }
    agent.set_slot_gauges();
    assert_eq!(agent.metrics.journal_records.get(), 2);
    for id in ["a1", "a2"] {
        let finished = pb::Finished {
            attempt_id: id.to_owned(),
            ..pb::Finished::default()
        };
        agent.result_recorded(finished, None);
    }
    assert_eq!(
        agent.metrics.journal_records.get(),
        2,
        "kept until acknowledged"
    );
    assert_eq!(agent.metrics.slots_used.get(), 0);
    agent.acknowledged("a1").await;
    assert_eq!(agent.metrics.journal_records.get(), 1);
    agent.acknowledged("a2").await;
    assert_eq!(agent.metrics.journal_records.get(), 0);
}

/// A `Registered` with a lease of 50 ms and `wait`, with a cleanup, `anywhere` as said.
fn silent_master(anywhere: bool) -> Vec<pb::MasterMessage> {
    silent_master_leased(anywhere, 50)
}

/// As [`silent_master`], with a lease of `lease_ms`: one long enough for a loaded machine to
/// handle the `Registered` before the assignment behind it comes past the lease (decision
/// 281: then never started).
fn silent_master_leased(anywhere: bool, lease_ms: u64) -> Vec<pb::MasterMessage> {
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: lease_ms,
            cancel_grace_ms: 1000,
        }),
        ..pb::Registered::default()
    }));
    let mut task = task_with_cleanup(1000);
    task["id"] = json!("wait");
    task["cleanup"]["anywhere"] = json!(anywhere);
    vec![registered, assign("a1", &task, "sync.py", false)]
}

#[tokio::test]
async fn a_master_gone_silent_stops_a_task_whose_cleanup_may_move() {
    let stops = Arc::new(Mutex::new(Vec::new()));
    let (seen_stops, stopped) = (Arc::clone(&stops), Arc::clone(&stops));
    let done = move |_: &[pb::WorkerMessage]| !stopped.lock().unwrap().is_empty();
    let script = silent_master_leased(true, 300);
    against_script_with(2, Start::With(vec![]), script, done, |agent| {
        agent.executor.stops = seen_stops;
    })
    .await;
    assert_eq!(*stops.lock().unwrap(), [StopReason::CutOff]);
}

#[tokio::test]
async fn a_stuck_fetch_of_inputs_doesnt_hold_up_the_stop_or_the_heartbeats() {
    let stops = Arc::new(Mutex::new(Vec::new()));
    let (seen_stops, stopped) = (Arc::clone(&stops), Arc::clone(&stops));
    // A lease of several heartbeats: since decision 281 the stream ends at the lease, and the
    // last heartbeats before it may go with it.
    let mut script = silent_master_leased(true, 300);
    let mut other = assign(
        "a2",
        &task_json(&json!({"kind": "python"})),
        "sync.py",
        false,
    );
    let Some(master_message::Message::AssignTask(assigned)) = &mut other.message else {
        panic!("expected an assignment")
    };
    assigned.inputs_external = true;
    script.push(other);
    let master = Scripted {
        script,
        inputs_stuck: true,
        ..Scripted::default()
    };
    // Both, in whatever order a loaded machine gives them: a heartbeat may come after the stop.
    let done = move |seen: &[pb::WorkerMessage]| {
        !stopped.lock().unwrap().is_empty()
            && seen
                .iter()
                .any(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_))))
    };
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.stops = seen_stops;
    })
    .await;
    assert_eq!(*stops.lock().unwrap(), [StopReason::CutOff]);
    let beats = seen
        .iter()
        .filter(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_))))
        .count();
    assert!(beats >= 1, "{:?}", kinds(&seen));
}

#[tokio::test]
async fn an_assignment_behind_a_fetch_cut_off_by_the_lease_is_not_started() {
    // Grok's review of M5.1b: a burst went on after its fetch ended the session at the
    // cutoff, journaling and launching the assignment behind it.
    let stops = Arc::new(Mutex::new(Vec::new()));
    let (seen_stops, stopped) = (Arc::clone(&stops), Arc::clone(&stops));
    let mut script = silent_master_leased(true, 300);
    let mut fetching = assign(
        "a2",
        &task_json(&json!({"kind": "python"})),
        "sync.py",
        false,
    );
    let Some(master_message::Message::AssignTask(assigned)) = &mut fetching.message else {
        panic!("expected an assignment")
    };
    assigned.inputs_external = true;
    script.push(fetching);
    script.extend(waits(&["a3"]));
    let master = Scripted {
        script,
        inputs_stuck: true,
        ..Scripted::default()
    };
    let done = move |_: &[pb::WorkerMessage]| !stopped.lock().unwrap().is_empty();
    let (seen, left) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.stops = seen_stops;
    })
    .await;
    assert_eq!(*stops.lock().unwrap(), [StopReason::CutOff]);
    let started: Vec<&str> = seen
        .iter()
        .filter_map(|m| match &m.message {
            Some(worker_message::Message::Started(s)) => Some(s.attempt_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(started, ["a1"], "{:?}", kinds(&seen));
    assert!(!left.iter().any(|id| id == "a3"), "a3 journaled: {left:?}");
}

#[tokio::test]
async fn an_answer_older_than_the_lease_starts_no_cleanup() {
    // The answer takes longer than the lease it states.
    let mut registered = decided("old-1", pb::AttemptDecision::Continue);
    let Some(master_message::Message::Registered(answer)) = &mut registered.message else {
        panic!("expected a registration")
    };
    answer.timing = Some(pb::Timing {
        heartbeat_interval_ms: 20,
        lease_timeout_ms: 20,
        cancel_grace_ms: 1000,
    });
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(200);
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let seen_cleanups = Arc::clone(&cleanups);
    let start = Start::Kept(vec![("old-1", JournalRecord::Running(None))]);
    let master = Scripted {
        script: vec![registered],
        answer_after: Duration::from_millis(50),
        ..Scripted::default()
    };
    against_master(2, start, master, done, |agent| {
        agent.executor.cleanups = seen_cleanups;
    })
    .await;
    assert_eq!(*cleanups.lock().unwrap(), Vec::<Vec<CleanupHandle>>::new());
}

#[tokio::test]
async fn a_drain_doesnt_wait_for_a_held_cleanup_but_does_for_its_task() {
    let done = |_: &[pb::WorkerMessage]| true;
    against_script_with(2, Start::With(vec![]), vec![], done, |agent| {
        let (stop, _) = Stops::new();
        agent.running.insert("a1".to_owned(), stop);
        let (gate, _passes) = watch::channel(Gate::Held);
        agent.gates.insert("a1".to_owned(), gate);
        assert_eq!(agent.drain_running(), 1, "its task is still being stopped");
        let ended = pb::Finished::default();
        agent.cleaning.insert("a1".to_owned(), ended);
        assert_eq!(agent.drain_running(), 0);
    })
    .await;
}

#[tokio::test]
async fn a_master_gone_silent_ends_the_stream_and_leaves_other_tasks_running() {
    let stops = Arc::new(Mutex::new(Vec::new()));
    let seen_stops = Arc::clone(&stops);
    let beats = |m: &[pb::WorkerMessage]| {
        m.iter()
            .filter(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_))))
            .count()
    };
    // Six leases: a stream kept open would have sent fifteen heartbeats (one each 20 ms).
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(300);
    let (seen, _) = against_script_with(
        2,
        Start::With(vec![]),
        silent_master(false),
        done,
        |agent| {
            agent.executor.stops = seen_stops;
        },
    )
    .await;
    assert_eq!(*stops.lock().unwrap(), Vec::<StopReason>::new());
    // Decision 281 (B163): the stream ends at the lease, whatever runs.
    assert!(beats(&seen) < 8, "{} heartbeats", beats(&seen));
}

#[tokio::test]
async fn an_assignment_arriving_after_a_lease_of_silence_isnt_started() {
    // B163: a one-way cut held an assignment while the master took it back at the lease and
    // ran the task elsewhere; delivered at the heal, it mustn't start here.
    let registered = silent_master(false).swap_remove(0);
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("late");
    let master = Scripted {
        script: vec![registered],
        late: Some((
            Duration::from_millis(150),
            vec![assign("a1", &task, "sync.py", false)],
        )),
        ..Scripted::default()
    };
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(400);
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |_| {}).await;
    assert_eq!(kinds(&seen), ["register"]);
}

#[tokio::test]
async fn an_assignment_journaled_past_the_lease_isnt_started() {
    // F14: a journal write stalled past the lease returned, and its assignment started after
    // the master took it back and ran it elsewhere.
    let mut script = silent_master(false);
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("stalled");
    script[1] = assign("a1", &task, "sync.py", false);
    let master = Scripted {
        script,
        ..Scripted::default()
    };
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(600);
    let launched = Arc::new(Mutex::new(Vec::new()));
    let seen_launches = Arc::clone(&launched);
    let stalled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen_stalls = Arc::clone(&stalled);
    let (seen, left) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.dirs = seen_launches;
        agent.journal_stall = (Duration::from_millis(250), seen_stalls);
    })
    .await;
    // The assignment was taken and journaled (its write delayed), then dropped from the
    // journal, never launched (F14's review: the test must see the write happen).
    assert_eq!(stalled.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(left, Vec::<String>::new());
    assert_eq!(launched.lock().unwrap().len(), 0, "{:?}", kinds(&seen));
    assert!(
        kinds(&seen).iter().all(|k| *k == "register"),
        "{:?}",
        kinds(&seen)
    );
}

#[tokio::test]
async fn an_assignment_arriving_late_still_starts() {
    // B240: no start deadline. The master answers the worker's heartbeats (every 20 ms) and
    // sends an assignment 400 ms after the stream opened: it starts, however late, as the
    // master takes it back only once the worker's messages show it never started it.
    let mut script = silent_master_leased(false, 5000);
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("anchored");
    let later = assign("a1", &task, "sync.py", false);
    script.truncate(1);
    let master = Scripted {
        script,
        late: Some((Duration::from_millis(400), vec![later])),
        ack_heartbeats: Some(Duration::ZERO),
        ..Scripted::default()
    };
    let launched = Arc::new(Mutex::new(Vec::new()));
    let seen_launches = Arc::clone(&launched);
    let watched = Arc::clone(&launched);
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| {
        !watched.lock().unwrap().is_empty() || started.elapsed() > Duration::from_secs(3)
    };
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.dirs = seen_launches;
    })
    .await;
    assert!(heartbeats(&seen) > 5, "{:?}", kinds(&seen));
    assert_eq!(launched.lock().unwrap().len(), 1, "{:?}", kinds(&seen));
}

#[tokio::test]
async fn a_deposed_masters_stream_is_ended_and_nothing_of_it_counts() {
    // Decision 444 (B231): a worker that has heard a master leading with lease epoch 5 ends a
    // stream from one leading with 4, a deposed master still answering: its assignment
    // isn't started.
    let mut script = silent_master_leased(false, 5000);
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("deposed");
    script[1] = assign("a1", &task, "sync.py", false);
    let master = Scripted {
        script,
        epoch: 4,
        ..Scripted::default()
    };
    let launched = Arc::new(Mutex::new(Vec::new()));
    let seen_launches = Arc::clone(&launched);
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(600);
    let refused = prometheus_client::metrics::counter::Counter::<u64>::default();
    let counted = refused.clone();
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.dirs = seen_launches;
        agent.leader_epoch = (5, Some(tokio::time::Instant::now()));
        agent.metrics.older_epoch = counted;
    })
    .await;
    assert!(refused.get() >= 1, "counted (decision 448)");
    assert_eq!(launched.lock().unwrap().len(), 0, "{:?}", kinds(&seen));
    // It did connect and register, more than once (Grok's review: silence passed too): each
    // session ended at the deposed master's `Registered`, before a heartbeat.
    let registers = seen
        .iter()
        .filter(|m| matches!(m.message, Some(worker_message::Message::Register(_))))
        .count();
    assert!(registers >= 1, "{:?}", kinds(&seen));
    assert!(
        !seen
            .iter()
            .any(|m| matches!(m.message, Some(worker_message::Message::Heartbeat(_)))),
        "the session ended at its Registered: {:?}",
        kinds(&seen)
    );
}

#[tokio::test]
async fn a_lower_epoch_is_followed_once_the_higher_has_been_silent_for_a_lease() {
    // Decision 444, a restored database: its epoch set back, every worker would refuse every
    // master. A lease (5 s here) after the last word of epoch 5, epoch 4 is followed.
    let mut script = silent_master_leased(false, 5000);
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("restored");
    script[1] = assign("a1", &task, "sync.py", false);
    let master = Scripted {
        script,
        epoch: 4,
        ..Scripted::default()
    };
    let launched = Arc::new(Mutex::new(Vec::new()));
    let seen_launches = Arc::clone(&launched);
    let watched = Arc::clone(&launched);
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| {
        !watched.lock().unwrap().is_empty() || started.elapsed() > Duration::from_secs(3)
    };
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.dirs = seen_launches;
        let long_ago = tokio::time::Instant::now()
            .checked_sub(Duration::from_secs(6))
            .unwrap();
        agent.leader_epoch = (5, Some(long_ago));
        agent.lease = Some(Duration::from_secs(5));
    })
    .await;
    assert_eq!(launched.lock().unwrap().len(), 1, "{:?}", kinds(&seen));
}

#[tokio::test]
async fn an_assignment_whose_inputs_come_after_a_lease_of_silence_isnt_started() {
    // Codex's review of B163: the fetch of inputs ran without the silence limit, and its
    // assignment started after the master took it back.
    let mut script = silent_master(false);
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("fetched");
    let mut external = assign("a1", &task, "sync.py", false);
    let Some(master_message::Message::AssignTask(assigned)) = &mut external.message else {
        panic!("expected an assignment")
    };
    assigned.inputs_external = true;
    script[1] = external;
    let master = Scripted {
        script,
        inputs_after: Duration::from_millis(250),
        ..Scripted::default()
    };
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(600);
    let launched = Arc::new(Mutex::new(Vec::new()));
    let seen_launches = Arc::clone(&launched);
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.dirs = seen_launches;
    })
    .await;
    assert_eq!(launched.lock().unwrap().len(), 0, "{:?}", kinds(&seen));
    assert!(
        kinds(&seen).iter().all(|k| *k == "register"),
        "{:?}",
        kinds(&seen)
    );
}

#[tokio::test]
async fn a_first_registration_older_than_its_lease_isnt_acted_on() {
    // Codex's review of B163: a worker's first session knew no lease yet, took a `Registered`
    // held past it and started the assignment behind it, which the master had taken back.
    let mut task = task_json(&json!({"kind": "python"}));
    task["id"] = json!("cold");
    let mut script = silent_master(false);
    script[1] = assign("a1", &task, "sync.py", false);
    let master = Scripted {
        script,
        answer_after: Duration::from_millis(150),
        ..Scripted::default()
    };
    let started = Instant::now();
    let done = move |_: &[pb::WorkerMessage]| started.elapsed() > Duration::from_millis(500);
    let launched = Arc::new(Mutex::new(Vec::new()));
    let seen_launches = Arc::clone(&launched);
    let (seen, _) = against_master(2, Start::With(vec![]), master, done, |agent| {
        agent.executor.dirs = seen_launches;
    })
    .await;
    assert_eq!(launched.lock().unwrap().len(), 0, "{:?}", kinds(&seen));
    assert!(
        kinds(&seen).iter().all(|k| *k == "register"),
        "{:?}",
        kinds(&seen)
    );
}

#[tokio::test]
async fn a_moved_cleanup_cut_off_by_a_restart_keeps_the_handles_of_its_assignment() {
    let script = vec![decided("old-1", pb::AttemptDecision::Continue)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: failed");
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let seen_cleanups = Arc::clone(&cleanups);
    against_script_with(2, Start::Moved("old-1"), script, done, |agent| {
        agent.executor.cleanups = seen_cleanups;
    })
    .await;
    assert_eq!(*cleanups.lock().unwrap(), [vec![one_handle()]]);
}

#[tokio::test]
async fn a_moved_cleanup_whose_handle_doesnt_read_is_refused_not_run_without_it() {
    let mut message = assign("a1", &task_with_cleanup(1000), "sync.py", false);
    let Some(master_message::Message::AssignTask(assigned)) = &mut message.message else {
        panic!("expected an assignment")
    };
    assigned.cleanup_only = Some(pb::CleanupOnly {
        handle_json: vec![b"not json".to_vec()],
    });
    let done = |m: &[pb::WorkerMessage]| kinds(m).iter().any(|k| k.starts_with("rejected"));
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let seen_cleanups = Arc::clone(&cleanups);
    let (seen, left) = against_script_with(2, Start::With(vec![]), vec![message], done, |agent| {
        agent.executor.cleanups = seen_cleanups;
    })
    .await;
    assert_eq!(kinds(&seen).len(), 2, "{:?}", kinds(&seen));
    assert_eq!(*cleanups.lock().unwrap(), Vec::<Vec<CleanupHandle>>::new());
    assert_eq!(left, Vec::<String>::new());
}

#[tokio::test]
async fn a_moved_cleanup_this_worker_cant_plan_is_refused_to_wait_for_another() {
    let mut message = assign("a1", &task_with_cleanup(1000), "missing.py", false);
    let Some(master_message::Message::AssignTask(assigned)) = &mut message.message else {
        panic!("expected an assignment")
    };
    assigned.cleanup_only = Some(pb::CleanupOnly::default());
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"rejected: missing");
    let (seen, _) = against_script(2, Start::With(vec![]), vec![message], done).await;
    assert_eq!(kinds(&seen), ["register", "rejected: missing"]);
}

#[test]
fn a_folded_hash_marks_its_kind_and_moves_with_what_it_folds_in() {
    let content = "sha256:abc";
    let script = fold_hash("script", content, &[]);
    assert!(
        script.starts_with("sha256:") && script.len() == 7 + 64,
        "{script}"
    );
    assert_ne!(script, content);
    assert_ne!(script, fold_hash("dag", content, &[]));
    let used = |hash: &str| vec![("jobs".to_owned(), hash.to_owned())];
    assert_eq!(
        fold_hash("dag", content, &used("h1")),
        fold_hash("dag", content, &used("h1"))
    );
    assert_ne!(
        fold_hash("dag", content, &used("h1")),
        fold_hash("dag", content, &used("h2"))
    );
}

/// The roots and hashes of the agent's reports, in order: `Register`'s, then each
/// `CodeVersionChanged` (an empty hash: withdrawn).
fn root_reports(messages: &[pb::WorkerMessage]) -> Vec<Vec<(String, String)>> {
    messages
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Register(r) => Some(
                r.roots
                    .iter()
                    .map(|root| (root.root.clone(), root.content_hash.clone()))
                    .collect(),
            ),
            worker_message::Message::CodeVersionChanged(c) => c
                .root
                .as_ref()
                .map(|root| vec![(root.root.clone(), root.content_hash.clone())]),
            _ => None,
        })
        .collect()
}

/// B167 and decisions 301, 303, 304: a script root holding the same files as a DAG root
/// reports another hash; a DAG root using it reports its hash folded in once the master says
/// so, and is withdrawn when it uses a script root this worker lacks.
#[tokio::test]
async fn a_dag_roots_hash_folds_in_the_script_roots_it_uses_and_one_lacking_withdraws_it() {
    let uses = |roots: &[(&str, &[&str])]| -> Vec<pb::RootUses> {
        roots
            .iter()
            .map(|(root, used)| pb::RootUses {
                root: (*root).to_owned(),
                script_roots: used.iter().map(|s| (*s).to_owned()).collect(),
            })
            .collect()
    };
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: uses(&[("etl", &["jobs"])]),
        ..pb::Registered::default()
    }));
    let lacking = to_worker(master_message::Message::ScriptUses(pb::ScriptUses {
        roots: uses(&[("etl", &["jobs", "elsewhere"])]),
    }));
    let scripts = TempDir::new().unwrap();
    let (seen, _) = against_script_with(
        1,
        Start::With(vec![]),
        vec![registered, lacking],
        |m| root_reports(m).len() >= 3,
        |agent| {
            // The same files as the DAG root `etl` (one empty `sync.py`).
            std::fs::write(scripts.path().join("sync.py"), "").unwrap();
            let store = agent.settings.code_dir.join("jobs");
            agent
                .settings
                .setup
                .script_roots
                .insert("jobs".to_owned(), scripts.path().into());
            agent.watchers.insert(
                "jobs".to_owned(),
                RootWatcher::new(scripts.path(), store, Duration::ZERO),
            );
        },
    )
    .await;
    let reports = root_reports(&seen);
    let first: BTreeMap<String, String> = reports[0].iter().cloned().collect();
    let (dag, script) = (&first["etl"], &first["jobs"]);
    assert_ne!(
        dag, script,
        "the same files as a DAG root and as a script root (B167)"
    );
    assert_eq!(*script, fold_hash("script", dag, &[]));
    let folded = fold_hash("dag", dag, &[("jobs".to_owned(), script.clone())]);
    assert_eq!(reports[1], [("etl".to_owned(), folded)]);
    assert_eq!(
        reports[2],
        [("etl".to_owned(), String::new())],
        "withdrawn (303)"
    );
}

#[tokio::test]
async fn a_task_imports_another_dag_roots_library_from_its_copy() {
    let python = task_json(&json!({"kind": "python"}));
    let libs = TempDir::new().unwrap();
    std::fs::write(libs.path().join("helpers.py"), "").unwrap();
    let configured = libs.path().to_string_lossy().into_owned();
    let sys_paths = Arc::new(Mutex::new(Vec::new()));
    let seen_paths = Arc::clone(&sys_paths);
    let (_, _) = against_script_with(
        1,
        Start::With(vec![]),
        vec![assign("a1", &python, "sync.py", false)],
        |m| kinds(m).iter().any(|k| k.starts_with("finished")),
        |agent| {
            agent.executor.sys_paths = seen_paths;
            let store = agent.settings.code_dir.join(code_folder("libs"));
            agent
                .settings
                .setup
                .roots
                .insert("libs".to_owned(), libs.path().into());
            agent.watchers.insert(
                "libs".to_owned(),
                RootWatcher::new(libs.path(), store, Duration::ZERO),
            );
        },
    )
    .await;
    let paths = sys_paths.lock().unwrap().clone();
    let libs_entry = paths[0]
        .iter()
        .find(|p| Path::new(p).join("helpers.py").is_file() && **p != configured);
    assert!(
        libs_entry.is_some_and(|p| p.contains("code")),
        "the copy of `libs`, not its configured path: {paths:?}"
    );
}

#[tokio::test]
async fn a_withdrawn_roots_task_is_rejected_even_if_the_master_sent_it_before_knowing() {
    // 303: the worker lacks script root `elsewhere`, which `etl` uses; an assignment the
    // master sent from the root's earlier report must not run.
    let registered = to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: vec![pb::RootUses {
            root: "etl".to_owned(),
            script_roots: vec!["elsewhere".to_owned()],
        }],
        ..pb::Registered::default()
    }));
    let python = task_json(&json!({"kind": "python"}));
    let mut sent = assign("a1", &python, "sync.py", false);
    if let Some(master_message::Message::AssignTask(a)) = sent.message.as_mut() {
        a.expected_code_hash = "sha256:earlier".to_owned();
    }
    let (seen, _) = against_script(1, Start::With(vec![]), vec![registered, sent], |m| {
        kinds(m)
            .iter()
            .any(|k| k.starts_with("rejected") || k.starts_with("finished"))
    })
    .await;
    let rejected = rejection(&seen);
    assert_eq!(
        rejected.detail,
        "root `etl` is withdrawn here: a script root it uses isn't on this worker"
    );
}

#[tokio::test]
async fn a_recovered_cleanup_imports_the_other_roots_from_the_copies_its_task_used() {
    let start = Start::Kept(vec![("old-1", JournalRecord::Running(None))]);
    let script = vec![decided("old-1", pb::AttemptDecision::Continue)];
    let done = |m: &[pb::WorkerMessage]| kinds(m).contains(&"finished: failed");
    let libs = TempDir::new().unwrap();
    std::fs::write(libs.path().join("helpers.py"), "VERSION = 2\n").unwrap();
    let copies = Arc::new(Mutex::new(Vec::new()));
    let seen_copies = Arc::clone(&copies);
    let old = Arc::new(Mutex::new(PathBuf::new()));
    let old_dir = Arc::clone(&old);
    against_script_with(2, start, script, done, |agent| {
        agent.executor.cleanup_copies = seen_copies;
        // `libs` moved on since the task ran: its copy then is an old one, the task's own.
        let store = agent.settings.code_dir.join(code_folder("libs"));
        let then = store.join("sha256-then");
        std::fs::create_dir_all(&then).unwrap();
        std::fs::write(then.join("helpers.py"), "VERSION = 1\n").unwrap();
        agent
            .settings
            .setup
            .roots
            .insert("libs".to_owned(), libs.path().into());
        agent.watchers.insert(
            "libs".to_owned(),
            RootWatcher::new(libs.path(), store, Duration::ZERO),
        );
        let mut kept = agent.journal.kept("old-1").unwrap().unwrap();
        kept.root_copies = [("libs".to_owned(), then.clone())].into();
        // Its script root's copy comes back too, or its cleanup would run newer code.
        kept.script_dir = Some(then.join("scripts"));
        std::fs::create_dir_all(then.join("scripts")).unwrap();
        agent.journal.keep("old-1", &kept).unwrap();
        *old_dir.lock().unwrap() = then;
    })
    .await;
    let then = old.lock().unwrap().clone();
    assert_eq!(
        *copies.lock().unwrap(),
        [vec![(then.clone(), true), (then.join("scripts"), true)]]
    );
}

#[tokio::test]
async fn a_moved_script_cleanup_is_taken_by_a_worker_lacking_only_the_tasks_interpreter() {
    let scripts = TempDir::new().unwrap();
    std::fs::write(scripts.path().join("undo.sh"), "").unwrap();
    let mut task = task_json(&json!({
        "kind": "script",
        "job": "jobs:schedr.toml#work",
        "script": "work.sh",
        "interpreter": "special",
        "working_dir": "",
    }));
    task["cleanup"] = json!({
        "run": {"script": {"script": "undo.sh", "interpreter": "bash"}},
        "retries": 0,
        "retry_delay_ms": 0,
    });
    let mut message = assign("moved-1", &task, "sync.py", false);
    let Some(master_message::Message::AssignTask(given)) = &mut message.message else {
        panic!("expected an assignment")
    };
    given.cleanup_only = Some(pb::CleanupOnly::default());
    let done = |m: &[pb::WorkerMessage]| {
        kinds(m)
            .iter()
            .any(|kind| kind.starts_with("finished") || kind.starts_with("rejected"))
    };
    let dir = scripts.path().to_path_buf();
    let (seen, _) = against_script_with(2, Start::With(Vec::new()), vec![message], done, |agent| {
        let setup = &mut agent.settings.setup;
        setup.script_roots.insert("jobs".to_owned(), dir);
        setup
            .interpreters
            .insert("bash".to_owned(), PathBuf::from("/bin/bash"));
    })
    .await;
    let kinds = kinds(&seen);
    assert!(kinds.contains(&"started"), "{kinds:?}");
    assert!(
        !kinds.iter().any(|k| k.starts_with("rejected")),
        "{kinds:?}"
    );
}

#[test]
fn the_reconnect_backoff_stays_under_half_the_masters_lease() {
    // B168: past the lease, a restarted master takes a live worker for lost.
    let max = Duration::from_secs(30);
    assert_eq!(reconnect_cap(max, None), max);
    assert_eq!(reconnect_cap(max, Some(Duration::from_secs(60))), max);
    assert_eq!(
        reconnect_cap(max, Some(Duration::from_secs(4))),
        Duration::from_secs(2)
    );
    // Jitter only shortens a wait.
    for seed in [0, u32::MAX / 2, u32::MAX] {
        assert!(jittered(Duration::from_secs(2), seed) <= Duration::from_secs(2));
    }
}

fn assign_trigger(instance: &str, task_id: &str) -> pb::MasterMessage {
    let mut task = task_json(&json!({"kind": "sensor", "mode": "loop", "arguments": {}}));
    task["id"] = json!(task_id);
    to_worker(master_message::Message::AssignTrigger(pb::AssignTrigger {
        instance_id: instance.to_owned(),
        dag_id: "sync".to_owned(),
        task_json: serde_json::to_vec(&task).unwrap(),
        dag_file: Some(pb::DagFileRef {
            root: "etl".to_owned(),
            path: "sync.py".to_owned(),
        }),
        after: Some("b".to_owned()),
        cancel_grace_ms: 1000,
        ..pb::AssignTrigger::default()
    }))
}

fn registered_fast() -> pb::MasterMessage {
    to_worker(master_message::Message::Registered(pb::Registered {
        script_uses: Vec::new(),
        max_message_size: 0,
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![],
    }))
}

fn trigger_events(seen: &[pb::WorkerMessage]) -> Vec<&pb::TriggerEvent> {
    seen.iter()
        .filter_map(|m| match &m.message {
            Some(worker_message::Message::TriggerEvent(event)) => Some(event),
            _ => None,
        })
        .collect()
}

fn trigger_stops(seen: &[pb::WorkerMessage]) -> Vec<&pb::TriggerStopped> {
    seen.iter()
        .filter_map(|m| match &m.message {
            Some(worker_message::Message::TriggerStopped(stopped)) => Some(stopped),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_trigger_tells_what_it_found_and_a_stop_ends_it_without_an_error() {
    let script = vec![
        registered_fast(),
        assign_trigger("t1", "found"),
        assign_trigger("t1", "found"),
        to_worker(master_message::Message::TriggerEventAck(
            pb::TriggerEventAck {
                instance_id: "t1".to_owned(),
                seq: 99,
            },
        )),
        to_worker(master_message::Message::CancelTrigger(pb::CancelTrigger {
            instance_id: "t1".to_owned(),
        })),
    ];
    let done = |m: &[pb::WorkerMessage]| !trigger_stops(m).is_empty();
    let (seen, _) = against_script(2, Start::With(vec![]), script, done).await;
    let events = trigger_events(&seen);
    assert_eq!(events.len(), 1, "once, however many times it was given");
    assert_eq!(events[0].dag_id, "sync");
    assert_eq!(events[0].instance_id, "t1");
    assert_eq!(events[0].seq, 1);
    assert_eq!(events[0].key, "after-b");
    assert_eq!(events[0].data_json, b"1");
    assert_eq!(events[0].at.as_ref().map(|at| at.seconds), Some(0));
    let stopped = trigger_stops(&seen);
    assert_eq!(stopped[0].instance_id, "t1");
    assert_eq!(stopped[0].error_kind, "");
}

#[tokio::test]
async fn a_failed_trigger_says_why_and_is_gone_from_the_heartbeat() {
    let script = vec![registered_fast(), assign_trigger("t1", "breaks")];
    let done = |m: &[pb::WorkerMessage]| !trigger_stops(m).is_empty() && heartbeats(m) >= 3;
    let (seen, _) = against_script(2, Start::With(vec![]), script, done).await;
    let stopped = trigger_stops(&seen);
    assert_eq!(stopped[0].error_kind, "trigger_failed");
    assert_eq!(stopped[0].message, "ValueError: no");
    let last_beat = seen
        .iter()
        .rev()
        .find_map(|m| match &m.message {
            Some(worker_message::Message::Heartbeat(beat)) => Some(beat),
            _ => None,
        })
        .unwrap();
    assert_eq!(last_beat.running_triggers, Vec::<String>::new());
}

#[tokio::test]
async fn a_trigger_that_cant_start_here_is_told_as_stopped() {
    let mut bad_json = assign_trigger("t1", "found");
    let Some(master_message::Message::AssignTrigger(assign)) = &mut bad_json.message else {
        unreachable!()
    };
    assign.task_json = b"{".to_vec();
    let mut gone_file = assign_trigger("t2", "found");
    let Some(master_message::Message::AssignTrigger(assign)) = &mut gone_file.message else {
        unreachable!()
    };
    assign.dag_file = Some(pb::DagFileRef {
        root: "etl".to_owned(),
        path: "gone.py".to_owned(),
    });
    let mut other_code = assign_trigger("t3", "found");
    let Some(master_message::Message::AssignTrigger(assign)) = &mut other_code.message else {
        unreachable!()
    };
    assign.expected_code_hash = "sha256:other".to_owned();
    let script = vec![registered_fast(), bad_json, gone_file, other_code];
    let done = |m: &[pb::WorkerMessage]| trigger_stops(m).len() >= 3;
    let (seen, _) = against_script(2, Start::With(vec![]), script, done).await;
    let stopped = trigger_stops(&seen);
    for (one, instance) in stopped.iter().zip(["t1", "t2", "t3"]) {
        assert_eq!(one.instance_id, instance);
        assert_eq!(one.error_kind, "not_placed", "{}", one.message);
        assert_ne!(one.message, "");
    }
}

#[test]
fn only_a_trigger_call_that_returned_is_done_with_its_run() {
    for (status, done) in [
        (pb::AttemptStatus::Success, true),
        (pb::AttemptStatus::Interrupted, false),
        (pb::AttemptStatus::Cancelled, false),
    ] {
        let finished = pb::Finished {
            status: status as i32,
            ..pb::Finished::default()
        };
        let stopped = super::trigger_stopped("sync".to_owned(), "i1".to_owned(), &finished);
        assert_eq!(stopped.error_kind, "", "{status:?}: no error");
        assert_eq!(stopped.done, done, "{status:?}");
    }
}

/// M4.11h: a task's API call not answered on the old stream is sent again once registered on a
/// new one; one its task gave up on is dropped.
#[tokio::test]
async fn an_unanswered_api_call_is_sent_again_on_a_new_stream() {
    let call = |request_id: u64| pb::ApiCall {
        request_id,
        attempt_id: "a1".to_owned(),
        method: "POST".to_owned(),
        path: "/api/v1/events".to_owned(),
        ..pb::ApiCall::default()
    };
    let (waiting, _answer) = tokio::sync::oneshot::channel();
    let (given_up, answer) = tokio::sync::oneshot::channel();
    drop(answer);
    let registered = to_worker(master_message::Message::Registered(
        pb::Registered::default(),
    ));
    let (seen, _) = against_script_with(
        1,
        Start::With(vec![]),
        vec![registered],
        |m| kinds(m).contains(&"api call"),
        |agent| {
            agent.api_calls.insert(1, (call(1), waiting));
            agent.api_calls.insert(2, (call(2), given_up));
        },
    )
    .await;
    let sent: Vec<u64> = seen
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::ApiCall(call) => Some(call.request_id),
            _ => None,
        })
        .collect();
    assert_eq!(sent, [1]);
}

/// Codex's and Grok's reviews of M4.11h: a call's ID is past the last incarnation's, one over
/// the master's limit is answered here, one nobody waits for any more isn't sent.
#[tokio::test]
async fn an_api_call_gets_a_fresh_id_and_one_too_large_or_given_up_isnt_sent() {
    let earlier = super::first_api_call_id();
    let dir = TempDir::new().unwrap();
    let mut agent = offline_agent(dir.path());
    let send = agent.api_send();
    let call = |path: String| pb::ApiCall {
        attempt_id: "a1".to_owned(),
        method: "POST".to_owned(),
        path,
        ..pb::ApiCall::default()
    };
    let _waiting = send(call("/api/v1/events".to_owned()));
    offline_until(&mut agent, |agent| agent.api_calls.len() == 1).await;
    let id = *agent.api_calls.keys().next().unwrap();
    assert!(id > earlier, "{id} after {earlier}");
    drop(send(call("/api/v1/events".to_owned())));
    agent.master_limit = 4096;
    let mut refused = send(call("x".repeat(8192)));
    let answer = loop {
        agent
            .offline(tokio::time::sleep(Duration::from_millis(10)))
            .await;
        if let Ok(answer) = refused.try_recv() {
            break answer;
        }
    };
    assert_eq!(answer.status, 413);
    assert_eq!(
        agent.api_calls.keys().copied().collect::<Vec<_>>(),
        [id],
        "neither the one given up nor the one too large kept to be sent again"
    );
}

/// An agent whose master time was last read from a heartbeat sent `ago`, stamped then (the
/// master's last word reached it only now), with a 60 s lease.
fn stamped_long_ago(dir: &Path, ago: Duration) -> Agent<Fake> {
    let mut agent = offline_agent(dir);
    agent.lease = Some(Duration::from_secs(60));
    let now = Local::now();
    let then = Local {
        boot: now.boot.checked_sub(ago).unwrap(),
        wall: now.wall.checked_sub(ago).unwrap(),
    };
    let sequence = agent.master_time.sent(then);
    let clock = pb::MasterClock {
        timeline: 7,
        master_ms: 1_000,
        heard_sequence: sequence,
        wall_ms: 0,
        lease_epoch: 1,
        sequence: 0,
    };
    agent.master_time.heard(&clock, then);
    agent.heard = tokio::time::Instant::now();
    agent
}

fn left(at: Option<tokio::time::Instant>) -> Duration {
    at.unwrap()
        .saturating_duration_since(tokio::time::Instant::now())
}

#[test]
fn a_reconnect_keeps_the_cutoff_the_last_stamps_set() {
    // Grok's review of D445: the last stamp was 30 s ago in master time, so the cutoff is in
    // about 30 s, not the receipt's 60 s; a new session's opening kept it there only until
    // this fix (it moved out to 60 s). A stamp of the new session counts from then on.
    let dir = TempDir::new().unwrap();
    let mut agent = stamped_long_ago(dir.path(), Duration::from_secs(30));
    assert!(left(agent.lease_deadline()) < Duration::from_secs(31));
    agent.open_session();
    assert!(
        left(agent.lease_deadline()) < Duration::from_secs(31),
        "kept over the reconnect"
    );
    // The session's `Register` (number 0) goes out through its sender, as `session` sends it.
    let (sender, _from_agent) = mpsc::unbounded_channel();
    let to_master = ToMaster::new(sender, agent.master_time.outbox());
    to_master
        .send(wrap(worker_message::Message::Register(
            pb::Register::default(),
        )))
        .unwrap();
    let clock = pb::MasterClock {
        timeline: 8,
        master_ms: 5_000,
        heard_sequence: 0,
        wall_ms: 0,
        lease_epoch: 1,
        sequence: 0,
    };
    agent.heard_master(Some(&clock), &to_master);
    assert!(
        left(agent.lease_deadline()) > Duration::from_secs(55),
        "the new stamp counts"
    );
}

#[test]
fn the_cleanup_gates_read_the_master_time_cutoff() {
    // Grok's review of D445: the deadline the executor's cleanup gates read was the receipt's
    // alone, 60 s out while the master-time cutoff was 30 s.
    let dir = TempDir::new().unwrap();
    let agent = stamped_long_ago(dir.path(), Duration::from_secs(30));
    agent.publish_deadline();
    assert!(left(*agent.deadline.borrow()) < Duration::from_secs(31));
}

#[test]
fn the_sample_age_grows_while_no_sample_is_kept() {
    // Grok's review of D448: between sessions the gauge stayed at the last sample's age.
    let dir = TempDir::new().unwrap();
    let mut agent = stamped_long_ago(dir.path(), Duration::from_secs(30));
    agent.publish_master_reading();
    let before = agent.metrics.master_sample_age.get();
    assert!(before >= 30.0, "{before}");
    agent.open_session();
    std::thread::sleep(Duration::from_millis(20));
    agent.publish_master_reading();
    assert!(agent.metrics.master_sample_age.get() > before);
}

#[tokio::test]
async fn a_fork_while_disconnected_is_told_to_the_next_session() {
    // B24: the second `Started` (fork) went only on a link up; the next session's `Register`
    // carries no method, so the master kept `spawn`.
    let dir = tempfile::tempdir().unwrap();
    let mut agent = offline_agent(dir.path());
    let (stop, _stopped) = mpsc::unbounded_channel();
    agent.running.insert("a1".to_owned(), stop);
    agent
        .on_done(
            Done::Spawned {
                attempt_id: "a1".to_owned(),
                identity: NO_PROCESS,
                forked: true,
            },
            None,
        )
        .await;
    let (sender, mut sent) = mpsc::unbounded_channel();
    let to_master = ToMaster::new(sender, SharedOutbox::default());
    let decision = pb::JournalDecision {
        attempt_id: "a1".to_owned(),
        decision: pb::AttemptDecision::Continue.into(),
        ..pb::JournalDecision::default()
    };
    agent.decide(&decision, &to_master).await;
    let mut methods = Vec::new();
    while let Ok(message) = sent.try_recv() {
        if let Some(worker_message::Message::Started(started)) = message.message {
            methods.push((started.attempt_id, started.start_method));
        }
    }
    assert_eq!(
        methods,
        [("a1".to_owned(), i32::from(pb::StartMethod::Fork))]
    );
}

#[tokio::test]
async fn a_stop_ends_a_session_whose_stream_never_opens() {
    // B151: opening the stream waited for the master (up to a lease) without hearing a stop.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // Accepts and holds each connection, answering nothing: the stream never opens.
    let held = tokio::spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            sockets.push(socket);
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let mut agent = offline_agent(dir.path());
    agent.settings.drain.grace = Duration::ZERO;
    let (requests, shutdown) = watch::channel(0);
    agent.shutdown = Some(shutdown);
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    let asking = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = requests.send(2);
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let ended = tokio::time::timeout(
        Duration::from_secs(10),
        agent.session(MasterClient::new(channel)),
    )
    .await;
    asking.abort();
    held.abort();
    assert!(
        matches!(ended, Ok(SessionEnd::Stopped)),
        "the session didn't end on the stop: {:?}",
        ended.map(|end| matches!(end, SessionEnd::Lost(_)))
    );
}

#[tokio::test]
async fn a_stop_while_connecting_waits_for_an_unacknowledged_result() {
    // Grok's review of B151: a drain finished at once while the stream opened, though a
    // result was still to be acknowledged.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let held = tokio::spawn(async move {
        let mut sockets = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            sockets.push(socket);
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let mut agent = offline_agent(dir.path());
    agent.settings.drain.grace = Duration::from_secs(30);
    let JournalRecord::Finished(finished) = success("a1") else {
        unreachable!()
    };
    agent.unacked.insert("a1".to_owned(), finished);
    let (requests, shutdown) = watch::channel(0);
    agent.shutdown = Some(shutdown);
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_lazy();
    let asking = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = requests.send(1);
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    let ended = tokio::time::timeout(
        Duration::from_secs(2),
        agent.session(MasterClient::new(channel)),
    )
    .await;
    asking.abort();
    held.abort();
    assert!(ended.is_err(), "it stopped with the result unacknowledged");
}

#[tokio::test]
async fn a_fork_that_finished_while_disconnected_is_told_before_its_result() {
    // Grok's review of B24: a forked attempt that ended before the reconnect kept `spawn`.
    let dir = tempfile::tempdir().unwrap();
    let mut agent = offline_agent(dir.path());
    let (stop, _stopped) = mpsc::unbounded_channel();
    agent.running.insert("a1".to_owned(), stop);
    agent
        .on_done(
            Done::Spawned {
                attempt_id: "a1".to_owned(),
                identity: NO_PROCESS,
                forked: true,
            },
            None,
        )
        .await;
    let JournalRecord::Finished(finished) = success("a1") else {
        unreachable!()
    };
    agent.on_done(Done::Attempt(finished), None).await;
    let (sender, mut sent) = mpsc::unbounded_channel();
    let to_master = ToMaster::new(sender, SharedOutbox::default());
    let decision = pb::JournalDecision {
        attempt_id: "a1".to_owned(),
        decision: pb::AttemptDecision::Continue.into(),
        ..pb::JournalDecision::default()
    };
    agent.decide(&decision, &to_master).await;
    let mut kinds = Vec::new();
    while let Ok(message) = sent.try_recv() {
        match message.message {
            Some(worker_message::Message::Started(s)) => {
                kinds.push(format!("started {}", s.start_method));
            }
            Some(worker_message::Message::Finished(_)) => kinds.push("finished".to_owned()),
            _ => {}
        }
    }
    let fork = i32::from(pb::StartMethod::Fork);
    assert_eq!(kinds, [format!("started {fork}"), "finished".to_owned()]);
}
