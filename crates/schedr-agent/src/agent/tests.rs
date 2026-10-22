use std::path::Path;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use schedr_db::Database;
use schedr_master::core::{MasterCore, Settings};
use schedr_master::service::Hub;
use schedr_model::commands::Command;
use schedr_model::{DagId, RunState};
use serde_json::json;
use tempfile::TempDir;
use tonic::transport::{Endpoint, Server};

use schedr_model::runner::DAG_FILE_MISSING;

use super::*;

/// Parses each file into a DAG named after it, with one Python task; runs tasks at once,
/// except task `wait`, which waits until it is stopped. Each attempt's "process" is one that
/// doesn't exist.
#[derive(Clone, Default)]
struct Fake {
    parsed: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Executor for Fake {
    async fn run(
        &self,
        spec: AttemptSpec,
        _plan: Plan,
        stop: oneshot::Receiver<StopReason>,
        spawned: Spawned,
    ) -> pb::Finished {
        spawned(NO_PROCESS);
        let status = if spec.task.id.as_str() == "wait" {
            match stop.await {
                Ok(StopReason::Interrupted) => pb::AttemptStatus::Interrupted,
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

    async fn parse(
        &self,
        root: String,
        _dir: PathBuf,
        files: Vec<String>,
    ) -> Result<Vec<ParsedFile>, String> {
        tokio::task::yield_now().await; // a real parse starts a runner
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
                        "produced_by": "0.1", "id": stem,
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
}

/// A master over SQLite, and an agent with a fake executor connected to it in process,
/// watching root `etl` (files `sync.py` and `wait.py`).
async fn world() -> World {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("etl");
    std::fs::create_dir_all(&root).unwrap();
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
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 2,
        setup: WorkerSetup {
            roots: BTreeMap::from([("etl".to_owned(), root.clone())]),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.path().join("journal"),
        termination_grace_period: Duration::from_secs(30),
        log_endpoint: String::new(),
    };
    let fake = Fake::default();
    let mut agent = Agent::new(settings, fake.clone(), "i1".to_owned());
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
async fn shut_down(world: World) {
    let _ = world.stop.send(1);
    tokio::time::timeout(Duration::from_secs(20), world.agent)
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
#[derive(Clone)]
struct Scripted {
    script: Vec<pb::MasterMessage>,
    received: Arc<Mutex<Vec<pb::WorkerMessage>>>,
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
        let (sender, stream) = mpsc::channel(64);
        for message in self.script.clone() {
            sender.send(Ok(message)).await.unwrap();
        }
        let received = Arc::clone(&self.received);
        tokio::spawn(async move {
            let _keep_open = sender;
            while let Ok(Some(message)) = inbound.message().await {
                received.lock().unwrap().push(message);
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(stream)))
    }

    async fn upload_parse_result(
        &self,
        _request: tonic::Request<pb::ParseResult>,
    ) -> Result<tonic::Response<pb::ParseResultAck>, tonic::Status> {
        Ok(tonic::Response::new(pb::ParseResultAck {}))
    }

    async fn fetch_inputs(
        &self,
        _request: tonic::Request<pb::FetchInputsRequest>,
    ) -> Result<tonic::Response<Self::FetchInputsStream>, tonic::Status> {
        let (sender, receiver) = mpsc::channel(1);
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

/// The journal an agent starts with.
enum Start {
    /// These records (none: a new worker).
    With(Vec<(&'static str, JournalRecord)>),
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
    let dir = TempDir::new().unwrap();
    let journal = Journal::new(dir.path().join("journal"));
    match start {
        Start::With(records) => {
            for (attempt_id, record) in records {
                journal.write(attempt_id, &record).unwrap();
            }
        }
        Start::Unwritable => std::fs::write(journal.dir(), "a file").unwrap(),
    }
    let root = dir.path().join("etl");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("sync.py"), "").unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let master = Scripted {
        script,
        received: Arc::clone(&received),
    };
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
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots,
        setup: WorkerSetup {
            roots: BTreeMap::from([("etl".to_owned(), root)]),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: journal.dir().to_path_buf(),
        termination_grace_period: Duration::from_secs(30),
        log_endpoint: "http://w1:8081".to_owned(),
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

fn kinds(messages: &[pb::WorkerMessage]) -> Vec<&'static str> {
    messages
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Register(_) => Some("register"),
            worker_message::Message::Heartbeat(_) => None,
            worker_message::Message::CodeVersionChanged(_) => Some("code"),
            worker_message::Message::Started(_) => Some("started"),
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
            worker_message::Message::Finished(f) => {
                Some(match pb::AttemptStatus::try_from(f.status) {
                    Ok(pb::AttemptStatus::Success) => "finished: success",
                    Ok(pb::AttemptStatus::Interrupted) => "finished: interrupted",
                    _ => "finished: failed",
                })
            }
            worker_message::Message::Draining(_) => Some("draining"),
            worker_message::Message::Deregister(_) => Some("leaving"),
        })
        .collect()
}

#[tokio::test]
async fn without_a_free_slot_an_assignment_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let (seen, _) = against_script(0, Start::With(vec![]), script, |m| m.len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: no slot"]);
    let worker_message::Message::Register(register) = seen[0].message.as_ref().unwrap() else {
        panic!("expected Register")
    };
    assert_eq!((register.worker_id.as_str(), register.slots), ("w1", 0));
    assert_eq!(register.log_endpoint, "http://w1:8081");
    assert_eq!(register.roots[0].root, "etl");
    assert!(register.roots[0].content_hash.starts_with("sha256:"));
}

#[tokio::test]
async fn bad_and_impossible_assignments_are_rejected_or_failed_and_good_ones_run() {
    let timing = to_worker(master_message::Message::Registered(pb::Registered {
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![
            pb::JournalDecision {
                attempt_id: "old-1".to_owned(),
                decision: pb::AttemptDecision::Ack.into(),
            },
            pb::JournalDecision {
                attempt_id: "old-2".to_owned(),
                decision: pb::AttemptDecision::Kill.into(),
            },
            pb::JournalDecision {
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
        timing: Some(pb::Timing {
            heartbeat_interval_ms: 20,
            lease_timeout_ms: 60_000,
            cancel_grace_ms: 1000,
        }),
        decisions: vec![
            pb::JournalDecision {
                attempt_id: "old-1".to_owned(),
                decision: pb::AttemptDecision::Ack.into(),
            },
            pb::JournalDecision {
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
async fn an_attempt_the_journal_cant_record_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let (seen, _) = against_script(2, Start::Unwritable, script, |m| m.len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: disk"]);
    let Some(worker_message::Message::Rejected(rejected)) = seen[1].message.as_ref() else {
        panic!("expected Rejected")
    };
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
async fn an_attempt_assigned_again_is_ignored() {
    let timing = to_worker(master_message::Message::Registered(pb::Registered {
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
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.path().join("journal"),
        termination_grace_period: Duration::from_secs(30),
        log_endpoint: String::new(),
    };
    let journal = Journal::new(settings.journal_dir.clone());
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    let done = agent.done_tx.clone();
    done.send(Done::Spawned {
        attempt_id: "a1".to_owned(),
        identity: NO_PROCESS,
    })
    .unwrap();
    done.send(Done::Spawned {
        attempt_id: "a2".to_owned(),
        identity: NO_PROCESS,
    })
    .unwrap();
    let JournalRecord::Finished(finished) = success("a2") else {
        unreachable!()
    };
    done.send(Done::Parse {
        job_id: "j1".to_owned(),
        root: root_hash("etl", "sha256:h"),
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
        }],
        "reported in the next `Register`"
    );
}

#[tokio::test]
async fn an_assignment_for_other_code_than_the_workers_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let mut stale = assign("a1", &python, "sync.py", false);
    if let Some(master_message::Message::AssignTask(a)) = stale.message.as_mut() {
        a.expected_code_hash = "sha256:newer".to_owned();
    }
    let (seen, _) = against_script(2, Start::With(vec![]), vec![stale], |m| m.len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: code"]);
    let Some(worker_message::Message::Rejected(rejected)) = seen[1].message.as_ref() else {
        panic!("expected Rejected")
    };
    assert!(
        rejected.detail.starts_with("root `etl` here is at sha256:")
            && rejected.detail.ends_with(", the task expects sha256:newer"),
        "{}",
        rejected.detail
    );
}

/// Each of the `count` results sent names the code it ran with.
fn assert_results_name_their_code(seen: &[pb::WorkerMessage], count: usize) {
    let code_hashes: Vec<&str> = seen
        .iter()
        .filter_map(|m| match m.message.as_ref()? {
            worker_message::Message::Finished(f) => Some(f.code_hash.as_str()),
            _ => None,
        })
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
    let mut watcher = RootWatcher::new(copy.path(), Duration::ZERO);
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
        agent.settings.termination_grace_period = Duration::ZERO;
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
async fn a_draining_agent_rejects_assignments_and_says_so_again_after_a_reconnect() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let (seen, _) = against_script_with(
        2,
        Start::With(vec![]),
        script,
        |m| kinds(m).len() >= 3,
        |agent| {
            agent.drain = Some(Drain::new(Instant::now(), Duration::from_secs(600)));
            // Something still runs: the drain isn't over.
            agent.running.insert("other".to_owned(), None);
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
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: settings_dir.path().join("logs"),
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: settings_dir.path().join("journal"),
        termination_grace_period: Duration::from_secs(600),
        log_endpoint: String::new(),
    };
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    assert!(!agent.on_shutdown(0));
    let (stop, stopped) = oneshot::channel();
    agent.running.insert("a1".to_owned(), Some(stop));
    assert!(agent.on_shutdown(1));
    assert_eq!(agent.drain_step(true), DrainStep::Wait, "600 s of grace");
    assert!(!agent.on_shutdown(2));
    assert_eq!(agent.drain_step(true), DrainStep::Interrupt);
    assert_eq!(stopped.blocking_recv(), Ok(StopReason::Interrupted));
}

#[tokio::test]
async fn a_cancelled_attempt_keeps_its_slot_until_its_process_has_ended() {
    let dir = TempDir::new().unwrap();
    let settings = AgentSettings {
        worker_id: "w1".to_owned(),
        queues: vec![],
        slots: 1,
        setup: WorkerSetup {
            roots: BTreeMap::new(),
            python: PathBuf::from("python3"),
            agent_env: BTreeMap::new(),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
        },
        poll_interval: Duration::from_millis(20),
        settle_time: Duration::ZERO,
        journal_dir: dir.path().join("journal"),
        termination_grace_period: Duration::from_secs(30),
        log_endpoint: String::new(),
    };
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    let (stop, stopped) = oneshot::channel();
    agent.running.insert("a1".to_owned(), Some(stop));
    let (to_master, _outbound) = mpsc::channel(8);
    let kill = pb::JournalDecision {
        attempt_id: "a1".to_owned(),
        decision: pb::AttemptDecision::Kill.into(),
    };
    agent.decide(&kill, &to_master).await;
    assert_eq!(stopped.await, Ok(StopReason::Cancelled));
    assert_eq!(agent.running(), ["a1"], "still running until it reports");
    assert_eq!(agent.free_slots(), 0);
}
