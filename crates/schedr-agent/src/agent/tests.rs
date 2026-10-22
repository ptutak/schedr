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

use super::*;

/// Parses each file into a DAG named after it, with one Python task; runs tasks at once,
/// except task `wait`, which waits until it is stopped.
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
    ) -> pb::Finished {
        let status = if spec.task.id.as_str() == "wait" {
            let _ = stop.await;
            pb::AttemptStatus::Cancelled
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
                        "trigger": {"kind": "none"}, "start": null, "end": null,
                        "initial_since": null, "max_active_runs": 1,
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

struct World {
    _dir: TempDir,
    root: PathBuf,
    db: Database,
    hub: Hub,
    fake: Fake,
    stop: Option<oneshot::Sender<()>>,
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
    };
    let fake = Fake::default();
    let mut agent = Agent::new(settings, fake.clone(), "i1".to_owned());
    let (stop, stopped) = oneshot::channel::<()>();
    let agent = tokio::spawn(async move {
        let connect = move || {
            let client = client.lock().unwrap().take();
            async move { client.ok_or_else(|| "connected once".to_owned()) }
        };
        agent
            .run(connect, async {
                let _ = stopped.await;
            })
            .await;
    });
    World {
        _dir: dir,
        root,
        db,
        hub,
        fake,
        stop: Some(stop),
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

async fn shut_down(mut world: World) {
    let _ = world.stop.take().unwrap().send(());
    world.agent.await.unwrap();
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

/// Runs an agent with `slots` against a master playing `script`; returns what the agent sent
/// once `done` holds for it.
async fn against_script(
    slots: u32,
    script: Vec<pb::MasterMessage>,
    done: impl Fn(&[pb::WorkerMessage]) -> bool,
) -> Vec<pb::WorkerMessage> {
    let dir = TempDir::new().unwrap();
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
    };
    let mut agent = Agent::new(settings, Fake::default(), "i1".to_owned());
    assert_eq!(agent.running(), Vec::<String>::new());
    let session = tokio::spawn(async move { agent.session(MasterClient::new(channel)).await });
    for _ in 0..500 {
        let seen = received.lock().unwrap().clone();
        if done(&seen) {
            session.abort();
            return seen;
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
                    _ => "rejected",
                })
            }
            worker_message::Message::Finished(f) => {
                Some(if f.status == i32::from(pb::AttemptStatus::Success) {
                    "finished: success"
                } else {
                    "finished: failed"
                })
            }
            worker_message::Message::Draining(_) | worker_message::Message::Deregister(_) => {
                Some("leaving")
            }
        })
        .collect()
}

#[tokio::test]
async fn without_a_free_slot_an_assignment_is_rejected() {
    let python = task_json(&json!({"kind": "python"}));
    let script = vec![assign("a1", &python, "sync.py", false)];
    let seen = against_script(0, script, |m| m.len() >= 2).await;
    assert_eq!(kinds(&seen), ["register", "rejected: no slot"]);
    let worker_message::Message::Register(register) = seen[0].message.as_ref().unwrap() else {
        panic!("expected Register")
    };
    assert_eq!((register.worker_id.as_str(), register.slots), ("w1", 0));
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
    let seen = against_script(2, script, done).await;
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
}
