//! The roles as separate processes (PLAN.md 2.1): `schedr master`, `schedr webserver` and
//! `schedr worker`, each its own process, on Postgres and on one SQLite file, driven through
//! the CLI like any deployment.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed) and
//! `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` sets both.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use schedr_agent::journal::{Journal, JournalRecord};
use schedr_proto::pb;
use serde_json::Value;
use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_schedr");

const CHAIN: &str = r#"
from schedr import CommandOperator, Dag, task

@task
def extract() -> dict[str, int]:
    return {"rows": 3}

@task
def load(rows: int) -> str:
    return f"loaded {rows}"

with Dag("chain"):
    done = load(extract()["rows"])
    CommandOperator("report", t"echo report {done}")
"#;

fn python() -> String {
    std::env::var("SCHEDR_TEST_PYTHON").expect(
        "SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)",
    )
}

/// A new database of this test's own on the test Postgres server.
fn postgres_database() -> String {
    let admin = std::env::var("SCHEDR_TEST_POSTGRES_URL").expect(
        "SCHEDR_TEST_POSTGRES_URL: an administrative Postgres URL (`make test-integration` sets it)",
    );
    let name = format!("roles_{}", uuid::Uuid::now_v7().simple());
    let created = Command::new("psql")
        .args([admin.as_str(), "-c", &format!("CREATE DATABASE {name}")])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let (server, _) = admin.rsplit_once('/').unwrap();
    format!("{server}/{name}")
}

/// A running role, killed when dropped.
struct Role {
    name: &'static str,
    child: Child,
    log: PathBuf,
}

impl Drop for Role {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The three roles over one DAG root, with their own directory and ports.
struct Cluster {
    dir: TempDir,
    database_url: String,
    /// The REST API, once the webserver listens.
    api: String,
    /// The master's port once it listens (kept for a restart) and the URL workers use.
    grpc_port: Option<u16>,
    roles: Vec<Role>,
}

impl Cluster {
    fn new(database_url: Option<String>, tick: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("dags/etl");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("chain.py"), CHAIN).unwrap();
        let database_url = database_url
            .unwrap_or_else(|| format!("sqlite://{}", dir.path().join("schedr.db").display()));
        // Port 0: each role binds a free port itself and logs it (picking free ports for it
        // races with the tests running in parallel).
        let config = format!(
            "[dags]\npoll_interval = \"200ms\"\nsettle_time = \"0s\"\n\n\
             [interpreters]\npython = \"{python}\"\n\n\
             [worker]\nid = \"w1\"\ndata_dir = \"{data}\"\nslots = 4\n\n\
             [master]\nbind = \"127.0.0.1\"\ngrpc_port = 0\ntick_interval = \"{tick}\"\n\
             heartbeat_interval = \"1s\"\nlease_timeout = \"4s\"\n\n\
             [webserver]\nbind = \"127.0.0.1\"\nport = 0\n",
            python = python(),
            data = dir.path().join("data").display(),
        );
        std::fs::write(dir.path().join("config.toml"), config).unwrap();
        Self {
            dir,
            database_url,
            api: String::new(),
            grpc_port: None,
            roles: Vec::new(),
        }
    }

    fn root(&self) -> PathBuf {
        self.dir.path().join("dags/etl")
    }

    /// `schedr <args>` with this cluster's configuration and database: exit code and stderr.
    fn schedr(&self, args: &[&str]) -> (i32, String) {
        let output = self.command(args).output().unwrap();
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(BINARY);
        command
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.dir.path())
            .env("TZ", "UTC")
            .env("SCHEDR_CONFIG__DATABASE__URL", &self.database_url)
            .env("SCHEDR_DAG_PATH", self.root());
        if let Some(port) = self.grpc_port {
            command
                .env("SCHEDR_CONFIG__MASTER__GRPC_PORT", port.to_string())
                .env(
                    "SCHEDR_CONFIG__WORKER__MASTER_URL",
                    format!("http://127.0.0.1:{port}"),
                );
        }
        command
            .arg("--config")
            .arg(self.dir.path().join("config.toml"))
            .args(args);
        command
    }

    /// Kills a role at once (SIGKILL), as a crash or a lost pod would.
    fn kill(&mut self, name: &'static str) {
        let index = self.roles.iter().position(|r| r.name == name).unwrap();
        drop(self.roles.remove(index));
    }

    fn start(&mut self, name: &'static str) {
        self.start_with(name, &[]);
    }

    /// Starts a role with more environment variables.
    fn start_with(&mut self, name: &'static str, env: &[(&str, &str)]) {
        self.start_as(name, name, env);
    }

    /// Starts `role` under the name `name` (its log is `<name>.log`), with more environment
    /// variables: a second worker, say.
    fn start_as(&mut self, name: &'static str, role: &str, env: &[(&str, &str)]) {
        let log = self.dir.path().join(format!("{name}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let child = self
            .command(&[role])
            .envs(env.iter().copied())
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        self.roles.push(Role {
            name,
            child,
            log: log.clone(),
        });
        let bound = |marker: &str| {
            let mut found = None;
            self.wait(&format!("`{marker}` in the {name} log"), || {
                let text = std::fs::read_to_string(&log).unwrap_or_default();
                found = text.split(marker).nth(1).map(|rest| {
                    rest.split(|c: char| c == '"' || c.is_whitespace())
                        .next()
                        .unwrap()
                        .to_owned()
                });
                found.is_some()
            });
            found.unwrap()
        };
        match name {
            "master" => {
                let address = bound("schedr master: gRPC on ");
                let port = address.rsplit(':').next().unwrap().parse().unwrap();
                self.grpc_port = Some(port);
            }
            "webserver" => self.api = format!("http://{}", bound("API on http://")),
            _ => {}
        }
    }

    fn logs(&self) -> String {
        use std::fmt::Write as _;
        let mut text = String::new();
        for role in &self.roles {
            let log = std::fs::read_to_string(&role.log).unwrap_or_default();
            let _ = write!(text, "--- {} ---\n{log}", role.name);
        }
        text
    }

    /// The CLI against the webserver: exit code, stdout, stderr.
    fn cli(&self, args: &[&str]) -> (i32, String, String) {
        let (group, rest) = args.split_first().unwrap();
        let output = Command::new(BINARY)
            .arg(group)
            .args(["--server", &self.api])
            .args(rest)
            .env("TZ", "UTC")
            .output()
            .unwrap();
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    fn cli_json(&self, args: &[&str]) -> Option<Value> {
        let mut with_json = args.to_vec();
        with_json.insert(1, "--json");
        let (code, out, _) = self.cli(&with_json);
        (code == 0).then(|| serde_json::from_str(&out).unwrap())
    }

    fn wait(&self, what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !done() {
            assert!(
                Instant::now() < deadline,
                "waited too long for {what}\n{}",
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Migrates the database and starts the master, the webserver and the worker.
    fn up(&mut self) {
        let (code, err) = self.schedr(&["db", "migrate"]);
        assert_eq!(code, 0, "{err}");
        for role in ["master", "webserver", "worker"] {
            self.start(role);
        }
        self.wait("DAG `chain`", || {
            self.cli_json(&["dags", "list"]).is_some_and(|dags| {
                dags["dags"]
                    .as_array()
                    .is_some_and(|d| d.iter().any(|dag| dag["dag_id"] == "chain"))
            })
        });
    }

    /// Unpauses `chain`, triggers a run for a past day, and waits until it succeeds.
    fn run_chain(&self) {
        let (code, _, err) = self.cli(&["dags", "unpause", "chain"]);
        assert_eq!(code, 0, "{err}\n{}", self.logs());
        let (code, out, err) = self.cli(&["runs", "trigger", "chain", "--for", "2026-10-15"]);
        assert_eq!(code, 0, "{err}\n{}", self.logs());
        assert!(out.contains("triggered run"), "{out}");
        self.wait("the run to succeed", || {
            self.cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
                .is_some_and(|runs| runs["runs"][0]["state"] == "success")
        });
        let logs = std::fs::read_dir(self.dir.path().join("data/logs"))
            .unwrap()
            .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap_or_default())
            .collect::<String>();
        assert!(logs.contains("report loaded 3"), "{logs}");
    }
}

#[test]
fn three_processes_on_postgres_run_a_dag() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    cluster.up();
    cluster.run_chain();
}

#[test]
fn three_processes_on_one_sqlite_file_run_a_dag() {
    let mut cluster = Cluster::new(None, "100ms");
    cluster.up();
    cluster.run_chain();
    let master = std::fs::read_to_string(&cluster.roles[0].log).unwrap();
    assert!(
        master.contains("on SQLite: every role must run on this host"),
        "{master}"
    );
}

#[test]
fn on_postgres_a_command_is_applied_at_once_not_at_the_next_tick() {
    // With an hour between ticks, only the notification can make the master apply a command
    // while the CLI waits for it (up to 30 s).
    let mut cluster = Cluster::new(Some(postgres_database()), "1h");
    cluster.up();
    let started = Instant::now();
    let (code, out, err) = cluster.cli(&["dags", "unpause", "chain"]);
    assert_eq!(code, 0, "{out}{err}\n{}", cluster.logs());
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn the_master_refuses_a_database_not_migrated_yet() {
    let cluster = Cluster::new(Some(postgres_database()), "100ms");
    let (code, err) = cluster.schedr(&["master"]);
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("run `schedr db migrate` first"), "{err}");
}

#[test]
fn a_worker_needs_its_master_url() {
    let cluster = Cluster::new(None, "100ms");
    let output = Command::new(BINARY)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", cluster.dir.path())
        .arg("worker")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.contains("set [worker] master_url"), "{err}");
}

#[test]
fn a_role_stops_on_sigterm() {
    let mut cluster = Cluster::new(None, "100ms");
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("webserver");
    cluster.wait("the API to answer", || {
        cluster.cli_json(&["dags", "list"]).is_some()
    });
    let role = &mut cluster.roles[0];
    let pid = role.child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = role.child.try_wait().unwrap() {
            assert!(status.success(), "{status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the webserver didn't stop on SIGTERM"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A task that runs long on its first try (leaving a marker with its process ID) and returns
/// at once after.
fn slow_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, time
from datetime import timedelta
from schedr import Dag, task

@task(retries=1, retry_delay=timedelta(0))
def work() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        with open(marker, "w") as file:
            file.write(str(os.getpid()))
        time.sleep(15)
    return "done"

with Dag("slow"):
    work()
"#,
        marker = marker.display().to_string()
    )
}

impl Cluster {
    fn api_get(&self, path: &str) -> Option<Value> {
        let output = Command::new("curl")
            .args(["-sf", &format!("{}{path}", self.api)])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| serde_json::from_slice(&output.stdout).unwrap())
    }

    /// Unpauses `dag`, triggers it, and returns the run's ID.
    fn trigger(&self, dag: &str) -> String {
        self.wait(&format!("DAG `{dag}`"), || {
            self.cli_json(&["dags", "list"]).is_some_and(|dags| {
                dags["dags"]
                    .as_array()
                    .is_some_and(|d| d.iter().any(|x| x["dag_id"] == dag))
            })
        });
        let (code, _, err) = self.cli(&["dags", "unpause", dag]);
        assert_eq!(code, 0, "{err}");
        let command = self.cli_json(&["runs", "trigger", dag]).unwrap();
        command["result"]["run_id"].as_str().unwrap().to_owned()
    }

    fn attempts(&self, run_id: &str, task: &str) -> Vec<Value> {
        self.api_get(&format!("/api/v1/runs/{run_id}/tasks/{task}/attempts"))
            .map(|a| a["attempts"].as_array().unwrap().clone())
            .unwrap_or_default()
    }

    fn worker_state(&self, worker: &str) -> Option<String> {
        self.cli_json(&["workers", "list"]).and_then(|list| {
            list["workers"].as_array().unwrap().iter().find_map(|w| {
                (w["worker_id"] == worker).then(|| w["state"].as_str().unwrap().to_owned())
            })
        })
    }
}

fn slow_cluster() -> Cluster {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(cluster.root().join("slow.py"), slow_first_try(&marker)).unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster
}

impl Cluster {
    /// The process ID the first try of `slow` wrote, once it did.
    fn first_try_pid(&self) -> String {
        let marker = self.dir.path().join("marker");
        let mut pid = String::new();
        self.wait("the first try's process ID", || {
            pid = std::fs::read_to_string(&marker).unwrap_or_default();
            !pid.is_empty()
        });
        pid
    }

    /// The worker's journal.
    fn journal(&self) -> Journal {
        Journal::new(self.dir.path().join("data/journal/w1"))
    }
}

#[test]
fn a_restarted_worker_stops_the_task_it_left_running_whose_attempt_is_lost_and_retried() {
    let mut cluster = slow_cluster();
    let run_id = cluster.trigger("slow");
    cluster.wait("the first attempt to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    let pid = cluster.first_try_pid();
    cluster.kill("worker");
    assert!(
        PathBuf::from(format!("/proc/{pid}")).exists(),
        "the task outlives its agent"
    );
    cluster.start("worker");
    cluster.wait("the task the previous agent left to be stopped", || {
        !PathBuf::from(format!("/proc/{pid}")).exists()
    });
    cluster.wait("the retry to succeed", || {
        cluster
            .cli_json(&["runs", "list", "slow"])
            .is_some_and(|runs| runs["runs"][0]["state"] == "success")
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["lost", "success"], "{}", cluster.logs());
    let lost = &cluster.attempts(&run_id, "work")[0];
    assert_eq!(lost["error_summary"]["exception"], "WorkerRestarted");
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("active"));
    assert_eq!(
        cluster.journal().records().unwrap(),
        [],
        "everything acknowledged"
    );
}

#[test]
fn a_result_the_master_didnt_acknowledge_survives_a_worker_restart() {
    let mut cluster = slow_cluster();
    let run_id = cluster.trigger("slow");
    let journal = cluster.journal();
    let mut recorded = Vec::new();
    cluster.wait("the first try's process in the journal", || {
        recorded = journal.records().unwrap_or_default();
        matches!(recorded.as_slice(), [(_, JournalRecord::Running(Some(_)))])
    });
    let [(attempt_id, JournalRecord::Running(Some(process)))] = recorded.as_slice() else {
        unreachable!()
    };
    assert_eq!(process.pgid.to_string(), cluster.first_try_pid());
    // The agent dies just after the task ended and its result was journalled, before the
    // master acknowledged it.
    cluster.kill("worker");
    let group = rustix::process::Pid::from_raw(process.pgid).unwrap();
    rustix::process::kill_process_group(group, rustix::process::Signal::KILL).unwrap();
    let finished = pb::Finished {
        attempt_id: attempt_id.clone(),
        status: pb::AttemptStatus::Success.into(),
        output_json: br#""done""#.to_vec(),
        ..pb::Finished::default()
    };
    journal
        .write(attempt_id, &JournalRecord::Finished(finished))
        .unwrap();
    cluster.start("worker");
    cluster.wait("the run to succeed", || {
        cluster
            .cli_json(&["runs", "list", "slow"])
            .is_some_and(|runs| runs["runs"][0]["state"] == "success")
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["success"], "not lost: {}", cluster.logs());
    cluster.wait("the result to be acknowledged", || {
        journal.records().is_ok_and(|records| records.is_empty())
    });
}

#[test]
fn a_worker_that_never_comes_back_is_lost_and_stays_listed() {
    let mut cluster = slow_cluster();
    let run_id = cluster.trigger("slow");
    cluster.wait("the first attempt to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    cluster.kill("worker");
    cluster.wait("the worker to be lost", || {
        cluster.worker_state("w1").as_deref() == Some("lost")
    });
    cluster.wait("its attempt to end lost", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| a["status"] == "lost")
    });
    let (code, out, _) = cluster.cli(&["workers", "list"]);
    assert_eq!(code, 0);
    assert!(out.contains("w1") && out.contains("lost"), "{out}");
}

#[test]
fn a_running_task_survives_a_master_restart() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(
        cluster.root().join("pause.py"),
        "import time\nfrom schedr import Dag, task\n\n@task\ndef pause() -> int:\n    time.sleep(4)\n    return 1\n\nwith Dag(\"pause\"):\n    pause()\n",
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    let run_id = cluster.trigger("pause");
    cluster.wait("the attempt to start", || {
        cluster
            .attempts(&run_id, "pause")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    cluster.kill("master");
    cluster.start("master");
    cluster.wait("the run to succeed after the restart", || {
        cluster
            .cli_json(&["runs", "list", "pause"])
            .is_some_and(|runs| runs["runs"][0]["state"] == "success")
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "pause")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(
        statuses,
        ["success"],
        "one attempt, continued: {}",
        cluster.logs()
    );
}

#[test]
fn a_second_worker_with_the_same_id_is_refused() {
    let mut cluster = Cluster::new(None, "100ms");
    cluster.up();
    let last_log = |cluster: &Cluster| {
        std::fs::read_to_string(&cluster.roles.last().unwrap().log).unwrap_or_default()
    };
    cluster.start("worker");
    cluster.wait("the second worker to be refused its journal", || {
        last_log(&cluster).contains("another schedr worker `w1` is using")
    });
    let elsewhere = cluster.dir.path().join("elsewhere");
    cluster.start_with(
        "worker",
        &[(
            "SCHEDR_CONFIG__WORKER__DATA_DIR",
            elsewhere.to_str().unwrap(),
        )],
    );
    cluster.wait("the third worker to be refused by the master", || {
        last_log(&cluster).contains("already has a live stream")
    });
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("active"));
}

/// `small` (the default queue) → `big` (queue `large`) → `rare` (queue `gpu`), and `pause`,
/// which sleeps, → `after`.
const QUEUES: &str = r#"
import time
from schedr import Dag, task

@task
def small() -> int:
    return 1

@task(queue="large")
def big(n: int) -> int:
    return n + 1

@task(queue="gpu")
def rare(n: int) -> int:
    return n + 1

with Dag("queues"):
    rare(big(small()))

@task
def pause() -> int:
    time.sleep(60)
    return 1

@task
def after(n: int) -> int:
    return n

with Dag("slow"):
    after(pause())
"#;

impl Cluster {
    fn task(&self, run_id: &str, task: &str) -> Value {
        let run = self.api_get(&format!("/api/v1/runs/{run_id}")).unwrap();
        run["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["task_id"] == task)
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn run_state(&self, run_id: &str) -> String {
        self.api_get(&format!("/api/v1/runs/{run_id}"))
            .map(|r| r["run"]["state"].as_str().unwrap_or_default().to_owned())
            .unwrap_or_default()
    }

    /// A worker serving `queues`, as `name` with worker ID `id`.
    fn start_worker(&mut self, name: &'static str, id: &str, queues: &str) {
        self.start_as(
            name,
            "worker",
            &[
                ("SCHEDR_CONFIG__WORKER__ID", id),
                ("SCHEDR_CONFIG__WORKER__QUEUES", queues),
            ],
        );
    }
}

#[test]
fn tasks_go_to_the_workers_serving_their_queue_and_wait_for_one() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(cluster.root().join("queues.py"), QUEUES).unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster.start_worker("worker-large", "w2", r#"["large"]"#);
    cluster.wait("both workers", || {
        cluster.worker_state("w2").as_deref() == Some("active")
    });
    let run_id = cluster.trigger("queues");
    cluster.wait("`big` to succeed and `rare` to wait", || {
        cluster.task(&run_id, "big")["state"] == "success"
            && cluster.task(&run_id, "rare")["reason"] == "no active worker serves queue `gpu`"
    });
    let worker_of = |task: &str| cluster.attempts(&run_id, task)[0]["worker_id"].clone();
    assert_eq!(worker_of("small"), "w1");
    assert_eq!(worker_of("big"), "w2");
    assert_eq!(cluster.task(&run_id, "rare")["state"], "queued");

    cluster.start_worker("worker-gpu", "w3", r#"["gpu"]"#);
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    assert_eq!(
        cluster.attempts(&run_id, "rare")[0]["worker_id"],
        "w3",
        "{}",
        cluster.logs()
    );
}

#[test]
fn a_running_task_is_cancelled_with_what_follows_it() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(cluster.root().join("queues.py"), QUEUES).unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    let run_id = cluster.trigger("slow");
    cluster.wait("`pause` to start", || {
        cluster
            .attempts(&run_id, "pause")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    let (code, out, err) = cluster.cli(&["tasks", "retry", &run_id, "pause"]);
    assert_ne!(code, 0);
    assert!(err.contains("is running, not unschedulable"), "{err}");
    let (code, out2, err) = cluster.cli(&["tasks", "cancel", &run_id, "pause"]);
    assert_eq!(code, 0, "{out}{err}");
    assert_eq!(
        out2,
        format!("cancelling task pause of run {run_id} (being stopped)\n")
    );
    cluster.wait("the run to end cancelled", || {
        cluster.run_state(&run_id) == "cancelled"
    });
    assert_eq!(cluster.task(&run_id, "pause")["state"], "cancelled");
    assert_eq!(cluster.task(&run_id, "after")["state"], "cancelled");
    assert_eq!(cluster.attempts(&run_id, "pause")[0]["status"], "cancelled");
    let (code, _, err) = cluster.cli(&["tasks", "cancel", &run_id, "after"]);
    assert_ne!(code, 0);
    assert!(err.contains("already finished (cancelled)"), "{err}");
}

/// DAG `moving` in `sync.py`, and in its newer code moved to `moved.py`.
const MOVING_OLD: &str = "from schedr import Dag, task\n\n@task\ndef step() -> int:\n    return 1\n\nwith Dag(\"moving\"):\n    step()\n";
const MOVING_NEW: &str = "from schedr import Dag, task\n\n@task\ndef step() -> int:\n    return 2\n\nwith Dag(\"moving\"):\n    step()\n";

impl Cluster {
    fn dag_view(&self, dag: &str) -> Value {
        self.cli_json(&["dags", "list"])
            .and_then(|dags| {
                dags["dags"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|d| d["dag_id"] == dag)
                    .cloned()
            })
            .unwrap_or(Value::Null)
    }
}

#[test]
fn the_newest_code_runs_and_a_worker_on_older_code_blocks_nothing() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    std::fs::write(cluster.root().join("sync.py"), MOVING_OLD).unwrap();
    let newer = cluster.dir.path().join("dags-newer/etl");
    std::fs::create_dir_all(&newer).unwrap();
    std::fs::write(newer.join("moved.py"), MOVING_NEW).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster.wait("DAG `moving` in sync.py", || {
        cluster.dag_view("moving")["location"]["path"] == "sync.py"
    });

    // w2 has newer code, where the DAG moved to another file: its code is used, no conflict.
    let newer_path = newer.to_str().unwrap().to_owned();
    cluster.start_as(
        "worker-newer",
        "worker",
        &[
            ("SCHEDR_CONFIG__WORKER__ID", "w2"),
            ("SCHEDR_DAG_PATH", &newer_path),
        ],
    );
    cluster.wait("DAG `moving` in moved.py", || {
        cluster.dag_view("moving")["location"]["path"] == "moved.py"
    });
    assert!(
        cluster.dag_view("moving")["conflict"].is_null(),
        "{}",
        cluster.logs()
    );
    let master_log = std::fs::read_to_string(&cluster.roles[0].log).unwrap();
    assert!(
        master_log.contains("workers report 2 code versions of the root"),
        "{master_log}"
    );

    // Only w2 has the newest code: the task runs there.
    let run_id = cluster.trigger("moving");
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    assert_eq!(cluster.attempts(&run_id, "step")[0]["worker_id"], "w2");
    let run = cluster.api_get(&format!("/api/v1/runs/{run_id}")).unwrap();
    assert_eq!(run["code_hashes"].as_array().unwrap().len(), 1, "{run}");
}

/// A task that runs long on its first try (leaving a marker) and returns at once after, with
/// no retries: only a run again without using a retry lets the run succeed.
fn interrupted_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, time
from schedr import Dag, task

@task
def work() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        open(marker, "w").close()
        time.sleep(60)
    return "done"

with Dag("drained"):
    work()
"#,
        marker = marker.display().to_string()
    )
}

impl Cluster {
    /// Sends SIGTERM to a role and waits until it exits; its exit status.
    fn terminate(&mut self, name: &str) -> std::process::ExitStatus {
        let role = self.roles.iter_mut().find(|r| r.name == name).unwrap();
        let pid = role.child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = role.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "{name} didn't stop on SIGTERM");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[test]
fn a_worker_stopped_with_sigterm_drains_and_its_interrupted_task_runs_again_without_a_retry() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(
        cluster.root().join("drained.py"),
        interrupted_first_try(&marker),
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    let grace = [("SCHEDR_CONFIG__WORKER__TERMINATION_GRACE_PERIOD", "2s")];
    cluster.start_as(
        "worker-2",
        "worker",
        &[("SCHEDR_CONFIG__WORKER__ID", "w2"), grace[0]],
    );
    cluster.wait("both workers", || {
        cluster.worker_state("w2").as_deref() == Some("active")
    });
    let run_id = cluster.trigger("drained");
    cluster.wait("the first try to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    let first = cluster.attempts(&run_id, "work")[0]["worker_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // The worker running it is stopped; w1 has the default 30 s grace, w2 2 s.
    let (role, other) = if first == "w2" {
        ("worker-2", "w1")
    } else {
        ("worker", "w2")
    };
    let status = cluster.terminate(role);
    assert!(status.success(), "{status}\n{}", cluster.logs());
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    let attempts = cluster.attempts(&run_id, "work");
    let summary: Vec<(Value, Value)> = attempts
        .iter()
        .map(|a| (a["status"].clone(), a["worker_id"].clone()))
        .collect();
    assert_eq!(
        summary,
        [
            (Value::from("interrupted"), Value::from(first.as_str())),
            (Value::from("success"), Value::from(other)),
        ],
        "{}",
        cluster.logs()
    );
    assert_eq!(cluster.worker_state(&first).as_deref(), Some("offline"));
}

#[test]
fn a_drained_worker_gets_no_tasks_until_resumed_even_across_a_master_restart() {
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    std::fs::write(cluster.root().join("pause.py"), "from schedr import Dag, task\n\n@task\ndef pause() -> int:\n    return 1\n\nwith Dag(\"pause\"):\n    pause()\n").unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    for role in ["master", "webserver", "worker"] {
        cluster.start(role);
    }
    cluster.start_as("worker-2", "worker", &[("SCHEDR_CONFIG__WORKER__ID", "w2")]);
    cluster.wait("both workers", || {
        cluster.worker_state("w2").as_deref() == Some("active")
    });
    let (code, out, err) = cluster.cli(&["workers", "drain", "w1"]);
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("draining w1: no new tasks;"), "{out}");
    let (code, _, err) = cluster.cli(&["workers", "drain", "w9"]);
    assert_ne!(code, 0);
    assert!(err.contains("no worker `w9`"), "{err}");
    cluster.kill("master");
    cluster.start("master");
    cluster.wait("w1 to be back, drained", || {
        cluster.cli_json(&["workers", "list"]).is_some_and(|list| {
            list["workers"][0]["state"] == "active" && list["workers"][0]["drained"] == true
        })
    });
    for _ in 0..3 {
        let run_id = cluster.trigger("pause");
        cluster.wait("the run to succeed", || {
            cluster.run_state(&run_id) == "success"
        });
        assert_eq!(cluster.attempts(&run_id, "pause")[0]["worker_id"], "w2");
    }
    let (code, out, err) = cluster.cli(&["workers", "resume", "w1"]);
    assert_eq!((code, out.as_str()), (0, "resumed w1\n"), "{err}");
    let (code, _, err) = cluster.cli(&["workers", "resume", "w1"]);
    assert_ne!(code, 0);
    assert!(err.contains("worker `w1` isn't drained"), "{err}");
}
