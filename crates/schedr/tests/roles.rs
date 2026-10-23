//! The roles as separate processes (PLAN.md 2.1): `schedr master`, `schedr webserver` and
//! `schedr worker`, each its own process, on Postgres and on one SQLite file, driven through
//! the CLI like any deployment.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed) and
//! `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` sets both.

use std::cell::RefCell;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use schedr_agent::journal::{Journal, JournalRecord};
use schedr_proto::pb;
use schedr_tls::testing::TestCa;
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
    /// The CA of `[tls] mode = "app"`; `mesh` mode without one.
    ca: Option<TestCa>,
    /// Each role's ops address, by name, once it listens.
    ops: Vec<(&'static str, String)>,
    /// CLI calls that failed or were slow, for failure messages (B4, B6).
    cli_trouble: RefCell<Vec<String>>,
    /// The system's I/O stall counters when the cluster was made (B6).
    io_at_start: Option<IoStall>,
}

/// A CLI call slower than this is listed in failure messages.
const SLOW_CLI: Duration = Duration::from_secs(2);

/// How many failed or slow CLI calls failure messages show (the latest).
const CLI_TROUBLE_KEPT: usize = 20;

/// Time the system's tasks spent stalled on I/O (`/proc/pressure/io`, Linux): `some` (at least
/// one task stalled) and `full` (all non-idle tasks stalled), in microseconds since boot.
#[derive(Debug, Clone, Copy)]
struct IoStall {
    some: u64,
    full: u64,
}

impl IoStall {
    fn now() -> Option<Self> {
        let text = std::fs::read_to_string("/proc/pressure/io").ok()?;
        let total = |kind: &str| -> Option<u64> {
            text.lines()
                .find(|l| l.starts_with(kind))?
                .split_whitespace()
                .find_map(|f| f.strip_prefix("total="))?
                .parse()
                .ok()
        };
        Some(Self {
            some: total("some")?,
            full: total("full")?,
        })
    }

    /// What has stalled since `self`, for a failure message.
    fn since(self) -> String {
        Self::now().map_or_else(
            || "I/O stall: unknown (no /proc/pressure/io)".to_owned(),
            |now| {
                format!(
                    "I/O stall since the cluster started (whole system): some {} ms, full {} ms",
                    now.some.saturating_sub(self.some) / 1000,
                    now.full.saturating_sub(self.full) / 1000
                )
            },
        )
    }
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
             [worker]\nid = \"w1\"\ndata_dir = \"{data}\"\nslots = 4\n\
             log_bind = \"127.0.0.1\"\nlog_port = 0\nlog_address = \"127.0.0.1\"\n\n\
             [master]\nbind = \"127.0.0.1\"\ngrpc_port = 0\ntick_interval = \"{tick}\"\n\
             heartbeat_interval = \"1s\"\nlease_timeout = \"4s\"\n\n\
             [webserver]\nbind = \"127.0.0.1\"\nport = 0\n\n\
             [ops]\nbind = \"127.0.0.1\"\nport = 0\n\n\
             [tls]\nmode = \"mesh\"\n",
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
            ca: None,
            ops: Vec::new(),
            cli_trouble: RefCell::default(),
            io_at_start: IoStall::now(),
        }
    }

    /// The cluster in `[tls] mode = "app"`: each role started with [`Self::start_tls`] gets a
    /// certificate from this CA, read again every 200 ms.
    fn with_tls(database_url: Option<String>) -> Self {
        let mut cluster = Self::new(database_url, "100ms");
        std::fs::create_dir_all(cluster.dir.path().join("tls")).unwrap();
        cluster.ca = Some(TestCa::new("schedr test CA"));
        cluster
    }

    /// Starts `role` as `name` with a certificate for `names` from the cluster's CA (or
    /// `ca`), and more environment variables.
    fn start_tls(
        &mut self,
        name: &'static str,
        role: &str,
        names: &[&str],
        ca: Option<&TestCa>,
        env: &[(&str, &str)],
    ) -> schedr_tls::TlsFiles {
        let ca = ca.or(self.ca.as_ref()).unwrap();
        let files = ca.write(&self.dir.path().join("tls"), name, names);
        let (ca_file, cert, key) = (
            files.ca.display().to_string(),
            files.cert.display().to_string(),
            files.key.display().to_string(),
        );
        let mut all = vec![
            ("SCHEDR_CONFIG__TLS__CA", ca_file.as_str()),
            ("SCHEDR_CONFIG__TLS__CERT", cert.as_str()),
            ("SCHEDR_CONFIG__TLS__KEY", key.as_str()),
        ];
        all.extend_from_slice(env);
        self.start_as(name, role, &all);
        files
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
        if self.ca.is_some() {
            command
                .env("SCHEDR_CONFIG__TLS__MODE", "app")
                .env("SCHEDR_CONFIG__TLS__RELOAD_INTERVAL", "200ms");
        }
        if let Some(port) = self.grpc_port {
            let scheme = if self.ca.is_some() { "https" } else { "http" };
            command
                .env("SCHEDR_CONFIG__MASTER__GRPC_PORT", port.to_string())
                .env(
                    "SCHEDR_CONFIG__WORKER__MASTER_URL",
                    format!("{scheme}://127.0.0.1:{port}"),
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
        let ops = bound("ops on http://");
        let (grpc_port, api) = match name {
            "master" => {
                let address = bound("schedr master: gRPC on ");
                (
                    Some(address.rsplit(':').next().unwrap().parse().unwrap()),
                    None,
                )
            }
            "webserver" => (None, Some(format!("http://{}", bound("API on http://")))),
            _ => (None, None),
        };
        self.grpc_port = grpc_port.or(self.grpc_port);
        if let Some(api) = api {
            self.api = api;
        }
        self.ops.retain(|(role, _)| *role != name);
        self.ops.push((name, ops));
    }

    fn logs(&self) -> String {
        use std::fmt::Write as _;
        let mut text = String::new();
        for role in &self.roles {
            let log = std::fs::read_to_string(&role.log).unwrap_or_default();
            let _ = write!(text, "--- {} ---\n{log}", role.name);
        }
        let trouble = self.cli_trouble.borrow();
        if !trouble.is_empty() {
            let _ = write!(
                text,
                "--- CLI calls that failed or were slow ---\n{}\n",
                trouble.join("\n")
            );
        }
        if let Some(start) = self.io_at_start {
            let _ = writeln!(text, "--- {} ---", start.since());
        }
        text
    }

    /// The CLI against the webserver: exit code, stdout, stderr.
    fn cli(&self, args: &[&str]) -> (i32, String, String) {
        let (group, rest) = args.split_first().unwrap();
        let started = Instant::now();
        let output = Command::new(BINARY)
            .arg(group)
            .args(["--server", &self.api])
            .args(rest)
            .env("TZ", "UTC")
            .output()
            .unwrap();
        let took = started.elapsed();
        let code = output.status.code().unwrap_or(-1);
        let err = String::from_utf8_lossy(&output.stderr).into_owned();
        if code != 0 || took > SLOW_CLI {
            let mut trouble = self.cli_trouble.borrow_mut();
            // Waits poll the CLI while a role starts: the latest calls are the telling ones.
            if trouble.len() == CLI_TROUBLE_KEPT {
                trouble.remove(0);
            }
            trouble.push(format!(
                "schedr {} -> exit {code} after {} ms: {}",
                args.join(" "),
                took.as_millis(),
                err.trim()
            ));
        }
        (
            code,
            String::from_utf8_lossy(&output.stdout).into_owned(),
            err,
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
        let logs = std::fs::read_dir(self.dir.path().join("data/logs/w1"))
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
    // The log comes from the worker, through the webserver.
    let run_id = cluster
        .cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
        .unwrap()["runs"][0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (code, out, err) = cluster.cli(&["tasks", "logs", &run_id, "report"]);
    assert_eq!(code, 0, "{err}\n{}", cluster.logs());
    assert_eq!(out, "report loaded 3\n");
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
        let command = self
            .cli_json(&["runs", "trigger", dag])
            .unwrap_or_else(|| panic!("`runs trigger {dag}` failed\n{}", self.logs()));
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
    // A 2 s grace period of which 1 s to SIGKILL and 1 s to report: interrupted at once.
    cluster.start_as(
        "worker-2",
        "worker",
        &[
            ("SCHEDR_CONFIG__WORKER__ID", "w2"),
            ("SCHEDR_CONFIG__WORKER__TERMINATION_GRACE_PERIOD", "2s"),
            ("SCHEDR_CONFIG__WORKER__DRAIN_TERMINATE_PERIOD", "1s"),
            ("SCHEDR_CONFIG__WORKER__DRAIN_REPORT_PERIOD", "1s"),
        ],
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

impl Cluster {
    /// The run of `chain` for 2026-10-15.
    fn chain_run(&self) -> String {
        self.cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
            .unwrap()["runs"][0]["run_id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    fn last_log(&self) -> String {
        std::fs::read_to_string(&self.roles.last().unwrap().log).unwrap_or_default()
    }

    fn log_of(&self, name: &str) -> String {
        let role = self.roles.iter().find(|r| r.name == name).unwrap();
        std::fs::read_to_string(&role.log).unwrap_or_default()
    }
}

impl Cluster {
    /// Over mTLS: master, webserver and workers `w1` and `w2` (its data in `w2/`), with
    /// `chain` run for 2026-10-15: its run ID.
    fn mtls_chain_on_two_workers(&mut self) -> String {
        let (code, err) = self.schedr(&["db", "migrate"]);
        assert_eq!(code, 0, "{err}");
        self.start_tls("master", "master", &["127.0.0.1"], None, &[]);
        self.start_tls("webserver", "webserver", &["schedr-webserver"], None, &[]);
        self.start_tls("worker", "worker", &["schedr-worker"], None, &[]);
        let elsewhere = self.dir.path().join("w2");
        let elsewhere = elsewhere.to_str().unwrap();
        self.start_tls(
            "worker2",
            "worker",
            &["schedr-worker"],
            None,
            &[
                ("SCHEDR_CONFIG__WORKER__ID", "w2"),
                ("SCHEDR_CONFIG__WORKER__DATA_DIR", elsewhere),
            ],
        );
        assert!(self.log_of("master").contains("(mTLS)"), "{}", self.logs());
        self.wait("both workers", || {
            self.worker_state("w1").as_deref() == Some("active")
                && self.worker_state("w2").as_deref() == Some("active")
        });
        let workers = self.cli_json(&["workers", "list"]).unwrap();
        for worker in workers["workers"].as_array().unwrap() {
            let endpoint = worker["log_endpoint"].as_str().unwrap();
            assert!(endpoint.starts_with("https://127.0.0.1:"), "{endpoint}");
        }
        self.wait("DAG `chain`", || {
            self.cli_json(&["dags", "list"])
                .is_some_and(|d| d["dags"][0]["dag_id"] == "chain")
        });
        let (code, _, err) = self.cli(&["dags", "unpause", "chain"]);
        assert_eq!(code, 0, "{err}");
        let (code, _, err) = self.cli(&["runs", "trigger", "chain", "--for", "2026-10-15"]);
        assert_eq!(code, 0, "{err}");
        self.wait("the run to succeed", || {
            self.cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
                .is_some_and(|runs| runs["runs"][0]["state"] == "success")
        });
        self.chain_run()
    }
}

#[test]
fn over_mtls_two_workers_run_a_dag_and_their_logs_come_through_the_webserver() {
    let mut cluster = Cluster::with_tls(Some(postgres_database()));
    let run_id = cluster.mtls_chain_on_two_workers();
    let elsewhere = cluster.dir.path().join("w2");

    // Every task's log, from whichever worker ran it, is the file on that worker.
    for task in ["extract", "load", "report"] {
        let attempt = cluster.attempts(&run_id, task)[0].clone();
        let worker = attempt["worker_id"].as_str().unwrap();
        let data = if worker == "w1" {
            cluster.dir.path().join("data")
        } else {
            elsewhere.clone()
        };
        let file = data.join(format!(
            "logs/{worker}/{}.log",
            attempt["attempt_id"].as_str().unwrap()
        ));
        let (code, out, err) = cluster.cli(&["tasks", "logs", &run_id, task]);
        assert_eq!(code, 0, "{err}\n{}", cluster.logs());
        assert_eq!(
            out,
            std::fs::read_to_string(&file).unwrap(),
            "{task} on {worker}"
        );
    }

    // A deleted log, then a worker gone, say so.
    let attempt = cluster.attempts(&run_id, "report")[0].clone();
    let worker = attempt["worker_id"].as_str().unwrap().to_owned();
    let data = if worker == "w1" {
        cluster.dir.path().join("data")
    } else {
        elsewhere
    };
    std::fs::remove_file(data.join(format!(
        "logs/{worker}/{}.log",
        attempt["attempt_id"].as_str().unwrap()
    )))
    .unwrap();
    let (code, _, err) = cluster.cli(&["tasks", "logs", &run_id, "report"]);
    assert_eq!(code, 1);
    assert!(
        err.contains(&format!(
            "the log file is unavailable at worker `{worker}` (it may have been deleted)"
        )),
        "{err}"
    );
    let name = if worker == "w1" { "worker" } else { "worker2" };
    let role = cluster.roles.iter().find(|r| r.name == name).unwrap();
    assert!(
        Command::new("kill")
            .args(["-TERM", &role.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    cluster.wait("the worker to go offline", || {
        cluster.worker_state(&worker).as_deref() == Some("offline")
    });
    let (code, _, err) = cluster.cli(&["tasks", "logs", &run_id, "report"]);
    assert_eq!(code, 1);
    assert!(
        err.contains(&format!(
            "worker `{worker}` is offline: the log of try 1 is unavailable"
        )),
        "{err}"
    );
}

#[test]
fn a_worker_with_a_certificate_from_another_ca_is_refused() {
    let mut cluster = Cluster::with_tls(Some(postgres_database()));
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start_tls("master", "master", &["127.0.0.1"], None, &[]);
    cluster.start_tls("webserver", "webserver", &["schedr-webserver"], None, &[]);
    // Its certificate comes from another CA; it trusts the cluster's.
    let files = TestCa::new("someone else").write(
        &cluster.dir.path().join("tls"),
        "foreign",
        &["schedr-worker"],
    );
    std::fs::write(&files.ca, cluster.ca.as_ref().unwrap().pem()).unwrap();
    let (ca, cert, key) = (
        files.ca.display().to_string(),
        files.cert.display().to_string(),
        files.key.display().to_string(),
    );
    cluster.start_as(
        "worker",
        "worker",
        &[
            ("SCHEDR_CONFIG__TLS__CA", &ca),
            ("SCHEDR_CONFIG__TLS__CERT", &cert),
            ("SCHEDR_CONFIG__TLS__KEY", &key),
        ],
    );
    cluster.wait("the master to refuse the worker's certificate", || {
        let master = cluster.log_of("master");
        master.contains("TLS handshake refused") && master.contains("UnknownIssuer")
    });
    cluster.wait("the worker to report why", || {
        cluster.last_log().contains("master connection lost")
    });
    assert_eq!(cluster.worker_state("w1"), None);
}

#[test]
fn the_master_takes_rotated_certificates_without_a_restart() {
    let mut cluster = Cluster::with_tls(Some(postgres_database()));
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    // The master starts on the old CA's certificate and trusts both CAs' workers.
    let old = TestCa::new("old CA");
    let master = cluster.start_tls("master", "master", &["127.0.0.1"], Some(&old), &[]);
    let new = cluster.ca.take().unwrap();
    std::fs::write(&master.ca, format!("{}{}", old.pem(), new.pem())).unwrap();
    cluster.ca = Some(new);
    cluster.start_tls("webserver", "webserver", &["schedr-webserver"], None, &[]);
    // The worker knows only the new CA, so it can't trust the master yet.
    cluster.start_tls("worker", "worker", &["schedr-worker"], None, &[]);
    cluster.wait("the worker to fail to connect", || {
        cluster.last_log().contains("master connection lost")
    });
    assert_eq!(cluster.worker_state("w1"), None);

    let issued = cluster.ca.as_ref().unwrap().issue(&["127.0.0.1"]);
    std::fs::write(&master.key, issued.key).unwrap();
    std::fs::write(&master.cert, issued.cert).unwrap();
    cluster.wait("the worker to register", || {
        cluster.worker_state("w1").as_deref() == Some("active")
    });
    assert!(
        cluster
            .log_of("master")
            .contains("TLS certificates changed"),
        "{}",
        cluster.logs()
    );
}

/// A task that allocates (and touches) 400 MiB on its first try, then finishes small.
fn greedy_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, time
from datetime import timedelta
from schedr import Dag, task

@task(retries=1, retry_delay=timedelta(0))
def greedy() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        open(marker, "w").close()
        chunks = []
        for _ in range(40):
            chunks.append(b"x" * (10 << 20))
            time.sleep(0.05)
        time.sleep(30)
    return "small"

with Dag("greedy"):
    greedy()
"#,
        marker = marker.display().to_string()
    )
}

#[test]
fn a_task_over_its_memory_budget_is_killed_alone_and_retried() {
    let mut cluster = Cluster::new(None, "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(cluster.root().join("greedy.py"), greedy_first_try(&marker)).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("master");
    cluster.start("webserver");
    cluster.start_with(
        "worker",
        &[
            ("SCHEDR_CONFIG__WORKER__MEMORY_BUDGET", "100MiB"),
            ("SCHEDR_CONFIG__WORKER__MEMORY_CHECK_INTERVAL", "100ms"),
        ],
    );
    cluster.wait("the worker to say its budget", || {
        cluster
            .log_of("worker")
            .contains("memory budget 100 MiB per task")
    });
    let run_id = cluster.trigger("greedy");
    cluster.wait("the run to succeed", || {
        cluster.run_state(&run_id) == "success"
    });
    let attempts = cluster.attempts(&run_id, "greedy");
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(attempts[0]["status"], "oom_killed");
    let message = attempts[0]["error_summary"]["message"].as_str().unwrap();
    assert!(
        message.starts_with("exceeded memory budget 100 MiB ("),
        "{message}"
    );
    assert!(attempts[0]["peak_memory_bytes"].as_u64().unwrap() > 100 << 20);
    assert_eq!(attempts[1]["status"], "success");
    // Measured, well under the budget.
    let peak = attempts[1]["peak_memory_bytes"].as_u64().unwrap();
    assert!(peak > 0 && peak < 100 << 20, "{peak}");
    // The worker itself is still there: only the task was killed.
    assert_eq!(cluster.worker_state("w1").as_deref(), Some("active"));
}

impl Cluster {
    /// `GET <path>` on the ops port of the role started as `name`: status and body.
    fn ops_get(&self, name: &str, path: &str) -> (u16, String) {
        let address = &self.ops.iter().find(|(role, _)| *role == name).unwrap().1;
        let output = Command::new("curl")
            .args([
                "-s",
                "-w",
                "\n%{http_code}",
                &format!("http://{address}{path}"),
            ])
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        let (body, status) = text.rsplit_once('\n').unwrap_or(("", "0"));
        (status.parse().unwrap_or(0), body.to_owned())
    }
}

#[test]
fn every_roles_ops_port_answers_and_counts_a_run() {
    let mut cluster = Cluster::new(None, "100ms");
    cluster.up();
    cluster.run_chain();
    for role in ["master", "webserver", "worker"] {
        assert_eq!(
            cluster.ops_get(role, "/healthz"),
            (200, "ok\n".to_owned()),
            "{role}"
        );
        cluster.wait(&format!("the {role} to be ready"), || {
            cluster.ops_get(role, "/readyz") == (200, "ok\n".to_owned())
        });
        let (status, metrics) = cluster.ops_get(role, "/metrics");
        assert_eq!(status, 200, "{role}");
        assert!(
            metrics.contains("schedr_build_info{version="),
            "{role}\n{metrics}"
        );
        assert!(
            metrics.contains("process_resident_memory_bytes "),
            "{role}\n{metrics}"
        );
    }
    let (_, master) = cluster.ops_get("master", "/metrics");
    for line in [
        "schedr_runs_finished_total{dag_id=\"chain\",result=\"success\"} 1",
        "schedr_attempts_ended_total{status=\"success\"} 3",
        "schedr_assignments_total 3",
        "schedr_commands_total{kind=\"trigger_run\",decision=\"applied\"} 1",
    ] {
        assert!(master.contains(line), "{line}\n{master}");
    }
    cluster.wait("the gauges to be refreshed", || {
        cluster
            .ops_get("master", "/metrics")
            .1
            .contains("schedr_workers{state=\"active\"} 1")
    });
    let (_, worker) = cluster.ops_get("worker", "/metrics");
    for line in [
        "schedr_worker_attempts_total{status=\"success\"} 3",
        "schedr_worker_slots 4",
    ] {
        assert!(worker.contains(line), "{line}\n{worker}");
    }
    let (_, webserver) = cluster.ops_get("webserver", "/metrics");
    assert!(
        webserver.contains("schedr_http_requests_total{route=\"/api/v1/dags\",status=\"200\"}"),
        "{webserver}"
    );
}

#[test]
fn a_master_waiting_for_its_database_is_alive_but_not_ready() {
    // A "database" that accepts connections and never answers.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = silent.local_addr().unwrap().port();
    let mut cluster = Cluster::new(
        Some(format!("postgres://schedr@127.0.0.1:{port}/schedr")),
        "100ms",
    );
    // Started under another name: only its ops address is waited for (its gRPC address
    // isn't logged before the database answers).
    cluster.start_as(
        "waiting-master",
        "master",
        &[("SCHEDR_CONFIG__DATABASE__QUERY_TIMEOUT", "20s")],
    );
    assert_eq!(
        cluster.ops_get("waiting-master", "/healthz"),
        (200, "ok\n".to_owned())
    );
    assert_eq!(
        cluster.ops_get("waiting-master", "/readyz"),
        (503, "starting: master\n".to_owned())
    );
    drop(silent);
}

/// A DAG whose task ignores SIGTERM on its first try (it stops only for SIGKILL) and succeeds
/// on the next.
fn ignores_sigterm_first_try(marker: &std::path::Path) -> String {
    format!(
        r#"
import os, signal, time
from schedr import Dag, task

@task
def work() -> str:
    marker = {marker:?}
    if not os.path.exists(marker):
        open(marker, "w").close()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(120)
    return "done"

with Dag("stubborn"):
    work()
"#,
        marker = marker.display().to_string()
    )
}

#[test]
fn a_drained_task_that_ignores_sigterm_is_interrupted_within_the_pods_grace_period() {
    // B8: the drain's SIGTERM came at the grace period minus 5 s, but SIGKILL only after the
    // master's cancel grace (30 s), long after the pod is killed; the task then ended `lost`
    // (using a retry) instead of `interrupted`.
    let mut cluster = Cluster::new(Some(postgres_database()), "100ms");
    let marker = cluster.dir.path().join("marker");
    std::fs::write(
        cluster.root().join("stubborn.py"),
        ignores_sigterm_first_try(&marker),
    )
    .unwrap();
    std::fs::remove_file(cluster.root().join("chain.py")).unwrap();
    let (code, err) = cluster.schedr(&["db", "migrate"]);
    assert_eq!(code, 0, "{err}");
    cluster.start("master");
    cluster.start("webserver");
    // The pod's terminationGracePeriodSeconds, matched in the worker's configuration.
    let grace = Duration::from_secs(8);
    let worker_env = [("SCHEDR_CONFIG__WORKER__TERMINATION_GRACE_PERIOD", "8s")];
    cluster.start_with("worker", &worker_env);
    let run_id = cluster.trigger("stubborn");
    cluster.wait("the first try to start", || {
        cluster
            .attempts(&run_id, "work")
            .first()
            .is_some_and(|a| !a["started_at"].is_null())
    });
    // Kubernetes: SIGTERM, then SIGKILL when the grace period is over.
    let exited = cluster.terminate_within("worker", grace);
    // SIGKILL if still running (the kubelet), and gone from the cluster's roles either way.
    cluster.kill("worker");
    // The pod comes back (a new incarnation, the journal on its volume).
    cluster.start_as("worker-again", "worker", &worker_env);
    cluster.wait("the run to end", || {
        ["success", "failed", "cancelled"].contains(&cluster.run_state(&run_id).as_str())
    });
    let statuses: Vec<Value> = cluster
        .attempts(&run_id, "work")
        .iter()
        .map(|a| a["status"].clone())
        .collect();
    assert_eq!(statuses, ["interrupted", "success"], "{}", cluster.logs());
    assert!(
        exited.is_some_and(|s| s.success()),
        "the worker didn't stop within the grace period: {exited:?}\n{}",
        cluster.logs()
    );
}

impl Cluster {
    /// Sends SIGTERM to a role and waits up to `limit` for it to exit; its exit status, or
    /// `None` if it is still running.
    fn terminate_within(
        &mut self,
        name: &str,
        limit: Duration,
    ) -> Option<std::process::ExitStatus> {
        let role = self.roles.iter_mut().find(|r| r.name == name).unwrap();
        let pid = role.child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if let Some(status) = role.child.try_wait().unwrap() {
                return Some(status);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        None
    }
}
