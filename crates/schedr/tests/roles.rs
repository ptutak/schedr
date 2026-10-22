//! The roles as separate processes (PLAN.md 2.1): `schedr master`, `schedr webserver` and
//! `schedr worker`, each its own process, on Postgres and on one SQLite file, driven through
//! the CLI like any deployment.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed) and
//! `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` sets both.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
    api: String,
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
        let (grpc, http) = (free_port(), free_port());
        let config = format!(
            "[dags]\npoll_interval = \"200ms\"\nsettle_time = \"0s\"\n\n\
             [interpreters]\npython = \"{python}\"\n\n\
             [worker]\nid = \"w1\"\ndata_dir = \"{data}\"\nslots = 4\n\
             master_url = \"http://127.0.0.1:{grpc}\"\n\n\
             [master]\nbind = \"127.0.0.1\"\ngrpc_port = {grpc}\ntick_interval = \"{tick}\"\n\n\
             [webserver]\nbind = \"127.0.0.1\"\nport = {http}\n",
            python = python(),
            data = dir.path().join("data").display(),
        );
        std::fs::write(dir.path().join("config.toml"), config).unwrap();
        Self {
            dir,
            database_url,
            api: format!("http://127.0.0.1:{http}"),
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
            .env("SCHEDR_DAG_PATH", self.root())
            .arg("--config")
            .arg(self.dir.path().join("config.toml"))
            .args(args);
        command
    }

    fn start(&mut self, name: &'static str) {
        let log = self.dir.path().join(format!("{name}.log"));
        let file = std::fs::File::create(&log).unwrap();
        let child = self
            .command(&[name])
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        self.roles.push(Role { name, child, log });
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
