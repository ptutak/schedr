//! The test cluster shared by the integration tests that run the roles as processes
//! (`roles.rs`, `stress.rs`): the real binary, its configuration, its logs, and the CLI and API
//! against it.
//!
//! Each test binary uses part of it.
#![allow(dead_code)]

use std::cell::RefCell;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use schedr_tls::testing::TestCa;
use serde_json::Value;
use tempfile::TempDir;

pub const BINARY: &str = env!("CARGO_BIN_EXE_schedr");

pub const CHAIN: &str = r#"
from schedr import CommandOperator, Dag, task

@task
def extract() -> dict[str, int]:
    return {"rows": 3}

@task
def load(rows: int) -> str:
    return f"loaded {rows}"

with Dag("chain"):
    done = load(extract()["rows"])
    CommandOperator("report", ["echo", t"report {done}"])
"#;

pub fn python() -> String {
    std::env::var("SCHEDR_TEST_PYTHON").expect(
        "SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)",
    )
}

/// A new database of this test's own on the test Postgres server.
pub fn postgres_database() -> String {
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
pub struct Role {
    pub name: String,
    pub child: Child,
    pub log: PathBuf,
    /// Where this process's own output starts in `log` (after earlier runs', when kept).
    pub log_from: usize,
}

impl Drop for Role {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The three roles over one DAG root, with their own directory and ports.
pub struct Cluster {
    pub dir: TempDir,
    pub database_url: String,
    /// The REST API, once the webserver listens.
    pub api: String,
    /// The master's port once it listens (kept for a restart) and the URL workers use.
    pub grpc_port: Option<u16>,
    pub roles: Vec<Role>,
    /// The CA of `[tls] mode = "app"`; `mesh` mode without one.
    pub ca: Option<TestCa>,
    /// Each role's ops address, by name, once it listens.
    pub ops: Vec<(String, String)>,
    /// CLI calls that failed or were slow, for failure messages (B4, B6).
    pub cli_trouble: RefCell<Vec<String>>,
    /// Whether a role started again appends to its log instead of starting it afresh (the
    /// stress suite keeps every run's output for its failure messages).
    pub keep_logs: bool,
    /// The system's I/O stall counters when the cluster was made (B6).
    pub io_at_start: Option<IoStall>,
}

/// A CLI call slower than this is listed in failure messages.
pub const SLOW_CLI: Duration = Duration::from_secs(2);

/// How many failed or slow CLI calls failure messages show (the latest).
pub const CLI_TROUBLE_KEPT: usize = 20;

/// Time the system's tasks spent stalled on I/O (`/proc/pressure/io`, Linux): `some` (at least
/// one task stalled) and `full` (all non-idle tasks stalled), in microseconds since boot.
#[derive(Debug, Clone, Copy)]
pub struct IoStall {
    pub some: u64,
    pub full: u64,
}

impl IoStall {
    pub fn now() -> Option<Self> {
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
    pub fn since(self) -> String {
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
    pub fn new(database_url: Option<String>, tick: &str) -> Self {
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
             heartbeat_interval = \"1s\"\nlease_timeout = \"4s\"\nlease_period = \"15s\"\n\n\
             [database]\nquery_timeout = \"5s\"\n\n\
             [webserver]\nbind = \"127.0.0.1\"\nport = 0\nui_port = 0\n\n\
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
            keep_logs: false,
            io_at_start: IoStall::now(),
        }
    }

    /// The cluster in `[tls] mode = "app"`: each role started with [`Self::start_tls`] gets a
    /// certificate from this CA, read again every 200 ms.
    pub fn with_tls(database_url: Option<String>) -> Self {
        let mut cluster = Self::new(database_url, "100ms");
        std::fs::create_dir_all(cluster.dir.path().join("tls")).unwrap();
        cluster.ca = Some(TestCa::new("schedr test CA"));
        cluster
    }

    /// Starts `role` as `name` with a certificate for `names` from the cluster's CA (or
    /// `ca`), and more environment variables.
    pub fn start_tls(
        &mut self,
        name: &str,
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

    pub fn root(&self) -> PathBuf {
        self.dir.path().join("dags/etl")
    }

    /// `schedr <args>` with this cluster's configuration and database: exit code and stderr.
    pub fn schedr(&self, args: &[&str]) -> (i32, String) {
        let output = self.command(args).output().unwrap();
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(BINARY);
        command
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.dir.path())
            .env("TZ", "UTC")
            // A panic's stack in the role's log (B15).
            .env("RUST_BACKTRACE", "1")
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
    pub fn kill(&mut self, name: &str) {
        let index = self.roles.iter().position(|r| r.name == name).unwrap();
        drop(self.roles.remove(index));
    }

    pub fn start(&mut self, name: &str) {
        self.start_with(name, &[]);
    }

    /// Starts a role with more environment variables.
    pub fn start_with(&mut self, name: &str, env: &[(&str, &str)]) {
        self.start_as(name, name, env);
    }

    /// Starts `role` under the name `name` (its log is `<name>.log`), with more environment
    /// variables: a second worker, say.
    pub fn start_as(&mut self, name: &str, role: &str, env: &[(&str, &str)]) {
        self.start_args(name, &[role], env);
    }

    /// Starts `schedr <args>` (a role and its options) under the name `name`, with more
    /// environment variables.
    pub fn start_args(&mut self, name: &str, args: &[&str], env: &[(&str, &str)]) {
        self.start_listening(name, args, env, true);
    }

    /// Starts `schedr <args>` as [`Self::start_args`] does, but waits only for its ops port: a
    /// role that doesn't listen yet (a webserver waiting for its schema).
    pub fn start_unready(&mut self, name: &str, args: &[&str]) {
        self.start_listening(name, args, &[], false);
    }

    fn start_listening(&mut self, name: &str, args: &[&str], env: &[(&str, &str)], ports: bool) {
        let role = if ports { args[0] } else { "" };
        let log = self.dir.path().join(format!("{name}.log"));
        let file = if self.keep_logs {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .unwrap()
        } else {
            std::fs::File::create(&log).unwrap()
        };
        let log_from = usize::try_from(file.metadata().unwrap().len()).unwrap();
        let child = self
            .command(args)
            .envs(env.iter().copied())
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        self.roles.push(Role {
            name: name.to_owned(),
            child,
            log: log.clone(),
            log_from,
        });
        let bound = |marker: &str| {
            let mut found = None;
            self.wait(&format!("`{marker}` in the {name} log"), || {
                let text = std::fs::read_to_string(&log).unwrap_or_default();
                found = text[log_from.min(text.len())..]
                    .split(marker)
                    .nth(1)
                    .map(|rest| {
                        rest.split(|c: char| c == '"' || c.is_whitespace())
                            .next()
                            .unwrap()
                            .to_owned()
                    });
                found.is_some()
            });
            found.unwrap()
        };
        // Standalone's banner goes on after it: `ops on http://<address>, data in ...`.
        let ops = bound("ops on http://").trim_end_matches(',').to_owned();
        let (grpc_port, api) = match role {
            // Only the master started as `master`: another one may not listen yet (a standby,
            // a master waiting for its database); see `Self::grpc_port_of`.
            "master" if name == "master" => {
                let address = bound("schedr master: gRPC on ");
                (
                    Some(address.rsplit(':').next().unwrap().parse().unwrap()),
                    None,
                )
            }
            "webserver" | "standalone" => {
                let address = bound("API on http://");
                (
                    None,
                    Some(format!("http://{}", address.trim_end_matches(','))),
                )
            }
            _ => (None, None),
        };
        self.grpc_port = grpc_port.or(self.grpc_port);
        if let Some(api) = api {
            self.api = api;
        }
        self.ops.retain(|(role, _)| role != name);
        self.ops.push((name.to_owned(), ops));
    }

    /// The gRPC port of the master started as `name`, once its log says it listens.
    pub fn grpc_port_of(&self, name: &str) -> u16 {
        let role = self.roles.iter().find(|r| r.name == name).unwrap();
        let (log, from) = (role.log.clone(), role.log_from);
        let mut port = None;
        self.wait(&format!("the gRPC address in the {name} log"), || {
            let text = std::fs::read_to_string(&log).unwrap_or_default();
            port = text[from.min(text.len())..]
                .split("schedr master: gRPC on ")
                .nth(1)
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|address| address.rsplit(':').next()?.parse().ok());
            port.is_some()
        });
        port.unwrap()
    }

    pub fn logs(&self) -> String {
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
    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
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

    pub fn cli_json(&self, args: &[&str]) -> Option<Value> {
        let mut with_json = args.to_vec();
        with_json.insert(1, "--json");
        let (code, out, _) = self.cli(&with_json);
        (code == 0).then(|| serde_json::from_str(&out).unwrap())
    }

    pub fn wait(&self, what: &str, done: impl FnMut() -> bool) {
        self.wait_for(what, Duration::from_secs(60), done);
    }

    /// As [`Self::wait`], up to `limit`.
    pub fn wait_for(&self, what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + limit;
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
    pub fn up(&mut self) {
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
    pub fn run_chain(&self) {
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

impl Cluster {
    pub fn api_get(&self, path: &str) -> Option<Value> {
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
    pub fn trigger(&self, dag: &str) -> String {
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

    pub fn attempts(&self, run_id: &str, task: &str) -> Vec<Value> {
        self.api_get(&format!("/api/v1/runs/{run_id}/tasks/{task}/attempts"))
            .map(|a| a["attempts"].as_array().unwrap().clone())
            .unwrap_or_default()
    }

    pub fn worker_state(&self, worker: &str) -> Option<String> {
        self.cli_json(&["workers", "list"]).and_then(|list| {
            list["workers"].as_array().unwrap().iter().find_map(|w| {
                (w["worker_id"] == worker).then(|| w["state"].as_str().unwrap().to_owned())
            })
        })
    }
}

impl Cluster {
    pub fn task(&self, run_id: &str, task: &str) -> Value {
        let run = self.api_get(&format!("/api/v1/runs/{run_id}")).unwrap();
        run["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["task_id"] == task)
            .cloned()
            .unwrap_or(Value::Null)
    }

    pub fn run_state(&self, run_id: &str) -> String {
        self.api_get(&format!("/api/v1/runs/{run_id}"))
            .map(|r| r["run"]["state"].as_str().unwrap_or_default().to_owned())
            .unwrap_or_default()
    }

    /// A worker serving `queues`, as `name` with worker ID `id`.
    pub fn start_worker(&mut self, name: &str, id: &str, queues: &str) {
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

impl Cluster {
    /// Sends SIGTERM to a role and waits until it exits; its exit status.
    pub fn terminate(&mut self, name: &str) -> std::process::ExitStatus {
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

impl Cluster {
    /// The run of `chain` for 2026-10-15.
    pub fn chain_run(&self) -> String {
        self.cli_json(&["runs", "list", "chain", "--for", "2026-10-15"])
            .unwrap()["runs"][0]["run_id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    pub fn last_log(&self) -> String {
        std::fs::read_to_string(&self.roles.last().unwrap().log).unwrap_or_default()
    }

    pub fn log_of(&self, name: &str) -> String {
        let role = self.roles.iter().find(|r| r.name == name).unwrap();
        std::fs::read_to_string(&role.log).unwrap_or_default()
    }
}

impl Cluster {
    /// `GET <path>` on the ops port of the role started as `name`: status and body.
    pub fn ops_get(&self, name: &str, path: &str) -> (u16, String) {
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

impl Cluster {
    /// Sends SIGTERM to a role and waits up to `limit` for it to exit; its exit status, or
    /// `None` if it is still running.
    pub fn terminate_within(
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
