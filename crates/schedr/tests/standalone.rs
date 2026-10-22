//! The real `schedr` binary end to end (PLAN.md 1.8, 1.10, 1.11, 1.13, 1.14): `schedr
//! standalone` over a temporary DAG root, driven through the CLI and the REST API, with the
//! real Python runner.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed) and, for the Postgres test,
//! `SCHEDR_TEST_POSTGRES_URL`; `make test-integration` sets both.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use pretty_assertions::assert_eq;
use serde_json::Value;
use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_schedr");

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

/// A running `schedr standalone` with its own directory, DAG root and ports.
struct Standalone {
    dir: TempDir,
    child: Child,
    api: String,
}

impl Standalone {
    /// Starts standalone over the DAG files given, on SQLite or the database given.
    fn start(files: &[(&str, &str)], database_url: Option<&str>) -> Self {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("dags/etl");
        std::fs::create_dir_all(&root).unwrap();
        for (name, text) in files {
            std::fs::write(root.join(name), text).unwrap();
        }
        let (grpc, http) = (free_port(), free_port());
        let config = format!(
            "[dags]\npoll_interval = \"200ms\"\nsettle_time = \"0s\"\n\n\
             [interpreters]\npython = \"{python}\"\n\n\
             [worker]\ndata_dir = \"{data}\"\nslots = 4\n\n\
             [master]\ngrpc_port = {grpc}\ntick_interval = \"100ms\"\n\n\
             [webserver]\nport = {http}\n",
            python = python(),
            data = dir.path().join("data").display(),
        );
        std::fs::write(dir.path().join("config.toml"), config).unwrap();
        let log = std::fs::File::create(dir.path().join("standalone.log")).unwrap();
        let mut command = Command::new(BINARY);
        command.env_clear();
        if let Some(url) = database_url {
            // A URL with a password is a secret: from the environment, never the file.
            command.env("SCHEDR_CONFIG__DATABASE__URL", url);
        }
        let child = command
            .arg("--config")
            .arg(dir.path().join("config.toml"))
            .arg("standalone")
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", dir.path())
            .env("SCHEDR_DAG_PATH", &root)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let standalone = Self {
            dir,
            child,
            api: format!("http://127.0.0.1:{http}"),
        };
        standalone.wait("the API to answer", || {
            standalone.get("/api/v1/dags").is_some()
        });
        standalone
    }

    fn root(&self) -> PathBuf {
        self.dir.path().join("dags/etl")
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("standalone.log")).unwrap_or_default()
    }

    /// Runs the CLI against this standalone: exit code, stdout, stderr.
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

    /// The CLI's `--json` output, failing the test if the command fails.
    fn cli_json(&self, args: &[&str]) -> Value {
        let mut with_json = args.to_vec();
        with_json.insert(1, "--json");
        let (code, out, err) = self.cli(&with_json);
        assert_eq!(code, 0, "schedr {args:?} failed: {err}\n{}", self.log());
        serde_json::from_str(&out).unwrap()
    }

    /// `GET` on the API; `None` until it answers.
    fn get(&self, path: &str) -> Option<Value> {
        let output = Command::new("curl")
            .args(["-sf", &format!("{}{path}", self.api)])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| serde_json::from_slice(&output.stdout).ok())
            .flatten()
    }

    fn dags(&self) -> Vec<(String, Value)> {
        self.cli_json(&["dags", "list"])["dags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| (d["dag_id"].as_str().unwrap().to_owned(), d.clone()))
            .collect()
    }

    fn dag_ids(&self) -> Vec<String> {
        self.dags().into_iter().map(|(id, _)| id).collect()
    }

    fn runs(&self, dag: &str) -> Vec<Value> {
        self.cli_json(&["runs", "list", dag])["runs"]
            .as_array()
            .unwrap()
            .clone()
    }

    fn run(&self, run_id: &str) -> Value {
        self.get(&format!("/api/v1/runs/{run_id}")).unwrap()
    }

    fn trigger(&self, dag: &str) -> String {
        let command = self.cli_json(&["runs", "trigger", dag]);
        command["result"]["run_id"].as_str().unwrap().to_owned()
    }

    fn wait_for_state(&self, run_id: &str, state: &str) -> Value {
        let mut last = Value::Null;
        self.wait(&format!("run {run_id} to be {state}"), || {
            last = self.run(run_id);
            last["run"]["state"] == state
        });
        last
    }

    /// Waits up to 60 s for `check`, showing standalone's log on failure.
    fn wait(&self, what: &str, mut check: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if check() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!(
            "timed out waiting for {what}\n--- standalone log ---\n{}",
            self.log()
        );
    }
}

impl Drop for Standalone {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn task_states(run: &Value) -> Vec<(String, String)> {
    run["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            (
                t["task_id"].as_str().unwrap().to_owned(),
                t["state"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect()
}

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

fn logs(dir: &Path) -> String {
    let mut text = String::new();
    for entry in std::fs::read_dir(dir.join("data/logs")).unwrap() {
        text.push_str(&std::fs::read_to_string(entry.unwrap().path()).unwrap_or_default());
    }
    text
}

fn a_chain_runs_end_to_end(standalone: &Standalone) {
    standalone.wait("DAG `chain`", || standalone.dag_ids() == ["chain"]);
    let (_, chain) = &standalone.dags()[0];
    assert_eq!(chain["paused"], true);
    assert_eq!(chain["location"]["path"], "chain.py");
    let run_id = standalone.trigger("chain");
    let run = standalone.wait_for_state(&run_id, "success");
    assert_eq!(
        task_states(&run),
        pairs(&[
            ("extract", "success"),
            ("load", "success"),
            ("report", "success")
        ])
    );
    assert!(logs(standalone.dir.path()).contains("report loaded 3"));
    let (code, out, _) = standalone.cli(&["runs", "list", "chain"]);
    assert_eq!(code, 0);
    assert!(
        out.contains(&run_id) && out.contains("manual") && out.contains("success"),
        "{out}"
    );
}

#[test]
fn a_chain_with_data_flow_into_a_command_runs_end_to_end() {
    let standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    a_chain_runs_end_to_end(&standalone);
}

#[test]
fn the_same_run_works_on_postgres() {
    let admin = std::env::var("SCHEDR_TEST_POSTGRES_URL").expect(
        "SCHEDR_TEST_POSTGRES_URL: an administrative Postgres URL (`make test-integration` sets it)",
    );
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("e2e_{}_{nanos}", std::process::id());
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
    let standalone = Standalone::start(&[("chain.py", CHAIN)], Some(&format!("{server}/{name}")));
    a_chain_runs_end_to_end(&standalone);
}

#[test]
fn a_failure_is_retried_then_fails_the_run_and_its_downstream_task() {
    let dag = r#"
from datetime import timedelta
from schedr import Dag, task

@task(retries=1, retry_delay=timedelta(milliseconds=200))
def flaky() -> int:
    raise ValueError("source is down")

@task
def after(value: int) -> None: ...

with Dag("flaky"):
    after(flaky())
"#;
    let standalone = Standalone::start(&[("flaky.py", dag)], None);
    standalone.wait("DAG `flaky`", || standalone.dag_ids() == ["flaky"]);
    let run_id = standalone.trigger("flaky");
    let run = standalone.wait_for_state(&run_id, "failed");
    assert_eq!(
        task_states(&run),
        pairs(&[("after", "upstream_failed"), ("flaky", "failed")])
    );
    let attempts = standalone
        .get(&format!("/api/v1/runs/{run_id}/tasks/flaky/attempts"))
        .unwrap();
    let attempts = attempts["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    for attempt in attempts {
        assert_eq!(attempt["status"], "failed");
        assert_eq!(attempt["error_summary"]["message"], "source is down");
    }
}

#[test]
fn a_running_task_is_stopped_when_its_run_is_cancelled() {
    let dag = r#"
import time
from schedr import Dag, task

@task
def slow() -> None:
    time.sleep(120)

with Dag("slow"):
    slow()
"#;
    let standalone = Standalone::start(&[("slow.py", dag)], None);
    standalone.wait("DAG `slow`", || standalone.dag_ids() == ["slow"]);
    let run_id = standalone.trigger("slow");
    standalone.wait("the task to run", || {
        let attempts = standalone.get(&format!("/api/v1/runs/{run_id}/tasks/slow/attempts"));
        attempts.is_some_and(|a| a["attempts"].as_array().is_some_and(|a| !a.is_empty()))
    });
    let (code, out, err) = standalone.cli(&["runs", "cancel", &run_id]);
    assert_eq!(code, 0, "{err}");
    assert!(
        out.starts_with(&format!("cancelling run {run_id}")),
        "{out}"
    );
    let run = standalone.wait_for_state(&run_id, "cancelled");
    assert_eq!(task_states(&run), pairs(&[("slow", "cancelled")]));
    let attempts = standalone
        .get(&format!("/api/v1/runs/{run_id}/tasks/slow/attempts"))
        .unwrap();
    assert_eq!(attempts["attempts"][0]["status"], "cancelled");
}

#[test]
fn files_added_changed_and_removed_update_the_dag_list() {
    let dag = |id: &str| format!("from schedr import Dag\nwith Dag('{id}'):\n    pass\n");
    let standalone = Standalone::start(&[("a.py", &dag("a"))], None);
    let root = standalone.root();
    standalone.wait("DAG `a`", || standalone.dag_ids() == ["a"]);
    std::fs::write(root.join("b.py"), dag("b")).unwrap();
    standalone.wait("DAG `b` added", || standalone.dag_ids() == ["a", "b"]);
    std::fs::write(root.join("a.py"), dag("a2")).unwrap();
    standalone.wait("`a` renamed to `a2`", || {
        standalone.dag_ids() == ["a2", "b"]
    });
    std::fs::remove_file(root.join("b.py")).unwrap();
    standalone.wait("`b` removed", || standalone.dag_ids() == ["a2"]);
    std::fs::write(root.join("a.py"), "this is not python").unwrap();
    std::fs::write(root.join("c.py"), dag("c")).unwrap();
    standalone.wait("`c` added", || {
        standalone.dag_ids().contains(&"c".to_owned())
    });
    assert_eq!(
        standalone.dag_ids(),
        ["a2", "c"],
        "a broken file keeps its DAGs"
    );
    std::fs::write(root.join("d.py"), dag("c")).unwrap();
    standalone.wait("`c` in conflict", || {
        standalone
            .dags()
            .iter()
            .any(|(id, d)| id == "c" && d["conflict"].as_array().is_some_and(|p| p.len() == 2))
    });
    let (_, out, _) = standalone.cli(&["dags", "list"]);
    assert!(out.contains("CONFLICT: 2 places"), "{out}");
}

#[test]
fn an_unpaused_dag_gets_scheduled_runs_until_paused() {
    let dag = r#"
from datetime import timedelta
from schedr import Dag, Every, task

@task
def tick() -> None: ...

with Dag("ticker", trigger=Every(timedelta(seconds=1))):
    tick()
"#;
    let standalone = Standalone::start(&[("ticker.py", dag)], None);
    standalone.wait("DAG `ticker`", || standalone.dag_ids() == ["ticker"]);
    assert_eq!(
        standalone.runs("ticker"),
        Vec::<Value>::new(),
        "paused when new"
    );
    let (code, out, _) = standalone.cli(&["dags", "unpause", "ticker"]);
    assert_eq!((code, out.as_str()), (0, "unpaused ticker\n"));
    standalone.wait("two successful scheduled runs", || {
        standalone
            .runs("ticker")
            .iter()
            .filter(|r| r["kind"] == "scheduled" && r["state"] == "success")
            .count()
            >= 2
    });
    let (code, _, _) = standalone.cli(&["dags", "pause", "ticker"]);
    assert_eq!(code, 0);
    std::thread::sleep(Duration::from_millis(500));
    let before = standalone.runs("ticker").len();
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        standalone.runs("ticker").len(),
        before,
        "no runs while paused"
    );
}

#[test]
fn the_cli_reports_rejections_and_bad_arguments() {
    let standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    standalone.wait("DAG `chain`", || standalone.dag_ids() == ["chain"]);
    let (code, _, err) = standalone.cli(&["dags", "pause", "ghost"]);
    assert_eq!(
        (code, err.as_str()),
        (1, "error: rejected: no DAG `ghost`\n")
    );
    let (code, _, err) =
        standalone.cli(&["runs", "trigger", "chain", "--for", "2999-01-01T00:00:00Z"]);
    assert_eq!(code, 1);
    assert!(
        err.starts_with("error: rejected: `--for` must not be in the future"),
        "{err}"
    );
    let (code, _, err) = standalone.cli(&["runs", "trigger", "chain", "--for", "2026-10-15T03:00"]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        err,
        "warning: no offset given: interpreted as 2026-10-15T03:00Z (UTC)\n"
    );
    let (code, _, err) = standalone.cli(&["runs", "list", "chain", "--for", "someday"]);
    assert_eq!(code, 1);
    assert!(err.contains("isn't a date or a date-time"), "{err}");
}
