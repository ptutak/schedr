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
use serde_json::{Value, json};
use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_schedr");

fn python() -> String {
    std::env::var("SCHEDR_TEST_PYTHON").expect(
        "SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)",
    )
}

/// The REST API's address from standalone's banner in `log` (`... API on http://ADDR, ...`),
/// waiting up to a minute for it; `None` if the process logged none. 0.0.0.0 is reached on
/// 127.0.0.1.
fn api_from_banner(log: &Path) -> Option<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        let text = std::fs::read_to_string(log).unwrap_or_default();
        if let Some(rest) = text.split("API on http://").nth(1) {
            let address = rest.split(',').next().unwrap();
            return Some(format!(
                "http://{}",
                address.replacen("0.0.0.0:", "127.0.0.1:", 1)
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    None
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
        Self::start_with(files, database_url, &[])
    }

    /// Starts standalone as [`Self::start`] does, with more `standalone` arguments.
    fn start_with(files: &[(&str, &str)], database_url: Option<&str>, extra: &[&str]) -> Self {
        Self::start_configured(files, database_url, extra, "", &[])
    }

    /// Starts standalone as [`Self::start_with`] does, with more configuration (TOML appended
    /// to the file) and environment variables.
    fn start_configured(
        files: &[(&str, &str)],
        database_url: Option<&str>,
        extra: &[&str],
        more_config: &str,
        more_env: &[(&str, &str)],
    ) -> Self {
        Self::launch(files, database_url, (extra, more_config, more_env), false)
    }

    /// Starts standalone in its DAG root with `--dag-path .`, `SCHEDR_DAG_PATH` naming another
    /// folder (`dags/elsewhere`, with a DAG of its own) that the option replaces (decision 488).
    fn start_in_root(files: &[(&str, &str)]) -> Self {
        Self::launch(files, None, (&["--dag-path", "."], "", &[]), true)
    }

    fn launch(
        files: &[(&str, &str)],
        database_url: Option<&str>,
        (extra, more_config, more_env): (&[&str], &str, &[(&str, &str)]),
        in_root: bool,
    ) -> Self {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("dags/etl");
        std::fs::create_dir_all(&root).unwrap();
        // A file under `../shared/` makes a second DAG root.
        let dag_path = if files.iter().any(|(name, _)| name.starts_with("../shared/")) {
            format!(
                "{}:{}",
                root.display(),
                dir.path().join("dags/shared").display()
            )
        } else {
            root.display().to_string()
        };
        for (name, text) in files {
            let file = root.join(name);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, text).unwrap();
        }
        // Port 0: standalone binds free ports itself and its banner says which (picking free
        // ports for it races with the tests running in parallel; B3).
        let config = format!(
            "[dags]\npoll_interval = \"200ms\"\nsettle_time = \"0s\"\n\n\
             [interpreters]\npython = \"{python}\"\n\n\
             [worker]\ndata_dir = \"{data}\"\nslots = 4\nlog_port = 0\n\n\
             [master]\ngrpc_port = 0\n\n\
             [webserver]\nport = 0\n\n\
             [ops]\nport = 0\n{more_config}",
            python = python(),
            data = dir.path().join("data").display(),
        );
        std::fs::write(dir.path().join("config.toml"), config).unwrap();
        let log = std::fs::File::create(dir.path().join("standalone.log")).unwrap();
        let mut command = Command::new(BINARY);
        command.env_clear();
        let dag_path = if in_root {
            let elsewhere = dir.path().join("dags/elsewhere");
            std::fs::create_dir_all(&elsewhere).unwrap();
            std::fs::write(
                elsewhere.join("decoy.py"),
                "from schedr import Dag, task\n\n\n@task\ndef decoy() -> None:\n    pass\n\n\n\
                 with Dag(\"decoy\"):\n    decoy()\n",
            )
            .unwrap();
            command.current_dir(&root);
            elsewhere.display().to_string()
        } else {
            dag_path
        };
        if let Some(url) = database_url {
            // A URL with a password is a secret: from the environment, never the file.
            command.env("SCHEDR_CONFIG__DATABASE__URL", url);
        }
        // The test's own PATH, when it gives one (a plugin to discover), wins.
        command.env("PATH", std::env::var("PATH").unwrap_or_default());
        command.envs(more_env.iter().copied());
        let child = command
            .arg("--config")
            .arg(dir.path().join("config.toml"))
            .args(["standalone", "--migrate"])
            .args(extra)
            .env("HOME", dir.path())
            .env("SCHEDR_DAG_PATH", &dag_path)
            .env("SCHEDR_HIDDEN__MASTER__TICK_INTERVAL", "100ms")
            .env("SCHEDR_SECRET_KEY", SECRET_KEY)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let log = dir.path().join("standalone.log");
        let api = api_from_banner(&log).unwrap_or_else(|| {
            panic!(
                "standalone logged no banner:\n{}",
                std::fs::read_to_string(&log).unwrap_or_default()
            )
        });
        let standalone = Self { dir, child, api };
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

    /// `schedr secrets ...` on this standalone's database (its SQLite file, or the URL
    /// given): exit code, stdout, stderr.
    fn secrets(&self, database_url: Option<&str>, args: &[&str]) -> (i32, String, String) {
        let mut command = Command::new(BINARY);
        command.env_clear();
        if let Some(url) = database_url {
            command.env("SCHEDR_CONFIG__DATABASE__URL", url);
        }
        let output = command
            .arg("--config")
            .arg(self.dir.path().join("config.toml"))
            .arg("secrets")
            .args(args)
            .env("HOME", self.dir.path())
            .env("SCHEDR_SECRET_KEY", SECRET_KEY)
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

    /// `GET` of a web UI page (HTML); `None` unless it answers 200.
    fn page(&self, path: &str) -> Option<String> {
        let output = Command::new("curl")
            .args(["-sf", &format!("{}{path}", self.api)])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
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
    CommandOperator("report", ["echo", t"report {done}"])
"#;

/// Standalone's `SCHEDR_SECRET_KEY` (the `db` secret storage's).
const SECRET_KEY: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

/// Reads a secret of the `db` storage, set with `schedr secrets set`.
const SECRETS: &str = r#"
from schedr import CommandOperator, Dag, secrets, task, task_ctx

@task(secrets=["etl/db#user"])
def check(password: str) -> int:
    assert password == "hunter22-pw", "the password"
    assert task_ctx.secret("etl/db", "user") == "etl-user"
    print("password is", password)
    return len(password)

with Dag("secrets"):
    n = check(secrets["etl/db"]["password"])
    CommandOperator("cmd", ["sh", "-c", 'echo "env $PW"; echo "len $0"', t"{n}"],
        env={"PW": secrets["etl/db"]["password"]})
"#;

/// A task reads secrets set with the CLI, masked in its log; the table holds no value.
fn secrets_set_with_the_cli_reach_a_task(standalone: &Standalone, database_url: Option<&str>) {
    let (code, out, err) = standalone.secrets(
        database_url,
        &["set", "etl/db", "password=hunter22-pw", "user=etl-user"],
    );
    assert_eq!(code, 0, "{out}{err}");
    let (code, out, err) = standalone.secrets(database_url, &["list"]);
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("etl/db\tpassword,user\t"), "{out}");
    standalone.wait("DAG `secrets`", || standalone.dag_ids() == ["secrets"]);
    let run_id = standalone.trigger("secrets");
    standalone.wait_for_state(&run_id, "success");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains("password is ****") && logs.contains("env ****") && logs.contains("len 11"),
        "{logs}"
    );
    assert!(!logs.contains("hunter22-pw"), "{logs}");
}

#[test]
fn a_task_reads_secrets_set_with_the_cli() {
    let standalone = Standalone::start(&[("secret_reads.py", SECRETS)], None);
    secrets_set_with_the_cli_reach_a_task(&standalone, None);
    // Encrypted at rest: no value in the SQLite files.
    for entry in std::fs::read_dir(standalone.dir.path().join("data")).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("schedr.db")
        {
            // A journal listed can be gone by now (SQLite removes it at a commit's end).
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => panic!("{}: {e}", path.display()),
            };
            assert!(
                !bytes.windows(11).any(|w| w == b"hunter22-pw"),
                "{} holds the value",
                path.display()
            );
        }
    }
}

/// Reads a secret from Vault: the default storage, and by name.
const VAULT_SECRETS: &str = r#"
from schedr import CommandOperator, Dag, secrets, task

@task
def check(password: str, port: str) -> int:
    assert password == "hunter22-vault", "the password"
    assert port == "5432", "a number as its JSON text"
    print("password is", password)
    return len(password)

with Dag("vault"):
    check(secrets["PATH"]["password"], secrets.vault["PATH"]["port"])
    CommandOperator("cmd", ["sh", "-c", 'echo "env $PW"'], env={"PW": secrets["PATH"]["password"]})
"#;

#[test]
fn a_task_reads_secrets_from_vault() {
    let url = std::env::var("SCHEDR_TEST_VAULT_URL")
        .expect("SCHEDR_TEST_VAULT_URL: a Vault in dev mode (`make test-integration` starts one)");
    let token = std::env::var("SCHEDR_TEST_VAULT_TOKEN").unwrap_or_else(|_| "root".to_owned());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = format!("e2e-{}-{nanos}/db", std::process::id());
    let written = Command::new("curl")
        .args([
            "-sf",
            "-X",
            "POST",
            "-H",
            &format!("X-Vault-Token: {token}"),
        ])
        .args([
            "-d",
            r#"{"data": {"password": "hunter22-vault", "port": 5432}}"#,
        ])
        .arg(format!("{url}/v1/secret/data/{path}"))
        .status()
        .unwrap();
    assert!(written.success());
    let dag = VAULT_SECRETS.replace("PATH", &path);
    let config = format!(
        "\n[secret_storage.vault]\ntype = \"vault\"\naddress = \"{url}\"\n\
         auth = {{ method = \"token\" }}\n"
    );
    let standalone = Standalone::start_configured(
        &[("vault.py", &dag)],
        None,
        &[],
        &config,
        &[("SCHEDR_CONFIG__SECRET_STORAGE__VAULT__TOKEN", &token)],
    );
    standalone.wait("DAG `vault`", || standalone.dag_ids() == ["vault"]);
    let run_id = standalone.trigger("vault");
    standalone.wait_for_state(&run_id, "success");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains("password is ****") && logs.contains("env ****"),
        "{logs}"
    );
    assert!(!logs.contains("hunter22-vault"), "{logs}");
}

/// Every task log, of every worker (`data/logs/<worker ID>/`).
fn logs(dir: &Path) -> String {
    let mut text = String::new();
    for worker in std::fs::read_dir(dir.join("data/logs")).unwrap() {
        for entry in std::fs::read_dir(worker.unwrap().path()).unwrap() {
            text.push_str(&std::fs::read_to_string(entry.unwrap().path()).unwrap_or_default());
        }
    }
    text
}

fn a_chain_runs_end_to_end(standalone: &Standalone) {
    standalone.wait("DAG `chain`", || standalone.dag_ids() == ["chain"]);
    let (_, chain) = &standalone.dags()[0];
    assert_eq!(chain["trigger_state"], Value::Null, "no trigger, no switch");
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
    // The same log through the API, from standalone's worker.
    let (code, out, err) = standalone.cli(&["tasks", "logs", &run_id, "report", "--try", "1"]);
    assert_eq!((code, out.as_str()), (0, "report loaded 3\n"), "{err}");
    let (code, out, _) = standalone.cli(&["runs", "list", "chain"]);
    assert_eq!(code, 0);
    assert!(
        out.contains(&run_id) && out.contains("manual") && out.contains("success"),
        "{out}"
    );
    // The web UI, served next to the API over the same deployment.
    let list = standalone.page("/").expect("the UI's DAG list");
    assert!(
        list.contains(r#"href="/d/default/dags/chain""#) && list.contains("Manual"),
        "{list}"
    );
    let page = standalone
        .page("/d/default/dags/chain")
        .expect("the UI's DAG page");
    assert!(
        page.contains(&run_id) && page.contains(r#"class="state success""#),
        "{page}"
    );
}

#[test]
fn a_chain_with_data_flow_into_a_command_runs_end_to_end() {
    let standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    a_chain_runs_end_to_end(&standalone);
}

/// A new database on the test Postgres (`SCHEDR_TEST_POSTGRES_URL`): its URL.
fn postgres_database() -> String {
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
    format!("{server}/{name}")
}

#[test]
fn the_same_run_works_on_postgres() {
    let standalone = Standalone::start(&[("chain.py", CHAIN)], Some(&postgres_database()));
    a_chain_runs_end_to_end(&standalone);
}

#[test]
fn a_task_reads_secrets_set_with_the_cli_on_postgres() {
    let url = postgres_database();
    let standalone = Standalone::start(&[("secret_reads.py", SECRETS)], Some(&url));
    secrets_set_with_the_cli_reach_a_task(&standalone, Some(&url));
    let stored = Command::new("psql")
        .args([url.as_str(), "-tAc", "SELECT data FROM secrets"])
        .output()
        .unwrap();
    let stored = String::from_utf8_lossy(&stored.stdout);
    assert!(stored.contains("\"password\": \"v1:"), "{stored}");
    assert!(!stored.contains("hunter22-pw"), "{stored}");
}

#[test]
fn a_standalones_deployment_is_one_process_holding_its_master_and_worker() {
    // M4.4b: one rectangle on the Deployments page, the process, holding both.
    let standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    let mut deployment = None;
    for _ in 0..200 {
        deployment = standalone
            .get("/api/v1/deployment")
            .filter(|d| d["masters"].as_array().is_some_and(|m| !m.is_empty()))
            .filter(|d| d["workers"][0]["process_id"].is_string());
        if deployment.is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let deployment = deployment.expect("the master's row and the worker's registration");
    let process = &deployment["webserver"]["process_id"];
    assert_eq!(deployment["masters"][0]["role"], "leader", "{deployment}");
    assert_eq!(&deployment["masters"][0]["process_id"], process);
    assert_eq!(&deployment["workers"][0]["process_id"], process);
    assert_eq!(
        deployment["workers"][0]["master_id"],
        deployment["masters"][0]["master_id"]
    );
    // Each part's configuration: the process's.
    assert!(deployment["workers"][0]["config"]["worker"].is_object());
    assert!(deployment["masters"][0]["config"]["master"].is_object());
    let page = Command::new("curl")
        .args(["-sf", &format!("{}/deployments", standalone.api)])
        .output()
        .unwrap();
    let page = String::from_utf8_lossy(&page.stdout);
    // One process: its UI, webserver, master and worker, with no line between them.
    assert_eq!(page.matches("<g class=\"process\">").count(), 1, "{page}");
    for kind in ["ui", "webserver", "master", "worker"] {
        let node = format!("<a class=\"node {kind}\"");
        assert_eq!(page.matches(&node).count(), 1, "{kind}: {page}");
    }
    assert_eq!(page.matches("<path class=\"edge ").count(), 0, "{page}");
}

#[test]
fn outcomes_route_a_run_from_python_and_from_exit_codes() {
    let dag = r#"
from dataclasses import dataclass
from schedr import CommandOperator, Dag, Outcome, task

@dataclass
class Rows:
    rows: list[int]

@dataclass
class Unavailable:
    reason: str

class Check(Outcome):
    CHANGED = 0
    UNCHANGED = 3

@task
def fetch() -> Rows | Unavailable:
    return Unavailable("down")

@task
def load(data: Rows) -> int:
    return len(data.rows)

@task
def report(why: Unavailable) -> str:
    return why.reason

@task
def react(outcome: Check) -> str:
    return outcome.name

with Dag("routed"):
    fetched = fetch()
    load(fetched.on(Rows))
    report(fetched.on(Unavailable))
    probe = CommandOperator("probe", "exit 3", outcomes=Check)
    probe.on(Check.CHANGED) >> CommandOperator("on_change", "true")
    react(probe.outcome)
"#;
    let standalone = Standalone::start(&[("routed.py", dag)], None);
    standalone.wait("DAG `routed`", || standalone.dag_ids() == ["routed"]);
    let run_id = standalone.trigger("routed");
    let run = standalone.wait_for_state(&run_id, "success");
    assert_eq!(
        task_states(&run),
        pairs(&[
            ("fetch", "success"),
            ("load", "skipped"),
            ("on_change", "skipped"),
            ("probe", "success"),
            ("react", "success"),
            ("report", "success"),
        ])
    );
    let output = |task: &str| {
        standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap()
    };
    // The outcome union's member reached `report` decoded; the exit code 3 is UNCHANGED.
    assert_eq!(output("report")["value"], "down", "{}", output("report"));
    assert_eq!(output("react")["value"], "UNCHANGED", "{}", output("react"));
    assert_eq!(
        output("fetch")["outcome"],
        "Unavailable",
        "{}",
        output("fetch")
    );
    assert_eq!(
        output("probe")["outcome"],
        "UNCHANGED",
        "{}",
        output("probe")
    );
}

#[test]
fn a_mapped_task_runs_its_instances_and_lanes_and_the_gathers_get_their_lists() {
    let dag = r#"
from schedr import Dag, Data, task

class Rows(Data):
    count: int

class Missing(Data):
    reason: str

@task
def list_files() -> list[str]:
    return ["a.csv", "missing.csv", "bb.csv"]

@task
def fetch(bucket: str, path: str) -> Rows | Missing:
    if path.startswith("missing"):
        return Missing(reason=f"{bucket}/{path} not found")
    return Rows(count=len(path))

@task
def count(rows: Rows) -> int:
    return rows.count * 10

@task
def total(counts: list[int | None]) -> list[int | None]:
    return counts

@task
def summary(results: list[Rows | Missing]) -> list[str]:
    return [type(r).__name__ for r in results]

with Dag("mapped"):
    results = fetch.partial(bucket="raw").expand(path=list_files())
    total(count.expand(rows=results.on(Rows)))
    summary(results)
"#;
    let standalone = Standalone::start(&[("mapped.py", dag)], None);
    standalone.wait("DAG `mapped`", || standalone.dag_ids() == ["mapped"]);
    let run_id = standalone.trigger("mapped");
    standalone.wait_for_state(&run_id, "success");
    let output = |task: &str| {
        standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap()
    };
    // `count` ran for the two `Rows` lanes (its instance for `Missing` was skipped: `None`
    // in the gathered list), in the order of the files.
    assert_eq!(
        output("total")["value"],
        serde_json::json!([50, null, 60]),
        "{}",
        output("total")
    );
    // A gather of the whole union sees every instance, each decoded as its own member.
    assert_eq!(
        output("summary")["value"],
        serde_json::json!(["Rows", "Missing", "Rows"]),
        "{}",
        output("summary")
    );

    mapped_run_per_instance(&standalone, &run_id);
}

/// The mapped run of the test above, per instance (decisions 80-86): the API, the CLI and the
/// UI.
fn mapped_run_per_instance(standalone: &Standalone, run_id: &str) {
    let output = |task: &str| {
        standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap()
    };
    // The run's tasks list the whole task, then its instances.
    let run = standalone.get(&format!("/api/v1/runs/{run_id}")).unwrap();
    let rows: Vec<(String, i64, Value)> = run["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["task_id"] == "fetch")
        .map(|t| {
            (
                t["state"].as_str().unwrap().to_owned(),
                t["map_index"].as_i64().unwrap(),
                t["instances"].clone(),
            )
        })
        .collect();
    let row = |state: &str, index, instances| (state.to_owned(), index, instances);
    assert_eq!(
        rows,
        [
            row("success", -1, serde_json::json!(3)),
            row("success", 0, Value::Null),
            row("success", 1, Value::Null),
            row("success", 2, Value::Null),
        ]
    );
    // The whole task's output is the gathered list; one instance's, its own.
    assert_eq!(output("count")["value"], serde_json::json!([50, null, 60]));
    let second = standalone
        .get(&format!(
            "/api/v1/runs/{run_id}/tasks/fetch/output?map_index=1"
        ))
        .unwrap();
    assert_eq!(
        (&second["outcome"], &second["value"]["reason"]),
        (
            &serde_json::json!("Missing"),
            &serde_json::json!("raw/missing.csv not found")
        )
    );
    // Attempts need an instance (a 400 without one); the CLI takes `--map-index`.
    let attempts = |query: &str| {
        standalone.get(&format!(
            "/api/v1/runs/{run_id}/tasks/fetch/attempts{query}"
        ))
    };
    assert_eq!(attempts(""), None);
    assert_eq!(
        attempts("?map_index=2").unwrap()["attempts"][0]["status"],
        "success"
    );
    let (code, _, err) = standalone.cli(&["tasks", "logs", run_id, "fetch", "--map-index", "0"]);
    assert_eq!(code, 0, "{err}");
    mapped_run_in_the_cli(standalone, run_id);
    // The UI: the mapped task's panel lists its instances.
    let panel = standalone
        .page(&format!("/d/default/runs/{run_id}?task=fetch"))
        .expect("the run page with the mapped task's panel");
    assert!(
        panel.contains("fetch ×3") && panel.contains("Instances") && panel.contains("[2]"),
        "{panel}"
    );
    let one = standalone
        .page(&format!(
            "/d/default/runs/{run_id}?task=fetch&map_index=1&tab=output"
        ))
        .expect("an instance's panel");
    assert!(one.contains("fetch[1]") && one.contains("Missing"), "{one}");
    // The instance's panel names the graph's node, so the node stays selected on a swap.
    assert!(
        one.contains(r#"<div class="panel-head" data-task="fetch">"#),
        "{one}"
    );
    // A download names an instance it can read, never the gathered list instead.
    let download = format!("/d/default/runs/{run_id}/tasks/fetch/output.json");
    assert!(
        standalone
            .page(&format!("{download}?map_index=1"))
            .is_some()
    );
    assert_eq!(standalone.page(&format!("{download}?map_index=nope")), None);
}

/// The mapped run in the CLI's reads (decision 86, M4.3d): `runs show`, `tasks attempts` and
/// `tasks output`.
fn mapped_run_in_the_cli(standalone: &Standalone, run_id: &str) {
    // `runs show` lists the whole task, then its instances (decision 86).
    let (code, out, err) = standalone.cli(&["runs", "show", run_id]);
    assert_eq!(code, 0, "{err}");
    let names: Vec<&str> = out
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| name.starts_with("fetch"))
        .collect();
    assert_eq!(
        names,
        ["fetch", "fetch[0]", "fetch[1]", "fetch[2]"],
        "{out}"
    );
    let whole = out.lines().find(|l| l.starts_with("fetch ")).unwrap();
    assert_eq!(
        whole.split_whitespace().collect::<Vec<_>>(),
        ["fetch", "success", "SUCCESS", "0", "3"],
        "{out}"
    );
    // Attempts of an instance; a mapped task needs one.
    let (code, out, err) =
        standalone.cli(&["tasks", "attempts", run_id, "fetch", "--map-index", "2"]);
    assert_eq!(code, 0, "{err}");
    assert!(
        out.lines()
            .nth(1)
            .is_some_and(|l| l.starts_with("1    success")),
        "{out}"
    );
    // That instance's attempt, not another's: every instance succeeded, so compare IDs.
    let api = |index: u32| {
        standalone
            .get(&format!(
                "/api/v1/runs/{run_id}/tasks/fetch/attempts?map_index={index}"
            ))
            .unwrap()["attempts"][0]["attempt_id"]
            .clone()
    };
    let cli = standalone.cli_json(&["tasks", "attempts", run_id, "fetch", "--map-index", "2"]);
    assert_eq!(cli["attempts"][0]["attempt_id"], api(2), "{cli}");
    assert_ne!(api(2), api(0));
    let (code, _, err) = standalone.cli(&["tasks", "attempts", run_id, "fetch"]);
    assert_ne!(code, 0);
    assert!(err.contains("map_index"), "{err}");
    // The gathered list as JSON, ready for a pipe; one instance's output and outcome.
    let (code, out, err) = standalone.cli(&["tasks", "output", run_id, "count"]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        serde_json::from_str::<Value>(&out).unwrap(),
        serde_json::json!([50, null, 60])
    );
    let one = standalone.cli_json(&["tasks", "output", run_id, "fetch", "--map-index", "1"]);
    assert_eq!(one["outcome"], "Missing", "{one}");
    assert_eq!(one["value"]["reason"], "raw/missing.csv not found", "{one}");
    // A task the run doesn't have is an error, not an empty table.
    let (code, _, err) = standalone.cli(&["tasks", "attempts", run_id, "ghost"]);
    assert_ne!(code, 0);
    assert!(err.contains("no task `ghost` in run"), "{err}");
}

#[test]
fn a_branch_runs_only_the_targets_it_chose() {
    let dag = r#"
from schedr import CommandOperator, Dag, Task, branch

@branch
def targets() -> list[Task]:
    return [to_b]

with Dag("branched"):
    chooser = targets()
    to_a = CommandOperator("to_a", "true")
    to_b = CommandOperator("to_b", "true")
    chooser >> [to_a, to_b]
"#;
    let standalone = Standalone::start(&[("branched.py", dag)], None);
    standalone.wait("DAG `branched`", || standalone.dag_ids() == ["branched"]);
    let run_id = standalone.trigger("branched");
    let run = standalone.wait_for_state(&run_id, "success");
    assert_eq!(
        task_states(&run),
        pairs(&[
            ("targets", "success"),
            ("to_a", "skipped"),
            ("to_b", "success")
        ])
    );
    let chosen = standalone
        .get(&format!("/api/v1/runs/{run_id}/tasks/targets/output"))
        .unwrap();
    assert_eq!(chosen["value"], serde_json::json!(["to_b"]), "{chosen}");
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
    std::fs::write(root.join("a.py"), "# schedr\nthis is not python").unwrap();
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

/// M4.9a: a scheduled job in a script root's `schedr.toml` is found as a one-task DAG, paused
/// when new, listed and reachable by its encoded ID; an unknown key is an import error naming
/// it.
#[test]
fn a_scheduled_script_job_is_found_paused_and_listed_and_a_bad_key_is_an_import_error() {
    let scripts = TempDir::new().unwrap();
    let root = scripts.path().join("jobs");
    std::fs::create_dir_all(root.join("team")).unwrap();
    std::fs::write(root.join("team/report.py"), "print('report')\n").unwrap();
    let job = "[jobs.daily]\nscript = \"report.py\"\ninterpreter = \"python\"\n\
               schedule = \"0 3 * * *\"\n";
    std::fs::write(root.join("team/schedr.toml"), job).unwrap();
    let standalone = Standalone::start_configured(
        &[],
        None,
        &[],
        "",
        &[("SCHEDR_SCRIPT_PATH", root.to_str().unwrap())],
    );
    let id = "jobs:team/schedr.toml#daily";
    standalone.wait("the script job", || standalone.dag_ids() == [id]);
    let (_, dag) = &standalone.dags()[0];
    assert_eq!(dag["trigger_state"], "paused");
    assert_eq!(dag["location"]["root"], "jobs");
    assert_eq!(dag["location"]["path"], "team/schedr.toml");
    let shown = standalone
        .get("/api/v1/dags/jobs%3Ateam%2Fschedr.toml%23daily")
        .expect("the job by its encoded ID");
    assert_eq!(shown["dag_id"], id, "{shown}");

    std::fs::write(
        root.join("team/schedr.toml"),
        format!("{job}colour = \"blue\"\n"),
    )
    .unwrap();
    let mut errors = Value::Null;
    standalone.wait("the import error", || {
        errors = standalone.get("/api/v1/import-errors").unwrap_or_default();
        errors["errors"]
            .as_array()
            .is_some_and(|list| !list.is_empty())
    });
    let error = &errors["errors"][0];
    assert_eq!(error["root"], "jobs");
    assert_eq!(error["path"], "team/schedr.toml");
    assert!(
        error.to_string().contains("colour"),
        "names the key: {error}"
    );
}

const SCRIPT_DAG: &str = r#"
from schedr import Dag, ScriptOperator, dag_ctx

with Dag("uses_script"):
    ScriptOperator(
        "clean",
        "jobs:team/schedr.toml#daily",
        args=["--run", dag_ctx.run.id],
        env={"MODE": "from-dag"},
    )
"#;

#[test]
fn a_script_runs_as_its_own_job_and_from_a_dag_with_its_library() {
    let scripts = TempDir::new().unwrap();
    let root = scripts.path().join("jobs");
    std::fs::create_dir_all(root.join("team")).unwrap();
    // A library at the script root, found through PYTHONPATH (its copy).
    std::fs::write(root.join("shared.py"), "GREETING = 'hello'\n").unwrap();
    std::fs::write(
        root.join("team/report.py"),
        "import os, sys\nimport shared\n\
         print(shared.GREETING, os.environ['MODE'], os.environ['LEVEL'], *sys.argv[1:])\n",
    )
    .unwrap();
    std::fs::write(
        root.join("team/schedr.toml"),
        "[jobs.daily]\nscript = \"report.py\"\ninterpreter = \"python\"\n\
         schedule = \"0 3 * * *\"\nargs = [\"--declared\"]\n\
         env = { MODE = \"declared\", LEVEL = \"info\" }\n",
    )
    .unwrap();
    let standalone = Standalone::start_configured(
        &[("uses.py", SCRIPT_DAG)],
        None,
        &[],
        "",
        &[("SCHEDR_SCRIPT_PATH", root.to_str().unwrap())],
    );
    let job = "jobs:team/schedr.toml#daily";
    standalone.wait("the job and the DAG", || {
        standalone.dag_ids() == [job, "uses_script"]
    });
    let stdout = |run_id: &str, task: &str| {
        let output = standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap_or_default();
        output["value"]["stdout"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    };
    // The job on its own: everything as declared.
    let run_id = standalone.trigger(job);
    standalone.wait_for_state(&run_id, "success");
    assert_eq!(stdout(&run_id, "daily"), "hello declared info --declared\n");
    // From a DAG: args= replaces, env= merges (decision 302).
    let run_id = standalone.trigger("uses_script");
    standalone.wait_for_state(&run_id, "success");
    assert_eq!(
        stdout(&run_id, "clean"),
        format!("hello from-dag info --run {run_id}\n")
    );
}

const SCRIPT_REST_DAG: &str = r#"
from dataclasses import dataclass
from schedr import Dag, EmptyOperator, Outcome, ScriptOperator, ScriptResource

class Check(Outcome):
    SUCCESS = 0
    NO_DATA = 3

@dataclass
class Info:
    cluster_id: str

with Dag("rest"):
    check = ScriptOperator("check", "jobs:team/schedr.toml#check", outcomes=Check)
    routed = EmptyOperator("routed")
    check.on(Check.NO_DATA) >> routed
    plain = ScriptOperator("plain", "jobs:team/schedr.toml#check", cleanup_retries=0)
    cluster = ScriptResource("cluster", "jobs:team/schedr.toml#make", value=Info)
    cluster.cleanup_script("jobs:team/schedr.toml#drop", args=[t"{cluster.cluster_id}"])
    ScriptOperator("load", "jobs:team/schedr.toml#noop", uses=[cluster])
"#;

/// M4.9c: params, outcomes and a cleanup on a job and from a DAG; a script as a resource.
#[test]
fn a_script_has_params_outcomes_a_cleanup_and_can_be_a_resource() {
    let scripts = TempDir::new().unwrap();
    let root = scripts.path().join("jobs");
    std::fs::create_dir_all(root.join("team")).unwrap();
    let marks = scripts.path().join("marks");
    let write =
        |name: &str, body: &str| std::fs::write(root.join("team").join(name), body).unwrap();
    write(
        "work.py",
        "import sys\nfrom schedr.script import dag_ctx\n\
         print(dag_ctx.params.region, dag_ctx.params.limit)\nsys.exit(3)\n",
    );
    write(
        "mark.py",
        "import sys\nwith open(sys.argv[1], 'a') as f:\n    print(*sys.argv[2:], file=f)\n",
    );
    write("exit3.py", "import sys\nsys.exit(3)\n");
    write("noop.py", "");
    write(
        "make.py",
        "import os, subprocess, sys\n\
         fd = int(os.environ['SCHEDR_CTX_FD'])\n\
         subprocess.run([sys.argv[1], 'task-ctx', 'resource', 'set', 'cluster_id=c-8'],\n\
         check=True, pass_fds=[fd])\n",
    );
    let marks_path = marks.to_str().unwrap();
    let schedr = env!("CARGO_BIN_EXE_schedr");
    write(
        "schedr.toml",
        &format!(
            "[jobs.daily]\nscript = \"work.py\"\ninterpreter = \"python\"\n\
             schedule = \"0 3 * * *\"\n\
             [jobs.daily.outcomes]\nSUCCESS = 0\nNO_DATA = 3\n\
             [jobs.daily.params]\nregion = \"eu\"\nlimit = 5\n\
             [jobs.daily.cleanup]\nscript = \"mark.py\"\ninterpreter = \"python\"\n\
             args = [\"{marks_path}\", \"daily-cleanup\"]\nretries = 0\n\
             [jobs.check]\nscript = \"exit3.py\"\ninterpreter = \"python\"\n\
             [jobs.check.outcomes]\nSUCCESS = 0\nNO_DATA = 3\n\
             [jobs.check.cleanup]\nscript = \"mark.py\"\ninterpreter = \"python\"\n\
             args = [\"{marks_path}\", \"check-cleanup\"]\n\
             [jobs.make]\nscript = \"make.py\"\ninterpreter = \"python\"\n\
             args = [\"{schedr}\"]\n\
             [jobs.drop]\nscript = \"mark.py\"\ninterpreter = \"python\"\n\
             args = [\"{marks_path}\", \"dropped\"]\n\
             [jobs.noop]\nscript = \"noop.py\"\ninterpreter = \"python\"\n"
        ),
    );
    // `cleanup_script`'s `args=` replaces the declared ones: the marks' path comes first.
    let dag = SCRIPT_REST_DAG.replace(
        "args=[t\"{cluster.cluster_id}\"]",
        &format!("args=[{marks_path:?}, \"dropped\", t\"{{cluster.cluster_id}}\"]"),
    );
    let standalone = Standalone::start_configured(
        &[("rest.py", &dag)],
        None,
        &[],
        "",
        &[("SCHEDR_SCRIPT_PATH", root.to_str().unwrap())],
    );
    let job = "jobs:team/schedr.toml#daily";
    standalone.wait("the job and the DAG", || {
        standalone.dag_ids() == [job, "rest"]
    });
    let stdout = |run_id: &str, task: &str| {
        let output = standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap_or_default();
        output["value"]["stdout"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    };
    let marked = || std::fs::read_to_string(&marks).unwrap_or_default();

    // The job: its defaults, a declared exit code a success, and its cleanup.
    let run_id = standalone.trigger(job);
    standalone.wait_for_state(&run_id, "success");
    assert_eq!(stdout(&run_id, "daily"), "eu 5\n");
    standalone.wait("the job's cleanup", || marked().contains("daily-cleanup"));
    // A trigger's value, checked against the types the defaults gave.
    let command =
        standalone.cli_json(&["runs", "trigger", job, "-p", "region=us", "-p", "limit:=7"]);
    let run_id = command["result"]["run_id"].as_str().unwrap().to_owned();
    standalone.wait_for_state(&run_id, "success");
    assert_eq!(stdout(&run_id, "daily"), "us 7\n");
    let (code, _, err) = standalone.cli(&["runs", "trigger", job, "-p", "limit=many"]);
    assert_ne!(code, 0, "a string for an integer: {err}");

    // From a DAG: routed with the enum, the declared exit code a success without it too, each
    // with its cleanup; the script resource's value reaches its users' cleanup.
    let run_id = standalone.trigger("rest");
    let run = standalone.wait_for_state(&run_id, "success");
    let done = task_states(&run);
    for task in ["routed", "plain", "load"] {
        assert!(
            done.contains(&(task.to_owned(), "success".to_owned())),
            "{task}: {done:?}"
        );
    }
    let lines = marked();
    assert_eq!(lines.matches("check-cleanup").count(), 2, "{lines}");
    assert_eq!(lines.matches("dropped c-8").count(), 1, "{lines}");
    resources_have_their_own_endpoints(&standalone, &run_id, &run);
}

/// M4.9d (decision 305): a package with relative imports in a root, imported by a DAG.
#[test]
fn a_library_in_a_root_is_never_parsed_and_a_dag_file_must_mention_schedr() {
    let dag = r#"
from mylib.core import greeting
from schedr import Dag, task

@task
def hello() -> str:
    return greeting()

with Dag("uses_library"):
    hello()
"#;
    let standalone = Standalone::start(
        &[
            ("uses_library.py", dag),
            ("mylib/__init__.py", ""),
            ("mylib/util.py", "VALUE = 'hi'\n"),
            // Loaded by its path on its own, it would be an import error.
            (
                "mylib/core.py",
                "from .util import VALUE\n\ndef greeting():\n    return VALUE\n",
            ),
            // Run by a parse, it would be one too.
            ("not_a_dag.py", "raise ValueError('run by a parse')\n"),
            ("shim.py", "from schedr import Dag\n"),
            // Its text doesn't say `schedr`: skipped, whatever it would define.
            (
                "without_mention.py",
                "from shim import Dag\n\nwith Dag('never_found'):\n    pass\n",
            ),
            (
                "with_mention.py",
                "# schedr\nfrom shim import Dag\n\nwith Dag('found'):\n    pass\n",
            ),
        ],
        None,
    );
    standalone.wait("the DAGs", || {
        standalone.dag_ids() == ["found", "uses_library"]
    });
    let errors = standalone.get("/api/v1/import-errors").unwrap_or_default();
    assert_eq!(
        errors["errors"].as_array().map_or(0, Vec::len),
        0,
        "{errors}"
    );
    let run_id = standalone.trigger("uses_library");
    standalone.wait_for_state(&run_id, "success");
    let output = standalone
        .get(&format!("/api/v1/runs/{run_id}/tasks/hello/output"))
        .unwrap();
    assert_eq!(output["value"], "hi");
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

#[test]
fn listen_opens_the_api_beyond_loopback() {
    let standalone = Standalone::start_with(&[("chain.py", CHAIN)], None, &["--listen", "0.0.0.0"]);
    assert!(standalone.get("/api/v1/dags").is_some());
    let log = standalone.log();
    assert!(
        log.contains("the REST API signs no one in (auth mode `none`)"),
        "{log}"
    );
    // The ops port listens there too (probes come from outside the pod), for every part.
    let ops = log
        .split("ops on http://")
        .nth(1)
        .and_then(|rest| rest.split(',').next())
        .unwrap()
        .replacen("0.0.0.0:", "127.0.0.1:", 1);
    assert!(log.contains("ops on http://0.0.0.0:"), "{log}");
    let get = |path: &str| {
        let output = Command::new("curl")
            .args(["-s", &format!("http://{ops}{path}")])
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    standalone.wait("standalone to be ready", || get("/readyz") == "ok\n");
    assert_eq!(get("/healthz"), "ok\n");
    let metrics = get("/metrics");
    for name in [
        "schedr_master_tick_duration_seconds",
        "schedr_worker_slots 4",
        "schedr_http_requests_total{route=\"/api/v1/dags\"",
    ] {
        assert!(metrics.contains(name), "{name}\n{metrics}");
    }
}

#[test]
fn a_second_standalone_on_the_same_data_is_refused_and_sigterm_stops_the_first() {
    let mut standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    let second = Command::new(BINARY)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", standalone.dir.path())
        .arg("--config")
        .arg(standalone.dir.path().join("config.toml"))
        .arg("standalone")
        .output()
        .unwrap();
    assert_eq!(second.status.code(), Some(1));
    let err = String::from_utf8_lossy(&second.stderr);
    assert!(err.contains("another schedr standalone is using"), "{err}");

    let pid = standalone.child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Some(status) = standalone.child.try_wait().unwrap() {
            assert!(status.success(), "{status}\n{}", standalone.log());
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "standalone didn't stop on SIGTERM\n{}",
            standalone.log()
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    // B248: stopped cleanly, it gave the master's lease up, so the next start leads at once
    // instead of waiting it out as a standby.
    let url = format!(
        "sqlite://{}",
        standalone.dir.path().join("data/schedr.db").display()
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let db = schedr_db::Database::connect(&url).await.unwrap();
        let taken = db
            .take_lease("the next start", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            taken.is_some(),
            "the lease is still held\n{}",
            standalone.log()
        );
    });
}

#[test]
fn standalone_ends_when_its_master_is_taken_over() {
    // The master's failure ends the whole process (the pod restarts and waits as a standby),
    // instead of an API and a worker going on with no master.
    let mut standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    standalone.wait("the master to lead", || {
        standalone.log().contains("\"holder\":\"")
    });
    let log = standalone.log();
    let holder = log.split("\"holder\":\"").nth(1).unwrap();
    let holder = holder.split('"').next().unwrap().to_owned();
    let url = format!(
        "sqlite://{}",
        standalone.dir.path().join("data/schedr.db").display()
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let db = schedr_db::Database::connect(&url).await.unwrap();
        db.release_lease(&holder).await.unwrap();
        let taken = db
            .take_lease("intruder", Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(taken.is_some(), "{}", standalone.log());
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = standalone.child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(1), "{}", standalone.log());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "standalone went on without its master\n{}",
            standalone.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        standalone.log().contains("took the lease over"),
        "{}",
        standalone.log()
    );
}

#[test]
fn without_migrate_a_database_not_migrated_is_refused() {
    let dir = TempDir::new().unwrap();
    // Port 0, as every standalone test: the defaults (8080, ...) may be taken on the machine
    // running the tests (B23).
    let config = format!(
        "[interpreters]\npython = \"{}\"\n\n[worker]\ndata_dir = \"{}\"\nlog_port = 0\n\n\
         [master]\ngrpc_port = 0\n\n[webserver]\nport = 0\n\n[ops]\nport = 0\n",
        python(),
        dir.path().join("data").display()
    );
    std::fs::write(dir.path().join("config.toml"), config).unwrap();
    let output = Command::new(BINARY)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", dir.path())
        .arg("--config")
        .arg(dir.path().join("config.toml"))
        .args(["standalone"])
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{err}");
    assert!(
        err.contains(
            "uninitialized: run `schedr db migrate`, or start standalone with `--migrate`"
        ),
        "{err}"
    );
}

#[test]
fn no_ui_serves_only_the_api() {
    // A federation of standalones: one `schedr ui` elsewhere shows them all.
    let standalone = Standalone::start_with(&[("chain.py", CHAIN)], None, &["--no-ui"]);
    assert!(standalone.get("/api/v1/dags").is_some());
    assert_eq!(standalone.page("/"), None);
    assert_eq!(standalone.page("/runs"), None);
}

#[test]
fn a_runs_params_reach_python_tasks_and_commands_and_are_shown_masked() {
    let dag = r#"
from dataclasses import dataclass
from datetime import date
from typing import Literal
from schedr import CommandOperator, Dag, dag_ctx, task

@dataclass
class ReportParams:
    name: str
    region: Literal["eu", "us"] = "eu"
    day: date | None = None
    db_password: str = ""
    start: date = date(2026, 1, 1)

@task
def extract(region: str, day: date | None) -> str:
    return f"{region} {day.year if day else None}"

@task
def inside() -> str:
    p = dag_ctx.params(ReportParams)
    return f"{p.name} {dag_ctx.params.region} {len(p.db_password)}"

with Dag("report", params=ReportParams):
    extract(region=dag_ctx.params.region, day=dag_ctx.params.day)
    inside()
    CommandOperator("publish", ["echo", t"{dag_ctx.params.start:%Y/%m}", t"{dag_ctx.params.name}"])
"#;
    let standalone = Standalone::start(&[("report.py", dag)], None);
    standalone.wait("DAG `report`", || standalone.dag_ids() == ["report"]);
    let command = standalone.cli_json(&[
        "runs",
        "trigger",
        "report",
        "-p",
        "name=q3",
        "-p",
        "region=us",
        "-p",
        "day=2026-10-01",
        "-p",
        "db_password=hunter2",
        "-p",
        "start=2026-10-01",
    ]);
    let run_id = command["result"]["run_id"].as_str().unwrap().to_owned();
    let run = standalone.wait_for_state(&run_id, "success");
    // Shown masked (decision 134): a field named by a secret word.
    assert_eq!(
        run["params"],
        json!({"name": "q3", "region": "us", "day": "2026-10-01", "db_password": "****",
               "start": "2026-10-01"})
    );
    let output = |task: &str| {
        standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap()["value"]
            .clone()
    };
    // A typed argument (a date stays a date), the class read in the task, a t-string.
    assert_eq!(output("extract"), "us 2026");
    assert_eq!(output("inside"), "q3 us 7");
    assert_eq!(output("publish")["stdout"], "2026/10 q3\n");
    let (code, out, err) = standalone.cli(&["runs", "show", &run_id]);
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains("params: day=2026-10-01 db_password=**** name=q3 region=us start=2026-10-01"),
        "{out}"
    );
    // The defaults fill in what a trigger leaves out; `--params` takes one object.
    let command =
        standalone.cli_json(&["runs", "trigger", "report", "--params", r#"{"name": "x"}"#]);
    let run_id = command["result"]["run_id"].as_str().unwrap().to_owned();
    let run = standalone.wait_for_state(&run_id, "success");
    assert_eq!(
        run["params"],
        json!({"name": "x", "region": "eu", "day": null, "db_password": "",
               "start": "2026-01-01"})
    );
    // Refused before a run exists, with the master's reason.
    for (given, reason) in [
        (vec!["-p", "name=x", "-p", "region=asia"], "`region`: "),
        (
            vec!["-p", "name=x", "-p", "nope=1"],
            "unknown parameter `nope`",
        ),
        (vec!["-p", "region=us"], "`name` is required"),
        (vec!["-p", "name=x", "-p", "day=someday"], "`day`: "),
        (
            vec!["-p", "name=x", "-p", "db_password:=5"],
            "`db_password`: doesn't fit its schema (the value isn't shown)",
        ),
    ] {
        let mut args = vec!["runs", "trigger", "report"];
        args.extend(given);
        let (code, _, err) = standalone.cli(&args);
        assert_eq!(code, 1, "{args:?}: {err}");
        assert!(err.starts_with("error: rejected: DAG `report`: "), "{err}");
        assert!(err.contains(reason), "{args:?}: {err}");
    }
    let (code, _, err) = standalone.cli(&["runs", "trigger", "report", "-p", "name"]);
    assert_eq!(code, 1);
    assert!(err.contains("give `name=text` or `name:=json`"), "{err}");
}

/// A DAG whose command, script and Python task read the run's context (`@SCHEDR@`: the
/// binary's path, quoted).
const CTX_DAG: &str = r#"
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from schedr import CommandOperator, Dag, task

SCHEDR = @SCHEDR@

@dataclass
class P:
    name: str
    day: date = date(2026, 1, 1)
    window: timedelta = timedelta(hours=1, minutes=30)
    pair: tuple[int, str] = (1, "a")

SCRIPT = (
    "from dataclasses import dataclass\n"
    "from datetime import date, timedelta\n"
    "from schedr.script import dag_ctx\n"
    "@dataclass\n"
    "class P:\n"
    "    name: str\n"
    "    day: date\n"
    "    window: timedelta\n"
    "    pair: tuple[int, str]\n"
    "p = dag_ctx.params\n"
    "print(type(p.day).__name__, p.day, type(p.window).__name__, p.window, p.pair, p.name)\n"
    "print(dag_ctx.run.id, dag_ctx.run.kind, dag_ctx.run.planned_for.isoformat(), dag_ctx.dag_id)\n"
    "print(dag_ctx.params(P).day.year)\n"
)
CHILD = (
    "import subprocess, sys\n"
    "r = subprocess.run([sys.argv[1], 'dag-ctx', 'params', 'get'], capture_output=True, text=True)\n"
    "print(r.returncode, r.stderr.strip())\n"
)

@task
def python_child() -> str:
    r = subprocess.run([SCHEDR, "dag-ctx", "run", "get"], capture_output=True, text=True)
    return f"{r.returncode} {r.stderr.strip()}"

with Dag("ctx", params=P):
    CommandOperator("cli", ["sh", "-c",
        f"{SCHEDR} dag-ctx params get && {SCHEDR} dag-ctx run get"
        f" && {SCHEDR} dag-ctx run get --field id && {SCHEDR} dag-ctx params get --field day"])
    CommandOperator("script", [sys.executable, "-c", SCRIPT])
    CommandOperator("child", [sys.executable, "-c", CHILD, SCHEDR])
    python_child()
"#;

#[test]
fn a_command_and_a_plain_script_read_the_same_params_and_run() {
    // A command (`schedr dag-ctx`), a Python script it runs (`schedr.script`), a Python script
    // starting `schedr dag-ctx` without `pass_fds`, and a Python task's subprocess (no channel).
    let dag = CTX_DAG.replace("@SCHEDR@", &format!("{BINARY:?}"));
    let standalone = Standalone::start(&[("ctx.py", &dag)], None);
    standalone.wait("DAG `ctx`", || standalone.dag_ids() == ["ctx"]);
    let command = standalone.cli_json(&[
        "runs",
        "trigger",
        "ctx",
        "-p",
        "name=q3",
        "-p",
        "day=2026-10-01",
    ]);
    let run_id = command["result"]["run_id"].as_str().unwrap().to_owned();
    let run = standalone.wait_for_state(&run_id, "success");
    let planned_for = run["run"]["planned_for"].as_str().unwrap().to_owned();
    let stdout = |task: &str| {
        standalone
            .get(&format!("/api/v1/runs/{run_id}/tasks/{task}/output"))
            .unwrap()["value"]["stdout"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let cli = stdout("cli");
    let lines: Vec<&str> = cli.lines().collect();
    assert_eq!(lines.len(), 4, "{cli}");
    assert_eq!(
        serde_json::from_str::<Value>(lines[0]).unwrap(),
        json!({"name": "q3", "day": "2026-10-01", "window": "P0DT5400.000000S",
               "pair": [1, "a"]})
    );
    let run_info: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(run_info["id"], run_id.as_str());
    assert_eq!(run_info["dag_id"], "ctx");
    assert_eq!(run_info["kind"], "manual");
    assert_eq!(lines[2], run_id);
    assert_eq!(lines[3], "2026-10-01");
    // The script reads the same values, decoded as in a task.
    let script = stdout("script");
    let script: Vec<&str> = script.lines().collect();
    assert_eq!(script[0], "date 2026-10-01 timedelta 1:30:00 (1, 'a') q3");
    let planned: jiff::Timestamp = planned_for.parse().unwrap();
    let said: jiff::Timestamp = script[1].split(' ').nth(2).unwrap().parse().unwrap();
    assert_eq!(said, planned);
    assert_eq!(
        script[1].split(' ').collect::<Vec<_>>()[..2],
        [run_id.as_str(), "manual"]
    );
    assert!(script[1].ends_with(" ctx"), "{}", script[1]);
    assert_eq!(script[2], "2026");
    // Python's subprocess closes the descriptor: the message says how to pass it.
    let child = stdout("child");
    assert!(child.starts_with("1 error: "), "{child}");
    assert!(child.contains("pass_fds=["), "{child}");
    // A Python task's processes have no channel.
    let output = standalone
        .get(&format!("/api/v1/runs/{run_id}/tasks/python_child/output"))
        .unwrap()["value"]
        .clone();
    let output = output.as_str().unwrap();
    assert!(
        output.starts_with("1 error: `schedr dag-ctx` must be run inside a schedr task"),
        "{output}"
    );
}

/// Connections of the test Postgres, its password a secret; `api`'s token never stored.
fn connections_file() -> String {
    let admin = std::env::var("SCHEDR_TEST_POSTGRES_URL").expect(
        "SCHEDR_TEST_POSTGRES_URL: an administrative Postgres URL (`make test-integration` sets it)",
    );
    let (userinfo, place) = admin
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://")
        .split_once('@')
        .expect("a URL with a user");
    let user = userinfo.split(':').next().unwrap_or_default();
    format!(
        r#"
from schedr import secrets
from schedr.connections import SQL, Generic

warehouse = SQL(
    "warehouse",
    url="postgresql://{place}?sslmode=disable",
    user="{user}",
    password=secrets["etl/db"]["password"],
)
api = Generic("api", token=secrets["etl/api"]["token"])
region = Generic("region", name="eu-west")
"#
    )
}

/// The password the test Postgres takes.
fn postgres_password() -> String {
    let admin = std::env::var("SCHEDR_TEST_POSTGRES_URL").unwrap_or_default();
    let userinfo = admin.split_once("://").unwrap_or_default().1;
    let userinfo = userinfo.split_once('@').unwrap_or_default().0;
    userinfo.split_once(':').unwrap_or_default().1.to_owned()
}

#[test]
fn connections_are_checked_and_tested_on_a_worker() {
    let standalone = Standalone::start(&[("conns.py", &connections_file())], None);
    let (code, out, err) = standalone.secrets(None, &["set", "etl/db", "password=wrong-pw-123"]);
    assert_eq!(code, 0, "{out}{err}");
    // Once parsed: the root's connections known to the master.
    let mut last = (0, String::new(), String::new());
    standalone.wait("the connections to be known", || {
        last = standalone.cli(&["connections", "check", "--json"]);
        !last.2.contains("no root defines connections") && !last.2.contains("no parsed code")
    });
    let (code, out, err) = last;
    assert_eq!(code, 1, "a missing secret fails the check: {out}{err}");
    let result: Value = serde_json::from_str(&out).unwrap();
    let job = &result["jobs"][0];
    assert_eq!(
        (job["root"].as_str(), job["state"].as_str()),
        (Some("etl"), Some("done"))
    );
    let reports: Vec<(String, bool, Vec<String>)> = job["connections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            let secrets = r["secrets"]
                .as_array()
                .map(|s| {
                    s.iter()
                        .map(|s| {
                            format!(
                                "{} {}",
                                s["secret"].as_str().unwrap(),
                                s["state"].as_str().unwrap()
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            (
                r["name"].as_str().unwrap().to_owned(),
                r["ok"].as_bool().unwrap(),
                secrets,
            )
        })
        .collect();
    assert_eq!(
        reports,
        [
            (
                "api".to_owned(),
                false,
                vec!["secrets['etl/api']['token'] missing".to_owned()]
            ),
            ("region".to_owned(), true, vec![]),
            (
                "warehouse".to_owned(),
                true,
                vec!["secrets['etl/db']['password'] present".to_owned()]
            ),
        ]
    );
    assert!(!out.contains("wrong-pw-123"), "{out}");
    // The table names what is missing.
    let (code, table, _) = standalone.cli(&["connections", "check", "etl", "--name", "api"]);
    assert_eq!(code, 1);
    assert!(
        table.contains("api: secret `secrets['etl/api']['token']` missing"),
        "{table}"
    );

    // A wrong password fails the test, its message masked; the right one passes.
    let (code, out, err) =
        standalone.cli(&["connections", "test", "warehouse", "--timeout", "30s"]);
    assert_eq!(code, 1, "{out}{err}\n{}", standalone.log());
    assert!(out.contains("password authentication failed"), "{out}");
    assert!(!out.contains("wrong-pw-123"), "{out}");
    let right = format!("password={}", postgres_password());
    let (code, out, err) = standalone.secrets(None, &["set", "etl/db", &right]);
    assert_eq!(code, 0, "{out}{err}");
    let (code, out, err) = standalone.cli(&["connections", "test", "warehouse"]);
    assert_eq!(code, 0, "{out}{err}\n{}", standalone.log());
    assert!(out.contains("warehouse") && out.contains("ok ("), "{out}");
    // Without a check of its own, opened and closed (decision 198).
    let (code, out, err) = standalone.cli(&["connections", "test", "region"]);
    assert_eq!(code, 0, "{out}{err}");
    // An unknown one is refused by the master.
    let (code, _, err) = standalone.cli(&["connections", "test", "nope"]);
    assert_eq!(code, 1);
    assert!(err.contains("no root defines connection `nope`"), "{err}");
}

const CLEANUPS: &str = r#"
import time
from datetime import timedelta

from schedr import Dag, task

def slow_and_failing() -> None:
    time.sleep(1.5)
    raise ValueError("can't clean")

@task(cleanup=slow_and_failing, cleanup_retries=1, cleanup_retry_delay=timedelta(0))
def works() -> int:
    return 1

with Dag("cleanups"):
    works()
"#;

#[test]
fn on_postgres_a_cleanup_shows_cleaning_up_and_its_failure_fails_the_run() {
    let url = postgres_database();
    let standalone = Standalone::start(&[("cleanups.py", CLEANUPS)], Some(&url));
    let query = |sql: &str| {
        let out = Command::new("psql")
            .args([url.as_str(), "-tAc", sql])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };
    standalone.wait("DAG `cleanups`", || standalone.dag_ids() == ["cleanups"]);
    let run_id = standalone.trigger("cleanups");
    standalone.wait("the instance in its cleanup", || {
        query("SELECT state FROM task_instances") == "cleaning_up"
    });
    standalone.wait_for_state(&run_id, "failed");
    assert_eq!(
        query("SELECT state, cleanup_result FROM task_instances"),
        "success|failed"
    );
    assert_eq!(
        query(
            "SELECT status, cleanup_result, cleanup_tries, cleanup_error_kind IS NULL FROM attempts"
        ),
        "success|failed|2|t"
    );
    assert!(
        query("SELECT cleanup_error_summary FROM attempts").contains("can't clean"),
        "the cleanup's error is stored"
    );
}

/// A DAG reading a flag of a library in the other root, into its description.
const USES_SHARED: &str = r#"from flags_lib import FLAG

from schedr import Dag, task


@task
def uses() -> str:
    import flags_lib

    return flags_lib.FLAG


with Dag("uses_shared", description=FLAG):
    uses()
"#;

fn description(standalone: &Standalone, dag: &str) -> Option<String> {
    standalone.dags().into_iter().find_map(|(id, value)| {
        (id == dag).then(|| value["description"].as_str().unwrap_or_default().to_owned())
    })
}

fn other_root_library_is_used_by_tasks_and_a_change_reparses(
    more_config: &str,
    env: &[(&str, &str)],
) {
    let standalone = Standalone::start_configured(
        &[
            ("uses.py", USES_SHARED),
            ("../shared/flags_lib.py", "FLAG = \"one\"\n# schedr\n"),
        ],
        None,
        &[],
        more_config,
        env,
    );
    standalone.wait("the DAG", || {
        description(&standalone, "uses_shared").as_deref() == Some("one")
    });
    // B174: the task finds the other root's module however it starts.
    let run = standalone.trigger("uses_shared");
    standalone.wait_for_state(&run, "success");
    // B173: a change in the other root alone reparses the DAG that imports it.
    let library = standalone.dir.path().join("dags/shared/flags_lib.py");
    std::fs::write(library, "FLAG = \"two\"\n# schedr\n").unwrap();
    standalone.wait("the new library in the DAG", || {
        description(&standalone, "uses_shared").as_deref() == Some("two")
    });
}

#[test]
fn a_library_in_another_root_is_used_and_reparses_spawned() {
    other_root_library_is_used_by_tasks_and_a_change_reparses("", &[]);
}

#[test]
fn a_library_in_another_root_is_used_and_reparses_forked() {
    other_root_library_is_used_by_tasks_and_a_change_reparses(
        &format!(
            "\n[interpreters.forked]\npath = \"{}\"\nfork_server = true\npreload = [\"wave\"]\n",
            python()
        ),
        &[("SCHEDR_CONFIG__DAGS__INTERPRETER", "forked")],
    );
}

const POLLS: &str = r#"from datetime import timedelta
from pathlib import Path

from schedr import NOT_YET, Dag, NotYet, sensor, task

READY = "READY_FILE"
CODE = "old"


# Calls running past `every` fail the poke (D458): 100 ms failed the run under a loaded check (B238).
@sensor.poke(every=timedelta(seconds=1), max_wait=timedelta(seconds=60))
async def polls() -> str | NotYet:
    Path(READY + ".called").write_text("yes")
    return CODE if Path(READY).exists() else NOT_YET


@task
def after(seen: str) -> str:
    return seen


with Dag("moves"):
    after(polls().on(str))
"#;

fn poke_on_changing_code(
    new_code: impl FnOnce(&str) -> String,
    log_says: &str,
) -> (Standalone, String) {
    let ready_dir = TempDir::new().unwrap();
    let ready = ready_dir.path().join("ready");
    // `ready.called` is `READY + ".called"`.
    let old = POLLS.replace("READY_FILE", ready.to_str().unwrap());
    let standalone = Standalone::start(&[("moves.py", &old)], None);
    standalone.wait("the DAG", || {
        standalone.dag_ids().contains(&"moves".to_owned())
    });
    let run = standalone.trigger("moves");
    // Its first call made: what it is told about new code it hears on its old code.
    standalone.wait("the poke to run", || {
        ready.with_extension("called").exists()
    });
    std::fs::write(standalone.root().join("moves.py"), new_code(&old)).unwrap();
    // Until the log says it, or the run ends without it: then the run says why (B238).
    let mut ended = Value::Null;
    standalone.wait("the move to be made or refused", || {
        let state = standalone.run(&run);
        if ["success", "failed", "cancelled"]
            .contains(&state["run"]["state"].as_str().unwrap_or(""))
        {
            ended = state;
            return true;
        }
        poke_log(&standalone, &run, "polls").contains(log_says)
    });
    assert!(
        ended.is_null(),
        "the run ended before {log_says:?}: {ended}"
    );
    std::fs::write(&ready, "").unwrap();
    standalone.wait_for_state(&run, "success");
    (standalone, run)
}

fn poke_log(standalone: &Standalone, run: &str, task: &str) -> String {
    let (_, out, _) = standalone.cli(&["tasks", "logs", run, task]);
    out
}

#[test]
fn a_running_poke_moves_to_new_code_on_a_real_worker() {
    let (standalone, run) = poke_on_changing_code(
        |old| old.replace("CODE = \"old\"", "CODE = \"new\""),
        "moved to new code",
    );
    // The poke's value and its task after it ran on the new code: only it is recorded.
    let recorded = standalone.run(&run)["code_hashes"].clone();
    assert_eq!(recorded.as_array().unwrap().len(), 1, "{recorded}");
    let hash = recorded[0].as_str().unwrap();
    let folder = hash.replace(':', "-");
    let copies = standalone.dir.path().join("data/code");
    let found = walk_for(&copies, &folder).expect("the copy it names");
    assert!(
        std::fs::read_to_string(found.join("moves.py"))
            .unwrap()
            .contains("CODE = \"new\""),
        "{hash}"
    );
}

#[test]
fn a_poke_renamed_in_new_code_finishes_its_run_on_a_real_worker() {
    // The user's example: a run polling `polls` while new code calls it `renamed`.
    poke_on_changing_code(
        |old| old.replace("polls", "renamed"),
        "it stays on its code",
    );
}

fn resources_have_their_own_endpoints(standalone: &Standalone, run_id: &str, run: &Value) {
    // Resources have their own endpoints (decision 340), as the run's detail shows them.
    let listed = standalone
        .get(&format!("/api/v1/runs/{run_id}/resources"))
        .unwrap();
    assert_eq!(listed["resources"], run["resources"], "{listed}");
    let name = listed["resources"][0]["setup"].as_str().unwrap();
    let one = standalone
        .get(&format!("/api/v1/runs/{run_id}/resources/{name}"))
        .unwrap();
    assert_eq!(one, listed["resources"][0]);
    assert!(
        standalone
            .get(&format!("/api/v1/runs/{run_id}/resources/nothing"))
            .is_none()
    );
    let (code, out, err) = standalone.cli(&["resources", "list", run_id]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains(name), "{out}");
}

const BLOCKS: &str = r#"import time

from schedr import Dag, sensor


@sensor
async def blocks() -> int:
    time.sleep(600)
    return 1


with Dag("blocking"):
    blocks()
"#;

fn run_blocks_with_kind(standalone: &Standalone, kind: &str) {
    let run = standalone.trigger("blocking");
    standalone.wait_for_state(&run, "failed");
    let attempts: Value = standalone
        .get(&format!("/api/v1/runs/{run}/tasks/blocks/attempts"))
        .unwrap();
    assert_eq!(attempts["attempts"][0]["error_kind"], kind, "{attempts}");
    assert_eq!(
        attempts["attempts"].as_array().unwrap().len(),
        1,
        "{attempts}"
    );
}

#[test]
fn a_wait_step_that_keeps_blocking_is_switched_off_until_new_code_or_by_hand() {
    keeps_blocking(None);
}

#[test]
fn a_wait_step_that_keeps_blocking_is_switched_off_on_postgres() {
    keeps_blocking(Some(&postgres_database()));
}

fn keeps_blocking(url: Option<&str>) {
    let config = "[watchers]\nblock_limit = \"1s\"\nswitch_off_after = 2\n";
    let standalone = Standalone::start_configured(&[("blocks.py", BLOCKS)], url, &[], config, &[]);
    standalone.wait("the DAG", || {
        standalone.dag_ids().contains(&"blocking".to_owned())
    });
    run_blocks_with_kind(&standalone, "blocked_event_loop");
    run_blocks_with_kind(&standalone, "blocked_event_loop");
    run_blocks_with_kind(&standalone, "switched_off");
    let off = standalone.cli_json(&["sensors", "switched-off"]);
    assert_eq!(off["sensors"][0]["sensor"], "blocks", "{off}");
    for (dag, sensor) in [("blocking", "nothing"), ("nothing", "blocks")] {
        let (code, _, err) = standalone.cli(&["sensors", "switch-on", dag, sensor]);
        assert_ne!(code, 0, "{dag} {sensor}");
        assert!(err.contains("no "), "{err}");
    }
    // By hand: the full count again.
    let (code, out, err) = standalone.cli(&["sensors", "switch-on", "blocking", "blocks"]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("switched on"), "{out}");
    let off = standalone.cli_json(&["sensors", "switched-off"]);
    assert_eq!(off["sensors"], json!([]), "{off}");
    run_blocks_with_kind(&standalone, "blocked_event_loop");
    run_blocks_with_kind(&standalone, "blocked_event_loop");
    run_blocks_with_kind(&standalone, "switched_off");
    // New code: one chance.
    std::fs::write(
        standalone.root().join("blocks.py"),
        format!("{BLOCKS}\n# new code\n"),
    )
    .unwrap();
    standalone.wait("it to be on again on new code", || {
        let run = standalone.trigger("blocking");
        standalone.wait_for_state(&run, "failed");
        let attempts: Value = standalone
            .get(&format!("/api/v1/runs/{run}/tasks/blocks/attempts"))
            .unwrap();
        attempts["attempts"][0]["error_kind"] == "blocked_event_loop"
    });
    run_blocks_with_kind(&standalone, "switched_off");
}

#[test]
fn a_drain_moves_a_poke_using_no_retry_and_it_keeps_its_deadline() {
    let ready_dir = TempDir::new().unwrap();
    let ready = ready_dir.path().join("ready");
    let source = POLLS.replace("READY_FILE", ready.to_str().unwrap());
    let standalone = Standalone::start(&[("moves.py", &source)], None);
    standalone.wait("the DAG", || {
        standalone.dag_ids().contains(&"moves".to_owned())
    });
    let run = standalone.trigger("moves");
    standalone.wait("the poke to run", || {
        ready.with_extension("called").exists()
    });
    let workers = standalone.cli_json(&["workers", "list"]);
    let worker = workers["workers"][0]["worker_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (code, out, err) = standalone.cli(&["workers", "drain", &worker]);
    assert_eq!(code, 0, "{out}{err}");
    let attempts = |standalone: &Standalone| -> Value {
        standalone
            .get(&format!("/api/v1/runs/{run}/tasks/polls/attempts"))
            .unwrap()
    };
    standalone.wait("the poke to be interrupted", || {
        attempts(&standalone)["attempts"][0]["status"] == "interrupted"
    });
    let (code, out, err) = standalone.cli(&["workers", "resume", &worker]);
    assert_eq!(code, 0, "{out}{err}");
    standalone.wait("it to run again", || {
        attempts(&standalone)["attempts"].as_array().unwrap().len() == 2
    });
    std::fs::write(&ready, "").unwrap();
    standalone.wait_for_state(&run, "success");
}

/// A folder of this name somewhere under `dir`.
fn walk_for(dir: &Path, name: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if entry.file_name() == name {
                return Some(path);
            }
            if let Some(found) = walk_for(&path, name) {
                return Some(found);
            }
        }
    }
    None
}

/// M4.11b: a trigger runs while its DAG is unpaused, each new key starts one run with its event, a
/// repeat none, and it starts again from the last key acted on.
#[test]
fn a_trigger_starts_a_run_per_new_key_and_goes_on_from_its_bookmark() {
    const DAG: &str = r#"
import asyncio
from collections.abc import AsyncIterator

from schedr import Dag, Event, dag_ctx, task, trigger


@trigger
async def found(after: str | None, first: int) -> AsyncIterator[Event[int]]:
    if after is None:
        yield Event("a", first)
        yield Event("b", first + 1)
        yield Event("a", 99)
    else:
        yield Event(f"after-{after}", first + 2)
    await asyncio.sleep(3600)


@task
def seen(key: str, data: int) -> str:
    print(f"seen {key} {data}")
    return key


with Dag("ingest", trigger=found(first=10)):
    seen(dag_ctx.event.key, dag_ctx.event.data)
"#;
    let standalone = Standalone::start(&[("ingest.py", DAG)], None);
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"ingest".to_owned())
    });
    assert_eq!(
        standalone.runs("ingest").len(),
        0,
        "paused, it isn't running"
    );
    let (code, _, err) = standalone.cli(&["dags", "unpause", "ingest"]);
    assert_eq!(code, 0, "{err}");
    let succeeded = |count: usize| {
        standalone.runs("ingest").len() == count
            && standalone
                .runs("ingest")
                .iter()
                .all(|run| run["state"] == "success")
    };
    standalone.wait("a run per new key", || succeeded(2));
    let hosts = || {
        let workers = standalone.cli_json(&["workers", "list"]);
        workers["workers"][0]["watcher_hosts"]
            .as_array()
            .map_or(0, Vec::len)
    };
    standalone.wait("it to take a watcher host", || hosts() == 1);
    let (code, _, err) = standalone.cli(&["dags", "pause", "ingest"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("it to stop", || hosts() == 0);
    let (code, _, err) = standalone.cli(&["dags", "unpause", "ingest"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("a run from its bookmark", || succeeded(3));
    let runs = standalone.runs("ingest");
    assert!(runs.iter().all(|run| run["kind"] == "event"), "{runs:?}");
    let logs = logs(standalone.dir.path());
    assert!(logs.contains("seen a 10"), "{logs}");
    assert!(logs.contains("seen b 11"), "{logs}");
    assert!(logs.contains("seen after-a 12"), "{logs}");
    assert!(
        !logs.contains("seen a 99"),
        "a repeat starts nothing: {logs}"
    );
}

/// M5.2h2: a trigger with more events ready than `in_flight` goes on as each is stored: every
/// one starts its run, in order, its bookmark the last.
#[test]
fn a_trigger_with_more_events_than_in_flight_gets_them_all_stored() {
    const DAG: &str = r#"
import asyncio
from collections.abc import AsyncIterator

from schedr import Dag, Event, dag_ctx, task, trigger


@trigger(in_flight=2)
async def backlog(after: str | None) -> AsyncIterator[Event[int]]:
    for n in range(8):
        yield Event(f"e{n}", n)
    await asyncio.sleep(3600)


@task
def seen(key: str) -> str:
    print(f"seen {key}")
    return key


with Dag("backlog", trigger=backlog()):
    seen(dag_ctx.event.key)
"#;
    let standalone = Standalone::start(&[("backlog.py", DAG)], None);
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"backlog".to_owned())
    });
    let (code, _, err) = standalone.cli(&["dags", "unpause", "backlog"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("a run for each of the 8 events, done", || {
        let runs = standalone.runs("backlog");
        runs.len() == 8 && runs.iter().all(|run| run["state"] == "success")
    });
    let logs = logs(standalone.dir.path());
    for n in 0..8 {
        assert!(logs.contains(&format!("seen e{n}\n")), "e{n}: {logs}");
    }
}

/// M4.11b: a poke that raises keeps its bookmark and goes on, a generator that returned stays
/// stopped, and one that keeps blocking is switched off.
#[test]
fn a_poke_goes_on_after_raising_and_a_generator_that_returned_stays_stopped() {
    const DAGS: &str = r#"
import asyncio
import time
from collections.abc import AsyncIterator
from datetime import timedelta

from schedr import Dag, Event, dag_ctx, task, trigger


FAILED: set[str] = set()


@trigger.poke(every=timedelta(milliseconds=100))
async def polled_found(after: str | None) -> list[Event[int]]:
    count = 0 if after is None else int(after)
    if count == 2 and not FAILED:
        FAILED.add("yes")
        raise ValueError("no more for now")
    return [Event(str(count + 1), count)] if count < 3 else []


@trigger
async def once_found(after: str | None) -> AsyncIterator[Event[int]]:
    yield Event(f"after-{after}", 1)


@trigger
async def blocks(after: str | None) -> AsyncIterator[Event[int]]:
    time.sleep(600)
    yield Event("never", 1)


@task
def seen(key: str) -> str:
    return key


with Dag("polled", trigger=polled_found()):
    seen(dag_ctx.event.key)

with Dag("once", trigger=once_found()):
    seen(dag_ctx.event.key)

with Dag("blocking", trigger=blocks()):
    seen(dag_ctx.event.key)
"#;
    let config = "[watchers]\nblock_limit = \"1s\"\nswitch_off_after = 2\n";
    let standalone = Standalone::start_configured(
        &[("polled.py", DAGS)],
        None,
        &[],
        config,
        // Quick after a failure, so what doesn't start again does so for another reason.
        &[("SCHEDR_HIDDEN__MASTER__TRIGGER_RESTART_BACKOFF", "50ms")],
    );
    standalone.wait("the DAGs to be found", || standalone.dag_ids().len() == 3);
    // One host for the file: the one that blocks kills it for the others.
    for dag in ["polled", "once"] {
        let (code, _, err) = standalone.cli(&["dags", "unpause", dag]);
        assert_eq!(code, 0, "{err}");
    }
    standalone.wait("a run for each key, past the raise", || {
        standalone.runs("polled").len() == 3
    });
    standalone.wait("the generator's event", || {
        standalone.runs("once").len() == 1
    });
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(
        standalone.runs("once").len(),
        1,
        "started again it would say `after-after-None`"
    );
    let (code, _, err) = standalone.cli(&["dags", "unpause", "blocking"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("the blocking trigger to be switched off", || {
        standalone.cli_json(&["sensors", "switched-off"])["sensors"]
            .as_array()
            .is_some_and(|off| off.iter().any(|s| s["sensor"] == "blocks"))
    });
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(standalone.runs("blocking").len(), 0);
    let off = standalone.cli_json(&["sensors", "switched-off"]);
    assert_eq!(off["sensors"][0]["sensor"], "blocks", "it stays off: {off}");
}

/// M4.11b: a trigger reads the secrets it declares, and what it reads doesn't reach an event.
#[test]
fn a_trigger_reads_its_secrets_and_an_event_keeps_none() {
    const DAG: &str = r#"
import asyncio
from collections.abc import AsyncIterator

from schedr import Dag, Event, dag_ctx, task, task_ctx, trigger


@trigger(secrets=["etl/db#password"])
async def found(after: str | None) -> AsyncIterator[Event[str]]:
    yield Event("one", f"has {task_ctx.secret('etl/db', 'password')} in it")
    await asyncio.sleep(3600)


@task
def seen(data: str) -> str:
    print(f"seen data: {data}")
    return data


with Dag("guarded", trigger=found()):
    seen(dag_ctx.event.data)
"#;
    let standalone = Standalone::start(&[("guarded.py", DAG)], None);
    let (code, out, err) = standalone.secrets(None, &["set", "etl/db", "password=hunter22-pw"]);
    assert_eq!(code, 0, "{out}{err}");
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"guarded".to_owned())
    });
    let (code, _, err) = standalone.cli(&["dags", "unpause", "guarded"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("its run", || {
        standalone
            .runs("guarded")
            .first()
            .is_some_and(|run| run["state"] == "success")
    });
    let logs = logs(standalone.dir.path());
    assert!(logs.contains("seen data: has **** in it"), "{logs}");
    assert!(!logs.contains("hunter22-pw"), "{logs}");
}

/// M4.11b: a trigger's status and events, from the CLI and the API; a manual run brings an event,
/// and writes none; a restart starts it from its bookmark; the DAG's page shows it.
#[test]
fn a_trigger_is_seen_restarted_and_run_again_by_hand() {
    const DAG: &str = r#"
import asyncio
from collections.abc import AsyncIterator

from schedr import Dag, Event, dag_ctx, task, trigger


@trigger
async def found(after: str | None) -> AsyncIterator[Event[int]]:
    yield Event("after-" + str(after), 1)
    await asyncio.sleep(3600)


@task
def seen(key: str, data: int) -> str:
    print(f"seen {key} {data}")
    return key


with Dag("ingest", trigger=found()):
    seen(dag_ctx.event.key, dag_ctx.event.data)
"#;
    let standalone = Standalone::start(&[("ingest.py", DAG)], None);
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"ingest".to_owned())
    });
    let shown = standalone.cli_json(&["triggers", "show", "ingest"]);
    assert_eq!(shown["state"], "paused", "{shown}");
    let (code, _, err) = standalone.cli(&["runs", "trigger", "ingest"]);
    assert_ne!(code, 0);
    assert!(err.contains("needs an event"), "{err}");
    let (code, _, err) = standalone.cli(&["dags", "unpause", "ingest"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("its first event", || standalone.runs("ingest").len() == 1);
    let list = standalone.cli_json(&["triggers", "list"]);
    assert_eq!(list["triggers"][0]["trigger"], "found", "{list}");
    assert_eq!(list["triggers"][0]["state"], "running", "{list}");
    assert_eq!(list["triggers"][0]["bookmark"], "after-None", "{list}");
    let events = standalone.cli_json(&["triggers", "events", "ingest"]);
    assert_eq!(events["events"][0]["key"], "after-None", "{events}");
    assert_eq!(events["events"][0]["data"], 1, "{events}");

    // By hand: one stored, one given; neither is written as an event.
    let (code, out, err) = standalone.cli(&["runs", "trigger", "ingest", "--event", "after-None"]);
    assert_eq!(code, 0, "{out}{err}");
    let (code, out, err) = standalone.cli(&[
        "runs",
        "trigger",
        "ingest",
        "--event-key",
        "by-hand",
        "--event-data",
        "7",
    ]);
    assert_eq!(code, 0, "{out}{err}");
    let (code, _, err) = standalone.cli(&["runs", "trigger", "ingest", "--event", "nothing"]);
    assert_ne!(code, 0);
    assert!(err.contains("no event `nothing`"), "{err}");
    standalone.wait("the manual runs", || {
        let runs = standalone.runs("ingest");
        runs.len() == 3 && runs.iter().all(|run| run["state"] == "success")
    });
    let logs_now = logs(standalone.dir.path());
    assert!(logs_now.contains("seen by-hand 7"), "{logs_now}");
    assert_eq!(
        logs_now.matches("seen after-None 1").count(),
        2,
        "{logs_now}"
    );
    let events = standalone.cli_json(&["triggers", "events", "ingest"]);
    assert_eq!(events["events"].as_array().unwrap().len(), 1, "{events}");

    // Started again, from where it is.
    let (code, _, err) = standalone.cli(&["triggers", "restart", "ingest"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("a run from its bookmark", || {
        standalone.runs("ingest").len() == 4
    });
    let events = standalone.cli_json(&["triggers", "events", "ingest"]);
    assert_eq!(events["events"][0]["key"], "after-after-None", "{events}");

    let list = standalone.page("/").expect("the UI's DAG list");
    let href = list
        .split('"')
        .find(|part| part.ends_with("/dags/ingest"))
        .unwrap_or_else(|| panic!("no link to the DAG: {list}"))
        .to_owned();
    let page = standalone.page(&href).unwrap_or_default();
    assert!(page.contains("@trigger found"), "{page}");
    assert!(page.contains("after-after-None"), "{page}");
    assert!(page.contains("Run again"), "{page}");
    let (code, _, err) = standalone.cli(&["triggers", "restart", "nothing"]);
    assert_ne!(code, 0);
    assert!(err.contains("nothing"), "{err}");
}

/// `report` waits, with an `@sensor(wait_for=...)`, for the `load` of the `orders` run planned
/// for the same moment, and builds on its output (decision 353).
const RUN_OF: &str = r#"
from datetime import timedelta
from schedr import Dag, RunOf, RunResult, sensor, task
from schedr.sensors import NotYet

@task
def load() -> int:
    return 3

@sensor(wait_for=RunOf("orders", task="load", lag=timedelta(0)), max_wait=timedelta(minutes=1))
async def loaded(run: RunResult[int], times: int) -> int | NotYet:
    print("given", run.dag_id, run.task_state, run.output)
    return (run.output or 0) * times

@task
def report(rows: int) -> None:
    print("rows", rows)

with Dag("orders"):
    load()

with Dag("report"):
    report(loaded(times=2).on(int))
"#;

const TRIGGER_RUN_OF: &str = r#"
from datetime import timedelta
from schedr import Dag, Event, RunOf, RunResult, dag_ctx, task, trigger

@task
def load() -> int:
    return 7

@trigger(wait_for=RunOf("orders", task="load", lag=timedelta(hours=1)))
async def on_orders(run: RunResult[int], least: int) -> Event[int] | None:
    print("called for", run.dag_id, run.task_state, run.output)
    if run.output is None or run.output < least:
        return None
    return Event(data=run.output)

@task
def tally(orders: int) -> None:
    print("tally", orders)

with Dag("orders"):
    load()

with Dag("tally", trigger=on_orders(least=5)):
    tally(dag_ctx.event.data)
"#;

/// M4.11d2: an `@trigger(wait_for=...)` is called once for each run of the other DAG that ends
/// while it is live, given that run, and what it returns starts a run.
#[test]
fn a_trigger_waiting_for_another_dags_runs_is_called_for_each_and_starts_runs() {
    let standalone = Standalone::start(&[("trigger_run_of.py", TRIGGER_RUN_OF)], None);
    standalone.wait("DAGs `orders` and `tally`", || {
        standalone.dag_ids() == ["orders", "tally"]
    });
    let (code, _, err) = standalone.cli(&["dags", "unpause", "tally"]);
    assert_eq!(code, 0, "{err}");
    let mut orders = Vec::new();
    for moment in ["2026-10-15T00:00:00Z", "2026-10-16T00:00:00Z"] {
        let command = standalone.cli_json(&["runs", "trigger", "orders", "--for", moment]);
        let run = command["result"]["run_id"].as_str().unwrap().to_owned();
        standalone.wait_for_state(&run, "success");
        orders.push(run);
    }
    let succeeded = || {
        let runs = standalone.runs("tally");
        runs.len() == 2 && runs.iter().all(|run| run["state"] == "success")
    };
    standalone.wait("a run of tally for each of orders", succeeded);
    let mut planned: Vec<String> = standalone
        .runs("tally")
        .iter()
        .map(|run| run["planned_for"].as_str().unwrap().to_owned())
        .collect();
    planned.sort();
    assert_eq!(
        planned,
        ["2026-10-15T01:00:00Z", "2026-10-16T01:00:00Z"],
        "planned for the run's moment plus lag"
    );
    let shown = standalone.cli_json(&["triggers", "show", "tally"]);
    assert!(shown.get("called_for").is_none(), "both passed: {shown}");
    let logs = logs(standalone.dir.path());
    assert_eq!(
        logs.matches("called for orders success 7").count(),
        2,
        "{logs}"
    );
    assert_eq!(logs.matches("tally 7").count(), 2, "{logs}");
}

/// `ship` starts once per order paid, and waits in its run for that order to be packed
/// (decision 354): both events posted with `schedr events post`.
const NAMED_EVENTS: &str = r#"
from schedr import Dag, Event, EventNamed, dag_ctx, task, wait_for
from schedr.types import Json

@task
def ship(order: str, packed: Event[Json]) -> None:
    print("shipped", order, packed.key, packed.data["box"])

with Dag("ship", trigger=EventNamed("paid")):
    ship(dag_ctx.event.key, wait_for(EventNamed("packed", key=dag_ctx.event.key)).on(Event))
"#;

/// M4.11e: an event posted starts a run of the DAG its name triggers, once per key; a step in
/// it waits for another event of its key, and its task reads that event.
#[test]
fn posted_events_start_runs_and_end_the_waits_for_them() {
    let standalone = Standalone::start(&[("ship.py", NAMED_EVENTS)], None);
    standalone.wait("DAG `ship`", || standalone.dag_ids() == ["ship"]);
    let (code, _, err) = standalone.cli(&["dags", "unpause", "ship"]);
    assert_eq!(code, 0, "{err}");
    let post = |name: &str, key: &str, data: &str| {
        let event = standalone.cli_json(&["events", "post", name, "--key", key, "--data", data]);
        assert_eq!(event["name"], name, "{event}");
    };
    post("paid", "o-1", r#"{"amount": 40}"#);
    post("paid", "o-1", r#"{"amount": 40}"#);
    standalone.wait("a run of ship", || standalone.runs("ship").len() == 1);
    // Another order's packing doesn't end the wait.
    post("packed", "o-2", r#"{"box": 2}"#);
    post("packed", "o-1", r#"{"box": 3}"#);
    let run = standalone.runs("ship")[0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    standalone.wait_for_state(&run, "success");
    assert_eq!(standalone.runs("ship").len(), 1, "one run per key");
    let logs = logs(standalone.dir.path());
    assert!(logs.contains("shipped o-1 o-1 3"), "{logs}");
    let listed = standalone.cli_json(&["events", "list", "--name", "paid"]);
    assert_eq!(
        listed["events"].as_array().map(Vec::len),
        Some(2),
        "{listed}"
    );
}

/// M4.11h: `pay` posts `paid` through `schedr.api` (twice with one idempotency key: one event)
/// and is refused a route not open to tasks; a command posts `packed` with `schedr task-ctx
/// events post`, a command's script with `schedr.api`; `audit`'s trigger, called for `paid`,
/// posts `audited` from its loop.
const API_EVENTS: &str = r#"
import sys
from schedr import CommandOperator, Dag, Event, EventNamed, api, task, task_ctx, trigger
from schedr.types import Json

SCHEDR = @SCHEDR@
SCRIPT = (
    "from schedr import api\n"
    "print('script posted', api.post_event('noted', key='o-1').posted_by)\n"
)

@task(secrets=["etl/db#password"])
def pay() -> None:
    # Masked as an output is: a secret it read isn't stored (Codex's review).
    api.post_event("leak", key="o-1", data={"echo": task_ctx.secret("etl/db", "password")})
    first = api.post_event("paid", key="o-1", data={"amount": 40}, idempotency_key="pay-o-1")
    again = api.post_event("paid", key="o-1", data={"amount": 40}, idempotency_key="pay-o-1")
    print("same event", first.id == again.id, first.posted_by)
    try:
        api.Client().request("GET", "/api/v1/dags")
    except api.ApiError as e:
        print("refused", e.status)

with Dag("order"):
    pay() >> CommandOperator("pack", ["sh", "-c",
        f"{SCHEDR} task-ctx events post packed --key o-1 --data '{{\"box\": 3}}'"]) >> CommandOperator(
        "note", [sys.executable, "-c", SCRIPT])

@trigger(wait_for=EventNamed("paid"))
async def on_paid(event: Event[Json]) -> Event | None:
    await api.apost_event("audited", key=event.key)
    return None

with Dag("audit", trigger=on_paid()):
    @task
    def nothing() -> None:
        pass
    nothing()
"#;

#[test]
fn tasks_commands_and_triggers_post_events_through_their_agent() {
    let dag = API_EVENTS.replace("@SCHEDR@", &format!("{BINARY:?}"));
    let standalone = Standalone::start(&[("ship.py", NAMED_EVENTS), ("order.py", &dag)], None);
    let (code, out, err) = standalone.secrets(None, &["set", "etl/db", "password=hunter22-pw"]);
    assert_eq!(code, 0, "{out}{err}");
    standalone.wait("DAGs", || {
        standalone.dag_ids() == ["audit", "order", "ship"]
    });
    for dag in ["ship", "audit"] {
        let (code, _, err) = standalone.cli(&["dags", "unpause", dag]);
        assert_eq!(code, 0, "{err}");
    }
    let command = standalone.cli_json(&["runs", "trigger", "order"]);
    let order = command["result"]["run_id"].as_str().unwrap().to_owned();
    standalone.wait_for_state(&order, "success");
    standalone.wait("a run of ship", || standalone.runs("ship").len() == 1);
    let ship = standalone.runs("ship")[0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    standalone.wait_for_state(&ship, "success");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains(&format!("same event True task:order/{order}/pay")),
        "{logs}"
    );
    assert!(logs.contains("refused 403"), "{logs}");
    assert!(logs.contains("shipped o-1 o-1 3"), "{logs}");
    assert!(
        logs.contains(&format!("script posted task:order/{order}/note")),
        "{logs}"
    );
    let events = |name: &str| {
        let listed = standalone.cli_json(&["events", "list", "--name", name]);
        listed["events"].as_array().cloned().unwrap_or_default()
    };
    assert_eq!(events("paid").len(), 1, "one event for one idempotency key");
    assert_eq!(events("leak")[0]["data"]["echo"], "****");
    assert!(
        logs.contains("held a secret this task read"),
        "the task is told: {logs}"
    );
    assert_eq!(
        events("packed")[0]["posted_by"],
        format!("task:order/{order}/pack")
    );
    standalone.wait("the trigger's event", || events("audited").len() == 1);
    assert_eq!(events("audited")[0]["posted_by"], "trigger:audit");
}

/// `keyed` waits for an event whose key a mapped task made (Codex's review of M4.11e).
const MAPPED_KEY: &str = r#"
from schedr import Dag, Event, EventNamed, task, wait_for
from schedr.types import Json

@task
def key_of(x: int) -> str:
    return f"order-{x}"

@task
def done(packed: Event[Json]) -> None:
    print("packed", packed.key)

with Dag("keyed"):
    keys = key_of.expand(x=[1])
    done(wait_for(EventNamed("packed", key=keys[0])).on(Event[Json]))
"#;

#[test]
fn a_wait_s_key_can_come_from_a_mapped_task() {
    let standalone = Standalone::start(&[("keyed.py", MAPPED_KEY)], None);
    standalone.wait("DAG `keyed`", || standalone.dag_ids() == ["keyed"]);
    let command = standalone.cli_json(&["runs", "trigger", "keyed"]);
    let run = command["result"]["run_id"].as_str().unwrap().to_owned();
    standalone.cli_json(&["events", "post", "packed", "--key", "order-1"]);
    standalone.wait_for_state(&run, "success");
    let logs = logs(standalone.dir.path());
    assert!(logs.contains("packed order-1"), "{logs}");
}

/// `bill` is started by an `@trigger(wait_for=EventNamed("paid"))` for each payment over 10,
/// and its `@sensor(wait_for=EventNamed("signed", key=...))` waits for that order's signature,
/// saying `NOT_YET` to one that isn't final (decision 354).
const EVENT_FUNCTIONS: &str = r#"
from schedr import Dag, Event, EventNamed, dag_ctx, sensor, task, trigger
from schedr.sensors import NOT_YET, NotYet
from schedr.types import Json

@trigger(wait_for=EventNamed("paid"))
async def on_paid(event: Event[Json], least: int) -> Event[int] | None:
    print("called for", event.key)
    amount = event.data["amount"]
    return Event(data=amount) if amount >= least else None

@task
def bill(order: str, amount: int, signer: str) -> None:
    print("billed", order, amount, signer)

with Dag("bill", trigger=on_paid(least=10)):
    @sensor(wait_for=EventNamed("signed", key=dag_ctx.event.key))
    async def signed(event: Event[Json]) -> str | NotYet:
        return event.data["by"] if event.data["final"] else NOT_YET

    bill(dag_ctx.event.key, dag_ctx.event.data, signed().on(str))
"#;

/// M4.11e part 2: an `@trigger(wait_for=EventNamed)` is called for each event posted, and an
/// `@sensor(wait_for=EventNamed)` is given the event of its key, then a later one.
#[test]
fn functions_waiting_for_events_are_called_with_them() {
    let standalone = Standalone::start(&[("bill.py", EVENT_FUNCTIONS)], None);
    standalone.wait("DAG `bill`", || standalone.dag_ids() == ["bill"]);
    let (code, _, err) = standalone.cli(&["dags", "unpause", "bill"]);
    assert_eq!(code, 0, "{err}");
    let post = |name: &str, key: &str, data: &str| {
        standalone.cli_json(&["events", "post", name, "--key", key, "--data", data]);
    };
    post("paid", "o-1", r#"{"amount": 4}"#);
    post("paid", "o-2", r#"{"amount": 40}"#);
    standalone.wait("a run of bill", || standalone.runs("bill").len() == 1);
    post("signed", "o-2", r#"{"by": "ann", "final": false}"#);
    post("signed", "o-2", r#"{"by": "bob", "final": true}"#);
    let run = standalone.runs("bill")[0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    standalone.wait_for_state(&run, "success");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains("called for o-1") && logs.contains("called for o-2"),
        "{logs}"
    );
    assert!(logs.contains("billed o-2 40 bob"), "{logs}");
    assert_eq!(
        standalone.runs("bill").len(),
        1,
        "the small payment started none"
    );
}

#[test]
fn a_sensor_waiting_for_another_dags_run_is_given_it_and_builds_on_its_output() {
    let standalone = Standalone::start(&[("run_of.py", RUN_OF)], None);
    standalone.wait("DAGs `orders` and `report`", || {
        standalone.dag_ids() == ["orders", "report"]
    });
    let moment = "2026-10-15T00:00:00Z";
    let trigger = |dag: &str| {
        let command = standalone.cli_json(&["runs", "trigger", dag, "--for", moment]);
        command["result"]["run_id"].as_str().unwrap().to_owned()
    };
    let report = trigger("report");
    let orders = trigger("orders");
    standalone.wait_for_state(&orders, "success");
    standalone.wait_for_state(&report, "success");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains("given orders success 3") && logs.contains("rows 6"),
        "{logs}"
    );
}

/// A run of one short wait step: its host is stopped once it has none left.
const ONE_DELAY: &str = r#"
from datetime import timedelta
from schedr import Dag
from schedr.sensors import delay

with Dag("one_delay"):
    delay(timedelta(seconds=1))
"#;

/// B253: a wait step host the agent stops, its last wait step gone, ends without the warning
/// of a channel that ended (it said so once per host, every wait step on a quiet worker).
#[test]
fn a_host_stopped_after_its_last_wait_step_ends_without_a_warning() {
    let standalone = Standalone::start_configured(
        &[("one_delay.py", ONE_DELAY)],
        None,
        &[],
        "\n[log]\nlevel = \"info,schedr_agent::watchers=debug\"\n",
        &[],
    );
    standalone.wait("DAG `one_delay`", || standalone.dag_ids() == ["one_delay"]);
    let run_id = standalone.trigger("one_delay");
    standalone.wait_for_state(&run_id, "success");
    standalone.wait("the host's end", || {
        standalone
            .log()
            .contains("a watcher host with no wait step left ended")
    });
    let log = standalone.log();
    assert!(!log.contains("a watcher host's channel ended"), "{log}");
}

/// `load` is started by an `@trigger.poke(batch=...)`: what its pokes find within a window is one
/// run, its data the list of them (decision 352).
const BATCHED: &str = r#"
from datetime import timedelta
from schedr import Dag, Event, dag_ctx, task, trigger

@trigger.poke(every=timedelta(milliseconds=200), batch=timedelta(seconds=2))
async def files(after: str | None) -> list[Event[str]]:
    number = 0 if after is None else int(after) + 1
    return [Event(str(number), f"file-{number}")] if number < 3 else []

@task
def load(found: list[Event[str]]) -> None:
    print("loaded", [event.data for event in found])

with Dag("load", trigger=files()):
    load(dag_ctx.event.data)
"#;

/// M4.11f: a poke's items gathered for `batch`, then one run with all of them.
#[test]
fn a_batched_poke_starts_one_run_per_window() {
    let standalone = Standalone::start(&[("load.py", BATCHED)], None);
    standalone.wait("DAG `load`", || standalone.dag_ids() == ["load"]);
    let (code, _, err) = standalone.cli(&["dags", "unpause", "load"]);
    assert_eq!(code, 0, "{err}");
    standalone.wait("a run of load", || standalone.runs("load").len() == 1);
    let run = standalone.runs("load")[0]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    standalone.wait_for_state(&run, "success");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains("loaded ['file-0', 'file-1', 'file-2']"),
        "{logs}"
    );
    assert_eq!(standalone.runs("load").len(), 1, "one window, one run");
}

/// What a webhook receiver got: each POST's path and body.
type Received = std::sync::Arc<std::sync::Mutex<Vec<(String, Value)>>>;

/// A webhook receiver on 127.0.0.1: answers 200 to each POST, keeping its path and body.
fn webhook_receiver() -> (String, Received) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let got = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = got.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut request = Vec::new();
            let mut buffer = [0; 8192];
            let (head, body) = loop {
                let Ok(read) = stream.read(&mut buffer) else {
                    break (String::new(), String::new());
                };
                if read == 0 {
                    break (String::new(), String::new());
                }
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).into_owned();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break (head.to_owned(), body.to_owned());
                    }
                }
            };
            let path = head.split(' ').nth(1).unwrap_or_default().to_owned();
            if let Ok(value) = serde_json::from_str(&body) {
                kept.lock().unwrap().push((path, value));
            }
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
        }
    });
    (base, got)
}

#[test]
fn a_failed_run_sends_its_webhooks_a_pure_cancel_none_and_a_task_its_own() {
    let (base, got) = webhook_receiver();
    let schedr = format!("{BINARY:?}");
    let dag = format!(
        r#"
import time
from schedr import CommandOperator, Dag, Notify, WebhookChannel, notify, task

relay = WebhookChannel(
    "relay",
    url="{base}/relay",
    body={{"title": t"[etl] {{notify.dag}} {{notify.event}}", "tasks": notify.failed_tasks,
          "rows": notify.data["rows"]}},
)

@task
def broken() -> None:
    raise ValueError("source is down")

@task
def report() -> None:
    relay.send("loaded", data={{"rows": 12}})

@task
def slow() -> None:
    time.sleep(120)

with Dag("fails", notify=[relay, "ops"]):
    broken()

with Dag("reports", notify={{relay: [Notify.RUN_FAILED]}}):
    report() >> CommandOperator("tell", [{schedr}, "task-ctx", "notify", "relay", "--text", "told",
                                         "--data", '{{"rows": 3}}'])

with Dag("cancelled", notify=[relay, "ops"]):
    slow()
"#
    );
    let config = format!(
        "\n[notifications]\ndag_urls = [\"{base}/\"]\n\n\
         [notifications.channels.ops]\ntype = \"webhook\"\nurl = \"{base}/ops\"\n"
    );
    let standalone = Standalone::start_configured(
        &[("notify.py", &dag)],
        None,
        &[],
        &config,
        &[("SCHEDR_HIDDEN__MASTER__OUTBOX_POLL", "100ms")],
    );
    standalone.wait("the DAGs", || {
        standalone.dag_ids() == ["cancelled", "fails", "reports"]
    });

    let failed = standalone.trigger("fails");
    standalone.wait_for_state(&failed, "failed");
    standalone.wait("two webhooks", || got.lock().unwrap().len() == 2);
    let mut sent = got.lock().unwrap().clone();
    sent.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(sent[0].0, "/ops");
    assert_eq!(sent[0].1["event"], "RUN_FAILED");
    assert_eq!(sent[0].1["run"], failed.as_str());
    assert_eq!(
        sent[0].1["failed_tasks"],
        json!([{"task": "broken", "error": "ValueError: source is down"}])
    );
    assert_eq!(sent[1].0, "/relay");
    assert_eq!(
        sent[1].1,
        json!({"title": "[etl] fails RUN_FAILED",
               "tasks": [{"task": "broken", "error": "ValueError: source is down"}],
               "rows": null})
    );

    let reports = standalone.trigger("reports");
    standalone.wait_for_state(&reports, "success");
    standalone.wait("the tasks' messages", || got.lock().unwrap().len() == 4);
    assert_eq!(
        got.lock().unwrap()[2..],
        [
            (
                "/relay".to_owned(),
                json!({"title": "[etl] reports MESSAGE", "tasks": [], "rows": 12})
            ),
            (
                "/relay".to_owned(),
                json!({"title": "[etl] reports MESSAGE", "tasks": [], "rows": 3})
            ),
        ]
    );

    let cancelled = standalone.trigger("cancelled");
    standalone.wait("the task to run", || {
        let attempts = standalone.get(&format!("/api/v1/runs/{cancelled}/tasks/slow/attempts"));
        attempts.is_some_and(|a| a["attempts"].as_array().is_some_and(|a| !a.is_empty()))
    });
    let (code, _, err) = standalone.cli(&["runs", "cancel", &cancelled]);
    assert_eq!(code, 0, "{err}");
    standalone.wait_for_state(&cancelled, "cancelled");
    // A few sender passes: nothing more arrives.
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(got.lock().unwrap().len(), 4);
}

/// M5.2i8 (DN18, DN28): `RUN_STARTED` tells of each run with its reason, `EVENT_POSTED` of each
/// event posted to the DAGs listening for its name, a repeat of a key too; `details` holds
/// both, a manual run's DAG gets its own once.
#[test]
fn a_run_started_and_an_event_posted_are_told_with_their_details() {
    let (base, got) = webhook_receiver();
    let dag = r#"
from schedr import Dag, EventNamed, Notify, dag_ctx, task, wait_for

@task
def ship(order: str) -> None:
    print("shipped", order)

# Listens by a wait step only, no run of it going (agy's review of M5.2i8).
with Dag("packer", notify={"ops": [Notify.EVENT_POSTED]}):
    wait_for(EventNamed("paid"))

@task
def hello() -> None:
    print("hello")

with Dag("ship", trigger=EventNamed("paid"),
         notify={"ops": [Notify.RUN_STARTED, Notify.EVENT_POSTED]}):
    ship(dag_ctx.event.key)

with Dag("plain", notify={"ops": [Notify.RUN_STARTED]}):
    hello()
"#;
    let config =
        format!("\n[notifications.channels.ops]\ntype = \"webhook\"\nurl = \"{base}/ops\"\n");
    let standalone = Standalone::start_configured(
        &[("started.py", dag)],
        None,
        &[],
        &config,
        &[("SCHEDR_HIDDEN__MASTER__OUTBOX_POLL", "100ms")],
    );
    standalone.wait("the DAGs", || {
        standalone.dag_ids() == ["packer", "plain", "ship"]
    });
    let (code, _, err) = standalone.cli(&["dags", "unpause", "ship"]);
    assert_eq!(code, 0, "{err}");

    let plain = standalone.trigger("plain");
    standalone.wait_for_state(&plain, "success");
    standalone.wait("RUN_STARTED", || got.lock().unwrap().len() == 1);
    let sent = got.lock().unwrap()[0].1.clone();
    assert_eq!(sent["event"], "RUN_STARTED", "{sent}");
    assert_eq!(sent["run"], plain.as_str());
    assert_eq!(sent["details"]["reason"], "manual", "{sent}");

    let post = |key: &str| {
        let event = standalone.cli_json(&["events", "post", "paid", "--key", key]);
        assert_eq!(event["name"], "paid", "{event}");
    };
    post("o-1");
    standalone.wait("a run of ship", || standalone.runs("ship").len() == 1);
    standalone.wait("EVENT_POSTED twice and RUN_STARTED", || {
        got.lock().unwrap().len() == 4
    });
    // A key seen before starts none: still told of.
    post("o-1");
    standalone.wait("the repeat's EVENT_POSTED", || {
        got.lock().unwrap().len() == 6
    });
    let sent: Vec<Value> = got.lock().unwrap()[1..]
        .iter()
        .map(|(_, b)| b.clone())
        .collect();
    let of = |event: &str| -> Vec<&Value> { sent.iter().filter(|b| b["event"] == event).collect() };
    let posted = of("EVENT_POSTED");
    let to =
        |dag: &str| -> Vec<&Value> { posted.iter().copied().filter(|b| b["dag"] == dag).collect() };
    let (ship, packer) = (to("ship"), to("packer"));
    assert_eq!((ship.len(), packer.len()), (2, 2), "{sent:?}");
    assert_eq!(ship[0]["run"], Value::Null);
    assert_eq!(
        ship[0]["details"],
        json!({"name": "paid", "key": "o-1", "listens": "trigger",
               "posted_by": ship[0]["details"]["posted_by"]})
    );
    assert_eq!(ship[1]["details"], ship[0]["details"], "{sent:?}");
    assert_eq!(packer[0]["details"]["listens"], "wait_step", "{sent:?}");
    let started = of("RUN_STARTED");
    assert_eq!(started.len(), 1, "{sent:?}");
    assert_eq!(
        started[0]["details"],
        json!({"reason": "trigger", "key": "o-1"})
    );
    assert_eq!(started[0]["run"], standalone.runs("ship")[0]["run_id"]);
    // A few sender passes: nothing more.
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(got.lock().unwrap().len(), 6);
}

/// A mail server on 127.0.0.1 speaking just enough SMTP, keeping each message's DATA.
fn smtp_receiver() -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let got = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = got.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let Ok(reader) = stream.try_clone() else {
                continue;
            };
            let mut lines = BufReader::new(reader).lines();
            let _ = stream.write_all(b"220 test ready\r\n");
            while let Some(Ok(line)) = lines.next() {
                let upper = line.to_ascii_uppercase();
                if upper.starts_with("DATA") {
                    let _ = stream.write_all(b"354 go on\r\n");
                    let mut data = String::new();
                    for line in lines.by_ref() {
                        let Ok(line) = line else { break };
                        if line == "." {
                            break;
                        }
                        data.push_str(&line);
                        data.push('\n');
                    }
                    kept.lock().unwrap().push(data);
                    let _ = stream.write_all(b"250 queued\r\n");
                } else if upper.starts_with("QUIT") {
                    let _ = stream.write_all(b"221 bye\r\n");
                    break;
                } else {
                    let _ = stream.write_all(b"250 ok\r\n");
                }
            }
        }
    });
    (port, got)
}

#[test]
fn a_failed_run_mails_its_email_channel_a_pure_cancel_doesnt_and_a_task_mails_its_own() {
    let (port, got) = smtp_receiver();
    let dag = r#"
import time
from schedr import Dag, api, task

@task
def broken() -> None:
    raise ValueError("source is down")

@task
def report() -> None:
    api.notify("oncall", text="report ready", data={"rows": 12})

@task
def slow() -> None:
    time.sleep(120)

with Dag("fails", notify=["oncall"]):
    broken()

with Dag("reports"):
    report()

with Dag("cancelled", notify=["oncall"]):
    slow()
"#;
    let config = format!(
        "\n[notifications.smtp]\nhost = \"127.0.0.1\"\nport = {port}\ntls = \"none\"\n\
         from = \"schedr <schedr@example.com>\"\n\n\
         [notifications.channels.oncall]\ntype = \"email\"\nto = [\"oncall@example.com\"]\n"
    );
    let standalone = Standalone::start_configured(
        &[("mail.py", dag)],
        None,
        &[],
        &config,
        &[("SCHEDR_HIDDEN__MASTER__OUTBOX_POLL", "100ms")],
    );
    standalone.wait("the DAGs", || {
        standalone.dag_ids() == ["cancelled", "fails", "reports"]
    });

    let failed = standalone.trigger("fails");
    standalone.wait_for_state(&failed, "failed");
    standalone.wait("an email", || got.lock().unwrap().len() == 1);
    // Long headers are folded onto the next line, starting with a space.
    let mail = got.lock().unwrap()[0].replace("\n ", " ");
    for part in [
        "To: oncall@example.com",
        &format!("Subject: DAG `fails`: run {failed} failed (failed: broken)"),
        "broken: ValueError: source is down",
    ] {
        assert!(mail.contains(part), "{part} in:\n{mail}");
    }

    let reports = standalone.trigger("reports");
    standalone.wait_for_state(&reports, "success");
    standalone.wait("the task's email", || got.lock().unwrap().len() == 2);
    let mail = got.lock().unwrap()[1].clone();
    assert!(mail.contains("Subject: report ready"), "{mail}");
    assert!(mail.contains("\"rows\": 12"), "{mail}");

    let cancelled = standalone.trigger("cancelled");
    standalone.wait("the task to run", || {
        let attempts = standalone.get(&format!("/api/v1/runs/{cancelled}/tasks/slow/attempts"));
        attempts.is_some_and(|a| a["attempts"].as_array().is_some_and(|a| !a.is_empty()))
    });
    let (code, _, err) = standalone.cli(&["runs", "cancel", &cancelled]);
    assert_eq!(code, 0, "{err}");
    standalone.wait_for_state(&cancelled, "cancelled");
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(got.lock().unwrap().len(), 2);
}

#[test]
fn retention_deletes_an_old_run_but_keeps_the_newest() {
    let dag = r#"
from schedr import Dag, task

@task
def one() -> int:
    return 1

with Dag("kept"):
    one()
"#;
    let standalone = Standalone::start_configured(
        &[("kept.py", dag)],
        None,
        &[],
        "\n[retention]\nruns = \"1s\"\n",
        &[("SCHEDR_HIDDEN__MASTER__RETENTION_INTERVAL", "200ms")],
    );
    standalone.wait("the DAG", || standalone.dag_ids() == ["kept"]);
    let first = standalone.trigger("kept");
    standalone.wait_for_state(&first, "success");
    let second = standalone.trigger("kept");
    standalone.wait_for_state(&second, "success");
    // The first ended over a second ago and isn't the newest: deleted.
    let ids = || {
        standalone.get("/api/v1/dags/kept/runs").map(|list| {
            list["runs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|run| run["run_id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
    };
    standalone.wait("the old run deleted", || {
        ids().is_some_and(|ids| ids == [second.clone()])
    });
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(ids(), Some(vec![second]), "the newest stays");
}

#[test]
fn a_task_over_its_own_memory_budget_is_killed_a_capped_one_fails_inside() {
    let dag = r#"
import time
from schedr import Dag, task

@task(memory="64MiB")
def greedy() -> None:
    held = bytearray(400 << 20)
    time.sleep(10)

@task(memory="64MiB", hard_memory_limit=True)
def capped() -> None:
    bytearray(400 << 20)

with Dag("greedy"):
    greedy()

with Dag("capped"):
    capped()
"#;
    let standalone = Standalone::start(&[("memory.py", dag)], None);
    standalone.wait("the DAGs", || standalone.dag_ids() == ["capped", "greedy"]);
    let attempt = |run: &str, task: &str| {
        standalone
            .get(&format!("/api/v1/runs/{run}/tasks/{task}/attempts"))
            .unwrap()["attempts"][0]
            .clone()
    };
    let greedy = standalone.trigger("greedy");
    standalone.wait_for_state(&greedy, "failed");
    let killed = attempt(&greedy, "greedy");
    assert_eq!(killed["status"], "oom_killed", "{killed}");
    assert!(
        killed.to_string().contains("exceeded memory budget 64 MiB"),
        "{killed}"
    );

    let capped = standalone.trigger("capped");
    standalone.wait_for_state(&capped, "failed");
    let failed = attempt(&capped, "capped");
    assert_eq!(failed["status"], "failed", "{failed}");
    assert!(failed.to_string().contains("MemoryError"), "{failed}");
}

/// `schedr standalone --dag-path .` in a folder of DAG files (decision 488): the folder is the
/// one DAG root, named after it, instead of `SCHEDR_DAG_PATH`'s; a module beside the DAG file
/// imports with no script root.
#[test]
fn standalone_in_a_folder_of_dags_takes_it_as_its_root() {
    let helpers = "def greeting(name: str) -> str:\n    return f\"hello {name}\"\n";
    let pipeline = "\
from helpers import greeting
from schedr import Dag, task


@task
def greet() -> str:
    print(greeting(\"folder\"))
    return greeting(\"folder\")


with Dag(\"pipeline\"):
    greet()
";
    let standalone =
        Standalone::start_in_root(&[("helpers.py", helpers), ("pipeline.py", pipeline)]);
    standalone.wait("DAG `pipeline`", || standalone.dag_ids() == ["pipeline"]);
    let (_, dag) = &standalone.dags()[0];
    assert_eq!(
        dag["location"]["root"], "etl",
        "named after the folder `.` is"
    );
    assert_eq!(dag["location"]["path"], "pipeline.py");
    let run_id = standalone.trigger("pipeline");
    let run = standalone.wait_for_state(&run_id, "success");
    assert_eq!(task_states(&run), pairs(&[("greet", "success")]));
    let (code, out, err) = standalone.cli(&["tasks", "logs", &run_id, "greet", "--try", "1"]);
    assert_eq!((code, out.as_str()), (0, "hello folder\n"), "{err}");
    assert_eq!(
        standalone.dag_ids(),
        ["pipeline"],
        "SCHEDR_DAG_PATH's DAG isn't there"
    );
}

/// `task`'s first attempt of `run` once it has `statuses` statuses and a link: written off the
/// master's round (decision 521), they come a moment after the run's end.
fn with_reports(standalone: &Standalone, run: &str, task: &str, statuses: usize) -> Value {
    let mut attempts = Value::Null;
    standalone.wait("its statuses and link", || {
        attempts = standalone
            .get(&format!("/api/v1/runs/{run}/tasks/{task}/attempts"))
            .unwrap();
        let attempt = &attempts["attempts"][0];
        attempt["statuses"].as_array().map(Vec::len) == Some(statuses)
            && attempt["links"].as_array().is_some_and(|l| !l.is_empty())
    });
    attempts["attempts"][0].clone()
}

/// The dummy extension's plugin (examples/extensions/dummy), run by the test's Python.
fn dummy_plugin() -> String {
    let plugin = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/extensions/dummy/schedr-ext-dummy")
        .canonicalize()
        .unwrap();
    format!(
        "\n[extensions.dummy]\ncommand = [\"{}\", \"{}\"]\nversions = [\"1.0\"]\n",
        python(),
        plugin.display()
    )
}

/// M5.2i (decision 510): an extension task runs in its plugin, its lines in the attempt's log and
/// its output a downstream task's input; one asking for a version no worker serves is given back
/// by the worker, no try counted, and ends unschedulable.
#[test]
fn an_extension_task_runs_in_its_plugin_and_one_no_worker_serves_is_unschedulable() {
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask, task


@task
def rows(found: dict[str, int]) -> int:
    print(f"rows: {found['rows']}")
    return found["rows"]


with Dag("extended"):
    job = ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[
            {"log": "the dummy plugin runs"},
            {"status": ["pulling", 0.25]},
            {"link": ["Pod", "https://dash.example/pods/job-1"]},
            {"status": ["running", None]},
            {"return": {"rows": 3}},
        ],
        returns=dict[str, int],
    )
    rows(job.output)

with Dag("unserved"):
    ExtensionTask("job", name="dummy", version="9.9", payload={"return": 1})
"#;
    let standalone =
        Standalone::start_configured(&[("extended.py", DAG)], None, &[], &dummy_plugin(), &[]);
    standalone.wait("the DAGs to be found", || {
        let ids = standalone.dag_ids();
        ids.contains(&"extended".to_owned()) && ids.contains(&"unserved".to_owned())
    });
    let run = standalone.trigger("extended");
    let done = standalone.wait_for_state(&run, "success");
    assert_eq!(
        task_states(&done),
        [
            ("job".to_owned(), "success".to_owned()),
            ("rows".to_owned(), "success".to_owned())
        ]
    );
    let output = standalone
        .get(&format!("/api/v1/runs/{run}/tasks/job/output"))
        .unwrap();
    assert_eq!(output["value"], serde_json::json!({"rows": 3}), "{output}");
    // Decision 513: its statuses and links, with the attempt.
    let attempt = &with_reports(&standalone, &run, "job", 2);
    let phases: Vec<&str> = attempt["statuses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["phase"].as_str().unwrap())
        .collect();
    assert_eq!(phases, ["pulling", "running"], "{attempt}");
    assert_eq!(attempt["statuses"][0]["progress"], 0.25, "{attempt}");
    assert_eq!(
        attempt["links"],
        serde_json::json!([{"label": "Pod", "url": "https://dash.example/pods/job-1"}])
    );
    let logs = logs(standalone.dir.path());
    assert!(logs.contains("the dummy plugin runs\n"), "{logs}");
    assert!(logs.contains("rows: 3\n"), "{logs}");
    let log = standalone.log();
    assert!(
        log.contains("no connected worker offers extension `dummy` 9.9"),
        "the master warns: {log}"
    );
    let run = standalone.trigger("unserved");
    standalone.wait("the unserved task to be unschedulable", || {
        task_states(&standalone.run(&run)) == [("job".to_owned(), "unschedulable".to_owned())]
    });
    let job = &standalone.run(&run)["tasks"][0];
    assert_eq!(job["try_number"], 0, "a rejection is no try: {job}");
}

/// M5.2i8 (decision 515, DN19, DN20): a plugin's declared event reaches a channel subscribed to
/// `EXTENSION_EVENT`, its extension, version and event in `details`; an undeclared one is
/// dropped, warned of once; both are on the extension's events panel.
#[test]
fn a_plugins_declared_event_is_told_and_an_undeclared_one_dropped_once() {
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask, Notify

with Dag("extended", notify={"ops": [Notify.EXTENSION_EVENT]}):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[
            {"notify": ["job_done", "the job is done", {"rows": 3}]},
            {"notify": ["made_up", None, None]},
            {"notify": ["made_up", None, None]},
            {"return": 1},
        ],
    )
"#;
    let (base, got) = webhook_receiver();
    let config = format!(
        "{}\n[notifications.channels.ops]\ntype = \"webhook\"\nurl = \"{base}/ops\"\n",
        dummy_plugin()
    );
    let standalone = Standalone::start_configured(
        &[("extended.py", DAG)],
        None,
        &[],
        &config,
        &[("SCHEDR_HIDDEN__MASTER__OUTBOX_POLL", "100ms")],
    );
    standalone.wait("the DAG", || standalone.dag_ids() == ["extended"]);
    let run = standalone.trigger("extended");
    standalone.wait_for_state(&run, "success");
    standalone.wait("the extension's event", || got.lock().unwrap().len() == 1);
    let (path, sent) = got.lock().unwrap()[0].clone();
    assert_eq!(path, "/ops");
    assert_eq!(sent["event"], "EXTENSION_EVENT", "{sent}");
    assert_eq!(sent["run"], run.as_str());
    assert_eq!(sent["task"], "job");
    assert_eq!(sent["text"], "the job is done");
    assert_eq!(sent["data"], json!({"rows": 3}));
    assert_eq!(
        sent["details"],
        json!({"extension": "dummy", "version": "1.0", "event": "job_done"})
    );
    let mut seen = Value::Null;
    standalone.wait("both on the events panel", || {
        seen = standalone
            .get("/api/v1/extensions/dummy")
            .unwrap_or_default();
        seen["events"].as_array().is_some_and(|e| e.len() >= 2)
    });
    let events: Vec<(String, String)> = seen["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["kind"].as_str().unwrap().to_owned(),
                e["message"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert!(
        events.contains(&(
            "notified".to_owned(),
            "job_done: the job is done".to_owned()
        )),
        "{events:?}"
    );
    let dropped: Vec<_> = events
        .iter()
        .filter(|(kind, _)| kind == "dropped")
        .collect();
    assert_eq!(dropped.len(), 1, "once: {events:?}");
    assert!(
        dropped[0]
            .1
            .contains("`made_up` isn't one its plugin declared"),
        "{events:?}"
    );
    let log = standalone.log();
    assert_eq!(log.matches("notification `made_up`").count(), 1, "{log}");
    // A few sender passes: nothing more.
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(got.lock().unwrap().len(), 1);
}

/// Decision 514: a Python task's `logging` is structured with no change to its code, as is
/// `schedr task-ctx log`; the Logs tab shows the lines with their level, and the log downloads
/// readable or as stored.
#[test]
fn a_tasks_logging_is_structured_shown_with_its_level_and_downloaded_both_ways() {
    const DAG: &str = r#"
import logging

from schedr import Dag, task

log = logging.getLogger("etl.load")


@task
def load() -> None:
    print("a plain line")
    log.info("loaded %d rows", 3, extra={"table": "sales"})
    log.debug("not at INFO")


@task
def big() -> None:
    log.info("big", extra={"blob": "x" * (2 << 20)})


with Dag("logged"):
    load()
    big()
"#;
    let standalone = Standalone::start(&[("logged.py", DAG)], None);
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"logged".to_owned())
    });
    let run = standalone.trigger("logged");
    standalone.wait_for_state(&run, "success");
    let (_, log, _) = standalone.cli(&["tasks", "logs", &run, "load", "--try", "1"]);
    assert!(log.contains("a plain line\n"), "{log}");
    let structured = log
        .lines()
        .find_map(schedr_model::log_line::LogLine::parse)
        .unwrap_or_else(|| panic!("no structured line: {log}"));
    assert_eq!(structured.message, "loaded 3 rows");
    assert_eq!(structured.fields["table"], "sales");
    assert_eq!(structured.fields["logger"], "etl.load");
    assert!(!log.contains("not at INFO"), "{log}");
    let panel = standalone
        .page(&format!("/d/default/runs/{run}?task=load&tab=logs"))
        .unwrap();
    assert!(panel.contains("<span class=\"sl lv-info\">"), "{panel}");
    assert!(
        panel.contains("format=json"),
        "a JSON download is offered: {panel}"
    );
    let plain = standalone
        .page(&format!(
            "/d/default/runs/{run}/tasks/load/log.txt?try=1&format=plain"
        ))
        .unwrap();
    assert!(
        plain.contains("INFO     loaded 3 rows  logger=etl.load  table=sales"),
        "{plain}"
    );
    let json = standalone
        .page(&format!(
            "/d/default/runs/{run}/tasks/load/log.txt?try=1&format=json"
        ))
        .unwrap();
    assert_eq!(json, log, "as stored");
    // A line longer than the view's parts, whole both ways (Codex's review of M5.2i5: cut).
    let download = |format: &str| {
        standalone
            .page(&format!(
                "/d/default/runs/{run}/tasks/big/log.txt?try=1&format={format}"
            ))
            .unwrap()
    };
    let (json, plain) = (download("json"), download("plain"));
    let big = json
        .lines()
        .filter_map(schedr_model::log_line::LogLine::parse)
        .find(|line| line.message == "big")
        .expect("the big line");
    assert_eq!(big.fields["blob"].as_str().map(str::len), Some(2 << 20));
    let blob = "x".repeat(2 << 20);
    assert!(
        plain.contains(&format!("blob={blob}")),
        "the plain one whole too"
    );
}

/// Decision 514: with `[worker] log_format = "plain"`, a structured line is stored readable, and
/// `schedr task-ctx log` writes one from a command task.
#[test]
fn a_plain_log_format_stores_structured_lines_readable() {
    const DAG: &str = r#"
from schedr import CommandOperator, Dag

with Dag("plainly"):
    CommandOperator("say", [SCHEDR, "task-ctx", "log", "copied", "--level", "warning", "--field", "rows=3"])
"#;
    let dag = DAG.replace("SCHEDR", &format!("{BINARY:?}"));
    let standalone = Standalone::start_configured(
        &[("plainly.py", &dag)],
        None,
        &[],
        "",
        &[("SCHEDR_CONFIG__WORKER__LOG_FORMAT", "plain")],
    );
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"plainly".to_owned())
    });
    let run = standalone.trigger("plainly");
    standalone.wait_for_state(&run, "success");
    let (_, log, err) = standalone.cli(&["tasks", "logs", &run, "say", "--try", "1"]);
    assert!(log.contains("WARNING  copied  rows=3"), "{log}{err}");
    assert!(!log.contains("schedr_log"), "stored readable: {log}");
}

/// Decision 513: a plugin's metrics, declared in its hello, are on the worker's `/metrics` and
/// reach the master: the Extensions API has its declarations, page and a minute's samples.
/// DN19: the dropped point an event of the extension, and the attempt's latest phase, both on
/// its view for its events panel.
fn a_dropped_point_and_the_phase_are_on_its_page(standalone: &Standalone) {
    let mut seen = serde_json::Value::Null;
    standalone.wait("the dropped point's event", || {
        seen = standalone
            .get("/api/v1/extensions/dummy")
            .unwrap_or_default();
        seen["events"].as_array().is_some_and(|e| !e.is_empty())
    });
    let (events, attempts) = (&seen["events"], &seen["attempts"]);
    assert_eq!(events[0]["kind"], "dropped", "{events}");
    assert!(
        events[0]["message"]
            .as_str()
            .unwrap()
            .contains("metric `undeclared` isn't declared"),
        "{events}"
    );
    assert_eq!(attempts[0]["phase"], "measuring", "{attempts}");
    assert_eq!(attempts[0]["dag_id"], "measured", "{attempts}");
}

#[test]
fn a_plugins_declared_metrics_are_served_and_sampled() {
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask

with Dag("measured"):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[
            {"metric": ["steps", 1, {"step": "log"}]},
            {"metric": ["steps", 2, {"step": "log"}]},
            {"metric": ["sleep_seconds", 0.5, {}]},
            {"metric": ["undeclared", 1, {}]},
            {"status": ["measuring", 0.5]},
        ],
    )
"#;
    let standalone = Standalone::start_configured(
        &[("measured.py", DAG)],
        None,
        &[],
        &dummy_plugin(),
        &[("SCHEDR_HIDDEN__WORKER__EXTENSION_FIGURES_INTERVAL", "1s")],
    );
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"measured".to_owned())
    });
    let run = standalone.trigger("measured");
    standalone.wait_for_state(&run, "success");
    let log = standalone.log();
    let ops = log
        .split("ops on http://")
        .nth(1)
        .and_then(|rest| rest.split([',', ' ', '\n']).next())
        .unwrap()
        .replacen("0.0.0.0:", "127.0.0.1:", 1);
    let metrics = || {
        let output = Command::new("curl")
            .args(["-s", &format!("http://{ops}/metrics")])
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let served = metrics();
    assert!(
        served.contains(
            "schedr_extension_dummy_steps_total{step=\"log\",extension_version=\"1.0\"} 3"
        ),
        "{served}"
    );
    assert!(
        served.contains("schedr_extension_dummy_sleep_seconds_count"),
        "{served}"
    );
    standalone.wait("the extension to be listed", || {
        standalone
            .get("/api/v1/extensions")
            .is_some_and(|list| list["extensions"][0]["name"] == "dummy")
    });
    let extension = standalone.get("/api/v1/extensions/dummy").unwrap();
    let panels: Vec<(&str, u64, &str)> = extension["panels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["title"].as_str().unwrap(),
                p["row"].as_u64().unwrap(),
                p["cell"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        panels,
        [
            ("In flight", 1, "left"),
            ("Steps", 1, "center"),
            ("Jobs by outcome", 1, "right"),
            ("Sleeps", 2, "left"),
            ("Sleep quantiles", 2, "right"),
            ("Queue depth", 3, "left"),
            ("Queues now", 3, "right"),
            ("Events", 4, "full"),
            // No panel shows `bytes_read`: a row of its own below them, titled by its help.
            ("Bytes the made-up jobs read", 5, "full"),
        ],
        "{extension}"
    );
    assert!(
        log.contains("a metric point dropped: metric `undeclared` isn't declared"),
        "{log}"
    );
    a_dropped_point_and_the_phase_are_on_its_page(&standalone);
    the_first_figures_are_sampled(&standalone);
    one_workers_figures_are_its_own_and_kept_a_day(&standalone);
}

/// DN21: `?worker=` gives that worker's figures (the only one's: all of them), another's none,
/// and a range past a day is refused; the page shows the extension in a tab, the worker in a
/// sub-tab, and a worker's longer range as its last day.
fn one_workers_figures_are_its_own_and_kept_a_day(standalone: &Standalone) {
    let extension = standalone.get("/api/v1/extensions/dummy").unwrap();
    let worker = extension["workers"][0]["worker_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let all = standalone
        .get("/api/v1/extensions/dummy/metrics?range=1h")
        .unwrap();
    let mine = standalone
        .get(&format!(
            "/api/v1/extensions/dummy/metrics?range=1h&worker={worker}"
        ))
        .unwrap();
    // Read twice, a minute's figures may have come between: the steps' series, one each (agy's
    // review of M5.2i5: none would pass too).
    assert!(
        mine["metrics"][0]["series"]
            .as_array()
            .is_some_and(|s| !s.is_empty()),
        "{mine}"
    );
    assert_eq!(
        mine["metrics"][0]["series"].as_array().map(Vec::len),
        all["metrics"][0]["series"].as_array().map(Vec::len),
        "{mine}\n{all}"
    );
    // Its events and attempts: its own, another's none (agy's review of M5.2i5: a worker's tab
    // showed every worker's attempts).
    let its = standalone
        .get(&format!("/api/v1/extensions/dummy?worker={worker}"))
        .unwrap();
    assert!(
        its["attempts"].as_array().is_some_and(|a| !a.is_empty()),
        "{its}"
    );
    let nobodys = standalone
        .get("/api/v1/extensions/dummy?worker=nobody")
        .unwrap();
    assert_eq!(
        (&nobodys["attempts"], &nobodys["events"]),
        (&serde_json::json!([]), &serde_json::json!([])),
        "{nobodys}"
    );
    assert_eq!(nobodys["workers"], its["workers"], "its workers all listed");
    let other = standalone
        .get("/api/v1/extensions/dummy/metrics?range=1h&worker=nobody")
        .unwrap();
    assert_eq!(
        other["metrics"][0]["series"],
        serde_json::json!([]),
        "{other}"
    );
    assert!(
        standalone
            .get(&format!(
                "/api/v1/extensions/dummy/metrics?range=7d&worker={worker}"
            ))
            .is_none(),
        "a worker's figures past a day are refused"
    );
    let page = standalone
        .page(&format!(
            "/d/default/extensions/dummy?range=7d&worker={worker}"
        ))
        .unwrap();
    assert!(page.contains("class=\"tab open\""), "{page}");
    assert!(page.contains(">All workers</a>"), "{page}");
    assert!(
        page.contains(&format!(
            "class=\"current\" aria-current=\"true\"><span class=\"mono\">{worker}</span>"
        )),
        "{page}"
    );
    assert!(
        !page.contains(">7d</a>"),
        "a worker's ranges stop at a day: {page}"
    );
    // The list goes to the first extension's tab.
    let output = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code} %{redirect_url}",
            &format!("{}/extensions", standalone.api),
        ])
        .output()
        .unwrap();
    let answer = String::from_utf8_lossy(&output.stdout);
    assert!(
        answer.starts_with("303 ") && answer.contains("/d/default/extensions/"),
        "{answer}"
    );
}

/// A worker's first figures count whole (Codex's review of M5.2i5: they were a baseline, lost):
/// the three steps and the sleep reach the samples.
fn the_first_figures_are_sampled(standalone: &Standalone) {
    let mut sampled = serde_json::Value::Null;
    standalone.wait("the steps sampled", || {
        sampled = standalone
            .get("/api/v1/extensions/dummy/metrics?range=1h")
            .unwrap_or_default();
        let grown: f64 = sampled["metrics"][0]["series"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|s| s["points"].as_array().cloned().unwrap_or_default())
            .filter_map(|p| p["value"].as_f64())
            .sum();
        grown >= 3.0
    });
    assert_eq!(sampled["metrics"][0]["name"], "steps", "{sampled}");
    assert_eq!(sampled["bucket_s"], 60, "{sampled}");
    let sleeps = &sampled["metrics"][1];
    assert_eq!(sleeps["name"], "sleep_seconds", "{sampled}");
    assert_eq!(
        sleeps["series"][0]["points"][0]["counts"]
            .as_array()
            .map(|c| c.iter().filter_map(serde_json::Value::as_u64).sum::<u64>()),
        Some(1),
        "{sampled}"
    );
}

/// DN19 and Codex's second review of M5.2i5: a plugin skipped at discovery (its `--describe`
/// fails) is known by its event: listed, its page found, the event on it.
#[test]
fn a_plugin_skipped_at_discovery_is_listed_and_its_event_shown() {
    use std::os::unix::fs::PermissionsExt as _;
    let plugins = tempfile::TempDir::new().unwrap();
    let broken = plugins.path().join("schedr-ext-broken");
    std::fs::write(&broken, "#!/bin/sh\nexit 3\n").unwrap();
    std::fs::set_permissions(&broken, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        plugins.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let standalone = Standalone::start_configured(
        &[],
        None,
        &[],
        "",
        &[
            ("SCHEDR_CONFIG__EXTENSIONS__DISCOVER", "true"),
            ("PATH", &path),
        ],
    );
    standalone.wait("the skipped plugin listed", || {
        standalone
            .get("/api/v1/extensions")
            .is_some_and(|list| list["extensions"].to_string().contains("\"broken\""))
    });
    let extension = standalone.get("/api/v1/extensions/broken").unwrap();
    assert_eq!(extension["version"], "", "{extension}");
    assert_eq!(extension["events"][0]["kind"], "skipped", "{extension}");
    // agy's review of M5.2i5: its workers are its events' (none were listed), and the list stays
    // by name with it in it.
    assert_eq!(
        extension["workers"][0]["worker_id"], extension["events"][0]["worker_id"],
        "{extension}"
    );
    let names: Vec<String> = standalone.get("/api/v1/extensions").unwrap()["extensions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_owned())
        .collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);
    let metrics = standalone
        .get("/api/v1/extensions/broken/metrics?range=1h")
        .unwrap();
    assert_eq!(metrics["metrics"], serde_json::json!([]), "{metrics}");
    let page = standalone.page("/d/default/extensions/broken").unwrap();
    assert!(page.contains("What happened"), "{page}");
    assert!(page.contains("schedr-ext-broken"), "{page}");
}

/// M5.2i: a plugin is supervised as the Python runner is: one that hangs is stopped at the task's
/// timeout, and one that fails fails the attempt with its error.
#[test]
fn an_extension_plugin_that_hangs_is_timed_out_and_one_that_fails_fails() {
    const DAG: &str = r#"
from datetime import timedelta

from schedr import Dag, ExtensionTask

with Dag("hangs"):
    # Past the plugin's start under load: a SIGTERM before its handler exists ends it at once,
    # with nothing to say (`make check` once took over 2 s).
    ExtensionTask(
        "job", name="dummy", version="1.0", payload={"hang": True}, timeout=timedelta(seconds=10)
    )

with Dag("fails"):
    ExtensionTask("job", name="dummy", version="1.0", payload={"fail": "the cluster said no"})
"#;
    let standalone =
        Standalone::start_configured(&[("extended.py", DAG)], None, &[], &dummy_plugin(), &[]);
    standalone.wait("the DAGs to be found", || {
        let ids = standalone.dag_ids();
        ids.contains(&"hangs".to_owned()) && ids.contains(&"fails".to_owned())
    });
    let hangs = standalone.trigger("hangs");
    let fails = standalone.trigger("fails");
    let hung = standalone.wait_for_state(&hangs, "failed");
    let job = &hung["tasks"][0];
    assert_eq!(job["state"], "failed", "{job}");
    standalone.wait_for_state(&fails, "failed");
    let attempts = standalone
        .get(&format!("/api/v1/runs/{fails}/tasks/job/attempts"))
        .unwrap();
    let text = attempts.to_string();
    assert!(text.contains("the cluster said no"), "{text}");
    let hung_attempts = standalone
        .get(&format!("/api/v1/runs/{hangs}/tasks/job/attempts"))
        .unwrap()
        .to_string();
    assert!(
        hung_attempts.contains("timed_out") || hung_attempts.contains("timeout"),
        "{hung_attempts}"
    );
    // Decision 513: SIGTERM reached it as a cancellation, and it asked why: a timeout, with
    // the grace (`cancel_grace`, 30 s) left.
    let (_, log, err) = standalone.cli(&["tasks", "logs", &hangs, "job", "--try", "1"]);
    assert!(log.contains("stopped: timeout, "), "{log}{err}");
    // It was let finish within its grace: no SIGKILL, so the attempt ended at once.
    let stopped_left: u64 = log
        .split("stopped: timeout, ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|seconds| seconds.parse().ok())
        .unwrap_or(0);
    assert!(stopped_left > 10, "{log}");
}

/// M5.2i: an extension task's cleanup runs its plugin again after the attempt, whatever its end,
/// with the handles it registered, in order.
#[test]
fn an_extension_tasks_cleanup_runs_its_plugin_again_with_its_handles() {
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask

with Dag("cleaned"):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[{"handle": {"id": "job-1"}}, {"handle": {"id": "job-2"}}, {"fail": "midway"}],
        cleanup=True,
    )
"#;
    let standalone =
        Standalone::start_configured(&[("cleaned.py", DAG)], None, &[], &dummy_plugin(), &[]);
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"cleaned".to_owned())
    });
    let run = standalone.trigger("cleaned");
    standalone.wait_for_state(&run, "failed");
    standalone.wait("the cleanup to have run", || {
        logs(standalone.dir.path()).contains("cleaning up job: {'id': 'job-2'}")
    });
    let logs = logs(standalone.dir.path());
    let first = logs.find("cleaning up job: {'id': 'job-1'}").expect(&logs);
    let second = logs.find("cleaning up job: {'id': 'job-2'}").expect(&logs);
    assert!(first < second, "in registration order: {logs}");
}

/// The Rust example plugin (crates/schedr-extension/examples/hello-plugin.rs), built into this
/// workspace's target (cargo builds a crate's examples only for its own tests).
fn hello_plugin() -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-q",
            "-p",
            "schedr-extension",
            "--example",
            "hello-plugin",
        ])
        .current_dir(&workspace)
        .status()
        .unwrap();
    assert!(status.success(), "the example plugin builds");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| workspace.join("target"), PathBuf::from);
    let plugin = target
        .join("debug/examples/hello-plugin")
        .canonicalize()
        .unwrap();
    format!(
        "\n[extensions.hello]\ncommand = [\"{}\"]\nversions = [\"1.0\"]\n",
        plugin.display()
    )
}

/// M5.2i: plugins in two languages in one DAG, the Python dummy and the Rust example, their
/// outputs read by one Python task.
#[test]
fn plugins_in_python_and_rust_run_side_by_side() {
    let dag = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/extensions/dummy_extension.py"),
    )
    .unwrap();
    let config = format!("{}{}", dummy_plugin(), hello_plugin());
    let standalone =
        Standalone::start_configured(&[("dummy_extension.py", &dag)], None, &[], &config, &[]);
    standalone.wait("the DAG to be found", || {
        standalone
            .dag_ids()
            .contains(&"extension_two_languages".to_owned())
    });
    let run = standalone.trigger("extension_two_languages");
    let done = standalone.wait_for_state(&run, "success");
    assert_eq!(task_states(&done).len(), 3, "{done}");
    let output = standalone
        .get(&format!("/api/v1/runs/{run}/tasks/together/output"))
        .unwrap();
    assert_eq!(output["value"], "hello, schedr: 3 rows", "{output}");
    let logs = logs(standalone.dir.path());
    assert!(
        logs.contains("hello, schedr\n"),
        "the Rust plugin's own line: {logs}"
    );
}

/// B275: a plugin announcing a message over the protocol's limit is cut off: its channel closed,
/// its write fails, and the attempt ends, even with no timeout.
#[test]
fn a_plugin_announcing_a_message_over_the_limit_is_cut_off() {
    const PLUGIN: &str = r#"
import json, os, socket, sys

sock = socket.socket(fileno=int(os.environ["SCHEDR_AGENT_FD"]))

def send(message):
    data = json.dumps(message).encode()
    sock.sendall(len(data).to_bytes(4, "big") + data)

def receive():
    size = int.from_bytes(sock.recv(4, socket.MSG_WAITALL), "big")
    return json.loads(sock.recv(size, socket.MSG_WAITALL))

send({"type": "hello", "schedr_version": "0", "mode": "extension", "protocol": 1})
receive()
receive()
sock.sendall((64 * 1024 * 1024 + 1).to_bytes(4, "big"))
try:
    while True:
        sock.sendall(b"x" * 65536)
except OSError as error:
    print(f"cut off: {error}", flush=True)
    sys.exit(1)
"#;
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask

with Dag("oversized"):
    ExtensionTask("job", name="loud", version="1.0", payload=None)
"#;
    let dir = TempDir::new().unwrap();
    let plugin = dir.path().join("loud.py");
    std::fs::write(&plugin, PLUGIN).unwrap();
    let config = format!(
        "\n[extensions.loud]\ncommand = [\"{}\", \"{}\"]\nversions = [\"1.0\"]\n",
        python(),
        plugin.display()
    );
    let standalone =
        Standalone::start_configured(&[("oversized.py", DAG)], None, &[], &config, &[]);
    standalone.wait("the DAG to be found", || {
        standalone.dag_ids().contains(&"oversized".to_owned())
    });
    let run = standalone.trigger("oversized");
    standalone.wait_for_state(&run, "failed");
    let logs = logs(standalone.dir.path());
    assert!(logs.contains("cut off:"), "{logs}");
}

/// M5.2i8: the backup script's declared event, through `task-ctx extension notify`, delivered
/// (agy's review: the page alone would pass on a notification never sent) and on its page.
fn the_scripts_event_is_delivered_and_listed(standalone: &Standalone, got: &Received, run: &str) {
    standalone.wait("the script's event delivered", || {
        !got.lock().unwrap().is_empty()
    });
    let (_, sent) = got.lock().unwrap()[0].clone();
    assert_eq!(
        (
            sent["event"].clone(),
            sent["details"].clone(),
            sent["text"].clone()
        ),
        (
            json!("EXTENSION_EVENT"),
            json!({"extension": "backup", "version": "1.0", "event": "backup_done"}),
            json!(format!("snapshot snap-{run} taken"))
        ),
        "{sent}"
    );
    standalone.wait("the script's event", || {
        standalone
            .get("/api/v1/extensions/backup")
            .is_some_and(|page| {
                page["events"].as_array().is_some_and(|events| {
                    events.iter().any(|e| {
                        e["kind"] == "notified"
                            && e["message"] == format!("backup_done: snapshot snap-{run} taken")
                    })
                })
            })
    });
}

/// M5.2i6 (decision 519): `schedr extension shell` runs a script as an extension's plugin: the
/// payload on its stdin, a declared secret, a handle and its cleanup, statuses, a link and a
/// metric point through `schedr task-ctx`, its output set; a failing exit fails the attempt, and
/// a cancel reaches the script, which asks why. A command task reports a status and a link too,
/// and is refused a metric.
#[test]
fn a_shell_script_is_an_extensions_plugin_through_task_ctx() {
    const DAG: &str = r#"
from schedr import CommandOperator, Dag, ExtensionTask, Notify

for dag_id, payload in [("backups", {"source": "/data"}), ("slow_backups", {"slow": True}),
                        ("failing_backups", {"fail": True})]:
    # Told of its plugin's events (M5.2i8): `backups` only.
    told = {"ops": [Notify.EXTENSION_EVENT]} if dag_id == "backups" else {}
    with Dag(dag_id, notify=told):
        ExtensionTask("nightly", name="backup", version="1.0", payload=payload,
                      secrets=["backup/target"], cleanup=True)

with Dag("reporting_command"):
    CommandOperator("report", ["sh", "-c", "{bin} task-ctx status working --progress 0.5"
                               " && {bin} task-ctx link Docs https://docs.example/x"
                               " && ! {bin} task-ctx extension metric x 1 2>&1"])
"#;
    let (base, got) = webhook_receiver();
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/extensions/backup/backup.sh")
        .canonicalize()
        .unwrap();
    let config = format!(
        "\n[extensions.backup]\ncommand = [\"{BINARY}\", \"extension\", \"shell\", \"--\", \
         \"{}\"]\nversions = [\"1.0\"]\nmetrics = [{{name = \"files_copied\", kind = \
         \"counter\", help = \"Files copied\", labels = [{{name = \"kind\"}}]}}]\n\
         notifications = [{{name = \"backup_done\", help = \"A snapshot taken\"}}]\n\
         \n[notifications.channels.ops]\ntype = \"webhook\"\nurl = \"{base}/ops\"\n",
        script.display()
    );
    let dag = DAG.replace("{bin}", BINARY);
    let standalone = Standalone::start_configured(
        &[("backups.py", dag.as_str())],
        None,
        &[],
        &config,
        &[("SCHEDR_HIDDEN__MASTER__OUTBOX_POLL", "100ms")],
    );
    let (code, out, err) =
        standalone.secrets(None, &["set", "backup/target", "token=s3cret-token-99"]);
    assert_eq!(code, 0, "{out}{err}");
    standalone.wait("the DAGs to be found", || standalone.dag_ids().len() == 4);

    let run = standalone.trigger("backups");
    standalone.wait_for_state(&run, "success");
    let output = standalone
        .get(&format!("/api/v1/runs/{run}/tasks/nightly/output"))
        .unwrap();
    assert_eq!(
        output["value"],
        serde_json::json!({"snapshot": format!("snap-{run}"), "files": 3}),
        "{output}"
    );
    let attempt = &with_reports(&standalone, &run, "nightly", 3);
    let phases: Vec<&str> = attempt["statuses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["phase"].as_str().unwrap())
        .collect();
    assert_eq!(phases, ["preparing", "copying", "done"], "{attempt}");
    assert_eq!(
        attempt["links"][0]["url"],
        format!("https://backups.example/snapshots/snap-{run}"),
        "{attempt}"
    );
    standalone.wait("the cleanup to remove the snapshot", || {
        logs(standalone.dir.path()).contains(&format!("removing snapshot snap-{run}"))
    });
    let logs_now = logs(standalone.dir.path());
    assert!(
        logs_now.contains(r#"backing up {"source":"/data"}"#),
        "the payload on stdin: {logs_now}"
    );
    assert!(
        logs_now.contains("the target's token has 15 characters"),
        "{logs_now}"
    );
    assert!(!logs_now.contains("s3cret-token-99"), "{logs_now}");
    the_scripts_event_is_delivered_and_listed(&standalone, &got, &run);
    let log = standalone.log();
    let ops = log
        .split("ops on http://")
        .nth(1)
        .and_then(|rest| rest.split([',', ' ', '\n']).next())
        .unwrap()
        .replacen("0.0.0.0:", "127.0.0.1:", 1);
    let served = Command::new("curl")
        .args(["-s", &format!("http://{ops}/metrics")])
        .output()
        .unwrap();
    let served = String::from_utf8_lossy(&served.stdout);
    assert!(
        served.contains(
            "schedr_extension_backup_files_copied_total{kind=\"full\",extension_version=\"1.0\"} 3"
        ),
        "{served}"
    );

    a_shell_plugin_fails_and_stops_and_a_command_task_reports(&standalone);
}

/// A failing script fails the attempt; a cancel reaches it, and it asks why; a command task
/// reports a status and a link, and is refused a metric.
fn a_shell_plugin_fails_and_stops_and_a_command_task_reports(standalone: &Standalone) {
    let failing = standalone.trigger("failing_backups");
    standalone.wait_for_state(&failing, "failed");
    let attempts = standalone
        .get(&format!("/api/v1/runs/{failing}/tasks/nightly/attempts"))
        .unwrap();
    assert_eq!(
        attempts["attempts"][0]["error_summary"]["message"], "the script exited with 3",
        "{attempts}"
    );

    let slow = standalone.trigger("slow_backups");
    standalone.wait("the slow backup to copy", || {
        standalone
            .get(&format!("/api/v1/runs/{slow}/tasks/nightly/attempts"))
            .is_some_and(|a| a.to_string().contains("\"copying\""))
    });
    let (code, _, err) = standalone.cli(&["runs", "cancel", &slow]);
    assert_eq!(code, 0, "{err}");
    standalone.wait_for_state(&slow, "cancelled");
    standalone.wait("the script to say why it stopped", || {
        logs(standalone.dir.path()).contains("stopped: cancel")
    });

    let command = standalone.trigger("reporting_command");
    standalone.wait_for_state(&command, "success");
    let attempt = &with_reports(standalone, &command, "report", 1);
    assert_eq!(attempt["statuses"][0]["phase"], "working", "{attempt}");
    assert_eq!(attempt["statuses"][0]["progress"], 0.5, "{attempt}");
    assert_eq!(attempt["links"][0]["label"], "Docs", "{attempt}");
    assert!(
        logs(standalone.dir.path()).contains("a metric is an extension's"),
        "the refusal printed"
    );
}

/// Decision 520: standalone's gRPC port is under mTLS made in memory: another process on the
/// machine, speaking plain gRPC or TLS with a certificate of its own, can't connect, while
/// standalone's own worker runs a DAG through it.
/// What it created (its data directory, its SQLite file) is its user's alone.
#[test]
fn another_process_cant_reach_standalones_grpc_port() {
    use std::io::{Read as _, Write as _};
    let standalone = Standalone::start(&[("chain.py", CHAIN)], None);
    let log = standalone.log();
    // Its API, signing no one in, is every local user's: said at start (Grok's review).
    assert!(
        log.contains("every user of this machine can reach it"),
        "{log}"
    );
    let master = log
        .split("master on ")
        .nth(1)
        .and_then(|rest| rest.split(',').next())
        .unwrap()
        .to_owned();
    // Plain HTTP/2: not answered as gRPC (a TLS alert, or the connection closed).
    let mut tcp = std::net::TcpStream::connect(&master).unwrap();
    tcp.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    tcp.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").unwrap();
    let mut answer = [0u8; 16];
    let read = tcp.read(&mut answer).unwrap_or(0);
    assert!(
        read == 0 || answer[0] == 0x15,
        "a plain client got {:?}",
        &answer[..read]
    );
    // TLS with a certificate standalone's CA didn't sign.
    let dir = TempDir::new().unwrap();
    let ca = schedr_tls::testing::TestCa::new("another CA");
    let other = schedr_tls::Tls::load(
        ca.write(dir.path(), "other", &["schedr-worker"]),
        "schedr-worker",
        "schedr-ui",
    )
    .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let refused = runtime
        .block_on(schedr_tls::channel(
            &format!("https://{master}"),
            Some(&other),
        ))
        .unwrap_err();
    assert!(refused.contains("UnknownIssuer"), "{refused}");
    // The servers themselves ask for a client certificate (agy's review of M5.2i14): a client
    // trusting any server, presenting none, is refused by the gRPC port and the log endpoint.
    let master: std::net::SocketAddr = master.parse().unwrap();
    let grpc = runtime.block_on(schedr_tls::testing::without_a_certificate(
        master,
        b"h2",
        b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
    ));
    assert!(
        grpc.as_ref()
            .is_err_and(|e| e.contains("CertificateRequired")),
        "{grpc:?}"
    );
    standalone.wait("the worker's log endpoint", || {
        standalone
            .get("/api/v1/workers")
            .is_some_and(|w| w["workers"][0]["log_endpoint"].is_string())
    });
    let workers = standalone.get("/api/v1/workers").unwrap();
    let endpoint = workers["workers"][0]["log_endpoint"]
        .as_str()
        .unwrap()
        .to_owned();
    let logs: std::net::SocketAddr = endpoint.trim_start_matches("https://").parse().unwrap();
    let http = runtime.block_on(schedr_tls::testing::without_a_certificate(
        logs,
        b"http/1.1",
        b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
    ));
    assert!(
        http.as_ref()
            .is_err_and(|e| e.contains("CertificateRequired")),
        "{http:?}"
    );
    a_chain_runs_end_to_end(&standalone);
    // What it created is its user's alone.
    let mode = |path: &Path| {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    };
    let data = standalone.dir.path().join("data");
    assert_eq!(mode(&data), 0o700);
    assert_eq!(mode(&data.join("schedr.db")), 0o600);
    for side in ["schedr.db-wal", "schedr.db-shm"] {
        let file = data.join(side);
        if file.exists() {
            assert_eq!(mode(&file), 0o600, "{side}");
        }
    }
}

/// The dummy extension with its service (decision 518), `more` keys added to its table; its
/// restarts quick for the tests.
fn dummy_with_service(more: &str) -> String {
    let plugin = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/extensions/dummy/schedr-ext-dummy")
        .canonicalize()
        .unwrap();
    format!(
        "{}service = [\"{}\", \"{}\", \"--service\"]\n{more}",
        dummy_plugin(),
        python(),
        plugin.display()
    )
}

const QUICK_RESTARTS: [(&str, &str); 2] = [
    ("SCHEDR_HIDDEN__WORKER__SERVICE_BACKOFF_FIRST", "100ms"),
    ("SCHEDR_HIDDEN__WORKER__SERVICE_BACKOFF_MAX", "200ms"),
];

/// Waits for standalone's log to hold `text`.
fn wait_log(standalone: &Standalone, text: &str) {
    standalone.wait(text, || standalone.log().contains(text));
}

/// M5.2i9 (decision 518): attempts at once share one service, its counter giving each its own
/// number; a crash fails its attempt and the retry lands once the service is ready again; on
/// SIGTERM it is stopped after the last attempt.
#[test]
fn attempts_share_a_service_and_one_crashing_it_is_retried_once_it_is_ready() {
    const DAG: &str = r#"
from datetime import timedelta

from schedr import Dag, ExtensionTask

with Dag("shared"):
    for name in ["a", "b", "c"]:
        ExtensionTask(name, name="dummy", version="1.0", payload=[{"service": ["count", None]}])

with Dag("crashing"):
    ExtensionTask(
        "job",
        name="dummy",
        version="1.0",
        payload=[{"service": ["crash_once", None]}],
        retries=2,
        retry_delay=timedelta(seconds=1),
    )
"#;
    let mut standalone = Standalone::start_configured(
        &[("service.py", DAG)],
        None,
        &[],
        &dummy_with_service(""),
        &QUICK_RESTARTS,
    );
    wait_log(&standalone, "the extension's service is ready");
    assert!(
        standalone.log().contains("service started with 0 secrets"),
        "{}",
        standalone.log()
    );
    standalone.wait("the DAGs", || standalone.dag_ids().len() == 2);
    let run = standalone.trigger("shared");
    standalone.wait_for_state(&run, "success");
    let mut counted: Vec<i64> = ["a", "b", "c"]
        .iter()
        .map(|task| {
            standalone
                .get(&format!("/api/v1/runs/{run}/tasks/{task}/output"))
                .unwrap()["value"]
                .as_i64()
                .unwrap()
        })
        .collect();
    counted.sort_unstable();
    assert_eq!(counted, [1, 2, 3], "one service, its counter shared");
    let run = standalone.trigger("crashing");
    let done = standalone.wait_for_state(&run, "success");
    assert_eq!(done["tasks"][0]["try_number"], 2, "{done}");
    let log = standalone.log();
    assert!(log.contains("the extension's service crashed"), "{log}");
    // SIGTERM: drained, its service stopped after the last attempt.
    let pid = standalone.child.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while standalone.child.try_wait().unwrap().is_none() {
        assert!(
            Instant::now() < deadline,
            "standalone didn't stop\n{}",
            standalone.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        standalone.log().contains("service stopped by the worker"),
        "{}",
        standalone.log()
    );
}

/// M5.2i9 (decision 518, DN31): a service past its memory is killed and its attempt fails; past
/// `service_restarts`, the extension is turned off, and its tasks wait for it as for code.
#[test]
fn a_service_past_its_memory_is_killed_and_past_its_restarts_turned_off() {
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask

with Dag("greedy"):
    ExtensionTask(
        "job", name="dummy", version="1.0", payload=[{"service": ["grow", 64]}], retries=0
    )

with Dag("crash"):
    ExtensionTask(
        "job", name="dummy", version="1.0", payload=[{"service": ["crash", None]}], retries=0
    )
"#;
    let standalone = Standalone::start_configured(
        &[("service.py", DAG)],
        None,
        &[],
        &dummy_with_service("service_memory = \"32MiB\"\nservice_restarts = 1\n"),
        &QUICK_RESTARTS,
    );
    wait_log(&standalone, "the extension's service is ready");
    standalone.wait("the DAGs", || standalone.dag_ids().len() == 2);
    let run = standalone.trigger("greedy");
    standalone.wait_for_state(&run, "failed");
    wait_log(&standalone, "past its service_memory of 32 MiB");
    // A second end within the settle: one more than its one restart, turned off.
    standalone.wait("the service ready again", || {
        standalone
            .log()
            .matches("the extension's service is ready")
            .count()
            >= 2
    });
    let run = standalone.trigger("crash");
    standalone.wait_for_state(&run, "failed");
    wait_log(
        &standalone,
        "the extension's service turned off on this worker",
    );
    // Its next task waits for it, no try used.
    let run = standalone.trigger("crash");
    standalone.wait("the task waiting for the service", || {
        let job = &standalone.run(&run)["tasks"][0];
        job["state"] == "queued" && job["try_number"] == 0
    });
}

/// M5.2i9 (DN32): a service whose loop is stuck leaves attempts' connections unanswered; each
/// such attempt fails at once, and `service_timeouts` of them in a row take it for hung.
#[test]
fn a_stuck_service_leaves_attempts_unanswered_and_is_taken_for_hung() {
    const DAG: &str = r#"
from schedr import Dag, ExtensionTask

with Dag("stuck"):
    ExtensionTask("staller", name="dummy", version="1.0", payload=[{"service": ["stall", 60]}])

with Dag("waiting"):
    ExtensionTask(
        "job", name="dummy", version="1.0", payload=[{"service": ["count", None]}], retries=0
    )
"#;
    let standalone = Standalone::start_configured(
        &[("service.py", DAG)],
        None,
        &[],
        &dummy_with_service("service_accept_timeout = \"1s\"\nservice_timeouts = 2\n"),
        &QUICK_RESTARTS,
    );
    wait_log(&standalone, "the extension's service is ready");
    standalone.wait("the DAGs", || standalone.dag_ids().len() == 2);
    standalone.trigger("stuck");
    // The staller's call is in once the service says so: its loop is blocked from here (agy's
    // review: a blind wait raced it under load).
    wait_log(&standalone, "service: stalling its loop");
    for _ in 0..2 {
        let run = standalone.trigger("waiting");
        standalone.wait_for_state(&run, "failed");
        let attempts = standalone
            .get(&format!("/api/v1/runs/{run}/tasks/job/attempts"))
            .unwrap();
        let message = attempts["attempts"][0]["error_summary"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        assert!(
            message.contains("didn't accept the attempt within its service_accept_timeout"),
            "{attempts}"
        );
    }
    wait_log(
        &standalone,
        "connections unanswered in a row: taken for hung",
    );
}

/// DN33: a maintenance drain is told to the worker's services, which keep running, and so is
/// the resume.
#[test]
fn a_maintenance_drain_and_resume_are_told_to_a_service() {
    let standalone = Standalone::start_configured(
        &[("chain.py", CHAIN)],
        None,
        &[],
        &dummy_with_service(""),
        &QUICK_RESTARTS,
    );
    wait_log(&standalone, "the extension's service is ready");
    let workers = standalone.get("/api/v1/workers").unwrap();
    let worker = workers["workers"][0]["worker_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (code, _, err) = standalone.cli(&["workers", "drain", &worker]);
    assert_eq!(code, 0, "{err}");
    wait_log(&standalone, "service told: drained");
    let (code, _, err) = standalone.cli(&["workers", "resume", &worker]);
    assert_eq!(code, 0, "{err}");
    wait_log(&standalone, "service told: resumed");
    assert_eq!(
        standalone
            .log()
            .matches("the extension's service is ready")
            .count(),
        1,
        "kept running"
    );
}
