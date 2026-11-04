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
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("dags/etl");
        std::fs::create_dir_all(&root).unwrap();
        for (name, text) in files {
            std::fs::write(root.join(name), text).unwrap();
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
        if let Some(url) = database_url {
            // A URL with a password is a secret: from the environment, never the file.
            command.env("SCHEDR_CONFIG__DATABASE__URL", url);
        }
        command.envs(more_env.iter().copied());
        let child = command
            .arg("--config")
            .arg(dir.path().join("config.toml"))
            .args(["standalone", "--migrate"])
            .args(extra)
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", dir.path())
            .env("SCHEDR_DAG_PATH", &root)
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
    let standalone = Standalone::start(&[("secrets.py", SECRETS)], None);
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
        list.contains(r#"href="/d/default/dags/chain""#) && list.contains("paused"),
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
    let standalone = Standalone::start(&[("secrets.py", SECRETS)], Some(&url));
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
