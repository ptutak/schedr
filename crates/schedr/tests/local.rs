//! `schedr dags check` and `schedr dags test` (PLAN.md 1.15): the real binary over temporary
//! DAG files, with the real Python runner and no server.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed); `make test-integration`
//! sets it.

use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use schedr_api::{ApiState, router};
use schedr_db::Database;
use schedr_db::workers::WorkerRegistration;
use schedr_model::{SchedrVersion, WorkerState};
use serde_json::Value;
use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_schedr");

const GOOD: &str = r#"
from schedr import Dag, task

@task
def extract() -> int:
    print("extracting")
    return 3

@task
def load(rows: int) -> str:
    return f"loaded {rows}"

with Dag("sync"):
    load(extract())
"#;

const FAILING: &str = r#"
from schedr import Dag, task

@task
def explode() -> None:
    raise ValueError("no rows")

with Dag("fragile"):
    explode()
"#;

const INVALID: &str = r#"
from schedr import Cron, Dag, task

@task
def nightly() -> None:
    pass

with Dag("bad_cron", trigger=Cron("61 3 * * *")):
    nightly()
"#;

/// A temporary DAG root `etl` with the files given, and a config naming the test Python.
struct Local {
    dir: TempDir,
}

impl Local {
    fn new(files: &[(&str, &str)]) -> Self {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("etl");
        std::fs::create_dir_all(&root).unwrap();
        for (name, text) in files {
            std::fs::write(root.join(name), text).unwrap();
        }
        let python = std::env::var("SCHEDR_TEST_PYTHON").expect(
            "SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)",
        );
        std::fs::write(
            dir.path().join("config.toml"),
            format!("[interpreters]\npython = \"{python}\"\n"),
        )
        .unwrap();
        Self { dir }
    }

    fn root(&self) -> std::path::PathBuf {
        self.dir.path().join("etl")
    }

    /// Runs `schedr dags <args>`: exit code, stdout, stderr.
    fn dags(&self, args: &[&str]) -> (i32, String, String) {
        let output = Command::new(BINARY)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.dir.path())
            .env("TZ", "UTC")
            .arg("--config")
            .arg(self.dir.path().join("config.toml"))
            .arg("dags")
            .args(args)
            .current_dir(self.dir.path())
            .output()
            .unwrap();
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    fn check(&self, extra: &[&str]) -> (i32, String, String) {
        let root = self.root();
        let mut args = vec!["check", path_str(&root)];
        args.extend_from_slice(extra);
        self.dags(&args)
    }

    fn test(&self, dag: &str, extra: &[&str]) -> (i32, String, String) {
        let root = self.root();
        let mut args = vec!["test", dag, "--path", path_str(&root)];
        args.extend_from_slice(extra);
        self.dags(&args)
    }
}

fn path_str(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn check_lists_the_dags_of_good_files() {
    let local = Local::new(&[("sync.py", GOOD), ("helpers.py", "X = 1\n")]);
    let (code, out, err) = local.check(&[]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        out,
        "ok     helpers.py: no DAGs\n\
         ok     sync.py: sync (2 tasks)\n\
         2 file(s), 1 DAG(s), 0 error(s)\n"
    );
}

#[test]
fn check_reports_broken_and_invalid_files_and_fails() {
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("broken.py", "import nowhere_to_be_found\n"),
        ("bad_cron.py", INVALID),
    ]);
    let (code, out, err) = local.check(&[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("error  broken.py: ModuleNotFoundError: "),
        "{out}"
    );
    assert!(out.contains("error  bad_cron.py: InvalidDag: "), "{out}");
    assert!(out.contains("ok     sync.py: sync (2 tasks)"), "{out}");
    assert!(out.ends_with("3 file(s), 1 DAG(s), 2 error(s)\n"), "{out}");
    assert!(err.contains("2 file(s) with errors"), "{err}");
    // The traceback starts at the user's code, not in schedr or importlib.
    assert!(!out.contains("importlib"), "{out}");
}

#[test]
fn check_with_json_gives_each_file() {
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("broken.py", "raise RuntimeError('boom')\n"),
    ]);
    let (code, out, _) = local.check(&["--json"]);
    assert_eq!(code, 1);
    let report: Value = serde_json::from_str(&out).unwrap();
    let files = report["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    let broken = files.iter().find(|f| f["path"] == "broken.py").unwrap();
    assert_eq!(broken["error"]["exception"], "RuntimeError");
    assert_eq!(broken["error"]["message"], "boom");
    let sync = files.iter().find(|f| f["path"] == "sync.py").unwrap();
    assert_eq!(sync["dags"], serde_json::json!(["sync"]));
    assert!(sync["error"].is_null());
}

#[test]
fn check_of_one_file_checks_only_it() {
    let local = Local::new(&[("sync.py", GOOD), ("broken.py", "raise RuntimeError\n")]);
    let file = local.root().join("sync.py");
    let (code, out, err) = local.dags(&["check", path_str(&file)]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        out,
        "ok     sync.py: sync (2 tasks)\n1 file(s), 1 DAG(s), 0 error(s)\n"
    );
}

#[test]
fn test_runs_the_dag_and_reports_each_task() {
    let local = Local::new(&[("sync.py", GOOD)]);
    let (code, out, err) = local.test("sync", &["--for", "2026-10-21T08:00:00Z"]);
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(out.contains("extract | extracting"), "{out}");
    assert!(out.contains("  extract: 3\n"), "{out}");
    assert!(out.contains("  load: \"loaded 3\"\n"), "{out}");
    let table = out.lines().find(|l| l.starts_with("TASK")).unwrap();
    assert!(
        table.contains("STATE") && table.contains("DURATION"),
        "{table}"
    );
    assert!(out.trim_end().ends_with(": success"), "{out}");
}

#[test]
fn test_with_json_gives_the_run_and_its_tasks() {
    let local = Local::new(&[("sync.py", GOOD)]);
    let (code, out, err) = local.test("sync", &["--json"]);
    assert_eq!(code, 0, "{err}");
    let report: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["run"]["state"], "success");
    assert_eq!(report["run"]["kind"], "manual");
    assert_eq!(report["location"]["path"], "sync.py");
    let tasks = report["tasks"].as_array().unwrap();
    let ids: Vec<&str> = tasks
        .iter()
        .map(|t| t["task_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["extract", "load"]);
    assert_eq!(tasks[0]["output"], 3);
    assert_eq!(tasks[0]["state"], "success");
    assert!(tasks[0]["log"].as_str().unwrap().contains("extracting"));
    assert_eq!(tasks[1]["output"], "loaded 3");
}

#[test]
fn a_failing_task_fails_the_test_with_its_log() {
    let local = Local::new(&[("fragile.py", FAILING)]);
    let (code, out, err) = local.test("fragile", &[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("explode | ") && out.contains("ValueError: no rows"),
        "{out}"
    );
    assert!(out.trim_end().ends_with(": failed"), "{out}");
    assert!(err.contains("ended failed"), "{err}");
}

#[test]
fn an_unknown_dag_is_an_error() {
    let local = Local::new(&[("sync.py", GOOD)]);
    let (code, _, err) = local.test("nope", &[]);
    assert_eq!(code, 1);
    assert!(err.contains("no DAG `nope` in the DAG files"), "{err}");
}

#[test]
fn an_unknown_dag_names_the_files_that_didnt_import() {
    let local = Local::new(&[("sync.py", GOOD), ("broken.py", "raise RuntimeError\n")]);
    let (code, _, err) = local.test("nope", &[]);
    assert_eq!(code, 1);
    assert!(
        err.contains("no DAG `nope` in the DAG files; these files have errors (`schedr dags check` shows them): etl:broken.py"),
        "{err}"
    );
}

#[test]
fn check_doesnt_wait_for_a_process_a_dag_file_detached() {
    // A process started in a session of its own outlives the runner's kill and keeps its
    // output pipes open; `dags check` must finish anyway.
    let detaching = format!(
        "import subprocess\nsubprocess.Popen(['sleep', '60'], start_new_session=True)\n{GOOD}"
    );
    let local = Local::new(&[("sync.py", detaching.as_str())]);
    let started = std::time::Instant::now();
    let (code, out, err) = local.check(&[]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(30),
        "took {:?}",
        started.elapsed()
    );
}

async fn add_worker(db: &Database, id: &str, version: &str, state: WorkerState, drained: bool) {
    db.register_worker(
        &WorkerRegistration {
            id,
            incarnation: "0190a0a0-0000-7000-8000-000000000001",
            protocol_version: "0.1",
            schedr_version: version,
            queues: &[],
            slots: 1,
            log_endpoint: "",
        },
        jiff::Timestamp::now(),
    )
    .await
    .unwrap();
    if state != WorkerState::Active {
        db.set_worker_state(id, state, jiff::Timestamp::now())
            .await
            .unwrap();
    }
    if drained {
        db.set_worker_drained(id, true).await.unwrap();
    }
}

async fn serve(db: Database) -> (String, Arc<Mutex<Vec<String>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let app = router(ApiState::new(db, None)).layer(axum::middleware::from_fn_with_state(
        seen.clone(),
        remember_auth,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, seen)
}

async fn remember_auth(
    axum::extract::State(seen): axum::extract::State<Arc<Mutex<Vec<String>>>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(header) = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        seen.lock().unwrap().push(header.to_owned());
    }
    next.run(request).await
}

/// A `major.minor.patch` string for `version`, as a worker reports it.
fn reported(version: SchedrVersion, patch: u32) -> String {
    format!("{version}.{patch}")
}

/// The minor before `current`, or the previous major when this one is `.0`.
fn older_minor(current: SchedrVersion) -> SchedrVersion {
    if current.minor > 0 {
        SchedrVersion::new(current.major, current.minor - 1)
    } else {
        SchedrVersion::new(current.major.saturating_sub(1), 0)
    }
}

/// The minor after `current`.
fn newer_minor(current: SchedrVersion) -> SchedrVersion {
    SchedrVersion::new(current.major, current.minor.saturating_add(1))
}

/// B20: `--against` used to print "isn't implemented yet" and stop.
#[tokio::test]
async fn check_against_fails_for_an_older_active_worker() {
    let current = SchedrVersion::current();
    let older = older_minor(current);
    assert!(older < current, "{current} has no older minor");
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    add_worker(&db, "w-lost", &reported(older, 0), WorkerState::Lost, false).await;
    add_worker(
        &db,
        "w-new",
        &reported(current, 0),
        WorkerState::Active,
        false,
    )
    .await;
    add_worker(&db, "w-old", &reported(older, 1), WorkerState::Active, true).await;
    let (url, seen) = serve(db).await;
    let local = Local::new(&[("sync.py", GOOD)]);
    let url2 = url.clone();
    let (code, out, err) = tokio::task::spawn_blocking(move || {
        local.check(&["--against", &url2, "--token", "s3cret"])
    })
    .await
    .unwrap();
    assert_eq!(code, 1, "{out}{err}");
    assert!(out.contains("ok     sync.py: sync (2 tasks)"), "{out}");
    let failure =
        format!("error: this schedr is {current}; older active workers at {url}: {older}: w-old\n");
    assert_eq!(err, failure);
    assert!(!err.contains("isn't implemented yet"), "{err}");
    assert!(!err.contains("w-lost"), "{err}");
    assert_eq!(seen.lock().unwrap().as_slice(), ["Bearer s3cret"]);

    let local = Local::new(&[("sync.py", GOOD)]);
    let url2 = url.clone();
    let (code, out, err) =
        tokio::task::spawn_blocking(move || local.check(&["--against", &url2, "--json"]))
            .await
            .unwrap();
    assert_eq!(code, 1, "{out}{err}");
    let report: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["version_check"]["ok"], false);
    assert_eq!(report["version_check"]["local"], current.to_string());
    assert_eq!(report["version_check"]["message"], failure.trim_end());
    let ids: Vec<&str> = report["version_check"]["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|worker| worker["worker_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["w-new", "w-old"]);
    assert!(!err.contains("isn't implemented yet"), "{err}");
}

#[tokio::test]
async fn check_against_passes_when_every_active_worker_is_current() {
    let current = SchedrVersion::current();
    let older = older_minor(current);
    let newer = newer_minor(current);
    assert!(
        older < current && newer > current,
        "{older} {current} {newer}"
    );
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    add_worker(
        &db,
        "w-old",
        &reported(older, 1),
        WorkerState::Disconnected,
        false,
    )
    .await;
    add_worker(
        &db,
        "w-same",
        &reported(current, 9),
        WorkerState::Active,
        false,
    )
    .await;
    add_worker(
        &db,
        "w-newer",
        &reported(newer, 0),
        WorkerState::Active,
        false,
    )
    .await;
    let (url, _) = serve(db).await;
    let local = Local::new(&[("sync.py", GOOD)]);
    let url2 = url.clone();
    let (code, out, err) = tokio::task::spawn_blocking(move || local.check(&["--against", &url2]))
        .await
        .unwrap();
    assert_eq!(code, 0, "{out}{err}");
    let summary = out.find("0 error(s)").expect(&out);
    let line = format!(
        "ok: this schedr is {current}; 2 active workers at {url} run {current} (w-same), {newer} (w-newer)"
    );
    let at = out.find(&line).expect(&out);
    assert!(summary < at, "{out}");
    assert!(!out.contains("w-old"), "{out}");
    assert!(!err.contains("error:"), "{err}");
}

#[tokio::test]
async fn check_against_fails_when_no_worker_is_active_or_the_server_is_down() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let (url, _) = serve(db).await;
    let local = Local::new(&[("sync.py", GOOD)]);
    let url2 = url.clone();
    let (code, out, err) = tokio::task::spawn_blocking(move || local.check(&["--against", &url2]))
        .await
        .unwrap();
    let local_version = SchedrVersion::current();
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("0 error(s)"), "{out}");
    assert_eq!(
        err,
        format!(
            "error: no active worker at {url} to compare with this schedr ({local_version}): \
             workers are recorded disconnected right after a master restart until they \
             register again, so retry in a minute\n"
        )
    );

    let local = Local::new(&[("sync.py", GOOD)]);
    let (code, out, err) =
        tokio::task::spawn_blocking(move || local.check(&["--against", "http://127.0.0.1:1"]))
            .await
            .unwrap();
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("ok     sync.py: sync (2 tasks)"), "{out}");
    assert!(
        err.contains("can't reach the schedr server at http://127.0.0.1:1"),
        "{err}"
    );
    assert!(!err.contains("isn't implemented yet"), "{err}");
}
