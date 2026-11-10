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
            let file = root.join(name);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, text).unwrap();
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
        self.dags_with(args, &[])
    }

    /// As [`Self::dags`], with more environment variables.
    fn dags_with(&self, args: &[&str], env: &[(&str, &str)]) -> (i32, String, String) {
        let output = Command::new(BINARY)
            .env_clear()
            .envs(env.iter().copied())
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
        "skipped helpers.py: it doesn't mention `schedr`, so it is never parsed (a file defining \
         DAGs needs at least `import schedr`)\n\
         ok     sync.py: sync (2 tasks)\n\
         1 file(s), 1 DAG(s), 0 error(s)\n"
    );
}

#[test]
fn check_reports_broken_and_invalid_files_and_fails() {
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("broken.py", "import schedr\nimport nowhere_to_be_found\n"),
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
fn check_says_a_wait_step_cant_start_a_process_with_its_line() {
    let dag = r#"import subprocess

from schedr import Dag, sensor


def helper() -> None:
    subprocess.run(["true"], check=True)


@sensor
async def waits() -> int:
    helper()
    return 1


with Dag("refused"):
    waits()
"#;
    let local = Local::new(&[("refused.py", dag)]);
    let (code, out, _) = local.check(&[]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("`subprocess.run`"), "{out}");
    assert!(out.contains("refused.py:7"), "{out}");
    assert!(out.contains("starts a process"), "{out}");
}

/// Every example together, as someone trying schedr points a deployment at them: one root
/// shares its connection names, which each example alone wouldn't show.
#[test]
fn the_examples_folder_is_one_good_dag_folder() {
    let local = Local::new(&[]);
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
    let (code, out, err) = local.dags_with(&["check", path_str(&examples)], &[]);
    assert_eq!(code, 0, "{out}\n{err}");
}

#[test]
fn check_skips_what_a_parse_skips_and_says_why() {
    let dag = "from schedr import Dag\n\nwith Dag(\"found\"):\n    pass\n";
    let local = Local::new(&[
        ("found.py", dag),
        ("library.py", "raise ValueError('run')\n"),
    ]);
    let root = local.root();
    let (code, out, _) = local.dags_with(&["check", path_str(&root)], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("ok     found.py: found"), "{out}");
    assert!(
        out.contains("skipped library.py: it doesn't mention `schedr`"),
        "{out}"
    );
    // Named on its own, as a deployment would treat it: never run.
    let library = root.join("library.py");
    let (code, out, _) = local.dags_with(&["check", path_str(&library)], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("skipped library.py"), "{out}");
    assert!(!out.contains("ValueError"), "{out}");
}

#[test]
fn check_looks_a_script_operator_up_in_the_configured_script_roots() {
    let dag = r#"
from schedr import Dag, ScriptOperator

with Dag("uses_script"):
    ScriptOperator("clean", "jobs:team/schedr.toml#daily")

with Dag("typo"):
    ScriptOperator("clean", "jobs:team/schedr.toml#dialy")
"#;
    let local = Local::new(&[("uses.py", dag)]);
    let jobs = local.dir.path().join("jobs");
    std::fs::create_dir_all(jobs.join("team")).unwrap();
    std::fs::write(jobs.join("team/report.py"), "").unwrap();
    std::fs::write(
        jobs.join("team/schedr.toml"),
        "[jobs.daily]\nscript = \"report.py\"\ninterpreter = \"python\"\n",
    )
    .unwrap();
    let root = local.root();
    let (code, out, _) = local.dags_with(
        &["check", path_str(&root)],
        &[("SCHEDR_SCRIPT_PATH", path_str(&jobs))],
    );
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("error  uses.py: LookupError: task `clean`: `jobs:team/schedr.toml` declares no job `dialy`"),
        "{out}"
    );
}

#[test]
fn check_with_json_gives_each_file() {
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("broken.py", "# schedr\nraise RuntimeError('boom')\n"),
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
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("broken.py", "# schedr\nraise RuntimeError\n"),
    ]);
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

/// A task posting an event through `schedr.api` posts to the test's own local schedr (M4.11h):
/// such a DAG passes `dags test` (a side note thought it would be refused).
#[test]
fn a_task_posting_an_event_passes_the_test() {
    let posting = r#"
from schedr import Dag, api, task

@task
def post() -> str:
    event = api.post_event("paid", key="o-1", data={"amount": 3})
    listed = api.events(name="paid")
    print("posted by", event.posted_by, "listed", len(listed))
    return event.key

with Dag("poster"):
    post()
"#;
    let local = Local::new(&[("poster.py", posting)]);
    let (code, out, err) = local.test("poster", &[]);
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(out.contains("posted by task:poster/"), "{out}");
    assert!(out.contains("listed 1"), "{out}");
    assert!(out.trim_end().ends_with(": success"), "{out}");
}

#[test]
fn test_names_a_mapped_tasks_outputs_by_instance() {
    let mapped = r#"
from schedr import Dag, task

@task
def double(x: int) -> int:
    return x * 2

with Dag("mapped"):
    double.expand(x=[1, 2])
"#;
    let local = Local::new(&[("mapped.py", mapped)]);
    let (code, out, err) = local.test("mapped", &[]);
    assert_eq!(code, 0, "{out}\n{err}");
    // The gathered list under the task's name, each instance's under its own (decision 86).
    let outputs: Vec<&str> = out
        .lines()
        .skip_while(|l| *l != "outputs:")
        .skip(1)
        .take_while(|l| l.starts_with("  "))
        .collect();
    assert_eq!(
        outputs,
        ["  double: [2,4]", "  double[0]: 2", "  double[1]: 4"],
        "{out}"
    );
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
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("broken.py", "# schedr\nraise RuntimeError\n"),
    ]);
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
            watcher_slots: 0,
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

#[test]
fn test_fails_at_once_when_its_root_cant_be_scanned() {
    // B103: a root its agent can't scan or copy (a file it can't read here) made `dags test`
    // retry for ever (macOS CI: two hours); it fails with the scan's error.
    use std::os::unix::fs::PermissionsExt;
    let local = Local::new(&[("sync.py", GOOD), ("rows.csv", "a,b\n")]);
    let unreadable = local.root().join("rows.csv");
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    let mut child = Command::new(BINARY)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", local.dir.path())
        .env("TZ", "UTC")
        .arg("--config")
        .arg(local.dir.path().join("config.toml"))
        .args(["dags", "test", "sync", "--path", path_str(&local.root())])
        .current_dir(local.dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(local.dir.path().join("stderr")).unwrap())
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break Some(status);
        }
        if started.elapsed() > std::time::Duration::from_secs(30) {
            child.kill().unwrap();
            child.wait().unwrap();
            break None;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o644)).unwrap();
    let err = std::fs::read_to_string(local.dir.path().join("stderr")).unwrap();
    let status = status.unwrap_or_else(|| panic!("still running after 30 s: {err}"));
    assert_eq!(status.code(), Some(1), "{err}");
    assert!(err.contains("can't scan DAG root `etl`"), "{err}");
    assert!(err.contains("rows.csv"), "{err}");
}

/// M4.6b: a command and a Python task read secrets given with `--secret`; their values are
/// masked in the logs and the stored output.
fn secrets_dag() -> String {
    let bin = BINARY;
    format!(
        r#"
from schedr import CommandOperator, Dag, SecretPrefix, secrets, task, task_ctx

@task(secrets=[SecretPrefix("tenants/")])
def py(password: str) -> str:
    print("password is", password)
    token = task_ctx.secret("tenants/a", "token")
    print("token is", token)
    return f"{{password}}/{{token}}"

@task
def undeclared() -> str:
    return task_ctx.secret("etl/db", "user")

@task(secrets=["tenants/b#token"])
def missing() -> str:
    return task_ctx.secret("tenants/b", "token")

with Dag("ok"):
    CommandOperator(
        "cmd",
        ["sh", "-c", '{bin} task-ctx secret get "etl/db#password"; echo; echo "env $PW"'],
        env={{"PW": secrets["etl/db"]["password"]}},
        secrets=["etl/db#password"],
    )
    py(secrets["etl/db"]["password"])

with Dag("denied"):
    undeclared()

with Dag("not_found"):
    missing()

with Dag("cmd_denied"):
    CommandOperator("read", ["{bin}", "task-ctx", "secret", "get", "etl/db#password"])

with Dag("cmd_ignored"):
    # A failed read the command gets past: the attempt succeeds (decision 168).
    CommandOperator("read", ["sh", "-c", '{bin} task-ctx secret get "etl/db#x" || true'])
"#
    )
}

const SECRET_ARGS: [&str; 4] = [
    "--secret",
    "etl/db#password=hunter22-pw",
    "--secret",
    "tenants/a#token=token-of-a",
];

#[test]
fn secrets_are_read_by_commands_and_python_tasks_and_masked() {
    let dag = secrets_dag();
    let local = Local::new(&[("secret_reads.py", dag.as_str())]);
    let mut args = vec!["--json"];
    args.extend(SECRET_ARGS);
    let (code, out, err) = local.test("ok", &args);
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(
        !out.contains("hunter22-pw") && !out.contains("token-of-a"),
        "{out}"
    );
    let report: Value = serde_json::from_str(&out).unwrap();
    let tasks = report["tasks"].as_array().unwrap();
    let cmd = &tasks[0];
    assert_eq!(cmd["state"], "success");
    let log = cmd["log"].as_str().unwrap();
    assert!(log.contains("****\nenv ****"), "{log}");
    assert!(
        log.contains("secrets['etl/db']['password']") && log.contains("masked"),
        "{log}"
    );
    let py = &tasks[1];
    assert_eq!(py["output"], "****/****");
    let log = py["log"].as_str().unwrap();
    assert!(
        log.contains("password is ****") && log.contains("token is ****"),
        "{log}"
    );
}

#[test]
fn a_secret_read_that_fails_fails_the_task_with_its_kind() {
    let dag = secrets_dag();
    let local = Local::new(&[("secret_reads.py", dag.as_str())]);
    for (dag, kind) in [
        ("denied", "secret_denied"),
        ("not_found", "secret_not_found"),
        ("cmd_denied", "secret_denied"),
    ] {
        let mut args = vec!["--json"];
        args.extend(SECRET_ARGS);
        let (code, out, err) = local.test(dag, &args);
        assert_eq!(code, 1, "{out}\n{err}");
        let report: Value = serde_json::from_str(&out).unwrap();
        let attempt = &report["tasks"][0]["attempts"][0];
        assert_eq!(attempt["error_kind"], kind, "{attempt}");
    }
    let (code, out, err) = local.test("cmd_ignored", &SECRET_ARGS);
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(out.contains("isn't declared"), "{out}");
}

/// A Python task that registers handles and fails, its cleanup writing what it got to `{out}`.
const CLEANED_UP: &str = r#"
from dataclasses import dataclass
from pathlib import Path

from schedr import Dag, cleanup_handle, task

@dataclass
class ExportJob:
    job_id: str

@dataclass
class TempDir:
    path: str

def undo(day: str, handles: list[ExportJob | TempDir]) -> None:
    Path("{out}").write_text(f"{day} {handles}")

@task(cleanup=undo)
def export(day: str, rows: int = 3) -> None:
    cleanup_handle(TempDir("/tmp/a"))
    cleanup_handle(ExportJob("j-1"))
    raise ValueError("no rows")

with Dag("cleaned"):
    export("2026-11-04")
"#;

const CLEANUP_FAILS: &str = r#"
from schedr import Dag, task

def undo() -> None:
    raise ValueError("can't clean")

@task(cleanup=undo, cleanup_retries=1, cleanup_retry_delay=__import__("datetime").timedelta(0))
def works() -> int:
    return 1

with Dag("unclean"):
    works()
"#;

/// A command registering a handle with the binary, its cleanup listing it into `{out}`.
const COMMAND_CLEANED_UP: &str = r#"
from schedr import CommandOperator, Dag

with Dag("command_cleaned"):
    CommandOperator(
        "export",
        "{schedr} task-ctx cleanup add ExportJob job_id=j-1 && exit 3",
        cleanup="{schedr} task-ctx cleanup list ExportJob --field job_id > {out}",
    )
"#;

/// A task that registers a handle its cleanup doesn't declare.
const OTHER_HANDLE: &str = r#"
from dataclasses import dataclass

from schedr import Dag, cleanup_handle, task

@dataclass
class ExportJob:
    job_id: str

@dataclass
class TempDir:
    path: str

def undo(handles: list[ExportJob]) -> None:
    pass

@task(cleanup=undo)
def export() -> None:
    cleanup_handle(TempDir("/tmp/a"))

with Dag("other_handle"):
    export()
"#;

#[test]
fn a_python_cleanup_gets_the_inputs_and_handles_of_a_failed_attempt() {
    let out = TempDir::new().unwrap();
    let file = out.path().join("cleaned");
    let text = CLEANED_UP.replace("{out}", path_str(&file));
    let local = Local::new(&[("cleaned.py", &text)]);
    let (code, stdout, stderr) = local.test("cleaned", &[]);
    assert_ne!(code, 0, "{stdout}\n{stderr}");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "2026-11-04 [TempDir(path='/tmp/a'), ExportJob(job_id='j-1')]"
    );
}

#[test]
fn a_failed_cleanup_fails_the_run_of_a_task_that_worked() {
    let local = Local::new(&[("unclean.py", CLEANUP_FAILS)]);
    let (code, stdout, stderr) = local.test("unclean", &[]);
    assert_ne!(code, 0, "{stdout}\n{stderr}");
    assert!(stdout.trim_end().ends_with(": failed"), "{stdout}");
    assert!(stdout.contains("cleanup: try 2"), "{stdout}");
}

#[test]
fn a_command_registers_and_its_cleanup_reads_handles_with_the_binary() {
    let out = TempDir::new().unwrap();
    let file = out.path().join("cleaned");
    let text = COMMAND_CLEANED_UP
        .replace("{schedr}", BINARY)
        .replace("{out}", path_str(&file));
    let local = Local::new(&[("command_cleaned.py", &text)]);
    let (code, stdout, stderr) = local.test("command_cleaned", &[]);
    assert_ne!(code, 0, "{stdout}\n{stderr}");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "j-1\n");
}

#[test]
fn a_handle_of_a_type_the_cleanup_doesnt_take_fails_the_attempt() {
    let local = Local::new(&[("other_handle.py", OTHER_HANDLE)]);
    let (code, stdout, stderr) = local.test("other_handle", &[]);
    assert_ne!(code, 0, "{stdout}\n{stderr}");
    assert!(stdout.contains("TempDir"), "{stdout}");
}

#[test]
fn a_handle_for_a_cleanup_without_a_handles_parameter_fails_the_attempt() {
    let text = OTHER_HANDLE.replace("def undo(handles: list[ExportJob])", "def undo()");
    let local = Local::new(&[("other_handle.py", &text)]);
    let (code, stdout, stderr) = local.test("other_handle", &[]);
    assert_ne!(code, 0, "{stdout}\n{stderr}");
    assert!(stdout.contains("takes no handles"), "{stdout}");
}

/// As [`Local::new`], with more configuration after the interpreter.
fn local_with(files: &[(&str, &str)], interpreter: &str) -> Local {
    let local = Local::new(files);
    let python = std::env::var("SCHEDR_TEST_PYTHON").unwrap();
    std::fs::write(
        local.dir.path().join("config.toml"),
        interpreter.replace("@python@", &python),
    )
    .unwrap();
    local
}

const FORKED: &str = "[interpreters.python]\npath = \"@python@\"\nfork_server = true\n\
                      preload = [\"wave\"]\n";

/// A DAG file that says whether it was forked from a server preloading `wave`.
const WAVE_LOADED: &str = r#"import sys

from schedr import Dag

assert "wave" in sys.modules, "not forked from the preloaded server"

with Dag("preloaded"):
    pass
"#;

#[test]
fn check_forks_from_the_configured_fork_server() {
    // A check runs as tasks will (decision 326).
    let local = local_with(&[("preloaded.py", WAVE_LOADED)], FORKED);
    let (code, out, err) = local.check(&[]);
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(out.contains("1 DAG(s), 0 error(s)"), "{out}");
}

#[test]
fn check_spawns_without_a_fork_server() {
    let local = Local::new(&[("preloaded.py", WAVE_LOADED)]);
    let (code, out, _) = local.check(&[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("not forked from the preloaded server"),
        "{out}"
    );
}

#[test]
fn a_root_module_named_as_a_preloaded_library_is_a_clear_error_on_both_start_methods() {
    // B175: forked, the preloaded library would win silently over the root's own module.
    let files = [
        ("wave.py", "CUSTOM = True\n"),
        (
            "main.py",
            "import wave\n\nfrom schedr import Dag\n\nwith Dag(\"m\", description=str(wave)):\n    pass\n",
        ),
    ];
    for local in [Local::new(&files), local_with(&files, FORKED)] {
        let (code, out, err) = local.check(&[]);
        assert_eq!(code, 1, "{out}\n{err}");
        assert!(out.contains("main.py"), "{out}");
        assert!(out.contains("`wave`"), "{out}");
        assert!(out.contains("0 DAG(s), 1 error(s)"), "{out}");
    }
}

#[test]
fn a_file_named_as_what_the_runner_imports_cant_stop_it() {
    // B178: the root isn't first on `sys.path` while the runner starts.
    let local = Local::new(&[
        ("sync.py", GOOD),
        ("typing.py", "raise RuntimeError('mine')\n"),
    ]);
    let (code, out, err) = local.check(&[]);
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(out.contains("ok     sync.py: sync"), "{out}");
}

#[test]
fn check_cant_import_what_a_worker_doesnt_get() {
    // B179: `.schedrignore`'s files aren't in a worker's copy, so not in a check's either.
    let local = Local::new(&[
        (".schedrignore", "ignored/\n"),
        ("ignored/__init__.py", "X = 1\n"),
        (
            "main.py",
            "import ignored\n\nfrom schedr import Dag\n\nwith Dag(\"m\"):\n    pass\n",
        ),
    ]);
    let (code, out, _) = local.check(&[]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("No module named 'ignored'"), "{out}");
}

#[test]
fn check_and_test_leave_no_copies_behind() {
    // B181: the copies are read-only; removing them takes more than `remove_dir_all`.
    let local = Local::new(&[("sync.py", GOOD)]);
    let temp = local.dir.path().join("temp");
    std::fs::create_dir_all(&temp).unwrap();
    let env = [("TMPDIR", path_str(&temp))];
    let root = local.root();
    let (code, out, err) = local.dags_with(&["check", path_str(&root)], &env);
    assert_eq!(code, 0, "{out}\n{err}");
    let (code, out, err) = local.dags_with(&["test", "sync", "--path", path_str(&root)], &env);
    assert_eq!(code, 0, "{out}\n{err}");
    let left: Vec<_> = std::fs::read_dir(&temp)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(left, Vec::<std::ffi::OsString>::new());
}

#[test]
fn files_parsed_at_once_are_reported_in_order_with_a_slow_one_timed_out_alone() {
    let local = local_with(
        &[
            (
                "a.py",
                "from schedr import Dag\n\nwith Dag(\"a\"):\n    pass\n",
            ),
            ("b.py", "# schedr\nimport time\n\ntime.sleep(60)\n"),
            (
                "c.py",
                "from schedr import Dag\n\nwith Dag(\"c\"):\n    pass\n",
            ),
        ],
        "[interpreters]\npython = \"@python@\"\n[dags]\nparse_concurrency = 3\nparse_file_timeout = \"3s\"\n",
    );
    let (code, out, _) = local.check(&[]);
    assert_eq!(code, 1, "{out}");
    let a = out.find("a.py").unwrap();
    let b = out.find("b.py").unwrap();
    let c = out.find("c.py").unwrap();
    assert!(a < b && b < c, "{out}");
    assert!(out.contains("2 DAG(s), 1 error(s)"), "{out}");
}

const CONNECTION_LIB: &str = r"from schedr import connection

@connection()
def warehouse():
    yield 1
";

const USES_CONNECTION: &str = r#"from lib.connections import warehouse

from schedr import Dag, task


@task(connections=[warehouse])
def load() -> None:
    pass


with Dag("sync"):
    load()
"#;

#[test]
fn a_folder_of_a_root_sees_the_connections_of_the_whole_root() {
    let local = Local::new(&[
        ("lib/connections.py", CONNECTION_LIB),
        ("team/job.py", USES_CONNECTION),
    ]);
    let root = local.root();
    let env = [("SCHEDR_DAG_PATH", path_str(&root))];
    let folder = root.join("team");
    let (code, out, err) = local.dags_with(&["check", path_str(&root)], &env);
    assert_eq!(code, 0, "{out}\n{err}");
    let (code, out, err) = local.dags_with(&["check", path_str(&folder)], &env);
    assert_eq!(code, 0, "{out}\n{err}");
    // Only what was named is reported.
    assert!(out.contains("1 file(s), 1 DAG(s), 0 error(s)"), "{out}");
    assert!(!out.contains("lib/connections.py"), "{out}");
    let (code, out, err) = local.dags_with(&["test", "sync", "--path", path_str(&folder)], &env);
    assert_eq!(code, 0, "{out}\n{err}");
}

#[test]
fn pass_env_reaches_what_parses_and_runs_on_both_start_methods() {
    // A `PYTHONPATH` in `pass_env` gave a spawned task one module, and a parse and a forked task
    // another.
    let dag = r#"import extra_helper

from schedr import Dag, task


@task
def uses() -> None:
    assert extra_helper.VALUE == "extra"


with Dag("uses_extra"):
    uses()
"#;
    let extra = TempDir::new().unwrap();
    std::fs::write(extra.path().join("extra_helper.py"), "VALUE = \"extra\"\n").unwrap();
    let env = [("PYTHONPATH", path_str(extra.path()))];
    for interpreter in [
        "[interpreters]\npython = \"@python@\"\n[worker]\npass_env = [\"PYTHONPATH\"]\n",
        "[interpreters.python]\npath = \"@python@\"\nfork_server = true\npreload = [\"wave\"]\n\
         [worker]\npass_env = [\"PYTHONPATH\"]\n",
    ] {
        let local = local_with(&[("uses.py", dag)], interpreter);
        let root = local.root();
        let (code, out, err) = local.dags_with(&["check", path_str(&root)], &env);
        assert_eq!(code, 0, "{interpreter}{out}\n{err}");
        let (code, out, err) =
            local.dags_with(&["test", "uses_extra", "--path", path_str(&root)], &env);
        assert_eq!(code, 0, "{interpreter}{out}\n{err}");
    }
}

#[test]
fn a_failing_preload_leaves_nothing_behind_either() {
    // Its server keeps retrying: not once its check has ended, which would make the folder anew.
    let dag = "import time\n\nfrom schedr import Dag, task\n\n\n@task\ndef slow() -> None:\n    \
               time.sleep(3)\n\n\nwith Dag(\"slow\"):\n    slow()\n";
    let local = local_with(
        &[("slow.py", dag)],
        "[interpreters.python]\npath = \"@python@\"\nfork_server = true\n\
         preload = [\"no_such_library\"]\n",
    );
    let temp = local.dir.path().join("temp");
    std::fs::create_dir_all(&temp).unwrap();
    let root = local.root();
    let (code, out, err) = local.dags_with(
        &["test", "slow", "--path", path_str(&root)],
        &[("TMPDIR", path_str(&temp))],
    );
    assert_eq!(code, 0, "{out}\n{err}");
    let left: Vec<_> = std::fs::read_dir(&temp)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(left, Vec::<std::ffi::OsString>::new());
}

#[test]
fn a_wait_step_reads_its_secrets_and_they_are_masked() {
    let dag = r#"
from schedr import Dag, sensor, task_ctx


@sensor(secrets=["etl/db#password"])
async def reads() -> str:
    value = task_ctx.secret("etl/db", "password")
    print(value)
    return "len " + str(len(value))


with Dag("reads"):
    reads()
"#;
    let local = Local::new(&[("reads.py", dag)]);
    let (code, out, err) = local.test(
        "reads",
        &["--json", "--secret", "etl/db#password=hunter22-pw"],
    );
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(!out.contains("hunter22-pw"), "{out}");
    assert!(out.contains("len 11"), "{out}");
}

#[test]
fn a_wait_step_reads_a_secret_under_its_prefix_without_holding_the_loop() {
    // Another coroutine keeps turning while the read waits on a thread; the sync read is there too.
    let dag = r#"
import asyncio

from schedr import Dag, SecretPrefix, sensor, task_ctx


@sensor(secrets=[SecretPrefix("etl/")])
async def reads() -> str:
    value = await task_ctx.asecret("etl/db", "password")
    same = await task_ctx.asecret_value("etl/db#password")
    assert value == same == task_ctx.secret("etl/db", "password")
    print(value)
    await asyncio.sleep(0)
    return "len " + str(len(value))


with Dag("reads"):
    reads()
"#;
    let local = Local::new(&[("reads.py", dag)]);
    let (code, out, err) = local.test(
        "reads",
        &["--json", "--secret", "etl/db#password=hunter22-pw"],
    );
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(!out.contains("hunter22-pw"), "{out}");
    assert!(out.contains("len 11"), "{out}");
}

#[test]
fn task_exec_caps_the_programs_data_and_runs_it_in_its_place() {
    let python = std::env::var("SCHEDR_TEST_PYTHON").expect("SCHEDR_TEST_PYTHON");
    let run = |megabytes: u32| {
        Command::new(BINARY)
            .args([
                "task-exec",
                "--data-limit",
                &(256_u64 << 20).to_string(),
                "--",
                &python,
            ])
            .args([
                "-c",
                &format!("import os; x = bytearray({megabytes} << 20); print(os.getpid())"),
            ])
            .output()
            .unwrap()
    };
    let within = run(64);
    assert!(
        within.status.success(),
        "{}",
        String::from_utf8_lossy(&within.stderr)
    );
    let over = run(512);
    assert!(!over.status.success());
    assert!(
        String::from_utf8_lossy(&over.stderr).contains("MemoryError"),
        "{over:?}"
    );
    // A program that isn't there: said so, exit 127.
    let missing = Command::new(BINARY)
        .args([
            "task-exec",
            "--data-limit",
            "1000000000",
            "--",
            "/no/such/program",
        ])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(127));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("can't start `/no/such/program`"));
}
