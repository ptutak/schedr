//! `schedr dags check` and `schedr dags test` (PLAN.md 1.15): the real binary over temporary
//! DAG files, with the real Python runner and no server.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed); `make test-integration`
//! sets it.

use std::path::Path;
use std::process::Command;

use pretty_assertions::assert_eq;
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

with Dag("bad_cron", schedule=Cron("61 3 * * *")):
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
