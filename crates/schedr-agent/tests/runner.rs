//! The real Python runner, started by the agent (PLAN.md 1.4, 1.7, 1.8): parse mode over a
//! directory of DAG files with the per-file timeout, task mode with inputs, failures, timeouts
//! and cancellation.
//!
//! Needs `SCHEDR_TEST_PYTHON`, a Python with `schedr` installed (`make test-integration` sets
//! it).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_agent::launch::{ProcessParseRunner, run_python};
use schedr_agent::parse::{RUNNER_DIED_EXCEPTION, TIMEOUT_EXCEPTION, parse_files};
use schedr_agent::process::ProcessSpec;
use schedr_agent::status::StopReason;
use schedr_agent::supervise::Limits;
use schedr_model::runner::{
    DagFile, ParseRequest, ParsedFile, RunInfo, RunnerMessage, TaskRequest,
};
use schedr_model::{Dag, RunKind};
use serde_json::json;
use tempfile::TempDir;

fn python() -> PathBuf {
    std::env::var_os("SCHEDR_TEST_PYTHON")
        .map(PathBuf::from)
        .expect("SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)")
}

fn runner(mode: &str, root: &Path, log: &Path) -> ProcessSpec {
    ProcessSpec {
        program: python(),
        args: vec!["-m".to_owned(), "schedr.runner".to_owned(), mode.to_owned()],
        env: BTreeMap::from([("PATH".to_owned(), std::env::var("PATH").unwrap_or_default())]),
        working_dir: root.to_path_buf(),
        log_file: log.to_path_buf(),
        max_output_size: 64 * 1024,
    }
}

const DAGS: &str = r#"
import time
from schedr import Dag, task

@task
def extract() -> dict[str, int]:
    return {"rows": 3}

@task
def load(rows: int) -> str:
    return f"loaded {rows}"

@task
def boom() -> None:
    raise ValueError("no rows")

@task
def slow() -> None:
    time.sleep(30)

with Dag("sync"):
    load(extract()["rows"])
    boom()
    slow()
"#;

/// A root `etl` with the test DAG files.
fn root() -> TempDir {
    let dir = TempDir::new().unwrap();
    let files = [
        ("a_sync.py", DAGS),
        ("b_helper.py", "VALUE = 1\n"),
        ("c_broken.py", "raise RuntimeError('import boom')\n"),
        ("d_hang.py", "import time\ntime.sleep(30)\n"),
        (
            "e_after.py",
            "from schedr import Dag\nwith Dag('after'):\n    pass\n",
        ),
        ("f_exits.py", "import os\nos._exit(3)\n"),
        (
            "g_last.py",
            "from schedr import Dag\nwith Dag('last'):\n    pass\n",
        ),
    ];
    for (name, text) in files {
        std::fs::write(dir.path().join(name), text).unwrap();
    }
    dir
}

async fn parse(dir: &Path, per_file: Duration) -> Vec<ParsedFile> {
    let mut runner = ProcessParseRunner {
        process: runner("parse", dir, &dir.join("parse.log")),
    };
    let mut files: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| Path::new(name).extension().is_some_and(|e| e == "py"))
        .collect();
    files.sort();
    let request = ParseRequest {
        root: "etl".to_owned(),
        root_dir: dir.to_string_lossy().into_owned(),
        files,
        sys_path: vec![dir.to_string_lossy().into_owned()],
    };
    parse_files(&mut runner, request, per_file).await.unwrap()
}

#[tokio::test]
async fn parse_mode_reports_every_file_and_survives_hangs_and_crashes() {
    let dir = root();
    let files = parse(dir.path(), Duration::from_secs(3)).await;
    let summary: Vec<(&str, Vec<&str>, Option<&str>)> = files
        .iter()
        .map(|f| {
            (
                f.path.as_str(),
                f.dags.iter().map(|d| d.id.as_str()).collect(),
                f.error.as_ref().map(|e| e.exception.as_str()),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("a_sync.py", vec!["sync"], None),
            ("b_helper.py", vec![], None),
            ("c_broken.py", vec![], Some("RuntimeError")),
            ("d_hang.py", vec![], Some(TIMEOUT_EXCEPTION)),
            ("e_after.py", vec!["after"], None),
            ("f_exits.py", vec![], Some(RUNNER_DIED_EXCEPTION)),
            ("g_last.py", vec!["last"], None),
        ]
    );
    let sync = &files[0].dags[0];
    assert_eq!(
        (sync.location.root.as_str(), sync.location.path.as_str()),
        ("etl", "a_sync.py")
    );
    assert!(dir.path().join("parse.log").exists());
}

/// DAG `sync` as the real runner parses it.
async fn the_dag() -> Dag {
    let only = TempDir::new().unwrap();
    std::fs::write(only.path().join("a_sync.py"), DAGS).unwrap();
    parse(only.path(), Duration::from_secs(10)).await[0].dags[0].clone()
}

fn request(dir: &Path, dag: &Dag, task: &str, inputs: serde_json::Value) -> TaskRequest {
    let planned_for = "2026-10-21T08:00:00Z".parse().unwrap();
    TaskRequest {
        dag_file: DagFile {
            root: "etl".to_owned(),
            root_dir: dir.to_string_lossy().into_owned(),
            path: "a_sync.py".to_owned(),
        },
        dag_id: "sync".to_owned(),
        task: dag
            .tasks
            .iter()
            .find(|t| t.id.as_str() == task)
            .cloned()
            .unwrap_or_else(|| {
                let mut ghost = dag.tasks[0].clone();
                ghost.id = schedr_model::TaskId::new(task).unwrap();
                ghost
            }),
        inputs: serde_json::from_value(inputs).unwrap(),
        run: RunInfo {
            dag_id: "sync".to_owned(),
            run_id: "r1".to_owned(),
            kind: RunKind::Manual,
            planned_for,
            since: planned_for,
            try_number: 1,
            map_index: None,
        },
        sys_path: vec![dir.to_string_lossy().into_owned()],
    }
}

async fn run(
    dir: &Path,
    request: TaskRequest,
    limits: Limits,
    stop: StopAfter,
) -> schedr_agent::attempt::Ran {
    let process = runner("task", dir, &dir.join("task.log"));
    let stop = async move {
        match stop {
            StopAfter::Never => std::future::pending().await,
            StopAfter::Cancel(after) => {
                tokio::time::sleep(after).await;
                StopReason::Cancelled
            }
        }
    };
    run_python(&process, request, limits, stop, |pid| assert!(pid > 1))
        .await
        .unwrap()
}

enum StopAfter {
    Never,
    Cancel(Duration),
}

const LIMITS: Limits = Limits {
    timeout: Some(Duration::from_secs(30)),
    grace: Duration::from_secs(5),
};

#[tokio::test]
async fn task_mode_runs_a_task_with_its_inputs() {
    let dir = root();
    let dag = the_dag().await;
    let inputs = json!({"extract": {"rows": 3}});
    let ran = run(
        dir.path(),
        request(dir.path(), &dag, "load", inputs),
        LIMITS,
        StopAfter::Never,
    )
    .await;
    assert_eq!(
        ran.runner_result,
        Some(RunnerMessage::TaskSucceeded {
            output: json!("loaded 3"),
            outcome: "SUCCESS".to_owned()
        })
    );
    assert_eq!(
        ran.supervised.termination,
        schedr_agent::status::Termination::Exited(0)
    );
}

#[tokio::test]
async fn task_mode_reports_the_tasks_failure_and_a_missing_task() {
    let dir = root();
    let dag = the_dag().await;
    let ran = run(
        dir.path(),
        request(dir.path(), &dag, "boom", json!({})),
        LIMITS,
        StopAfter::Never,
    )
    .await;
    let Some(RunnerMessage::TaskFailed { error, kind }) = ran.runner_result else {
        panic!("expected TaskFailed, got {:?}", ran.runner_result)
    };
    assert_eq!(
        (error.exception.as_str(), error.message.as_str(), kind),
        ("ValueError", "no rows", None)
    );
    assert_eq!(
        ran.supervised.termination,
        schedr_agent::status::Termination::Exited(1)
    );

    let ran = run(
        dir.path(),
        request(dir.path(), &dag, "ghost", json!({})),
        LIMITS,
        StopAfter::Never,
    )
    .await;
    let Some(RunnerMessage::TaskFailed { kind, .. }) = ran.runner_result else {
        panic!("expected TaskFailed")
    };
    assert_eq!(kind.as_deref(), Some("task_not_found"));
}

#[tokio::test]
async fn task_mode_stops_a_task_past_its_timeout_or_when_cancelled() {
    let dir = root();
    let dag = the_dag().await;
    let short = Limits {
        timeout: Some(Duration::from_millis(1500)),
        grace: Duration::from_secs(2),
    };
    let ran = run(
        dir.path(),
        request(dir.path(), &dag, "slow", json!({})),
        short,
        StopAfter::Never,
    )
    .await;
    assert_eq!(ran.supervised.stopped, Some(StopReason::TimedOut));
    assert_eq!(ran.runner_result, None);

    let ran = run(
        dir.path(),
        request(dir.path(), &dag, "slow", json!({})),
        LIMITS,
        StopAfter::Cancel(Duration::from_millis(1500)),
    )
    .await;
    assert_eq!(ran.supervised.stopped, Some(StopReason::Cancelled));
}
