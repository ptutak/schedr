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
use schedr_agent::supervise::{Limits, Stops};
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
        mask: schedr_agent::mask::Mask::default(),
        start_timeout: schedr_agent::launch::RUNNER_START_TIMEOUT,
        output_drain: schedr_agent::launch::OUTPUT_DRAIN_TIMEOUT,
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

@task
def nap() -> None:
    time.sleep(1)

with Dag("sync"):
    load(extract()["rows"])
    boom()
    slow()
    nap()
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
    parse_watched(dir, per_file, schedr_agent::memory::MemoryWatch::default()).await
}

async fn parse_watched(
    dir: &Path,
    per_file: Duration,
    memory: schedr_agent::memory::MemoryWatch,
) -> Vec<ParsedFile> {
    let runner = ProcessParseRunner {
        process: runner("parse", dir, &dir.join("parse.log")),
        memory,
        kills: prometheus_client::metrics::counter::Counter::default(),
        forks: None,
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
    parse_files(&runner, request, per_file).await.unwrap()
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
        trigger: None,
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
        outcomes: std::collections::BTreeMap::new(),
        run: RunInfo {
            event: None,
            params: serde_json::json!({}),
            dag_id: "sync".to_owned(),
            run_id: "r1".to_owned(),
            kind: RunKind::Manual,
            planned_for,
            since: planned_for,
            try_number: 1,
            map_index: None,
        },
        sys_path: vec![dir.to_string_lossy().into_owned()],
        secrets: Vec::new(),
        connection_test: None,
        cleanup: None,
    }
}

async fn run(
    dir: &Path,
    request: TaskRequest,
    limits: Limits,
    stop: StopAfter,
) -> schedr_agent::attempt::Ran {
    let process = runner("task", dir, &dir.join("task.log"));
    let (sender, stops) = Stops::new();
    if let StopAfter::Cancel(after) = stop {
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let _ = sender.send(StopReason::Cancelled);
        });
    }
    let stop = stops;
    run_python(
        &process,
        request,
        limits,
        stop,
        None,
        &mut None,
        |pid, forked: bool| {
            assert!(pid > 1 && !forked);
        },
    )
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
    interrupt_grace: Duration::from_secs(5),
    poll: schedr_agent::supervise::STOP_POLL,
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
    // Both phases of the start timed, each within the run.
    let start = ran.start.expect("a Python task's start is timed");
    let import = start.import.expect("the DAG file was imported");
    assert!(start.runner > Duration::ZERO && start.runner < ran.duration);
    assert!(import > Duration::ZERO && start.runner + import < ran.duration);
}

#[tokio::test]
async fn task_mode_times_the_import_apart_from_the_task() {
    let dir = root();
    let dag = the_dag().await;
    let ran = run(
        dir.path(),
        request(dir.path(), &dag, "nap", json!({})),
        LIMITS,
        StopAfter::Never,
    )
    .await;
    assert!(matches!(
        ran.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    // The import ends before the task's 1 s nap, not with the result after it.
    let import = ran
        .start
        .and_then(|s| s.import)
        .expect("the import is timed");
    assert!(import < Duration::from_secs(1), "{import:?}");
    assert!(ran.duration > Duration::from_secs(1), "{:?}", ran.duration);
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
    // Its runner started, but nothing was imported to run.
    let start = ran.start.expect("a Python task's start is timed");
    assert_eq!(start.import, None);
}

#[tokio::test]
async fn task_mode_stops_a_task_past_its_timeout_or_when_cancelled() {
    let dir = root();
    let dag = the_dag().await;
    let short = Limits {
        timeout: Some(Duration::from_millis(1500)),
        grace: Duration::from_secs(2),
        interrupt_grace: Duration::from_secs(2),
        poll: schedr_agent::supervise::STOP_POLL,
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

#[tokio::test]
async fn a_file_whose_import_takes_too_much_memory_is_an_import_error_and_parsing_goes_on() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("a_greedy.py"),
        "import time\nchunks = []\nfor _ in range(40):\n    chunks.append(b'x' * (10 << 20))\n    \
         time.sleep(0.05)\ntime.sleep(30)\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b_fine.py"),
        "from schedr import Dag\nwith Dag('fine'):\n    pass\n",
    )
    .unwrap();
    let memory = schedr_agent::memory::MemoryWatch {
        budget: Some(100 << 20),
        interval: Duration::from_millis(100),
        ..schedr_agent::memory::MemoryWatch::default()
    };
    let files = parse_watched(dir.path(), Duration::from_secs(20), memory).await;
    let error = files[0].error.as_ref().unwrap();
    assert!(
        error
            .message
            .contains("the runner exceeded memory budget 100 MiB ("),
        "{error:?}"
    );
    assert_eq!(files[1].dags[0].id.as_str(), "fine");
}

/// A cancel or the timeout reaching a runner before its hello stops it then, reported as
/// that, not as a failed start 30 s later (B56).
#[tokio::test]
async fn a_cancel_or_timeout_during_the_runners_start_stops_it() {
    let dir = TempDir::new().unwrap();
    let dag = the_dag().await;
    let silent = ProcessSpec {
        program: PathBuf::from("/bin/sh"),
        args: vec!["-c".to_owned(), "sleep 30".to_owned()],
        env: BTreeMap::new(),
        working_dir: dir.path().to_path_buf(),
        log_file: dir.path().join("silent.log"),
        max_output_size: 1024,
        mask: schedr_agent::mask::Mask::default(),
        start_timeout: schedr_agent::launch::RUNNER_START_TIMEOUT,
        output_drain: schedr_agent::launch::OUTPUT_DRAIN_TIMEOUT,
    };
    let limits = |timeout| Limits {
        timeout,
        grace: Duration::from_secs(1),
        interrupt_grace: Duration::from_secs(1),
        poll: schedr_agent::supervise::STOP_POLL,
    };
    for (timeout, cancel, reason) in [
        (None, true, StopReason::Cancelled),
        (
            Some(Duration::from_millis(300)),
            false,
            StopReason::TimedOut,
        ),
    ] {
        let (sender, stops) = Stops::new();
        if cancel {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = sender.send(StopReason::Cancelled);
            });
        }
        let request = request(dir.path(), &dag, "slow", json!({}));
        let mut hold = None;
        let running = run_python(
            &silent,
            request,
            limits(timeout),
            stops,
            None,
            &mut hold,
            |_, _| {},
        );
        let ran = tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .expect("stopped within its grace, not after the 30 s start timeout")
            .expect("a stopped start is a run, not a failed start");
        assert_eq!(ran.supervised.stopped, Some(reason));
        assert_eq!(ran.runner_result, None);
        assert_eq!(ran.start, None);
    }
}

/// Codex's review of B56: the reason a start was stopped for is the one recorded, before
/// another queued meanwhile; a stopped start doesn't wait forever for output held by a
/// process that escaped the runner's group; and the request is sent within the timeout.
#[tokio::test]
async fn a_stopped_start_keeps_its_reason_and_ends_on_time() {
    let dir = TempDir::new().unwrap();
    let dag = the_dag().await;
    let shell = |script: &str| ProcessSpec {
        program: PathBuf::from("/bin/sh"),
        args: vec!["-c".to_owned(), script.to_owned()],
        env: BTreeMap::from([("PATH".to_owned(), std::env::var("PATH").unwrap_or_default())]),
        working_dir: dir.path().to_path_buf(),
        log_file: dir.path().join("start.log"),
        max_output_size: 1024,
        mask: schedr_agent::mask::Mask::default(),
        start_timeout: schedr_agent::launch::RUNNER_START_TIMEOUT,
        output_drain: schedr_agent::launch::OUTPUT_DRAIN_TIMEOUT,
    };
    let limits = |timeout| Limits {
        timeout,
        grace: Duration::from_secs(1),
        interrupt_grace: Duration::from_secs(1),
        poll: schedr_agent::supervise::STOP_POLL,
    };
    let start = async |spec: &ProcessSpec, stops, timeout, inputs| {
        let mut hold = None;
        let running = run_python(
            spec,
            request(dir.path(), &dag, "slow", inputs),
            limits(timeout),
            stops,
            None,
            &mut hold,
            |_, _| {},
        );
        let began = std::time::Instant::now();
        let ran = tokio::time::timeout(Duration::from_secs(12), running)
            .await
            .expect("ended on time")
            .unwrap_or_else(|e| {
                let log = std::fs::read_to_string(dir.path().join("start.log"));
                panic!("{e}: {log:?}")
            });
        (ran, began.elapsed())
    };
    // A cancel, then a drain's interrupt, both before the hello: cancelled.
    let (sender, stops) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    sender.send(StopReason::Interrupted).unwrap();
    let (ran, _) = start(&shell("sleep 30"), stops, None, json!({})).await;
    assert_eq!(ran.supervised.stopped, Some(StopReason::Cancelled));
    // A process in a session of its own holds the output open: given up after a while.
    let (sender, stops) = Stops::new();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = sender.send(StopReason::Cancelled);
    });
    let escaped = shell("setsid sleep 20 & sleep 30");
    let (ran, took) = start(&escaped, stops, None, json!({})).await;
    assert_eq!(ran.supervised.stopped, Some(StopReason::Cancelled));
    assert!(took < Duration::from_secs(9), "{took:?}");
    // A runner that says hello, then reads nothing: a big request can't be sent, and the
    // timeout still stops it.
    let deaf = ProcessSpec {
        program: python(),
        args: vec![
            "-c".to_owned(),
            "import os, time\n\
             from importlib.metadata import version\n\
             from schedr.runner.protocol import from_fd\n\
             c = from_fd(int(os.environ['SCHEDR_AGENT_FD']))\n\
             c.send({'type': 'hello', 'schedr_version': version('schedr'), 'mode': 'task'})\n\
             c.receive()\n\
             time.sleep(30)\n"
                .to_owned(),
        ],
        ..shell("")
    };
    let (_, stops) = Stops::new();
    let big = json!({"rows": "x".repeat(4 << 20)});
    let (ran, took) = start(&deaf, stops, Some(Duration::from_millis(500)), big).await;
    assert_eq!(ran.supervised.stopped, Some(StopReason::TimedOut));
    assert!(took < Duration::from_secs(5), "{took:?}");
}

/// A connections test run by the real executor, at the copy of the code it was sent for (the
/// root's own folder no longer having the file), ending by its deadline whatever its check
/// does.
#[tokio::test]
async fn a_connection_test_runs_at_its_code_and_ends_by_its_deadline() {
    use std::sync::Arc;

    use schedr_agent::agent::{Executor, ProcessExecutor};
    use schedr_agent::attempt::WorkerSetup;
    use schedr_agent::connection_jobs::ConnectionJob;
    use schedr_agent::metrics::AgentMetrics;
    use schedr_agent::secrets::SecretStores;
    use schedr_model::connection::Registry;

    let dir = TempDir::new().unwrap();
    let configured = dir.path().join("etl");
    let code = dir.path().join("code");
    std::fs::create_dir_all(&configured).unwrap();
    std::fs::create_dir_all(&code).unwrap();
    std::fs::write(
        code.join("conns.py"),
        "import time\n\
         from collections.abc import Iterator\n\
         from schedr.connections import connection\n\n\
         @connection()\n\
         def fine() -> Iterator[str]:\n    yield 'x'\n\n\
         @connection()\n\
         def slow() -> Iterator[str]:\n    yield 'x'\n\n\
         @slow.check\n\
         def _wait(x: str) -> None:\n    time.sleep(60)\n",
    )
    .unwrap();
    let executor = ProcessExecutor {
        setup: WorkerSetup {
            roots: BTreeMap::from([("etl".to_owned(), configured)]),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::default(),
            python: python(),
            agent_env: BTreeMap::from([(
                "PATH".to_owned(),
                std::env::var("PATH").unwrap_or_default(),
            )]),
            pass_env: vec![],
            log_dir: dir.path().join("logs"),
            connection_dir: dir.path().join("connections"),
            memory: schedr_agent::memory::MemoryWatch::default(),
            timeouts: schedr_agent::attempt::WorkerTimeouts::default(),
            fork_server: None,
        },
        parse: schedr_agent::parse::ParseLimits::one_at_a_time(Duration::from_secs(5)),
        metrics: AgentMetrics::default(),
        drain_terminate_period: Duration::from_secs(1),
        forks: None,
        watchers: schedr_agent::watchers::WatcherHosts::default(),
        secrets: Arc::new(SecretStores::none()),
    };
    let registry: Registry = serde_json::from_value(json!({
        "connections": {"fine": {"name": "fine", "params": {}},
                        "slow": {"name": "slow", "params": {}}},
        "files": {"fine": "conns.py", "slow": "conns.py"},
    }))
    .unwrap();
    let job = |name: &str, allowed: Duration| ConnectionJob {
        root: "etl".to_owned(),
        code_dir: Some(code.clone()),
        registry: Arc::new(registry.clone()),
        test: Some((name.to_owned(), Duration::from_secs(600))),
        cancel_grace: schedr_agent::connection_jobs::CANCEL_GRACE,
        attempt_id: format!("connection-test-{name}"),
        deadline: tokio::time::Instant::now() + allowed,
    };
    let fine = executor
        .connection_job(job("fine", Duration::from_secs(30)), Arc::new(|_, _| {}))
        .await;
    assert_eq!(
        (fine[0].ok, fine[0].error.as_deref()),
        (true, None),
        "{fine:?}"
    );
    let started = std::time::Instant::now();
    let slow = executor
        .connection_job(job("slow", Duration::from_secs(2)), Arc::new(|_, _| {}))
        .await;
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
    assert!(!slow[0].ok, "{slow:?}");
    assert!(
        slow[0]
            .error
            .as_deref()
            .is_some_and(|e| e.starts_with("stopped after its timeout")),
        "{slow:?}"
    );
}
