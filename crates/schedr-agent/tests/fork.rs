//! The fork servers with the real Python runner (PLAN.md M2.5.4a): tasks forked from the
//! preloaded server, the fallbacks (bare, spawn), a forked task stopped, a dead shim, a
//! fork server restarted.
//!
//! Needs `SCHEDR_TEST_PYTHON`, a Python with `schedr` installed (`make test-integration` sets
//! it).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pretty_assertions::assert_eq;
use schedr_agent::fork::{ForkMetrics, ForkServers, ForkSetup};
use schedr_agent::launch::run_python;
use schedr_agent::metrics::{ForkFallbackLabels, ForkServerLabels};
use schedr_agent::process::ProcessSpec;
use schedr_agent::status::Termination;
use schedr_agent::supervise::{Limits, Stops};
use schedr_model::runner::{DagFile, RunInfo, RunnerMessage, RunnerMode, TaskRequest};
use schedr_model::{Dag, RunKind};
use tempfile::TempDir;

const DAGS: &str = r#"
import os, time
from schedr import Dag, task

@task
def whoami() -> int:
    return os.getpid()

@task
def slow() -> None:
    time.sleep(30)

@task
def sees_preload() -> bool:
    import sys
    return "schedr_probe_preloaded" in sys.modules

@task(preload=False)
def bare_sees_preload() -> bool:
    import sys
    return "schedr_probe_preloaded" in sys.modules

@task(fresh_process=True)
def fresh_sees_preload() -> bool:
    import sys
    return "schedr_probe_preloaded" in sys.modules

with Dag("sync"):
    whoami()
    slow()
    sees_preload()
    bare_sees_preload()
    fresh_sees_preload()
"#;

fn python() -> PathBuf {
    std::env::var_os("SCHEDR_TEST_PYTHON")
        .map(PathBuf::from)
        .expect("SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)")
}

fn spec(program: PathBuf, args: &[&str], dir: &Path, log: &str) -> ProcessSpec {
    ProcessSpec {
        program,
        args: args.iter().map(|a| (*a).to_owned()).collect(),
        env: BTreeMap::from([("PATH".to_owned(), std::env::var("PATH").unwrap_or_default())]),
        working_dir: dir.to_path_buf(),
        log_file: dir.join(log),
        max_output_size: 64 * 1024,
    }
}

fn root() -> TempDir {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("a_sync.py"), DAGS).unwrap();
    dir
}

/// Fork servers for `python`, preloading `preload`.
fn servers(program: PathBuf, dir: &Path, preload: &[&str]) -> (ForkServers, ForkMetrics) {
    retrying_servers(program, dir, preload, Duration::ZERO)
}

/// The same, a failing import retried for `import_retry`; `dir` is on the servers' path.
fn retrying_servers(
    program: PathBuf,
    dir: &Path,
    preload: &[&str],
    import_retry: Duration,
) -> (ForkServers, ForkMetrics) {
    let metrics = ForkMetrics::default();
    let mut setup = ForkSetup {
        interpreter: "python".to_owned(),
        process: spec(
            program,
            &["-m", "schedr.runner", "fork_server"],
            dir,
            "fork-server.log",
        ),
        preload: preload.iter().map(|p| (*p).to_owned()).collect(),
        import_retry,
    };
    setup
        .process
        .env
        .insert("PYTHONPATH".to_owned(), dir.to_string_lossy().into_owned());
    (ForkServers::start(&setup, &metrics), metrics)
}

fn state(metrics: &ForkMetrics, kind: &str, state: &str) -> i64 {
    metrics
        .servers
        .get_or_create(&ForkServerLabels {
            interpreter: "python".to_owned(),
            kind: kind.to_owned(),
            state: state.to_owned(),
        })
        .get()
}

fn fallbacks(metrics: &ForkMetrics, reason: &str) -> u64 {
    metrics
        .fallbacks
        .get_or_create(&ForkFallbackLabels {
            interpreter: "python".to_owned(),
            reason: reason.to_owned(),
        })
        .get()
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn the_dag(dir: &Path) -> Dag {
    // The DAG as the runner's parse mode reports it.
    let process = spec(
        python(),
        &["-m", "schedr.runner", "parse"],
        dir,
        "parse.log",
    );
    let mut runner = schedr_agent::launch::ProcessParseRunner {
        process,
        memory: schedr_agent::memory::MemoryWatch {
            budget: None,
            interval: Duration::from_secs(1),
            proc_root: PathBuf::from("/proc"),
        },
        kills: prometheus_client::metrics::counter::Counter::default(),
        forks: None,
    };
    let request = schedr_model::runner::ParseRequest {
        root: "etl".to_owned(),
        root_dir: dir.to_string_lossy().into_owned(),
        files: vec!["a_sync.py".to_owned()],
        sys_path: vec![dir.to_string_lossy().into_owned()],
    };
    let files = schedr_agent::parse::parse_files(&mut runner, request, Duration::from_secs(30))
        .await
        .unwrap();
    files[0].dags[0].clone()
}

fn request(dir: &Path, dag: &Dag, task: &str) -> TaskRequest {
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
            .unwrap(),
        inputs: BTreeMap::new(),
        outcomes: std::collections::BTreeMap::new(),
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

fn limits(timeout: Option<Duration>) -> Limits {
    Limits {
        timeout,
        grace: Duration::from_secs(1),
        interrupt_grace: Duration::from_secs(1),
    }
}

fn gone(pid: i32) -> bool {
    !Path::new(&format!("/proc/{pid}")).exists()
}

/// The parent of `pid` (field 4 of its `stat`).
fn parent(pid: i32) -> i32 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let after = &stat[stat.rfind(')').unwrap() + 1..];
    after.split_whitespace().nth(1).unwrap().parse().unwrap()
}

#[tokio::test]
async fn a_task_forks_from_the_preloaded_server_and_its_pid_is_held_until_released() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    let (forks, metrics) = servers(python(), dir.path(), &["json"]);
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let mut seen = None;
    let mut hold = None;
    let ran = run_python(
        &task,
        request(dir.path(), &dag, "whoami"),
        limits(None),
        Stops::new().1,
        Some(&forks),
        &mut hold,
        |pid, forked| seen = Some((pid, forked)),
    )
    .await
    .unwrap();
    let (pid, forked) = seen.unwrap();
    assert!(forked && ran.forked);
    let Some(RunnerMessage::TaskSucceeded { output, .. }) = &ran.runner_result else {
        panic!("no result: {ran:?}");
    };
    assert_eq!(output, &serde_json::json!(pid));
    assert_eq!(ran.supervised.termination, Termination::Exited(0));
    assert!(ran.start.unwrap().runner < Duration::from_secs(5));
    // Its shim's view: CPU spent, the peak RSS (with the server's shared pages), no core.
    let usage = ran.ended.usage.expect("the shim's usage");
    assert!(usage.user_cpu_micros + usage.system_cpu_micros > 0);
    assert!(
        usage.peak_rss_bytes.is_some_and(|b| b > 1 << 20),
        "{usage:?}"
    );
    assert!(!ran.ended.core_dumped);
    // Ended, but held (a zombie of its shim) until the hold goes.
    drop(ran);
    assert!(hold.is_some() && !gone(pid));
    drop(hold);
    until("the shim reaps it", || gone(pid)).await;
    assert_eq!(
        fallbacks(&metrics, "disabled") + fallbacks(&metrics, "down"),
        0
    );
}

#[tokio::test]
async fn a_failing_preload_disables_the_preloaded_server_and_the_bare_one_forks() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    let (forks, metrics) = servers(python(), dir.path(), &["schedr_no_such_module"]);
    until(
        "the preloaded server is disabled and the bare one up",
        || state(&metrics, "preloaded", "disabled") == 1 && state(&metrics, "bare", "up") == 1,
    )
    .await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let ran = run_python(
        &task,
        request(dir.path(), &dag, "whoami"),
        limits(None),
        Stops::new().1,
        Some(&forks),
        &mut None,
        |_, _| {},
    )
    .await
    .unwrap();
    assert!(ran.forked);
    assert_eq!(fallbacks(&metrics, "disabled"), 1);
    let disabled = forks.disabled().await;
    assert_eq!(disabled.len(), 1);
    assert!(
        disabled[0].1.contains("schedr_no_such_module"),
        "{disabled:?}"
    );
    // The worker's warning says so, and what it means for tasks.
    let warnings = forks.warnings().borrow().clone();
    assert_eq!(warnings.len(), 1);
    assert_eq!(
        (warnings[0].kind.as_str(), warnings[0].interpreter.as_str()),
        ("fork_server_disabled", "python")
    );
    assert!(
        warnings[0]
            .message
            .starts_with("the preloaded fork server is disabled: preloading")
            && warnings[0]
                .message
                .ends_with("; tasks are forked by the bare fork server, or spawned"),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn without_a_fork_server_up_the_task_is_spawned() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    let (forks, metrics) = servers(dir.path().join("no-such-python"), dir.path(), &["json"]);
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let ran = run_python(
        &task,
        request(dir.path(), &dag, "whoami"),
        limits(None),
        Stops::new().1,
        Some(&forks),
        &mut None,
        |_, forked| assert!(!forked),
    )
    .await
    .unwrap();
    assert!(!ran.forked);
    assert_eq!(ran.supervised.termination, Termination::Exited(0));
    // Read from its zombie: CPU, no peak RSS (gone before it is read).
    let usage = ran.ended.usage.expect("the zombie's usage");
    assert!(
        usage.user_cpu_micros + usage.system_cpu_micros > 0,
        "{usage:?}"
    );
    assert_eq!(usage.peak_rss_bytes, None);
    assert_eq!(fallbacks(&metrics, "down"), 2);
}

#[tokio::test]
async fn a_forked_task_over_its_timeout_is_stopped() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    let (forks, metrics) = servers(python(), dir.path(), &["json"]);
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let started = Instant::now();
    let ran = run_python(
        &task,
        request(dir.path(), &dag, "slow"),
        limits(Some(Duration::from_secs(1))),
        Stops::new().1,
        Some(&forks),
        &mut None,
        |_, _| {},
    )
    .await
    .unwrap();
    assert!(ran.forked);
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(
        ran.supervised.stopped,
        Some(schedr_agent::status::StopReason::TimedOut)
    );
    assert!(matches!(
        ran.supervised.termination,
        Termination::Signaled(_)
    ));
}

#[tokio::test]
async fn a_dead_shim_ends_the_attempt_unknown_and_its_group_is_killed() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    let (forks, metrics) = servers(python(), dir.path(), &["json"]);
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let mut pid = 0;
    let mut hold = None;
    let running = run_python(
        &task,
        request(dir.path(), &dag, "slow"),
        limits(None),
        Stops::new().1,
        Some(&forks),
        &mut hold,
        |started, _| {
            pid = started;
            let shim = parent(started);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", &shim.to_string()])
                    .status();
            });
        },
    );
    let ran = tokio::time::timeout(Duration::from_secs(20), running)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ran.supervised.termination, Termination::Unknown);
    assert!(ran.runner_result.is_none());
    until("the task's group is gone", || gone(pid)).await;
}

#[tokio::test]
async fn a_killed_fork_server_is_restarted() {
    let dir = root();
    let (_forks, metrics) = servers(python(), dir.path(), &["json"]);
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    // Both of its servers (preloaded and bare) are this test's children.
    let me = i32::try_from(std::process::id()).unwrap();
    let servers: Vec<i32> = std::fs::read_dir("/proc")
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse::<i32>().ok())
        .filter(|pid| {
            std::fs::read(format!("/proc/{pid}/cmdline"))
                .is_ok_and(|c| c.split(|b| *b == 0).any(|a| a == b"fork_server"))
                && parent(*pid) == me
        })
        .collect();
    assert_eq!(servers.len(), 2, "{servers:?}");
    for server in servers {
        std::process::Command::new("kill")
            .args(["-KILL", &server.to_string()])
            .status()
            .unwrap();
    }
    until("it is down", || state(&metrics, "preloaded", "down") == 1).await;
    until("it is up again", || state(&metrics, "preloaded", "up") == 1).await;
}

#[tokio::test]
async fn a_failing_import_is_retried_and_the_server_comes_up_once_it_imports() {
    let dir = root();
    let ready = dir.path().join("ready");
    std::fs::write(
        dir.path().join("schedr_flaky_probe.py"),
        format!(
            "import os\nif not os.path.exists({:?}):\n    raise RuntimeError('not ready')\n",
            ready.display().to_string()
        ),
    )
    .unwrap();
    let (forks, metrics) = retrying_servers(
        python(),
        dir.path(),
        &["schedr_flaky_probe"],
        Duration::from_secs(60),
    );
    until("the preloaded server is disabled", || {
        state(&metrics, "preloaded", "disabled") == 1
    })
    .await;
    assert!(forks.disabled().await[0].1.contains("not ready"));
    std::fs::write(&ready, "").unwrap();
    until("it came up on a retry", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    assert_eq!(forks.disabled().await, Vec::new());
    // Its warning went when it came up.
    assert_eq!(forks.warnings().borrow().clone(), Vec::new());
}

#[tokio::test]
async fn an_abandoned_answer_restarts_the_server_instead_of_pairing_the_next_request_with_it() {
    let dir = root();
    // Every fork in the server takes half a second: an answer the agent stops waiting for.
    std::fs::write(
        dir.path().join("schedr_slow_fork.py"),
        "import os, time\n_fork = os.fork\nos.fork = lambda: (time.sleep(0.5), _fork())[1]\n",
    )
    .unwrap();
    let (forks, metrics) =
        retrying_servers(python(), dir.path(), &["schedr_slow_fork"], Duration::ZERO);
    // Both up: the bare one takes the request the preloaded one can't.
    until("both servers are up", || {
        state(&metrics, "preloaded", "up") == 1 && state(&metrics, "bare", "up") == 1
    })
    .await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let abandoned = tokio::time::timeout(
        Duration::from_millis(100),
        forks.fork(&task, RunnerMode::Task, true),
    )
    .await;
    assert!(
        abandoned.is_err(),
        "the fork should still be waiting for its answer"
    );
    // The next request finds the answer abandoned: the preloaded server is let go (the bare
    // one forks it) and restarted.
    let next = forks.fork(&task, RunnerMode::Task, true).await;
    assert!(next.is_ok());
    assert_eq!(fallbacks(&metrics, "down"), 1);
    until("it is up again", || state(&metrics, "preloaded", "up") == 1).await;
    // Up again, it answers a request as its own: no fallback this time.
    let again = forks.fork(&task, RunnerMode::Task, true).await;
    assert!(again.is_ok());
    assert_eq!(fallbacks(&metrics, "down"), 1);
}

#[tokio::test]
async fn the_fork_that_finds_a_late_thread_disables_the_server() {
    let dir = root();
    // A thread started a moment after the server said it was ready.
    std::fs::write(
        dir.path().join("schedr_late_thread.py"),
        "import signal, threading, time\n\
         def start(*_):\n    threading.Thread(target=time.sleep, args=(60,), daemon=True).start()\n\
         signal.signal(signal.SIGALRM, start)\n\
         signal.setitimer(signal.ITIMER_REAL, 0.3)\n",
    )
    .unwrap();
    let (forks, metrics) = retrying_servers(
        python(),
        dir.path(),
        &["schedr_late_thread"],
        Duration::ZERO,
    );
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let forked = forks.fork(&task, RunnerMode::Task, true).await;
    // The bare server forked it; the preloaded one is disabled by this very request.
    assert!(forked.is_ok());
    assert_eq!(fallbacks(&metrics, "disabled"), 1);
    assert_eq!(state(&metrics, "preloaded", "disabled"), 1);
    let disabled = forks.disabled().await;
    assert!(disabled[0].1.contains("threads"), "{disabled:?}");
}

#[tokio::test]
async fn a_forked_runner_that_fails_to_start_stays_held_until_the_caller_lets_it_go() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    let (forks, metrics) = servers(python(), dir.path(), &["json"]);
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    // A working directory that doesn't exist: the forked runner exits before its hello.
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        &dir.path().join("missing"),
        "task.log",
    );
    let mut task = task;
    task.log_file = dir.path().join("task.log");
    let mut pid = 0;
    let mut hold = None;
    let failed = run_python(
        &task,
        request(dir.path(), &dag, "whoami"),
        limits(None),
        Stops::new().1,
        Some(&forks),
        &mut hold,
        |started, _| pid = started,
    )
    .await;
    assert!(failed.is_err());
    // Its group isn't the shim's to reap yet: the caller's watchdog may still run.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(hold.is_some() && !gone(pid));
    drop(hold);
    until("the shim reaps it", || gone(pid)).await;
}

#[tokio::test]
async fn a_task_picks_the_bare_server_or_a_fresh_process_and_the_rest_fork_preloaded() {
    let dir = root();
    let dag = the_dag(dir.path()).await;
    std::fs::write(dir.path().join("schedr_probe_preloaded.py"), "").unwrap();
    let (forks, metrics) = retrying_servers(
        python(),
        dir.path(),
        &["schedr_probe_preloaded"],
        Duration::ZERO,
    );
    until("both servers are up", || {
        state(&metrics, "preloaded", "up") == 1 && state(&metrics, "bare", "up") == 1
    })
    .await;
    let task = spec(
        python(),
        &["-m", "schedr.runner", "task"],
        dir.path(),
        "task.log",
    );
    let mut seen = Vec::new();
    for name in ["sees_preload", "bare_sees_preload", "fresh_sees_preload"] {
        let mut hold = None;
        let ran = run_python(
            &task,
            request(dir.path(), &dag, name),
            limits(None),
            Stops::new().1,
            Some(&forks),
            &mut hold,
            |_, _| {},
        )
        .await
        .unwrap();
        let Some(RunnerMessage::TaskSucceeded { output, .. }) = &ran.runner_result else {
            panic!("{name}: no result: {ran:?}");
        };
        seen.push((name, ran.forked, output.clone()));
    }
    assert_eq!(
        seen,
        [
            ("sees_preload", true, serde_json::json!(true)),
            ("bare_sees_preload", true, serde_json::json!(false)),
            ("fresh_sees_preload", false, serde_json::json!(false)),
        ]
    );
    // Skipping a server by choice isn't a fallback.
    for reason in ["disabled", "down", "fork_failed", "shim_died"] {
        assert_eq!(fallbacks(&metrics, reason), 0, "{reason}");
    }
}

/// Parses `file` in `dir`, forked by `forks` when given; the DAG IDs it defines.
async fn parsed_ids(dir: &Path, file: &str, forks: Option<ForkServers>) -> Vec<String> {
    let process = spec(
        python(),
        &["-m", "schedr.runner", "parse"],
        dir,
        "parse.log",
    );
    let mut runner = schedr_agent::launch::ProcessParseRunner {
        process,
        memory: schedr_agent::memory::MemoryWatch {
            budget: None,
            interval: Duration::from_secs(1),
            proc_root: PathBuf::from("/proc"),
        },
        kills: prometheus_client::metrics::counter::Counter::default(),
        forks,
    };
    let request = schedr_model::runner::ParseRequest {
        root: "etl".to_owned(),
        root_dir: dir.to_string_lossy().into_owned(),
        files: vec![file.to_owned()],
        sys_path: vec![dir.to_string_lossy().into_owned()],
    };
    let files = schedr_agent::parse::parse_files(&mut runner, request, Duration::from_secs(30))
        .await
        .unwrap();
    files[0].dags.iter().map(|d| d.id.to_string()).collect()
}

#[tokio::test]
async fn a_parse_job_forks_from_the_preloaded_server() {
    let dir = root();
    std::fs::write(dir.path().join("schedr_probe_preloaded.py"), "").unwrap();
    std::fs::write(
        dir.path().join("probe.py"),
        "import sys\nfrom schedr import Dag\n\
         with Dag('preloaded' if 'schedr_probe_preloaded' in sys.modules else 'not_preloaded'):\n    pass\n",
    )
    .unwrap();
    let (forks, metrics) = retrying_servers(
        python(),
        dir.path(),
        &["schedr_probe_preloaded"],
        Duration::ZERO,
    );
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    assert_eq!(
        parsed_ids(dir.path(), "probe.py", Some(forks)).await,
        ["preloaded"]
    );
    assert_eq!(
        parsed_ids(dir.path(), "probe.py", None).await,
        ["not_preloaded"]
    );
}

#[tokio::test]
async fn a_forked_parse_dropped_unfinished_is_killed() {
    use schedr_agent::parse::ParseRunner;
    let dir = root();
    let pid_file = dir.path().join("pid");
    std::fs::write(
        dir.path().join("hangs.py"),
        format!(
            "import os, time\nopen({:?}, 'w').write(str(os.getpid()))\ntime.sleep(600)\n",
            pid_file.display().to_string()
        ),
    )
    .unwrap();
    let (forks, metrics) = servers(python(), dir.path(), &["json"]);
    until("the preloaded server is up", || {
        state(&metrics, "preloaded", "up") == 1
    })
    .await;
    let mut runner = schedr_agent::launch::ProcessParseRunner {
        process: spec(
            python(),
            &["-m", "schedr.runner", "parse"],
            dir.path(),
            "parse.log",
        ),
        memory: schedr_agent::memory::MemoryWatch {
            budget: None,
            interval: Duration::from_secs(1),
            proc_root: PathBuf::from("/proc"),
        },
        kills: prometheus_client::metrics::counter::Counter::default(),
        forks: Some(forks),
    };
    let session = runner
        .start(schedr_model::runner::ParseRequest {
            root: "etl".to_owned(),
            root_dir: dir.path().to_string_lossy().into_owned(),
            files: vec!["hangs.py".to_owned()],
            sys_path: vec![dir.path().to_string_lossy().into_owned()],
        })
        .await
        .unwrap();
    until("the file is being imported", || pid_file.exists()).await;
    let pid: i32 = std::fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    assert!(!gone(pid));
    // Dropped without `stop` (its parse task aborted): killed, as a spawned runner would be.
    drop(session);
    until("the forked runner is gone", || gone(pid)).await;
}

#[tokio::test]
async fn the_fork_servers_start_before_the_log_directory_exists() {
    // As on a new worker: its log directory is made by its first task.
    let dir = root();
    let logs = dir.path().join("logs").join("w1");
    let metrics = ForkMetrics::default();
    let setup = ForkSetup {
        interpreter: "python".to_owned(),
        process: spec(
            python(),
            &["-m", "schedr.runner", "fork_server"],
            &logs,
            "fork-server.log",
        ),
        preload: vec!["json".to_owned()],
        import_retry: Duration::ZERO,
    };
    let _forks = ForkServers::start(&setup, &metrics);
    until("both servers are up", || {
        state(&metrics, "preloaded", "up") == 1 && state(&metrics, "bare", "up") == 1
    })
    .await;
    assert!(logs.join("fork-server-preloaded.log").exists());
}
