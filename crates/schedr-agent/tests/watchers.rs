//! The watcher host with the real Python runner (decision 320): wait steps sharing a process, a
//! host per code set, each one's log its own, and how they end.
//!
//! Needs `SCHEDR_TEST_PYTHON`, a Python with `schedr` installed (`make test-integration` sets
//! it).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_agent::attempt::{Ran, WorkerSetup, WorkerTimeouts};
use schedr_agent::channel::write_message;
use schedr_agent::launch::{ProcessParseRunner, RUNNER_START_TIMEOUT, start_runner};
use schedr_agent::metrics::{CodeLabels, WatcherMetrics};
use schedr_agent::parse::parse_files;
use schedr_agent::process::ProcessSpec;
use schedr_agent::status::{StopReason, Termination};
use schedr_agent::supervise::{Limits, Stops};
use schedr_agent::watchers::{WatcherHosts, run_watcher};
use schedr_model::runner::{
    AgentMessage, DagFile, ParseRequest, RunInfo, RunnerMessage, RunnerMode, TaskRequest,
    WatchersRequest,
};
use schedr_model::{Dag, RunKind};
use serde_json::{Value, json};
use tempfile::TempDir;

/// What parsing refuses is reached with `getattr` here: these test what happens at run time.
const DAGS: &str = r#"
import asyncio
import ctypes
import os
import subprocess
import threading
import time
from datetime import timedelta
from pathlib import Path

from schedr import NOT_YET, Dag, NotYet, SecretPrefix, sensor, task_ctx

CODE = "old"
WAIT = 30

with Path(os.environ["IMPORTS"]).open("a") as f:
    f.write("imported\n")
if Path(os.environ["IMPORTS"]).with_name("slow_import").exists():
    time.sleep(2)


@sensor
async def alpha() -> int:
    print("from alpha")
    await asyncio.sleep(0.3)
    return os.getpid()


@sensor
async def beta() -> int:
    print("from beta")
    await asyncio.sleep(0.3)
    return os.getpid()


@sensor
async def forever() -> int:
    try:
        Path(os.environ["IMPORTS"]).with_name("forever_started").write_text("yes")
        await asyncio.sleep(600)
    finally:
        Path(os.environ["IMPORTS"]).with_name("cleaned").write_text("yes")
    return 0


@sensor
async def refuses() -> int:
    while True:
        try:
            await asyncio.sleep(600)
        except asyncio.CancelledError:
            pass


@sensor
async def crashes() -> int:
    await asyncio.sleep(0.2)
    os._exit(7)


@sensor
async def fails() -> int:
    raise ValueError("no luck")


@sensor
async def secret_printer() -> int:
    print("hunter22-pw")
    return 1


@sensor
async def thread_printer() -> int:
    thread = getattr(threading, "Thread")(
        target=lambda: print("thread says hunter22-pw", flush=True)
    )
    thread.start()
    thread.join()
    return 1


@sensor
async def native_printer() -> int:
    os.write(1, b"native says hunter22-pw\n")
    return 1


@sensor
async def refused_in_a_thread() -> int:
    errors = []

    def starts() -> None:
        try:
            getattr(subprocess, "run")(["true"], check=True)
        except Exception as error:
            errors.append(error)

    thread = getattr(threading, "Thread")(target=starts)
    thread.start()
    thread.join()
    raise errors[0]


def _starts_sleep() -> None:
    child = getattr(subprocess, "Popen")(["sleep", "600"])
    Path(os.environ["IMPORTS"]).with_name("child").write_text(str(child.pid))


@sensor
async def starts_a_process() -> int:
    # A plain thread is what the refusal can't see: its end has to hold anyway.
    thread = getattr(threading, "Thread")(target=_starts_sleep)
    thread.start()
    thread.join()
    await asyncio.sleep(600)
    return 1


@sensor
async def refused_a_process() -> int:
    getattr(subprocess, "run")(["true"], check=True)
    return 1


@sensor
async def refused_async_process() -> int:
    await getattr(asyncio, "create_subprocess_exec")("true")
    return 1


def _spins_until_stopped() -> None:
    marks = Path(os.environ["IMPORTS"]).parent
    try:
        print("thread says hunter22-pw", flush=True)
        (marks / "spinning").write_text("yes")
        while True:
            time.sleep(0.01)
    finally:
        (marks / "spin_stopped").write_text("yes")


@sensor
async def in_a_thread_stoppable() -> int:
    await asyncio.to_thread(_spins_until_stopped)
    return 1


@sensor(max_wait=timedelta(milliseconds=300))
async def thread_out_of_time() -> int:
    await asyncio.to_thread(_spins_until_stopped)
    return 1


@sensor(max_wait=timedelta(milliseconds=300))
async def thread_blocked_in_c() -> int:
    await asyncio.to_thread(time.sleep, 600)
    return 1


def _stops_slowly() -> None:
    try:
        while True:
            time.sleep(0.01)
    finally:
        time.sleep(0.7)


@sensor(max_wait=timedelta(milliseconds=200))
async def thread_stopping_slowly() -> int:
    await asyncio.to_thread(_stops_slowly)
    return 1


@sensor
async def holds_the_loop() -> int:
    time.sleep(600)
    return 1


@sensor(max_wait=timedelta(seconds=WAIT))
async def waits_for_release() -> int:
    marks = Path(os.environ["IMPORTS"]).parent
    with (marks / "waits_starts").open("a") as f:
        f.write("started\n")
    while not (marks / "release").exists():
        await asyncio.sleep(0.02)
    return os.getpid()


@sensor(max_wait=timedelta(seconds=3))
async def waits_out_its_time() -> int:
    marks = Path(os.environ["IMPORTS"]).parent
    with (marks / "waits_starts").open("a") as f:
        f.write(f"{time.time()}\n")
    await asyncio.sleep(600)
    return 1


@sensor
async def holds_everything() -> int:
    # In C, keeping the interpreter's lock: not even the watchdog runs.
    getattr(ctypes, "PyDLL")(None).sleep(600)
    return 1


@sensor
async def leaves_a_thread() -> int:
    getattr(threading, "Thread")(
        target=time.sleep, args=(2,), name="left-behind", daemon=True
    ).start()
    return 1


@sensor
async def floods_its_log() -> int:
    for line in range(500):
        print(f"line {line}")
    return 1


@sensor(secrets=[SecretPrefix("etl/")])
async def slow_reader() -> int:
    Path(os.environ["IMPORTS"]).with_name("reading").write_text("yes")
    try:
        await task_ctx.asecret("etl/db", "password")
    finally:
        Path(os.environ["IMPORTS"]).with_name("cleaned").write_text("yes")
    return 1


@sensor.poke(every=timedelta(milliseconds=50), max_wait=timedelta(seconds=WAIT))
async def polls() -> str | NotYet:
    os.write(1, b"native hunter22-pw\n")
    if not Path(os.environ["IMPORTS"]).with_name("ready").exists():
        return NOT_YET
    return CODE


@sensor.poke(every=timedelta(seconds=7), max_wait=timedelta(seconds=WAIT))
async def slow_call() -> str | NotYet:
    state = Path(os.environ["IMPORTS"]).parent
    while not (state / "release").exists():
        await asyncio.sleep(0.05)
    return CODE if (state / "ready").exists() else NOT_YET


with Dag("sync"):
    native_printer()
    holds_everything()
    leaves_a_thread()
    holds_the_loop()
    waits_for_release()
    waits_out_its_time()
    thread_stopping_slowly()
    floods_its_log()
    in_a_thread_stoppable()
    thread_out_of_time()
    thread_blocked_in_c()
    refused_in_a_thread()
    refused_a_process()
    refused_async_process()
    slow_call()
    starts_a_process()
    slow_reader()
    polls()
    thread_printer()
    alpha()
    beta()
    forever()
    refuses()
    crashes()
    fails()
    secret_printer()
"#;

fn python() -> PathBuf {
    std::env::var_os("SCHEDR_TEST_PYTHON").map(PathBuf::from).expect(
        "SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)",
    )
}

struct Worker {
    dir: TempDir,
    root: PathBuf,
    setup: WorkerSetup,
    hosts: WatcherHosts,
    dag: Dag,
}

impl Worker {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("etl");
        let logs = dir.path().join("logs");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(root.join("a_sync.py"), DAGS).unwrap();
        let imports = dir.path().join("imports");
        let agent_env = BTreeMap::from([
            ("PATH".to_owned(), std::env::var("PATH").unwrap_or_default()),
            ("IMPORTS".to_owned(), imports.display().to_string()),
        ]);
        let setup = WorkerSetup {
            roots: BTreeMap::from([("etl".to_owned(), root.clone())]),
            parse_only: BTreeMap::new(),
            script_roots: BTreeMap::new(),
            interpreters: BTreeMap::new(),
            python: python(),
            agent_env: agent_env.clone(),
            pass_env: vec!["IMPORTS".to_owned()],
            log_dir: logs.clone(),
            connection_dir: dir.path().join("connections"),
            memory: schedr_agent::memory::MemoryWatch::default(),
            timeouts: WorkerTimeouts::default(),
            fork_server: None,
        };
        let parser = ProcessParseRunner {
            process: spec(&setup, &root, &logs.join("parse.log"), "parse"),
            memory: schedr_agent::memory::MemoryWatch::default(),
            kills: prometheus_client::metrics::counter::Counter::default(),
            forks: None,
        };
        let request = ParseRequest {
            root: "etl".to_owned(),
            root_dir: root.display().to_string(),
            files: vec!["a_sync.py".to_owned()],
            sys_path: vec![root.display().to_string()],
        };
        let parsed = parse_files(&parser, request, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(parsed[0].error, None);
        std::fs::remove_file(&imports).unwrap();
        Self {
            root,
            setup,
            hosts: WatcherHosts::default(),
            dag: parsed[0].dags[0].clone(),
            dir,
        }
    }

    /// `forever` is running.
    async fn forever_started(&self) {
        while !self.dir.path().join("forever_started").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// `waits_for_release` has made its first call.
    async fn peer_started(&self) {
        let starts = self.dir.path().join("waits_starts");
        while !starts.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// `waits_for_release` started again, on a new host: it may now end.
    async fn release_peer_started_again(&self) {
        let starts = self.dir.path().join("waits_starts");
        while std::fs::read_to_string(&starts)
            .unwrap_or_default()
            .lines()
            .count()
            < 2
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        std::fs::write(self.dir.path().join("release"), "").unwrap();
    }

    fn assert_started_again(&self, peer: &Ran) {
        assert!(
            matches!(
                peer.runner_result,
                Some(RunnerMessage::TaskSucceeded { .. })
            ),
            "{:?}",
            peer.runner_result
        );
        assert_eq!(peer.host_lost, None);
        let log = self.log("waits_for_release");
        assert!(log.contains("starts again on a new host"), "{log}");
    }

    fn imports(&self) -> usize {
        std::fs::read_to_string(self.dir.path().join("imports"))
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn log(&self, task: &str) -> String {
        std::fs::read_to_string(self.log_file(task)).unwrap_or_default()
    }

    fn log_file(&self, task: &str) -> PathBuf {
        self.dir.path().join("logs").join(format!("{task}.log"))
    }

    fn request(&self, task: &str, sys_path: &[&Path]) -> TaskRequest {
        let planned_for = "2026-10-21T08:00:00Z".parse().unwrap();
        TaskRequest {
            trigger: None,
            dag_file: DagFile {
                root: "etl".to_owned(),
                root_dir: self.root.display().to_string(),
                path: "a_sync.py".to_owned(),
            },
            dag_id: "sync".to_owned(),
            task: self
                .dag
                .tasks
                .iter()
                .find(|t| t.id.as_str() == task)
                .unwrap()
                .clone(),
            inputs: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            run: RunInfo {
                event: None,
                params: json!({}),
                dag_id: "sync".to_owned(),
                run_id: "r1".to_owned(),
                kind: RunKind::Manual,
                planned_for,
                since: planned_for,
                try_number: 1,
                map_index: None,
            },
            sys_path: sys_path.iter().map(|p| p.display().to_string()).collect(),
            secrets: Vec::new(),
            connection_test: None,
            cleanup: None,
        }
    }

    async fn run(&self, task: &str, stop: Stops, mask: schedr_agent::mask::Mask) -> Ran {
        self.run_in(task, &[&self.root], stop, mask).await
    }

    async fn run_in(
        &self,
        task: &str,
        sys_path: &[&Path],
        stop: Stops,
        mask: schedr_agent::mask::Mask,
    ) -> Ran {
        self.run_with_grace(task, sys_path, stop, mask, Duration::from_millis(500))
            .await
    }

    async fn run_with_grace(
        &self,
        task: &str,
        sys_path: &[&Path],
        stop: Stops,
        mask: schedr_agent::mask::Mask,
        grace: Duration,
    ) -> Ran {
        let mut process = spec(&self.setup, &self.root, &self.log_file(task), "task");
        process.mask = mask;
        let limits = Limits {
            timeout: None,
            grace,
            interrupt_grace: Duration::from_millis(500),
            poll: Duration::from_millis(50),
        };
        run_watcher(
            &self.hosts,
            &self.setup,
            &process,
            self.request(task, sys_path).into(),
            limits,
            stop,
            |pid| assert!(pid > 1),
        )
        .await
        .unwrap()
    }
}

fn spec(setup: &WorkerSetup, root: &Path, log: &Path, mode: &str) -> ProcessSpec {
    ProcessSpec {
        program: setup.python.clone(),
        args: vec!["-m".to_owned(), "schedr.runner".to_owned(), mode.to_owned()],
        env: setup.agent_env.clone(),
        working_dir: root.to_path_buf(),
        log_file: log.to_path_buf(),
        max_output_size: 64 * 1024,
        mask: schedr_agent::mask::Mask::default(),
        start_timeout: RUNNER_START_TIMEOUT,
        output_drain: Duration::from_secs(5),
    }
}

fn no_stop() -> Stops {
    Stops::new().1
}

fn mask_none() -> schedr_agent::mask::Mask {
    schedr_agent::mask::Mask::default()
}

fn output_of(ran: &Ran) -> Value {
    match &ran.runner_result {
        Some(RunnerMessage::TaskSucceeded { output, .. }) => output.clone(),
        other => panic!("{other:?}: {:?}", ran.supervised),
    }
}

#[tokio::test]
async fn wait_steps_of_a_code_set_share_a_process_each_with_its_own_log() {
    let worker = Worker::new().await;
    let (alpha, beta) = tokio::join!(
        worker.run("alpha", no_stop(), mask_none()),
        worker.run("beta", no_stop(), mask_none()),
    );
    assert_eq!(output_of(&alpha), output_of(&beta), "one process");
    assert_eq!(alpha.supervised.termination, Termination::Exited(0));
    assert_eq!(worker.log("alpha"), "from alpha\n");
    assert_eq!(worker.log("beta"), "from beta\n");
    // Its top level ran once, for both.
    assert_eq!(worker.imports(), 1);
    // The last one out stops it.
    assert_eq!(worker.hosts.running().await, 0);
}

#[tokio::test]
async fn new_code_gets_a_new_host_while_the_old_one_finishes() {
    let worker = Worker::new().await;
    let other = worker.dir.path().join("other_root");
    std::fs::create_dir_all(&other).unwrap();
    let both: [&Path; 2] = [&worker.root, &other];
    let (old, new) = tokio::join!(
        worker.run("alpha", no_stop(), mask_none()),
        worker.run_in("beta", &both, no_stop(), mask_none()),
    );
    assert_ne!(output_of(&old), output_of(&new));
    assert_eq!(worker.imports(), 2);
    assert_eq!(worker.hosts.running().await, 0);
}

#[tokio::test]
async fn a_cancel_ends_only_its_watcher_and_runs_its_finally() {
    let worker = Worker::new().await;
    let (cancel, stops) = Stops::new();
    let forever = worker.run("forever", stops, mask_none());
    let alpha = async {
        worker.forever_started().await;
        cancel.send(StopReason::Cancelled).unwrap();
        worker.run("alpha", no_stop(), mask_none()).await
    };
    let (forever, alpha) = tokio::join!(forever, alpha);
    assert_eq!(forever.supervised.stopped, Some(StopReason::Cancelled));
    assert_eq!(forever.supervised.termination, Termination::Signaled(15));
    assert!(!forever.supervised.killed);
    assert_eq!(forever.runner_result, None);
    assert!(matches!(
        alpha.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    assert_eq!(
        std::fs::read_to_string(worker.dir.path().join("cleaned")).unwrap(),
        "yes"
    );
}

#[tokio::test]
async fn one_that_wont_be_cancelled_has_its_host_killed() {
    let worker = Worker::new().await;
    let (cancel, stops) = Stops::new();
    let peer = worker.run("waits_for_release", no_stop(), mask_none());
    let refusing = async {
        worker.peer_started().await;
        let refuses = worker.run("refuses", stops, mask_none());
        let cancelling = async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            cancel.send(StopReason::Cancelled).unwrap();
        };
        let (refuses, ()) = tokio::join!(refuses, cancelling);
        worker.release_peer_started_again().await;
        refuses
    };
    let (peer, refuses) = tokio::join!(peer, refusing);
    assert!(refuses.supervised.killed);
    assert_eq!(refuses.supervised.termination, Termination::Signaled(9));
    worker.assert_started_again(&peer);
}

#[tokio::test]
async fn a_crash_ends_all_on_the_host_and_the_next_gets_a_new_one() {
    let worker = Worker::new().await;
    let (crashes, forever) = tokio::join!(
        worker.run("crashes", no_stop(), mask_none()),
        worker.run("forever", no_stop(), mask_none()),
    );
    assert_eq!(crashes.supervised.termination, Termination::Exited(7));
    assert_eq!(forever.supervised.termination, Termination::Exited(7));
    assert_eq!(forever.runner_result, None);
    // Nothing to blame: it ends `lost`, using a retry.
    let why = forever.host_lost.unwrap();
    assert!(why.contains("Exited(7)"), "{why}");
    assert!(
        why.contains("sync.crashes") && why.contains("sync.forever"),
        "{why}"
    );
    let alpha = worker.run("alpha", no_stop(), mask_none()).await;
    assert!(matches!(
        alpha.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
}

#[tokio::test]
async fn a_failing_watcher_fails_alone() {
    let worker = Worker::new().await;
    let (fails, alpha) = tokio::join!(
        worker.run("fails", no_stop(), mask_none()),
        worker.run("alpha", no_stop(), mask_none()),
    );
    assert_eq!(fails.supervised.termination, Termination::Exited(1));
    let Some(RunnerMessage::TaskFailed { error, kind: None }) = &fails.runner_result else {
        panic!("{:?}", fails.runner_result);
    };
    assert_eq!(error.exception, "ValueError");
    assert_eq!(error.message, "no luck");
    assert!(matches!(
        alpha.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
}

#[tokio::test]
async fn what_a_watcher_prints_is_masked() {
    let worker = Worker::new().await;
    let set = schedr_agent::mask::MaskSet::new(4);
    set.add("etl/db", "hunter22-pw");
    let mask = schedr_agent::mask::Mask(Some(std::sync::Arc::new(set)));
    worker.run("secret_printer", no_stop(), mask).await;
    assert_eq!(worker.log("secret_printer"), "****\n");
}

#[tokio::test]
async fn what_a_thread_prints_goes_to_its_steps_log_masked() {
    let worker = Worker::new().await;
    let ran = worker
        .run("thread_printer", no_stop(), masking_the_secret())
        .await;
    assert!(matches!(
        ran.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    assert_eq!(worker.log("thread_printer"), "thread says ****\n");
}

#[tokio::test]
async fn what_c_code_prints_goes_to_the_hosts_log_masked() {
    let worker = Worker::new().await;
    worker
        .run("native_printer", no_stop(), masking_the_secret())
        .await;
    let wanted = "native says ****\n";
    for _ in 0..100 {
        if host_logs(&worker) == wanted {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(host_logs(&worker), wanted);
    assert_eq!(worker.log("native_printer"), "");
}

/// What running `polls` and changing the code do: `change` gets the new code's folder, and
/// `ready` makes it find its value.
async fn poke_with_new_code(worker: &Worker, new_code: &str) -> Ran {
    let new_root = worker.dir.path().join("sha256-abc");
    std::fs::create_dir_all(&new_root).unwrap();
    std::fs::write(new_root.join("a_sync.py"), new_code).unwrap();
    let copies = BTreeMap::from([("etl".to_owned(), new_root)]);
    let changing = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        worker.hosts.code_changed(&copies);
        // Moved, or said why not.
        for _ in 0..600 {
            if worker.log("polls").contains("new code") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        std::fs::write(worker.dir.path().join("ready"), "").unwrap();
    };
    let (ran, ()) = tokio::join!(worker.run("polls", no_stop(), mask_none()), changing);
    ran
}

#[tokio::test]
async fn a_poke_makes_its_next_call_on_new_code() {
    let worker = Worker::new().await;
    let ran = poke_with_new_code(&worker, &DAGS.replace("CODE = \"old\"", "CODE = \"new\"")).await;
    assert_eq!(output_of(&ran), json!("new"));
    assert_eq!(ran.moved_to.as_deref(), Some("sha256:abc"));
    assert!(
        worker.log("polls").contains("moved to new code"),
        "{}",
        worker.log("polls")
    );
    assert_eq!(worker.hosts.running().await, 0);
    assert_eq!(worker.hosts.code_in_use(), Vec::<String>::new());
}

#[tokio::test]
async fn a_poke_renamed_in_new_code_stays_on_its_code() {
    let worker = Worker::new().await;
    let renamed = DAGS
        .replace("async def polls()", "async def renamed()")
        .replace("    polls()\n", "    renamed()\n");
    let ran = poke_with_new_code(&worker, &renamed).await;
    assert_eq!(output_of(&ran), json!("old"));
    assert_eq!(ran.moved_to, None);
    let log = worker.log("polls");
    assert!(
        log.contains("new code can't start this wait step, it stays on its code"),
        "{log}"
    );
    assert!(log.contains("polls"), "{log}");
    assert!(!log.contains("moved to new code"), "{log}");
}

#[tokio::test]
async fn a_poke_stays_on_its_code_when_new_code_doesnt_import() {
    let worker = Worker::new().await;
    let ran = poke_with_new_code(&worker, "raise RuntimeError('broken deploy')\n").await;
    assert_eq!(output_of(&ran), json!("old"));
    assert_eq!(ran.moved_to, None);
    let log = worker.log("polls");
    assert!(log.contains("broken deploy"), "{log}");
}

#[tokio::test]
async fn a_poke_keeps_its_deadline_across_a_move() {
    let worker = Worker::new().await;
    let new_root = worker.dir.path().join("new_root");
    std::fs::create_dir_all(&new_root).unwrap();
    std::fs::write(
        new_root.join("a_sync.py"),
        DAGS.replace("CODE = \"old\"", "CODE = \"new\""),
    )
    .unwrap();
    let mut request = worker.request("polls", &[&worker.root]);
    let schedr_model::TaskKind::Sensor { max_wait_ms, .. } = &mut request.task.kind else {
        panic!()
    };
    *max_wait_ms = Some(4000);
    let process = spec(
        &worker.setup,
        &worker.root,
        &worker.log_file("polls"),
        "task",
    );
    let limits = Limits {
        timeout: None,
        grace: Duration::from_millis(500),
        interrupt_grace: Duration::from_millis(500),
        poll: Duration::from_millis(50),
    };
    let began = std::time::Instant::now();
    let copies = BTreeMap::from([("etl".to_owned(), new_root)]);
    let changing = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        worker.hosts.code_changed(&copies);
    };
    let running = run_watcher(
        &worker.hosts,
        &worker.setup,
        &process,
        request.into(),
        limits,
        no_stop(),
        |_| {},
    );
    let (ran, ()) = tokio::join!(running, changing);
    let ran = ran.unwrap();
    let RunnerMessage::TaskSucceeded { outcome, .. } = ran.runner_result.as_ref().unwrap() else {
        panic!("{:?}", ran.runner_result);
    };
    assert_eq!(outcome, "TimedOut");
    assert!(worker.log("polls").contains("moved to new code"));
    // Its 4 s from the start, not 4 s more from the move.
    assert!(
        began.elapsed() < Duration::from_millis(5400),
        "{:?}",
        began.elapsed()
    );
}

#[tokio::test]
async fn a_full_control_wait_step_never_moves() {
    let worker = Worker::new().await;
    let new_root = worker.dir.path().join("new_root");
    std::fs::create_dir_all(&new_root).unwrap();
    std::fs::write(new_root.join("a_sync.py"), DAGS).unwrap();
    let copies = BTreeMap::from([("etl".to_owned(), new_root)]);
    let changing = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        worker.hosts.code_changed(&copies);
    };
    let (ran, ()) = tokio::join!(worker.run("alpha", no_stop(), mask_none()), changing);
    assert!(matches!(
        ran.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    assert_eq!(worker.log("alpha"), "from alpha\n");
}

/// The hosts' logs, together.
fn host_logs(worker: &Worker) -> String {
    let logs = worker.dir.path().join("logs");
    std::fs::read_dir(&logs)
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("watchers-"))
        .map(|entry| std::fs::read_to_string(entry.path()).unwrap())
        .collect()
}

fn masking_the_secret() -> schedr_agent::mask::Mask {
    let set = schedr_agent::mask::MaskSet::new(4);
    set.add("etl/db", "hunter22-pw");
    schedr_agent::mask::Mask(Some(std::sync::Arc::new(set)))
}

#[tokio::test]
async fn a_moved_poke_stays_masked_in_its_new_hosts_log() {
    let worker = Worker::new().await;
    let new_root = worker.dir.path().join("sha256-abc");
    std::fs::create_dir_all(&new_root).unwrap();
    std::fs::write(new_root.join("a_sync.py"), DAGS).unwrap();
    let copies = BTreeMap::from([("etl".to_owned(), new_root)]);
    let changing = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        worker.hosts.code_changed(&copies);
        while !worker.log("polls").contains("moved to new code") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        std::fs::write(worker.dir.path().join("ready"), "").unwrap();
    };
    let (ran, ()) = tokio::join!(
        worker.run("polls", no_stop(), masking_the_secret()),
        changing
    );
    assert_eq!(ran.moved_to.as_deref(), Some("sha256:abc"));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let logs = host_logs(&worker);
    assert!(logs.contains("native ****"), "{logs}");
    assert!(!logs.contains("hunter22-pw"), "{logs}");
}

#[tokio::test]
async fn what_new_code_raises_is_masked_in_the_note_about_it() {
    let worker = Worker::new().await;
    let new_root = worker.dir.path().join("sha256-abc");
    std::fs::create_dir_all(&new_root).unwrap();
    std::fs::write(
        new_root.join("a_sync.py"),
        "raise RuntimeError('hunter22-pw')\n",
    )
    .unwrap();
    let copies = BTreeMap::from([("etl".to_owned(), new_root)]);
    let changing = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        worker.hosts.code_changed(&copies);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        std::fs::write(worker.dir.path().join("ready"), "").unwrap();
    };
    let (ran, ()) = tokio::join!(
        worker.run("polls", no_stop(), masking_the_secret()),
        changing
    );
    assert_eq!(output_of(&ran), json!("old"));
    let log = worker.log("polls");
    assert!(log.contains("RuntimeError: ****"), "{log}");
    assert!(!log.contains("hunter22-pw"), "{log}");
}

#[tokio::test]
async fn a_new_signature_that_doesnt_fit_the_run_keeps_the_poke_on_its_code() {
    let worker = Worker::new().await;
    let changed = DAGS.replace("async def polls()", "async def polls(required: int)");
    let ran = poke_with_new_code(&worker, &changed).await;
    assert_eq!(output_of(&ran), json!("old"));
    let log = worker.log("polls");
    assert!(log.contains("required"), "{log}");
    assert!(!log.contains("moved to new code"), "{log}");
}

#[tokio::test]
async fn losing_the_agent_ends_what_watchers_started() {
    let worker = Worker::new().await;
    let log = worker.dir.path().join("logs/host.log");
    let watchers = spec(&worker.setup, &worker.root, &log, "watchers");
    let mut runner = start_runner(&watchers, RunnerMode::Watchers, |_| {})
        .await
        .unwrap();
    let sys_path = vec![worker.root.display().to_string()];
    let request = AgentMessage::Watchers(WatchersRequest {
        sys_path,
        block_limit_ms: 30_000,
        import_limit_ms: 30_000,
    });
    write_message(&mut runner.stream, &request).await.unwrap();
    let first = AgentMessage::StartWatcher {
        watcher: 1,
        task: Box::new(worker.request("starts_a_process", &[&worker.root])),
        cancel_grace_ms: 500,
        moved_in: false,
    };
    write_message(&mut runner.stream, &first).await.unwrap();
    let child_file = worker.dir.path().join("child");
    let mut child = String::new();
    for _ in 0..400 {
        child = std::fs::read_to_string(&child_file).unwrap_or_default();
        if !child.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!child.is_empty(), "no process started");
    // The agent's end of the socket closes: it died.
    let leader = runner.process;
    drop(runner.stream);
    let stat = format!("/proc/{child}/stat");
    for _ in 0..100 {
        // Gone, or a zombie no one reaps here.
        let state = std::fs::read_to_string(&stat).unwrap_or_default();
        if state.is_empty()
            || state
                .rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z'))
        {
            drop(leader);
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("process {child} outlived its host");
}

#[tokio::test]
async fn a_poke_whose_new_host_died_goes_on_on_its_code() {
    // Told to stop while in a call, its new host then dies: with nowhere to go, when its call
    // ends it starts again where it was.
    let worker = Worker::new().await;
    let new_root = worker.dir.path().join("sha256-abc");
    std::fs::create_dir_all(&new_root).unwrap();
    std::fs::write(
        new_root.join("a_sync.py"),
        DAGS.replace(
            "CODE = \"old\"",
            "CODE = \"new\"\nimport threading\nthreading.Timer(1, lambda: os._exit(9)).start()",
        ),
    )
    .unwrap();
    let copies = BTreeMap::from([("etl".to_owned(), new_root)]);
    let changing = async {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        worker.hosts.code_changed(&copies);
        tokio::time::sleep(Duration::from_millis(3000)).await;
        // By now it was told to stop and its new host is gone.
        let log = worker.log("slow_call");
        assert!(log.contains("its host ended"), "{log}");
        std::fs::write(worker.dir.path().join("release"), "").unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        std::fs::write(worker.dir.path().join("ready"), "").unwrap();
    };
    let (ran, ()) = tokio::join!(worker.run("slow_call", no_stop(), mask_none()), changing);
    assert_eq!(output_of(&ran), json!("old"), "{}", worker.log("slow_call"));
    assert_eq!(ran.moved_to, None);
}

/// A storage that never answers.
#[derive(Debug)]
struct NeverAnswers;

impl schedr_agent::secrets::SecretStorage for NeverAnswers {
    fn read<'a>(&'a self, _path: &'a str) -> schedr_agent::secrets::ReadFuture<'a> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn a_cancel_during_a_slow_secret_read_leaves_the_others_alone() {
    let worker = Worker::new().await;
    let request = worker.request("slow_reader", &[&worker.root]);
    let secrets = schedr_agent::secrets::AttemptSecrets::fetch(
        std::sync::Arc::new(schedr_agent::secrets::SecretStores::for_test(NeverAnswers)),
        &[],
        request.task.secrets.clone(),
    )
    .await
    .unwrap();
    let task = schedr_agent::launch::PythonTask {
        events: None,
        request,
        secrets: Some(std::sync::Arc::new(secrets)),
        handles: None,
        api: None,
    };
    let process = spec(
        &worker.setup,
        &worker.root,
        &worker.log_file("slow_reader"),
        "task",
    );
    let limits = Limits {
        timeout: None,
        grace: Duration::from_millis(500),
        interrupt_grace: Duration::from_millis(500),
        poll: Duration::from_millis(50),
    };
    let (cancel, stops) = Stops::new();
    let reader = Box::pin(run_watcher(
        &worker.hosts,
        &worker.setup,
        &process,
        task,
        limits,
        stops,
        |_| {},
    ));
    let (cancel_peer, peer_stops) = Stops::new();
    let peer = worker.run("forever", peer_stops, mask_none());
    let stopping = async {
        while !worker.dir.path().join("reading").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel.send(StopReason::Cancelled).unwrap();
        // Past its grace: a host killed for it would have ended the peer by now.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        cancel_peer.send(StopReason::Cancelled).unwrap();
    };
    let (reader, peer, ()) = tokio::join!(reader, peer, stopping);
    let reader = reader.unwrap();
    assert!(!reader.supervised.killed);
    assert_eq!(reader.supervised.termination, Termination::Signaled(15));
    assert_eq!(peer.supervised.stopped, Some(StopReason::Cancelled));
    assert_eq!(peer.supervised.termination, Termination::Signaled(15));
}

#[tokio::test]
async fn an_attempt_dropped_is_still_held_to_its_cancels_grace() {
    let worker = Worker::new().await;
    let peer = worker.run("waits_for_release", no_stop(), mask_none());
    let dropped = async {
        worker.peer_started().await;
        let refusing = worker.run("refuses", no_stop(), mask_none());
        let _ = tokio::time::timeout(Duration::from_millis(1500), refusing).await;
        // Its coroutine won't end, so the host goes, and the peer starts again.
        worker.release_peer_started_again().await;
    };
    let (peer, ()) = tokio::time::timeout(
        Duration::from_secs(30),
        Box::pin(async { tokio::join!(peer, dropped) }),
    )
    .await
    .expect("the host wasn't killed");
    worker.assert_started_again(&peer);
}

#[tokio::test]
async fn a_thread_has_its_own_steps_grace_not_that_of_who_started_the_host() {
    let worker = Worker::new().await;
    let (cancel, stops) = Stops::new();
    let root: &[&Path] = &[&worker.root];
    let peer = worker.run_with_grace(
        "forever",
        root,
        stops,
        mask_none(),
        Duration::from_millis(50),
    );
    let slow = async {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let ran = worker
            .run_with_grace(
                "thread_stopping_slowly",
                root,
                no_stop(),
                mask_none(),
                Duration::from_secs(5),
            )
            .await;
        // Its 0.7 s are within its own grace, and far past the peer's.
        assert_eq!(worker.hosts.running().await, 1);
        cancel.send(StopReason::Cancelled).unwrap();
        ran
    };
    let (peer, slow) = tokio::join!(peer, slow);
    assert!(!worker.log("thread_stopping_slowly").contains("didn't stop"));
    assert!(matches!(
        slow.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    assert_eq!(peer.supervised.stopped, Some(StopReason::Cancelled));
}

#[tokio::test]
async fn a_blocked_loop_fails_who_blocked_it_and_the_others_start_again() {
    let mut worker = Worker::new().await;
    worker.setup.timeouts.watcher_block_limit = Duration::from_millis(800);
    let metrics = WatcherMetrics::default();
    worker.hosts = WatcherHosts::with_metrics(metrics.clone());
    let peer = worker.run("waits_for_release", no_stop(), mask_none());
    let blocking = async {
        worker.peer_started().await;
        let ran = worker.run("holds_the_loop", no_stop(), mask_none()).await;
        worker.release_peer_started_again().await;
        ran
    };
    let (peer, blocker) = tokio::join!(peer, blocking);
    assert_eq!(metrics.blocked_kills.get(), 1);
    assert_eq!(metrics.restarts.get(), 1);
    let Some(RunnerMessage::TaskFailed { error, kind }) = &blocker.runner_result else {
        panic!("{:?}", blocker.runner_result);
    };
    assert_eq!(kind.as_deref(), Some("blocked_event_loop"));
    assert!(error.traceback.contains("holds_the_loop"), "{error:?}");
    assert!(error.message.contains("block_limit"), "{error:?}");
    // Its retries are untouched: it ran on, on another host.
    worker.assert_started_again(&peer);
}

#[tokio::test]
async fn a_host_that_stops_answering_is_killed_and_its_wait_steps_are_lost() {
    let mut worker = Worker::new().await;
    worker.setup.timeouts.watcher_block_limit = Duration::from_millis(500);
    worker.setup.timeouts.watcher_import_limit = Duration::from_millis(500);
    let ran = worker.run("holds_everything", no_stop(), mask_none()).await;
    assert_eq!(ran.runner_result, None);
    let why = ran.host_lost.unwrap();
    assert!(why.contains("didn't answer"), "{why}");
}

#[tokio::test]
async fn a_host_is_told_as_it_is_and_one_lost_is_counted_by_its_code() {
    let mut worker = Worker::new().await;
    worker.setup.memory.interval = Duration::from_millis(50);
    let metrics = WatcherMetrics::default();
    worker.hosts = WatcherHosts::with_metrics(metrics.clone());
    assert_eq!(worker.hosts.reports(), []);
    let peer = worker.run("waits_for_release", no_stop(), mask_none());
    let told = async {
        worker.peer_started().await;
        let report = loop {
            let reports = worker.hosts.reports();
            if let Some(report) = reports.first().filter(|r| r.threads > 0) {
                break report.clone();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        std::fs::write(worker.dir.path().join("release"), "").unwrap();
        report
    };
    let (ran, report) = tokio::join!(peer, told);
    assert!(matches!(
        ran.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    assert_eq!(report.wait_steps, 1);
    assert_eq!(report.tied_threads, 0, "{report:?}");
    assert!(report.memory_bytes > 0, "{report:?}");
    assert_eq!(report.hosts_lost, 0);
    assert_eq!(metrics.hosts.get(), 1);
    assert!(metrics.memory.get() > 0);
    assert_eq!(
        metrics.blocked_kills.get(),
        0,
        "a host stopped empty isn't killed"
    );

    // A host left with no wait steps counts for nothing; a crash counts for its code.
    let crashed = worker.run("crashes", no_stop(), mask_none()).await;
    assert!(crashed.host_lost.is_some());
    // A look begun before the crash may still count its host: the next one doesn't.
    let code = loop {
        let reports = worker.hosts.reports();
        let gone = metrics.hosts.get() == 0;
        if let Some(report) = reports.iter().find(|r| gone && r.hosts_lost > 0) {
            assert_eq!(report.hosts_lost, 1);
            assert_eq!(report.threads, 0, "{report:?}");
            assert_eq!(reports.len(), 1, "a host that died isn't one running");
            break report.code.clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(
        metrics.hosts_lost.get_or_create(&CodeLabels { code }).get(),
        1
    );
    assert_eq!(metrics.hosts.get(), 0);
    assert_eq!(metrics.restarts.get(), 0);
}

#[tokio::test]
async fn a_slow_import_isnt_a_blocked_loop() {
    let mut worker = Worker::new().await;
    worker.setup.timeouts.watcher_block_limit = Duration::from_millis(300);
    std::fs::write(worker.dir.path().join("slow_import"), "").unwrap();
    let ran = worker.run("alpha", no_stop(), mask_none()).await;
    assert!(
        matches!(ran.runner_result, Some(RunnerMessage::TaskSucceeded { .. })),
        "{:?}",
        ran.runner_result
    );
}

#[tokio::test]
async fn a_thread_a_step_left_running_is_said_in_its_log() {
    let worker = Worker::new().await;
    let ran = worker.run("leaves_a_thread", no_stop(), mask_none()).await;
    assert!(matches!(
        ran.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    let log = worker.log("leaves_a_thread");
    assert!(
        log.contains("still running") && log.contains("left-behind"),
        "{log}"
    );
}

#[tokio::test]
async fn one_starting_again_keeps_its_deadline_whatever_its_new_host_takes() {
    let mut worker = Worker::new().await;
    worker.setup.timeouts.watcher_block_limit = Duration::from_millis(500);
    worker.setup.timeouts.watcher_import_limit = Duration::from_secs(30);
    let peer = worker.run("waits_out_its_time", no_stop(), mask_none());
    let starts = worker.dir.path().join("waits_starts");
    let blocking = async {
        worker.peer_started().await;
        // Two of its 3 s go on the new host's import.
        std::fs::write(worker.dir.path().join("slow_import"), "").unwrap();
        worker.run("holds_the_loop", no_stop(), mask_none()).await
    };
    let (peer, _) = tokio::join!(peer, blocking);
    let ended = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    assert!(
        matches!(&peer.runner_result, Some(RunnerMessage::TaskSucceeded { outcome, .. }) if outcome == "TimedOut"),
        "{:?}",
        peer.runner_result
    );
    let starts = std::fs::read_to_string(&starts).unwrap();
    let times: Vec<f64> = starts.lines().map(|line| line.parse().unwrap()).collect();
    assert_eq!(times.len(), 2, "{starts}");
    // A fresh count on the new host would end over 5 s after the first call.
    let took = ended - times[0];
    assert!(took < 4.5, "{took} s for a `max_wait` of 3 s");
}

#[tokio::test]
async fn hosts_over_their_memory_budget_end_their_wait_steps_lost() {
    let mut worker = Worker::new().await;
    worker.setup.timeouts.watcher_memory_budget = 1;
    let ran = worker.run("forever", no_stop(), mask_none()).await;
    assert_eq!(ran.runner_result, None);
    let why = ran.host_lost.unwrap();
    assert!(why.contains("memory_budget"), "{why}");
}

#[tokio::test]
async fn more_output_than_a_route_holds_all_arrives_and_the_others_go_on() {
    let worker = Worker::new().await;
    let (flood, peer) = tokio::join!(
        worker.run("floods_its_log", no_stop(), mask_none()),
        worker.run("alpha", no_stop(), mask_none()),
    );
    assert!(matches!(
        flood.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    assert!(matches!(
        peer.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
    let log = worker.log("floods_its_log");
    assert!(
        log.contains("line 0\n") && log.contains("line 499\n"),
        "{log}"
    );
}

#[tokio::test]
async fn a_cancel_stops_a_steps_thread_and_leaves_the_others_alone() {
    let worker = Worker::new().await;
    let (cancel, stops) = Stops::new();
    let step = worker.run("in_a_thread_stoppable", stops, masking_the_secret());
    let peer = worker.run("alpha", no_stop(), mask_none());
    let spinning = worker.dir.path().join("spinning");
    let cancelling = async {
        while !spinning.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.send(StopReason::Cancelled).unwrap();
    };
    let (step, peer, ()) = tokio::join!(step, peer, cancelling);
    assert!(!step.supervised.killed);
    assert_eq!(step.supervised.stopped, Some(StopReason::Cancelled));
    assert!(worker.dir.path().join("spin_stopped").exists());
    let log = worker.log("in_a_thread_stoppable");
    assert!(log.contains("thread says"), "{log}");
    assert!(!log.contains("hunter22-pw"), "{log}");
    assert!(matches!(
        peer.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
}

#[tokio::test]
async fn a_steps_time_running_out_stops_its_thread() {
    let worker = Worker::new().await;
    let ran = worker
        .run("thread_out_of_time", no_stop(), mask_none())
        .await;
    assert!(
        matches!(&ran.runner_result, Some(RunnerMessage::TaskSucceeded { outcome, .. }) if outcome == "TimedOut"),
        "{:?}",
        ran.runner_result
    );
    assert!(worker.dir.path().join("spin_stopped").exists());
    assert!(!ran.supervised.killed);
}

#[tokio::test]
async fn a_thread_that_wont_stop_ends_its_host_after_telling_the_result() {
    let worker = Worker::new().await;
    let peer = worker.run("waits_for_release", no_stop(), mask_none());
    let stuck = async {
        worker.peer_started().await;
        let stuck = worker
            .run("thread_blocked_in_c", no_stop(), mask_none())
            .await;
        worker.release_peer_started_again().await;
        stuck
    };
    let (peer, stuck) = tokio::join!(peer, stuck);
    assert!(
        matches!(&stuck.runner_result, Some(RunnerMessage::TaskSucceeded { outcome, .. }) if outcome == "TimedOut"),
        "{:?}",
        stuck.runner_result
    );
    let log = worker.log("thread_blocked_in_c");
    assert!(log.contains("didn't stop in time"), "{log}");
    worker.assert_started_again(&peer);
}

#[tokio::test]
async fn a_wait_step_cant_start_a_process_and_others_go_on() {
    let worker = Worker::new().await;
    let (plain, asynchronous, threaded, alpha) = tokio::join!(
        worker.run("refused_a_process", no_stop(), mask_none()),
        worker.run("refused_async_process", no_stop(), mask_none()),
        worker.run("refused_in_a_thread", no_stop(), mask_none()),
        worker.run("alpha", no_stop(), mask_none()),
    );
    for refused in [plain, asynchronous, threaded] {
        let Some(RunnerMessage::TaskFailed { error, .. }) = &refused.runner_result else {
            panic!("{:?}", refused.runner_result);
        };
        assert_eq!(error.exception, "ProcessInWaitStepError");
        assert!(
            error.message.contains("can't start a process"),
            "{}",
            error.message
        );
    }
    assert!(matches!(
        alpha.runner_result,
        Some(RunnerMessage::TaskSucceeded { .. })
    ));
}
