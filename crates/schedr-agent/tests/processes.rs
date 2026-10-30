//! Real processes under the agent's supervision (PLAN.md 1.5): exit codes and log files,
//! timeouts, SIGTERM then SIGKILL, process groups, output floods, the clean environment.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_agent::launch::TaskProcess;
use schedr_agent::output::TailBuffer;
use schedr_agent::output::process_result;
use schedr_agent::process::OsProcess;
use schedr_agent::process::{ProcessSpec, spawn};
use schedr_agent::status::{StopReason, Termination};
use schedr_agent::supervise::{Limits, Process, Stops, supervise};
use tempfile::TempDir;

fn shell(dir: &Path, script: &str, max_output_size: usize) -> ProcessSpec {
    ProcessSpec {
        program: PathBuf::from("/bin/sh"),
        args: vec!["-c".to_owned(), script.to_owned()],
        env: BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
        working_dir: dir.to_path_buf(),
        log_file: dir.join("task.log"),
        max_output_size,
    }
}

const NO_LIMITS: Limits = Limits {
    timeout: None,
    grace: Duration::from_secs(5),
    interrupt_grace: Duration::from_secs(5),
};

/// A process whose output can be collected once it ended.
trait Collect {
    async fn collect(self) -> std::io::Result<(TailBuffer, TailBuffer)>;
}

impl Collect for OsProcess {
    async fn collect(self) -> std::io::Result<(TailBuffer, TailBuffer)> {
        self.output().await
    }
}

impl Collect for TaskProcess {
    async fn collect(self) -> std::io::Result<(TailBuffer, TailBuffer)> {
        self.output().await
    }
}

/// No reason to stop, ever.
fn never() -> Stops {
    Stops::new().1
}

#[tokio::test]
async fn the_exit_code_output_and_log_file_are_kept() {
    let dir = TempDir::new().unwrap();
    let spec = shell(dir.path(), "echo out; echo err >&2; exit 3", 1024);
    let mut process = spawn(&spec).await.unwrap();
    let pid = rustix::process::Pid::from_raw(process.id()).unwrap();
    assert_eq!(
        rustix::process::getpgid(Some(pid)).unwrap(),
        pid,
        "it leads a group of its own"
    );
    let supervised = supervise(&mut process, NO_LIMITS, never()).await.unwrap();
    assert_eq!(supervised.termination, Termination::Exited(3));
    assert_eq!(supervised.stopped, None);
    let (stdout, stderr) = process.output().await.unwrap();
    assert_eq!(
        (stdout.text(), stderr.text()),
        ("out\n".to_owned(), "err\n".to_owned())
    );
    let log = std::fs::read_to_string(dir.path().join("task.log")).unwrap();
    assert!(log.contains("out\n") && log.contains("err\n"), "{log}");
    let result = process_result(&stdout, &stderr, supervised.termination.exit_code());
    assert_eq!(result["exit_code"], 3);
}

#[tokio::test]
async fn a_process_past_its_timeout_is_stopped_with_sigterm() {
    let dir = TempDir::new().unwrap();
    let mut process = spawn(&shell(dir.path(), "exec sleep 30", 1024))
        .await
        .unwrap();
    let limits = Limits {
        timeout: Some(Duration::from_millis(200)),
        grace: Duration::from_secs(5),
        interrupt_grace: Duration::from_secs(5),
    };
    let supervised = supervise(&mut process, limits, never()).await.unwrap();
    assert_eq!(supervised.stopped, Some(StopReason::TimedOut));
    assert_eq!(supervised.termination, Termination::Signaled(15));
    assert!(!supervised.killed);
    assert_eq!(supervised.termination.exit_code(), 128 + 15);
}

#[tokio::test]
async fn a_process_ignoring_sigterm_is_killed_after_the_grace_time() {
    let dir = TempDir::new().unwrap();
    let script = "trap '' TERM; while true; do sleep 0.1; done";
    let mut process = spawn(&shell(dir.path(), script, 1024)).await.unwrap();
    let limits = Limits {
        timeout: Some(Duration::from_millis(200)),
        grace: Duration::from_millis(300),
        interrupt_grace: Duration::from_millis(300),
    };
    let started = std::time::Instant::now();
    let supervised = supervise(&mut process, limits, never()).await.unwrap();
    assert!(supervised.killed);
    assert_eq!(supervised.termination, Termination::Signaled(9));
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn the_whole_process_group_is_stopped() {
    let dir = TempDir::new().unwrap();
    let pid_file = dir.path().join("child.pid");
    let script = format!("sleep 30 & echo $! > {}; wait", pid_file.display());
    let mut process = spawn(&shell(dir.path(), &script, 1024)).await.unwrap();
    while !pid_file.exists()
        || std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .is_empty()
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let child = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .to_owned();
    let (sender, stop) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    let supervised = supervise(&mut process, NO_LIMITS, stop).await.unwrap();
    assert_eq!(supervised.stopped, Some(StopReason::Cancelled));
    for _ in 0..100 {
        let status = std::fs::read_to_string(format!("/proc/{child}/status"));
        let gone = status.is_err() || status.is_ok_and(|s| s.contains("State:\tZ"));
        if gone {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the child process {child} survived its group being stopped");
}

#[tokio::test]
async fn a_stop_ends_even_when_a_child_outlives_the_leader_with_its_output_open() {
    child_outlives_the_leader(false).await;
}

/// The same through the wrapper every Python task runs in.
#[tokio::test]
async fn a_task_process_stops_what_outlives_its_leader_too() {
    child_outlives_the_leader(true).await;
}

async fn child_outlives_the_leader(wrapped: bool) {
    // B55: the leader exits on SIGTERM; its child ignores SIGTERM and keeps stdout open.
    let dir = TempDir::new().unwrap();
    let pid_file = dir.path().join("child.pid");
    let script = format!(
        "(trap '' TERM; while true; do sleep 1; done) & echo $! > {}; \
         trap 'exit 0' TERM; while true; do sleep 1; done",
        pid_file.display()
    );
    let process = spawn(&shell(dir.path(), &script, 1024)).await.unwrap();
    if wrapped {
        stop_and_collect(TaskProcess::Spawned(process), &pid_file).await;
    } else {
        stop_and_collect(process, &pid_file).await;
    }
}

/// Stops `process` (whose child's PID is written to `pid_file`), then checks that its output
/// is collected and the child is gone.
async fn stop_and_collect<P: Process + Collect>(mut process: P, pid_file: &Path) {
    while !pid_file.exists() || std::fs::read_to_string(pid_file).unwrap().trim().is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let child = std::fs::read_to_string(pid_file).unwrap().trim().to_owned();
    let (sender, stop) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    let supervised = supervise(&mut process, NO_LIMITS, stop).await.unwrap();
    assert_eq!(supervised.stopped, Some(StopReason::Cancelled));
    // The output is collected once the group is gone: within the grace, not never.
    let collected = tokio::time::timeout(Duration::from_secs(10), process.collect()).await;
    assert!(
        collected.is_ok(),
        "still collecting the output: the child {child} keeps it open"
    );
    for _ in 0..100 {
        let status = std::fs::read_to_string(format!("/proc/{child}/status"));
        if status.is_err() || status.is_ok_and(|s| s.contains("State:\tZ")) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the child process {child} survived its leader being stopped");
}

#[tokio::test]
async fn a_child_still_shutting_down_keeps_the_rest_of_the_grace() {
    // Decision 100: the leader exits on SIGTERM at once; its child takes a second to shut
    // down cleanly, within the grace, and must not be killed for it.
    let dir = TempDir::new().unwrap();
    let ready = dir.path().join("ready");
    let done = dir.path().join("done");
    let script = format!(
        "(trap 'sleep 1; touch {done}; exit 0' TERM; touch {ready}; while true; do sleep 0.1; done) & \
         trap 'exit 0' TERM; while true; do sleep 1; done",
        done = done.display(),
        ready = ready.display()
    );
    let mut process = spawn(&shell(dir.path(), &script, 1024)).await.unwrap();
    while !ready.exists() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (sender, stop) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    let supervised = supervise(&mut process, NO_LIMITS, stop).await.unwrap();
    assert_eq!(supervised.stopped, Some(StopReason::Cancelled));
    assert!(
        done.exists(),
        "the child was killed before it finished shutting down"
    );
}

#[tokio::test]
async fn a_group_of_zombies_is_gone() {
    // An agent running as a container's first process (or a subreaper) adopts its tasks'
    // orphans, which stay zombies until it reaps them: they are no longer running, and a
    // stop must not wait out the grace for them.
    rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
    let dir = TempDir::new().unwrap();
    let pid_file = dir.path().join("child.pid");
    let script = format!(
        "(trap 'sleep 0.3; exit 0' TERM; while true; do sleep 0.1; done) & echo $! > {}; \
         trap 'exit 0' TERM; while true; do sleep 1; done",
        pid_file.display()
    );
    let mut process = spawn(&shell(dir.path(), &script, 1024)).await.unwrap();
    while std::fs::read_to_string(&pid_file).map_or(true, |p| p.trim().is_empty()) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let child: i32 = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let (sender, stop) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    let started = std::time::Instant::now();
    let supervised = supervise(&mut process, NO_LIMITS, stop).await.unwrap();
    let took = started.elapsed();
    // Reap the orphan, a zombie by now (adopted by this test process).
    let _ = rustix::process::waitpid(
        rustix::process::Pid::from_raw(child),
        rustix::process::WaitOptions::empty(),
    );
    assert!(!supervised.killed, "the child had ended by itself");
    assert!(
        took < Duration::from_secs(3),
        "waited {took:?} for a group of zombies (grace {:?})",
        NO_LIMITS.grace
    );
}

/// Whether any thread of process `pid` still runs (a zombie thread doesn't).
fn any_thread_running(pid: i32) -> bool {
    let Ok(threads) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return false;
    };
    threads.flatten().any(|thread| {
        std::fs::read_to_string(thread.path().join("stat")).is_ok_and(|stat| {
            let state = stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().next());
            !matches!(state, Some("Z" | "X" | "x"))
        })
    })
}

#[tokio::test]
async fn a_process_whose_main_thread_ended_still_runs() {
    // A process whose main thread called `pthread_exit` shows `Z` in `/proc/<pid>/stat`
    // while its other threads still run: its group isn't gone, and a stop must kill it after
    // the grace, not return at once and leave it running.
    let dir = TempDir::new().unwrap();
    let pid_file = dir.path().join("helper.pid");
    let helper = dir.path().join("helper.py");
    std::fs::write(
        &helper,
        format!(
            "import ctypes, os, signal, threading, time\n\
             signal.signal(signal.SIGTERM, signal.SIG_IGN)\n\
             threading.Thread(target=time.sleep, args=(60,)).start()\n\
             open({pid:?}, 'w').write(str(os.getpid()))\n\
             ctypes.CDLL(None).pthread_exit(None)\n",
            pid = pid_file.display().to_string()
        ),
    )
    .unwrap();
    let script = format!(
        "python3 {} & trap 'exit 0' TERM; while true; do sleep 1; done",
        helper.display()
    );
    let mut process = spawn(&shell(dir.path(), &script, 1024)).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let helper_pid = loop {
        if let Ok(Ok(pid)) = std::fs::read_to_string(&pid_file).map(|p| p.trim().parse::<i32>()) {
            break pid;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the helper never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Its main thread has ended.
    while !std::fs::read_to_string(format!("/proc/{helper_pid}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(')')
            .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z'))
    }) {
        assert!(
            std::time::Instant::now() < deadline,
            "the helper's main thread never ended"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let limits = Limits {
        grace: Duration::from_secs(1),
        ..NO_LIMITS
    };
    let (sender, stop) = Stops::new();
    sender.send(StopReason::Cancelled).unwrap();
    supervise(&mut process, limits, stop).await.unwrap();
    let mut still_running = any_thread_running(helper_pid);
    for _ in 0..50 {
        if !still_running {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        still_running = any_thread_running(helper_pid);
    }
    if still_running {
        // Clean up after a failure: this process only, by its own PID.
        let _ = rustix::process::kill_process(
            rustix::process::Pid::from_raw(helper_pid).unwrap(),
            rustix::process::Signal::KILL,
        );
    }
    assert!(
        !still_running,
        "the stop left a process with a running thread behind"
    );
}

#[tokio::test]
async fn an_output_flood_keeps_only_the_tail_but_the_log_has_everything() {
    let dir = TempDir::new().unwrap();
    let script = "yes 0123456789 | head -c 2000000";
    let mut process = spawn(&shell(dir.path(), script, 1000)).await.unwrap();
    let supervised = supervise(&mut process, NO_LIMITS, never()).await.unwrap();
    assert_eq!(supervised.termination, Termination::Exited(0));
    let (stdout, _) = process.output().await.unwrap();
    assert!(stdout.truncated());
    assert_eq!(stdout.text().len(), 1000);
    let log = std::fs::metadata(dir.path().join("task.log")).unwrap();
    assert_eq!(log.len(), 2_000_000);
}

#[tokio::test]
async fn the_process_gets_only_its_environment_and_working_directory() {
    let dir = TempDir::new().unwrap();
    let mut spec = shell(dir.path(), "env; pwd", 64 * 1024);
    spec.env.insert("SCHEDR_RUN_ID".to_owned(), "r1".to_owned());
    // The test process has HOME, USER, ... set: none of them may reach the child.
    let mut process = spawn(&spec).await.unwrap();
    supervise(&mut process, NO_LIMITS, never()).await.unwrap();
    let (stdout, _) = process.output().await.unwrap();
    let text = stdout.text();
    let mut names: Vec<&str> = text
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name))
        .collect();
    names.retain(|name| !["PWD", "SHLVL", "_"].contains(name));
    names.sort_unstable();
    assert_eq!(names, ["PATH", "SCHEDR_RUN_ID"]);
    let working_dir = dir.path().canonicalize().unwrap();
    assert!(text.lines().any(|l| Path::new(l) == working_dir), "{text}");
}

#[tokio::test]
async fn a_program_that_doesnt_exist_fails_to_start() {
    let dir = TempDir::new().unwrap();
    let mut spec = shell(dir.path(), "", 1024);
    spec.program = dir.path().join("no-such-program");
    assert!(spawn(&spec).await.is_err());
}
