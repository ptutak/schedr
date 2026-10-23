//! Real processes under the agent's supervision (PLAN.md 1.5): exit codes and log files,
//! timeouts, SIGTERM then SIGKILL, process groups, output floods, the clean environment.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pretty_assertions::assert_eq;
use schedr_agent::output::process_result;
use schedr_agent::process::{ProcessSpec, spawn};
use schedr_agent::status::{StopReason, Termination};
use schedr_agent::supervise::{Limits, supervise};
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

fn never() -> impl Future<Output = StopReason> {
    std::future::pending()
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
    let stop = async { StopReason::Cancelled };
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
