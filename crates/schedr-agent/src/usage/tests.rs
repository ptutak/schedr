use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;

/// A fake `/proc` with process 42 in state `state`: utime 150, stime 40, cutime 50,
/// cstime 10 ticks, 3 + 2 major faults.
fn fake_proc(state: &str) -> TempDir {
    let dir = TempDir::new().unwrap();
    let process = dir.path().join("42");
    std::fs::create_dir(&process).unwrap();
    // Fields 1-17: pid (comm) state ppid pgrp session tty tpgid flags minflt cminflt majflt
    // cmajflt utime stime cutime cstime.
    let stat = format!("42 (a (b) c) {state} 1 42 42 0 -1 0 100 5 3 2 150 40 50 10 20 0\n");
    std::fs::write(process.join("stat"), stat).unwrap();
    dir
}

#[test]
fn a_zombies_usage_counts_its_reaped_children_and_has_no_peak_rss_or_switches() {
    let proc = fake_proc("Z");
    assert_eq!(
        zombie_usage(proc.path(), 42, 100),
        Some(ProcessUsage {
            user_cpu_micros: 2_000_000,
            system_cpu_micros: 500_000,
            peak_rss_bytes: None,
            major_faults: 5,
            voluntary_context_switches: None,
            involuntary_context_switches: None,
        })
    );
}

#[test]
fn a_running_or_missing_process_has_no_usage() {
    let proc = fake_proc("S");
    assert_eq!(zombie_usage(proc.path(), 42, 100), None);
    assert_eq!(zombie_usage(proc.path(), 43, 100), None);
}

#[tokio::test]
async fn an_exited_child_is_seen_without_being_reaped() {
    let mut child = tokio::process::Command::new("true").spawn().unwrap();
    let pid = Pid::from_raw(i32::try_from(child.id().unwrap()).unwrap()).unwrap();
    exited(pid).await.unwrap();
    let usage = zombie_usage(
        Path::new("/proc"),
        pid.as_raw_nonzero().get(),
        clock_ticks(),
    );
    let usage = usage.expect("still a zombie, read");
    // Its leader thread's switches only are in `/proc`: none reported rather than too few.
    assert_eq!(
        (
            usage.voluntary_context_switches,
            usage.involuntary_context_switches
        ),
        (None, None)
    );
    assert!(child.wait().await.unwrap().success());
    // Reaped: no longer a child.
    assert!(exited(pid).await.is_err());
}
