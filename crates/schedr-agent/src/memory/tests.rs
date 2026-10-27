use pretty_assertions::assert_eq;
use tempfile::TempDir;

use prometheus_client::metrics::counter::Counter;

use super::*;
use crate::status::StopReason;
use crate::supervise::Stops;

/// A fake `/proc` with processes `(pid, pgid, smaps_rollup Pss in kB or none, statm resident
/// pages)`.
fn fake_proc(processes: &[(i32, i32, Option<u64>, u64)]) -> TempDir {
    let dir = TempDir::new().unwrap();
    for (pid, pgid, pss, resident) in processes {
        let process = dir.path().join(pid.to_string());
        std::fs::create_dir_all(&process).unwrap();
        // The command holds a space and a parenthesis, as commands may.
        std::fs::write(
            process.join("stat"),
            format!("{pid} (python3 -m x)) S 1 {pgid} {pgid} 0 -1 4194304 0 0"),
        )
        .unwrap();
        if let Some(kib) = pss {
            std::fs::write(
                process.join("smaps_rollup"),
                format!("00400000-7ffd [rollup]\nRss:    9999 kB\nPss:    {kib} kB\n"),
            )
            .unwrap();
        }
        std::fs::write(process.join("statm"), format!("100 {resident} 10 1 0 50 0")).unwrap();
    }
    std::fs::create_dir_all(dir.path().join("self")).unwrap();
    std::fs::write(dir.path().join("sys"), "not a process").unwrap();
    dir
}

#[test]
fn a_groups_memory_is_the_pss_of_its_processes_else_their_rss() {
    let proc = fake_proc(&[
        (100, 100, Some(1000), 1),
        (101, 100, None, 3),
        (200, 200, Some(5000), 1),
    ]);
    let page = page_size();
    assert_eq!(
        group_memory(proc.path(), 100, true),
        Some(1000 * 1024 + 3 * page)
    );
    assert_eq!(group_memory(proc.path(), 200, true), Some(5000 * 1024));
    assert_eq!(group_memory(proc.path(), 300, true), Some(0));
    assert_eq!(
        group_memory(&proc.path().join("missing"), 100, true),
        Some(0)
    );
}

#[test]
fn a_forked_groups_memory_is_unknown_without_every_pss() {
    // A forked process's RSS counts the fork server's shared pages: it never stands in.
    let proc = fake_proc(&[
        (100, 100, Some(1000), 1),
        (101, 100, None, 3),
        (200, 200, Some(5000), 1),
    ]);
    assert_eq!(group_memory(proc.path(), 100, false), None);
    assert_eq!(group_memory(proc.path(), 200, false), Some(5000 * 1024));
    assert_eq!(group_memory(proc.path(), 300, false), Some(0));
}

#[test]
fn the_process_group_is_read_after_the_command() {
    assert_eq!(process_group("42 (a b) c)) S 1 40 40 0"), Some(40));
    assert_eq!(process_group("42 (sh) S 1 7 7"), Some(7));
    assert_eq!(process_group("42 (sh"), None);
    assert_eq!(process_group("42 (sh) S 1"), None);
}

#[test]
fn the_limit_is_the_smallest_memory_max_up_to_the_root() {
    let proc = TempDir::new().unwrap();
    std::fs::create_dir_all(proc.path().join("self")).unwrap();
    std::fs::write(proc.path().join("self/cgroup"), "0::/kubepods/pod1/c1\n").unwrap();
    let cgroup = TempDir::new().unwrap();
    let c1 = cgroup.path().join("kubepods/pod1/c1");
    std::fs::create_dir_all(&c1).unwrap();
    std::fs::write(c1.join("memory.max"), "max\n").unwrap();
    std::fs::write(
        cgroup.path().join("kubepods/pod1/memory.max"),
        "2147483648\n",
    )
    .unwrap();
    std::fs::write(cgroup.path().join("kubepods/memory.max"), "8589934592\n").unwrap();
    assert_eq!(
        cgroup_limit(proc.path(), cgroup.path()).unwrap(),
        Some(2 << 30)
    );

    // In a pod with its own cgroup namespace, the path is `/`.
    std::fs::write(proc.path().join("self/cgroup"), "0::/\n").unwrap();
    std::fs::write(cgroup.path().join("memory.max"), "1073741824\n").unwrap();
    assert_eq!(
        cgroup_limit(proc.path(), cgroup.path()).unwrap(),
        Some(1 << 30)
    );
}

#[test]
fn without_a_limit_or_cgroup_v2_there_is_none() {
    let proc = TempDir::new().unwrap();
    std::fs::create_dir_all(proc.path().join("self")).unwrap();
    let cgroup = TempDir::new().unwrap();
    std::fs::write(proc.path().join("self/cgroup"), "0::/user.slice\n").unwrap();
    std::fs::create_dir_all(cgroup.path().join("user.slice")).unwrap();
    std::fs::write(cgroup.path().join("user.slice/memory.max"), "max\n").unwrap();
    assert_eq!(cgroup_limit(proc.path(), cgroup.path()).unwrap(), None);
    // cgroup v1 only.
    std::fs::write(proc.path().join("self/cgroup"), "4:memory:/user.slice\n").unwrap();
    assert_eq!(cgroup_limit(proc.path(), cgroup.path()).unwrap(), None);
    std::fs::remove_file(proc.path().join("self/cgroup")).unwrap();
    assert!(cgroup_limit(proc.path(), cgroup.path()).is_err());
}

#[test]
fn the_budget_is_configured_or_the_limit_less_a_tenth_by_slot() {
    assert_eq!(
        budget(Some(512 << 20), Some(8 << 30), 2, WORKER_RESERVE),
        Some(512 << 20)
    );
    assert_eq!(budget(None, Some(1000), 2, WORKER_RESERVE), Some(450));
    assert_eq!(budget(None, Some(1000), 0, WORKER_RESERVE), Some(900));
    assert_eq!(budget(None, None, 2, WORKER_RESERVE), None);
    // Standalone keeps a quarter; a reserve can be a size; it never goes below zero.
    assert_eq!(budget(None, Some(4000), 2, STANDALONE_RESERVE), Some(1500));
    assert_eq!(
        budget(None, Some(4000), 2, Reserve::Bytes(1000)),
        Some(1500)
    );
    assert_eq!(budget(None, Some(4000), 2, Reserve::Bytes(9000)), Some(0));
}

#[test]
fn sizes_are_shown_as_people_read_them() {
    assert_eq!(show_size(512), "512 B");
    assert_eq!(show_size(64 << 10), "64 KiB");
    assert_eq!(show_size(1536 << 20), "1.5 GiB");
    assert_eq!(show_size(100 << 20), "100 MiB");
    assert_eq!(show_size(u64::MAX), "16777216 TiB");
    assert_eq!(
        over_budget_message(2 << 30, 2_469_606_195),
        "exceeded memory budget 2 GiB (2.3 GiB used)"
    );
}

#[test]
fn the_default_watch_measures_from_proc_every_second() {
    assert_eq!(
        MemoryWatch::default(),
        MemoryWatch {
            budget: None,
            interval: Duration::from_secs(1),
            proc_root: PathBuf::from("/proc"),
        }
    );
}

#[tokio::test]
async fn over_budget_records_the_peak_and_completes_past_the_budget() {
    let proc = fake_proc(&[(100, 100, Some(1000), 0)]);
    let watch = MemoryWatch {
        budget: Some(2000 * 1024),
        interval: Duration::from_millis(10),
        proc_root: proc.path().to_path_buf(),
    };
    let seen = SharedSeen::default();
    let grow = {
        let rollup = proc.path().join("100/smaps_rollup");
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::write(rollup, "Pss: 3000 kB\n").unwrap();
            std::future::pending::<()>().await;
        }
    };
    let used = tokio::select! {
        used = over_budget(&watch, 100, &seen, true) => used,
        () = grow => unreachable!(),
    };
    assert_eq!(used, 3000 * 1024);
    assert_eq!(
        *seen.lock().unwrap(),
        MemorySeen {
            peak: 3000 * 1024,
            over: Some(3000 * 1024)
        }
    );
}

#[tokio::test]
async fn without_a_budget_it_only_measures() {
    let proc = fake_proc(&[(100, 100, Some(1000), 0)]);
    let watch = MemoryWatch {
        interval: Duration::from_millis(10),
        proc_root: proc.path().to_path_buf(),
        ..MemoryWatch::default()
    };
    let seen = SharedSeen::default();
    let waited = tokio::time::timeout(
        Duration::from_millis(100),
        over_budget(&watch, 100, &seen, true),
    )
    .await;
    assert!(waited.is_err());
    assert_eq!(seen.lock().unwrap().peak, 1000 * 1024);
    assert_eq!(seen.lock().unwrap().over, None);
}

#[tokio::test]
async fn a_group_watch_reports_its_kill_and_stops_when_dropped() {
    // A group ID no kernel hands out (above any pid_max): the kill finds nothing.
    let pgid = i32::MAX;
    let proc = fake_proc(&[(pgid, pgid, Some(3000), 0)]);
    let watch = MemoryWatch {
        budget: Some(2000 * 1024),
        interval: Duration::from_millis(10),
        proc_root: proc.path().to_path_buf(),
    };
    let seen = SharedSeen::default();
    let (killed_tx, mut killed) = Stops::new();
    let kills = Counter::default();
    let _watching = watch_group(
        watch.clone(),
        pgid,
        seen.clone(),
        killed_tx,
        kills.clone(),
        false,
    );
    let reason = tokio::time::timeout(Duration::from_secs(5), killed.next_reason())
        .await
        .unwrap();
    assert_eq!(reason, StopReason::OomKilled);
    assert_eq!(seen.lock().unwrap().over, Some(3000 * 1024));
    // Told before the kill, which is counted right after it.
    for _ in 0..100 {
        if kills.get() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(kills.get(), 1);

    let (quiet_tx, mut quiet) = Stops::new();
    let watching = watch_group(
        MemoryWatch {
            budget: None,
            ..watch
        },
        pgid,
        SharedSeen::default(),
        quiet_tx,
        Counter::default(),
        false,
    );
    drop(watching);
    // Dropped: the task is aborted, nothing is ever sent.
    let nothing = tokio::time::timeout(Duration::from_millis(50), quiet.next_reason()).await;
    assert!(nothing.is_err());
}

#[tokio::test]
async fn a_forked_group_over_the_budget_by_rss_alone_isnt_killed() {
    // PSS unreadable, RSS (shared with the fork server) far over the budget: no kill on it.
    let pgid = i32::MAX;
    let proc = fake_proc(&[(pgid, pgid, None, 1_000_000)]);
    let watch = MemoryWatch {
        budget: Some(1024),
        interval: Duration::from_millis(5),
        proc_root: proc.path().to_path_buf(),
    };
    let (stop_tx, mut stop) = Stops::new();
    let kills = Counter::default();
    let seen = SharedSeen::default();
    let _forked = watch_group(
        watch.clone(),
        pgid,
        seen.clone(),
        stop_tx,
        kills.clone(),
        true,
    );
    let nothing = tokio::time::timeout(Duration::from_millis(100), stop.next_reason()).await;
    assert!(nothing.is_err());
    assert_eq!((kills.get(), seen.lock().unwrap().peak), (0, 0));
    // The same group spawned: RSS stands in, and it is killed.
    let (stop_tx, mut stop) = Stops::new();
    let _spawned = watch_group(watch, pgid, SharedSeen::default(), stop_tx, kills, false);
    let reason = tokio::time::timeout(Duration::from_secs(5), stop.next_reason())
        .await
        .unwrap();
    assert_eq!(reason, StopReason::OomKilled);
}
