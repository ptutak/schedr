use std::os::unix::process::CommandExt;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;

fn finished(attempt_id: &str) -> pb::Finished {
    pb::Finished {
        attempt_id: attempt_id.to_owned(),
        status: pb::AttemptStatus::Success.into(),
        output_json: br#"{"rows": 3}"#.to_vec(),
        duration_ms: Some(1500),
        ..pb::Finished::default()
    }
}

fn journal() -> (TempDir, Journal) {
    let dir = TempDir::new().unwrap();
    let journal = Journal::new(dir.path().join("journal"));
    (dir, journal)
}

fn names(journal: &Journal) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(journal.dir())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn records_are_written_replaced_read_back_and_removed() {
    let (_dir, journal) = journal();
    let identity = ProcessIdentity {
        pgid: 4242,
        start_time: 987_654,
    };
    journal.write("a1", &JournalRecord::Running(None)).unwrap();
    journal
        .write("a1", &JournalRecord::Running(Some(identity)))
        .unwrap();
    journal
        .write("a2", &JournalRecord::Finished(finished("a2")))
        .unwrap();
    assert_eq!(names(&journal), ["a1.json", "a2.json"]);
    assert_eq!(
        journal.recover().unwrap(),
        [
            ("a1".to_owned(), JournalRecord::Running(Some(identity))),
            ("a2".to_owned(), JournalRecord::Finished(finished("a2"))),
        ]
    );

    journal.remove("a1").unwrap();
    journal.remove("a1").unwrap();
    assert_eq!(names(&journal), ["a2.json"]);
}

#[test]
fn one_record_is_read_without_changing_anything() {
    let (_dir, journal) = journal();
    assert_eq!(journal.record("a1").unwrap(), None);
    journal.write("a1", &JournalRecord::Running(None)).unwrap();
    assert_eq!(
        journal.record("a1").unwrap(),
        Some(JournalRecord::Running(None))
    );
    journal
        .write("a1", &JournalRecord::Finished(finished("a1")))
        .unwrap();
    assert_eq!(
        journal.record("a1").unwrap(),
        Some(JournalRecord::Finished(finished("a1")))
    );
    fs::write(journal.dir().join("a2.json"), "half").unwrap();
    assert_eq!(journal.record("a2").unwrap(), None);
    assert_eq!(
        journal.record("../a1").unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn recovery_removes_temporary_and_unreadable_files() {
    let (_dir, journal) = journal();
    journal.write("a1", &JournalRecord::Running(None)).unwrap();
    let dir = journal.dir();
    fs::write(dir.join(".tmp-a2"), "{").unwrap();
    fs::write(dir.join("a3.json"), "not json").unwrap();
    fs::write(
        dir.join("a4.json"),
        r#"{"state": "running", "attempt_id": "a1", "process": null}"#,
    )
    .unwrap();
    fs::write(
        dir.join("a5.json"),
        r#"{"state": "finished", "attempt_id": "a5", "finished": "zz"}"#,
    )
    .unwrap();
    fs::write(
        dir.join("a6.json"),
        r#"{"state": "finished", "attempt_id": "a6", "finished": "ff"}"#,
    )
    .unwrap();
    fs::write(
        dir.join("a7.json"),
        r#"{"state": "finished", "attempt_id": "a7", "finished": "abc"}"#,
    )
    .unwrap();
    fs::write(dir.join("notes.txt"), "kept").unwrap();
    let only_a1 = [("a1".to_owned(), JournalRecord::Running(None))];
    assert_eq!(journal.records().unwrap(), only_a1, "read, nothing removed");
    assert_eq!(names(&journal).len(), 8);
    assert_eq!(
        journal.recover().unwrap(),
        [("a1".to_owned(), JournalRecord::Running(None))]
    );
    assert_eq!(names(&journal), ["a1.json", "notes.txt"]);
}

#[test]
fn files_recovery_cant_remove_dont_stop_it() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, journal) = journal();
    journal.write("a1", &JournalRecord::Running(None)).unwrap();
    let dir = journal.dir();
    fs::write(dir.join(".tmp-a2"), "{").unwrap();
    fs::write(dir.join("a3.json"), "not json").unwrap();
    fs::set_permissions(dir, fs::Permissions::from_mode(0o555)).unwrap();
    let recovered = journal.recover();
    fs::set_permissions(dir, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        recovered.unwrap(),
        [("a1".to_owned(), JournalRecord::Running(None))]
    );
}

#[test]
fn an_empty_journal_is_created_on_recovery() {
    let (_dir, journal) = journal();
    assert_eq!(journal.recover().unwrap(), []);
    assert!(journal.dir().is_dir());
}

#[test]
fn attempt_ids_must_be_safe_file_names() {
    let (_dir, journal) = journal();
    for id in ["", "../escape", "a/b", ".tmp-x"] {
        let error = journal
            .write(id, &JournalRecord::Running(None))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{id}");
        assert_eq!(
            journal.remove(id).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    journal
        .write(
            "0190a0a0-0000-7000-8000-000000000001",
            &JournalRecord::Running(None),
        )
        .unwrap();
}

#[test]
fn one_process_holds_the_lock() {
    let (_dir, journal) = journal();
    let held = journal.lock().unwrap();
    // flock locks belong to the open file: a second open is refused, as another process is.
    assert_eq!(
        journal.lock().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        journal.recover().unwrap(),
        [],
        "the lock file isn't a record"
    );
    drop(held);
    journal.lock().unwrap();
}

#[test]
fn an_unwritable_journal_is_an_error() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("journal");
    fs::write(&file, "a file, not a directory").unwrap();
    let journal = Journal::new(file);
    assert!(journal.write("a1", &JournalRecord::Running(None)).is_err());
    assert!(journal.recover().is_err());
    assert!(journal.records().is_err());
    assert!(journal.lock().is_err());
}

#[test]
fn a_process_identity_is_its_pid_and_start_time() {
    let pid = i32::try_from(std::process::id()).unwrap();
    let identity = process_identity(pid).unwrap();
    assert_eq!(identity.pgid, pid);
    assert!(identity.start_time > 0);
    assert_eq!(process_identity(pid).unwrap(), identity);
    assert_eq!(
        process_identity(i32::MAX).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}

/// Starts `script` in a process group of its own; a thread reaps it, as `init` would an
/// orphan.
fn group(script: &str) -> (ProcessIdentity, std::thread::JoinHandle<()>) {
    let mut child = std::process::Command::new("sh")
        .args(["-c", script])
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = i32::try_from(child.id()).unwrap();
    let identity = process_identity(pid).unwrap();
    let reaper = std::thread::spawn(move || {
        let _ = child.wait();
    });
    (identity, reaper)
}

#[tokio::test]
async fn an_orphan_is_stopped_with_sigterm_and_killed_if_it_ignores_it() {
    let (identity, reaper) = group("exec sleep 30");
    assert_eq!(
        stop_orphan(identity, Duration::from_secs(10), OrphanWaits::default())
            .await
            .unwrap(),
        Orphan::Stopped
    );
    reaper.join().unwrap();

    let (identity, reaper) = group("trap '' TERM; while :; do sleep 0.05; done");
    tokio::time::sleep(Duration::from_millis(100)).await; // the trap is set
    assert_eq!(
        stop_orphan(identity, Duration::from_millis(200), OrphanWaits::default())
            .await
            .unwrap(),
        Orphan::Killed
    );
    reaper.join().unwrap();
}

#[tokio::test]
async fn a_slow_poll_doesnt_outlast_the_grace_and_the_kill_wait() {
    let (identity, reaper) = group("trap '' TERM; while :; do sleep 0.05; done");
    tokio::time::sleep(Duration::from_millis(100)).await; // the trap is set
    let waits = OrphanWaits {
        poll: Duration::from_secs(30),
        kill_wait: Duration::from_millis(200),
    };
    let started = std::time::Instant::now();
    let stopped = stop_orphan(identity, Duration::from_millis(100), waits).await;
    assert_eq!(stopped.unwrap(), Orphan::Killed);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    reaper.join().unwrap();
}

#[tokio::test]
async fn a_gone_or_recycled_process_is_left_alone() {
    let pid = i32::try_from(std::process::id()).unwrap();
    let mut identity = process_identity(pid).unwrap();
    identity.start_time += 1;
    assert_eq!(
        stop_orphan(identity, Duration::ZERO, OrphanWaits::default())
            .await
            .unwrap(),
        Orphan::Gone
    );
    let gone = ProcessIdentity {
        pgid: i32::MAX,
        start_time: 1,
    };
    assert_eq!(
        stop_orphan(gone, Duration::ZERO, OrphanWaits::default())
            .await
            .unwrap(),
        Orphan::Gone
    );
}

#[test]
fn the_group_of_init_or_of_everything_is_never_an_orphan() {
    // B88: signalling group 1 is `kill(-1)`, every process of the user; a journal naming it
    // (corrupted, or init's own identity) must not reach a signal.
    assert_eq!(orphan_group(1), None);
    assert_eq!(orphan_group(0), None);
    assert_eq!(orphan_group(-7), None);
    assert_eq!(orphan_group(4242), Pid::from_raw(4242));
}

fn handle(id: &str) -> CleanupHandle {
    CleanupHandle {
        handle_type: "Job".to_owned(),
        value: serde_json::json!({"id": id}),
    }
}

fn kept() -> KeptAttempt {
    KeptAttempt {
        assign: pb::AssignTask {
            attempt_id: "a1".to_owned(),
            inputs_json: b"{}".to_vec(),
            ..pb::AssignTask::default()
        },
        code_dir: Some(PathBuf::from("/code/h1")),
        code_hash: Some("h1".to_owned()),
        root_copies: [("libs".to_owned(), PathBuf::from("/code/libs/h2"))].into(),
    }
}

#[test]
fn a_cleaning_record_keeps_the_tasks_result_and_the_process() {
    let (_dir, journal) = journal();
    let process = ProcessIdentity {
        pgid: 7,
        start_time: 9,
    };
    let record = JournalRecord::Cleaning(finished("a1"), Some(process));
    journal.write("a1", &record).unwrap();
    assert_eq!(journal.record("a1").unwrap(), Some(record));
}

#[test]
fn what_a_cleanup_needs_is_kept_and_goes_with_the_attempt() {
    let (_dir, journal) = journal();
    journal.write("a1", &JournalRecord::Running(None)).unwrap();
    journal.keep("a1", &kept()).unwrap();
    journal.add_handle("a1", &handle("j-1")).unwrap();
    journal.add_handle("a1", &handle("j-2")).unwrap();
    assert_eq!(journal.kept("a1").unwrap(), Some(kept()));
    assert_eq!(
        journal.handles("a1").unwrap(),
        [handle("j-1"), handle("j-2")]
    );
    assert_eq!(journal.recover().unwrap().len(), 1, "they aren't records");
    journal.remove("a1").unwrap();
    assert_eq!(names(&journal), Vec::<String>::new());
    assert_eq!(journal.kept("a1").unwrap(), None);
}

#[test]
fn a_handle_cut_off_by_a_crash_is_left_out() {
    // Never acknowledged: the task didn't go on believing it registered.
    let (_dir, journal) = journal();
    journal.add_handle("a1", &handle("j-1")).unwrap();
    let path = journal.dir().join("a1.handles");
    let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(br#"{"handle_type":"Jo"#).unwrap();
    assert_eq!(journal.handles("a1").unwrap(), [handle("j-1")]);
}

#[test]
fn kept_files_with_no_record_are_removed_on_start() {
    let (_dir, journal) = journal();
    journal.keep("a1", &kept()).unwrap();
    journal.add_handle("a1", &handle("j-1")).unwrap();
    journal.write("a2", &JournalRecord::Running(None)).unwrap();
    journal.keep("a2", &kept()).unwrap();
    journal.recover().unwrap();
    assert_eq!(names(&journal), ["a2.attempt", "a2.json"]);
}

#[test]
fn a_handles_file_that_cant_be_read_is_an_error_not_an_empty_list() {
    // Read as none, a cleanup could report success and leave the resources behind.
    let (_dir, journal) = journal();
    fs::create_dir_all(journal.dir().join("a1.handles")).unwrap();
    assert!(journal.handles("a1").is_err());
    fs::create_dir_all(journal.dir().join("a1.attempt")).unwrap();
    assert!(journal.kept("a1").is_err());
}

#[test]
fn a_damaged_complete_handle_line_is_an_error() {
    let (_dir, journal) = journal();
    journal.add_handle("a1", &handle("j-1")).unwrap();
    let path = journal.dir().join("a1.handles");
    let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(b"not json\n").unwrap();
    assert_eq!(
        journal.handles("a1").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn the_journals_files_are_its_owners_alone() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, journal) = journal();
    journal.write("a1", &JournalRecord::Running(None)).unwrap();
    journal.keep("a1", &kept()).unwrap();
    journal.add_handle("a1", &handle("j-1")).unwrap();
    for name in names(&journal) {
        let mode = fs::metadata(journal.dir().join(&name))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "{name}");
    }
}
