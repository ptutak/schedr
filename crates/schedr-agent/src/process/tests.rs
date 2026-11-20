use super::*;

#[test]
fn a_stat_line_gives_its_state_and_group() {
    let line = |state: &str, pgrp: i32| format!("4242 (sh) {state} 1 {pgrp} {pgrp} 0 -1 4194560");
    assert_eq!(stat_fields(&line("S", 7)), Some(("S", 7)));
    assert_eq!(stat_fields(&line("Z", 8)), Some(("Z", 8)));
    // A command name may hold spaces and parentheses: the fields start after the last `)`.
    assert_eq!(stat_fields("4242 (a) Z (b) S 1 7 7 0"), Some(("S", 7)));
    assert_eq!(stat_fields("garbage"), None);
    assert_eq!(stat_fields("4242 (sh) S"), None);
}

#[test]
fn only_a_zombie_or_dead_state_has_ended() {
    for state in ["R", "S", "D", "T", "t", "I"] {
        assert!(!ended(state), "{state}");
    }
    for state in ["Z", "X", "x"] {
        assert!(ended(state), "{state}");
    }
}

#[tokio::test]
async fn a_log_file_is_never_written_through_a_link() {
    // B62: a link planted where an attempt's log goes doesn't make the agent append the
    // task's output to its target.
    let dir = tempfile::TempDir::new().unwrap();
    let target = dir.path().join("target");
    std::fs::write(&target, "untouched").unwrap();
    let log_file = dir.path().join("attempt.log");
    std::os::unix::fs::symlink(&target, &log_file).unwrap();
    let spec = ProcessSpec {
        program: "/bin/true".into(),
        args: Vec::new(),
        env: std::collections::BTreeMap::new(),
        working_dir: dir.path().to_path_buf(),
        log_file,
        max_output_size: 1024,
        mask: Mask::default(),
        start_timeout: crate::launch::RUNNER_START_TIMEOUT,
        output_drain: crate::launch::OUTPUT_DRAIN_TIMEOUT,
        barrier: None,
    };
    assert!(open_log(&spec).await.is_err());
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "untouched");
}

#[tokio::test]
async fn only_a_regular_file_is_opened_never_a_link_or_a_directory() {
    let dir = tempfile::TempDir::new().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, "x").unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&file, &link).unwrap();
    let read = || {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true);
        options
    };
    assert!(open_regular(read(), &file).await.is_ok());
    let error = open_regular(read(), &link).await.unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(rustix::io::Errno::LOOP.raw_os_error())
    );
    let error = open_regular(read(), dir.path()).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().ends_with("isn't a regular file"),
        "{error}"
    );
    // A hard link to another file is that file under a second name.
    let hard = dir.path().join("hard");
    std::fs::hard_link(&file, &hard).unwrap();
    let error = open_regular(read(), &hard).await.unwrap_err();
    assert!(
        error.to_string().ends_with("has another hard link"),
        "{error}"
    );
    assert!(
        open_regular(read(), &file).await.is_err(),
        "both names are refused"
    );
}

/// The permission bits of a log `open_log` creates at `name` in `dir`.
async fn created_mode(dir: &std::path::Path, name: &str) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    let spec = ProcessSpec {
        program: "/bin/true".into(),
        args: Vec::new(),
        env: std::collections::BTreeMap::new(),
        working_dir: dir.to_path_buf(),
        log_file: dir.join(name),
        max_output_size: 1024,
        mask: Mask::default(),
        start_timeout: crate::launch::RUNNER_START_TIMEOUT,
        output_drain: crate::launch::OUTPUT_DRAIN_TIMEOUT,
        barrier: None,
    };
    drop(open_log(&spec).await.unwrap());
    std::fs::metadata(dir.join(name))
        .unwrap()
        .permissions()
        .mode()
        & 0o777
}

#[tokio::test]
async fn a_new_log_is_its_owners_only_unless_the_worker_opens_it_up() {
    // B182: under umask 0022 a log was 0644, readable by every uid on the node.
    let dir = tempfile::TempDir::new().unwrap();
    assert_eq!(created_mode(dir.path(), "a.log").await, 0o600);
    // Each test is a process of its own (nextest): the mode set here goes nowhere else.
    set_log_mode(0o640);
    assert_eq!(
        created_mode(dir.path(), "b.log").await,
        0o640,
        "whatever the umask"
    );
}

#[tokio::test]
async fn a_log_reopened_or_made_under_a_tight_umask_still_gets_the_workers_mode() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::TempDir::new().unwrap();
    // A log from before (an upgrade, a looser mode then) is tightened when it is opened again.
    let old = dir.path().join("parse-etl.log");
    std::fs::write(&old, "from before\n").unwrap();
    std::fs::set_permissions(&old, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(created_mode(dir.path(), "parse-etl.log").await, 0o600);
    // A umask taking the owner's write doesn't: the owner always reads and writes. Each test is
    // a process of its own (nextest), so the umask set here goes nowhere else.
    let _ = rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o277));
    assert_eq!(created_mode(dir.path(), "tight.log").await, 0o600);
    set_log_mode(0o640);
    assert_eq!(created_mode(dir.path(), "group.log").await, 0o640);
}

#[test]
fn at_its_start_the_worker_gives_its_logs_and_their_folder_the_log_mode() {
    use std::os::unix::fs::PermissionsExt as _;
    let mode_of = |path: &std::path::Path| {
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    };
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path().join("logs").join("w1");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    // Written before an upgrade: readable by every uid until retention deletes it.
    let old = dir.join("attempt.log");
    std::fs::write(&old, "task output\n").unwrap();
    std::fs::set_permissions(&old, std::fs::Permissions::from_mode(0o644)).unwrap();
    // A link is left alone, its target untouched.
    let outside = tmp.path().join("outside");
    std::fs::write(&outside, "").unwrap();
    std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::os::unix::fs::symlink(&outside, dir.join("link.log")).unwrap();
    secure_log_dir(&dir, 0o600).unwrap();
    assert_eq!((mode_of(&dir), mode_of(&old)), (0o700, 0o600));
    assert_eq!(mode_of(&outside), 0o644);
    secure_log_dir(&dir, 0o640).unwrap();
    assert_eq!((mode_of(&dir), mode_of(&old)), (0o750, 0o640));
    let fresh = tmp.path().join("new");
    secure_log_dir(&fresh, 0o644).unwrap();
    assert_eq!(mode_of(&fresh), 0o755);
}

#[tokio::test]
async fn a_process_dropped_unreaped_takes_its_whole_group_with_it() {
    // Codex's review of M5.2d: a parse dropped mid-way (its code superseded) left the runner's
    // children running; `kill_on_drop` kills the leader alone.
    let dir = tempfile::TempDir::new().unwrap();
    let pid_file = dir.path().join("child.pid");
    let spec = ProcessSpec {
        program: "/bin/sh".into(),
        args: vec![
            "-c".to_owned(),
            format!("sleep 30 & echo $! > {}; wait", pid_file.display()),
        ],
        env: std::collections::BTreeMap::new(),
        working_dir: dir.path().to_path_buf(),
        log_file: dir.path().join("p.log"),
        max_output_size: 1024,
        mask: Mask::default(),
        start_timeout: crate::launch::RUNNER_START_TIMEOUT,
        output_drain: crate::launch::OUTPUT_DRAIN_TIMEOUT,
        barrier: None,
    };
    let process = spawn(&spec).await.unwrap();
    let child = loop {
        if let Ok(text) = std::fs::read_to_string(&pid_file)
            && let Ok(pid) = text.trim().parse::<i32>()
        {
            break pid;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let proc_dir = std::path::PathBuf::from(format!("/proc/{child}"));
    assert!(proc_dir.exists(), "the child runs");
    drop(process);
    // Killed, then reaped by its parent's end (it was the shell's): gone, or a zombie.
    let mut gone = false;
    for _ in 0..200 {
        let state = std::fs::read_to_string(proc_dir.join("stat")).ok();
        if state.is_none_or(|s| stat_fields(&s).is_none_or(|(state, _)| ended(state))) {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if !gone {
        let _ = rustix::process::kill_process(
            rustix::process::Pid::from_raw(child).unwrap(),
            rustix::process::Signal::KILL,
        );
    }
    assert!(gone, "the child outlived its dropped leader");
}

#[test]
fn plain_storage_renders_structured_lines_split_anywhere_and_passes_the_rest() {
    let line =
        r#"{"schedr_log":1,"level":"info","message":"pod scheduled","fields":{"node":"n1"}}"#;
    let text = format!("plain\n{line}\nlast");
    let mut plainer = Plainer::default();
    let mut out = Vec::new();
    // Split into small reads, a structured line cut through its middle.
    for chunk in text.as_bytes().chunks(7) {
        out.extend(plainer.push(chunk));
    }
    out.extend(plainer.finish());
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "plain\nINFO     pod scheduled  node=n1\nlast"
    );
}

#[test]
fn a_line_too_long_to_hold_is_written_as_it_comes() {
    let mut plainer = Plainer::default();
    let long = vec![b'x'; STRUCTURED_LINE_MAX + 10];
    let first = plainer.push(&long);
    assert_eq!(first.len(), long.len(), "passed on, not held");
    assert_eq!(plainer.push(b"yy\nnext\n"), b"yy\nnext\n");
}
