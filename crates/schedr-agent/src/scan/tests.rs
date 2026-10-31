use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::*;

fn root_with(files: &[(&str, &str)]) -> TempDir {
    let dir = TempDir::new().unwrap();
    for (path, text) in files {
        write(dir.path(), path, text);
    }
    dir
}

fn write(root: &Path, path: &str, text: &str) {
    let full = root.join(path);
    fs::create_dir_all(full.parent().unwrap()).unwrap();
    fs::write(full, text).unwrap();
}

fn paths(snapshot: &Snapshot) -> Vec<&str> {
    snapshot.files().keys().map(String::as_str).collect()
}

#[test]
fn scans_recursively_with_slash_paths_in_order() {
    let dir = root_with(&[
        ("team_a/sync.py", "x"),
        ("team_a/sql/load.sql", "select 1"),
        ("top.py", "y"),
    ]);
    let snapshot = scan(dir.path()).unwrap();
    assert_eq!(
        paths(&snapshot),
        ["team_a/sql/load.sql", "team_a/sync.py", "top.py"]
    );
    assert_eq!(snapshot.python_files(), ["team_a/sync.py", "top.py"]);
    assert_eq!(snapshot.files()["top.py"].len, 1);
    assert!(snapshot.files()["top.py"].modified.is_some());
    assert_eq!(snapshot.root(), dir.path().canonicalize().unwrap());
}

#[test]
fn schedrignore_files_apply_in_any_directory_with_gitignore_syntax() {
    let dir = root_with(&[
        (".schedrignore", "scratch/\n*.tmp\n"),
        ("scratch/a.py", "x"),
        ("b.tmp", "x"),
        ("team/.schedrignore", "old_*.py\n!old_keep.py\n"),
        ("team/old_a.py", "x"),
        ("team/old_keep.py", "x"),
        ("team/new.py", "x"),
    ]);
    assert_eq!(
        paths(&scan(dir.path()).unwrap()),
        [
            ".schedrignore",
            "team/.schedrignore",
            "team/new.py",
            "team/old_keep.py"
        ]
    );
}

#[test]
fn airflowignore_and_gitignore_files_are_not_honoured() {
    let dir = root_with(&[
        (".airflowignore", "a.py\n"),
        (".gitignore", "b.py\n"),
        ("a.py", ""),
        ("b.py", ""),
    ]);
    assert_eq!(
        paths(&scan(dir.path()).unwrap()),
        [".airflowignore", ".gitignore", "a.py", "b.py"]
    );
}

#[test]
fn git_and_bytecode_are_skipped() {
    let dir = root_with(&[
        (".git/HEAD", "ref"),
        ("__pycache__/a.cpython-314.pyc", "x"),
        ("pkg/__pycache__/b.cpython-314.pyc", "x"),
        ("stray.pyc", "x"),
        ("a.py", "x"),
    ]);
    assert_eq!(paths(&scan(dir.path()).unwrap()), ["a.py"]);
}

#[test]
fn a_linked_root_resolves_to_its_target() {
    let dir = TempDir::new().unwrap();
    write(dir.path(), "rev1/a.py", "x");
    write(dir.path(), "rev2/b.py", "y");
    let link = dir.path().join("current");
    symlink(dir.path().join("rev1"), &link).unwrap();
    let first = scan(&link).unwrap();
    assert_eq!(paths(&first), ["a.py"]);
    fs::remove_file(&link).unwrap();
    symlink(dir.path().join("rev2"), &link).unwrap();
    let second = scan(&link).unwrap();
    assert_eq!(paths(&second), ["b.py"]);
    assert_eq!(
        second.root(),
        dir.path().join("rev2").canonicalize().unwrap()
    );
}

#[test]
fn linked_directories_inside_a_root_are_followed() {
    let dir = TempDir::new().unwrap();
    write(dir.path(), "shared/lib.py", "x");
    write(dir.path(), "root/a.py", "x");
    symlink(dir.path().join("shared"), dir.path().join("root/shared")).unwrap();
    assert_eq!(
        paths(&scan(&dir.path().join("root")).unwrap()),
        ["a.py", "shared/lib.py"]
    );
}

#[test]
fn a_missing_root_is_an_error() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("nope");
    let error = scan(&missing).unwrap_err();
    assert!(matches!(error, ScanError::Root { .. }));
    assert!(
        error
            .to_string()
            .starts_with(&format!("can't read DAG root {}: ", missing.display()))
    );
}

#[test]
fn a_dangling_link_is_a_walk_error() {
    let dir = root_with(&[("a.py", "x")]);
    symlink(dir.path().join("gone"), dir.path().join("b.py")).unwrap();
    let error = scan(dir.path()).unwrap_err();
    assert!(matches!(error, ScanError::Walk { .. }));
    assert!(error.to_string().starts_with("scanning DAG root "));
}

#[test]
fn a_path_that_isnt_utf8_is_an_error() {
    let dir = root_with(&[("a.py", "x")]);
    let name = std::ffi::OsStr::from_bytes(b"bad\xff.py");
    fs::write(dir.path().join(name), "x").unwrap();
    let error = scan(dir.path()).unwrap_err();
    assert!(matches!(error, ScanError::NotUtf8 { .. }));
    assert!(error.to_string().ends_with(": the path isn't valid UTF-8"));
}

#[test]
fn the_hash_depends_on_paths_and_contents_only() {
    let one = root_with(&[("a.py", "x"), ("sql/q.sql", "select 1")]);
    let two = root_with(&[("sql/q.sql", "select 1"), ("a.py", "x")]);
    let hash = content_hash(&scan(one.path()).unwrap()).unwrap();
    assert_eq!(hash, content_hash(&scan(two.path()).unwrap()).unwrap());
    assert!(hash.starts_with(HASH_PREFIX));
    assert_eq!(hash.len(), HASH_PREFIX.len() + 64);

    write(two.path(), "sql/q.sql", "select 2");
    assert_ne!(hash, content_hash(&scan(two.path()).unwrap()).unwrap());
    let renamed = root_with(&[("b.py", "x"), ("sql/q.sql", "select 1")]);
    assert_ne!(hash, content_hash(&scan(renamed.path()).unwrap()).unwrap());
}

#[test]
fn moving_bytes_between_files_changes_the_hash() {
    let one = root_with(&[("a", "xy"), ("b", "")]);
    let two = root_with(&[("a", "x"), ("b", "y")]);
    assert_ne!(
        content_hash(&scan(one.path()).unwrap()).unwrap(),
        content_hash(&scan(two.path()).unwrap()).unwrap()
    );
}

#[test]
fn a_file_removed_after_the_scan_is_a_read_error() {
    let dir = root_with(&[("a.py", "x")]);
    let snapshot = scan(dir.path()).unwrap();
    fs::remove_file(dir.path().join("a.py")).unwrap();
    let error = content_hash(&snapshot).unwrap_err();
    assert!(matches!(error, ScanError::Read { .. }));
    assert!(error.to_string().starts_with("reading "));
}

#[test]
fn a_capture_is_a_read_only_copy_with_the_same_hash() {
    use std::os::unix::fs::PermissionsExt;
    let (root, store) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    std::fs::create_dir(root.path().join("team")).unwrap();
    std::fs::write(root.path().join("team/sync.py"), "x = 1\n").unwrap();
    let script = root.path().join("run.sh");
    std::fs::write(&script, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    // A linked file is copied as what it points at, as the scan follows it.
    let outside = TempDir::new().unwrap();
    std::fs::write(outside.path().join("shared.sql"), "select 1").unwrap();
    std::os::unix::fs::symlink(outside.path().join("shared.sql"), root.path().join("q.sql"))
        .unwrap();
    let snapshot = scan(root.path()).unwrap();
    let captured = capture(&snapshot, store.path()).unwrap();
    assert_eq!(captured.hash, content_hash(&snapshot).unwrap());
    assert_eq!(
        captured.dir,
        store.path().join(captured.hash.replace(':', "-"))
    );
    let copy = |path: &str| captured.dir.join(path);
    assert_eq!(
        std::fs::read_to_string(copy("team/sync.py")).unwrap(),
        "x = 1\n"
    );
    assert!(!copy("q.sql").symlink_metadata().unwrap().is_symlink());
    assert_eq!(std::fs::read_to_string(copy("q.sql")).unwrap(), "select 1");
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&copy("run.sh")), 0o555, "executable, not writable");
    assert_eq!(mode(&copy("team/sync.py")), 0o444);
    assert_eq!(mode(&copy("team")), 0o555, "no file added or removed");
    assert_eq!(mode(&captured.dir), 0o555);
    // Edited in place afterwards: the copy keeps the hashed bytes.
    std::fs::write(root.path().join("team/sync.py"), "x = 2\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(copy("team/sync.py")).unwrap(),
        "x = 1\n"
    );
    remove_capture(&captured.dir).unwrap();
}

#[test]
fn the_same_code_again_reuses_its_copy_and_leaves_nothing_behind() {
    let (root, store) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    std::fs::write(root.path().join("sync.py"), "x = 1\n").unwrap();
    let first = capture(&scan(root.path()).unwrap(), store.path()).unwrap();
    assert!(first.fresh);
    // Nothing written for it: not even with the store full or read-only.
    let mode = |mode| std::os::unix::fs::PermissionsExt::from_mode(mode);
    std::fs::set_permissions(store.path(), mode(0o555)).unwrap();
    let second = capture(&scan(root.path()).unwrap(), store.path());
    std::fs::set_permissions(store.path(), mode(0o755)).unwrap();
    let second = second.unwrap();
    assert_eq!((&second.hash, &second.dir), (&first.hash, &first.dir));
    assert!(!second.fresh);
    let names: Vec<_> = std::fs::read_dir(store.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names.len(), 1, "no partial copy left: {names:?}");
    remove_capture(&first.dir).unwrap();
    assert!(!first.dir.exists());
}

#[test]
fn a_capture_that_cant_read_a_file_leaves_no_partial_copy() {
    let (root, store) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    std::fs::write(root.path().join("sync.py"), "x").unwrap();
    let snapshot = scan(root.path()).unwrap();
    std::fs::remove_file(root.path().join("sync.py")).unwrap();
    assert!(matches!(
        capture(&snapshot, store.path()),
        Err(ScanError::Read { .. })
    ));
    assert_eq!(std::fs::read_dir(store.path()).unwrap().count(), 0);
}

#[test]
fn a_removed_copy_never_leaves_its_hashs_name_behind() {
    // Renamed away before it is deleted: a removal cut short leaves a temporary name, never
    // a partial copy a later capture of the same hash would use as it is.
    let (root, store) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    std::fs::create_dir(root.path().join("team")).unwrap();
    std::fs::write(root.path().join("team/helper.py"), "x").unwrap();
    let first = capture(&scan(root.path()).unwrap(), store.path()).unwrap();
    // A leftover of a removal cut short (a killed agent): a temporary name only.
    let leftover = store.path().join(".removing-1-0");
    std::fs::create_dir(&leftover).unwrap();
    remove_capture(&first.dir).unwrap();
    let names: Vec<_> = std::fs::read_dir(store.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, [leftover.file_name().unwrap()]);
    let again = capture(&scan(root.path()).unwrap(), store.path()).unwrap();
    assert!(again.fresh, "copied again, whole");
    assert!(again.dir.join("team/helper.py").is_file());
    remove_capture(&again.dir).unwrap();
    remove_capture(&leftover).unwrap();
    assert_eq!(std::fs::read_dir(store.path()).unwrap().count(), 0);
}

/// This process's peak resident memory (`VmHWM`), in bytes. Each test runs in a process of
/// its own (nextest).
fn peak_memory() -> u64 {
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .unwrap()
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .unwrap();
    kib * 1024
}

#[test]
fn a_big_file_is_hashed_and_copied_without_reading_it_whole() {
    // B63: a data file bigger than the worker can hold once failed the hash.
    const SIZE: u64 = 32 << 20;
    let dir = root_with(&[("a.py", "x = 1\n")]);
    fs::File::create(dir.path().join("data.bin"))
        .unwrap()
        .set_len(SIZE)
        .unwrap();
    let snapshot = scan(dir.path()).unwrap();
    let store = TempDir::new().unwrap();
    let before = peak_memory();
    let hash = content_hash(&snapshot).unwrap();
    let captured = capture(&snapshot, store.path()).unwrap();
    let grew = peak_memory() - before;
    assert!(grew < SIZE / 4, "peak memory grew by {grew} bytes");
    assert_eq!(captured.hash, hash);
    assert_eq!(
        fs::metadata(captured.dir.join("data.bin")).unwrap().len(),
        SIZE
    );
}
