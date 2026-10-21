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
