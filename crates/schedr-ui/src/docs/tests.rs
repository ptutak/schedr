use pretty_assertions::assert_eq;

use super::*;

fn site() -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(dir.path().join("index.html"), "<h1>docs</h1>").unwrap();
    std::fs::create_dir(dir.path().join("03-guide")).unwrap();
    std::fs::write(dir.path().join("03-guide/index.html"), "guide").unwrap();
    std::fs::write(dir.path().join("03-guide/05-workers.html"), "workers").unwrap();
    dir
}

#[test]
fn files_and_directories_under_the_root_resolve() {
    let dir = site();
    let root = dir.path().canonicalize().unwrap();
    assert_eq!(
        resolve(dir.path(), "03-guide/05-workers.html"),
        Some(root.join("03-guide/05-workers.html"))
    );
    assert_eq!(
        resolve(dir.path(), "03-guide"),
        Some(root.join("03-guide/index.html"))
    );
    assert_eq!(resolve(dir.path(), "missing.html"), None);
}

#[test]
fn nothing_outside_the_root_is_served() {
    let dir = site();
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("link.html")).unwrap();
    for path in [
        "../etc/passwd",
        "03-guide/../../x",
        "/etc/passwd",
        "link.html",
    ] {
        assert_eq!(resolve(dir.path(), path), None, "{path}");
    }
}

#[test]
fn files_get_their_content_types() {
    for (name, kind) in [
        ("a.html", "text/html; charset=utf-8"),
        ("a.css", "text/css; charset=utf-8"),
        ("a.js", "text/javascript; charset=utf-8"),
        ("a.svg", "image/svg+xml"),
        ("a.woff2", "font/woff2"),
        ("objects.inv", "application/octet-stream"),
    ] {
        assert_eq!(content_type(Path::new(name)), kind, "{name}");
    }
}
