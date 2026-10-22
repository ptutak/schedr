use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use pretty_assertions::assert_eq;
use schedr_config::config::BUILTIN_PYTHON_ORIGIN;
use tempfile::TempDir;

use super::*;

/// Creates an empty file at `path` (and its directories); discovery only checks it exists.
fn touch(path: &Path) -> PathBuf {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, "").unwrap();
    path.to_path_buf()
}

fn env(pairs: &[(&str, &Path)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.display().to_string()))
        .collect()
}

fn no_pyenv(_: &Path, _: &Host<'_>) -> Result<PathBuf, String> {
    panic!("pyenv must not be asked")
}

fn find(host: &Host<'_>, project: bool) -> Found {
    discover(host, project, no_pyenv).unwrap()
}

#[test]
fn an_active_virtualenv_comes_first_for_local_commands() {
    let tmp = TempDir::new().unwrap();
    let active = touch(&tmp.path().join("active/bin/python3"));
    touch(&tmp.path().join("proj/.venv/bin/python3"));
    let vars = env(&[("VIRTUAL_ENV", &tmp.path().join("active"))]);
    let host = Host {
        env: &vars,
        cwd: Some(&tmp.path().join("proj")),
        exe: None,
    };
    assert_eq!(
        find(&host, true),
        Found {
            path: active,
            how: How::ActiveVenv
        }
    );
    assert_eq!(find(&host, false).how, How::NotFound);
}

#[test]
fn a_virtualenv_without_python3_is_skipped() {
    let tmp = TempDir::new().unwrap();
    let vars = env(&[("VIRTUAL_ENV", &tmp.path().join("gone"))]);
    let host = Host {
        env: &vars,
        cwd: None,
        exe: None,
    };
    assert_eq!(find(&host, true).how, How::NotFound);
}

#[test]
fn a_project_venv_is_the_nearest_dot_venv_then_venv() {
    let tmp = TempDir::new().unwrap();
    let outer = touch(&tmp.path().join("venv/bin/python3"));
    let inner = touch(&tmp.path().join("proj/venv/bin/python3"));
    let dot = touch(&tmp.path().join("proj/.venv/bin/python3"));
    let empty = BTreeMap::new();
    let at = |cwd: &Path| {
        find(
            &Host {
                env: &empty,
                cwd: Some(cwd),
                exe: None,
            },
            true,
        )
    };
    let deep = tmp.path().join("proj/dags/team_a");
    fs::create_dir_all(&deep).unwrap();
    assert_eq!(
        at(&deep),
        Found {
            path: dot.clone(),
            how: How::ProjectVenv
        }
    );
    fs::remove_file(&dot).unwrap();
    assert_eq!(at(&deep).path, inner);
    assert_eq!(at(tmp.path()).path, outer);
}

#[test]
fn next_to_the_binary_follows_a_symlinked_binary() {
    let tmp = TempDir::new().unwrap();
    let python = touch(&tmp.path().join("venv/bin/python3"));
    let exe = touch(&tmp.path().join("venv/bin/schedr"));
    let link = tmp.path().join("schedr");
    std::os::unix::fs::symlink(&exe, &link).unwrap();
    let empty = BTreeMap::new();
    let host = Host {
        env: &empty,
        cwd: None,
        exe: Some(&link),
    };
    let found = find(&host, false);
    assert_eq!(found.how, How::NextToBinary);
    assert_eq!(found.path, fs::canonicalize(&python).unwrap());
}

#[test]
fn python3_on_path_is_used_as_found() {
    let tmp = TempDir::new().unwrap();
    let python = touch(&tmp.path().join("b/python3"));
    let vars = BTreeMap::from([
        (
            "PATH".to_owned(),
            format!(
                "{}::{}",
                tmp.path().join("a").display(),
                tmp.path().join("b").display()
            ),
        ),
        ("HOME".to_owned(), tmp.path().display().to_string()),
    ]);
    let host = Host {
        env: &vars,
        cwd: None,
        exe: Some(&tmp.path().join("bin/schedr")),
    };
    assert_eq!(
        find(&host, true),
        Found {
            path: python,
            how: How::Path
        }
    );
}

/// A pyenv layout: `<root>/shims/python3` on `PATH`, `<root>/bin/pyenv`.
fn pyenv_layout(tmp: &TempDir) -> (PathBuf, BTreeMap<String, String>) {
    let root = tmp.path().join("pyenv");
    let shim = touch(&root.join("shims/python3"));
    touch(&root.join("bin/pyenv"));
    let vars = BTreeMap::from([
        ("PATH".to_owned(), root.join("shims").display().to_string()),
        ("PYENV_ROOT".to_owned(), root.display().to_string()),
    ]);
    (shim, vars)
}

#[test]
fn a_pyenv_shim_is_resolved_with_pyenv() {
    let tmp = TempDir::new().unwrap();
    let (_, vars) = pyenv_layout(&tmp);
    let cwd = tmp.path().join("proj");
    let host = Host {
        env: &vars,
        cwd: Some(&cwd),
        exe: None,
    };
    let found = discover(&host, true, |pyenv, host| {
        assert_eq!(pyenv, tmp.path().join("pyenv/bin/pyenv"));
        assert_eq!(host.cwd, Some(cwd.as_path()));
        Ok(PathBuf::from("/pyenv/versions/3.14.8/bin/python3"))
    })
    .unwrap();
    assert_eq!(
        found,
        Found {
            path: PathBuf::from("/pyenv/versions/3.14.8/bin/python3"),
            how: How::Pyenv
        }
    );
}

#[test]
fn the_pyenv_root_defaults_to_home_and_pyenv_on_path_wins() {
    let tmp = TempDir::new().unwrap();
    let shims = tmp.path().join(".pyenv/shims");
    touch(&shims.join("python3"));
    let pyenv = touch(&tmp.path().join("tools/pyenv"));
    let vars = BTreeMap::from([
        (
            "PATH".to_owned(),
            format!("{}:{}", shims.display(), tmp.path().join("tools").display()),
        ),
        ("HOME".to_owned(), tmp.path().display().to_string()),
    ]);
    let host = Host {
        env: &vars,
        cwd: None,
        exe: None,
    };
    let found = discover(&host, true, |bin, _| {
        assert_eq!(bin, pyenv);
        Ok(PathBuf::from("/x/python3"))
    })
    .unwrap();
    assert_eq!(found.how, How::Pyenv);
}

#[test]
fn a_shim_pyenv_cant_resolve_is_an_error_saying_what_to_do() {
    let tmp = TempDir::new().unwrap();
    let (shim, vars) = pyenv_layout(&tmp);
    let host = Host {
        env: &vars,
        cwd: None,
        exe: None,
    };
    let error = discover(&host, true, |_, _| {
        Err("pyenv: version `3.99` is not installed".to_owned())
    })
    .unwrap_err();
    assert_eq!(
        error,
        PyenvError {
            shim: shim.clone(),
            message: "pyenv: version `3.99` is not installed".to_owned()
        }
    );
    assert_eq!(
        error.to_string(),
        format!(
            "`python3` on PATH is a pyenv shim ({}), but `pyenv which python3` failed: pyenv: \
             version `3.99` is not installed; choose a Python with `pyenv local`, or set \
             [interpreters] python (SCHEDR_CONFIG__INTERPRETERS__PYTHON)",
            shim.display()
        )
    );
}

#[test]
fn nothing_found_leaves_python3_to_the_system() {
    let empty = BTreeMap::new();
    let host = Host {
        env: &empty,
        cwd: None,
        exe: None,
    };
    assert_eq!(
        find(&host, true),
        Found {
            path: PathBuf::from("python3"),
            how: How::NotFound
        }
    );
}

#[test]
fn every_way_has_a_description() {
    let all = [
        How::ActiveVenv,
        How::ProjectVenv,
        How::NextToBinary,
        How::Path,
        How::Pyenv,
        How::NotFound,
    ];
    let texts: Vec<String> = all.iter().map(ToString::to_string).collect();
    assert_eq!(
        texts,
        [
            "active virtualenv, $VIRTUAL_ENV",
            "project venv",
            "next to the schedr binary",
            "python3 on PATH",
            "pyenv shim on PATH, resolved with `pyenv which python3`",
            "no python3 found: left to the system",
        ]
    );
}

#[test]
fn pyenv_which_reports_a_pyenv_that_cant_start() {
    let empty = BTreeMap::new();
    let host = Host {
        env: &empty,
        cwd: None,
        exe: None,
    };
    let error = pyenv_which(Path::new("/nonexistent/pyenv"), &host).unwrap_err();
    assert!(
        error.starts_with("can't run /nonexistent/pyenv: "),
        "{error}"
    );
}

#[test]
fn apply_replaces_only_the_builtin_python() {
    let tmp = TempDir::new().unwrap();
    let python = touch(&tmp.path().join(".venv/bin/python3"));
    let empty = BTreeMap::new();
    let host = Host {
        env: &empty,
        cwd: Some(tmp.path()),
        exe: None,
    };
    let mut builtin = Config {
        builtin_python: Some(BUILTIN_PYTHON_ORIGIN.to_owned()),
        ..Config::default()
    };
    apply(&mut builtin, &host, true).unwrap();
    assert_eq!(builtin.interpreters["python"], Interpreter::path(&python));
    assert_eq!(builtin.builtin_python.as_deref(), Some("project venv"));
    let mut configured = Config::default();
    configured.interpreters.insert(
        "python".to_owned(),
        Interpreter::path("/usr/bin/python3.14"),
    );
    apply(&mut configured, &host, true).unwrap();
    assert_eq!(
        configured.interpreters["python"],
        Interpreter::path("/usr/bin/python3.14")
    );
    assert_eq!(configured.builtin_python, None);
}

#[test]
fn apply_doesnt_look_when_the_dag_interpreter_is_another() {
    let tmp = TempDir::new().unwrap();
    // A pyenv shim pyenv can't resolve would fail the lookup, if it ran.
    let (_, vars) = pyenv_layout(&tmp);
    let host = Host {
        env: &vars,
        cwd: None,
        exe: None,
    };
    let mut config = Config {
        builtin_python: Some(BUILTIN_PYTHON_ORIGIN.to_owned()),
        ..Config::default()
    };
    config.dags.interpreter = "py314".to_owned();
    config
        .interpreters
        .insert("python".to_owned(), Interpreter::path("python3"));
    apply(&mut config, &host, true).unwrap();
    assert_eq!(config.interpreters["python"], Interpreter::path("python3"));
    assert_eq!(
        config.builtin_python.as_deref(),
        Some(BUILTIN_PYTHON_ORIGIN)
    );
}
