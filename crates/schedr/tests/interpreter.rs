//! Which Python the local commands run (PROJECT.md, Configuration: built-in interpreter): the
//! real binary finding a project venv or resolving a pyenv shim, and saying what went wrong
//! when the interpreter can't run the runner.
//!
//! Needs `SCHEDR_TEST_PYTHON` (a Python with `schedr` installed); `make test-integration`
//! sets it.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_schedr");

const DAG: &str = r#"
from schedr import Dag, task

@task
def extract() -> int:
    return 3

with Dag("sync"):
    extract()
"#;

/// Writes an executable shell script.
fn script(path: &Path, body: &str) -> PathBuf {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_path_buf()
}

/// A `python3` that runs the test Python (which has `schedr`), as a venv's would.
fn python_wrapper(path: &Path) -> PathBuf {
    let python = std::env::var("SCHEDR_TEST_PYTHON").expect(
        "SCHEDR_TEST_PYTHON: the Python with schedr installed (`make test-integration` sets it)",
    );
    script(path, &format!("exec \"{python}\" \"$@\""))
}

/// A temporary directory with a DAG root `etl` holding one DAG file.
fn project() -> TempDir {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("etl")).unwrap();
    std::fs::write(dir.path().join("etl/sync.py"), DAG).unwrap();
    dir
}

/// Runs `schedr <args>` with only the given environment: exit code and stderr.
fn schedr(cwd: &Path, env: &[(&str, String)], args: &[&str]) -> (i32, String) {
    let output = Command::new(BINARY)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn a_project_venv_in_a_parent_directory_is_used() {
    let dir = project();
    python_wrapper(&dir.path().join(".venv/bin/python3"));
    let deeper = dir.path().join("etl");
    let env = [
        ("PATH", "/usr/bin:/bin".to_owned()),
        ("HOME", dir.path().display().to_string()),
    ];
    let (code, err) = schedr(&deeper, &env, &["dags", "check", "sync.py"]);
    assert_eq!(code, 0, "{err}");
    assert!(err.contains("project venv"), "{err}");
}

#[test]
fn a_pyenv_shim_is_resolved_once_with_pyenv() {
    let dir = project();
    let python = python_wrapper(&dir.path().join("versions/3.14/bin/python3"));
    let root = dir.path().join("pyenv");
    script(
        &root.join("shims/python3"),
        "echo 'the shim must not be run' >&2; exit 1",
    );
    script(
        &root.join("bin/pyenv"),
        &format!(
            "[ \"$1 $2\" = 'which python3' ] && echo '{}'",
            python.display()
        ),
    );
    let env = [
        (
            "PATH",
            format!("{}:/usr/bin:/bin", root.join("shims").display()),
        ),
        ("PYENV_ROOT", root.display().to_string()),
        ("HOME", dir.path().display().to_string()),
    ];
    let (code, err) = schedr(dir.path(), &env, &["dags", "check", "etl/sync.py"]);
    assert_eq!(code, 0, "{err}");
    assert!(err.contains("resolved with `pyenv which python3`"), "{err}");
}

#[test]
fn an_interpreter_that_cant_run_the_runner_is_named_with_its_output() {
    let dir = project();
    std::fs::write(
        dir.path().join("config.toml"),
        "[interpreters]\npython = \"/bin/sh\"\n",
    )
    .unwrap();
    let env = [("PATH", "/usr/bin:/bin".to_owned())];
    let (code, err) = schedr(
        dir.path(),
        &env,
        &["--config", "config.toml", "dags", "check", "etl/sync.py"],
    );
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("interpreter `python`: /bin/sh (configured in [interpreters])"),
        "{err}"
    );
    assert!(err.contains("its output:"), "{err}");
    assert!(err.contains("it needs Python 3.14+ with schedr"), "{err}");
}

#[test]
fn another_dag_interpreter_is_named_as_configured() {
    let dir = project();
    std::fs::write(
        dir.path().join("config.toml"),
        "[dags]\ninterpreter = \"py314\"\n\n[interpreters]\npy314 = \"/bin/sh\"\n",
    )
    .unwrap();
    python_wrapper(&dir.path().join(".venv/bin/python3"));
    let env = [("PATH", "/usr/bin:/bin".to_owned())];
    let (code, err) = schedr(
        dir.path(),
        &env,
        &["--config", "config.toml", "dags", "check", "etl/sync.py"],
    );
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("interpreter `py314`: /bin/sh (configured in [interpreters])"),
        "{err}"
    );
    assert!(
        err.contains("choose another with [interpreters] py314, or [dags] interpreter"),
        "{err}"
    );
    assert!(
        !err.contains("project venv"),
        "no lookup for another interpreter: {err}"
    );
}
