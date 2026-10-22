//! Which Python the built-in `python` interpreter is, when `[interpreters]` doesn't define it.
//!
//! The local commands (`standalone`, `dags check`, `dags test`) look, first match winning: an
//! active virtualenv (`$VIRTUAL_ENV`), a project venv (`.venv`, then `venv`, in the current
//! directory and its parents), the `python3` next to the `schedr` binary, then `python3` on
//! `PATH`; a pyenv shim found there is resolved once with `pyenv which python3`, in the user's
//! environment and directory, since tasks get a clean environment and run elsewhere. A
//! configured `python` always wins. See PROJECT.md, Configuration: built-in interpreter.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

use schedr_config::Config;
use schedr_config::Interpreter;
use schedr_config::config::BUILTIN_PYTHON;

#[cfg(test)]
mod tests;

/// The interpreter name the built-in one is registered under.
const PYTHON: &str = "python";

/// The project venv directories looked for, in order.
pub const PROJECT_VENVS: [&str; 2] = [".venv", "venv"];

/// Where the process runs: what discovery looks at.
#[derive(Debug, Clone, Copy)]
pub struct Host<'a> {
    /// The user's environment (`VIRTUAL_ENV`, `PATH`, `PYENV_ROOT`, `HOME`, ...).
    pub env: &'a BTreeMap<String, String>,
    /// The current directory, if known.
    pub cwd: Option<&'a Path>,
    /// The running `schedr` executable, if known.
    pub exe: Option<&'a Path>,
}

/// How the interpreter was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum How {
    /// `$VIRTUAL_ENV/bin/python3`.
    ActiveVenv,
    /// `.venv/bin/python3` or `venv/bin/python3` in the current directory or a parent.
    ProjectVenv,
    /// `python3` in the `schedr` binary's directory.
    NextToBinary,
    /// `python3` on `PATH`.
    Path,
    /// A pyenv shim on `PATH`, resolved with `pyenv which python3`.
    Pyenv,
    /// Nothing found: plain `python3`, left to the system to find.
    NotFound,
}

impl fmt::Display for How {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ActiveVenv => "active virtualenv, $VIRTUAL_ENV",
            Self::ProjectVenv => "project venv",
            Self::NextToBinary => "next to the schedr binary",
            Self::Path => "python3 on PATH",
            Self::Pyenv => "pyenv shim on PATH, resolved with `pyenv which python3`",
            Self::NotFound => "no python3 found: left to the system",
        })
    }
}

/// The interpreter found, and how.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    /// The executable, used as written (a venv's `python3` must not be resolved).
    pub path: PathBuf,
    /// How it was found.
    pub how: How,
}

/// A pyenv shim that pyenv couldn't resolve.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "`python3` on PATH is a pyenv shim ({shim}), but `pyenv which python3` failed: {message}; \
     choose a Python with `pyenv local`, or set [interpreters] python \
     (SCHEDR_CONFIG__INTERPRETERS__PYTHON)"
)]
pub struct PyenvError {
    /// The shim.
    pub shim: PathBuf,
    /// What pyenv said.
    pub message: String,
}

/// Finds the Python for the built-in `python` interpreter.
///
/// `project` turns on the steps that depend on the user's shell and directory (an active
/// virtualenv, a project venv): the local commands use them, a worker won't (milestone M2).
/// `pyenv` resolves a shim (normally [`pyenv_which`]).
///
/// # Errors
///
/// [`PyenvError`] if `python3` on `PATH` is a pyenv shim that pyenv can't resolve.
pub fn discover(
    host: &Host<'_>,
    project: bool,
    pyenv: impl Fn(&Path, &Host<'_>) -> Result<PathBuf, String>,
) -> Result<Found, PyenvError> {
    let found = |path: PathBuf, how| Ok(Found { path, how });
    if project {
        if let Some(venv) = host.env.get("VIRTUAL_ENV").filter(|v| !v.is_empty()) {
            let python = Path::new(venv).join("bin").join(BUILTIN_PYTHON);
            if python.is_file() {
                return found(python, How::ActiveVenv);
            }
        }
        if let Some(cwd) = host.cwd {
            for dir in cwd.ancestors() {
                for venv in PROJECT_VENVS {
                    let python = dir.join(venv).join("bin").join(BUILTIN_PYTHON);
                    if python.is_file() {
                        return found(python, How::ProjectVenv);
                    }
                }
            }
        }
    }
    if let Some(exe) = host.exe {
        let exe = std::fs::canonicalize(exe).unwrap_or_else(|_| exe.to_path_buf());
        if let Some(python) = exe.parent().map(|dir| dir.join(BUILTIN_PYTHON))
            && python.is_file()
        {
            return found(python, How::NextToBinary);
        }
    }
    let Some(python) = on_path(host, BUILTIN_PYTHON) else {
        return found(PathBuf::from(BUILTIN_PYTHON), How::NotFound);
    };
    let Some(root) = pyenv_root(host) else {
        return found(python, How::Path);
    };
    let shims = root.join("shims");
    let is_shim = shims.is_dir()
        && python.parent().is_some_and(|dir| {
            dir == shims || std::fs::canonicalize(dir).ok() == std::fs::canonicalize(&shims).ok()
        });
    if !is_shim {
        return found(python, How::Path);
    }
    let pyenv_bin = on_path(host, "pyenv").unwrap_or_else(|| root.join("bin").join("pyenv"));
    match pyenv(&pyenv_bin, host) {
        Ok(path) => found(path, How::Pyenv),
        Err(message) => Err(PyenvError {
            shim: python,
            message,
        }),
    }
}

/// Runs `pyenv which python3` in the user's environment and directory.
///
/// # Errors
///
/// What went wrong, as text: pyenv couldn't be started, failed (its stderr), or printed no
/// path.
pub fn pyenv_which(pyenv: &Path, host: &Host<'_>) -> Result<PathBuf, String> {
    let mut command = Command::new(pyenv);
    command
        .args(["which", BUILTIN_PYTHON])
        .env_clear()
        .envs(host.env);
    if let Some(cwd) = host.cwd {
        command.current_dir(cwd);
    }
    let output = command
        .output()
        .map_err(|e| format!("can't run {}: {e}", pyenv.display()))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if path.is_empty() {
        return Err("it printed no path".to_owned());
    }
    Ok(PathBuf::from(path))
}

/// Sets the built-in `python` interpreter for a local command, if `[interpreters]` doesn't
/// define it and the DAG interpreter is `python`, and records how it was found
/// ([`Config::builtin_python`]). Another DAG interpreter needs no lookup.
///
/// # Errors
///
/// [`PyenvError`] as for [`discover`].
pub fn apply_local(config: &mut Config, host: &Host<'_>) -> Result<(), PyenvError> {
    if config.builtin_python.is_none() || config.dags.interpreter != PYTHON {
        return Ok(());
    }
    let found = discover(host, true, pyenv_which)?;
    tracing::info!(python = %found.path.display(), how = %found.how, "DAG interpreter");
    config
        .interpreters
        .insert(PYTHON.to_owned(), Interpreter::path(found.path));
    config.builtin_python = Some(found.how.to_string());
    Ok(())
}

fn on_path(host: &Host<'_>, name: &str) -> Option<PathBuf> {
    host.env
        .get("PATH")?
        .split(':')
        .filter(|dir| !dir.is_empty())
        .map(|dir| Path::new(dir).join(name))
        .find(|candidate| candidate.is_file())
}

fn pyenv_root(host: &Host<'_>) -> Option<PathBuf> {
    host.env
        .get("PYENV_ROOT")
        .filter(|r| !r.is_empty())
        .map(PathBuf::from)
        .or_else(|| host.env.get("HOME").map(|h| Path::new(h).join(".pyenv")))
}
