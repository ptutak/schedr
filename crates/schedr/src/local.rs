//! The DAG author's local commands: `schedr dags check` and `schedr dags test`
//! (see archive/PROJECT.md, Testing DAGs).
//!
//! - **`dags check <path>`** imports the DAG files with the real runner (parse mode, the
//!   per-file timeout) and applies the master's checks, so whatever the master would refuse is
//!   reported here; exit 1 on any error. **`--against <url>`** then reads that server's active
//!   workers and fails when this schedr is newer (by minor version) than any of them. A pass
//!   prints one `ok:` line. A version the master cannot parse does not fail the check.
//! - **`dags test <dag> [--for]`** runs one real run in an in-process standalone with a
//!   temporary SQLite database: every task its own process, exactly as in production. It prints
//!   each task's state as it changes, then the logs (prefixed with the task) and the outputs;
//!   exit 0 if the run succeeded, 1 otherwise.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use schedr_agent::launch::ProcessParseRunner;
use schedr_agent::parse::{ParseLimits, parse_files_with};
use schedr_agent::process::ProcessSpec;
use schedr_agent::scan::scan;
use schedr_api::views::TriggerRequest;
use schedr_api::views::{AttemptList, LocationView, RunDetail, WorkerView};
use schedr_config::{Config, Root};
use schedr_db::Database;
use schedr_master::validate::{Checks, validate_files};
use schedr_model::runner::{ParseRequest, ParsedFile};
use schedr_model::{SchedrVersion, WorkerState};
use serde::Serialize;
use serde_json::{Value, json};

use crate::app::{AppError, connect_to};
use crate::cli::{ClientArgs, DagsCommand};
use crate::client::{Auth, Client, ClientError, HttpTransport};
use crate::standalone::{plan, run};

#[cfg(test)]
mod tests;

/// How long `dags test` waits for its run (a hanging task is stopped by its own timeout).
pub const TEST_RUN_TIMEOUT: Duration = Duration::from_hours(1);

/// The files of one root to check or test: a directory, or one file in its directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalRoot {
    /// The root's name: the directory's name.
    pub name: String,
    /// The directory.
    pub dir: PathBuf,
    /// The `.py` files to parse, relative to `dir`.
    pub files: Vec<String>,
    /// The `.py` files a parse skips: they don't mention `schedr` (decision 305).
    pub skipped: Vec<String>,
    /// The root's other `.py` files, when a file or folder of it was named.
    pub unselected: Vec<String>,
}

impl LocalRoot {
    /// The same files, read from a copy of the root.
    fn clone_at(&self, dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            ..self.clone()
        }
    }

    /// The root for `path` within a configured root, if it is in one: the file or folder is
    /// selected, the root's boundary and names kept, so package imports work as on a worker
    /// (decision 331).
    ///
    /// # Errors
    ///
    /// [`AppError::BadArgument`] for an unreadable path, one in several roots, or one a
    /// worker doesn't get (`.schedrignore`).
    pub fn at_in(path: &Path, config: &Config) -> Result<Self, AppError> {
        let bad = |e: std::io::Error| AppError::BadArgument(format!("{}: {e}", path.display()));
        let given = std::path::absolute(path).map_err(bad)?;
        let resolved = path.canonicalize().map_err(bad)?;
        let matches: Vec<_> = config
            .dag_roots
            .iter()
            .filter_map(|configured| {
                let base = configured.path.canonicalize().ok()?;
                let given_base = std::path::absolute(&configured.path).ok()?;
                let relative = given
                    .strip_prefix(given_base)
                    .ok()
                    .filter(|relative| {
                        !relative
                            .components()
                            .any(|c| matches!(c, std::path::Component::ParentDir))
                    })
                    .or_else(|| resolved.strip_prefix(&base).ok())?;
                let selected = base.join(relative);
                Some((configured, base, selected))
            })
            .collect();
        if matches.len() > 1 {
            return Err(AppError::BadArgument(format!(
                "{} is inside several DAG roots; configure one root that holds it",
                path.display()
            )));
        }
        let Some((configured, base, selected)) = matches.first() else {
            return Self::at(path);
        };
        let mut root = Self::at(base)?;
        root.name.clone_from(&configured.name);
        let (files, unselected) = std::mem::take(&mut root.files)
            .into_iter()
            .partition(|relative| base.join(relative).starts_with(selected));
        root.files = files;
        root.unselected = unselected;
        root.skipped
            .retain(|relative| base.join(relative).starts_with(selected));
        if selected.is_file() && root.files.is_empty() && root.skipped.is_empty() {
            return Err(AppError::BadArgument(format!(
                "{} is left out of DAG root `{}` (`.schedrignore`), so workers don't get it",
                path.display(),
                configured.name
            )));
        }
        Ok(root)
    }

    /// The root for `path`: a directory scanned like a DAG root (`.schedrignore` honoured), or
    /// a single file.
    ///
    /// # Errors
    ///
    /// [`AppError::BadArgument`] if the path doesn't exist or can't be scanned.
    pub fn at(path: &Path) -> Result<Self, AppError> {
        let resolved = path
            .canonicalize()
            .map_err(|e| AppError::BadArgument(format!("{}: {e}", path.display())))?;
        let (dir, files) = if resolved.is_dir() {
            let snapshot = scan(&resolved).map_err(|e| AppError::BadArgument(e.to_string()))?;
            (resolved, snapshot.python_files())
        } else if !resolved.is_file() {
            return Err(AppError::BadArgument(format!(
                "{}: not a file or a directory",
                path.display()
            )));
        } else {
            let dir = resolved
                .parent()
                .map_or_else(|| PathBuf::from("/"), Path::to_path_buf);
            let name = resolved
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            (dir, vec![name])
        };
        let (files, skipped) = schedr_agent::scan::split_dag_files(&dir, files);
        let name = dir
            .file_name()
            .map_or_else(|| "root".to_owned(), |n| n.to_string_lossy().into_owned());
        Ok(Self {
            name,
            dir,
            files,
            skipped,
            unselected: Vec::new(),
        })
    }
}

/// Runs `schedr dags check` or `schedr dags test`.
///
/// # Errors
///
/// [`AppError`] for bad arguments, a failure to run the runner, errors found by `check`, a
/// worker version older than this schedr, a server `--against` cannot read, or a run of
/// `test` that didn't succeed.
pub async fn run_local(
    command: &DagsCommand,
    config: &Config,
    env: &BTreeMap<String, String>,
    client: &ClientArgs,
    out: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        DagsCommand::Check { path, against } => {
            check_path(path, against.as_deref(), client, config, env, out).await
        }
        DagsCommand::Test {
            dag,
            planned_for,
            params,
            params_json,
            event_key,
            event_data,
            event_at,
            secrets,
            connections,
            task,
            path,
        } => {
            let params = crate::user::given_params(params, params_json.as_deref())?;
            let secrets = secrets
                .iter()
                .map(|spec| schedr_agent::secrets::MemoryStorage::parse(spec))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::BadArgument(format!("--secret {e}")))?;
            let connections = connections
                .iter()
                .map(|spec| given_connection(spec))
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map_err(|e| AppError::BadArgument(format!("--connection {e}")))?;
            if task.is_some() {
                return Err(not_yet("dags test --task", "M4"));
            }
            let at = planned_for
                .as_deref()
                .map(crate::times::parse_for)
                .transpose()
                .map_err(AppError::BadArgument)?
                .map(|t| t.at);
            let event = event_key.clone().map(|key| (key, event_data.as_deref()));
            let event = event
                .map(|(key, data)| {
                    let data =
                        crate::user::given_event_data(data).map_err(AppError::BadArgument)?;
                    Ok::<_, AppError>((key, data))
                })
                .transpose()?;
            let event_at = event_at
                .as_deref()
                .map(crate::times::parse_for)
                .transpose()
                .map_err(AppError::BadArgument)?
                .map(|t| t.at);
            let run = TestRun {
                planned_for: at,
                params,
                event,
                event_at,
                secrets,
                connections,
            };
            Box::pin(test_dag(
                dag,
                &run,
                path.as_deref(),
                config,
                env,
                client.json,
                out,
            ))
            .await
        }
        _ => Err(AppError::BadArgument("not a local command".to_owned())),
    }
}

/// Parses `path`, reports it, and when `against` is set compares this schedr with that
/// server's active workers.
///
/// # Errors
///
/// As [`run_local`] for `dags check`.
async fn check_path(
    path: &Path,
    against: Option<&str>,
    client: &ClientArgs,
    config: &Config,
    env: &BTreeMap<String, String>,
    out: &mut impl Write,
) -> Result<(), AppError> {
    if let Some(files) = check_scripts(path, config)? {
        let version = match against {
            Some(url) => Some(worker_versions(url, client, env).await),
            None => None,
        };
        return finish_check(&files, version, client.json, out);
    }
    let root = LocalRoot::at_in(path, config)?;
    // A folder's connections can be defined anywhere in its root: all is parsed, as a worker
    // does, and what was named is reported.
    let mut whole = root.clone();
    if path.is_dir() {
        whole.files.extend(whole.unselected.iter().cloned());
        whole.files.sort();
    }
    let mut files = check_root(&whole, config, env).await?;
    // Not in `--json`'s output: it stays what a parse reports.
    if !client.json {
        for file in &root.skipped {
            writeln!(
                out,
                "skipped {file}: it doesn't mention `schedr`, so it is never parsed (a file \
                 defining DAGs needs at least `import schedr`)"
            )?;
        }
    }
    if path.is_dir() {
        // A whole root: what its tasks declare in `connections=` is checked against its own.
        files = schedr_master::validate::connection_problems(files);
        files.retain(|file| !root.unselected.contains(&file.path));
    }
    let version = match against {
        Some(url) => Some(worker_versions(url, client, env).await),
        None => None,
    };
    finish_check(&files, version, client.json, out)
}

/// A script root's `schedr.toml` files read and checked, as a worker reads them (M4.9): for a
/// path in a configured script root (its scripts never imported, as a DAG check would), or
/// outside the configured roots a `schedr.toml` or a directory holding one. The root is the
/// configured script root, else the directory (the file's own for a file). `None` for a DAG
/// check: a path in a configured DAG root, or anything else.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the path doesn't exist or can't be scanned, or is a file in a
/// script root that isn't a `schedr.toml` (no DAG file there), or a `schedr.toml` in a DAG
/// root (read only in script roots).
pub fn check_scripts(path: &Path, config: &Config) -> Result<Option<Vec<ParsedFile>>, AppError> {
    let bad = |e: std::io::Error| AppError::BadArgument(format!("{}: {e}", path.display()));
    let is_file = path.metadata().map_err(bad)?.is_file();
    // A file by its own name (a `schedr.toml` may link elsewhere) in its resolved directory.
    let named_config =
        is_file && path.file_name().and_then(|n| n.to_str()) == Some(schedr_agent::jobs::FILE_NAME);
    let resolved = if is_file {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        parent
            .canonicalize()
            .map_err(bad)?
            .join(path.file_name().unwrap_or_default())
    } else {
        path.canonicalize().map_err(bad)?
    };
    let inside = |roots: &[schedr_config::roots::Root]| {
        roots.iter().find_map(|root| {
            let dir = root.path.canonicalize().ok()?;
            resolved.starts_with(&dir).then(|| (root.name.clone(), dir))
        })
    };
    let (name, dir) = if let Some(configured) = inside(&config.script_roots) {
        if is_file && !named_config {
            return Err(AppError::BadArgument(format!(
                "{}: in script root `{}`, whose files aren't DAG files: check its `{}` files",
                path.display(),
                configured.0,
                schedr_agent::jobs::FILE_NAME
            )));
        }
        configured
    } else if let Some((root, _)) = inside(&config.dag_roots) {
        if named_config {
            return Err(AppError::BadArgument(format!(
                "{}: in DAG root `{root}`: `{}` files are read only in script roots \
                 (`SCHEDR_SCRIPT_PATH`)",
                path.display(),
                schedr_agent::jobs::FILE_NAME
            )));
        }
        return Ok(None);
    } else if is_file {
        if !named_config {
            return Ok(None);
        }
        let dir = resolved
            .parent()
            .map_or_else(|| PathBuf::from("/"), Path::to_path_buf);
        (directory_name(&dir), dir)
    } else {
        (directory_name(&resolved), resolved.clone())
    };
    let snapshot = scan(&dir).map_err(|e| AppError::BadArgument(e.to_string()))?;
    let mut files: Vec<String> = snapshot.files().keys().cloned().collect();
    let wanted: Vec<String> = if is_file {
        // Asked for by name: read even if `.schedrignore` leaves it out.
        let relative = resolved
            .strip_prefix(&dir)
            .map_err(|e| AppError::BadArgument(format!("{}: {e}", path.display())))?
            .to_string_lossy()
            .replace('\\', "/");
        if !files.contains(&relative) {
            files.push(relative.clone());
        }
        vec![relative]
    } else {
        files
            .iter()
            .filter(|file| {
                schedr_agent::jobs::is_config(file) && dir.join(file).starts_with(&resolved)
            })
            .cloned()
            .collect()
    };
    if wanted.is_empty() && inside(&config.script_roots).is_none() {
        // Outside the configured roots, a directory without one is a DAG directory.
        return Ok(None);
    }
    let interpreters: Vec<String> = config.interpreters.keys().cloned().collect();
    let parsed = schedr_agent::jobs::read_root(&name, &dir, (&files, &wanted), Some(&interpreters));
    let limit = usize::try_from(config.grpc.max_message_size).unwrap_or(usize::MAX);
    Ok(Some(validate_files(
        &name,
        parsed,
        &Checks::new(SchedrVersion::current(), limit),
    )))
}

/// A directory's last component, as a root's default name.
fn directory_name(dir: &Path) -> String {
    dir.file_name()
        .map_or_else(|| "root".to_owned(), |n| n.to_string_lossy().into_owned())
}

/// Parses a root's files with the real runner and applies the master's checks.
///
/// # Errors
///
/// [`AppError::BadArgument`] if the DAG interpreter isn't defined or the runner can't be run.
pub async fn check_root(
    root: &LocalRoot,
    config: &Config,
    env: &BTreeMap<String, String>,
) -> Result<Vec<ParsedFile>, AppError> {
    // What a worker gets: `.schedrignore`'s files aren't there to import (decision 330, B179).
    let mut copies = BTreeMap::from([(root.name.clone(), CheckCopy::new(&root.name, &root.dir)?)]);
    let configured = |path: &Path| path.canonicalize().ok().as_deref() == Some(root.dir.as_path());
    if config.dag_roots.iter().any(|other| configured(&other.path)) {
        for other in config
            .dag_roots
            .iter()
            .filter(|other| !configured(&other.path))
        {
            copies.insert(
                other.name.clone(),
                CheckCopy::new(&other.name, &other.path)?,
            );
        }
    }
    let sys_path: Vec<String> = copies
        .values()
        .map(|copy| copy.dir.to_string_lossy().into_owned())
        .collect();
    let in_copy = root.clone_at(&copies[&root.name].dir);
    let root = &in_copy;
    let python = config
        .interpreters
        .get(&config.dags.interpreter)
        .map(|i| i.path.clone())
        .ok_or_else(|| {
            AppError::BadArgument(format!(
                "the DAG interpreter `{}` isn't defined in [interpreters]",
                config.dags.interpreter
            ))
        })?;
    let log_file = std::env::temp_dir().join(format!("schedr-check-{}.log", std::process::id()));
    let check_forks = CheckForks::start(config, env).await?;
    let runner = ProcessParseRunner {
        process: ProcessSpec {
            program: python,
            args: vec![
                // Not the working directory first: a root's file can't shadow what the runner imports (B178).
                "-P".to_owned(),
                "-m".to_owned(),
                "schedr.runner".to_owned(),
                "parse".to_owned(),
            ],
            env: env
                .iter()
                // As a task's (`PYTHONPATH` changes what imports find).
                .filter(|(name, _)| {
                    schedr_agent::env::is_allowed(name) || config.worker.pass_env.contains(name)
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            working_dir: root.dir.clone(),
            log_file: log_file.clone(),
            max_output_size: 64 * 1024,
            mask: schedr_agent::mask::Mask::default(),
            start_timeout: config.worker.runner_start_timeout,
            output_drain: config.timings.output_drain_timeout,
        },
        memory: schedr_agent::memory::MemoryWatch::default(),
        kills: prometheus_client::metrics::counter::Counter::default(),
        // As tasks will run (decision 326): forked from the configured fork server.
        forks: check_forks.as_ref().map(|started| started.servers.clone()),
    };
    let request = ParseRequest {
        root: root.name.clone(),
        root_dir: root.dir.to_string_lossy().into_owned(),
        files: root.files.clone(),
        sys_path,
    };
    // A `ScriptOperator` is looked up in the configured script roots, read in place.
    let scripts = config
        .script_roots
        .iter()
        .map(|script| {
            let at = schedr_agent::jobs::ScriptRoot {
                configured: script.path.clone(),
                copy: script.path.clone(),
            };
            (script.name.clone(), at)
        })
        .collect();
    let limits = ParseLimits::configured(
        config.dags.parse_file_timeout,
        config.dags.parse_concurrency,
    );
    let parsed = parse_files_with(&runner, request, limits, &scripts).await;
    drop(check_forks);
    let output = std::fs::read_to_string(&log_file).unwrap_or_default();
    let _ = std::fs::remove_file(&log_file);
    let parsed = parsed.map_err(|e| {
        AppError::BadArgument(runner_failed(
            &e.to_string(),
            config,
            &runner.process.program,
            &output,
        ))
    })?;
    let limit = usize::try_from(config.grpc.max_message_size).unwrap_or(usize::MAX);
    Ok(validate_files(
        &root.name,
        parsed,
        &Checks::new(SchedrVersion::current(), limit),
    ))
}

/// How many of the runner's last output lines a failed parse shows.
const RUNNER_OUTPUT_LINES: usize = 20;

/// Why parsing failed, with what helps fixing it: the interpreter, how it was chosen, the
/// runner's last output lines, and what the interpreter needs.
fn runner_failed(error: &str, config: &Config, python: &Path, output: &str) -> String {
    let builtin = config
        .builtin_python
        .as_deref()
        .filter(|_| config.dags.interpreter == "python");
    let how = builtin.unwrap_or("configured in [interpreters]");
    let mut message = format!(
        "parsing failed: {error}\n  interpreter `{}`: {} ({how})",
        config.dags.interpreter,
        python.display()
    );
    let lines: Vec<&str> = output.lines().collect();
    if !lines.is_empty() {
        message.push_str("\n  its output:");
        for line in &lines[lines.len().saturating_sub(RUNNER_OUTPUT_LINES)..] {
            message.push_str("\n    ");
            message.push_str(line);
        }
    }
    let choose = if config.dags.interpreter == "python" {
        "[interpreters] python (SCHEDR_CONFIG__INTERPRETERS__PYTHON)".to_owned()
    } else {
        format!(
            "[interpreters] {}, or [dags] interpreter",
            config.dags.interpreter
        )
    };
    let _ = write!(
        message,
        "\n  it needs Python 3.14+ with schedr {} installed; choose another with {choose}",
        crate::VERSION
    );
    message
}

/// An active worker compared with the local schedr (`dags check --against`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ComparedWorker {
    /// Its ID.
    pub worker_id: String,
    /// The version it reported (`major.minor` or `major.minor.patch`).
    pub schedr_version: String,
}

/// What `--against` found. `ok` is false when an active worker is older than `local`, or
/// none is active. A version that does not parse is unknown, as the master stores it, and
/// does not by itself make `ok` false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionCheck {
    /// This schedr, as `major.minor`.
    pub local: SchedrVersion,
    /// Whether every active worker is eligible: at least `local`, or an unknown version.
    pub ok: bool,
    /// Set when `ok` is false. The CLI prints it as `error: {message}`.
    pub message: Option<String>,
    /// Every active worker, in the order the server listed them.
    pub workers: Vec<ComparedWorker>,
    /// The server URL the lines name, without a trailing slash.
    server: String,
}

impl VersionCheck {
    /// The line a person sees: `ok: ...` when the check passes, `error: ...` when it fails.
    #[must_use]
    pub(crate) fn line(&self) -> String {
        match &self.message {
            Some(body) => format!("error: {body}"),
            None => passing_line(self.local, &self.server, &self.workers),
        }
    }
}

/// Reads `GET /api/v1/workers` at `url` with the CLI's client ([`connect_to`]) and compares
/// the active workers with this build of schedr.
///
/// # Errors
///
/// [`ClientError`] when the server cannot be reached or the answer is not the worker list.
pub(crate) async fn worker_versions(
    url: &str,
    client: &ClientArgs,
    env: &BTreeMap<String, String>,
) -> Result<VersionCheck, ClientError> {
    let api = connect_to(url, client, env);
    let server = api.transport().server().to_owned();
    let listed = api.workers().await?;
    Ok(compare_active_workers(
        SchedrVersion::current(),
        &server,
        &listed.workers,
    ))
}

/// Compares `local` with the active workers at `server`.
///
/// Active includes a worker drained for maintenance. Draining, disconnected, offline and lost
/// workers are left out. The patch level is ignored. A version that does not parse is unknown:
/// the master stores `None` and still assigns tasks, so the check does not fail for that worker.
#[must_use]
pub(crate) fn compare_active_workers(
    local: SchedrVersion,
    server: &str,
    workers: &[WorkerView],
) -> VersionCheck {
    let active_name = WorkerState::Active.to_string();
    let mut compared = Vec::new();
    for worker in workers {
        if worker.state == active_name {
            compared.push(ComparedWorker {
                worker_id: worker.worker_id.clone(),
                schedr_version: worker.schedr_version.clone(),
            });
        }
    }
    if compared.is_empty() {
        return VersionCheck {
            local,
            ok: false,
            message: Some(no_active_message(server, local)),
            workers: compared,
            server: server.to_owned(),
        };
    }
    let message = older_workers(local, server, &compared);
    VersionCheck {
        local,
        ok: message.is_none(),
        message,
        workers: compared,
        server: server.to_owned(),
    }
}

/// No counted worker. A master restart records workers disconnected until they register.
fn no_active_message(server: &str, local: SchedrVersion) -> String {
    format!(
        "no active worker at {server} to compare with this schedr ({local}): \
         workers are recorded disconnected right after a master restart until they \
         register again, so retry in a minute"
    )
}

/// The failure when any of `active` is older than `local`, naming those workers.
///
/// An unreadable version is left out: the master treats it as unknown and still assigns tasks.
fn older_workers(local: SchedrVersion, server: &str, active: &[ComparedWorker]) -> Option<String> {
    let mut by_version: BTreeMap<SchedrVersion, Vec<&str>> = BTreeMap::new();
    for worker in active {
        if let Ok(version) = worker.schedr_version.parse::<SchedrVersion>()
            && local > version
        {
            by_version
                .entry(version)
                .or_default()
                .push(worker.worker_id.as_str());
        }
    }
    if by_version.is_empty() {
        return None;
    }
    let mut groups = Vec::new();
    for (version, ids) in &by_version {
        groups.push(format!("{version}: {}", ids.join(", ")));
    }
    Some(format!(
        "this schedr is {local}; older active workers at {server}: {}",
        groups.join("; ")
    ))
}

/// The passing line, for example `ok: this schedr is 0.3; 2 active workers at the server run 0.3 (w1, w2)`.
fn passing_line(local: SchedrVersion, server: &str, active: &[ComparedWorker]) -> String {
    let n = active.len();
    let (noun, verb) = if n == 1 {
        ("worker", "runs")
    } else {
        ("workers", "run")
    };
    format!(
        "ok: this schedr is {local}; {n} active {noun} at {server} {verb} {}",
        run_groups(active)
    )
}

/// Versions oldest first, then unknown texts, each with its worker ids.
fn run_groups(active: &[ComparedWorker]) -> String {
    let mut by_version: BTreeMap<SchedrVersion, Vec<&str>> = BTreeMap::new();
    let mut unknown: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for worker in active {
        match worker.schedr_version.parse::<SchedrVersion>() {
            Ok(version) => by_version
                .entry(version)
                .or_default()
                .push(worker.worker_id.as_str()),
            Err(_) => unknown
                .entry(worker.schedr_version.as_str())
                .or_default()
                .push(worker.worker_id.as_str()),
        }
    }
    let mut groups = Vec::new();
    for (version, ids) in &by_version {
        groups.push(format!("{version} ({})", ids.join(", ")));
    }
    for (text, ids) in &unknown {
        groups.push(format!("unknown version `{text}` ({})", ids.join(", ")));
    }
    groups.join(", ")
}

/// One file in `dags check --json`.
#[derive(Debug, Serialize)]
struct CheckedFile<'a> {
    path: &'a str,
    dags: Vec<&'a str>,
    error: Option<&'a schedr_model::runner::ErrorSummary>,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    warnings: &'a [String],
}

/// `dags check --json`, with `version_check` when `--against` read the worker list.
#[derive(Serialize)]
struct CheckJson<'a> {
    files: Vec<CheckedFile<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_check: Option<VersionJson<'a>>,
}

/// The `version_check` object. `message` is the line a person sees.
#[derive(Serialize)]
struct VersionJson<'a> {
    local: SchedrVersion,
    ok: bool,
    message: String,
    workers: &'a [ComparedWorker],
}

/// Prints the file report and returns the version failure, the file failure, or both.
///
/// # Errors
///
/// [`AppError::Failed`] when a file has an error or an active worker is older (both, when
/// both happened), [`AppError::Client`] when the server could not be read and the files were
/// fine, or [`AppError::Output`] when writing fails.
pub(crate) fn finish_check(
    files: &[ParsedFile],
    version: Option<Result<VersionCheck, ClientError>>,
    json: bool,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let shown = version.as_ref().and_then(|result| result.as_ref().ok());
    let errors = write_report(files, shown, json, out)?;
    combine(file_failure(errors), version_failure(version))
}

/// The file-report half of a failure, when any file has an error.
fn file_failure(errors: usize) -> Option<String> {
    (errors > 0).then(|| format!("{errors} file(s) with errors"))
}

/// The `--against` half of a failure.
fn version_failure(version: Option<Result<VersionCheck, ClientError>>) -> Option<AppError> {
    match version {
        Some(Err(error)) => Some(AppError::Client(error)),
        Some(Ok(check)) => check.message.map(AppError::Failed),
        None => None,
    }
}

/// One error, or both with a second `error:` line.
fn combine(files: Option<String>, version: Option<AppError>) -> Result<(), AppError> {
    match (files, version) {
        (None, None) => Ok(()),
        (Some(files), None) => Err(AppError::Failed(files)),
        (None, Some(error)) => Err(error),
        (Some(files), Some(error)) => Err(AppError::Failed(format!("{files}\nerror: {error}"))),
    }
}

/// Writes the file report and returns how many files have an error.
///
/// # Errors
///
/// [`AppError::Output`] when writing fails.
fn write_report(
    files: &[ParsedFile],
    version: Option<&VersionCheck>,
    json: bool,
    out: &mut impl Write,
) -> Result<usize, AppError> {
    let errors = files.iter().filter(|f| f.error.is_some()).count();
    if json {
        write_json(files, version, out)?;
    } else {
        write_text(files, errors, version, out)?;
    }
    Ok(errors)
}

/// `--json`: the files, and `version_check` when the worker list was read.
fn write_json(
    files: &[ParsedFile],
    version: Option<&VersionCheck>,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let report = CheckJson {
        files: files
            .iter()
            .map(|file| CheckedFile {
                path: &file.path,
                dags: file.dags.iter().map(|dag| dag.id.as_str()).collect(),
                error: file.error.as_ref(),
                warnings: &file.warnings,
            })
            .collect(),
        version_check: version.map(|check| VersionJson {
            local: check.local,
            ok: check.ok,
            message: check.line(),
            workers: &check.workers,
        }),
    };
    writeln!(
        out,
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_default()
    )?;
    Ok(())
}

/// One line per file and one per warning it raised, then the summary, then the passing
/// `--against` line.
fn write_text(
    files: &[ParsedFile],
    errors: usize,
    version: Option<&VersionCheck>,
    out: &mut impl Write,
) -> Result<(), AppError> {
    for file in files {
        match &file.error {
            None if file.dags.is_empty() => writeln!(out, "ok     {}: no DAGs", file.path)?,
            None => {
                let dags: Vec<String> = file
                    .dags
                    .iter()
                    .map(|dag| format!("{} ({} tasks)", dag.id, dag.tasks.len()))
                    .collect();
                writeln!(out, "ok     {}: {}", file.path, dags.join(", "))?;
            }
            Some(error) => {
                writeln!(
                    out,
                    "error  {}: {}: {}",
                    file.path, error.exception, error.message
                )?;
                for line in error.traceback.lines() {
                    writeln!(out, "         {line}")?;
                }
            }
        }
        // Warnings don't fail the check: the DAG runs as written (e.g. a `TypingHint`).
        for warning in &file.warnings {
            writeln!(out, "warn   {}: {warning}", file.path)?;
        }
    }
    let dags: usize = files.iter().map(|file| file.dags.len()).sum();
    writeln!(
        out,
        "{} file(s), {dags} DAG(s), {errors} error(s)",
        files.len()
    )?;
    if let Some(check) = version.filter(|check| check.ok) {
        writeln!(out, "{}", check.line())?;
    }
    Ok(())
}

/// Runs one real run of `dag` locally and reports it.
/// The run `dags test` makes: when for, and its parameters.
struct TestRun {
    planned_for: Option<jiff::Timestamp>,
    params: Option<serde_json::Map<String, serde_json::Value>>,
    /// `--event-key` and `--event-data`.
    event: Option<(String, Option<Value>)>,
    /// `--event-at`.
    event_at: Option<jiff::Timestamp>,
    /// `--secret` values: in memory, standing for every storage the DAG reads.
    secrets: Vec<(schedr_model::SecretName, String)>,
    /// `--connection` values, by connection and parameter.
    connections: BTreeMap<String, BTreeMap<String, Value>>,
}

/// A `--connection` value: `name={"param": value, ...}`.
///
/// # Errors
///
/// What is wrong with it.
pub(crate) fn given_connection(spec: &str) -> Result<(String, BTreeMap<String, Value>), String> {
    let Some((name, json)) = spec.split_once('=') else {
        return Err(format!("`{spec}`: expected `name={{\"param\": value}}`"));
    };
    schedr_model::TaskId::new(name).map_err(|e| format!("`{name}`: {e}"))?;
    let values = serde_json::from_str(json)
        .map_err(|e| format!("`{name}`: its values must be a JSON object: {e}"))?;
    Ok((name.to_owned(), values))
}

async fn test_dag(
    dag: &str,
    test: &TestRun,
    path: Option<&Path>,
    config: &Config,
    env: &BTreeMap<String, String>,
    json: bool,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let roots: Vec<Root> = match path {
        Some(path) => {
            // A named file alone is parsed, with the root it is in (B118, decision 331); a
            // folder, all of its root, whose connections it can use, as on a worker. The other
            // configured roots are importable.
            let root = LocalRoot::at_in(path, config)?;
            let name = root.name.clone();
            let mut roots = vec![Root {
                only: path.is_file().then_some(root.files),
                name: root.name,
                path: root.dir.clone(),
            }];
            if config
                .dag_roots
                .iter()
                .any(|known| known.path.canonicalize().ok().as_ref() == Some(&root.dir))
            {
                for known in config.dag_roots.iter().filter(|known| known.name != name) {
                    let mut library = known.clone();
                    library.only = Some(Vec::new());
                    roots.push(library);
                }
            }
            roots
        }
        None if !config.dag_roots.is_empty() => config.dag_roots.clone(),
        None => {
            let root = LocalRoot::at(Path::new("."))?;
            vec![Root {
                name: root.name,
                path: root.dir,
                only: None,
            }]
        }
    };
    ensure_defined(dag, &roots, config, env).await?;
    let data = std::env::temp_dir().join(format!(
        "schedr-test-{}-{}",
        std::process::id(),
        jiff::Timestamp::now().as_nanosecond()
    ));
    if let Err(error) = scannable(&roots, &data.join("preflight")) {
        let _ = std::fs::remove_dir_all(&data);
        return Err(error);
    }
    let mut local = config.clone();
    local.dag_roots = roots;
    local.database.url = Some(format!("sqlite://{}", data.join("schedr.db").display()));
    local.worker.data_dir = Some(data.clone());
    local.master.grpc_port = 0;
    local.webserver.port = 0;
    local.worker.log_port = 0;
    local.ops.port = 0;
    local.timings.tick_interval = Duration::from_millis(100);
    local.dags.poll_interval = Duration::from_millis(200);
    local.dags.settle_time = Duration::ZERO;
    let mut setup = plan(&local, env)?;
    // A database of its own, created here.
    setup.migrate = true;
    // A test run's secrets are only its `--secret` values (none: every read is "not found"),
    // never the configured storages standalone would otherwise put in their place.
    let values = schedr_agent::secrets::MemoryStorage::new(test.secrets.clone());
    setup.secret_storage = setup.secret_storage.clone().given(
        schedr_agent::secrets::SecretStores::for_test(values)
            .with_test_connections(test.connections.clone()),
    );
    let logs = setup.agent.setup.log_dir.clone();
    let (stop, stopped) = tokio::sync::watch::channel(0);
    let (bound, addresses) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(run(
        setup,
        move |listening| {
            let _ = bound.send(listening);
        },
        stopped,
    ));
    let result = match addresses.await {
        Ok(listening) => {
            drive(
                dag,
                test,
                &format!("http://{}", listening.http),
                (&data, &logs),
                json,
                out,
            )
            .await
        }
        Err(_) => Err(AppError::BadArgument(
            "the local schedr didn't start".to_owned(),
        )),
    };
    // Two requests: whatever still runs (a run that timed out) is interrupted at once.
    let _ = stop.send(2);
    if let Ok(Err(error)) = server.await {
        tracing::warn!(%error, "local schedr stopped with an error");
    }
    // The master's background tasks aren't waited for, and one may be writing to the database:
    // its journal appears while the folder is being removed. Once it is gone they can't bring it back.
    for _ in 0..50 {
        if schedr_agent::scan::remove_tree(&data).is_ok() || !data.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    result
}

/// The DAG interpreter's fork servers for one check, when it has `fork_server = true`: their logs
/// go to a folder removed when dropped.
struct CheckForks {
    servers: schedr_agent::fork::ForkServers,
    logs: PathBuf,
}

impl CheckForks {
    /// Started and settled: a check runs as tasks will, so it doesn't spawn because the server
    /// is still starting. What is wrong with one (a preload failing) is logged as a warning.
    async fn start(
        config: &Config,
        env: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, AppError> {
        if crate::roles::fork_config(config).is_none() {
            return Ok(None);
        }
        let logs = std::env::temp_dir().join(format!("schedr-check-{}", uuid::Uuid::now_v7()));
        let settings = crate::roles::agent_settings(
            config,
            env,
            &logs,
            schedr_agent::memory::STANDALONE_RESERVE,
        )?;
        std::fs::create_dir_all(&settings.setup.log_dir)
            .map_err(|e| AppError::BadArgument(format!("{}: {e}", logs.display())))?;
        let metrics = schedr_agent::fork::ForkMetrics::default();
        let Some(servers) = schedr_agent::fork::ForkServers::for_worker(&settings.setup, &metrics)
        else {
            return Ok(None);
        };
        for warning in servers
            .settled(config.worker.fork_server_preload_timeout)
            .await
        {
            tracing::warn!(%warning, "fork server");
        }
        Ok(Some(Self { servers, logs }))
    }
}

impl Drop for CheckForks {
    fn drop(&mut self) {
        let _ = schedr_agent::scan::remove_tree(&self.logs);
    }
}

/// A root's read-only copy for one check, as a worker's: removed when dropped.
struct CheckCopy {
    store: PathBuf,
    dir: PathBuf,
}

impl CheckCopy {
    fn new(name: &str, root: &Path) -> Result<Self, AppError> {
        let bad = |e: schedr_agent::scan::ScanError| {
            AppError::BadArgument(format!("can't scan DAG root `{name}`: {e}"))
        };
        let store = std::env::temp_dir().join(format!("schedr-check-{}", uuid::Uuid::now_v7()));
        let before = scan(root).map_err(bad)?;
        let captured = schedr_agent::scan::capture(&before, &store).map_err(bad)?;
        let copy = Self {
            store,
            dir: captured.dir,
        };
        if scan(root).map_err(bad)? != before {
            return Err(AppError::BadArgument(
                "the DAG files changed while being copied; run the check again".to_owned(),
            ));
        }
        Ok(copy)
    }
}

impl Drop for CheckCopy {
    fn drop(&mut self) {
        let _ = schedr_agent::scan::remove_tree(&self.store);
    }
}

/// Fails at once if a root can't be scanned and copied as its agent will (B103): the agent
/// would retry for ever, its worker never registering. Each is copied into `store`, then
/// removed.
fn scannable(roots: &[Root], store: &Path) -> Result<(), AppError> {
    for root in roots {
        let failed = |error: schedr_agent::scan::ScanError| {
            AppError::Failed(format!("can't scan DAG root `{}`: {error}", root.name))
        };
        let snapshot = schedr_agent::scan::scan(&root.path).map_err(failed)?;
        let captured =
            schedr_agent::scan::capture(&snapshot, &store.join(&root.name)).map_err(failed)?;
        if let Err(error) = schedr_agent::scan::remove_capture(&captured.dir) {
            tracing::warn!(dir = %captured.dir.display(), %error, "can't remove a copy of DAG code");
        }
    }
    Ok(())
}

/// Fails fast if no file in the roots defines `dag`, naming the files that didn't import.
async fn ensure_defined(
    dag: &str,
    roots: &[Root],
    config: &Config,
    env: &BTreeMap<String, String>,
) -> Result<(), AppError> {
    let mut broken = Vec::new();
    for root in roots {
        let mut local = LocalRoot::at(&root.path)?;
        local.name.clone_from(&root.name);
        if let Some(only) = &root.only {
            local.files.clone_from(only);
        }
        for file in check_root(&local, config, env).await? {
            if file.dags.iter().any(|d| d.id.as_str() == dag) {
                return Ok(());
            }
            if file.error.is_some() {
                broken.push(format!("{}:{}", root.name, file.path));
            }
        }
    }
    let message = if broken.is_empty() {
        format!("no DAG `{dag}` in the DAG files (`schedr dags check` shows what they define)")
    } else {
        format!(
            "no DAG `{dag}` in the DAG files; these files have errors \
             (`schedr dags check` shows them): {}",
            broken.join(", ")
        )
    };
    Err(AppError::Failed(message))
}

/// Triggers the run, follows it, and prints the report.
async fn drive(
    dag: &str,
    test: &TestRun,
    api: &str,
    (data, logs): (&Path, &Path),
    json: bool,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let client = Client::new(
        HttpTransport::new(api, Auth::None),
        Duration::from_millis(100),
        Duration::from_secs(60),
    );
    let location = wait_for_dag(&client, dag).await?;
    let request = TriggerRequest {
        planned_for: test.planned_for,
        params: test.params.clone(),
        event_key: test.event.as_ref().map(|(key, _)| key.clone()),
        event_data: test.event.as_ref().and_then(|(_, data)| data.clone()),
        event_at: test.event_at,
        event: None,
    };
    let command = client
        .wait(
            client
                .send_command(|| client.trigger_with(dag, &request))
                .await?,
        )
        .await?;
    let run_id = command
        .result
        .as_ref()
        .and_then(|r| r["run_id"].as_str())
        .unwrap_or_default()
        .to_owned();
    let started = Instant::now();
    let mut shown: BTreeMap<String, String> = BTreeMap::new();
    let detail = loop {
        let detail = client.run(&run_id).await?;
        if !json {
            for task in &detail.tasks {
                let worth_showing = !["scheduled", "queued"].contains(&task.state.as_str());
                let name = shown_name(task);
                if worth_showing && shown.get(&name) != Some(&task.state) {
                    writeln!(out, "{:<10} {name}", task.state)?;
                    shown.insert(name, task.state.clone());
                }
            }
        }
        if ["success", "failed", "cancelled"].contains(&detail.run.state.as_str()) {
            break detail;
        }
        if started.elapsed() > TEST_RUN_TIMEOUT {
            return Err(AppError::Failed(format!(
                "run {run_id} didn't finish in time"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let report = report(&client, &detail, location.as_ref(), (data, logs)).await?;
    if json {
        writeln!(
            out,
            "{}",
            serde_json::to_string_pretty(&report).unwrap_or_default()
        )?;
    } else {
        print_report(&report, out)?;
    }
    if detail.run.state == "success" {
        Ok(())
    } else {
        Err(AppError::Failed(format!(
            "run {run_id} ended {}",
            detail.run.state
        )))
    }
}

/// Waits until the local master has stored `dag`; returns the file that defines it.
async fn wait_for_dag(
    client: &Client<HttpTransport>,
    dag: &str,
) -> Result<Option<LocationView>, AppError> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let dags = client.dags().await?;
        if let Some(found) = dags.dags.iter().find(|d| d.dag_id == dag) {
            if let Some(places) = &found.conflict {
                let places: Vec<String> = places
                    .iter()
                    .map(|p| format!("{}:{}", p.root, p.path))
                    .collect();
                return Err(AppError::Failed(format!(
                    "DAG `{dag}` is defined in more than one place: {}",
                    places.join(", ")
                )));
            }
            return Ok(found.location.clone());
        }
        if Instant::now() > deadline {
            return Err(AppError::Failed(format!(
                "no DAG `{dag}` in the DAG files (`schedr dags check` shows what they define)"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The run's report: the run, the file defining the DAG, and each task's state, output,
/// attempts and log.
async fn report(
    client: &Client<HttpTransport>,
    detail: &RunDetail,
    location: Option<&LocationView>,
    (data, logs): (&Path, &Path),
) -> Result<Value, AppError> {
    let db = Database::connect(&format!("sqlite://{}", data.join("schedr.db").display())).await?;
    let run_id = &detail.run.run_id;
    let mut tasks = Vec::new();
    for task in &detail.tasks {
        let instance = u32::try_from(task.map_index).ok();
        // A mapped task as a whole has no attempts; its output is the gathered list.
        let attempts: AttemptList = if task.instances.is_some() {
            AttemptList {
                attempts: Vec::new(),
            }
        } else {
            client
                .instance_attempts(run_id, &task.task_id, instance)
                .await?
        };
        let mut log = String::new();
        for attempt in &attempts.attempts {
            let file = logs.join(format!("{}.log", attempt.attempt_id));
            log.push_str(&std::fs::read_to_string(file).unwrap_or_default());
        }
        let output = if task.instances.is_some() {
            client
                .instance_output(run_id, &task.task_id, None)
                .await
                .ok()
                .map(|o| o.value)
        } else {
            match schedr_model::TaskId::new(task.task_id.clone()) {
                Ok(id) => db
                    .instance_output(run_id, &id, task.map_index)
                    .await?
                    .map(|(_, value)| value),
                Err(_) => None,
            }
        };
        tasks.push(json!({
            "task_id": task.task_id,
            "map_index": task.map_index,
            "instances": task.instances,
            "state": task.state,
            "outcome": task.outcome,
            "try_number": task.try_number,
            "cleanup": task.cleanup,
            "output": output,
            "attempts": attempts.attempts,
            "log": log,
        }));
    }
    Ok(json!({
        "run": detail.run,
        "location": location,
        "tasks": tasks,
        "resources": detail.resources,
    }))
}

/// A task as the report shows it: `fetch`, or `fetch[2]` for an instance (decision 86).
fn shown_name(task: &schedr_api::views::TaskView) -> String {
    if task.map_index >= 0 {
        format!("{}[{}]", task.task_id, task.map_index)
    } else {
        task.task_id.clone()
    }
}

/// [`shown_name`] of a task in the report's JSON.
fn reported_name(task: &Value) -> String {
    let id = task["task_id"].as_str().unwrap_or_default();
    match task["map_index"].as_i64() {
        Some(index) if index >= 0 => format!("{id}[{index}]"),
        _ => id.to_owned(),
    }
}

fn print_report(report: &Value, out: &mut impl Write) -> Result<(), AppError> {
    let empty = Vec::new();
    let tasks = report["tasks"].as_array().unwrap_or(&empty);
    for task in tasks {
        let name = reported_name(task);
        let id = name.as_str();
        for line in task["log"].as_str().unwrap_or_default().lines() {
            writeln!(out, "{id} | {line}")?;
        }
        for attempt in task["attempts"].as_array().unwrap_or(&empty) {
            let error = &attempt["error_summary"];
            if error.is_null() {
                continue;
            }
            writeln!(
                out,
                "{id} | try {} failed ({}):",
                attempt["try_number"],
                attempt["error_kind"].as_str().unwrap_or("error")
            )?;
            match error["traceback"].as_str().filter(|t| !t.is_empty()) {
                Some(traceback) => {
                    for line in traceback.lines() {
                        writeln!(out, "{id} | {line}")?;
                    }
                }
                None => writeln!(
                    out,
                    "{id} | {}: {}",
                    error["exception"].as_str().unwrap_or_default(),
                    error["message"].as_str().unwrap_or_default()
                )?,
            }
        }
    }
    let rows: Vec<Vec<String>> = tasks
        .iter()
        .map(|task| {
            vec![
                reported_name(task),
                task["state"].as_str().unwrap_or_default().to_owned(),
                task["outcome"].as_str().unwrap_or("-").to_owned(),
                task["try_number"].to_string(),
                duration(task),
            ]
        })
        .collect();
    crate::user::write_table(
        out,
        &["TASK", "STATE", "OUTCOME", "TRIES", "DURATION"],
        &rows,
    )?;
    writeln!(out, "outputs:")?;
    for task in tasks {
        if !task["output"].is_null() {
            // Named as in the table: `fetch` the gathered list, `fetch[0]` an instance's.
            writeln!(out, "  {}: {}", reported_name(task), task["output"])?;
        }
    }
    writeln!(
        out,
        "run {}: {}",
        report["run"]["run_id"].as_str().unwrap_or_default(),
        report["run"]["state"].as_str().unwrap_or_default()
    )?;
    Ok(())
}

/// How long the task's last attempt ran, e.g. `1.2s`; `-` if it never started.
/// The last attempt's duration as the worker measured it (the start and end timestamps come
/// from the master's wall clock, which can step, so they aren't subtracted).
fn duration(task: &Value) -> String {
    task["attempts"]
        .as_array()
        .and_then(|a| a.last())
        .and_then(|last| last["duration_ms"].as_u64())
        .map_or_else(|| "-".to_owned(), crate::user::seconds)
}

const fn not_yet(command: &'static str, milestone: &'static str) -> AppError {
    AppError::NotImplemented { command, milestone }
}
