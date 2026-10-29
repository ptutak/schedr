//! The DAG author's local commands: `schedr dags check` and `schedr dags test`
//! (see PROJECT.md, Testing DAGs).
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
use schedr_agent::parse::parse_files;
use schedr_agent::process::ProcessSpec;
use schedr_agent::scan::scan;
use schedr_api::views::{AttemptList, LocationView, RunDetail, WorkerView};
use schedr_config::{Config, Root};
use schedr_db::Database;
use schedr_master::validate::validate_files;
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
    /// The `.py` files, relative to `dir`.
    pub files: Vec<String>,
}

impl LocalRoot {
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
        let name = dir
            .file_name()
            .map_or_else(|| "root".to_owned(), |n| n.to_string_lossy().into_owned());
        Ok(Self { name, dir, files })
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
            secrets,
            task,
            path,
        } => {
            if !params.is_empty() {
                return Err(not_yet("dags test -p", "M4"));
            }
            if !secrets.is_empty() {
                return Err(not_yet("dags test --secret", "M4"));
            }
            if task.is_some() {
                return Err(not_yet("dags test --task", "M4"));
            }
            let at = planned_for
                .as_deref()
                .map(crate::times::parse_for)
                .transpose()
                .map_err(AppError::BadArgument)?
                .map(|t| t.at);
            test_dag(dag, at, path.as_deref(), config, env, client.json, out).await
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
    let root = LocalRoot::at(path)?;
    let files = check_root(&root, config, env).await?;
    let version = match against {
        Some(url) => Some(worker_versions(url, client, env).await),
        None => None,
    };
    finish_check(&files, version, client.json, out)
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
    let mut runner = ProcessParseRunner {
        process: ProcessSpec {
            program: python,
            args: vec![
                "-m".to_owned(),
                "schedr.runner".to_owned(),
                "parse".to_owned(),
            ],
            env: env
                .iter()
                .filter(|(name, _)| schedr_agent::env::is_allowed(name))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            working_dir: root.dir.clone(),
            log_file: log_file.clone(),
            max_output_size: 64 * 1024,
        },
        memory: schedr_agent::memory::MemoryWatch::default(),
        kills: prometheus_client::metrics::counter::Counter::default(),
        // `schedr dags check` parses once: a fork server would only add its start.
        forks: None,
    };
    let request = ParseRequest {
        root: root.name.clone(),
        root_dir: root.dir.to_string_lossy().into_owned(),
        files: root.files.clone(),
        sys_path: vec![root.dir.to_string_lossy().into_owned()],
    };
    let parsed = parse_files(&mut runner, request, config.dags.parse_file_timeout).await;
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
    let limit = usize::try_from(config.master.grpc_max_message_size).unwrap_or(usize::MAX);
    Ok(validate_files(
        &root.name,
        parsed,
        SchedrVersion::current(),
        limit,
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
async fn test_dag(
    dag: &str,
    planned_for: Option<jiff::Timestamp>,
    path: Option<&Path>,
    config: &Config,
    env: &BTreeMap<String, String>,
    json: bool,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let roots: Vec<Root> = match path {
        Some(path) => {
            let root = LocalRoot::at(path)?;
            vec![Root {
                name: root.name,
                path: root.dir,
            }]
        }
        None if !config.dag_roots.is_empty() => config.dag_roots.clone(),
        None => {
            let root = LocalRoot::at(Path::new("."))?;
            vec![Root {
                name: root.name,
                path: root.dir,
            }]
        }
    };
    ensure_defined(dag, &roots, config, env).await?;
    let data = std::env::temp_dir().join(format!(
        "schedr-test-{}-{}",
        std::process::id(),
        jiff::Timestamp::now().as_nanosecond()
    ));
    let mut local = config.clone();
    local.dag_roots = roots;
    local.database.url = Some(format!("sqlite://{}", data.join("schedr.db").display()));
    local.worker.data_dir = Some(data.clone());
    local.master.grpc_port = 0;
    local.webserver.port = 0;
    local.worker.log_port = 0;
    local.ops.port = 0;
    local.master.tick_interval = Duration::from_millis(100);
    local.dags.poll_interval = Duration::from_millis(200);
    local.dags.settle_time = Duration::ZERO;
    let mut setup = plan(&local, env)?;
    // A database of its own, created here.
    setup.migrate = true;
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
                planned_for,
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
    let _ = std::fs::remove_dir_all(&data);
    result
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
    planned_for: Option<jiff::Timestamp>,
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
    let command = client.wait(client.trigger(dag, planned_for).await?).await?;
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
                if worth_showing && shown.get(&task.task_id) != Some(&task.state) {
                    writeln!(out, "{:<10} {}", task.state, task.task_id)?;
                    shown.insert(task.task_id.clone(), task.state.clone());
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
    let outputs = db.run_outputs(&detail.run.run_id).await?;
    let mut tasks = Vec::new();
    for task in &detail.tasks {
        let attempts: AttemptList = client.attempts(&detail.run.run_id, &task.task_id).await?;
        let mut log = String::new();
        for attempt in &attempts.attempts {
            let file = logs.join(format!("{}.log", attempt.attempt_id));
            log.push_str(&std::fs::read_to_string(file).unwrap_or_default());
        }
        let output = schedr_model::TaskId::new(task.task_id.clone())
            .ok()
            .and_then(|id| outputs.get(&id).cloned());
        tasks.push(json!({
            "task_id": task.task_id,
            "state": task.state,
            "outcome": task.outcome,
            "try_number": task.try_number,
            "output": output,
            "attempts": attempts.attempts,
            "log": log,
        }));
    }
    Ok(json!({"run": detail.run, "location": location, "tasks": tasks}))
}

fn print_report(report: &Value, out: &mut impl Write) -> Result<(), AppError> {
    let empty = Vec::new();
    let tasks = report["tasks"].as_array().unwrap_or(&empty);
    for task in tasks {
        let id = task["task_id"].as_str().unwrap_or_default();
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
                task["task_id"].as_str().unwrap_or_default().to_owned(),
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
            writeln!(
                out,
                "  {}: {}",
                task["task_id"].as_str().unwrap_or_default(),
                task["output"]
            )?;
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
        .map_or_else(
            || "-".to_owned(),
            |ms| format!("{:.1}s", Duration::from_millis(ms).as_secs_f64()),
        )
}

const fn not_yet(command: &'static str, milestone: &'static str) -> AppError {
    AppError::NotImplemented { command, milestone }
}
