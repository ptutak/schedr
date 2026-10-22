//! The DAG author's local commands, with no server: `schedr dags check` and `schedr dags test`
//! (see PROJECT.md, Testing DAGs).
//!
//! - **`dags check <path>`** imports the DAG files with the real runner (parse mode, the
//!   per-file timeout) and applies the master's checks, so whatever the master would refuse is
//!   reported here; exit 1 on any error.
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
use schedr_api::views::{AttemptList, LocationView, RunDetail};
use schedr_config::{Config, Root};
use schedr_db::Database;
use schedr_master::validate::validate_files;
use schedr_model::SchedrVersion;
use schedr_model::runner::{ParseRequest, ParsedFile};
use serde::Serialize;
use serde_json::{Value, json};

use crate::app::AppError;
use crate::cli::DagsCommand;
use crate::client::{Auth, Client, HttpTransport};
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
/// [`AppError`] for bad arguments, a failure to run the runner, errors found by `check`, or a
/// run of `test` that didn't succeed.
pub async fn run_local(
    command: &DagsCommand,
    config: &Config,
    env: &BTreeMap<String, String>,
    json: bool,
    out: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        DagsCommand::Check { path, against } => {
            if against.is_some() {
                return Err(not_yet("dags check --against", "M2"));
            }
            let root = LocalRoot::at(path)?;
            let files = check_root(&root, config, env).await?;
            report_check(&files, json, out)
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
            test_dag(dag, at, path.as_deref(), config, env, json, out).await
        }
        _ => Err(AppError::BadArgument("not a local command".to_owned())),
    }
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
    Ok(validate_files(&root.name, parsed, SchedrVersion::current()))
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

/// One file in `dags check --json`.
#[derive(Debug, Serialize)]
struct CheckedFile<'a> {
    path: &'a str,
    dags: Vec<&'a str>,
    error: Option<&'a schedr_model::runner::ErrorSummary>,
}

fn report_check(files: &[ParsedFile], json: bool, out: &mut impl Write) -> Result<(), AppError> {
    let errors = files.iter().filter(|f| f.error.is_some()).count();
    if json {
        let checked: Vec<CheckedFile<'_>> = files
            .iter()
            .map(|f| CheckedFile {
                path: &f.path,
                dags: f.dags.iter().map(|d| d.id.as_str()).collect(),
                error: f.error.as_ref(),
            })
            .collect();
        writeln!(
            out,
            "{}",
            serde_json::to_string_pretty(&json!({"files": checked})).unwrap_or_default()
        )?;
    } else {
        for file in files {
            match &file.error {
                None if file.dags.is_empty() => writeln!(out, "ok     {}: no DAGs", file.path)?,
                None => {
                    let dags: Vec<String> = file
                        .dags
                        .iter()
                        .map(|d| format!("{} ({} tasks)", d.id, d.tasks.len()))
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
        }
        let dags: usize = files.iter().map(|f| f.dags.len()).sum();
        writeln!(
            out,
            "{} file(s), {dags} DAG(s), {errors} error(s)",
            files.len()
        )?;
    }
    if errors > 0 {
        return Err(AppError::Failed(format!("{errors} file(s) with errors")));
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
    local.master.tick_interval = Duration::from_millis(100);
    local.dags.poll_interval = Duration::from_millis(200);
    local.dags.settle_time = Duration::ZERO;
    let setup = plan(&local, env)?;
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
                &data,
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
    data: &Path,
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
    let report = report(&client, &detail, location.as_ref(), data).await?;
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
    data: &Path,
) -> Result<Value, AppError> {
    let db = Database::connect(&format!("sqlite://{}", data.join("schedr.db").display())).await?;
    let outputs = db.run_outputs(&detail.run.run_id).await?;
    let mut tasks = Vec::new();
    for task in &detail.tasks {
        let attempts: AttemptList = client.attempts(&detail.run.run_id, &task.task_id).await?;
        let mut log = String::new();
        for attempt in &attempts.attempts {
            let file = data
                .join("logs")
                .join(format!("{}.log", attempt.attempt_id));
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
