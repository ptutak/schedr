//! The user CLI's commands over the REST API: `schedr dags ...` and `schedr runs ...`.
//!
//! Human output is aligned columns with times in UTC and local time; `--json` prints the API's
//! JSON (UTC only). Actions wait for the master's decision and print the result, or fail with
//! the master's reason (see PROJECT.md, Web UI and CLI).

use std::io::Write;

use jiff::Timestamp;
use jiff::tz::TimeZone;
use schedr_api::views::{CommandView, DagView, RunView, WorkerView};

use crate::app::AppError;
use crate::cli::{DagsCommand, RunsCommand, TasksCommand, WorkersCommand};
use crate::client::{Client, TaskAction, Transport};
use crate::times::{parse_for, show, show_short};

#[cfg(test)]
mod tests;

/// How output is written.
#[derive(Debug, Clone)]
pub struct Output {
    /// Print the API's JSON instead of text.
    pub json: bool,
    /// The machine's time zone, for the local half of times.
    pub local: TimeZone,
}

/// Runs a `schedr dags` command.
///
/// # Errors
///
/// [`AppError::Client`] if the server can't be reached, answers with an error or rejects the
/// command; [`AppError::BadArgument`] for `check` / `test`, which run locally
/// ([`crate::local`]).
pub async fn run_dags<T: Transport>(
    command: &DagsCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        DagsCommand::List => {
            let dags = client.dags().await?;
            if output.json {
                return print_json(out, &dags);
            }
            write_table(
                out,
                &["DAG", "PAUSED", "TRIGGER", "DEFINED IN"],
                &dag_rows(&dags.dags, &output.local),
            )?;
        }
        DagsCommand::Pause { dag } => {
            let command = client
                .wait(client.set_paused(dag, true, false).await?)
                .await?;
            done(out, output, &command, &format!("paused {dag}"))?;
        }
        DagsCommand::Unpause { dag } => {
            let command = client
                .wait(client.set_paused(dag, false, false).await?)
                .await?;
            done(out, output, &command, &format!("unpaused {dag}"))?;
        }
        DagsCommand::Check { .. } | DagsCommand::Test { .. } => {
            return Err(AppError::BadArgument(
                "`dags check` and `dags test` run locally, not against a server".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Runs a `schedr workers` command (`list`; draining comes with M2.6).
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`]; [`AppError::NotImplemented`] for `drain` and
/// `resume`.
pub async fn run_workers<T: Transport>(
    command: &WorkersCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        WorkersCommand::List => {
            let workers = client.workers().await?;
            if output.json {
                return print_json(out, &workers);
            }
            let rows: Vec<Vec<String>> = workers
                .workers
                .iter()
                .map(|w| worker_row(w, &output.local))
                .collect();
            write_table(
                out,
                &["WORKER", "STATE", "RUNNING", "QUEUES", "LAST SEEN"],
                &rows,
            )
        }
        WorkersCommand::Drain { .. } | WorkersCommand::Resume { .. } => {
            Err(AppError::NotImplemented {
                command: "workers",
                milestone: "M2",
            })
        }
    }
}

fn worker_row(worker: &WorkerView, local: &TimeZone) -> Vec<String> {
    vec![
        worker.worker_id.clone(),
        worker.state.clone(),
        format!("{}/{}", worker.running, worker.slots),
        worker.queues.join(","),
        show(worker.last_heartbeat_at, local),
    ]
}

/// Runs a `schedr runs` command; warnings (e.g. `--for` read as UTC) go to `err`.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`], [`AppError::BadArgument`] for a `--for` that
/// doesn't read, [`AppError::NotImplemented`] for parameters and backfills (milestone M4).
pub async fn run_runs<T: Transport>(
    command: &RunsCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
    err: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        RunsCommand::Trigger {
            dag,
            planned_for,
            params,
            params_json,
        } => {
            if !params.is_empty() || params_json.is_some() {
                return Err(not_yet("runs trigger -p / --params", "M4"));
            }
            let at = for_time(planned_for.as_deref(), err)?;
            let command = client.wait(client.trigger(dag, at).await?).await?;
            let run_id = command
                .result
                .as_ref()
                .and_then(|r| r["run_id"].as_str())
                .unwrap_or_default()
                .to_owned();
            if output.json {
                return print_json(out, &command);
            }
            let run = client.run(&run_id).await?;
            writeln!(
                out,
                "triggered run {run_id} of {dag} for {} ({})",
                show(run.run.planned_for, &output.local),
                run.run.state
            )?;
        }
        RunsCommand::List { dag, planned_for } => {
            let at = for_time(planned_for.as_deref(), err)?;
            let runs = client.runs(dag, at).await?;
            if output.json {
                return print_json(out, &runs);
            }
            let rows: Vec<Vec<String>> = runs
                .runs
                .iter()
                .map(|run| run_row(run, &output.local))
                .collect();
            write_table(out, &["RUN", "KIND", "STATE", "FOR", "BY"], &rows)?;
        }
        RunsCommand::Cancel { run_id, backfill } => {
            let Some(run_id) = run_id.as_deref().filter(|_| backfill.is_none()) else {
                return Err(not_yet("runs cancel --backfill", "M4"));
            };
            let command = client.wait(client.cancel(run_id).await?).await?;
            let stopping = command
                .result
                .as_ref()
                .and_then(|r| r["stopping_attempts"].as_u64())
                .unwrap_or(0);
            done(
                out,
                output,
                &command,
                &format!("cancelling run {run_id} ({stopping} running task(s) being stopped)"),
            )?;
        }
        RunsCommand::Backfill { .. } => return Err(not_yet("runs backfill", "M4")),
    }
    Ok(())
}

/// `schedr tasks cancel` and `retry`: each waits for the master's decision.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`] (a rejection carries the master's reason);
/// [`AppError::NotImplemented`] for `--map-index`.
pub async fn run_tasks<T: Transport>(
    command: &TasksCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let (target, action) = match command {
        TasksCommand::Cancel(target) => (target, TaskAction::Cancel),
        TasksCommand::Retry(target) => (target, TaskAction::Retry),
    };
    if target.map_index.is_some() {
        return Err(not_yet("tasks --map-index", "M4"));
    }
    let (run_id, task_id) = (&target.run_id, &target.task_id);
    let command = client
        .wait(client.task_action(run_id, task_id, action).await?)
        .await?;
    let state = command
        .result
        .as_ref()
        .and_then(|r| r["state"].as_str())
        .unwrap_or_default();
    let message = match (action, state) {
        (TaskAction::Cancel, "stopping") => {
            format!("cancelling task {task_id} of run {run_id} (being stopped)")
        }
        (TaskAction::Cancel, _) => format!("cancelled task {task_id} of run {run_id}"),
        (TaskAction::Retry, _) => format!("queued task {task_id} of run {run_id} again"),
    };
    done(out, output, &command, &message)
}

fn for_time(text: Option<&str>, err: &mut impl Write) -> Result<Option<jiff::Timestamp>, AppError> {
    let Some(text) = text else {
        return Ok(None);
    };
    let parsed = parse_for(text).map_err(AppError::BadArgument)?;
    if let Some(warning) = parsed.warning {
        writeln!(err, "{warning}")?;
    }
    Ok(Some(parsed.at))
}

fn done(
    out: &mut impl Write,
    output: &Output,
    command: &CommandView,
    message: &str,
) -> Result<(), AppError> {
    if output.json {
        return print_json(out, command);
    }
    writeln!(out, "{message}")?;
    Ok(())
}

fn print_json(out: &mut impl Write, value: &impl serde::Serialize) -> Result<(), AppError> {
    let text =
        serde_json::to_string_pretty(value).map_err(|e| AppError::BadArgument(e.to_string()))?;
    writeln!(out, "{text}")?;
    Ok(())
}

const fn not_yet(command: &'static str, milestone: &'static str) -> AppError {
    AppError::NotImplemented { command, milestone }
}

fn dag_rows(dags: &[DagView], local: &TimeZone) -> Vec<Vec<String>> {
    dags.iter()
        .map(|dag| {
            let place = dag.conflict.as_ref().map_or_else(
                || {
                    dag.location
                        .as_ref()
                        .map_or_else(String::new, |l| format!("{}:{}", l.root, l.path))
                },
                |places| format!("CONFLICT: {} places", places.len()),
            );
            vec![
                dag.dag_id.clone(),
                if dag.paused { "yes" } else { "no" }.to_owned(),
                dag.trigger
                    .as_ref()
                    .map_or_else(String::new, |t| trigger_text(t, local)),
                place,
            ]
        })
        .collect()
}

/// A trigger as people write it: `0 3 * * * (Europe/Warsaw)`, `every 1h, catchup`,
/// `@daily, 2025-12-31 23:00Z (local:+1h)..` (its window, either end open), `once at …`,
/// `-`; times in UTC with
/// `local`'s offset.
fn trigger_text(trigger: &serde_json::Value, local: &TimeZone) -> String {
    let time = |key: &str| {
        trigger[key]
            .as_str()
            .and_then(|text| text.parse::<Timestamp>().ok())
            .map(|at| show_short(at, local))
    };
    let text = match trigger["kind"].as_str() {
        Some("cron") => {
            let expression = trigger["expression"].as_str().unwrap_or_default();
            match trigger["timezone"].as_str() {
                Some("UTC") | None => expression.to_owned(),
                Some(zone) => format!("{expression} ({zone})"),
            }
        }
        Some("every") => {
            let ms = trigger["interval_ms"].as_u64().unwrap_or(0);
            format!(
                "every {:#}",
                jiff::SignedDuration::from_millis(i64::try_from(ms).unwrap_or(i64::MAX))
            )
        }
        Some("once") => format!("once at {}", time("at").unwrap_or_default()),
        _ => "-".to_owned(),
    };
    let mut parts = vec![text];
    if trigger["catchup"].as_bool() == Some(true) {
        parts.push("catchup".to_owned());
    }
    match (time("start"), time("end")) {
        (None, None) => {}
        (start, end) => parts.push(format!(
            "{}..{}",
            start.unwrap_or_default(),
            end.unwrap_or_default()
        )),
    }
    parts.join(", ")
}

fn run_row(run: &RunView, local: &TimeZone) -> Vec<String> {
    vec![
        run.run_id.clone(),
        run.kind.clone(),
        run.state.clone(),
        show(run.planned_for, local),
        run.triggered_by.clone(),
    ]
}

/// Writes rows under headers in aligned columns (two spaces apart); no trailing spaces.
pub(crate) fn write_table(
    out: &mut impl Write,
    headers: &[&str],
    rows: &[Vec<String>],
) -> Result<(), AppError> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let line = |cells: Vec<&str>| -> String {
        let mut text = String::new();
        for (i, (cell, width)) in cells.iter().zip(&widths).enumerate() {
            if i > 0 {
                text.push_str("  ");
            }
            text.push_str(cell);
            if i + 1 < cells.len() {
                text.push_str(&" ".repeat(width - cell.chars().count()));
            }
        }
        text
    };
    writeln!(out, "{}", line(headers.to_vec()))?;
    for row in rows {
        writeln!(out, "{}", line(row.iter().map(String::as_str).collect()))?;
    }
    Ok(())
}
