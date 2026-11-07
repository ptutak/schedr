//! The user CLI's commands over the REST API: `schedr dags ...`, `schedr runs ...`, `schedr
//! tasks ...` and `schedr workers ...`.
//!
//! Human output is aligned columns with times in UTC and local time; `--json` prints the API's
//! JSON (UTC only). Actions wait for the master's decision and print the result, or fail with
//! the master's reason (see PROJECT.md, Web UI and CLI).

use std::io::Write;

use jiff::tz::TimeZone;
use schedr_api::views::{
    AttemptView, CheckConnectionsRequest, CommandView, DagView, RunDetail, RunView, TaskView,
    TestConnectionRequest, TriggerRequest, TriggerView, WorkerView,
};

use crate::app::AppError;
use crate::cli::{
    ConnectionsCommand, DagsCommand, LogsArgs, ResourcesCommand, RunsCommand, SensorsCommand,
    TasksCommand, TriggersCommand, WorkersCommand,
};
use crate::client::{Client, TaskAction, Transport, WorkerAction};
use crate::times::{parse_for, show, show_short, trigger_text};
use schedr_model::commands::{ConnectionsResult, JobState, MAX_CONNECTION_TEST_MS, SecretState};

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
                &["DAG", "PAUSED", "HELD", "TRIGGER", "DEFINED IN"],
                &dag_rows(&dags.dags, &output.local),
            )?;
            // Why each held DAG gets no runs, and what to do (decision 72.3).
            for held in dags.dags.iter().filter_map(|d| d.held.as_ref()) {
                writeln!(out, "\n{}", held.reason)?;
            }
        }
        DagsCommand::Pause { dag } => {
            let command = client
                .wait(
                    client
                        .send_command(|| client.set_paused(dag, true, false))
                        .await?,
                )
                .await?;
            done(out, output, &command, &format!("paused {dag}"))?;
        }
        DagsCommand::Unpause { dag } => {
            let command = client
                .wait(
                    client
                        .send_command(|| client.set_paused(dag, false, false))
                        .await?,
                )
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
/// [`AppError::Client`] as for [`run_dags`] (a rejection carries the master's reason).
pub async fn run_sensors<T: Transport>(
    command: &SensorsCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        SensorsCommand::SwitchedOff => {
            let list = client.switched_off_sensors().await?;
            if output.json {
                return print_json(out, &list);
            }
            let rows: Vec<Vec<String>> = list
                .sensors
                .iter()
                .map(|s| {
                    vec![
                        s.dag_id.clone(),
                        s.sensor.clone(),
                        s.strikes.to_string(),
                        s.switched_off_at.clone(),
                    ]
                })
                .collect();
            write_table(out, &["DAG", "SENSOR", "STRIKES", "SWITCHED OFF"], &rows)?;
            Ok(())
        }
        SensorsCommand::SwitchOn { dag, sensor } => {
            let command = client
                .wait(
                    client
                        .send_command(|| client.switch_on_sensor(dag, sensor))
                        .await?,
                )
                .await?;
            if output.json {
                return print_json(out, &command);
            }
            writeln!(out, "switched on {dag} {sensor}")?;
            Ok(())
        }
    }
}

/// `schedr triggers ...`.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`] (a rejection carries the master's reason).
pub async fn run_triggers<T: Transport>(
    command: &TriggersCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    match command {
        TriggersCommand::List => {
            let list = client.triggers().await?;
            if output.json {
                return print_json(out, &list);
            }
            write_table(
                out,
                &[
                    "DAG",
                    "TRIGGER",
                    "STATE",
                    "WORKER",
                    "BOOKMARK",
                    "FAILURES",
                    "LAST ERROR",
                ],
                &list.triggers.iter().map(trigger_row).collect::<Vec<_>>(),
            )?;
        }
        TriggersCommand::Show { dag } => {
            let trigger = client.dag_trigger(dag).await?;
            if output.json {
                return print_json(out, &trigger);
            }
            write_table(
                out,
                &[
                    "DAG",
                    "TRIGGER",
                    "STATE",
                    "WORKER",
                    "BOOKMARK",
                    "FAILURES",
                    "LAST ERROR",
                ],
                &[trigger_row(&trigger)],
            )?;
            for (what, at) in [
                ("started", trigger.started_at),
                ("last event", trigger.last_event_at),
                ("last poked", trigger.last_poked_at),
                ("last idle restart", trigger.last_restart_at),
                ("not before", trigger.not_before),
            ] {
                if let Some(at) = at {
                    writeln!(out, "{what}: {at}")?;
                }
            }
            if let Some(why) = &trigger.stopped_why {
                writeln!(out, "stopped: {}", one_line(why))?;
            }
        }
        TriggersCommand::Events { dag, limit } => {
            let list = client.trigger_events(dag, *limit).await?;
            if output.json {
                return print_json(out, &list);
            }
            let rows: Vec<Vec<String>> = list
                .events
                .iter()
                .map(|event| {
                    vec![
                        one_line(&event.key),
                        event.received_at.to_string(),
                        event.data.to_string(),
                    ]
                })
                .collect();
            write_table(out, &["KEY", "RECEIVED", "DATA"], &rows)?;
        }
        TriggersCommand::Restart { dag } => {
            let command = client
                .wait(client.send_command(|| client.restart_trigger(dag)).await?)
                .await?;
            if output.json {
                return print_json(out, &command);
            }
            writeln!(out, "restarting the trigger of {dag}")?;
        }
        TriggersCommand::SwitchOn { dag } => {
            let command = client
                .wait(
                    client
                        .send_command(|| client.switch_on_trigger(dag))
                        .await?,
                )
                .await?;
            if output.json {
                return print_json(out, &command);
            }
            writeln!(out, "switched on the trigger of {dag}")?;
        }
    }
    Ok(())
}

/// What a trigger said, on one line of a table and with nothing a terminal would act on.
fn one_line(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

fn trigger_row(trigger: &TriggerView) -> Vec<String> {
    let shown = |text: &Option<String>| text.as_deref().map_or_else(|| "-".to_owned(), one_line);
    vec![
        trigger.dag_id.clone(),
        trigger.trigger.clone(),
        trigger.state.clone(),
        shown(&trigger.worker),
        shown(&trigger.bookmark),
        trigger.failures.to_string(),
        shown(&trigger.last_error),
    ]
}

/// `--event-data`: JSON, or `@file.json`.
///
/// # Errors
///
/// What is wrong with it.
pub(crate) fn given_event_data(given: Option<&str>) -> Result<Option<serde_json::Value>, String> {
    let Some(given) = given else {
        return Ok(None);
    };
    let text = match given.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("--event-data: can't read `{path}`: {e}"))?,
        None => given.to_owned(),
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|e| format!("--event-data must be JSON: {e}"))
}

/// `schedr resources ...`.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`].
pub async fn run_resources<T: Transport>(
    command: &ResourcesCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let ResourcesCommand::List { run } = command;
    let list = client.run_resources(run).await?;
    if output.json {
        return print_json(out, &list);
    }
    let rows: Vec<Vec<String>> = list
        .resources
        .iter()
        .map(|r| {
            vec![
                r.setup.clone(),
                r.cleanup.clone(),
                r.state.clone(),
                r.users.join(","),
            ]
        })
        .collect();
    write_table(out, &["RESOURCE", "CLEANUP", "STATE", "USERS"], &rows)?;
    Ok(())
}

/// `schedr workers ...`.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`] (a rejection carries the master's reason).
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
                &[
                    "WORKER",
                    "STATE",
                    "RUNNING",
                    "QUEUES",
                    "LAST SEEN",
                    "WARNINGS",
                ],
                &rows,
            )?;
            // The warnings themselves under the table: too long for a column.
            for worker in &workers.workers {
                for warning in &worker.warnings {
                    writeln!(out, "{}: {}", worker.worker_id, warning.message)?;
                }
            }
            Ok(())
        }
        WorkersCommand::Drain { worker } => {
            let command = client
                .wait(
                    client
                        .send_command(|| client.worker_action(worker, WorkerAction::Drain))
                        .await?,
                )
                .await?;
            let running = command
                .result
                .as_ref()
                .and_then(|r| r["running"].as_u64())
                .unwrap_or(0);
            done(
                out,
                output,
                &command,
                &format!(
                    "draining {worker}: no new tasks; {running} running task(s) finishing \
                     (`schedr workers resume {worker}` to undo)"
                ),
            )
        }
        WorkersCommand::Resume { worker } => {
            let command = client
                .wait(
                    client
                        .send_command(|| client.worker_action(worker, WorkerAction::Resume))
                        .await?,
                )
                .await?;
            done(out, output, &command, &format!("resumed {worker}"))
        }
    }
}

/// Runs `schedr connections ...`: the command sent, then its result read back until every
/// worker answered; a table of each connection, or the result as JSON.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`]; [`AppError::Failed`] when a connection isn't
/// fine, or a worker didn't answer.
pub async fn run_connections<T: Transport>(
    command: &ConnectionsCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    // A test may take its timeout; past it and a margin, its worker won't answer.
    let (sent, wait) = match command {
        ConnectionsCommand::Check { root, name, worker } => {
            let request = CheckConnectionsRequest {
                root: root.clone(),
                name: name.clone(),
                worker: worker.clone(),
            };
            let sent = client
                .send_command(|| client.check_connections(&request))
                .await?;
            (sent, CONNECTIONS_ANSWER_MARGIN)
        }
        ConnectionsCommand::Test {
            name,
            root,
            worker,
            timeout,
        } => {
            let request = TestConnectionRequest {
                root: root.clone(),
                worker: worker.clone(),
                timeout_ms: timeout.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX)),
            };
            let sent = client
                .send_command(|| client.test_connection(name, &request))
                .await?;
            // Not given, it is the webserver's to say: the longest any test may take is waited
            // for at most, and the job's end comes first.
            let longest = std::time::Duration::from_millis(MAX_CONNECTION_TEST_MS);
            (sent, timeout.unwrap_or(longest) + CONNECTIONS_ANSWER_MARGIN)
        }
    };
    let applied = client.wait(sent).await?;
    let result = client.wait_connections(applied, wait).await?;
    if output.json {
        print_json(out, &result)?;
    } else {
        write_connections(out, &result)?;
    }
    let bad = result
        .jobs
        .iter()
        .map(|job| match job.state {
            JobState::Done => job.connections.iter().filter(|c| !c.ok).count(),
            _ => 1,
        })
        .sum::<usize>();
    if bad > 0 {
        return Err(AppError::Failed(format!(
            "{bad} connection(s) or worker(s) not fine"
        )));
    }
    Ok(())
}

/// How long past its own time a connections job's worker has to answer.
const CONNECTIONS_ANSWER_MARGIN: std::time::Duration = std::time::Duration::from_secs(60);

fn write_connections(out: &mut impl Write, result: &ConnectionsResult) -> Result<(), AppError> {
    let mut rows = Vec::new();
    let mut notes = Vec::new();
    for job in &result.jobs {
        if job.state != JobState::Done {
            rows.push(vec![
                job.root.clone(),
                "-".to_owned(),
                job.worker.clone(),
                match job.state {
                    JobState::Lost => "lost".to_owned(),
                    _ => "no answer".to_owned(),
                },
            ]);
            if let Some(reason) = &job.reason {
                notes.push(format!("{}: {reason}", job.root));
            }
            continue;
        }
        for report in &job.connections {
            let state = if report.ok { "ok" } else { "failed" };
            let state = match report.duration_ms {
                Some(ms) => format!("{state} ({}.{:01} s)", ms / 1000, ms % 1000 / 100),
                None => state.to_owned(),
            };
            rows.push(vec![
                job.root.clone(),
                report.name.clone(),
                job.worker.clone(),
                state,
            ]);
            for secret in &report.secrets {
                if secret.state != SecretState::Present {
                    let state = serde_json::to_value(secret.state)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_default();
                    let why = secret
                        .message
                        .as_deref()
                        .map(|m| format!(": {m}"))
                        .unwrap_or_default();
                    notes.push(format!(
                        "{}: secret `{}` {state}{why}",
                        report.name, secret.secret
                    ));
                }
            }
            if let Some(error) = &report.error {
                notes.push(format!("{}: {error}", report.name));
            }
        }
    }
    write_table(out, &["ROOT", "CONNECTION", "WORKER", "STATE"], &rows)?;
    for note in notes {
        writeln!(out, "{note}")?;
    }
    Ok(())
}

fn worker_row(worker: &WorkerView, local: &TimeZone) -> Vec<String> {
    vec![
        worker.worker_id.clone(),
        if worker.drained {
            format!("{} (drained)", worker.state)
        } else {
            worker.state.clone()
        },
        format!("{}/{}", worker.running, worker.slots),
        worker.queues.join(","),
        show(worker.last_heartbeat_at, local),
        worker.warnings.len().to_string(),
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
            event,
            event_key,
            event_data,
            event_at,
        } => {
            let params = given_params(params, params_json.as_deref())?;
            let event_at = for_time(event_at.as_deref(), err)?;
            let at = for_time(planned_for.as_deref(), err)?;
            let request = TriggerRequest {
                planned_for: at,
                params,
                event: event.clone(),
                event_key: event_key.clone(),
                event_data: given_event_data(event_data.as_deref())
                    .map_err(AppError::BadArgument)?,
                event_at,
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
        RunsCommand::Show { run_id } => {
            let detail = client.run(run_id).await?;
            if output.json {
                return print_json(out, &detail);
            }
            print_run(&detail, &output.local, out)?;
        }
        RunsCommand::Cancel { run_id, backfill } => {
            let Some(run_id) = run_id.as_deref().filter(|_| backfill.is_none()) else {
                return Err(not_yet("runs cancel --backfill", "M4"));
            };
            let command = client
                .wait(client.send_command(|| client.cancel(run_id)).await?)
                .await?;
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

/// `schedr tasks cancel` and `retry`: each waits for the master's decision; `logs`,
/// `attempts` and `output` read.
///
/// # Errors
///
/// [`AppError::Client`] as for [`run_dags`] (a rejection carries the master's reason).
pub async fn run_tasks<T: Transport>(
    command: &TasksCommand,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let (target, action) = match command {
        TasksCommand::Cancel(target) => (target, TaskAction::Cancel),
        TasksCommand::Retry(target) => (target, TaskAction::Retry),
        TasksCommand::Logs(args) => return print_log(args, client, output, out).await,
        TasksCommand::Attempts(target) => {
            let attempts = client
                .instance_attempts(&target.run_id, &target.task_id, target.map_index)
                .await?;
            if output.json {
                return print_json(out, &attempts);
            }
            let rows: Vec<Vec<String>> = attempts
                .attempts
                .iter()
                .map(|attempt| attempt_row(attempt, &output.local))
                .collect();
            let headers = [
                "TRY", "STATUS", "WORKER", "STARTED", "ENDED", "DURATION", "EXIT", "ERROR",
            ];
            return write_table(out, &headers, &rows);
        }
        TasksCommand::Output(target) => {
            let found = client
                .instance_output(&target.run_id, &target.task_id, target.map_index)
                .await?;
            // Text output is the value alone, ready for a pipe; `--json` adds the outcome.
            return if output.json {
                print_json(out, &found)
            } else {
                print_json(out, &found.value)
            };
        }
    };
    let (run_id, task_id) = (&target.run_id, &target.task_id);
    let instance = (task_id.as_str(), target.map_index);
    let command = client
        .wait(
            client
                .send_command(|| client.task_action(run_id, instance, action))
                .await?,
        )
        .await?;
    let task_id = shown_task(task_id, target.map_index);
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

/// `schedr tasks logs`: the attempt's log as it is, part by part (`--json`: each part as the
/// API's JSON, one per line); with `--follow`, waiting for the attempt to start and on until
/// it ends.
async fn print_log<T: Transport>(
    args: &LogsArgs,
    client: &Client<T>,
    output: &Output,
    out: &mut impl Write,
) -> Result<(), AppError> {
    let target = &args.target;
    let (run_id, task_id) = (target.run_id.as_str(), target.task_id.as_str());
    let map_index = target.map_index;
    let try_number = loop {
        let attempts = client
            .instance_attempts(run_id, task_id, map_index)
            .await?
            .attempts;
        let attempt = match args.try_number {
            Some(n) => attempts.into_iter().find(|a| a.try_number == n),
            None => attempts.into_iter().max_by_key(|a| a.try_number),
        };
        match (attempt, args.try_number) {
            // Assigned, not started yet: following waits for it.
            (Some(a), _) if args.follow && a.started_at.is_none() && a.status.is_none() => {}
            (Some(a), _) => break a.try_number,
            // The API says what is missing.
            (None, Some(n)) => break n,
            (None, None) if args.follow => {}
            (None, None) => {
                return Err(AppError::BadArgument(format!(
                    "task `{task_id}` of run `{run_id}` hasn't been tried yet: no log so far"
                )));
            }
        }
        tokio::time::sleep(client.poll()).await;
    };
    let mut from = 0;
    loop {
        let chunk = client
            .instance_log_part(
                run_id,
                (task_id, map_index),
                try_number,
                from,
                schedr_model::logs::MAX_CHUNK,
            )
            .await?;
        if output.json {
            let line =
                serde_json::to_string(&chunk).map_err(|e| AppError::BadArgument(e.to_string()))?;
            writeln!(out, "{line}")?;
        } else {
            out.write_all(chunk.content.as_bytes())?;
        }
        out.flush()?;
        let progressed = chunk.next > from;
        from = chunk.next;
        if from < chunk.size && progressed {
            continue;
        }
        if !(args.follow && chunk.running) {
            return Ok(());
        }
        tokio::time::sleep(client.poll()).await;
    }
}

/// `schedr runs show`: the run, then its tasks; a mapped task's own row (with its instance
/// count) comes before its instances, `fetch[0]`, `fetch[1]`, ... (decision 86).
fn print_run(detail: &RunDetail, local: &TimeZone, out: &mut impl Write) -> Result<(), AppError> {
    let run = &detail.run;
    writeln!(
        out,
        "run {} of {} ({}) for {}: {}",
        run.run_id,
        run.dag_id,
        run.kind,
        show(run.planned_for, local),
        run_state(run)
    )?;
    if detail.code_hashes.len() > 1 {
        // Its code changed while it ran.
        writeln!(out, "ran on code {}", detail.code_hashes.join(", "))?;
    }
    if let Some(params) = detail.params.as_object().filter(|p| !p.is_empty()) {
        // As given with `-p` (masked by the webserver, decision 134).
        let shown: Vec<String> = params
            .iter()
            .map(|(name, value)| match value {
                serde_json::Value::String(text) => format!("{name}={text}"),
                other => format!("{name}:={other}"),
            })
            .collect();
        writeln!(out, "params: {}", shown.join(" "))?;
    }
    let rows: Vec<Vec<String>> = detail.tasks.iter().map(task_row).collect();
    write_table(
        out,
        &["TASK", "STATE", "OUTCOME", "TRIES", "INSTANCES", "REASON"],
        &rows,
    )
}

fn task_row(task: &TaskView) -> Vec<String> {
    let index = u32::try_from(task.map_index).ok();
    vec![
        shown_task(&task.task_id, index),
        task.state.clone(),
        task.outcome.clone().unwrap_or_default(),
        task.try_number.to_string(),
        task.instances.map_or_else(String::new, |n| n.to_string()),
        task.reason.clone().unwrap_or_default(),
    ]
}

fn attempt_row(attempt: &AttemptView, local: &TimeZone) -> Vec<String> {
    let status = attempt.status.clone().unwrap_or_else(|| {
        if attempt.started_at.is_some() {
            "running"
        } else {
            "assigned"
        }
        .to_owned()
    });
    let time = |at: Option<jiff::Timestamp>| at.map_or_else(String::new, |t| show_short(t, local));
    vec![
        attempt.try_number.to_string(),
        status,
        attempt.worker_id.clone().unwrap_or_default(),
        time(attempt.started_at),
        time(attempt.ended_at),
        // Measured by the worker; the timestamps are the master's.
        attempt.duration_ms.map_or_else(String::new, seconds),
        attempt
            .exit_code
            .map_or_else(String::new, |c| c.to_string()),
        attempt_error(attempt),
    ]
}

/// A duration measured in milliseconds, in seconds rounded to a tenth (half up): `0.1s` for
/// 99 ms, `0.3s` for 250 ms. `tasks attempts` and `dags test` both show it so.
pub(crate) fn seconds(ms: u64) -> String {
    let tenths = (ms + 50) / 100;
    format!("{}.{}s", tenths / 10, tenths % 10)
}

/// What went wrong, in a cell: the error kind schedr recognised, else the task's own
/// exception and the first line of its message (the whole summary is in `--json`).
fn attempt_error(attempt: &AttemptView) -> String {
    const WIDTH: usize = 60;
    if let Some(kind) = &attempt.error_kind {
        return kind.clone();
    }
    let Some(summary) = &attempt.error_summary else {
        return String::new();
    };
    let exception = summary["exception"].as_str().unwrap_or_default();
    let message = summary["message"]
        .as_str()
        .and_then(|m| m.lines().next())
        .unwrap_or_default();
    let text = match (exception.is_empty(), message.is_empty()) {
        (false, false) => format!("{exception}: {message}"),
        (false, true) => exception.to_owned(),
        _ => message.to_owned(),
    };
    if text.chars().count() <= WIDTH {
        return text;
    }
    let cut: String = text.chars().take(WIDTH - 1).collect();
    format!("{cut}…")
}

/// A task as shown: `fetch`, or `fetch[2]` for an instance of a mapped task (decision 86).
fn shown_task(task_id: &str, map_index: Option<u32>) -> String {
    map_index.map_or_else(|| task_id.to_owned(), |i| format!("{task_id}[{i}]"))
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

/// A run's parameters from `-p` fields (`key=value` a text, `key:=json` any JSON value, the
/// field syntax of the in-task CLI) or `--params` (one JSON object); `None` when none given.
///
/// # Errors
///
/// [`AppError::BadArgument`] for a field without `=`, an empty or repeated name, JSON that
/// doesn't read, or `--params` that isn't an object.
pub fn given_params(
    fields: &[String],
    json: Option<&str>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, AppError> {
    if let Some(json) = json {
        return match serde_json::from_str(json) {
            Ok(serde_json::Value::Object(params)) => Ok(Some(params)),
            Ok(_) => Err(AppError::BadArgument(
                "--params takes a JSON object, e.g. '{\"region\": \"us\"}'".to_owned(),
            )),
            Err(e) => Err(AppError::BadArgument(format!("--params isn't JSON: {e}"))),
        };
    }
    if fields.is_empty() {
        return Ok(None);
    }
    // A refusal names the field by its number, or by its name masked, never by its value: it
    // may be a secret, mistyped.
    let mut params = serde_json::Map::new();
    for (n, field) in fields.iter().enumerate().map(|(i, f)| (i + 1, f)) {
        let Some((name, value)) = field.split_once('=') else {
            return Err(AppError::BadArgument(format!(
                "-p #{n}: give `name=text` or `name:=json`"
            )));
        };
        let json_name = name.strip_suffix(':');
        let name = json_name.unwrap_or(name);
        if name.is_empty() {
            return Err(AppError::BadArgument(format!(
                "-p #{n}: no name before `=`"
            )));
        }
        let value = match json_name {
            Some(_) => serde_json::from_str(value).map_err(|e| {
                AppError::BadArgument(format!(
                    "-p {}: the value isn't JSON (line {}, column {}); text is `name=...`",
                    schedr_config::masked::mask_text(name),
                    e.line(),
                    e.column()
                ))
            })?,
            None => serde_json::Value::String(value.to_owned()),
        };
        if params.insert(name.to_owned(), value).is_some() {
            return Err(AppError::BadArgument(format!(
                "-p: `{}` is given twice",
                schedr_config::masked::mask_text(name)
            )));
        }
    }
    Ok(Some(params))
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
                // Held while its file fails to import (decision 72): the error's class (the
                // message is on the UI's Import errors page).
                dag.held
                    .as_ref()
                    .map_or_else(|| "no".to_owned(), |h| format!("yes: {}", h.exception)),
                dag.trigger
                    .as_ref()
                    .map_or_else(String::new, |t| trigger_text(t, local)),
                place,
            ]
        })
        .collect()
}

/// A run's state, with why a `success` turned yellow after its end (decision 309):
/// `success (late_cleanup_failed)`.
fn run_state(run: &RunView) -> String {
    match &run.late_failure {
        Some(late) => format!("{} ({late})", run.state),
        None => run.state.clone(),
    }
}

fn run_row(run: &RunView, local: &TimeZone) -> Vec<String> {
    vec![
        run.run_id.clone(),
        run.kind.clone(),
        run_state(run),
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
        // Empty cells at the end of a row leave no padding behind.
        text.truncate(text.trim_end().len());
        text
    };
    writeln!(out, "{}", line(headers.to_vec()))?;
    for row in rows {
        writeln!(out, "{}", line(row.iter().map(String::as_str).collect()))?;
    }
    Ok(())
}
