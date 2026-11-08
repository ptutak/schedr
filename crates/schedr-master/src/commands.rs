//! Applying commands: the master reads pending command rows, oldest first, and applies each
//! through the scheduler and the dispatcher (see PROJECT.md, Web UI and CLI).
//!
//! A command that can't be applied (an unknown DAG or run, a run already finished, `--for` in
//! the future, a payload that doesn't read) is **rejected with a reason**; an applied one
//! records what it produced (the run a trigger created, the runs a pause cancelled). A database
//! failure leaves the command pending: the next pass tries again.

use std::time::Duration;

use jiff::Timestamp;
use schedr_db::commands::PendingCommand;
use schedr_db::{Database, DbError};
use schedr_model::commands::AskedEvent;
use schedr_model::commands::{
    CONNECTION_CHECK_TIMEOUT_MS, Command, CommandStatus, ConnectionJobResult, ConnectionsResult,
    JobState, MAX_CONNECTION_TEST_MS,
};
use schedr_model::{DagId, RunState, TaskId, TaskInstanceState};
use schedr_proto::pb;
use schedr_proto::pb::master_message;
use serde_json::json;

use crate::dispatch::{DispatchError, Dispatcher, Outgoing};
use crate::execution::{ExecutionError, TaskCancel, TaskRetry};
use crate::metrics::{CommandLabels, MasterMetrics};
use crate::quiet::WakeReason;
use crate::scheduler::{Scheduler, SchedulerError};

/// The most commands a round applies (about 8 ms each in stress runs): a burst (101 triggers
/// at once, about 0.85 s) goes over a few rounds, the oldest first (B18).
pub const COMMANDS_PER_ROUND: u32 = 50;
#[cfg(test)]
mod tests;

/// Why applying commands stopped (the command stays pending).
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The dispatcher failed (a database failure while cancelling).
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

/// How a command was decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decided {
    /// The command's ID.
    pub id: String,
    /// `applied` or `rejected`.
    pub status: CommandStatus,
    /// Why it was rejected.
    pub reason: Option<String>,
    /// What it produced, when applied.
    pub result: Option<serde_json::Value>,
}

/// What a pass over the pending commands did.
#[derive(Debug, Default)]
pub struct Applied {
    /// The commands decided, in order.
    pub decided: Vec<Decided>,
    /// Messages for workers (cancelled attempts).
    pub outgoing: Vec<Outgoing>,
    /// Workers drained (`true`) or resumed (`false`) for maintenance.
    pub drained: Vec<(String, bool)>,
    /// Connection jobs sent, by job ID, with their worker (lost with it) and how long it has
    /// to answer.
    pub connection_jobs: Vec<SentJob>,
}

/// A connections job sent to a worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentJob {
    /// Its ID.
    pub job_id: String,
    /// Its worker.
    pub worker_id: String,
    /// How long its worker has to do it (an answer may take a margin more).
    pub allowed: Duration,
}

/// The outcome of one command before it is stored.
enum Outcome {
    Applied(serde_json::Value),
    Rejected(String),
}

/// Applies command rows.
#[derive(Debug, Clone)]
pub struct Commander {
    db: Database,
    metrics: MasterMetrics,
}

impl Commander {
    /// A commander over `db`.
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            metrics: MasterMetrics::default(),
        }
    }

    /// Counts decisions and their latency in `metrics`.
    pub fn set_metrics(&mut self, metrics: MasterMetrics) {
        self.metrics = metrics;
    }

    /// Applies every pending command, oldest first.
    ///
    /// # Errors
    ///
    /// [`CommandError`] if the database fails; the command being applied stays pending, the
    /// ones decided before it stay decided.
    pub async fn apply_pending(
        &self,
        scheduler: &Scheduler,
        dispatcher: &mut Dispatcher,
        now: Timestamp,
    ) -> Result<Applied, CommandError> {
        let mut applied = Applied::default();
        self.apply_into(scheduler, dispatcher, &mut applied, now)
            .await?;
        Ok(applied)
    }

    /// As [`Self::apply_pending`], into `applied`: when a command fails, what the ones before
    /// it produced (their messages to workers among it) is there still.
    ///
    /// # Errors
    ///
    /// As [`Self::apply_pending`].
    pub async fn apply_into(
        &self,
        scheduler: &Scheduler,
        dispatcher: &mut Dispatcher,
        applied: &mut Applied,
        now: Timestamp,
    ) -> Result<(), CommandError> {
        // At most so many a round, oldest first: a burst is applied over a few rounds rather
        // than in one long one (B18).
        // Read so many only: a backlog isn't read whole each round (B52).
        for pending in self.db.pending_commands(COMMANDS_PER_ROUND).await? {
            // Marked before its first write: should this round fail before the decision is
            // stored, the next application knows it may find its own work done (B7).
            if !pending.applying {
                self.db.mark_applying(&pending.id, now).await?;
            }
            let sent_before = (applied.outgoing.len(), applied.connection_jobs.len());
            let outcome = self
                .apply(&pending, scheduler, dispatcher, applied, now)
                .await?;
            let (status, reason, result) = match outcome {
                Outcome::Applied(result) => (CommandStatus::Applied, None, Some(result)),
                Outcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason), None),
            };
            if let Err(error) = self
                .db
                .decide_command(&pending.id, status, reason.as_deref(), result.as_ref(), now)
                .await
            {
                // A connections job is sent with its decision only: applied again next round,
                // it would run twice (the worker can't tell).
                if matches!(
                    pending.command,
                    Ok(Command::CheckConnections { .. } | Command::TestConnection { .. })
                ) {
                    applied.outgoing.truncate(sent_before.0);
                    applied.connection_jobs.truncate(sent_before.1);
                }
                return Err(error.into());
            }
            self.count(&pending, status, now);
            applied.decided.push(Decided {
                id: pending.id,
                status,
                reason,
                result,
            });
        }
        Ok(())
    }

    fn count(&self, pending: &PendingCommand, status: CommandStatus, now: Timestamp) {
        let kind = pending.command.as_ref().map_or("unreadable", Command::kind);
        self.metrics
            .commands
            .get_or_create(&CommandLabels {
                kind: kind.to_owned(),
                decision: match status {
                    CommandStatus::Applied => "applied",
                    CommandStatus::Rejected => "rejected",
                    CommandStatus::Pending => "pending",
                }
                .to_owned(),
            })
            .inc();
        let waited = now.duration_since(pending.requested_at).as_secs_f64();
        self.metrics.command_latency.observe(waited.max(0.0));
    }

    /// The event a manual run asks for; why not as the error, for a key unknown or data that the
    /// trigger's own events couldn't have.
    async fn asked_event(
        &self,
        dag_id: &DagId,
        asked: Option<&AskedEvent>,
    ) -> Result<Result<Option<schedr_model::RunEvent>, String>, CommandError> {
        let (key, data, at) = match asked {
            None => return Ok(Ok(None)),
            Some(AskedEvent::Stored { key }) => {
                let found = self.db.stored_event(dag_id.as_str(), key).await?;
                return Ok(found.map_or_else(
                    || Err(format!("no event `{key}` of DAG `{dag_id}`")),
                    |event| Ok(Some(event)),
                ));
            }
            Some(AskedEvent::Given { key, data, at }) => (key, data, *at),
        };
        if let Err(why) = schedr_model::trigger::check_event_key(key) {
            return Ok(Err(why));
        }
        let stored = self.db.stored_dag(dag_id).await?;
        let limit =
            stored
                .and_then(|dag| dag.current)
                .and_then(|current| match &current.dag.trigger {
                    schedr_model::Trigger::Event { task, .. } => {
                        Some(task.settings.max_output_size)
                    }
                    _ => None,
                });
        let size = serde_json::to_vec(data).map_or(0, |bytes| bytes.len());
        if limit.is_some_and(|limit| u64::try_from(size).unwrap_or(u64::MAX) > limit) {
            return Ok(Err(format!(
                "the event's data is {size} bytes, over its trigger's `max_output_size`"
            )));
        }
        Ok(Ok(Some(schedr_model::RunEvent {
            key: key.clone(),
            data: data.clone(),
            at,
        })))
    }

    /// Starts a DAG's trigger again; a DAG without one is refused.
    async fn restart_trigger(&self, dag_id: &DagId) -> Result<Outcome, CommandError> {
        let Some(dag) = self.db.stored_dag(dag_id).await? else {
            return Ok(no_dag(dag_id));
        };
        let event = dag
            .current
            .is_some_and(|current| current.dag.trigger.is_event());
        if !event {
            return Ok(Outcome::Rejected(format!(
                "DAG `{dag_id}` has no event trigger"
            )));
        }
        self.db.restart_trigger(dag_id.as_str()).await?;
        Ok(Outcome::Applied(json!({})))
    }

    /// Passes the run an `@trigger(wait_for=...)` was given (decision 353); anything else is
    /// refused.
    async fn skip_trigger_run(
        &self,
        dag_id: &DagId,
        pending: &PendingCommand,
    ) -> Result<Outcome, CommandError> {
        let Some(dag) = self.db.stored_dag(dag_id).await? else {
            return Ok(no_dag(dag_id));
        };
        let Some(current) = dag.current else {
            return Ok(no_dag(dag_id));
        };
        // The command's ID in it: recovery matches this command's skip, never an earlier one
        // (Grok's review).
        let why = format!(
            "skipped by {} (command {})",
            pending.requested_by, pending.id
        );
        let passed =
            crate::core::triggers::pass_handed_run(&self.db, &current.dag, &why, None).await?;
        Ok(match passed {
            Ok(run) => Outcome::Applied(json!({ "run_id": run })),
            Err(refused) => {
                // Applied in a round that failed before its decision was stored (Codex's
                // review): what it passed is the trigger's last error.
                let record = self.db.trigger(dag_id.as_str()).await?;
                let done = record
                    .and_then(|r| r.last_error)
                    .filter(|_| pending.applying)
                    .and_then(|error| {
                        let rest = error.strip_prefix("passed run ")?;
                        let (run, said) = rest.split_once(": ")?;
                        (said == why).then(|| run.to_owned())
                    });
                match done {
                    Some(run) => Outcome::Applied(json!({ "run_id": run })),
                    None => Outcome::Rejected(refused),
                }
            }
        })
    }

    /// Forgets a wait step's strikes; anything else named is refused, as it would look done.
    async fn switch_on(&self, dag_id: &DagId, sensor: &TaskId) -> Result<Outcome, CommandError> {
        let Some(dag) = self.db.stored_dag(dag_id).await? else {
            return Ok(no_dag(dag_id));
        };
        let is_sensor = dag.current.is_some_and(|current| {
            let trigger = matches!(
                &current.dag.trigger,
                schedr_model::Trigger::Event { task, .. } if &task.id == sensor
            );
            trigger
                || current
                    .dag
                    .task(sensor)
                    .is_some_and(|task| matches!(task.kind, schedr_model::TaskKind::Sensor { .. }))
        });
        if !is_sensor {
            return Ok(Outcome::Rejected(format!(
                "no wait step `{sensor}` in DAG `{dag_id}`"
            )));
        }
        self.db
            .clear_wait_step(dag_id.as_str(), sensor.as_str())
            .await?;
        Ok(Outcome::Applied(json!({})))
    }

    async fn apply(
        &self,
        pending: &PendingCommand,
        scheduler: &Scheduler,
        dispatcher: &mut Dispatcher,
        applied: &mut Applied,
        now: Timestamp,
    ) -> Result<Outcome, CommandError> {
        let outgoing = &mut applied.outgoing;
        let command = match &pending.command {
            Ok(command) => command,
            Err(reason) => return Ok(Outcome::Rejected(format!("invalid command: {reason}"))),
        };
        // A command about a DAG wakes it if quiet (unpaused, it may have runs to create).
        if let Command::TriggerRun { dag_id, .. }
        | Command::PauseDag { dag_id, .. }
        | Command::UnpauseDag { dag_id } = command
        {
            scheduler.wakes().wake(dag_id, WakeReason::Command);
        }
        match command {
            Command::TriggerRun {
                dag_id,
                planned_for,
                params,
                event,
            } => {
                let requested = (*planned_for, params.as_ref(), event.as_ref());
                self.trigger(pending, scheduler, dag_id, requested, now)
                    .await
            }
            Command::RestartTrigger { dag_id } => self.restart_trigger(dag_id).await,
            Command::SkipTriggerRun { dag_id } => self.skip_trigger_run(dag_id, pending).await,
            Command::CancelRun { run_id } => {
                let Some(run) = self.db.run_tasks(run_id).await? else {
                    return Ok(Outcome::Rejected(format!("no run `{run_id}`")));
                };
                if run.state.is_final() {
                    if pending.applying && run.state == RunState::Cancelled {
                        // Cancelled by this command before its decision was stored.
                        return Ok(Outcome::Applied(json!({"stopping_attempts": 0})));
                    }
                    return Ok(Outcome::Rejected(format!(
                        "run `{run_id}` already finished ({})",
                        run.state
                    )));
                }
                let stopping = cancel(dispatcher, run_id, outgoing, now).await?;
                Ok(Outcome::Applied(json!({"stopping_attempts": stopping})))
            }
            Command::PauseDag {
                dag_id,
                cancel_runs,
            } => {
                if !self.db.set_paused_at(dag_id, true, now).await? {
                    return Ok(no_dag(dag_id));
                }
                let mut cancelled = Vec::new();
                if *cancel_runs {
                    for run_id in self.db.runs_in_progress(dag_id).await? {
                        cancel(dispatcher, &run_id, outgoing, now).await?;
                        cancelled.push(run_id);
                    }
                }
                Ok(Outcome::Applied(json!({"cancelled_runs": cancelled})))
            }
            Command::UnpauseDag { dag_id } => {
                if !self.db.set_paused_at(dag_id, false, now).await? {
                    return Ok(no_dag(dag_id));
                }
                Ok(Outcome::Applied(json!({})))
            }
            Command::SwitchOnSensor { dag_id, sensor } => self.switch_on(dag_id, sensor).await,
            Command::CancelTask {
                run_id,
                task_id,
                map_index,
            } => {
                let instance = (task_id, *map_index);
                cancel_task(dispatcher, pending, run_id, instance, outgoing, now).await
            }
            Command::RetryTask {
                run_id,
                task_id,
                map_index,
            } => retry_task(dispatcher, pending, run_id, (task_id, *map_index), now).await,
            Command::DrainWorker { worker_id } => {
                if !self.db.set_worker_drained(worker_id, true).await? {
                    return Ok(no_worker(worker_id));
                }
                outgoing.extend(dispatcher.drain(worker_id));
                applied.drained.push((worker_id.clone(), true));
                let running = dispatcher
                    .workers()
                    .get(worker_id)
                    .map_or(0, |w| w.attempts.len() + w.wait_steps.len());
                Ok(Outcome::Applied(json!({"running": running})))
            }
            Command::ResumeWorker { worker_id } => {
                self.resume(pending, worker_id, dispatcher, applied, now)
                    .await
            }
            Command::CheckConnections { .. } | Command::TestConnection { .. } => Ok(
                connections_command(dispatcher, &pending.id, command, applied),
            ),
        }
    }
}

/// `check_connections` and `test_connection` (decisions 174, 193): their jobs sent.
fn connections_command(
    dispatcher: &Dispatcher,
    command_id: &str,
    command: &Command,
    applied: &mut Applied,
) -> Outcome {
    match command {
        Command::CheckConnections { root, name, worker } => check_connections(
            dispatcher,
            command_id,
            (root.as_deref(), name.as_deref(), worker.as_deref()),
            applied,
        ),
        Command::TestConnection {
            name,
            root,
            worker,
            timeout_ms,
        } => test_connection(
            dispatcher,
            command_id,
            (name, root.as_deref(), worker.as_deref(), *timeout_ms),
            applied,
        ),
        _ => Outcome::Rejected(format!("`{}` isn't a connections command", command.kind())),
    }
}

/// `check_connections`: a job per root to a worker having its current code (decision 193).
fn check_connections(
    dispatcher: &Dispatcher,
    command_id: &str,
    (root, name, worker): (Option<&str>, Option<&str>, Option<&str>),
    applied: &mut Applied,
) -> Outcome {
    let roots = match root {
        Some(root) => vec![root.to_owned()],
        None => dispatcher.connection_roots(),
    };
    if roots.is_empty() {
        return Outcome::Rejected("no root defines connections".to_owned());
    }
    let mut jobs = Vec::new();
    for root in roots {
        let Some(registry) = dispatcher.registry(&root) else {
            return Outcome::Rejected(format!("root `{root}` defines no connections"));
        };
        let registry = match name {
            Some(name)
                if !registry.connections.contains_key(name)
                    && !registry.refused.contains_key(name) =>
            {
                return Outcome::Rejected(format!("root `{root}` defines no connection `{name}`"));
            }
            Some(name) => registry.only(&[name.to_owned()]),
            None => (**registry).clone(),
        };
        let (worker, hash) = match dispatcher.connection_worker(&root, worker) {
            Ok(found) => found,
            Err(why) => return Outcome::Rejected(why),
        };
        jobs.push(connection_job(
            command_id, &root, &worker, &hash, &registry, None,
        ));
    }
    let allowed = Duration::from_millis(CONNECTION_CHECK_TIMEOUT_MS);
    Outcome::Applied(sent(jobs, allowed, applied))
}

/// `test_connection`: one job to a worker having the root's current code (decisions 193,
/// 198-200), the root the one defining the connection unless given.
fn test_connection(
    dispatcher: &Dispatcher,
    command_id: &str,
    (name, root, worker, timeout_ms): (&str, Option<&str>, Option<&str>, u64),
    applied: &mut Applied,
) -> Outcome {
    if timeout_ms == 0 || timeout_ms > MAX_CONNECTION_TEST_MS {
        return Outcome::Rejected(format!(
            "a test's timeout is 1 ms to {} s",
            MAX_CONNECTION_TEST_MS / 1000
        ));
    }
    let defining: Vec<String> = dispatcher
        .connection_roots()
        .into_iter()
        .filter(|r| root.is_none_or(|given| given == r))
        .filter(|r| {
            dispatcher.registry(r).is_some_and(|registry| {
                registry.connections.contains_key(name) || registry.refused.contains_key(name)
            })
        })
        .collect();
    let root = match defining.as_slice() {
        [root] => root.clone(),
        [] => {
            return Outcome::Rejected(match root {
                Some(root) => format!("root `{root}` defines no connection `{name}`"),
                None => format!("no root defines connection `{name}`"),
            });
        }
        several => {
            return Outcome::Rejected(format!(
                "connection `{name}` is defined in roots {}: give one (`--root`)",
                several.join(", ")
            ));
        }
    };
    let registry = dispatcher
        .registry(&root)
        .map(|registry| registry.only(&[name.to_owned()]))
        .unwrap_or_default();
    if let Some(why) = registry.refused.get(name) {
        return Outcome::Rejected(why.clone());
    }
    let (worker, hash) = match dispatcher.connection_worker(&root, worker) {
        Ok(found) => found,
        Err(why) => return Outcome::Rejected(why),
    };
    let test = pb::ConnectionTest {
        name: name.to_owned(),
        timeout_ms,
    };
    let job = connection_job(command_id, &root, &worker, &hash, &registry, Some(test));
    let allowed = Duration::from_millis(timeout_ms);
    Outcome::Applied(sent(vec![job], allowed, applied))
}

/// The result of a connections command once its jobs are sent: one running job per root; the
/// messages and the jobs (lost with their worker) into `applied`.
fn sent(
    jobs: Vec<(Outgoing, ConnectionJobResult)>,
    allowed: Duration,
    applied: &mut Applied,
) -> serde_json::Value {
    let mut result = ConnectionsResult::default();
    for (outgoing, job) in jobs {
        applied.connection_jobs.push(SentJob {
            job_id: job_id(&outgoing),
            worker_id: outgoing.worker_id.clone(),
            allowed,
        });
        applied.outgoing.push(outgoing);
        result.jobs.push(job);
    }
    serde_json::to_value(result).unwrap_or_default()
}

/// A connections job's ID: its command's and its root, so an answer finds its place in the
/// command's result whatever the master remembers.
#[must_use]
pub fn connection_job_id(command_id: &str, root: &str) -> String {
    format!("{command_id}/{root}")
}

/// The command and the root a job ID names.
#[must_use]
pub fn parse_connection_job_id(job_id: &str) -> Option<(&str, &str)> {
    job_id.split_once('/')
}

fn job_id(outgoing: &Outgoing) -> String {
    match &outgoing.message.message {
        Some(master_message::Message::ConnectionJob(job)) => job.job_id.clone(),
        _ => String::new(),
    }
}

fn connection_job(
    command_id: &str,
    root: &str,
    worker: &str,
    hash: &str,
    registry: &schedr_model::connection::Registry,
    test: Option<pb::ConnectionTest>,
) -> (Outgoing, ConnectionJobResult) {
    let message = pb::ConnectionJob {
        job_id: connection_job_id(command_id, root),
        root: Some(pb::RootHash {
            kind: pb::RootKind::Dag.into(),
            root: root.to_owned(),
            content_hash: hash.to_owned(),
        }),
        connections_json: serde_json::to_vec(registry).unwrap_or_default(),
        test,
    };
    (
        Outgoing {
            worker_id: worker.to_owned(),
            message: pb::MasterMessage {
                message: Some(master_message::Message::ConnectionJob(message)),
            },
        },
        ConnectionJobResult {
            root: root.to_owned(),
            worker: worker.to_owned(),
            state: JobState::Running,
            reason: None,
            connections: Vec::new(),
        },
    )
}

impl Commander {
    /// `trigger_run`: a manual run whose ID is the command's, so applying it again finds the
    /// same run, looked up first, before any check that could now fail (the DAG removed
    /// meanwhile).
    async fn trigger(
        &self,
        pending: &PendingCommand,
        scheduler: &Scheduler,
        dag_id: &DagId,
        (planned_for, params, asked): (
            Option<Timestamp>,
            Option<&serde_json::Map<String, serde_json::Value>>,
            Option<&AskedEvent>,
        ),
        now: Timestamp,
    ) -> Result<Outcome, CommandError> {
        if pending.applying && self.db.run_tasks(&pending.id).await?.is_some() {
            return Ok(Outcome::Applied(json!({"run_id": pending.id})));
        }
        let event = match self.asked_event(dag_id, asked).await? {
            Ok(event) => event,
            Err(why) => return Ok(Outcome::Rejected(why)),
        };
        let created = scheduler
            .trigger_as(
                Some(&pending.id),
                dag_id,
                planned_for,
                (params, event.as_ref()),
                &pending.requested_by,
                now,
            )
            .await;
        match created {
            Ok(run) => Ok(Outcome::Applied(json!({"run_id": run.run_id}))),
            Err(SchedulerError::Db(error)) => Err(error.into()),
            Err(error) => Ok(Outcome::Rejected(error.to_string())),
        }
    }

    /// `resume_worker`. Applied again after its round failed, a worker found not drained was
    /// resumed by this command: what follows a resume is done again (harmless twice), as the
    /// first try may not have got to it.
    async fn resume(
        &self,
        pending: &PendingCommand,
        worker_id: &str,
        dispatcher: &mut Dispatcher,
        applied: &mut Applied,
        now: Timestamp,
    ) -> Result<Outcome, CommandError> {
        match self.db.resume_worker(worker_id).await? {
            None => Ok(no_worker(worker_id)),
            Some(false) if !pending.applying => Ok(Outcome::Rejected(format!(
                "worker `{worker_id}` isn't drained"
            ))),
            Some(_) => {
                dispatcher.resume(worker_id, now).await?;
                applied.drained.push((worker_id.to_owned(), false));
                Ok(Outcome::Applied(json!({})))
            }
        }
    }
}

/// Cancels one task through the dispatcher (`schedr tasks cancel`). Applied again after its
/// round failed (`pending.applying`), a task or run found already `cancelled` is its own
/// work: applied, not rejected.
async fn cancel_task(
    dispatcher: &mut Dispatcher,
    pending: &PendingCommand,
    run_id: &str,
    (task_id, map_index): (&TaskId, Option<i32>),
    outgoing: &mut Vec<Outgoing>,
    now: Timestamp,
) -> Result<Outcome, CommandError> {
    let cancelled = dispatcher
        .cancel_task(run_id, (task_id, map_index), now)
        .await;
    let (cancelled, messages) = match cancelled {
        Ok(done) => done,
        Err(DispatchError::Execution(
            unknown @ (ExecutionError::UnknownRun(_)
            | ExecutionError::UnknownTask { .. }
            | ExecutionError::UnknownInstance { .. }),
        )) => return Ok(Outcome::Rejected(unknown.to_string())),
        Err(error) => return Err(error.into()),
    };
    let stopping = messages.len();
    outgoing.extend(messages);
    Ok(match cancelled {
        TaskCancel::Cancelled(_) => Outcome::Applied(json!({"state": "cancelled"})),
        TaskCancel::Stopping => {
            Outcome::Applied(json!({"state": "stopping", "stopping_attempts": stopping}))
        }
        TaskCancel::AlreadyFinished(TaskInstanceState::Cancelled)
        | TaskCancel::RunFinished(RunState::Cancelled)
            if pending.applying =>
        {
            Outcome::Applied(json!({"state": "cancelled"}))
        }
        TaskCancel::AlreadyFinished(state) => Outcome::Rejected(format!(
            "task `{task_id}` of run `{run_id}` already finished ({state})"
        )),
        TaskCancel::RunFinished(state) => {
            Outcome::Rejected(format!("run `{run_id}` already finished ({state})"))
        }
        TaskCancel::RunQueued => Outcome::Rejected(format!(
            "run `{run_id}` hasn't started its tasks yet: cancel the run \
             (`schedr runs cancel`), or try again once it runs"
        )),
    })
}

/// Queues an `unschedulable` task again through the dispatcher (`schedr tasks retry`).
/// Applied again after its round failed, a task found `queued` is its own work (no round
/// assigned it in between: commands come first).
async fn retry_task(
    dispatcher: &mut Dispatcher,
    pending: &PendingCommand,
    run_id: &str,
    (task_id, map_index): (&TaskId, Option<i32>),
    now: Timestamp,
) -> Result<Outcome, CommandError> {
    let retried = match dispatcher
        .retry_task(run_id, (task_id, map_index), now)
        .await
    {
        Ok(retried) => retried,
        Err(DispatchError::Execution(
            unknown @ (ExecutionError::UnknownRun(_)
            | ExecutionError::UnknownTask { .. }
            | ExecutionError::UnknownInstance { .. }),
        )) => return Ok(Outcome::Rejected(unknown.to_string())),
        Err(error) => return Err(error.into()),
    };
    Ok(match retried {
        TaskRetry::Queued(_) => Outcome::Applied(json!({"state": "queued"})),
        TaskRetry::NotUnschedulable(TaskInstanceState::Queued) if pending.applying => {
            Outcome::Applied(json!({"state": "queued"}))
        }
        TaskRetry::NotUnschedulable(state) => Outcome::Rejected(format!(
            "task `{task_id}` of run `{run_id}` is {state}, not unschedulable: only an \
             unschedulable task is retried (a failed one is retried by its `retries`)"
        )),
        TaskRetry::RunFinished(state) => Outcome::Rejected(format!(
            "run `{run_id}` already finished ({state}): trigger a new run"
        )),
        TaskRetry::RunQueued => Outcome::Rejected(format!(
            "run `{run_id}` hasn't started its tasks yet: nothing to retry"
        )),
    })
}

/// Cancels a run through the dispatcher; returns how many attempts are being stopped.
async fn cancel(
    dispatcher: &mut Dispatcher,
    run_id: &str,
    outgoing: &mut Vec<Outgoing>,
    now: Timestamp,
) -> Result<usize, CommandError> {
    match dispatcher.cancel_run(run_id, now).await {
        Ok(messages) => {
            let count = messages.len();
            outgoing.extend(messages);
            Ok(count)
        }
        Err(DispatchError::Execution(ExecutionError::UnknownRun(_))) => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn no_worker(worker_id: &str) -> Outcome {
    Outcome::Rejected(format!(
        "no worker `{worker_id}` (`schedr workers list` shows the workers the master has seen)"
    ))
}

fn no_dag(dag_id: &DagId) -> Outcome {
    Outcome::Rejected(format!("no DAG `{dag_id}`"))
}
