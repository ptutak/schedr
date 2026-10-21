//! Giving tasks to workers and handling what workers report about them (see PROJECT.md, Task
//! assignment and Task rejections).
//!
//! - **Assign:** ready tasks, oldest ready first, each to the worker chosen by [`Workers::pick`];
//!   the assignment is stored before `AssignTask` is sent.
//! - **`Started`** means accepted. For an attempt that was taken back or ended, the worker is
//!   told to kill it.
//! - **`Rejected`** takes the assignment back (it isn't a try): a temporary reason queues the
//!   task again at once; "code not there yet" after [`CODE_RETRY_DELAY`]; a worker-specific
//!   reason leaves that worker out for the task, which becomes `unschedulable` when every
//!   eligible worker rejected it, until the set of workers changes.
//! - **`Finished`** is stored (the first one counts) and always acknowledged.
//! - An attempt with no `Started` within [`START_TIMEOUT`] ends `assignment_expired` and the
//!   task runs again without using a retry; a lost worker's attempts end `lost`.
//!
//! The dispatcher only builds messages; the gRPC server sends them (milestone 1.14).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::attempts::{AttemptEnd, ReadyInstance, StoredAttempt};
use schedr_db::{Database, DbError};
use schedr_model::{AttemptStatus, Reference, TaskId, TaskInstanceState};
use schedr_proto::convert::{
    RejectReaction, attempt_status_from_proto, reject_reaction, run_kind_to_proto,
};
use schedr_proto::pb;

use crate::execution::{AttemptResult, Execution, ExecutionError};
use crate::workers::Workers;

#[cfg(test)]
mod tests;

/// How long a worker has to report `Started` after an assignment.
pub const START_TIMEOUT: SignedDuration = SignedDuration::from_secs(10);

/// When a task whose code wasn't on the worker yet is tried again: about the git-sync
/// interval plus the settle time.
pub const CODE_RETRY_DELAY: SignedDuration = SignedDuration::from_secs(35);

/// Inputs up to this size travel inside `AssignTask`; bigger ones are fetched separately.
pub const INLINE_INPUTS_LIMIT: usize = 4 * 1024 * 1024;

/// Inputs over this size make the task fail (`inputs_too_large`, never retried).
pub const MAX_INPUTS: usize = 64 * 1024 * 1024;

/// The error kind of a task whose inputs are over [`MAX_INPUTS`].
pub const INPUTS_TOO_LARGE: &str = "inputs_too_large";

/// The grace time between SIGTERM and SIGKILL when an attempt is cancelled (master
/// configuration; this is the default).
pub const DEFAULT_CANCEL_GRACE: Duration = Duration::from_secs(30);

/// A message for one worker.
#[derive(Debug, Clone, PartialEq)]
pub struct Outgoing {
    /// The worker.
    pub worker_id: String,
    /// The message.
    pub message: pb::MasterMessage,
}

/// Why a dispatcher operation failed.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// The database failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// Moving a run forward failed.
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    /// A message from a worker can't be understood.
    #[error("message from worker `{worker_id}`: {reason}")]
    BadMessage {
        /// The worker.
        worker_id: String,
        /// What is wrong.
        reason: String,
    },
}

/// Tasks given to workers and their reports handled.
#[derive(Debug)]
pub struct Dispatcher {
    db: Database,
    execution: Execution,
    workers: Workers,
    cancel_grace: Duration,
    /// Workers that rejected a task instance for a worker-specific reason, with the detail.
    rejections: BTreeMap<String, BTreeMap<String, String>>,
}

impl Dispatcher {
    /// A dispatcher over `db` with no workers.
    #[must_use]
    pub fn new(db: Database, cancel_grace: Duration) -> Self {
        Self {
            execution: Execution::new(db.clone()),
            db,
            workers: Workers::new(),
            cancel_grace,
            rejections: BTreeMap::new(),
        }
    }

    /// The registered workers.
    #[must_use]
    pub const fn workers(&self) -> &Workers {
        &self.workers
    }

    /// A worker registered. A new worker or a restarted one gives `unschedulable` tasks
    /// another chance.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn register(
        &mut self,
        worker_id: &str,
        incarnation: &str,
        queues: &[String],
        slots: u32,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        if self.workers.register(worker_id, incarnation, queues, slots) {
            self.rejections.clear();
            self.db.requeue_unschedulable(now).await?;
        }
        Ok(())
    }

    /// A reconnecting worker still runs `attempt_id`: it keeps its slot.
    pub fn adopt(&mut self, worker_id: &str, attempt_id: &str, now: Timestamp) {
        self.workers.reserve(worker_id, attempt_id, now);
    }

    /// The inputs of an open attempt as JSON (what `FetchInputs` streams when they were too
    /// big for `AssignTask`); `None` for an unknown or ended attempt.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn attempt_inputs(&self, attempt_id: &str) -> Result<Option<Vec<u8>>, DispatchError> {
        let Some(attempt) = self.db.attempt(attempt_id).await? else {
            return Ok(None);
        };
        if attempt.status.is_some() {
            return Ok(None);
        }
        let Some(run) = self.db.run_tasks(&attempt.run_id).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.inputs(&run.dag, &attempt.run_id, &attempt.task_id)
                .await?,
        ))
    }

    /// A worker is draining: it gets no new tasks.
    pub fn draining(&mut self, worker_id: &str) {
        self.workers.draining(worker_id);
    }

    /// One step of the loop: new runs get their tasks, retries whose delay passed are queued,
    /// assignments with no `Started` are taken back, and ready tasks are assigned.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn step(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, DispatchError> {
        self.execution.prepare_runs(now).await?;
        self.execution.queue_due_retries(now).await?;
        self.expire(now).await?;
        self.assign(now).await
    }

    /// Assigns ready tasks to workers with free slots.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn assign(&mut self, now: Timestamp) -> Result<Vec<Outgoing>, DispatchError> {
        let mut outgoing = Vec::new();
        for ready in self.db.ready_instances(now).await? {
            let Some(task) = ready.dag.task(&ready.task_id) else {
                continue;
            };
            let excluded: BTreeSet<String> = self
                .rejections
                .get(&ready.instance_id)
                .map(|r| r.keys().cloned().collect())
                .unwrap_or_default();
            let Some(worker_id) = self
                .workers
                .pick(&task.settings.queue, &excluded)
                .map(str::to_owned)
            else {
                continue;
            };
            let Some((attempt_id, try_number)) = self
                .db
                .assign_attempt(&ready.instance_id, &worker_id, now)
                .await?
            else {
                continue;
            };
            let inputs = self
                .inputs(&ready.dag, &ready.run_id, &ready.task_id)
                .await?;
            if inputs.len() > MAX_INPUTS {
                self.end(
                    &attempt_id,
                    &ready.run_id,
                    &ready.task_id,
                    inputs_too_large(),
                    now,
                )
                .await?;
                continue;
            }
            self.workers.reserve(&worker_id, &attempt_id, now);
            let message = self.assign_message(&ready, &attempt_id, try_number, inputs);
            outgoing.push(Outgoing {
                worker_id,
                message: pb::MasterMessage {
                    message: Some(pb::master_message::Message::AssignTask(message)),
                },
            });
        }
        Ok(outgoing)
    }

    /// A worker reported `Started`. Returns a `CancelAttempt` if the attempt was taken back,
    /// ended or belongs to another worker.
    ///
    /// # Errors
    ///
    /// [`DispatchError::Db`] if the database fails.
    pub async fn started(
        &mut self,
        worker_id: &str,
        started: &pb::Started,
        now: Timestamp,
    ) -> Result<Option<Outgoing>, DispatchError> {
        let method = match pb::StartMethod::try_from(started.start_method) {
            Ok(pb::StartMethod::Fork) => "fork",
            _ => "spawn",
        };
        let attempt = self.db.attempt(&started.attempt_id).await?;
        let mine = attempt.as_ref().is_some_and(|a| a.worker_id == worker_id);
        if mine
            && self
                .db
                .mark_started(&started.attempt_id, method, now)
                .await?
        {
            return Ok(None);
        }
        Ok(Some(cancel(worker_id, &started.attempt_id)))
    }

    /// A worker rejected an assignment.
    ///
    /// # Errors
    ///
    /// [`DispatchError::BadMessage`] for an unknown reason, or [`DispatchError::Db`].
    pub async fn rejected(
        &mut self,
        worker_id: &str,
        rejected: &pb::Rejected,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let reaction = reject_reaction(rejected.reason).map_err(|e| DispatchError::BadMessage {
            worker_id: worker_id.to_owned(),
            reason: e.to_string(),
        })?;
        let Some(attempt) = self.db.attempt(&rejected.attempt_id).await? else {
            return Ok(());
        };
        if attempt.worker_id != worker_id || attempt.status.is_some() || attempt.started {
            return Ok(());
        }
        self.workers.release(&attempt.id);
        let (state, ready_at, reason) = match reaction {
            RejectReaction::Temporary => (TaskInstanceState::Queued, Some(now), None),
            RejectReaction::CodeNotThereYet => (
                TaskInstanceState::Queued,
                now.checked_add(CODE_RETRY_DELAY).ok(),
                Some(format!(
                    "waiting for the code on the workers: {}",
                    rejected.detail
                )),
            ),
            RejectReaction::WorkerSpecific => {
                let queue = self.db.run_tasks(&attempt.run_id).await?.and_then(|run| {
                    run.dag
                        .task(&attempt.task_id)
                        .map(|t| t.settings.queue.clone())
                });
                self.worker_specific(&attempt, rejected, queue, now)
            }
        };
        self.db
            .undo_assignment(&attempt.id, state, ready_at, reason.as_deref(), now)
            .await?;
        Ok(())
    }

    /// A worker reported `Finished`: stored once, and always acknowledged.
    ///
    /// # Errors
    ///
    /// [`DispatchError::BadMessage`] for an unknown status, or a database failure.
    pub async fn finished(
        &mut self,
        worker_id: &str,
        finished: &pb::Finished,
        now: Timestamp,
    ) -> Result<Outgoing, DispatchError> {
        let status =
            attempt_status_from_proto(finished.status).map_err(|e| DispatchError::BadMessage {
                worker_id: worker_id.to_owned(),
                reason: e.to_string(),
            })?;
        if let Some(attempt) = self.db.attempt(&finished.attempt_id).await? {
            let output = (status == AttemptStatus::Success)
                .then(|| serde_json::from_slice(&finished.output_json).ok())
                .flatten();
            let end = AttemptEnd {
                status,
                exit_code: finished.exit_code,
                signal: finished.signal,
                peak_memory_bytes: Some(finished.peak_memory_bytes),
                error_kind: non_empty(&finished.error_kind),
                error_summary: (!finished.error_summary_json.is_empty())
                    .then(|| String::from_utf8_lossy(&finished.error_summary_json).into_owned()),
                code_hash: non_empty(&finished.code_hash),
            };
            let result = AttemptResult {
                status,
                error_kind: end.error_kind.clone(),
                output,
            };
            self.record(&attempt, &end, &result, now).await?;
        }
        Ok(Outgoing {
            worker_id: worker_id.to_owned(),
            message: pb::MasterMessage {
                message: Some(pb::master_message::Message::FinishedAck(pb::FinishedAck {
                    attempt_id: finished.attempt_id.clone(),
                })),
            },
        })
    }

    /// Takes back assignments with no `Started` within [`START_TIMEOUT`]: they end
    /// `assignment_expired` and their tasks run again without using a retry. Returns how many.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn expire(&mut self, now: Timestamp) -> Result<usize, DispatchError> {
        let cutoff = now.checked_sub(START_TIMEOUT).unwrap_or(Timestamp::MIN);
        let expired = self.db.unstarted_attempts(cutoff).await?;
        for attempt in &expired {
            let end = AttemptEnd::by_master(AttemptStatus::AssignmentExpired);
            self.record(
                attempt,
                &end,
                &by_master(AttemptStatus::AssignmentExpired),
                now,
            )
            .await?;
        }
        Ok(expired.len())
    }

    /// A worker is lost: its attempts end `lost` (each uses a retry) and it is forgotten.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] if the database fails.
    pub async fn worker_lost(
        &mut self,
        worker_id: &str,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.remove(worker_id);
        for attempt in self.db.open_attempts(Some(worker_id)).await? {
            let end = AttemptEnd::by_master(AttemptStatus::Lost);
            self.record(&attempt, &end, &by_master(AttemptStatus::Lost), now)
                .await?;
        }
        Ok(())
    }

    /// Cancels a run; returns a `CancelAttempt` for each of its running attempts.
    ///
    /// # Errors
    ///
    /// [`DispatchError`] for an unknown run or a database failure.
    pub async fn cancel_run(
        &mut self,
        run_id: &str,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, DispatchError> {
        let cancelled = self.execution.cancel_run(run_id, now).await?;
        let running: BTreeSet<&TaskId> = cancelled.running.iter().collect();
        Ok(self
            .db
            .open_attempts(None)
            .await?
            .into_iter()
            .filter(|a| a.run_id == run_id && running.contains(&a.task_id))
            .map(|a| cancel(&a.worker_id, &a.id))
            .collect())
    }

    async fn record(
        &mut self,
        attempt: &StoredAttempt,
        end: &AttemptEnd,
        result: &AttemptResult,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        self.workers.release(&attempt.id);
        if self.db.end_attempt(&attempt.id, end, now).await? {
            self.execution
                .attempt_finished(&attempt.run_id, &attempt.task_id, result, now)
                .await?;
        }
        Ok(())
    }

    async fn end(
        &mut self,
        attempt_id: &str,
        run_id: &str,
        task_id: &TaskId,
        end: AttemptEnd,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let result = AttemptResult {
            status: end.status,
            error_kind: end.error_kind.clone(),
            output: None,
        };
        if self.db.end_attempt(attempt_id, &end, now).await? {
            self.execution
                .attempt_finished(run_id, task_id, &result, now)
                .await?;
        }
        Ok(())
    }

    /// Records a worker-specific rejection; the task is `unschedulable` once every eligible
    /// worker rejected it.
    fn worker_specific(
        &mut self,
        attempt: &StoredAttempt,
        rejected: &pb::Rejected,
        queue: Option<String>,
        now: Timestamp,
    ) -> (TaskInstanceState, Option<Timestamp>, Option<String>) {
        let by = self
            .rejections
            .entry(attempt.instance_id.clone())
            .or_default();
        by.insert(attempt.worker_id.clone(), rejected.detail.clone());
        let everyone =
            queue.is_none_or(|queue| self.workers.serving(&queue).all(|w| by.contains_key(&w.id)));
        if everyone {
            let reasons: Vec<String> = by.iter().map(|(w, d)| format!("{w}: {d}")).collect();
            return (
                TaskInstanceState::Unschedulable,
                None,
                Some(format!(
                    "every eligible worker rejected it ({})",
                    reasons.join("; ")
                )),
            );
        }
        (TaskInstanceState::Queued, Some(now), None)
    }

    /// The upstream outputs the task uses, as JSON (`{task_id: output}`).
    async fn inputs(
        &self,
        dag: &schedr_model::Dag,
        run_id: &str,
        task_id: &TaskId,
    ) -> Result<Vec<u8>, DispatchError> {
        let Some(task) = dag.task(task_id) else {
            return Ok(b"{}".to_vec());
        };
        let used: BTreeSet<&TaskId> = task
            .references()
            .into_iter()
            .filter_map(Reference::upstream_task)
            .collect();
        if used.is_empty() {
            return Ok(b"{}".to_vec());
        }
        let outputs = self.db.run_outputs(run_id).await?;
        let inputs: BTreeMap<&TaskId, &serde_json::Value> =
            outputs.iter().filter(|(id, _)| used.contains(id)).collect();
        Ok(serde_json::to_vec(&inputs).unwrap_or_else(|_| b"{}".to_vec()))
    }

    fn assign_message(
        &self,
        ready: &ReadyInstance,
        attempt_id: &str,
        try_number: u32,
        inputs: Vec<u8>,
    ) -> pb::AssignTask {
        let task = ready.dag.task(&ready.task_id);
        let external = inputs.len() > INLINE_INPUTS_LIMIT;
        pb::AssignTask {
            attempt_id: attempt_id.to_owned(),
            task_id: ready.task_id.to_string(),
            map_index: -1,
            try_number,
            run: Some(pb::RunInfo {
                run_id: ready.run_id.clone(),
                kind: run_kind_to_proto(ready.run_kind),
                planned_for: Some(proto_time(ready.planned_for)),
                since: Some(proto_time(ready.since)),
                dag_id: ready.dag.id.to_string(),
            }),
            task_json: task
                .and_then(|t| serde_json::to_vec(t).ok())
                .unwrap_or_default(),
            dag_file: Some(pb::DagFileRef {
                root: ready.dag.location.root.clone(),
                path: ready.dag.location.path.clone(),
            }),
            expected_code_hash: ready.content_hash.clone(),
            inputs_json: if external { Vec::new() } else { inputs },
            inputs_external: external,
            cancel_grace_ms: u64::try_from(self.cancel_grace.as_millis()).unwrap_or(u64::MAX),
            memory_budget_bytes: 0,
            setrlimit: false,
        }
    }
}

fn cancel(worker_id: &str, attempt_id: &str) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(pb::master_message::Message::CancelAttempt(
                pb::CancelAttempt {
                    attempt_id: attempt_id.to_owned(),
                },
            )),
        },
    }
}

const fn by_master(status: AttemptStatus) -> AttemptResult {
    AttemptResult {
        status,
        error_kind: None,
        output: None,
    }
}

fn inputs_too_large() -> AttemptEnd {
    AttemptEnd {
        error_kind: Some(INPUTS_TOO_LARGE.to_owned()),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    }
}

fn non_empty(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

fn proto_time(t: Timestamp) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.as_second(),
        nanos: t.subsec_nanosecond(),
    }
}
