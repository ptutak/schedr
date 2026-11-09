//! The `TriggerDagOperator` node (decisions 357-362, 365, 378-381): no worker takes it. The
//! master starts its attempt with the ID of the run it is to create recorded first, then
//! creates that run (by that ID, so a master stopped in between creates it once, B7), and,
//! with `wait=True`, ends the attempt when the run has ended, its `max_wait` ran out, or its
//! own run or task is cancelled (cancelling the run it waits for).

use jiff::Timestamp;
use schedr_db::attempts::{AttemptEnd, ChainedAttempt, ReadyInstance};
use schedr_model::runner::ErrorSummary;
use schedr_model::task::TaskKind;
use schedr_model::value::{DagCtxField, Reference, Value};
use schedr_model::{AttemptStatus, DagId, RunEvent, RunState};
use serde_json::{Value as Json, json};

use super::{DispatchError, Dispatcher, Outgoing};
use crate::execution::AttemptResult;
use crate::execution::{FAILED, SUCCESS, TIMED_OUT};
use crate::mapping;
use crate::scheduler::SchedulerError;

/// Error kinds of a `TriggerDagOperator`: the target missing or held (tried again, decision
/// 379); and those not tried again (each would fail the same way): parameters or `planned_for`
/// the target refuses, a target that isn't `Chained` for this DAG. Cycles between DAGs are
/// allowed (decision 399).
pub const TARGET_MISSING: &str = "chained_target_missing";
/// See [`TARGET_MISSING`].
pub const TARGET_HELD: &str = "chained_target_held";
/// See [`TARGET_MISSING`].
pub const RUN_FAILED: &str = "chained_run_failed";
/// See [`TARGET_MISSING`]; not tried again.
pub const PARAMS_REFUSED: &str = "chained_params_refused";
/// See [`TARGET_MISSING`]; not tried again.
pub const TARGET_REFUSED: &str = "chained_target_refused";

/// Why a run couldn't be created: the error kind, the exception's name and the message.
type Refusal = (&'static str, &'static str, String);

/// The task's settings, as the DAG gives them.
struct Target<'a> {
    dag: &'a DagId,
    params: &'a std::collections::BTreeMap<String, Value>,
    planned_for: Option<&'a Value>,
    wait: bool,
    max_wait_ms: Option<u64>,
}

impl Dispatcher {
    /// Starts the queued `TriggerDagOperator`s, then moves each open one on: creates its run if
    /// it isn't yet, and ends it as above. The cancels for a cancelled run's attempts go into
    /// `outgoing`.
    pub(super) async fn chained_into(
        &mut self,
        now: Timestamp,
        queued: &[ReadyInstance],
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        for ready in queued {
            let child = uuid::Uuid::now_v7().to_string();
            self.db
                .start_chained_attempt(&ready.instance_id, &child, now)
                .await?;
        }
        for attempt in self.db.chained_attempts().await? {
            self.chained_step(&attempt, now, outgoing).await?;
        }
        Ok(())
    }

    async fn chained_step(
        &mut self,
        attempt: &ChainedAttempt,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), DispatchError> {
        let task = attempt.dag.task(&attempt.task_id).map(|t| &t.kind);
        let Some(TaskKind::TriggerDag {
            dag,
            params,
            planned_for,
            wait,
            max_wait_ms,
        }) = task
        else {
            let why = "its task is no longer a TriggerDagOperator";
            return self
                .end_chained(
                    attempt,
                    failed(TARGET_REFUSED, "NotChained", why),
                    None,
                    now,
                )
                .await;
        };
        let target = Target {
            dag,
            params,
            planned_for: planned_for.as_ref(),
            wait: *wait,
            max_wait_ms: *max_wait_ms,
        };
        let child = &attempt.chained_run_id;
        if attempt.cancel_requested {
            // The run it waits for goes with it; one it doesn't wait for runs on.
            let waited = target.wait && attempt.chained_state.is_some_and(|s| !s.is_final());
            let mut stops = Vec::new();
            self.whole(async |me| {
                let end = AttemptEnd::by_master(AttemptStatus::Cancelled);
                me.end_chained(attempt, end, None, now).await?;
                if waited {
                    stops = me.cancel_run(child, now).await?;
                }
                Ok(())
            })
            .await?;
            outgoing.extend(stops);
            return Ok(());
        }
        let state = match attempt.chained_state {
            Some(state) => state,
            None => match self.create_chained(attempt, &target, now).await? {
                Ok(()) => RunState::Queued,
                Err((kind, exception, why)) => {
                    let end = failed(kind, exception, &why);
                    return self.end_chained(attempt, end, None, now).await;
                }
            },
        };
        let ended = if !target.wait {
            Some(succeeded(SUCCESS, json!(child)))
        } else if state.is_final()
            && attempt
                .chained_ended_at
                .is_none_or(|at| at <= self.deadline(attempt.started_at, target.max_wait_ms))
        {
            Some(Self::run_ended(attempt, state))
        } else if now >= self.deadline(attempt.started_at, target.max_wait_ms) {
            // Also a run that ended past it, seen only now (a round late): as a `RunOf` wait.
            // The run goes on; the task stops waiting for it (decision 359).
            Some(succeeded(TIMED_OUT, json!({})))
        } else {
            None
        };
        if let Some((end, output)) = ended {
            self.end_chained(attempt, end, output, now).await?;
        }
        Ok(())
    }

    /// How a waiting task ends with its run ended `state`: its value, the run's ID, on success;
    /// `Failed` (the run's state and ID) when the DAG routes it, else the task fails and is
    /// tried again (decision 358).
    fn run_ended(attempt: &ChainedAttempt, state: RunState) -> (AttemptEnd, Option<Ended>) {
        let child = &attempt.chained_run_id;
        if state == RunState::Success {
            return succeeded(SUCCESS, json!(child));
        }
        let routed = attempt
            .dag
            .edges
            .iter()
            .any(|e| e.from == attempt.task_id && e.on.iter().any(|on| on == FAILED));
        if routed {
            return succeeded(FAILED, json!({"state": state.to_string(), "run_id": child}));
        }
        let why = format!(
            "the run `{child}` it started ended `{state}`, and nothing here takes `.on(Failed)`"
        );
        (failed(RUN_FAILED, "ChainedRunFailed", &why), None)
    }

    /// Creates the attempt's run, by its recorded ID; `Err` with why it can't be.
    async fn create_chained(
        &self,
        attempt: &ChainedAttempt,
        target: &Target<'_>,
        now: Timestamp,
    ) -> Result<Result<(), Refusal>, DispatchError> {
        let context = Context::read(&self.db, attempt).await?;
        let mut params = serde_json::Map::new();
        for (key, value) in target.params {
            match self.resolve_here(attempt, &context, value).await? {
                // Stored form (tagged) back to what a trigger gives (`--params`): the target's
                // schema checks it and stores it by its own types (Codex's review of M4.12a).
                Ok(json) => {
                    params.insert(key.clone(), schedr_model::params::shown(&json));
                }
                Err(why) => {
                    let why = format!("`params[{key:?}]`: {why}");
                    return Ok(Err((PARAMS_REFUSED, "ParamsRefused", why)));
                }
            }
        }
        let planned_for = match target.planned_for {
            None => attempt.planned_for,
            Some(value) => match self.resolve_here(attempt, &context, value).await? {
                Ok(json) => {
                    let Some(at) = timestamp_of(&json) else {
                        let why = format!(
                            "`planned_for` is {}, not a datetime with an offset",
                            mapping::kind_of(&json)
                        );
                        return Ok(Err((PARAMS_REFUSED, "ParamsRefused", why)));
                    };
                    at
                }
                Err(why) => {
                    let why = format!("`planned_for`: {why}");
                    return Ok(Err((PARAMS_REFUSED, "ParamsRefused", why)));
                }
            },
        };
        let created = self
            .scheduler
            .trigger_chained(
                &attempt.chained_run_id,
                target.dag,
                planned_for,
                Some(&params),
                (&attempt.run_id, &attempt.dag.id),
                now,
            )
            .await;
        Ok(match created {
            Ok(_) => Ok(()),
            Err(SchedulerError::Db(error)) => return Err(error.into()),
            Err(error @ SchedulerError::NotRunnable(_)) => {
                // `schedr dags test` loads only the files under the path it tests (decision 378).
                let why = format!(
                    "{error} (under `schedr dags test`, the target's DAG file must be in the \
                     path tested to be run)"
                );
                Err((TARGET_MISSING, "TargetMissing", why))
            }
            Err(
                error @ (SchedulerError::Held { .. }
                | SchedulerError::FireTimes { .. }
                | SchedulerError::ChainedPaused(_)),
            ) => Err((TARGET_HELD, "TargetHeld", error.to_string())),
            Err(error @ (SchedulerError::Params { .. } | SchedulerError::ManualRun(_))) => {
                Err((PARAMS_REFUSED, "ParamsRefused", error.to_string()))
            }
            Err(
                error @ (SchedulerError::NeedsEvent(_)
                | SchedulerError::NoEventTrigger(_)
                | SchedulerError::NotChained { .. }),
            ) => Err((TARGET_REFUSED, "TargetRefused", error.to_string())),
        })
    }

    /// What `value` stands for in the attempt's run: its `dag_ctx` fields and parameters
    /// filled in, its upstream outputs read; `Err` with why it can't be (validation refuses
    /// templates and secrets).
    async fn resolve_here(
        &self,
        attempt: &ChainedAttempt,
        context: &Context,
        value: &Value,
    ) -> Result<Result<Json, String>, DispatchError> {
        let filled = match context.fill(attempt, value) {
            Ok(filled) => filled,
            Err(why) => return Ok(Err(why)),
        };
        let tasks = filled
            .references()
            .into_iter()
            .filter_map(Reference::upstream_task)
            .collect();
        let sources =
            mapping::sources(&self.db, &attempt.dag, &attempt.run_id, &tasks, |_, _| None).await?;
        Ok(mapping::resolve(&filled, &sources))
    }

    /// Ends a `TriggerDagOperator`'s attempt and moves its run on, in one transaction.
    async fn end_chained(
        &mut self,
        attempt: &ChainedAttempt,
        end: AttemptEnd,
        output: Option<Ended>,
        now: Timestamp,
    ) -> Result<(), DispatchError> {
        let (outcome, output) = output.map_or((None, None), |(o, v)| (Some(o.to_owned()), Some(v)));
        let result = AttemptResult {
            status: end.status,
            error_kind: end.error_kind.clone(),
            output,
            outcome,
            cleanup: None,
        };
        self.whole(async |me| {
            if me.db.end_attempt(&attempt.id, &end, now).await? {
                me.metrics.attempt_ended(&end.status);
                let recorded = me
                    .execution
                    .attempt_finished(
                        &attempt.run_id,
                        &attempt.task_id,
                        attempt.map_index,
                        &result,
                        now,
                    )
                    .await?;
                me.stop(&attempt.run_id, &recorded.stops).await?;
            }
            Ok(())
        })
        .await
    }
}

/// An outcome and its value.
type Ended = (&'static str, Json);

fn succeeded(outcome: &'static str, value: Json) -> (AttemptEnd, Option<Ended>) {
    (
        AttemptEnd::by_master(AttemptStatus::Success),
        Some((outcome, value)),
    )
}

fn failed(kind: &str, exception: &str, why: &str) -> AttemptEnd {
    AttemptEnd {
        error_kind: Some(kind.to_owned()),
        error_summary: Some(ErrorSummary::json(exception, why)),
        ..AttemptEnd::by_master(AttemptStatus::Failed)
    }
}

/// A tagged `datetime` (`{"$datetime": "..."}`, with an offset) as a moment.
fn timestamp_of(json: &Json) -> Option<Timestamp> {
    json.get("$datetime")?.as_str()?.parse().ok()
}

/// What the run's `dag_ctx` references read: its parameters and event.
struct Context {
    params: Json,
    event: Option<RunEvent>,
}

impl Context {
    async fn read(
        db: &schedr_db::Database,
        attempt: &ChainedAttempt,
    ) -> Result<Self, DispatchError> {
        Ok(Self {
            params: db.run_params(&attempt.run_id).await?.unwrap_or(Json::Null),
            event: db.run_event(&attempt.run_id).await?,
        })
    }

    /// `value` with its `dag_ctx` fields and parameters in place as literals, as the runner
    /// would read them.
    fn fill(&self, attempt: &ChainedAttempt, value: &Value) -> Result<Value, String> {
        let at = |t: Timestamp| json!({"$datetime": t.to_string()});
        let no_event = || "the run has no event".to_owned();
        Ok(match value {
            Value::Ref(Reference::DagCtx { field }) => Value::Literal(match field {
                DagCtxField::RunId => json!(attempt.run_id),
                DagCtxField::RunKind => json!(attempt.run_kind.to_string()),
                DagCtxField::PlannedFor => at(attempt.planned_for),
                DagCtxField::Since => at(attempt.since),
                DagCtxField::DagId => json!(attempt.dag.id.as_str()),
                DagCtxField::EventKey => json!(self.event.as_ref().ok_or_else(no_event)?.key),
                DagCtxField::EventData => {
                    let event = self.event.as_ref().ok_or_else(no_event)?;
                    schedr_model::params::escape_tags(&event.data)
                }
                DagCtxField::EventAt => {
                    let event = self.event.as_ref().ok_or_else(no_event)?;
                    event.at.map_or(Json::Null, at)
                }
            }),
            Value::Ref(Reference::Param { path }) => Value::Literal(mapping::follow(
                &self.params,
                path,
                "dag_ctx.params",
                "the run's parameters",
            )?),
            Value::Ref(Reference::Secret(_)) => return Err("a secret isn't read here".to_owned()),
            Value::Template(_) => return Err("a text template isn't rendered here".to_owned()),
            Value::List(items) => Value::List(
                items
                    .iter()
                    .map(|item| self.fill(attempt, item))
                    .collect::<Result<_, _>>()?,
            ),
            Value::Map(entries) => Value::Map(
                entries
                    .iter()
                    .map(|(key, item)| Ok((key.clone(), self.fill(attempt, item)?)))
                    .collect::<Result<_, String>>()?,
            ),
            Value::Literal(_) | Value::Ref(_) => value.clone(),
        })
    }
}
