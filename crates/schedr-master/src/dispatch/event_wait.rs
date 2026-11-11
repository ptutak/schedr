//! The `wait_for(EventNamed(...))` node (decision 354): no worker takes it; each round the
//! master looks for the first event of its name, and key, posted since its run was created,
//! and ends it with that event, or `TimedOut` past its `max_wait`.

use jiff::Timestamp;
use schedr_db::attempts::ReadyInstance;
use schedr_db::named_events::NamedEvent;
use schedr_db::run_of::SensorWait;
use schedr_model::task::EventNamed;
use schedr_model::value::{DagCtxField, Reference, Value};
use serde_json::{Value as Json, json};

use super::{DispatchError, Dispatcher};
use crate::execution::WaitEnd;
use crate::mapping;

impl Dispatcher {
    /// A `wait_for(EventNamed(...))` queued: ended with the first matching event posted since
    /// its run was created (one posted before it began waiting counts), `TimedOut` past its
    /// `max_wait`, failed if its key isn't one; else it says what it waits for.
    pub(super) async fn wait_for_event(
        &mut self,
        waiting: &ReadyInstance,
        event: &EventNamed,
        max_wait_ms: Option<u64>,
        now: Timestamp,
        reasons: &mut Vec<(String, String)>,
    ) -> Result<(), DispatchError> {
        let Some(since) = self.db.ready_since(&waiting.instance_id).await? else {
            return Ok(());
        };
        let Some(run) = self.db.run_record(&waiting.run_id).await? else {
            return Ok(());
        };
        let deadline = self.deadline(since, max_wait_ms);
        let ended = match self.event_key(waiting, event).await? {
            Err(reason) => WaitEnd::Refused {
                reason: format!("its event's key: {reason}"),
            },
            Ok(key) => {
                let found = self
                    .db
                    .first_named_event(event.name.as_str(), key.as_deref(), run.triggered_at, None)
                    .await?;
                // One posted after its time ran out doesn't count, though seen late (a round
                // late, a master restarting; Codex's review), as for a run waited for.
                match found.filter(|found| found.posted_at <= deadline) {
                    Some(found) => WaitEnd::Event {
                        value: event_value(&found),
                    },
                    None if now >= deadline => WaitEnd::TimedOut,
                    None => {
                        let reason = waiting_reason(event, key.as_deref());
                        if waiting.queued_reason.as_deref() != Some(reason.as_str()) {
                            reasons.push((waiting.instance_id.clone(), reason));
                        }
                        return Ok(());
                    }
                }
            }
        };
        self.whole(async |me| {
            let (_, stops) = me
                .execution
                .wait_ended(&waiting.run_id, &waiting.task_id, ended, now)
                .await?;
            me.stop(&waiting.run_id, &stops).await
        })
        .await?;
        Ok(())
    }

    /// An `@sensor(wait_for=EventNamed(...))` queued (decision 354): the first matching event
    /// posted since its run was created (after the one its function said `NOT_YET` to; a retry
    /// gets the one it was handed) is recorded and to be assigned with it; past its `max_wait`
    /// it ends `TimedOut`; a key resolving to none fails it; else it says what it waits for.
    /// Returns whether it is to be assigned.
    pub(super) async fn sensor_event(
        &mut self,
        waiting: &ReadyInstance,
        event: &EventNamed,
        wait: &SensorWait,
        (deadline, now): (Timestamp, Timestamp),
        reasons: &mut Vec<(String, String)>,
    ) -> Result<bool, DispatchError> {
        let key = match self.event_key(waiting, event).await? {
            Ok(key) => key,
            Err(reason) => {
                let ended = WaitEnd::Refused {
                    reason: format!("its event's key: {reason}"),
                };
                self.whole(async |me| {
                    let (_, stops) = me
                        .execution
                        .wait_ended(&waiting.run_id, &waiting.task_id, ended, now)
                        .await?;
                    me.stop(&waiting.run_id, &stops).await
                })
                .await?;
                return Ok(false);
            }
        };
        let again = wait
            .handed
            .clone()
            .filter(|(handed, _)| wait.waited.as_ref().is_none_or(|(seen, _)| seen != handed));
        let again_id = again.as_ref().map(|(id, _)| id.clone());
        let found = if let Some((id, _)) = again {
            self.db.named_events_by_id(&id).await?
        } else {
            let Some(run) = self.db.run_record(&waiting.run_id).await? else {
                return Ok(false);
            };
            let named = (event.name.as_str(), key.as_deref());
            match &wait.waited {
                // After one it said `NOT_YET` to: read back a lookback, those passed left out
                // (B185).
                Some((_, at)) => {
                    let back = (self.event_lookback, waiting.instance_id.as_str());
                    self.db
                        .next_named_event_for(named, run.triggered_at, *at, back)
                        .await?
                }
                None => {
                    self.db
                        .first_named_event(named.0, named.1, run.triggered_at, None)
                        .await?
                }
            }
        };
        // A new one posted after its time ran out doesn't count, though seen late (as for the
        // node, Codex's review of part 1); a retry gets the one it was handed.
        let found = found.filter(|found| again_id.is_some() || found.posted_at <= deadline);
        if let Some(found) = found {
            self.db
                .hand_run(&waiting.instance_id, &found.id, found.posted_at)
                .await?;
            self.handing
                .insert(waiting.instance_id.clone(), event_value(&found));
            return Ok(true);
        }
        if now >= deadline {
            self.whole(async |me| me.time_out(waiting, now).await)
                .await?;
            return Ok(false);
        }
        let reason = waiting_reason(event, key.as_deref());
        if waiting.queued_reason.as_deref() != Some(reason.as_str()) {
            reasons.push((waiting.instance_id.clone(), reason));
        }
        Ok(false)
    }

    /// The key a wait matches in its run: `None` for any (no key given); `Err` with why for a
    /// key that resolves to no string (an output or parameter of another kind, or missing).
    async fn event_key(
        &self,
        waiting: &ReadyInstance,
        event: &EventNamed,
    ) -> Result<Result<Option<String>, String>, DispatchError> {
        let Some(key) = &event.key else {
            return Ok(Ok(None));
        };
        let resolved = match key {
            Value::Literal(json) => Ok(json.clone()),
            Value::Ref(Reference::DagCtx {
                field: DagCtxField::RunId,
            }) => Ok(json!(waiting.run_id)),
            Value::Ref(Reference::DagCtx {
                field: DagCtxField::DagId,
            }) => Ok(json!(waiting.dag.id.as_str())),
            Value::Ref(Reference::DagCtx {
                field: DagCtxField::EventKey,
            }) => match self.db.run_event(&waiting.run_id).await? {
                Some(event) => Ok(json!(event.key)),
                None => Err("`dag_ctx.event.key`: the run has no event".to_owned()),
            },
            Value::Ref(Reference::Param { path }) => {
                // As the runner reads them: a tuple stored as `$tuple`, a dict escaped.
                let params = self.db.run_params(&waiting.run_id).await?;
                mapping::follow(
                    params.as_ref().unwrap_or(&Json::Null),
                    path,
                    "dag_ctx.params",
                    "the run's parameters",
                )
            }
            Value::Ref(reference @ Reference::Output { task, .. }) => {
                // A mapped task's output is the list of its instances' (Codex's review).
                let tasks = std::collections::BTreeSet::from([task]);
                let sources =
                    mapping::sources(&self.db, &waiting.dag, &waiting.run_id, &tasks, |_, _| None)
                        .await?;
                mapping::resolve(&Value::Ref(reference.clone()), &sources)
            }
            // Refused when the DAG is validated.
            _ => Err("it can't be resolved here".to_owned()),
        };
        Ok(match resolved {
            Ok(Json::String(key)) if !key.is_empty() => {
                schedr_model::trigger::check_event_key(&key).map(|()| Some(key))
            }
            Ok(Json::String(_)) => Err("it is empty".to_owned()),
            Ok(other) => Err(format!("it is {}, not a string", mapping::kind_of(&other))),
            Err(why) => Err(why),
        })
    }
}

/// What a step waiting for `event` says it waits for (its key masked, as shown): a key read
/// from a field named as a secret (`params.token`) is hidden, as the run's page hides that
/// field (Grok's review).
fn waiting_reason(event: &EventNamed, key: Option<&str>) -> String {
    let secret = matches!(
        &event.key,
        Some(Value::Ref(Reference::Param { path } | Reference::Output { path, .. }))
            if path.iter().any(|elem| matches!(
                elem,
                schedr_model::value::PathElem::Field(name)
                    if schedr_config::masked::names_secret_in_words(name)
            ))
    );
    match key {
        Some(_) if secret => format!("waiting for event `{}` with key `****`", event.name),
        Some(key) => format!(
            "waiting for event `{}` with key `{}`",
            event.name,
            schedr_config::masked::mask_text(key)
        ),
        None => format!("waiting for event `{}`", event.name),
    }
}

/// An event as the node's output, an `Event[Json]` (a dataclass, stored as its fields): its
/// data's dicts shaped like a tag escaped (B66), its `at` a tagged `datetime`.
pub(crate) fn event_value(event: &NamedEvent) -> Json {
    json!({
        "key": event.key,
        "data": schedr_model::params::escape_tags(&event.data),
        "at": {"$datetime": event.at.to_string()},
    })
}
