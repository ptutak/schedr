//! Event-triggered DAGs' triggers (M4.11b): one running per DAG, on a worker's watcher slot, from
//! its bookmark; each new key it finds starts a run.

use std::collections::BTreeSet;
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::runs::ActiveDag;
use schedr_db::triggers::TriggerRecord;
use schedr_model::trigger::MAX_EVENT_KEY;
use schedr_model::{DagId, RunEvent, Trigger};
use schedr_proto::pb::{self, master_message};

use super::{CoreError, MasterCore};
use crate::dispatch::Outgoing;

/// The most of a trigger's error kept for people.
const MAX_ERROR_CHARS: usize = 4096;

/// What the pass decides for a DAG's running trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keep {
    /// It goes on.
    Yes,
    /// Stopped, the worker told.
    Stop,
    /// Stopped; its worker is gone, and there is nobody to tell.
    Forget,
}

/// Whether a DAG should have its trigger running now.
fn wanted(active: &ActiveDag, now: Timestamp) -> bool {
    let Trigger::Event { start, end, .. } = &active.dag.trigger else {
        return false;
    };
    !active.paused
        && active.held.is_none()
        && start.is_none_or(|start| start <= now)
        && end.is_none_or(|end| now <= end)
}

/// The wait before a trigger that failed `failures` times in a row starts again: doubling, up to
/// `most`.
fn backoff(failures: u32, first: Duration, most: Duration) -> Duration {
    let doubled = 2_u32
        .checked_pow(failures.saturating_sub(1))
        .unwrap_or(u32::MAX);
    first.saturating_mul(doubled).min(most)
}

impl MasterCore {
    /// Stops the triggers that should not run where they are, and starts those that should run
    /// and don't.
    pub(crate) async fn trigger_pass(
        &mut self,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        let records = self.db.triggers().await?;
        let active = self.db.active_dags().await?;
        for record in records.iter().filter(|r| r.instance_id.is_some()) {
            let dag = active.iter().find(|a| a.dag.id.as_str() == record.dag_id);
            let keep = self.keep_trigger(record, dag, now);
            if keep != Keep::Yes {
                self.stop_trigger(record, keep, outgoing).await?;
            }
        }
        let records = self.db.triggers().await?;
        for dag in active.iter().filter(|a| wanted(a, now)) {
            let record = records.iter().find(|r| r.dag_id == dag.dag.id.as_str());
            let (running, not_before) =
                record.map_or((false, None), |r| (r.instance_id.is_some(), r.not_before));
            if running || not_before.is_some_and(|at| now < at) {
                continue;
            }
            self.start_trigger(dag, record, now, outgoing).await?;
        }
        Ok(())
    }

    /// Whether a running trigger goes on: its worker there, on the DAG's current version and
    /// code, and its DAG wanting it.
    fn keep_trigger(
        &self,
        record: &TriggerRecord,
        dag: Option<&ActiveDag>,
        now: Timestamp,
    ) -> Keep {
        let worker_id = record.worker_id.as_deref().unwrap_or_default();
        // Told as gone only when its lease ran out; a master just started hasn't heard from
        // its workers yet.
        if !self.connections.contains_key(worker_id) {
            return Keep::Forget;
        }
        let Some(dag) = dag.filter(|dag| wanted(dag, now)) else {
            return Keep::Stop;
        };
        if record.dag_version_id.as_deref() != Some(dag.version_id.as_str()) {
            return Keep::Stop;
        }
        let Some(worker) = self.dispatcher.worker(worker_id) else {
            return Keep::Yes;
        };
        if worker.state != crate::workers::WorkerState::Active || worker.drained {
            return Keep::Stop;
        }
        // New code under the same definition, or a root the worker no longer offers.
        match worker.roots.get(&dag.dag.location.root) {
            Some(there) if record.code_hash.as_ref() == Some(there) => Keep::Yes,
            _ => Keep::Stop,
        }
    }

    /// Ends a running trigger, and tells its worker if it can be told.
    async fn stop_trigger(
        &mut self,
        record: &TriggerRecord,
        keep: Keep,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        let (Some(instance), Some(worker)) = (&record.instance_id, &record.worker_id) else {
            return Ok(());
        };
        self.db
            .trigger_ended(&record.dag_id, instance, None, None)
            .await?;
        self.dispatcher.trigger_released(instance);
        if keep == Keep::Stop {
            outgoing.push(cancel_trigger(worker, instance));
        }
        tracing::info!(dag = record.dag_id, instance, worker, "a trigger stopped");
        Ok(())
    }

    /// Gives a DAG's trigger to a worker with a free watcher slot; with none, the next pass asks
    /// again.
    async fn start_trigger(
        &mut self,
        dag: &ActiveDag,
        record: Option<&TriggerRecord>,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        let instance = uuid::Uuid::now_v7().to_string();
        let after = record.and_then(|r| r.bookmark.clone());
        let Some((message, code)) = self.dispatcher.place_trigger(dag, &instance, after, now)
        else {
            return Ok(());
        };
        let stored = self
            .db
            .trigger_assigned(
                dag.dag.id.as_str(),
                &instance,
                &message.worker_id,
                &dag.version_id,
                &code,
            )
            .await;
        if let Err(error) = stored {
            // Nothing says it was given: its slot would be gone for good.
            self.dispatcher.trigger_released(&instance);
            return Err(error.into());
        }
        outgoing.push(message);
        Ok(())
    }

    /// A trigger found something: stored at most once, and acknowledged only then, so what the
    /// master never got is sent again. What isn't the running trigger's on this worker, or is of
    /// an older version than the DAG's, is acknowledged without being stored: the one that takes
    /// over resumes from the bookmark. What can't be an event fails the trigger.
    pub(crate) async fn trigger_event(
        &mut self,
        worker_id: &str,
        event: pb::TriggerEvent,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let ack = Outgoing {
            worker_id: worker_id.to_owned(),
            message: pb::MasterMessage {
                message: Some(master_message::Message::TriggerEventAck(
                    pb::TriggerEventAck {
                        instance_id: event.instance_id.clone(),
                        seq: event.seq,
                    },
                )),
            },
        };
        let record = self.db.trigger(&event.dag_id).await?;
        let Some(record) = record.filter(|r| holds(r, worker_id, &event.instance_id)) else {
            return Ok(vec![ack]);
        };
        let Ok(dag_id) = DagId::new(&event.dag_id) else {
            return Ok(vec![ack]);
        };
        let dag = self.db.active_dags_of(&[&dag_id]).await?.into_iter().next();
        let Some(dag) = dag.filter(|d| record.dag_version_id.as_ref() == Some(&d.version_id))
        else {
            return Ok(vec![ack]);
        };
        let found = match event_of(&event, &dag) {
            Ok(found) => found,
            Err(why) => {
                self.trigger_failed(
                    &event.dag_id,
                    &event.instance_id,
                    record.failures,
                    &why,
                    now,
                )
                .await?;
                return Ok(vec![cancel_trigger(worker_id, &event.instance_id)]);
            }
        };
        self.scheduler
            .event_run(&dag, &found, record.last_planned_for, now)
            .await?;
        Ok(vec![ack])
    }

    /// A trigger ended by itself or as asked: failed, it starts again after a wait. Only the
    /// worker that has it says so.
    pub(crate) async fn trigger_stopped(
        &mut self,
        worker_id: &str,
        stopped: pb::TriggerStopped,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let record = self.db.trigger(&stopped.dag_id).await?;
        let Some(record) = record.filter(|r| holds(r, worker_id, &stopped.instance_id)) else {
            return Ok(Vec::new());
        };
        if stopped.error_kind.is_empty() {
            self.dispatcher.trigger_released(&stopped.instance_id);
            self.db
                .trigger_ended(&stopped.dag_id, &stopped.instance_id, None, None)
                .await?;
            return Ok(Vec::new());
        }
        let why = if stopped.message.is_empty() {
            stopped.error_kind
        } else {
            format!("{}: {}", stopped.error_kind, stopped.message)
        };
        self.trigger_failed(
            &stopped.dag_id,
            &stopped.instance_id,
            record.failures,
            &why,
            now,
        )
        .await?;
        Ok(Vec::new())
    }

    /// Ends a trigger with an error: it starts again after a wait longer with each in a row.
    async fn trigger_failed(
        &mut self,
        dag_id: &str,
        instance: &str,
        failures: u32,
        why: &str,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        self.dispatcher.trigger_released(instance);
        let wait = backoff(
            failures.saturating_add(1),
            self.settings.timings.trigger_restart_backoff,
            self.settings.timings.trigger_restart_backoff_max,
        );
        let wait = SignedDuration::try_from(wait).unwrap_or(SignedDuration::MAX);
        let why = fit_error(why);
        self.db
            .trigger_ended(dag_id, instance, Some(&why), now.checked_add(wait).ok())
            .await?;
        tracing::warn!(dag = dag_id, why, "a trigger failed");
        Ok(())
    }

    /// A heartbeat: a trigger the master doesn't count as running there is told to stop (its
    /// worker came back after it was given elsewhere).
    pub(crate) async fn triggers_reported(
        &mut self,
        worker_id: &str,
        reported: &[String],
    ) -> Result<Vec<Outgoing>, CoreError> {
        if reported.is_empty() {
            return Ok(Vec::new());
        }
        let records = self.db.triggers().await?;
        let current: BTreeSet<&str> = records
            .iter()
            .filter(|r| r.worker_id.as_deref() == Some(worker_id))
            .filter_map(|r| r.instance_id.as_deref())
            .collect();
        Ok(reported
            .iter()
            .filter(|instance| !current.contains(instance.as_str()))
            .map(|instance| cancel_trigger(worker_id, instance))
            .collect())
    }

    /// A worker registered: what it still runs keeps its slot, what it doesn't has ended, and
    /// what isn't its any more is told to stop.
    pub(crate) async fn triggers_on_register(
        &mut self,
        worker_id: &str,
        reported: &[String],
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let records = self.db.triggers().await?;
        for record in records
            .iter()
            .filter(|r| r.worker_id.as_deref() == Some(worker_id))
        {
            let Some(instance) = &record.instance_id else {
                continue;
            };
            if reported.contains(instance) {
                self.dispatcher.trigger_kept(worker_id, instance, now);
            } else {
                self.dispatcher.trigger_released(instance);
                self.db
                    .trigger_ended(&record.dag_id, instance, None, None)
                    .await?;
            }
        }
        self.triggers_reported(worker_id, reported).await
    }
}

/// Whether this worker has this run of a trigger.
fn holds(record: &TriggerRecord, worker_id: &str, instance: &str) -> bool {
    record.worker_id.as_deref() == Some(worker_id)
        && record.instance_id.as_deref() == Some(instance)
}

/// An event as a run takes it; why not, for what a worker sent that can't be one.
fn event_of(event: &pb::TriggerEvent, dag: &ActiveDag) -> Result<RunEvent, String> {
    let key = &event.key;
    if key.is_empty() || key.len() > MAX_EVENT_KEY || key.contains('\0') {
        return Err(format!(
            "event_refused: an event's key has 1 to {MAX_EVENT_KEY} bytes and no NUL, got {} bytes",
            key.len()
        ));
    }
    let Trigger::Event { task, .. } = &dag.dag.trigger else {
        return Err("event_refused: the DAG no longer has an event trigger".to_owned());
    };
    let limit = task.settings.max_output_size;
    if u64::try_from(event.data_json.len()).unwrap_or(u64::MAX) > limit {
        return Err(format!(
            "event_refused: the data of event `{key}` is {} bytes, over the limit of {limit}",
            event.data_json.len()
        ));
    }
    let data = serde_json::from_slice(&event.data_json)
        .map_err(|_| format!("event_refused: the data of event `{key}` doesn't read"))?;
    Ok(RunEvent {
        key: key.clone(),
        data,
        at: event.at.as_ref().and_then(crate::dispatch::time_of),
    })
}

/// A trigger's error as it is stored: short, and text a database takes.
fn fit_error(why: &str) -> String {
    why.chars()
        .take(MAX_ERROR_CHARS)
        .map(|c| if c == '\0' { '\u{fffd}' } else { c })
        .collect()
}

/// Tells a worker to stop a trigger.
fn cancel_trigger(worker_id: &str, instance: &str) -> Outgoing {
    Outgoing {
        worker_id: worker_id.to_owned(),
        message: pb::MasterMessage {
            message: Some(master_message::Message::CancelTrigger(pb::CancelTrigger {
                instance_id: instance.to_owned(),
            })),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_doubles_up_to_its_limit() {
        let first = Duration::from_secs(5);
        let most = Duration::from_mins(5);
        assert_eq!(backoff(1, first, most), first);
        assert_eq!(backoff(2, first, most), Duration::from_secs(10));
        assert_eq!(backoff(5, first, most), Duration::from_secs(80));
        assert_eq!(backoff(7, first, most), most);
        assert_eq!(backoff(4000, first, most), most);
    }
}
