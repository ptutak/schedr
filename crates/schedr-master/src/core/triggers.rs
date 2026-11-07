//! Event-triggered DAGs' triggers (M4.11b): one running per DAG, on a worker's watcher slot, from
//! its bookmark; each new key it finds starts a run.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::runs::ActiveDag;
use schedr_db::triggers::{STOPPED_BACK_PRESSURE, STOPPED_RETURNED, TriggerRecord};
use schedr_model::trigger::check_event_key;
use schedr_model::{DagId, RunEvent, SensorMode, TaskKind, Trigger};
use schedr_proto::pb::{self, master_message, worker_message};

use super::{CoreError, MasterCore};
use crate::dispatch::Outgoing;
use crate::metrics::TriggerCounts;

/// The error a generator's return ends it with.
const TRIGGER_RETURNED: &str = "trigger_returned";

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
    /// Stopped: too many of its runs are queued. It starts again when they have started.
    BackPressure,
    /// Stopped: it found nothing for `restart_if_idle`. It starts again at once.
    Idle,
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

/// Whether a generator found nothing for its `restart_if_idle`; a poke's calls have their limit.
fn idle_too_long(record: &TriggerRecord, dag: &ActiveDag, now: Timestamp) -> bool {
    let Trigger::Event {
        task,
        restart_if_idle_ms: Some(idle),
        ..
    } = &dag.dag.trigger
    else {
        return false;
    };
    if matches!(
        task.kind,
        TaskKind::Sensor {
            mode: SensorMode::Poke,
            ..
        }
    ) {
        return false;
    }
    let since = record.last_event_at.max(record.started_at);
    let idle = SignedDuration::from_millis(i64::try_from(*idle).unwrap_or(i64::MAX));
    since
        .and_then(|since| since.checked_add(idle).ok())
        .is_some_and(|at| at < now)
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
        let queued: BTreeMap<String, i64> =
            self.db.queued_event_runs().await?.into_iter().collect();
        let queued_of = |dag: &str| queued.get(dag).copied().unwrap_or(0);
        for record in records.iter().filter(|r| r.instance_id.is_some()) {
            let dag = active.iter().find(|a| a.dag.id.as_str() == record.dag_id);
            let keep = self.keep_trigger(record, dag, queued_of(&record.dag_id), now);
            if keep != Keep::Yes {
                self.stop_trigger(record, keep, now, outgoing).await?;
            }
        }
        let records = self.db.triggers().await?;
        for dag in active.iter().filter(|a| wanted(a, now)) {
            let record = records.iter().find(|r| r.dag_id == dag.dag.id.as_str());
            if self
                .may_start(dag, record, queued_of(dag.dag.id.as_str()), now)
                .await?
            {
                self.start_trigger(dag, record, now, outgoing).await?;
            }
        }
        self.count_triggers(&active, now).await?;
        Ok(())
    }

    /// Whether a trigger that isn't running starts now.
    async fn may_start(
        &self,
        dag: &ActiveDag,
        record: Option<&TriggerRecord>,
        queued: i64,
        now: Timestamp,
    ) -> Result<bool, CoreError> {
        if self.too_many_queued(queued) {
            return Ok(false);
        }
        if let Some(record) = record {
            if record.instance_id.is_some() || record.not_before.is_some_and(|at| now < at) {
                return Ok(false);
            }
            match record.stopped_why.as_deref() {
                Some(STOPPED_BACK_PRESSURE) if queued > 0 => return Ok(false),
                Some(STOPPED_RETURNED)
                    if !self.moved_on(dag, record.stopped_on.as_deref().unwrap_or_default()) =>
                {
                    return Ok(false);
                }
                _ => {}
            }
        }
        let Trigger::Event { task, .. } = &dag.dag.trigger else {
            return Ok(false);
        };
        Ok(!self
            .dispatcher
            .is_switched_off(&dag.dag, task.id.as_str())
            .await?)
    }

    /// Whether a DAG has more queued event runs than its trigger may run with.
    const fn too_many_queued(&self, queued: i64) -> bool {
        let most = self.settings.max_queued_events;
        most > 0 && queued >= most as i64
    }

    /// Whether a DAG has moved on from what a trigger stopped on (`<version> <code>`).
    fn moved_on(&self, dag: &ActiveDag, stopped_on: &str) -> bool {
        let (version, code) = stopped_on.split_once(' ').unwrap_or((stopped_on, ""));
        version != dag.version_id || self.dispatcher.code_is_gone(&dag.dag, code)
    }

    /// Whether a running trigger goes on: its worker there, on the DAG's current version and
    /// code, and its DAG wanting it.
    fn keep_trigger(
        &self,
        record: &TriggerRecord,
        dag: Option<&ActiveDag>,
        queued: i64,
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
        if self.too_many_queued(queued) {
            return Keep::BackPressure;
        }
        if idle_too_long(record, dag, now) {
            return Keep::Idle;
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
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        let (Some(instance), Some(worker)) = (&record.instance_id, &record.worker_id) else {
            return Ok(());
        };
        self.db
            .trigger_ended(&record.dag_id, instance, None, None)
            .await?;
        if keep == Keep::Forget {
            self.dispatcher.trigger_released(instance);
        } else {
            // Its worker counts its slot until it has ended.
            self.stopping_triggers
                .insert(instance.clone(), (worker.clone(), record.dag_id.clone()));
        }
        match keep {
            Keep::BackPressure => {
                self.db
                    .set_trigger_stopped(&record.dag_id, Some((STOPPED_BACK_PRESSURE, "")))
                    .await?;
                self.metrics.trigger_back_pressure_stops.inc();
            }
            Keep::Idle => {
                self.db.trigger_idle_restart(&record.dag_id, now).await?;
                self.metrics.trigger_idle_restarts.inc();
            }
            Keep::Yes | Keep::Stop | Keep::Forget => {}
        }
        if keep != Keep::Forget {
            outgoing.push(cancel_trigger(worker, instance));
        }
        tracing::info!(
            dag = record.dag_id,
            instance,
            worker,
            ?keep,
            "a trigger stopped"
        );
        Ok(())
    }

    /// Sets the triggers' gauge: each DAG with a trigger that wants to run, by how it goes.
    async fn count_triggers(&self, active: &[ActiveDag], now: Timestamp) -> Result<(), CoreError> {
        let records = self.db.triggers().await?;
        let mut counts = TriggerCounts::default();
        for dag in active.iter().filter(|a| wanted(a, now)) {
            let record = records.iter().find(|r| r.dag_id == dag.dag.id.as_str());
            let failures = record.map_or(0, |r| r.failures);
            let failing =
                self.settings.failing_after > 0 && failures >= self.settings.failing_after;
            if failing {
                counts.failing += 1;
            } else if record.is_some_and(|r| r.instance_id.is_some()) {
                counts.running += 1;
            } else if record.is_some_and(|r| r.stopped_why.is_some()) {
                counts.stopped += 1;
            } else {
                counts.waiting += 1;
            }
        }
        self.metrics.set_triggers(counts);
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
        let start = record.map_or((None, 0), |r| (r.bookmark.clone(), r.failures));
        let Some((message, code)) = self.dispatcher.place_trigger(dag, &instance, start, now)
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
                now,
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

    /// What a worker says of a trigger.
    pub(super) async fn trigger_message(
        &mut self,
        worker_id: &str,
        message: worker_message::Message,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        match message {
            worker_message::Message::TriggerEvent(event) => {
                Box::pin(self.trigger_event(worker_id, event, now)).await
            }
            worker_message::Message::TriggerPoked(poked) => {
                self.trigger_poked(worker_id, poked, now).await
            }
            worker_message::Message::TriggerStopped(stopped) => {
                self.trigger_stopped(worker_id, stopped, now).await
            }
            _ => Ok(Vec::new()),
        }
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
        if self.too_many_queued(self.db.queued_event_runs_of(&event.dag_id).await?) {
            // Not stored and not acknowledged: it comes again from the source, from the bookmark.
            let mut told = Vec::new();
            self.stop_trigger(&record, Keep::BackPressure, now, &mut told)
                .await?;
            return Ok(told);
        }
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
        let started = self
            .scheduler
            .event_run(&dag, &found, record.last_planned_for, now)
            .await?;
        self.metrics.trigger_event(started.is_some());
        // It works: a strike is for one that keeps getting its host killed.
        if let Trigger::Event { task, .. } = &dag.dag.trigger {
            self.db
                .clear_wait_step(&event.dag_id, task.id.as_str())
                .await?;
        }
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
        // It was told to stop: only what it ended with is left to count.
        if self
            .stopping_triggers
            .get(&stopped.instance_id)
            .is_some_and(|(worker, _)| worker == worker_id)
        {
            let dag = self.stopping_triggers.remove(&stopped.instance_id);
            self.dispatcher.trigger_released(&stopped.instance_id);
            if let Some((_, dag)) = dag {
                self.count_strike(&dag, None, &stopped, now).await?;
            }
            return Ok(Vec::new());
        }
        let record = self.db.trigger(&stopped.dag_id).await?;
        let Some(record) = record.filter(|r| holds(r, worker_id, &stopped.instance_id)) else {
            return Ok(Vec::new());
        };
        let code = record.code_hash.as_deref();
        self.count_strike(&record.dag_id, code, &stopped, now)
            .await?;
        if stopped.error_kind.is_empty() {
            self.dispatcher.trigger_released(&stopped.instance_id);
            self.db
                .trigger_ended(&stopped.dag_id, &stopped.instance_id, None, None)
                .await?;
            return Ok(Vec::new());
        }
        if stopped.error_kind == TRIGGER_RETURNED {
            let on = format!(
                "{} {}",
                record.dag_version_id.as_deref().unwrap_or_default(),
                record.code_hash.as_deref().unwrap_or_default()
            );
            self.db
                .set_trigger_stopped(&stopped.dag_id, Some((STOPPED_RETURNED, &on)))
                .await?;
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

    /// Counts a strike of a trigger that got its host killed (decision 338), on the code it ran:
    /// what it says, else what it was given.
    async fn count_strike(
        &self,
        dag_id: &str,
        code: Option<&str>,
        stopped: &pb::TriggerStopped,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        if !stopped.struck {
            return Ok(());
        }
        let Ok(id) = DagId::new(dag_id) else {
            return Ok(());
        };
        let dag = self.db.active_dags_of(&[&id]).await?.into_iter().next();
        let Some(Trigger::Event { task, .. }) = dag.as_ref().map(|d| &d.dag.trigger) else {
            return Ok(());
        };
        let code = Some(stopped.code_hash.as_str())
            .filter(|code| !code.is_empty())
            .or(code)
            .unwrap_or_default();
        let limit = self.settings.switch_off_after;
        if self
            .db
            .strike_wait_step((dag_id, task.id.as_str()), limit, code, now)
            .await?
        {
            tracing::warn!(dag = dag_id, "a trigger was switched off");
        }
        Ok(())
    }

    /// How a poke's calls have gone, from the worker that has it. One that went well clears its
    /// strikes: it works.
    pub(crate) async fn trigger_poked(
        &mut self,
        worker_id: &str,
        poked: pb::TriggerPoked,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        let record = self.db.trigger(&poked.dag_id).await?;
        let Some(record) = record.filter(|r| holds(r, worker_id, &poked.instance_id)) else {
            return Ok(Vec::new());
        };
        let error = (!poked.error.is_empty()).then(|| fit_error(&poked.error));
        let at = poked
            .at
            .as_ref()
            .and_then(crate::dispatch::time_of)
            .unwrap_or(now);
        self.db
            .trigger_poked(
                &record.dag_id,
                &poked.instance_id,
                at,
                error.as_deref(),
                poked.failures,
            )
            .await?;
        if error.is_none()
            && let Ok(dag_id) = DagId::new(&record.dag_id)
            && let Some(dag) = self.db.active_dags_of(&[&dag_id]).await?.first()
            && let Trigger::Event { task, .. } = &dag.dag.trigger
        {
            self.db
                .clear_wait_step(&record.dag_id, task.id.as_str())
                .await?;
        }
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
        // What it was told to stop: gone by itself, or still holding a slot of its count.
        let told: Vec<String> = self
            .stopping_triggers
            .iter()
            .filter(|(_, (worker, _))| worker == worker_id)
            .map(|(instance, _)| instance.clone())
            .collect();
        for instance in told {
            if !reported.contains(&instance) {
                self.stopping_triggers.remove(&instance);
                self.dispatcher.trigger_released(&instance);
            }
        }
        let current: BTreeSet<&str> = records
            .iter()
            .filter_map(|r| r.instance_id.as_deref())
            .collect();
        for instance in reported {
            if !current.contains(instance.as_str()) {
                self.dispatcher.trigger_kept(worker_id, instance, now);
                self.stopping_triggers
                    .entry(instance.clone())
                    .or_insert_with(|| (worker_id.to_owned(), String::new()));
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
    check_event_key(key).map_err(|why| format!("event_refused: {why}"))?;
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
