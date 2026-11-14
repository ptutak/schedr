//! Event-triggered DAGs' triggers (M4.11b): one running per DAG, on a worker's watcher slot, from
//! its bookmark; each new key it finds starts a run.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_db::dags::NO_FIRE;
use schedr_db::run_of::{RunOfFrom, RunOfSeen};
use schedr_db::runs::ActiveDag;
use schedr_db::triggers::{STOPPED_BACK_PRESSURE, STOPPED_RETURNED, TriggerRecord};
use schedr_db::{Database, DbError};
use schedr_model::task::{EventNamed, SensorWaitFor, WaitSource};
use schedr_model::trigger::check_event_key;
use schedr_model::value::Value;
use schedr_model::{DagId, RunEvent, SensorMode, TaskId, TaskKind, Trigger};
use schedr_proto::pb::{self, master_message, worker_message};
use serde_json::json;

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
    idle_deadline(record, dag).is_some_and(|at| at < now)
}

/// When the trigger loop must look at `dag`'s trigger again on its own (decision 486), at `now`
/// after a pass: the earliest of its window opening or closing, its restart's backoff ending
/// and a generator's `restart_if_idle` coming; [`NO_FIRE`] when none is ahead (a `RunOf` or an
/// `EventNamed` has none: what it waits for is a write). A `restart_if_idle` passed already
/// (the pass couldn't restart it) comes back as `now`: looked at again at the next tick.
fn next_look(dag: &ActiveDag, record: Option<&TriggerRecord>, now: Timestamp) -> Timestamp {
    let Trigger::Event { start, end, .. } = &dag.dag.trigger else {
        return NO_FIRE;
    };
    let running = record.is_some_and(|r| r.instance_id.is_some());
    let backoff = record.filter(|_| !running).and_then(|r| r.not_before);
    let idle = record
        .filter(|_| running)
        .and_then(|r| idle_deadline(r, dag));
    [*start, *end, backoff, idle]
        .into_iter()
        .flatten()
        .filter(|at| *at > now || Some(*at) == idle)
        .map(|at| at.max(now))
        .min()
        .unwrap_or(NO_FIRE)
}

/// When a running generator has found nothing for its `restart_if_idle` (a poke's calls have
/// their limit, so a poke has none).
fn idle_deadline(record: &TriggerRecord, dag: &ActiveDag) -> Option<Timestamp> {
    let Trigger::Event {
        task,
        restart_if_idle_ms: Some(idle),
        ..
    } = &dag.dag.trigger
    else {
        return None;
    };
    if matches!(
        task.kind,
        TaskKind::Sensor {
            mode: SensorMode::Poke,
            ..
        }
    ) {
        return None;
    }
    let since = record.last_event_at.max(record.started_at)?;
    since
        .checked_add(SignedDuration::from_millis(
            i64::try_from(*idle).unwrap_or(i64::MAX),
        ))
        .ok()
}

/// What an `@trigger(wait_for=RunOf(...))` waits for, and its `run_retries` (decision 353).
pub(crate) fn waits_for(dag: &schedr_model::Dag) -> Option<(&SensorWaitFor, Option<u32>)> {
    let Trigger::Event {
        task, run_retries, ..
    } = &dag.trigger
    else {
        return None;
    };
    let TaskKind::Sensor {
        wait_for: Some(wait),
        ..
    } = &task.kind
    else {
        return None;
    };
    Some((wait, *run_retries))
}

/// What an `@trigger(wait_for=...)`'s cursor is of: its source, as stored with it.
fn source_of(wait: &SensorWaitFor) -> String {
    match wait.source() {
        Some(WaitSource::RunOf(run_of)) => serde_json::to_string(run_of),
        Some(WaitSource::Event(event)) => serde_json::to_string(event),
        None => Ok(String::new()),
    }
    .unwrap_or_default()
}

/// The most runs one look for the next run to hand passes because they are gone.
const GONE_RUNS_OF_A_LOOK: usize = 10;

/// The wait before a trigger that failed `failures` times in a row starts again: doubling, up to
/// `most`.
fn backoff(failures: u32, first: Duration, most: Duration) -> Duration {
    let doubled = 2_u32
        .checked_pow(failures.saturating_sub(1))
        .unwrap_or(u32::MAX);
    first.saturating_mul(doubled).min(most)
}

/// How many runs one pass starts for a DAG started by `RunOf`; the rest wait for the next.
const RUNS_OF_A_PASS: u32 = 50;

impl MasterCore {
    /// Stops the triggers that should not run where they are, and starts those that should run
    /// and don't.
    pub(crate) async fn trigger_pass(
        &mut self,
        now: Timestamp,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), CoreError> {
        // Nothing for the pass: no trigger it looks after is switched on or running (decision
        // 485).
        if self.db.skipped(schedr_db::probe::Check::Triggers).await? {
            return Ok(());
        }
        // The looks taken before anything is read: one cleared during the pass (a named event
        // posted after its scan) stays cleared (Grok's reviews of M5.1f2).
        let looks = self.db.claim_looks().await?;
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
        for dag in active
            .iter()
            .filter(|a| matches!(a.dag.trigger, Trigger::RunOf { .. }))
        {
            self.run_of_pass(dag, queued_of(dag.dag.id.as_str()), now)
                .await?;
        }
        for dag in active
            .iter()
            .filter(|a| matches!(a.dag.trigger, Trigger::EventNamed { .. }))
        {
            self.event_named_pass(dag, queued_of(dag.dag.id.as_str()), now)
                .await?;
        }
        Box::pin(self.store_next_looks(&active, &looks, now)).await
    }

    /// Stores, for each trigger the pass looks after, when it must look again (decision 486),
    /// where that changed: the tick's probe wakes the loop then. What happens in between (an
    /// event, a run ending, a worker gone) is a write, which wakes it anyway.
    /// `stored` is what was stored when the pass began: a look changed since isn't overwritten.
    async fn store_next_looks(
        &self,
        active: &[ActiveDag],
        stored: &std::collections::HashMap<DagId, Option<Timestamp>>,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let records = self.db.triggers().await?;
        for dag in active.iter().filter(|a| !a.paused) {
            if !matches!(
                dag.dag.trigger,
                Trigger::Event { .. } | Trigger::RunOf { .. } | Trigger::EventNamed { .. }
            ) {
                continue;
            }
            let record = records.iter().find(|r| r.dag_id == dag.dag.id.as_str());
            let next = next_look(dag, record, now);
            match stored.get(&dag.dag.id) {
                Some(Some(was)) if *was != next => {
                    self.db.set_next_fire_if(&dag.dag.id, *was, next).await?;
                }
                // Not known when taken (cleared between the claim and the read) or its row
                // came with this pass (a new version): left for the next pass, which the
                // tick's probe wakes for it, to claim.
                _ => {}
            }
        }
        Ok(())
    }

    /// How many runs a pass may start for a DAG with `queued` event runs waiting: at most
    /// [`RUNS_OF_A_PASS`], and what `[triggers] max_queued_events` leaves room for.
    fn pass_room(&self, queued: i64) -> u32 {
        let most = self.settings.max_queued_events;
        if most == 0 {
            RUNS_OF_A_PASS
        } else {
            let left = i64::from(most).saturating_sub(queued);
            u32::try_from(left.clamp(0, i64::from(RUNS_OF_A_PASS))).unwrap_or(0)
        }
    }

    /// A DAG started by `EventNamed` (decision 354): a run per event of that name posted while
    /// it is live, keyed by the event's key (a key seen before starts none: the DAG's events
    /// table keeps one per key), with the event's data, planned for when it happened. As for
    /// `RunOf`, what is posted while it is paused or gone is never caught up; held, it waits.
    async fn event_named_pass(
        &self,
        dag: &ActiveDag,
        queued: i64,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let Trigger::EventNamed { event: named } = &dag.dag.trigger else {
            return Ok(());
        };
        let id = dag.dag.id.as_str();
        if dag.paused || dag.held.is_some() {
            return Ok(());
        }
        let Some(live) = self.db.live_since(id).await? else {
            return Ok(());
        };
        let room = self.pass_room(queued);
        if room == 0 {
            return Ok(());
        }
        // The cursor (the newest event acted on, by when posted) is of this name only. Read
        // again from a little before it, by the keys the DAG has (Codex's review): a post's
        // time is taken before its commit, so one can be stored behind it.
        let source = serde_json::to_string(named).unwrap_or_default();
        let seen = self.db.run_of_seen(id, &source).await?;
        let lookback =
            SignedDuration::try_from(self.settings.event_lookback).unwrap_or(SignedDuration::MAX);
        let since = seen
            .as_ref()
            .and_then(|seen| seen.at.checked_sub(lookback).ok())
            .map_or(live, |from| from.max(live));
        let found = self
            .db
            .named_events_unseen(named.name.as_str(), id, since, room)
            .await?;
        let mut last_planned_for = self.db.trigger(id).await?.and_then(|r| r.last_planned_for);
        let mut last = seen.filter(|seen| seen.at >= live);
        let mut moved = false;
        for posted in found {
            let event = RunEvent {
                key: posted.key.clone(),
                // Outside JSON, never schedr's tags: a dict shaped like one is escaped (Codex's
                // review), as the waiting node's value is.
                data: schedr_model::params::escape_tags(&posted.data),
                at: Some(posted.at),
            };
            let started = self
                .scheduler
                .event_run(dag, &event, last_planned_for, now)
                .await?;
            // A repeat starts none and leaves the window as it was (Codex's review).
            if started.is_some() {
                last_planned_for = Some(posted.at);
            }
            if last.as_ref().is_none_or(|last| posted.posted_at > last.at) {
                last = Some(RunOfSeen {
                    at: posted.posted_at,
                    run: posted.id,
                });
                moved = true;
            }
        }
        if let Some(last) = last.filter(|_| moved) {
            self.db.set_run_of_seen(id, &source, &last).await?;
        }
        Ok(())
    }

    /// A DAG started by `RunOf` (decision 353): a run per run of the other DAG (or its task)
    /// that succeeded while it is live, as an event keyed by that run's ID, so each starts
    /// one; planned for that run's `planned_for` plus `lag`, or for now. Only what ended since
    /// it is live (`dag_triggers.live_since`, kept with each pause, unpause and removal) counts: what
    /// ends while it is paused or gone is never caught up; held (its file failing to import),
    /// it waits and goes on after. At most what `[triggers] max_queued_events` leaves room for.
    async fn run_of_pass(
        &self,
        dag: &ActiveDag,
        queued: i64,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let Trigger::RunOf { run_of } = &dag.dag.trigger else {
            return Ok(());
        };
        let id = dag.dag.id.as_str();
        if dag.paused || dag.held.is_some() {
            return Ok(());
        }
        let Some(live) = self.db.live_since(id).await? else {
            return Ok(());
        };
        let room = self.pass_room(queued);
        if room == 0 {
            return Ok(());
        }
        // The cursor is of this source only: a trigger changed starts from when it was live again.
        let source = serde_json::to_string(run_of).unwrap_or_default();
        let seen = self.db.run_of_seen(id, &source).await?;
        // From what it has seen, never from before it was live (decision 353, none caught up).
        let from = match &seen {
            Some(seen) if seen.at >= live => RunOfFrom::After(seen),
            _ => RunOfFrom::Since(live),
        };
        let task = run_of.task.as_ref().map(TaskId::as_str);
        let found = self
            .db
            .succeeded_since((run_of.dag.as_str(), task), id, from, room)
            .await?;
        let mut last_planned_for = self.db.trigger(id).await?.and_then(|r| r.last_planned_for);
        let mut last = None;
        for (other, planned_for, ended) in found {
            let at = run_of.lag_ms.and_then(|lag| {
                planned_for
                    .checked_add(SignedDuration::from_millis(lag))
                    .ok()
            });
            let event = RunEvent {
                key: other.clone(),
                data: json!({
                    "run_id": other,
                    "dag_id": run_of.dag.as_str(),
                    "planned_for": planned_for.to_string(),
                }),
                at,
            };
            let started = self
                .scheduler
                .event_run(dag, &event, last_planned_for, now)
                .await?;
            // A repeat starts none and leaves the window as it was (Codex's review of M4.11e).
            if started.is_some() {
                last_planned_for = Some(at.unwrap_or(now));
            }
            last = Some(RunOfSeen {
                at: ended,
                run: other,
            });
        }
        if let Some(last) = last {
            self.db.set_run_of_seen(id, &source, &last).await?;
        }
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
        // A call being stopped still runs, with the run it was given: the next waits for its
        // end, or for its worker to be gone, so a run is never called twice (Grok's review).
        // One whose DAG isn't known (a worker reporting it to a master started since) may be
        // any DAG's: every such trigger waits for it, its cancel already sent (Codex's review).
        let id = dag.dag.id.as_str();
        if waits_for(&dag.dag).is_some()
            && self
                .stopping_triggers
                .values()
                .any(|(_, d)| d == id || d.is_empty())
        {
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
        let mut start = record.map_or((None, 0), |r| (r.bookmark.clone(), r.failures));
        let mut handed = None;
        let mut given = None;
        if let Some((wait, _)) = waits_for(&dag.dag) {
            // Called for one run at a time: with none ended to hand, it doesn't start.
            let Some((run, result, fresh)) = self.run_to_hand(dag, wait).await? else {
                return Ok(());
            };
            if fresh {
                // Its failures were another run's: handing this one starts them again.
                start.1 = 0;
            }
            let mut with_run = dag.clone();
            if let Trigger::Event { task, .. } =
                &mut std::sync::Arc::make_mut(&mut with_run.dag).trigger
                && let TaskKind::Sensor { arguments, .. } = &mut task.kind
            {
                arguments.insert(wait.param.clone(), Value::Literal(result));
            }
            handed = Some(run);
            given = Some(with_run);
        }
        let placed = given.as_ref().unwrap_or(dag);
        let Some((message, code)) = self.dispatcher.place_trigger(placed, &instance, start, now)
        else {
            return Ok(());
        };
        // The whole message, within what a worker takes (Grok's and Codex's reviews): the run
        // rides in it. Over, it is a failed call without one, which `run_retries` and a skip
        // pass.
        if let Some(run) = &handed
            && let Some(why) =
                crate::dispatch::too_big_to_send(&message.message, self.settings.max_message_size)
        {
            self.dispatcher.trigger_released(&instance);
            self.db.hand_trigger_run(dag.dag.id.as_str(), run).await?;
            self.refuse_call(dag, &why, now).await?;
            return Ok(());
        }
        if let Some(run) = &handed
            && let Err(error) = self.db.hand_trigger_run(dag.dag.id.as_str(), run).await
        {
            self.dispatcher.trigger_released(&instance);
            return Err(error.into());
        }
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

    /// The run an `@trigger(wait_for=...)` is called for next, as its function gets it (another
    /// DAG's output masked, as the API shows it): the one it was given and hasn't passed, while
    /// it is still of the time it is live; else the next that ended since. A run gone (deleted)
    /// is passed. For an `EventNamed` (decision 354), the same with the events of its name
    /// posted while it is live.
    async fn run_to_hand(
        &self,
        dag: &ActiveDag,
        wait: &SensorWaitFor,
    ) -> Result<Option<(RunOfSeen, serde_json::Value, bool)>, CoreError> {
        let id = dag.dag.id.as_str();
        let Some(live) = self.db.live_since(id).await? else {
            return Ok(None);
        };
        let source = source_of(wait);
        let run_of = match wait.source() {
            Some(WaitSource::RunOf(run_of)) => run_of,
            Some(WaitSource::Event(event)) => {
                return self.event_to_hand(id, event, &source, live).await;
            }
            None => return Ok(None),
        };
        let task = run_of.task.as_ref();
        if let Some(handed) = self.db.trigger_run_handed(id).await?
            && handed.at >= live
        {
            if let Some(result) = self.masked_result(&handed.run, task).await? {
                return Ok(Some((handed, result, false)));
            }
            self.db
                .pass_trigger_run(id, &source, Some(&gone(&handed.run)), None)
                .await?;
        }
        for _ in 0..GONE_RUNS_OF_A_LOOK {
            let seen = self.db.run_of_seen(id, &source).await?;
            let from = match &seen {
                Some(seen) if seen.at >= live => RunOfFrom::After(seen),
                _ => RunOfFrom::Since(live),
            };
            let found = self
                .db
                .ended_since((run_of.dag.as_str(), task.map(TaskId::as_str)), from, 1)
                .await?;
            let Some((run, _, ended)) = found.into_iter().next() else {
                return Ok(None);
            };
            let next = RunOfSeen { at: ended, run };
            if let Some(result) = self.masked_result(&next.run, task).await? {
                return Ok(Some((next, result, true)));
            }
            tracing::warn!(dag = id, run = next.run, "a run to hand is gone; passed");
            self.db.set_run_of_seen(id, &source, &next).await?;
        }
        Ok(None)
    }

    /// The event an `@trigger(wait_for=EventNamed(...))` is called for next (decision 354): the
    /// one it was given and hasn't passed, while of the time it is live; else the next of its
    /// name posted since, by when posted. The cursor holds an event's posting time and ID.
    async fn event_to_hand(
        &self,
        id: &str,
        event: &EventNamed,
        source: &str,
        live: Timestamp,
    ) -> Result<Option<(RunOfSeen, serde_json::Value, bool)>, CoreError> {
        if let Some(handed) = self.db.trigger_run_handed(id).await?
            && handed.at >= live
        {
            if let Some(found) = self.db.named_events_by_id(&handed.run).await? {
                return Ok(Some((handed, crate::dispatch::event_value(&found), false)));
            }
            self.db
                .pass_trigger_run(id, source, Some(&gone(&handed.run)), None)
                .await?;
        }
        // From a little before the last one passed, by what was passed (Codex's review of part
        // 1): a post's time is taken before its commit, so one can be stored behind it.
        let seen = self.db.run_of_seen(id, source).await?;
        let lookback =
            SignedDuration::try_from(self.settings.event_lookback).unwrap_or(SignedDuration::MAX);
        // Never below what was forgotten (a lookback raised meanwhile; Grok's review).
        let floor = self.db.passed_floor(id).await?;
        let since = seen
            .as_ref()
            .and_then(|seen| seen.at.checked_sub(lookback).ok())
            .map_or(live, |from| from.max(live))
            .max(floor.unwrap_or(live));
        let found = self
            .db
            .named_event_unpassed(event.name.as_str(), id, since)
            .await?;
        if found.is_some() {
            // Once an event, not each round: what is behind the window is never read again.
            self.db
                .forget_passed_before(id, event.name.as_str(), since)
                .await?;
        }
        Ok(found.map(|found| {
            let value = crate::dispatch::event_value(&found);
            let next = RunOfSeen {
                at: found.posted_at,
                run: found.id,
            };
            (next, value, true)
        }))
    }

    /// A run's result as an `@trigger(wait_for=...)` or a sensor gets it: secret fields and
    /// passwords of its output masked (decision 131); `None` if the run is gone.
    async fn masked_result(
        &self,
        run: &str,
        task: Option<&TaskId>,
    ) -> Result<Option<serde_json::Value>, CoreError> {
        let mut result = self.db.run_result(run, task).await?;
        if let Some(output) = result.as_mut().and_then(|r| r.get_mut("output")) {
            schedr_config::masked::mask_data(output);
        }
        Ok(result)
    }

    /// What a worker says of a trigger.
    pub(super) async fn trigger_message(
        &mut self,
        worker_id: &str,
        message: worker_message::Message,
        now: Timestamp,
    ) -> Result<Vec<Outgoing>, CoreError> {
        // What it changes may be in memory only (a trigger stopping, a call done): the trigger
        // loop looks (Grok's review of M5.1f2).
        self.dispatcher.trigger_wake().notify_one();
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
                clock: None,
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
        let started = match batch_items(&found, &dag) {
            None => {
                self.scheduler
                    .event_run(&dag, &found, record.last_planned_for, now)
                    .await?
            }
            Some(Ok(items)) => {
                self.scheduler
                    .batch_run(&dag, &found, &items, record.last_planned_for, now)
                    .await?
            }
            Some(Err(why)) => {
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
        // Stored: done with the item its call was given (one dropped above is handed again).
        if let Some((wait, _)) = waits_for(&dag.dag) {
            self.db
                .pass_trigger_run_stored(&event.dag_id, &source_of(wait), &event.instance_id)
                .await?;
        }
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
                // Its call returned before the stop reached it: done with its run all the same,
                // unless it returned an event, dropped while it stopped: handed again (Codex's
                // review). Of the DAG it was given, never of the one the message names (Grok's
                // review).
                if stopped.done
                    && !stopped.returned_event
                    && let Some(active) = self.active_dag(&dag).await?
                    && let Some((wait, _)) = waits_for(&active.dag)
                {
                    self.call_done(&dag, wait, None).await?;
                }
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
        let dag = self.active_dag(&stopped.dag_id).await?;
        let waiting = dag.as_ref().and_then(|dag| waits_for(&dag.dag));
        if stopped.error_kind.is_empty() {
            self.dispatcher.trigger_released(&stopped.instance_id);
            // Done with the run it was given (decision 353), the call ended in the same
            // statement: the next call gets the next. Asked to stop, it keeps its run. One that
            // returned an event was passed when it was stored; if it wasn't, it is handed again.
            let passed = match (waiting, stopped.done && !stopped.returned_event) {
                (Some((wait, _)), true) => {
                    self.call_done(&stopped.dag_id, wait, Some(&stopped.instance_id))
                        .await?
                }
                _ => false,
            };
            if !passed {
                self.db
                    .trigger_ended(&stopped.dag_id, &stopped.instance_id, None, None)
                    .await?;
            }
            return Ok(Vec::new());
        }
        if stopped.error_kind == TRIGGER_RETURNED {
            let on = format!(
                "{} {}",
                record.dag_version_id.as_deref().unwrap_or_default(),
                record.code_hash.as_deref().unwrap_or_default()
            );
            // One write: the stop and its notification (Codex's review of M4.13a).
            let db = self.db.clone();
            Box::pin(db.atomically(async {
                self.db
                    .set_trigger_stopped(&stopped.dag_id, Some((STOPPED_RETURNED, &on)))
                    .await?;
                self.notify_trigger_failing(
                    &stopped.dag_id,
                    "its generator returned: it starts no more runs until the DAG changes",
                    now,
                )
                .await
            }))
            .await?;
        }
        let why = if stopped.message.is_empty() {
            stopped.error_kind
        } else {
            format!("{}: {}", stopped.error_kind, stopped.message)
        };
        // Its `run_retries` used up: the run is passed, recorded, and the next one called for.
        if let (Some(dag), Some((_, Some(retries)))) = (&dag, waiting)
            && record.failures >= retries
        {
            self.dispatcher.trigger_released(&stopped.instance_id);
            let tries = record.failures.saturating_add(1);
            let why = format!("failed {tries} times, the last: {why}");
            let call = Some(stopped.instance_id.as_str());
            if pass_handed_run(&self.db, &dag.dag, &why, call)
                .await?
                .is_err()
            {
                self.db
                    .trigger_ended(&stopped.dag_id, &stopped.instance_id, None, None)
                    .await?;
            }
            return Ok(Vec::new());
        }
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

    /// An `@trigger(wait_for=...)`'s call that can't be made for its run: a failure as a
    /// raise is, with its wait; past its `run_retries`, the run is passed.
    async fn refuse_call(
        &self,
        dag: &ActiveDag,
        why: &str,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let id = dag.dag.id.as_str();
        let failures = self.db.trigger(id).await?.map_or(0, |r| r.failures);
        if let Some((_, Some(retries))) = waits_for(&dag.dag)
            && failures >= retries
        {
            let tries = failures.saturating_add(1);
            let why = format!("failed {tries} times, the last: {why}");
            pass_handed_run(&self.db, &dag.dag, &why, None).await?.ok();
            return Ok(());
        }
        let wait = backoff(
            failures.saturating_add(1),
            self.settings.timings.trigger_restart_backoff,
            self.settings.timings.trigger_restart_backoff_max,
        );
        let wait = SignedDuration::try_from(wait).unwrap_or(SignedDuration::MAX);
        self.db
            .trigger_call_refused(id, &fit_error(why), now.checked_add(wait).ok())
            .await?;
        tracing::warn!(dag = id, why, "a trigger's call couldn't be made");
        Ok(())
    }

    /// A DAG as it is now, if it is.
    async fn active_dag(&self, dag_id: &str) -> Result<Option<ActiveDag>, CoreError> {
        let Ok(id) = DagId::new(dag_id) else {
            return Ok(None);
        };
        Ok(self.db.active_dags_of(&[&id]).await?.into_iter().next())
    }

    /// An `@trigger(wait_for=...)` of `dag_id` had its call return: its run is passed (with
    /// `call`, ending that call in the same statement), and, if it was, its strikes are cleared,
    /// as it works (Codex's review). Gives whether the run was passed.
    async fn call_done(
        &self,
        dag_id: &str,
        wait: &SensorWaitFor,
        call: Option<&str>,
    ) -> Result<bool, CoreError> {
        let passed = self
            .db
            .pass_trigger_run(dag_id, &source_of(wait), None, call)
            .await?;
        if passed
            && let Some(dag) = self.active_dag(dag_id).await?
            && let Trigger::Event { task, .. } = &dag.dag.trigger
        {
            self.db.clear_wait_step(dag_id, task.id.as_str()).await?;
        }
        Ok(passed)
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
        // A poke trigger that keeps failing stays alive: its channels hear of it as it crosses
        // `failing_after` (Codex's review of M4.13a), in the write that records it.
        let limit = self.settings.failing_after;
        let crossed = limit > 0 && record.failures < limit && poked.failures >= limit;
        let db = self.db.clone();
        Box::pin(db.atomically(async {
            self.db
                .trigger_poked(
                    &record.dag_id,
                    &poked.instance_id,
                    at,
                    error.as_deref(),
                    poked.failures,
                )
                .await?;
            if crossed && let Some(why) = &error {
                self.notify_trigger_failing(&record.dag_id, why, now)
                    .await?;
            }
            Ok::<_, CoreError>(())
        }))
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
        // `failing_after` failures in a row: its channels hear of it once, as it crosses.
        let failing = self.settings.failing_after > 0
            && failures.saturating_add(1) == self.settings.failing_after
            && !why.starts_with(TRIGGER_RETURNED);
        let db = self.db.clone();
        Box::pin(db.atomically(async {
            self.db
                .trigger_ended(dag_id, instance, Some(&why), now.checked_add(wait).ok())
                .await?;
            if failing {
                self.notify_trigger_failing(dag_id, &why, now).await?;
            }
            Ok::<_, CoreError>(())
        }))
        .await?;
        tracing::warn!(dag = dag_id, why, "a trigger failed");
        Ok(())
    }

    /// Queues `TRIGGER_FAILING` for DAG `dag_id`'s channels.
    async fn notify_trigger_failing(
        &self,
        dag_id: &str,
        why: &str,
        now: Timestamp,
    ) -> Result<(), CoreError> {
        let Ok(id) = DagId::new(dag_id) else {
            return Ok(());
        };
        let Some(active) = self.db.active_dags_of(&[&id]).await?.into_iter().next() else {
            return Ok(());
        };
        if active.dag.notify.is_empty() {
            return Ok(());
        }
        let mut event = crate::notifications::Event::new(&active.dag, now);
        event.error = Some(why.to_owned());
        let queued = self.notifications.queued(
            &event,
            schedr_model::notify::NotifyEvent::TriggerFailing,
            &self.metrics,
        );
        self.db.queue_notifications(&queued, now).await?;
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

/// Passes the run an `@trigger(wait_for=...)` of `dag` was given without its function having
/// ended well with it (`schedr triggers skip`, or its `run_retries` used up): its next call gets
/// the next run. Refused while a call of it runs. Gives the run passed, or why none was.
///
/// # Errors
///
/// [`DbError`] if the store fails.
pub(crate) async fn pass_handed_run(
    db: &Database,
    dag: &schedr_model::Dag,
    why: &str,
    call: Option<&str>,
) -> Result<Result<String, String>, DbError> {
    let id = dag.id.as_str();
    let Some((wait, _)) = waits_for(dag) else {
        return Ok(Err(format!(
            "DAG `{id}`: its trigger isn't called for another DAG's runs (`wait_for=`)"
        )));
    };
    let Some(handed) = db.trigger_run_handed(id).await? else {
        return Ok(Err(format!("DAG `{id}`: its trigger has no run to skip")));
    };
    let why = fit_error(&format!("passed run {}: {why}", handed.run));
    if !db
        .pass_trigger_run(id, &source_of(wait), Some(&why), call)
        .await?
    {
        return Ok(Err(format!(
            "DAG `{id}`: its trigger is being called with run {}; skip once that call has ended",
            handed.run
        )));
    }
    tracing::warn!(
        dag = id,
        run = handed.run,
        why,
        "a trigger's run was passed"
    );
    Ok(Ok(handed.run))
}

/// Why a run handed to a trigger was passed: it is gone.
fn gone(run: &str) -> String {
    format!("passed run {run}: it is gone")
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

/// A batch's items (`@trigger.poke(batch=...)`, decision 352), each read as an event and kept
/// as stored, its `at` a tagged `datetime` or `null`; `None` for a trigger that doesn't batch.
fn batch_items(
    found: &RunEvent,
    dag: &ActiveDag,
) -> Option<Result<Vec<(RunEvent, serde_json::Value)>, String>> {
    let Trigger::Event { task, .. } = &dag.dag.trigger else {
        return None;
    };
    if !matches!(
        task.kind,
        TaskKind::Sensor {
            batch_ms: Some(_),
            ..
        }
    ) {
        return None;
    }
    let refused = |why: &str| format!("event_refused: batch `{}`: {why}", found.key);
    let Some(items) = found.data.as_array().filter(|items| !items.is_empty()) else {
        return Some(Err(refused("its data isn't a list of events")));
    };
    Some(
        items
            .iter()
            .map(|raw| {
                let key = raw["key"]
                    .as_str()
                    .ok_or_else(|| refused("an item has no key"))?;
                check_event_key(key).map_err(|why| refused(&why))?;
                let at = match &raw["at"] {
                    serde_json::Value::Null => None,
                    tagged => Some(
                        tagged["$datetime"]
                            .as_str()
                            .and_then(|at| at.parse::<Timestamp>().ok())
                            .ok_or_else(|| refused("an item's `at` doesn't read"))?,
                    ),
                };
                let event = RunEvent {
                    key: key.to_owned(),
                    data: raw.get("data").cloned().unwrap_or(serde_json::Value::Null),
                    at,
                };
                Ok((event, raw.clone()))
            })
            .collect(),
    )
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
            clock: None,
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
