//! `RUN_STARTED` (M5.2i8, DN18): each run started and not yet announced is announced by the
//! round's rest, its notifications queued with the mark in one transaction, however it started
//! (the scheduler, a backfill, a trigger, another DAG's run, a person) and whenever (at once, or
//! a queued run past `max_active_runs` later), a master's restart included. `EVENT_POSTED` the
//! same way: each posted event, once, to the DAGs listening for its name.

use jiff::Timestamp;
use schedr_db::named_events::NamedEvent;
use schedr_db::notifications::StartedRun;
use schedr_model::dag::Listens;
use schedr_model::notify::NotifyEvent;
use schedr_model::states::RunKind;
use serde_json::{Value, json};

use super::{CoreError, MasterCore};
use crate::notifications::Event;

/// The most runs a round announces; the rest next round.
pub const ANNOUNCED_A_ROUND: u32 = 200;

/// What a run's `RUN_STARTED` says of how it started (`notify.details`, DN28).
#[must_use]
pub fn started_details(run: &StartedRun) -> Value {
    let chain = |via: &str, (run_id, dag): &(String, String)| {
        // The task that started it isn't kept with the run: null.
        json!({"reason": "chain", "via": via, "dag": dag, "run": run_id, "task": null})
    };
    match (run.kind, &run.parent) {
        (RunKind::Scheduled, _) => json!({"reason": "schedule"}),
        (RunKind::Manual, _) => json!({"reason": "manual", "by": run.triggered_by}),
        (RunKind::Backfill, _) => match &run.backfill {
            Some((id, from, to)) => json!({
                "reason": "backfill",
                "backfill": id,
                "from": from.to_string(),
                "to": to.to_string(),
            }),
            None => json!({"reason": "backfill"}),
        },
        (RunKind::Chained, Some(parent)) => chain("triggered", parent),
        (RunKind::Chained, None) => json!({"reason": "chain", "via": "triggered"}),
        // A `RunOf` trigger's run: another DAG's run started it.
        (RunKind::Event, Some(parent)) => chain("run_of", parent),
        (RunKind::Event, None) => json!({"reason": "trigger", "key": run.event_key}),
    }
}

/// What a posted event's `EVENT_POSTED` says of it to `listener` (`notify.details`, DN28): that
/// it occurred, its name and key, not its data nor whether it starts a run.
#[must_use]
pub fn posted_details(event: &NamedEvent, listens: Listens) -> Value {
    json!({
        "name": event.name,
        "key": event.key,
        "listens": match listens {
            Listens::Trigger => "trigger",
            Listens::WaitStep => "wait_step",
        },
        "posted_by": event.posted_by,
    })
}

impl MasterCore {
    /// Announces the runs started since the last time (the round's rest).
    ///
    /// # Errors
    ///
    /// [`CoreError`] on a database failure: nothing is marked, tried again next round.
    pub(super) async fn announce_runs(&self, now: Timestamp) -> Result<(), CoreError> {
        let runs = self.db.unannounced_runs(ANNOUNCED_A_ROUND).await?;
        if runs.is_empty() {
            return Ok(());
        }
        let mut queued = Vec::new();
        for run in &runs {
            let dag = self.db.dag_version(&run.dag_version_id).await?;
            if dag.notify.is_empty() {
                continue;
            }
            let mut event = Event::new(&dag, now);
            event.run_id = Some(&run.id);
            event.planned_for = Some(run.planned_for);
            event.state = Some("running");
            event.details = started_details(run);
            queued.extend(self.notifications.queued(
                &event,
                NotifyEvent::RunStarted,
                &self.metrics,
            ));
        }
        let ids: Vec<String> = runs.into_iter().map(|run| run.id).collect();
        let db = &self.db;
        Box::pin(db.atomically(async {
            db.queue_notifications(&queued, now).await?;
            db.mark_announced(&ids).await
        }))
        .await?;
        Ok(())
    }

    /// Announces the events posted since the last time to the DAGs listening for them (the
    /// round's rest).
    ///
    /// # Errors
    ///
    /// [`CoreError`] on a database failure: nothing is marked, tried again next round.
    pub(super) async fn announce_events(&self, now: Timestamp) -> Result<(), CoreError> {
        let events = self.db.unannounced_named_events(ANNOUNCED_A_ROUND).await?;
        if events.is_empty() {
            return Ok(());
        }
        // Who listens is read from each DAG's code, its version cached (agy's review of
        // M5.2i8: a running sensor's, a trigger's `wait_for`, a step ended a moment before were
        // missed when read from its instances): removed and conflicting DAGs aren't listed.
        let dags = self.db.active_dags().await?;
        let mut queued = Vec::new();
        for posted in &events {
            for active in &dags {
                let Some(listens) = active.dag.listens_for(&posted.name) else {
                    continue;
                };
                if active.dag.notify.is_empty() {
                    continue;
                }
                let mut event = Event::new(&active.dag, now);
                event.details = posted_details(posted, listens);
                queued.extend(self.notifications.queued(
                    &event,
                    NotifyEvent::EventPosted,
                    &self.metrics,
                ));
            }
        }
        let ids: Vec<String> = events.into_iter().map(|event| event.id).collect();
        let db = &self.db;
        Box::pin(db.atomically(async {
            db.queue_notifications(&queued, now).await?;
            db.mark_events_announced(&ids).await
        }))
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn run(kind: RunKind) -> StartedRun {
        StartedRun {
            id: "r-2".into(),
            dag_version_id: "v".into(),
            kind,
            triggered_by: "ana".into(),
            planned_for: "2026-11-22T10:00:00Z".parse().unwrap(),
            event_key: None,
            parent: None,
            backfill: None,
        }
    }

    #[test]
    fn a_runs_reason_is_read_from_how_it_was_made() {
        assert_eq!(
            started_details(&run(RunKind::Scheduled)),
            json!({"reason": "schedule"})
        );
        assert_eq!(
            started_details(&run(RunKind::Manual)),
            json!({"reason": "manual", "by": "ana"})
        );
        let mut backfill = run(RunKind::Backfill);
        backfill.backfill = Some((
            "b-1".into(),
            "2026-11-01T00:00:00Z".parse().unwrap(),
            "2026-11-03T00:00:00Z".parse().unwrap(),
        ));
        assert_eq!(
            started_details(&backfill),
            json!({"reason": "backfill", "backfill": "b-1",
                   "from": "2026-11-01T00:00:00Z", "to": "2026-11-03T00:00:00Z"})
        );
        let mut chained = run(RunKind::Chained);
        chained.parent = Some(("r-1".into(), "upstream".into()));
        assert_eq!(
            started_details(&chained),
            json!({"reason": "chain", "via": "triggered", "dag": "upstream", "run": "r-1",
                   "task": null})
        );
        let mut run_of = run(RunKind::Event);
        run_of.parent = Some(("r-1".into(), "upstream".into()));
        assert_eq!(started_details(&run_of)["via"], "run_of");
        let mut triggered = run(RunKind::Event);
        triggered.event_key = Some("order-7".into());
        assert_eq!(
            started_details(&triggered),
            json!({"reason": "trigger", "key": "order-7"})
        );
    }
}
