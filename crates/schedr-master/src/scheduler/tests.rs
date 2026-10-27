use pretty_assertions::assert_eq;
use schedr_db::dags::{DagChange, NewParse};
use schedr_model::Dag;
use serde_json::{Value as Json, json};
use sqlx::AssertSqlSafe;

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

fn sync() -> DagId {
    DagId::new("sync").unwrap()
}

/// A daily 03:00 UTC DAG from 2026-10-01, changed by `changes`.
fn dag(changes: &Json) -> Dag {
    let mut dag = json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": {"kind": "cron", "expression": "0 3 * * *", "timezone": "UTC", "catchup": false, "start": "2026-10-01T00:00:00Z"},
        "initial_since": null,
        "max_active_runs": 1, "tasks": [], "edges": []
    });
    for (key, value) in changes.as_object().unwrap() {
        dag[key] = value.clone();
    }
    serde_json::from_value(dag).unwrap()
}

/// A scheduler over a fresh database holding `dag` (paused, as new DAGs are).
async fn scheduler_with(dag: Dag) -> (Scheduler, Database) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts("2026-09-15T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let change = DagChange::Define {
        dag: Box::new(dag),
        parse_id,
        pause: false,
    };
    db.apply_dag_changes(&[change], ts("2026-09-15T00:00:00Z"))
        .await
        .unwrap();
    (Scheduler::new(db.clone()), db)
}

/// Runs test SQL; the statements are fixed strings written in this file, so they are safe.
async fn execute(db: &Database, query: &str) {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query(AssertSqlSafe(query.to_owned()))
        .execute(pool)
        .await
        .unwrap();
}

fn planned(pass: &Pass) -> Vec<String> {
    pass.created
        .iter()
        .map(|r| r.times.planned_for.to_string())
        .collect()
}

#[tokio::test]
async fn a_paused_dag_gets_no_scheduled_runs() {
    let (scheduler, _db) = scheduler_with(dag(&json!({}))).await;
    let pass = scheduler.pass(ts("2026-10-21T08:00:00Z")).await.unwrap();
    assert_eq!(planned(&pass), Vec::<String>::new());
}

#[tokio::test]
async fn an_unpaused_dag_gets_its_most_recent_fire_time_once() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    let now = ts("2026-10-21T08:00:00Z");
    let pass = scheduler.pass(now).await.unwrap();
    assert_eq!(planned(&pass), ["2026-10-21T03:00:00Z"]);
    let run = &pass.created[0];
    assert_eq!(run.kind, RunKind::Scheduled);
    assert_eq!(run.times.since, ts("2026-10-20T03:00:00Z"));
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_fire_time_at_the_limit_is_missed_and_the_latest_runs_when_a_place_frees() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    scheduler.pass(ts("2026-10-20T03:00:00Z")).await.unwrap();
    // Still running a day and a half later: 10-21 is missed.
    assert_eq!(
        planned(&scheduler.pass(ts("2026-10-21T15:00:00Z")).await.unwrap()),
        Vec::<String>::new()
    );
    execute(&db, "UPDATE runs SET state = 'success'").await;
    assert_eq!(
        planned(&scheduler.pass(ts("2026-10-22T04:00:00Z")).await.unwrap()),
        ["2026-10-22T03:00:00Z"]
    );
}

#[tokio::test]
async fn with_catchup_missed_fire_times_run_in_order() {
    let (scheduler, db) = scheduler_with(dag(&json!({"trigger": {
            "kind": "cron", "expression": "0 3 * * *", "timezone": "UTC", "catchup": true,
            "start": "2026-10-01T00:00:00Z"
        }, "max_active_runs": 2})))
    .await;
    db.set_paused(&sync(), false).await.unwrap();
    let now = ts("2026-10-04T08:00:00Z");
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()),
        ["2026-10-01T03:00:00Z", "2026-10-02T03:00:00Z"]
    );
    execute(&db, "UPDATE runs SET state = 'success'").await;
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()),
        ["2026-10-03T03:00:00Z", "2026-10-04T03:00:00Z"]
    );
}

#[tokio::test]
async fn manual_runs_of_a_paused_dag_run_and_wait_for_a_place() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    let now = ts("2026-10-21T08:00:00Z");
    let first = scheduler.trigger(&sync(), None, "anna", now).await.unwrap();
    assert_eq!(first.kind, RunKind::Manual);
    assert_eq!(first.times.planned_for, now);
    assert_eq!(first.times.since, ts("2026-10-21T03:00:00Z"));
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);

    let later = ts("2026-10-21T09:00:00Z");
    let second = scheduler
        .trigger(&sync(), None, "anna", later)
        .await
        .unwrap();
    assert_eq!(second.times.since, now, "since the nearest earlier run");
    assert_eq!(db.queued_runs(&sync()).await.unwrap().len(), 1);

    execute(
        &db,
        "UPDATE runs SET state = 'success' WHERE state = 'running'",
    )
    .await;
    let pass = scheduler.pass(later).await.unwrap();
    assert_eq!(pass.started, [second.run_id]);
    assert_eq!(db.queued_runs(&sync()).await.unwrap(), []);
}

#[tokio::test]
async fn scheduled_runs_take_a_freed_place_before_queued_ones() {
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    scheduler.pass(ts("2026-10-20T04:00:00Z")).await.unwrap();
    scheduler
        .trigger(
            &sync(),
            Some(ts("2026-10-18T03:00:00Z")),
            "anna",
            ts("2026-10-20T05:00:00Z"),
        )
        .await
        .unwrap();
    execute(
        &db,
        "UPDATE runs SET state = 'success' WHERE state = 'running'",
    )
    .await;
    let pass = scheduler.pass(ts("2026-10-21T04:00:00Z")).await.unwrap();
    assert_eq!(planned(&pass), ["2026-10-21T03:00:00Z"]);
    assert_eq!(pass.started, Vec::<String>::new());
    assert_eq!(db.queued_runs(&sync()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_manual_run_for_a_fire_time_gets_its_period() {
    let (scheduler, _db) = scheduler_with(dag(&json!({}))).await;
    let run = scheduler
        .trigger(
            &sync(),
            Some(ts("2026-10-15T03:00:00Z")),
            "anna",
            ts("2026-10-21T08:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(run.times.since, ts("2026-10-14T03:00:00Z"));
}

#[tokio::test]
async fn triggers_that_cant_run_are_errors() {
    let (scheduler, _db) = scheduler_with(dag(&json!({}))).await;
    let now = ts("2026-10-21T08:00:00Z");
    let error = scheduler
        .trigger(&sync(), Some(ts("2026-10-22T00:00:00Z")), "anna", now)
        .await
        .unwrap_err();
    assert!(matches!(error, SchedulerError::ManualRun(_)), "{error}");
    let other = DagId::new("other").unwrap();
    let error = scheduler
        .trigger(&other, None, "anna", now)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "DAG `other` can't run: it doesn't exist, was removed or is in conflict"
    );
}

#[tokio::test]
async fn a_dag_whose_schedule_cant_be_evaluated_is_skipped() {
    let broken = json!({"kind": "cron", "expression": "0 3 * * *", "timezone": "Mars/Base", "catchup": false});
    let (scheduler, db) = scheduler_with(dag(&json!({"trigger": broken}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    let pass = scheduler.pass(ts("2026-10-21T08:00:00Z")).await.unwrap();
    assert_eq!(pass.created, []);
    assert_eq!(
        pass.skipped[0].to_string(),
        "DAG `sync`: unknown time zone `Mars/Base`"
    );
    let error = scheduler
        .trigger(&sync(), None, "anna", ts("2026-10-21T08:00:00Z"))
        .await
        .unwrap_err();
    assert!(matches!(error, SchedulerError::FireTimes { .. }), "{error}");
}

#[tokio::test]
async fn a_manual_run_triggered_again_with_its_id_is_the_same_run() {
    // B7: `trigger_run` passes its command's ID, so applying it again creates nothing new.
    let (scheduler, db) = scheduler_with(dag(&json!({}))).await;
    let now = ts("2026-10-21T08:00:00Z");
    let id = "01a16884-4f28-7563-9d35-7d0555a017f1";
    let first = scheduler
        .trigger_as(Some(id), &sync(), None, "anna", now)
        .await
        .unwrap();
    let again = scheduler
        .trigger_as(Some(id), &sync(), None, "anna", now)
        .await
        .unwrap();
    assert_eq!((first.run_id.as_str(), again.run_id.as_str()), (id, id));
    assert_eq!(db.running_count(&sync()).await.unwrap(), 1);
}

#[tokio::test]
async fn a_dag_catching_up_gets_at_most_so_many_runs_a_round() {
    // B18: a DAG catching up after a long pause had every due run created in one round.
    let (scheduler, db) = scheduler_with(dag(&json!({
        "trigger": {"kind": "cron", "expression": "0 * * * *", "timezone": "UTC",
                    "catchup": true, "start": "2026-10-01T00:00:00Z"},
        "max_active_runs": 1000
    })))
    .await;
    execute(&db, "UPDATE dags SET paused = FALSE").await;
    let now = ts("2026-10-03T00:30:00Z");
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()).len(),
        SCHEDULED_PER_ROUND
    );
    assert_eq!(
        planned(&scheduler.pass(now).await.unwrap()).len(),
        SCHEDULED_PER_ROUND
    );
}

/// The comparison test's DAGs (PROJECT.md, "Quiet DAGs"): each kind of trigger, some quiet
/// most of the time.
fn timeline_dags() -> Vec<Dag> {
    let cron = |expression: &str, catchup: bool, start: &str| {
        json!({"kind": "cron", "expression": expression, "timezone": "UTC",
               "catchup": catchup, "start": start})
    };
    let every =
        |seconds: u64| json!({"kind": "every", "interval_ms": seconds * 1000, "catchup": false});
    [
        ("daily", cron("0 3 * * *", false, "2026-10-01T00:00:00Z")),
        ("every5", every(300)),
        ("every90", every(90)),
        ("hourly", cron("0 * * * *", true, "2026-10-26T20:00:00Z")),
        (
            "once",
            json!({"kind": "once", "at": "2026-10-27T02:30:00Z"}),
        ),
        ("manual", json!({"kind": "none"})),
        ("later", cron("*/20 * * * *", false, "2026-10-01T00:00:00Z")),
    ]
    .into_iter()
    .map(|(id, trigger)| {
        let mut dag = dag(&json!({"trigger": trigger}));
        dag.id = DagId::new(id).unwrap();
        dag
    })
    .collect()
}

/// A database holding the timeline's DAGs, every one unpaused but `later`.
async fn timeline_db() -> Database {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.migrate().await.unwrap();
    let parse_id = db
        .insert_parse(&NewParse {
            root: "etl",
            content_hash: "h1",
            schedr_version: "0.1.0",
            worker_id: "w1",
            parsed_at: ts("2026-10-27T00:00:00Z"),
            files: &[],
        })
        .await
        .unwrap();
    let changes: Vec<DagChange> = timeline_dags()
        .into_iter()
        .map(|dag| DagChange::Define {
            dag: Box::new(dag),
            parse_id: parse_id.clone(),
            pause: false,
        })
        .collect();
    db.apply_dag_changes(&changes, ts("2026-10-27T00:00:00Z"))
        .await
        .unwrap();
    for dag in timeline_dags().iter().filter(|d| d.id.as_str() != "later") {
        db.set_paused(&dag.id, false).await.unwrap();
    }
    db
}

/// The IDs of the running runs, in one query.
async fn running(db: &Database) -> Vec<String> {
    let schedr_db::database::Pool::Sqlite(pool) = db.pool() else {
        panic!("expected SQLite")
    };
    sqlx::query_scalar("SELECT id FROM runs WHERE state = 'running'")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// Three hours of rounds every 20 s through `scheduler`: runs end 30 s after they start
/// (waking their DAG, as execution does), `manual` is triggered twice and `later` unpaused
/// (waking it, as the commander does). Every run at the end, without its ID.
async fn timeline(scheduler: &Scheduler, db: &Database) -> Vec<String> {
    timeline_marking(scheduler, db, true).await
}

/// As [`timeline`], the unpause marked only if `mark_unpause` (else a missed mark).
async fn timeline_marking(scheduler: &Scheduler, db: &Database, mark_unpause: bool) -> Vec<String> {
    let start = ts("2026-10-27T01:00:00Z");
    let manual = DagId::new("manual").unwrap();
    let later = DagId::new("later").unwrap();
    for step in 0..=540 {
        let now = start + jiff::SignedDuration::from_secs(step * 20);
        match step {
            90 | 285 => {
                scheduler.trigger(&manual, None, "me", now).await.unwrap();
            }
            210 => {
                db.set_paused(&later, false).await.unwrap();
                if mark_unpause {
                    scheduler.wakes().wake(&later, WakeReason::Command);
                }
            }
            _ => {}
        }
        scheduler.pass(now).await.unwrap();
        for run_id in running(db).await {
            let run = db.run_record(&run_id).await.unwrap().unwrap();
            let ran_long = run
                .started_at
                .is_some_and(|at| now.duration_since(at) >= jiff::SignedDuration::from_secs(30));
            if ran_long {
                db.finish_run(&run_id, RunState::Success, now)
                    .await
                    .unwrap();
                scheduler.wakes().wake(&run.dag_id, WakeReason::RunEnded);
            }
        }
    }
    let mut runs = Vec::new();
    for dag in timeline_dags() {
        for run in db.dag_runs(&dag.id, None, 1000).await.unwrap() {
            runs.push(format!(
                "{} {} for {} at {} started {:?} ended {:?} {}",
                run.dag_id,
                run.kind,
                run.planned_for,
                run.triggered_at,
                run.started_at,
                run.ended_at,
                run.state
            ));
        }
    }
    runs.sort();
    runs
}

#[tokio::test]
async fn quiet_dags_create_the_same_runs_at_the_same_times_as_visiting_every_dag() {
    let every_round = QuietSettings {
        enabled: false,
        ..QuietSettings::default()
    };
    let db = timeline_db().await;
    let full = timeline(
        &Scheduler::with_quiet(db.clone(), every_round, Wakes::default()),
        &db,
    )
    .await;
    let db = timeline_db().await;
    let quiet = Scheduler::with_quiet(db.clone(), QuietSettings::default(), Wakes::default());
    let metrics = crate::metrics::MasterMetrics::default();
    quiet.set_metrics(metrics.clone());
    let split = timeline(&quiet, &db).await;
    // The quiet path was taken: DAGs turned quiet and were woken by their timers and marks,
    // and the sweep never found one a mark had missed.
    let wakes = |reason: &str| {
        metrics
            .dag_wakes
            .get_or_create(&crate::metrics::ReasonLabels {
                reason: reason.to_owned(),
            })
            .get()
    };
    assert!(wakes("timer") > 0, "no timer wakes");
    assert!(wakes("run_ended") + wakes("command") > 0, "no marks");
    assert_eq!(wakes("sweep"), 0);
    // Every DAG ran: the timeline exercises each trigger.
    for dag in timeline_dags() {
        assert!(
            full.iter()
                .any(|run| run.starts_with(&format!("{} ", dag.id))),
            "{} never ran:\n{full:#?}",
            dag.id
        );
    }
    assert!(full.len() > 100, "{}", full.len());
    assert_eq!(split, full);
}

#[tokio::test]
async fn a_queued_run_wakes_its_quiet_dag_whatever_stored_it() {
    let (scheduler, db) = scheduler_with(dag(&json!({"trigger": {"kind": "none"}}))).await;
    db.set_paused(&sync(), false).await.unwrap();
    let start = ts("2026-10-27T12:00:00Z");
    let after = |seconds| start + jiff::SignedDuration::from_secs(seconds);
    scheduler.pass(start).await.unwrap();
    scheduler.pass(after(60)).await.unwrap();
    // Quiet now; a chained or event run (M4) is stored queued without a mark.
    let active = db.active_dags().await.unwrap().remove(0);
    let times = RunTimes {
        planned_for: after(61),
        since: after(61),
    };
    let mut run = new_run(&active, RunKind::Event, times, after(61), "an event");
    run.state = RunState::Queued;
    let run_id = db.insert_run(&run).await.unwrap().unwrap();
    // The next round starts it: the queued run woke its DAG.
    let pass = scheduler.pass(after(62)).await.unwrap();
    assert_eq!(pass.started, [run_id]);
}

#[tokio::test]
async fn the_sweep_catches_a_missed_mark_within_the_interval() {
    let every_round = QuietSettings {
        enabled: false,
        ..QuietSettings::default()
    };
    let db = timeline_db().await;
    let full = timeline(
        &Scheduler::with_quiet(db.clone(), every_round, Wakes::default()),
        &db,
    )
    .await;
    // The same timeline, the unpause's mark missed: only the sweep finds `later`.
    let db = timeline_db().await;
    let quiet = Scheduler::with_quiet(db.clone(), QuietSettings::default(), Wakes::default());
    let metrics = crate::metrics::MasterMetrics::default();
    quiet.set_metrics(metrics.clone());
    let missed = timeline_marking(&quiet, &db, false).await;
    let sweeps = metrics
        .dag_wakes
        .get_or_create(&crate::metrics::ReasonLabels {
            reason: "sweep".to_owned(),
        })
        .get();
    assert!(sweeps >= 1, "the sweep found nothing");
    // Every other DAG's runs are the same; `later`'s first run comes at most an interval
    // (and a round) late, for the same fire time, and the rest of its runs are the same.
    let others = |runs: &[String]| -> Vec<String> {
        runs.iter()
            .filter(|r| !r.starts_with("later "))
            .cloned()
            .collect()
    };
    assert_eq!(others(&missed), others(&full));
    let later = |runs: &[String]| -> Vec<String> {
        runs.iter()
            .filter(|r| r.starts_with("later "))
            .cloned()
            .collect()
    };
    let (late, on_time) = (later(&missed), later(&full));
    assert_eq!(late.len(), on_time.len());
    assert_eq!(late[1..], on_time[1..]);
    let field = |run: &str, name: &str| -> Timestamp {
        let rest = run.split(&format!(" {name} ")).nth(1).unwrap();
        rest.split(' ').next().unwrap().parse().unwrap()
    };
    assert_eq!(field(&late[0], "for"), field(&on_time[0], "for"));
    let delay = field(&late[0], "at").duration_since(field(&on_time[0], "at"));
    assert!(
        delay > jiff::SignedDuration::ZERO && delay <= jiff::SignedDuration::from_secs(80),
        "{delay:?}"
    );
}
