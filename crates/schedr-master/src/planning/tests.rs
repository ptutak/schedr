use pretty_assertions::assert_eq;
use serde_json::{Value as Json, json};

use super::*;

fn ts(text: &str) -> Timestamp {
    text.parse().unwrap()
}

/// The daily 03:00 UTC trigger starting on 2026-10-01, changed by `changes`.
fn daily(changes: &Json) -> Json {
    let mut trigger = json!({
        "kind": "cron", "expression": "0 3 * * *", "timezone": "UTC", "catchup": false,
        "start": "2026-10-01T00:00:00Z"
    });
    for (key, value) in changes.as_object().unwrap() {
        trigger[key] = value.clone();
    }
    trigger
}

/// A DAG with a daily 03:00 UTC trigger, changed by `changes`.
fn dag(changes: &Json) -> Dag {
    let mut dag = json!({
        "produced_by": "0.1", "id": "sync", "location": {"root": "etl", "path": "a.py"},
        "trigger": daily(&json!({})),
        "initial_since": null,
        "max_active_runs": 1, "tasks": [], "edges": []
    });
    for (key, value) in changes.as_object().unwrap() {
        dag[key] = value.clone();
    }
    serde_json::from_value(dag).unwrap()
}

/// Runs `check` with the plan of `dag` (first stored on 2026-09-15).
fn with_plan<T>(dag: &Dag, check: impl FnOnce(&Plan<'_>) -> T) -> T {
    let fire_times = FireTimes::new(&dag.trigger).unwrap();
    let plan = Plan {
        dag,
        fire_times: &fire_times,
        first_seen: ts("2026-09-15T00:00:00Z"),
    };
    check(&plan)
}

fn times(planned_for: &str, since: &str) -> RunTimes {
    RunTimes {
        planned_for: ts(planned_for),
        since: ts(since),
    }
}

#[test]
fn a_fire_time_run_covers_one_period() {
    with_plan(&dag(&json!({})), |plan| {
        assert_eq!(
            plan.fire_time_since(ts("2026-10-15T03:00:00Z")),
            ts("2026-10-14T03:00:00Z")
        );
        // The first fire time at or after `start` has an ordinary period too.
        assert_eq!(
            plan.fire_time_since(ts("2026-10-01T03:00:00Z")),
            ts("2026-09-30T03:00:00Z")
        );
    });
}

#[test]
fn initial_since_applies_to_the_first_fire_time_only() {
    let with_history = dag(&json!({"initial_since": "2020-01-01T00:00:00Z"}));
    with_plan(&with_history, |plan| {
        assert_eq!(
            plan.fire_time_since(ts("2026-10-01T03:00:00Z")),
            ts("2020-01-01T00:00:00Z")
        );
        assert_eq!(
            plan.fire_time_since(ts("2026-10-02T03:00:00Z")),
            ts("2026-10-01T03:00:00Z")
        );
    });
}

#[test]
fn a_once_schedule_uses_initial_since_or_an_empty_window() {
    let once = json!({"kind": "once", "at": "2026-12-24T22:00:00Z"});
    with_plan(&dag(&json!({"trigger": once})), |plan| {
        assert_eq!(
            plan.fire_time_since(ts("2026-12-24T22:00:00Z")),
            ts("2026-12-24T22:00:00Z")
        );
    });
    let with_since = dag(&json!({"trigger": once, "initial_since": "2026-12-01T00:00:00Z"}));
    with_plan(&with_since, |plan| {
        assert_eq!(
            plan.fire_time_since(ts("2026-12-24T22:00:00Z")),
            ts("2026-12-01T00:00:00Z")
        );
    });
}

#[test]
fn windows_around_a_dst_change_cover_the_real_day() {
    let warsaw = daily(&json!({"timezone": "Europe/Warsaw"}));
    with_plan(&dag(&json!({"trigger": warsaw})), |plan| {
        // 2026-10-25 has 25 hours in Warsaw: 03:00 CEST (01:00Z) to 03:00 CET (02:00Z).
        let planned_for = ts("2026-10-26T02:00:00Z");
        let since = plan.fire_time_since(planned_for);
        assert_eq!(since, ts("2026-10-25T02:00:00Z"));
        assert_eq!(
            plan.fire_time_since(since),
            ts("2026-10-24T01:00:00Z"),
            "the day of the change is 25 hours long"
        );
    });
}

#[test]
fn a_manual_run_on_a_fire_time_gets_its_period() {
    with_plan(&dag(&json!({})), |plan| {
        let run = plan
            .manual_times(
                Some(ts("2026-10-15T03:00:00Z")),
                ts("2026-10-21T08:00:00Z"),
                Some(ts("2026-10-15T00:00:00Z")),
            )
            .unwrap();
        assert_eq!(run, times("2026-10-15T03:00:00Z", "2026-10-14T03:00:00Z"));
    });
}

#[test]
fn a_manual_run_elsewhere_covers_since_the_nearest_earlier_run() {
    with_plan(&dag(&json!({})), |plan| {
        let now = ts("2026-10-21T08:00:00Z");
        assert_eq!(manual_planned_for(None, now), now);
        let run = plan
            .manual_times(None, now, Some(ts("2026-10-21T03:00:00Z")))
            .unwrap();
        assert_eq!(run, times("2026-10-21T08:00:00Z", "2026-10-21T03:00:00Z"));
        // No earlier run: the previous fire time.
        let first = plan.manual_times(None, now, None).unwrap();
        assert_eq!(first.since, ts("2026-10-21T03:00:00Z"));
    });
}

#[test]
fn a_first_manual_run_without_a_schedule_uses_initial_since_or_an_empty_window() {
    let manual = json!({"kind": "none"});
    let now = ts("2026-10-21T08:00:00Z");
    with_plan(&dag(&json!({"trigger": manual})), |plan| {
        assert_eq!(plan.manual_times(None, now, None).unwrap().since, now);
    });
    let with_since = dag(&json!({"trigger": manual, "initial_since": "2026-01-01T00:00:00Z"}));
    with_plan(&with_since, |plan| {
        assert_eq!(
            plan.manual_times(None, now, None).unwrap().since,
            ts("2026-01-01T00:00:00Z")
        );
    });
}

#[test]
fn for_in_the_future_is_rejected_beyond_the_tolerance() {
    with_plan(&dag(&json!({})), |plan| {
        let now = ts("2026-10-21T08:00:00Z");
        let skewed = ts("2026-10-21T08:00:59Z");
        assert!(plan.manual_times(Some(skewed), now, None).is_ok());
        let error = plan
            .manual_times(Some(ts("2026-10-22T03:00:00Z")), now, None)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "`--for` must not be in the future (2026-10-22T03:00:00Z is after now, \
             2026-10-21T08:00:00Z)"
        );
    });
}

#[test]
fn without_catchup_only_the_most_recent_fire_time_gets_a_run() {
    with_plan(&dag(&json!({})), |plan| {
        let now = ts("2026-10-21T08:00:00Z");
        assert_eq!(
            plan.scheduled_runs(Some(ts("2026-10-15T03:00:00Z")), 0, now),
            [times("2026-10-21T03:00:00Z", "2026-10-20T03:00:00Z")]
        );
        assert_eq!(
            plan.scheduled_runs(Some(ts("2026-10-21T03:00:00Z")), 0, now),
            []
        );
    });
}

#[test]
fn the_first_run_is_the_most_recent_fire_time_at_or_after_start() {
    with_plan(&dag(&json!({})), |plan| {
        assert_eq!(
            plan.scheduled_runs(None, 0, ts("2026-09-30T08:00:00Z")),
            [],
            "nothing before start"
        );
        assert_eq!(
            plan.scheduled_runs(None, 0, ts("2026-10-01T03:00:00Z")),
            [times("2026-10-01T03:00:00Z", "2026-09-30T03:00:00Z")]
        );
    });
}

#[test]
fn without_start_the_schedule_starts_when_the_dag_was_first_stored() {
    let no_start = dag(
        &json!({"trigger": daily(&json!({"catchup": true, "start": null})), "max_active_runs": null}),
    );
    with_plan(&no_start, |plan| {
        let runs = plan.scheduled_runs(None, 0, ts("2026-09-17T08:00:00Z"));
        let planned: Vec<String> = runs.iter().map(|r| r.planned_for.to_string()).collect();
        assert_eq!(
            planned,
            [
                "2026-09-15T03:00:00Z",
                "2026-09-16T03:00:00Z",
                "2026-09-17T03:00:00Z"
            ]
        );
    });
}

#[test]
fn with_catchup_every_missed_fire_time_gets_a_run_up_to_the_limit() {
    let catchup = dag(&json!({"trigger": daily(&json!({"catchup": true})), "max_active_runs": 2}));
    with_plan(&catchup, |plan| {
        let now = ts("2026-10-21T08:00:00Z");
        assert_eq!(
            plan.scheduled_runs(Some(ts("2026-10-15T03:00:00Z")), 0, now),
            [
                times("2026-10-16T03:00:00Z", "2026-10-15T03:00:00Z"),
                times("2026-10-17T03:00:00Z", "2026-10-16T03:00:00Z")
            ]
        );
        assert_eq!(
            plan.scheduled_runs(Some(ts("2026-10-15T03:00:00Z")), 1, now)
                .len(),
            1
        );
        assert_eq!(
            plan.scheduled_runs(Some(ts("2026-10-15T03:00:00Z")), 2, now),
            []
        );
    });
}

#[test]
fn at_the_limit_a_fire_time_is_missed() {
    with_plan(&dag(&json!({})), |plan| {
        assert_eq!(plan.scheduled_runs(None, 1, ts("2026-10-21T08:00:00Z")), []);
    });
}

#[test]
fn without_a_limit_a_pass_creates_a_bounded_number_of_runs() {
    let hourly = json!({
        "kind": "every", "interval_ms": 3_600_000, "catchup": true,
        "start": "2026-10-01T00:00:00Z"
    });
    let unlimited = dag(&json!({"trigger": hourly, "max_active_runs": null}));
    with_plan(&unlimited, |plan| {
        let runs = plan.scheduled_runs(None, 50, ts("2026-12-31T00:00:00Z"));
        assert_eq!(runs.len(), MAX_RUNS_PER_PASS);
        assert_eq!(runs[0].planned_for, ts("2026-10-01T00:00:00Z"));
    });
    assert_eq!(
        with_plan(&unlimited, |plan| plan.free_places(7)),
        usize::MAX
    );
}

#[test]
fn end_is_the_last_fire_time_scheduled() {
    let ended = dag(&json!({
        "trigger": daily(&json!({"catchup": true, "end": "2026-10-10T12:00:00Z"})),
        "max_active_runs": null
    }));
    with_plan(&ended, |plan| {
        let runs = plan.scheduled_runs(
            Some(ts("2026-10-08T03:00:00Z")),
            0,
            ts("2026-10-21T00:00:00Z"),
        );
        let planned: Vec<String> = runs.iter().map(|r| r.planned_for.to_string()).collect();
        assert_eq!(planned, ["2026-10-09T03:00:00Z", "2026-10-10T03:00:00Z"]);
        assert_eq!(
            plan.scheduled_runs(
                Some(ts("2026-10-10T03:00:00Z")),
                0,
                ts("2026-10-21T00:00:00Z")
            ),
            []
        );
    });
    let ended = dag(&json!({"trigger": daily(&json!({"end": "2026-10-10T12:00:00Z"}))}));
    with_plan(&ended, |plan| {
        assert_eq!(
            plan.scheduled_runs(None, 0, ts("2026-10-21T00:00:00Z")),
            [times("2026-10-10T03:00:00Z", "2026-10-09T03:00:00Z")]
        );
    });
}

#[test]
fn no_schedule_creates_no_scheduled_runs() {
    with_plan(&dag(&json!({"trigger": {"kind": "none"}})), |plan| {
        assert_eq!(plan.scheduled_runs(None, 0, ts("2026-10-21T00:00:00Z")), []);
    });
}

fn queued(id: &str, kind: RunKind, at: &str) -> QueuedRun {
    QueuedRun {
        id: id.to_owned(),
        kind,
        triggered_at: ts(at),
    }
}

#[test]
fn manual_chained_and_event_runs_start_before_backfills_oldest_first() {
    let waiting = [
        queued("b1", RunKind::Backfill, "2026-10-01T00:00:00Z"),
        queued("m2", RunKind::Manual, "2026-10-03T00:00:00Z"),
        queued("t1", RunKind::Chained, "2026-10-02T00:00:00Z"),
        queued("e1", RunKind::Event, "2026-10-02T12:00:00Z"),
        queued("b2", RunKind::Backfill, "2026-10-01T00:00:00Z"),
    ];
    let ids = |free| -> Vec<&str> {
        runs_to_start(&waiting, free)
            .iter()
            .map(|r| r.id.as_str())
            .collect()
    };
    assert_eq!(ids(10), ["t1", "e1", "m2", "b1", "b2"]);
    assert_eq!(ids(1), ["t1"]);
    assert_eq!(ids(0), Vec::<&str>::new());
    assert_eq!(priority(RunKind::Scheduled), 0);
}
