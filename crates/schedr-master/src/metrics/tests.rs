use pretty_assertions::assert_eq;
use prometheus_client::encoding::text::encode;
use schedr_model::metric_doc::scrape_mismatches;
use schedr_model::{AttemptStatus, RunState};

use super::*;

fn text(metrics: &MasterMetrics) -> String {
    let mut registry = Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    let mut out = String::new();
    encode(&mut out, &registry).unwrap();
    out
}

#[test]
fn every_metric_is_registered_as_documented() {
    // A label family shows once it has a member.
    let metrics = MasterMetrics::default();
    metrics.set_counts(&StateCounts {
        runs: vec![("running".to_owned(), "manual".to_owned(), 1)],
        tasks: vec![("queued".to_owned(), 1)],
        workers: vec![("active".to_owned(), 1)],
    });
    metrics.set_waiting(&[("no_slot", 1)]);
    metrics.attempt_ended(&AttemptStatus::Success);
    metrics.core_lock_waited("turn", Duration::from_millis(2));
    metrics.turn_part_done("started", 3, Duration::from_millis(5));
    metrics.round_part_done("assign", Duration::from_millis(7));
    metrics.extension_query_done("statuses", Duration::from_millis(1));
    metrics.count_extension_dropped("full", 1);
    metrics.run_finished("sync", &RunState::Success);
    metrics.run_late_failed("sync", "late_cleanup_failed");
    metrics.notification_sent("ops");
    metrics.notification_failed("ops");
    metrics.retention_deleted("runs", 2);
    metrics
        .rejections
        .get_or_create(&ReasonLabels {
            reason: "no_free_slot".to_owned(),
        })
        .inc();
    metrics
        .commands
        .get_or_create(&CommandLabels {
            kind: "trigger_run".to_owned(),
            decision: "applied".to_owned(),
        })
        .inc();
    metrics.set_dags(3, 600, 2);
    metrics.dag_woken("timer");
    metrics.set_triggers(crate::metrics::TriggerCounts::default());
    metrics.trigger_event(true);
    let out = text(&metrics);
    assert_eq!(
        scrape_mismatches(&out, Some("schedr"), METRICS),
        Vec::<String>::new(),
        "{out}"
    );
    for sample in [
        "schedr_master_dags{state=\"busy\"} 3",
        "schedr_master_dags{state=\"quiet\"} 600",
        "schedr_master_dags{state=\"held\"} 2",
        "schedr_master_dag_wakes_total{reason=\"timer\"} 1",
    ] {
        assert!(out.contains(sample), "{sample}\n{out}");
    }
}

#[test]
fn counts_set_the_gauges_and_replace_the_old_ones() {
    let metrics = MasterMetrics::default();
    metrics.set_counts(&StateCounts {
        runs: vec![("running".to_owned(), "manual".to_owned(), 2)],
        tasks: vec![("queued".to_owned(), 3)],
        workers: vec![("active".to_owned(), 1), ("lost".to_owned(), 1)],
    });
    let out = text(&metrics);
    assert!(
        out.contains("schedr_runs{state=\"running\",kind=\"manual\"} 2"),
        "{out}"
    );
    assert!(out.contains("schedr_tasks{state=\"queued\"} 3"), "{out}");
    assert!(out.contains("schedr_workers{state=\"lost\"} 1"), "{out}");
    // Emptied states read 0: they don't vanish.
    metrics.set_counts(&StateCounts::default());
    let out = text(&metrics);
    assert!(
        out.contains("schedr_runs{state=\"running\",kind=\"manual\"} 0"),
        "{out}"
    );
    assert!(out.contains("schedr_workers{state=\"active\"} 0"), "{out}");
}

#[test]
fn events_are_counted_with_bounded_labels() {
    let metrics = MasterMetrics::default();
    metrics.attempt_ended(&AttemptStatus::OomKilled);
    metrics.run_finished("sync", &RunState::Failed);
    metrics.set_waiting(&[("no_slot", 4)]);
    let out = text(&metrics);
    assert!(
        out.contains("schedr_attempts_ended_total{status=\"oom_killed\"} 1"),
        "{out}"
    );
    assert!(
        out.contains("schedr_runs_finished_total{dag_id=\"sync\",result=\"failed\"} 1"),
        "{out}"
    );
    assert!(
        out.contains("schedr_tasks_waiting{reason=\"no_slot\"} 4"),
        "{out}"
    );
    metrics.set_waiting(&[]);
    assert!(text(&metrics).contains("schedr_tasks_waiting{reason=\"no_slot\"} 0"));
}

#[test]
fn the_gauges_refresh_often_enough_for_a_scrape() {
    assert_eq!(GAUGE_REFRESH, Duration::from_secs(15));
}

/// M5.3e: a tick is broken down only while begun; its parts summed by name, the round's lock
/// waits beside them (a turn's not), the others' holds of the lock by holder, the rest
/// unaccounted.
#[test]
fn a_tick_is_broken_down_into_its_parts_and_lock_waits() {
    let metrics = MasterMetrics::default();
    metrics.round_part_done("assign", Duration::from_millis(500));
    assert_eq!(metrics.end_tick(), None, "not begun: nothing kept");
    metrics.begin_tick();
    metrics.round_part_done("assign", Duration::from_millis(300));
    metrics.round_part_done("results", Duration::from_millis(1200));
    metrics.round_part_done("assign", Duration::from_millis(100));
    metrics.core_lock_waited("round", Duration::from_millis(250));
    metrics.core_lock_waited("turn", Duration::from_secs(9));
    metrics.core_lock_held("turn", Duration::from_millis(700));
    metrics.core_lock_held("triggers", Duration::from_millis(200));
    metrics.core_lock_held("turn", Duration::from_millis(100));
    metrics.core_lock_held("round", Duration::from_secs(5));
    let parts = metrics.end_tick().unwrap();
    assert_eq!(
        parts.describe_held(),
        "turn 0.80 s, triggers 0.20 s",
        "others' holds summed, the round's own left out"
    );
    assert_eq!(parts.parts["assign"], Duration::from_millis(400));
    assert_eq!(parts.lock_wait, Duration::from_millis(250));
    assert_eq!(parts.describe(), "results 1.20 s, assign 0.40 s");
    assert_eq!(
        parts.unaccounted(Duration::from_secs(3)),
        Duration::from_millis(1150)
    );
    assert_eq!(parts.unaccounted(Duration::from_secs(1)), Duration::ZERO);
    assert_eq!(metrics.end_tick(), None, "ended once");
}
