use prometheus_client::encoding::text::encode;
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
fn every_metric_is_registered_with_its_prefix() {
    // A label family shows once it has a member.
    let metrics = MasterMetrics::default();
    metrics.set_counts(&StateCounts {
        runs: vec![("running".to_owned(), "manual".to_owned(), 1)],
        tasks: vec![("queued".to_owned(), 1)],
        workers: vec![("active".to_owned(), 1)],
    });
    metrics.set_waiting(&[("no_slot", 1)]);
    metrics.attempt_ended(&AttemptStatus::Success);
    metrics.run_finished("sync", &RunState::Success);
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
    let out = text(&metrics);
    for name in [
        "schedr_master_tick_duration_seconds",
        "schedr_runs",
        "schedr_runs_finished",
        "schedr_tasks",
        "schedr_tasks_waiting",
        "schedr_assignments",
        "schedr_rejections",
        "schedr_attempts_ended",
        "schedr_parse_jobs_started",
        "schedr_parse_jobs_finished",
        "schedr_parse_duration_seconds",
        "schedr_commands",
        "schedr_command_latency_seconds",
        "schedr_workers",
    ] {
        assert!(out.contains(&format!("# TYPE {name} ")), "{name}\n{out}");
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
