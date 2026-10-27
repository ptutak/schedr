use pretty_assertions::assert_eq;
use prometheus_client::encoding::text::encode;
use schedr_model::AttemptStatus;
use schedr_model::metric_doc::scrape_mismatches;

use std::time::Duration;

use super::*;

#[test]
fn every_metric_is_registered_and_attempts_count_by_status() {
    let metrics = AgentMetrics::default();
    let mut registry = Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    metrics.slots.set(4);
    metrics.attempt_ended(&AttemptStatus::OomKilled);
    metrics.watchdog_kills.inc();
    metrics.task_started(
        "spawn",
        &StartTimes {
            runner: Duration::from_millis(150),
            import: Some(Duration::from_millis(800)),
        },
    );
    metrics.task_started(
        "spawn",
        &StartTimes {
            runner: Duration::from_millis(150),
            import: None,
        },
    );
    metrics
        .fork
        .servers
        .get_or_create(&ForkServerLabels {
            interpreter: "python".to_owned(),
            kind: "preloaded".to_owned(),
            state: "up".to_owned(),
        })
        .set(1);
    metrics
        .fork
        .fallbacks
        .get_or_create(&ForkFallbackLabels {
            interpreter: "python".to_owned(),
            reason: "disabled".to_owned(),
        })
        .inc();
    let mut out = String::new();
    encode(&mut out, &registry).unwrap();
    for name in [
        "schedr_worker_slots 4",
        "schedr_worker_slots_used 0",
        "schedr_worker_attempts_total{status=\"oom_killed\"} 1",
        "schedr_worker_watchdog_kills_total 1",
        // Both starts reached the runner; only one imported its DAG file.
        "schedr_worker_task_start_seconds_count{method=\"spawn\",phase=\"runner\"} 2",
        "schedr_worker_task_start_seconds_count{method=\"spawn\",phase=\"import\"} 1",
        "schedr_worker_task_start_seconds_bucket{le=\"0.16\",method=\"spawn\",phase=\"runner\"} 2",
        "schedr_worker_journal_records 0",
        "schedr_worker_log_bytes 0",
        "schedr_worker_log_swept_bytes_total 0",
        "schedr_worker_fork_servers{interpreter=\"python\",kind=\"preloaded\",state=\"up\"} 1",
        "schedr_worker_fork_fallbacks_total{interpreter=\"python\",reason=\"disabled\"} 1",
    ] {
        assert!(out.contains(name), "{name}\n{out}");
    }
    assert_eq!(
        scrape_mismatches(&out, Some("schedr"), METRICS),
        Vec::<String>::new(),
        "{out}"
    );
}
