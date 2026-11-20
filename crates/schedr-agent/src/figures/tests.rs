#![expect(
    clippy::float_cmp,
    reason = "the figures are sums of small whole numbers: exact"
)]

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn declarations() -> Declarations {
    serde_json::from_value(json!({
        "metrics": [
            {"name": "pods_started", "kind": "counter", "help": "Pods started",
             "labels": [{"name": "namespace"}]},
            {"name": "pods_running", "kind": "gauge", "help": "Pods running"},
            {"name": "queue_seconds", "kind": "histogram", "help": "Queue time",
             "buckets": [1.0, 5.0]},
            {"name": "phase_total", "kind": "counter", "help": "By phase",
             "labels": [{"name": "phase", "values": ["Pending", "Running"]}]}
        ]
    }))
    .unwrap()
}

fn point(name: &str, value: f64, labels: &[(&str, &str)]) -> MetricPoint {
    MetricPoint {
        name: name.to_owned(),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        value,
    }
}

fn figures(max_series: u32) -> Figures {
    let figures = Figures::default();
    figures.declare("kubernetes", "1.0", declarations(), max_series);
    figures
}

fn series<'a>(all: &'a [ExtensionFigures], metric: &str) -> &'a [SeriesFigures] {
    &all[0]
        .metrics
        .iter()
        .find(|m| m.name == metric)
        .unwrap()
        .series
}

#[test]
fn a_counter_adds_a_gauge_is_set_and_a_histogram_counts_by_bucket() {
    let figures = figures(100);
    let observe = |p: MetricPoint| figures.observe("kubernetes", "1.0", &p).unwrap();
    observe(point("pods_started", 1.0, &[("namespace", "a")]));
    observe(point("pods_started", 2.0, &[("namespace", "a")]));
    observe(point("pods_running", 3.0, &[]));
    observe(point("pods_running", 2.0, &[]));
    for v in [0.5, 3.0, 9.0, 9.0] {
        observe(point("queue_seconds", v, &[]));
    }
    let all = figures.take_figures();
    assert_eq!(series(&all, "pods_started")[0].value, 3.0);
    assert_eq!(series(&all, "pods_running")[0].value, 2.0);
    let histogram = &series(&all, "queue_seconds")[0];
    assert_eq!(
        (histogram.counts.clone(), histogram.sum),
        (vec![1, 1, 2], 21.5)
    );
}

#[test]
fn points_breaking_their_declaration_are_not_kept() {
    let figures = figures(100);
    let observe = |p: MetricPoint| figures.observe("kubernetes", "1.0", &p);
    assert_eq!(
        observe(point("nope", 1.0, &[])),
        Err(NotKept::Undeclared("nope".to_owned()))
    );
    assert!(matches!(
        observe(point("pods_started", 1.0, &[])),
        Err(NotKept::Labels { .. })
    ));
    assert!(matches!(
        observe(point("pods_running", 1.0, &[("x", "y")])),
        Err(NotKept::Labels { .. })
    ));
    assert!(matches!(
        observe(point("pods_started", -1.0, &[("namespace", "a")])),
        Err(NotKept::Value(_))
    ));
    assert!(matches!(
        observe(point("pods_running", f64::NAN, &[])),
        Err(NotKept::Value(_))
    ));
    assert!(
        figures
            .observe("other", "1.0", &point("pods_running", 1.0, &[]))
            .is_err()
    );
    assert!(figures.warn_once("kubernetes", "1.0", "nope"));
    assert!(!figures.warn_once("kubernetes", "1.0", "nope"));
}

#[test]
fn past_its_series_cap_a_metric_folds_new_labels_into_other() {
    let figures = figures(2);
    for namespace in ["a", "b", "c", "d", "a"] {
        figures
            .observe(
                "kubernetes",
                "1.0",
                &point("pods_started", 1.0, &[("namespace", namespace)]),
            )
            .unwrap();
    }
    figures
        .observe(
            "kubernetes",
            "1.0",
            &point("phase_total", 1.0, &[("phase", "Weird")]),
        )
        .unwrap();
    let all = figures.take_figures();
    let shown: Vec<(&str, f64)> = series(&all, "pods_started")
        .iter()
        .map(|s| (s.labels["namespace"].as_str(), s.value))
        .collect();
    assert_eq!(shown, [("a", 2.0), ("b", 1.0), ("other", 2.0)]);
    assert_eq!(series(&all, "phase_total")[0].labels["phase"], "other");
}

#[test]
fn declarations_go_with_a_streams_first_figures_and_series_survive_the_same_ones() {
    let figures = figures(100);
    figures
        .observe("kubernetes", "1.0", &point("pods_running", 4.0, &[]))
        .unwrap();
    assert!(figures.take_figures()[0].declarations.is_some());
    assert!(figures.take_figures()[0].declarations.is_none());
    figures.connected();
    assert!(figures.take_figures()[0].declarations.is_some());
    // agy's review of M5.2i5: figures dropped past the heartbeat's room took them along.
    figures.unsent("kubernetes", "1.0");
    assert!(figures.take_figures()[0].declarations.is_some());
    figures.declare("kubernetes", "1.0", declarations(), 100);
    assert_eq!(
        series(&figures.take_figures(), "pods_running")[0].value,
        4.0
    );
}

#[test]
fn metrics_are_served_for_prometheus_with_their_version() {
    let figures = figures(100);
    figures
        .observe(
            "kubernetes",
            "1.0",
            &point("pods_started", 2.0, &[("namespace", "a")]),
        )
        .unwrap();
    figures
        .observe("kubernetes", "1.0", &point("queue_seconds", 3.0, &[]))
        .unwrap();
    let mut registry = prometheus_client::registry::Registry::default();
    registry
        .sub_registry_with_prefix("schedr")
        .register_collector(Box::new(figures));
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    assert!(
        text.contains(
            "schedr_extension_kubernetes_pods_started_total{namespace=\"a\",extension_version=\"1.0\"} 2"
        ),
        "{text}"
    );
    assert!(
        text.contains("schedr_extension_kubernetes_queue_seconds_bucket{le=\"5.0\",extension_version=\"1.0\"} 1")
            || text.contains("schedr_extension_kubernetes_queue_seconds_bucket{extension_version=\"1.0\",le=\"5.0\"} 1"),
        "{text}"
    );
}

#[test]
fn events_wait_for_the_next_heartbeat_the_oldest_going_past_the_cap() {
    use schedr_model::extension::{EventKind, ExtensionEvent, MAX_PENDING_EVENTS};
    let figures = Figures::default();
    for n in 0..MAX_PENDING_EVENTS + 3 {
        figures.event(ExtensionEvent::now(
            "k",
            None,
            EventKind::Dropped,
            &format!("e{n}"),
        ));
    }
    let taken = figures.take_events();
    assert_eq!(taken.len(), MAX_PENDING_EVENTS);
    assert_eq!(taken[0].message, "e3", "the oldest went");
    assert!(figures.take_events().is_empty(), "sent once");
}

#[test]
fn undeclared_names_are_warned_of_once_each_and_no_more_past_the_cap() {
    let figures = Figures::default();
    figures.declare("k", "1", Declarations::default(), 10);
    for n in 0..MAX_WARNED {
        assert!(figures.warn_once("k", "1", &format!("m{n}")));
    }
    assert!(!figures.warn_once("k", "1", "m0"), "once each");
    assert!(
        !figures.warn_once("k", "1", "one-more"),
        "past the cap: none kept"
    );
}

#[test]
fn figures_are_the_growth_since_the_last_sent_the_totals_kept_for_metrics() {
    // Codex's review of M5.2i5: totals diffed by the master lost a worker's first minute and
    // mixed a restarted worker's; what grew is sent once instead.
    let figures = figures(10);
    let ns = [("namespace", "a")];
    figures
        .observe("kubernetes", "1.0", &point("pods_started", 2.0, &ns))
        .unwrap();
    figures
        .observe("kubernetes", "1.0", &point("pods_running", 4.0, &[]))
        .unwrap();
    figures
        .observe("kubernetes", "1.0", &point("queue_seconds", 3.0, &[]))
        .unwrap();
    let value_of = |sent: &[ExtensionFigures], metric: &str| -> Vec<(f64, Vec<u64>)> {
        sent[0]
            .metrics
            .iter()
            .find(|m| m.name == metric)
            .unwrap()
            .series
            .iter()
            .map(|s| (s.value, s.counts.clone()))
            .collect()
    };
    let first = figures.take_figures();
    assert_eq!(value_of(&first, "pods_started"), [(2.0, Vec::new())]);
    assert_eq!(value_of(&first, "queue_seconds"), [(0.0, vec![0, 1, 0])]);
    let queue = first[0]
        .metrics
        .iter()
        .find(|m| m.name == "queue_seconds")
        .unwrap();
    assert_eq!(
        (queue.kind, queue.buckets.clone()),
        (MetricKind::Histogram, vec![1.0, 5.0])
    );
    // Sent once: nothing grew since; the gauge's value is sent each time.
    let second = figures.take_figures();
    assert_eq!(value_of(&second, "pods_started"), []);
    assert_eq!(value_of(&second, "pods_running"), [(4.0, Vec::new())]);
    figures
        .observe("kubernetes", "1.0", &point("pods_started", 1.0, &ns))
        .unwrap();
    assert_eq!(
        value_of(&figures.take_figures(), "pods_started"),
        [(1.0, Vec::new())]
    );
    // The total, for /metrics, goes on.
    let mut served = String::new();
    let mut registry = prometheus_client::registry::Registry::default();
    registry.register_collector(Box::new(figures.clone()));
    prometheus_client::encoding::text::encode(&mut served, &registry).unwrap();
    assert!(
        served.contains(
            "extension_kubernetes_pods_started_total{namespace=\"a\",extension_version=\"1.0\"} 3"
        ),
        "{served}"
    );
}

#[test]
fn the_events_held_are_the_workers_hidden_limit_the_oldest_going() {
    // D493: `SCHEDR_HIDDEN__WORKER__EXTENSION_EVENTS_PENDING`, not a constant.
    let figures = Figures::default();
    figures.hold_at_most(3);
    for n in 0..5 {
        figures.event(schedr_model::extension::ExtensionEvent::now(
            "dummy",
            None,
            schedr_model::extension::EventKind::Dropped,
            &format!("e{n}"),
        ));
    }
    let held: Vec<String> = figures
        .take_events()
        .into_iter()
        .map(|e| e.message)
        .collect();
    assert_eq!(held, ["e2", "e3", "e4"]);
    assert!(figures.take_events().is_empty(), "none kept after");
}
