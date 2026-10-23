use pretty_assertions::assert_eq;
use prometheus_client::encoding::text::encode;
use schedr_model::metric_doc::scrape_mismatches;

use super::*;

#[test]
fn log_errors_are_counted_by_kind() {
    let metrics = ApiMetrics::default();
    let mut registry = Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    metrics.log_error("unreachable");
    metrics.log_error("unreachable");
    let mut out = String::new();
    encode(&mut out, &registry).unwrap();
    assert!(
        out.contains("schedr_log_proxy_errors_total{kind=\"unreachable\"} 2"),
        "{out}"
    );
}

#[test]
fn every_metric_is_registered_as_documented() {
    let metrics = ApiMetrics::default();
    let mut registry = Registry::with_prefix("schedr");
    metrics.register(&mut registry);
    metrics.log_error("missing");
    metrics
        .requests
        .get_or_create(&RequestLabels {
            route: "/api/v1/dags".to_owned(),
            status: 200,
        })
        .inc();
    let mut out = String::new();
    encode(&mut out, &registry).unwrap();
    assert_eq!(
        scrape_mismatches(&out, Some("schedr"), METRICS),
        Vec::<String>::new(),
        "{out}"
    );
}
