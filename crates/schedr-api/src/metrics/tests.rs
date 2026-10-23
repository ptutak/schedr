use prometheus_client::encoding::text::encode;

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
