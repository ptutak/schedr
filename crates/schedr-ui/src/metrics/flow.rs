//! The Metrics page's attempt charts (decision 494): a deployment's attempts ended in each
//! bucket, stacked by how they ended, and the start latency of those started, p50 and p95,
//! from its `GET /api/v1/metrics/tasks`.

use jiff::tz::TimeZone;
use schedr_api::views::{TaskFlowList, TaskFlowView};

use super::{LegendItem, secs};
use crate::chart::{self, Line, Spec, Unit};

/// A series of the charts: its label, and what of a bucket it shows.
type Count = (&'static str, fn(&TaskFlowView) -> u64);
/// A percentile of the latency chart: its label, and its value in a bucket.
type Percentile = (&'static str, fn(&TaskFlowView) -> Option<u64>);

/// A deployment's two attempt charts and their legends.
#[derive(Debug, Clone, Default)]
pub struct FlowCharts {
    /// Attempts ended, stacked: succeeded, failed, stopped.
    pub ended: String,
    /// Its legend, each with the range's total.
    pub ended_legend: Vec<LegendItem>,
    /// Start latency, p50 and p95.
    pub latency: String,
    /// Its legend, each with the range's highest.
    pub latency_legend: Vec<LegendItem>,
}

/// `name`'s charts of `list`.
#[must_use]
pub fn charts(name: &str, list: &TaskFlowList, zone: &TimeZone) -> FlowCharts {
    #[expect(clippy::cast_precision_loss, reason = "a chart's coordinates")]
    let f = |n: u64| n as f64;
    let spec = |title: String, unit, lines, stacked: Vec<Line>| Spec {
        bars: !stacked.is_empty(),
        title,
        unit,
        width: chart::WIDTH,
        from: secs(list.from),
        to: secs(list.to),
        step: i64::from(list.step_s),
        lines,
        stacked,
        band: None,
        limits: Vec::new(),
        zone: zone.clone(),
    };
    let kinds: [Count; 3] = [
        ("succeeded", |b| b.succeeded),
        ("failed", |b| b.failed),
        ("stopped", |b| b.stopped),
    ];
    let mut ended_legend = Vec::new();
    let mut stacked = Vec::new();
    for (slot, (label, count)) in (1..).zip(kinds) {
        let total: u64 = list.buckets.iter().map(count).sum();
        ended_legend.push(LegendItem {
            slot,
            label: format!("{label} {total}"),
        });
        stacked.push(Line {
            label: label.to_owned(),
            slot,
            points: list
                .buckets
                .iter()
                .map(|b| (secs(b.at), f(count(b))))
                .collect(),
        });
    }
    let percentiles: [Percentile; 2] = [("p50", |b| b.start_p50_ms), ("p95", |b| b.start_p95_ms)];
    let mut latency_legend = Vec::new();
    let mut lines = Vec::new();
    for (slot, (label, value)) in (1..).zip(percentiles) {
        let highest = list.buckets.iter().filter_map(value).max();
        latency_legend.push(LegendItem {
            slot,
            label: highest.map_or_else(
                || format!("{label}: none started"),
                |ms| format!("{label}, highest {}", chart::millis(f(ms))),
            ),
        });
        lines.push(Line {
            label: label.to_owned(),
            slot,
            points: list
                .buckets
                .iter()
                .filter_map(|b| value(b).map(|ms| (secs(b.at), f(ms))))
                .collect(),
        });
    }
    FlowCharts {
        ended: chart::draw(&spec(
            format!("Attempts of {name} ended"),
            Unit::Count,
            Vec::new(),
            stacked,
        )),
        ended_legend,
        latency: chart::draw(&spec(
            format!("Start latency of {name}'s attempts"),
            Unit::Millis,
            lines,
            Vec::new(),
        )),
        latency_legend,
    }
}
