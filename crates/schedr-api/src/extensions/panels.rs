//! A panel's lines (decision 516): its metric's series grouped by `group_by`, each bucket's
//! values combined with `combine` (the workers' and the series a group joins), its `point`
//! worked out from them, the largest `limit` lines kept and the rest folded into `other`.
//!
//! Order: values are combined at each moment first (a minute's readings over workers, or a
//! rollup window's points over the series of a group), then the bucket's point is taken over
//! those moments. So `sum` + `max` is the peak of the total, and three workers reading one
//! cluster's 10 pending pods show 10 with `combine: max`.

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_config::masked::mask_text;
use schedr_db::extension_samples::{ExtensionSample, GaugePoints, SampleData};
use schedr_db::samples::ROLLUP;
use schedr_model::extension::{
    ChartKind, Combine, MetricDeclaration, MetricKind, OTHER, Panel, Point,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{ExtensionPointView, ExtensionSeriesView, Labels, empty};

/// One panel's lines, worked out as its functions say.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionPanelView {
    /// Its place among the extension's page panels (`panels` of the extension's view).
    pub index: u32,
    /// The metric it draws.
    pub metric: String,
    /// Its lines: each point's `value` the panel's point (a counter's increase or rate, a
    /// gauge's last, mean, highest or lowest, a histogram's count or mean); a histogram's
    /// `counts` and `sum` kept for its quantiles and distribution.
    pub series: Vec<ExtensionSeriesView>,
    /// Whether lines past the metric's series cap or the panel's `limit` were folded into
    /// `other`.
    pub folded: bool,
}

/// The point a panel takes: its own, or its kind's default (a counter's rate on a `rate`
/// chart).
#[must_use]
pub fn point_of(panel: &Panel, kind: MetricKind) -> Point {
    panel.point.unwrap_or(match (panel.chart, kind) {
        (ChartKind::Rate, MetricKind::Counter) => Point::Rate,
        _ => Point::default_for(kind),
    })
}

/// Whether `panel`'s points are per second: a `rate` point, or a histogram's count on a `rate`
/// chart that named no point (agy's review of M5.2i10: an `increase` named on a `rate` chart
/// was divided all the same).
#[must_use]
pub fn per_second(panel: &Panel, kind: MetricKind) -> bool {
    match point_of(panel, kind) {
        Point::Rate => true,
        Point::Count => panel.chart == ChartKind::Rate && panel.point.is_none(),
        _ => false,
    }
}

/// Whether `labels` are a folded series' (all `other`).
fn is_other(labels: &Labels) -> bool {
    !labels.is_empty() && labels.values().all(|v| v == OTHER)
}

/// `panel`'s lines from `samples`, its metric's, in buckets of `bucket_s` seconds.
#[must_use]
pub fn panel_view(
    index: u32,
    panel: &Panel,
    declared: &MetricDeclaration,
    samples: &[&ExtensionSample],
    max_series: u32,
    bucket_s: u32,
) -> ExtensionPanelView {
    let how = How {
        kind: declared.kind,
        point: point_of(panel, declared.kind),
        combine: panel.combine.unwrap_or(Combine::Sum),
        per_second: per_second(panel, declared.kind),
        bucket_s: bucket_s.max(1),
    };
    // Grouped first, then capped (agy's review of M5.2i10: capped first, a series folded lost
    // the labels its group needs).
    let mut groups: BTreeMap<Labels, Vec<&ExtensionSample>> = BTreeMap::new();
    for sample in samples {
        let key = match &panel.group_by {
            None => sample.labels.clone(),
            Some(kept) => sample
                .labels
                .iter()
                .filter(|(name, _)| kept.contains(name))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        groups.entry(key).or_default().push(sample);
    }
    let mut lines: Vec<(Labels, Vec<ExtensionPointView>, Vec<&ExtensionSample>)> = groups
        .into_iter()
        .map(|(labels, samples)| {
            let points = how.points(&samples);
            (labels, points, samples)
        })
        .collect();
    // A line folded already (by a worker or the master) is the `other` the rest join, never
    // ranked among the named (agy's review of M5.2i10: two `other`s, a named line evicted).
    let mut other: Option<(Labels, Vec<&ExtensionSample>)> = None;
    if let Some(at) = lines.iter().position(|(labels, _, _)| is_other(labels)) {
        let (labels, _, samples) = lines.remove(at);
        other = Some((labels, samples));
    }
    let cap = usize::try_from(max_series).unwrap_or(usize::MAX);
    let kept = panel
        .limit
        .and_then(|l| usize::try_from(l).ok())
        .map_or(cap, |limit| limit.min(cap));
    let mut folded = other.is_some();
    if lines.len() > kept {
        // The largest by their peak over the range; the rest worked out again with `other`.
        lines.sort_by(|a, b| {
            peak(&b.1)
                .total_cmp(&peak(&a.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        for (labels, _, samples) in lines.split_off(kept) {
            let (_, joined) = other.get_or_insert_with(|| {
                let folded: Labels = labels
                    .keys()
                    .map(|k| (k.clone(), OTHER.to_owned()))
                    .collect();
                (folded, Vec::new())
            });
            joined.extend(samples);
        }
        lines.sort_by(|a, b| a.0.cmp(&b.0));
        folded = true;
    }
    if let Some((labels, samples)) = other {
        let points = how.points(&samples);
        lines.push((labels, points, samples));
    }
    ExtensionPanelView {
        index,
        metric: declared.name.clone(),
        series: lines
            .into_iter()
            .map(|(labels, points, _)| ExtensionSeriesView {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| (name, mask_text(&value)))
                    .collect(),
                points,
            })
            .collect(),
        folded,
    }
}

/// A line's peak over the range, to rank it: its points' largest value, or a histogram's
/// largest count.
fn peak(points: &[ExtensionPointView]) -> f64 {
    points
        .iter()
        .map(|p| {
            if p.counts.is_empty() {
                p.value.abs()
            } else {
                #[expect(clippy::cast_precision_loss, reason = "a rank, not a figure")]
                let count = p.counts.iter().sum::<u64>() as f64;
                count
            }
        })
        .fold(0.0, f64::max)
}

/// How a panel works its points out.
struct How {
    kind: MetricKind,
    point: Point,
    combine: Combine,
    per_second: bool,
    bucket_s: u32,
}

impl How {
    fn bucket_of(&self, at: Timestamp) -> Timestamp {
        let step = i64::from(self.bucket_s);
        Timestamp::from_second(at.as_second().div_euclid(step) * step).unwrap_or(at)
    }

    /// One line's points, oldest first, from its samples (all its series', all workers').
    fn points(&self, samples: &[&ExtensionSample]) -> Vec<ExtensionPointView> {
        if self.kind == MetricKind::Histogram {
            return self.histogram(samples);
        }
        // Per bucket, per moment: the values combined there, each with the minutes it covers.
        let mut moments: BTreeMap<Timestamp, BTreeMap<Timestamp, Vec<(f64, u32)>>> =
            BTreeMap::new();
        for sample in samples {
            let Some((value, minutes)) = self.value_of(sample) else {
                continue;
            };
            moments
                .entry(self.bucket_of(sample.at))
                .or_default()
                .entry(sample.at)
                .or_default()
                .push((value, minutes.max(1)));
        }
        moments
            .into_iter()
            .map(|(bucket, moments)| {
                let combined: Vec<(f64, u32)> = moments
                    .into_values()
                    .map(|values| self.combined(&values))
                    .collect();
                let mut point = empty(bucket);
                point.value = self.point_over(&combined);
                if self.kind == MetricKind::Gauge {
                    point.peak = Some(combined.iter().map(|m| m.0).fold(f64::MIN, f64::max));
                }
                point
            })
            .collect()
    }

    /// A moment's values combined, and the minutes it covers. Rollup means added up are each
    /// weighted by the minutes their series covered (agy's review of M5.2i10: a series seen one
    /// minute of 15 counted as one seen all 15).
    fn combined(&self, values: &[(f64, u32)]) -> (f64, u32) {
        let minutes = values.iter().map(|v| v.1).max().unwrap_or(1);
        if self.combine == Combine::Sum && self.point == Point::Mean {
            let weighted: f64 = values.iter().map(|(v, m)| v * f64::from(*m)).sum();
            return (weighted / f64::from(minutes), minutes);
        }
        let plain: Vec<f64> = values.iter().map(|v| v.0).collect();
        (combine(self.combine, &plain), minutes)
    }

    /// A sample's value for this panel and the minutes it covers: a minute's reading or growth
    /// as it is; a rollup's point under this panel's combine (decision 516).
    fn value_of(&self, sample: &ExtensionSample) -> Option<(f64, u32)> {
        let rollup = sample.step_s == ROLLUP;
        match &sample.data {
            SampleData::Counter { growth, across } => {
                let value = match (rollup, across, self.combine) {
                    (true, Some(a), Combine::Max) => a.max,
                    (true, Some(a), Combine::Min) => a.min,
                    (true, Some(a), Combine::Mean) => a.mean,
                    _ => *growth,
                };
                Some((value, sample.step_s / 60))
            }
            SampleData::Gauge {
                avg,
                peak,
                minutes,
                across,
            } => {
                let value = match across.as_deref() {
                    Some(across) if rollup => {
                        let points = match self.combine {
                            Combine::Sum => across.sum,
                            Combine::Max => across.max,
                            Combine::Min => across.min,
                            Combine::Mean => across.mean,
                        };
                        gauge_point(&points, self.point)
                    }
                    // A minute's reading; a rollup without them (older) its sum's average or
                    // peak.
                    _ if self.point == Point::Max => *peak,
                    _ => *avg,
                };
                Some((value, *minutes))
            }
            SampleData::Histogram { .. } => None,
        }
    }

    /// The bucket's point over its moments, oldest first.
    fn point_over(&self, moments: &[(f64, u32)]) -> f64 {
        let values: Vec<f64> = moments.iter().map(|m| m.0).collect();
        match self.point {
            Point::Increase | Point::Rate | Point::Count | Point::Quantiles => {
                let total: f64 = values.iter().sum();
                if self.per_second {
                    total / f64::from(self.bucket_s)
                } else {
                    total
                }
            }
            Point::Last => values.last().copied().unwrap_or_default(),
            Point::Max => values.iter().copied().fold(f64::MIN, f64::max),
            Point::Min => values.iter().copied().fold(f64::MAX, f64::min),
            Point::Mean => {
                let minutes: u32 = moments.iter().map(|m| m.1).sum();
                let weighted: f64 = moments.iter().map(|(v, m)| v * f64::from(*m)).sum();
                weighted / f64::from(minutes.max(1))
            }
        }
    }

    /// A histogram's line: its observations added (its only combine), the point its count
    /// (per second on a `rate` chart) or mean; the counts kept.
    fn histogram(&self, samples: &[&ExtensionSample]) -> Vec<ExtensionPointView> {
        let mut buckets: BTreeMap<Timestamp, ExtensionPointView> = BTreeMap::new();
        for sample in samples {
            if let SampleData::Histogram { counts, sum } = &sample.data {
                let bucket = self.bucket_of(sample.at);
                let point = buckets.entry(bucket).or_insert_with(|| empty(bucket));
                if point.counts.len() < counts.len() {
                    point.counts.resize(counts.len(), 0);
                }
                for (total, count) in point.counts.iter_mut().zip(counts) {
                    *total = total.saturating_add(*count);
                }
                point.sum += sum;
            }
        }
        for point in buckets.values_mut() {
            #[expect(clippy::cast_precision_loss, reason = "a chart's value")]
            let count = point.counts.iter().sum::<u64>() as f64;
            point.value = match self.point {
                Point::Mean if count > 0.0 => point.sum / count,
                Point::Mean | Point::Quantiles => 0.0,
                _ if self.per_second => count / f64::from(self.bucket_s),
                _ => count,
            };
        }
        buckets.into_values().collect()
    }
}

fn gauge_point(points: &GaugePoints, point: Point) -> f64 {
    match point {
        Point::Mean => points.mean,
        Point::Max => points.max,
        Point::Min => points.min,
        _ => points.last,
    }
}

/// `values` combined as `how` says.
fn combine(how: Combine, values: &[f64]) -> f64 {
    match how {
        Combine::Sum => values.iter().sum(),
        Combine::Max => values.iter().copied().fold(f64::MIN, f64::max),
        Combine::Min => values.iter().copied().fold(f64::MAX, f64::min),
        Combine::Mean => {
            #[expect(clippy::cast_precision_loss, reason = "a few workers and series")]
            let n = values.len().max(1) as f64;
            values.iter().sum::<f64>() / n
        }
    }
}
