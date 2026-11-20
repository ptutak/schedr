//! A worker's extension figures (decision 513) as samples: its totals checked against what a
//! worker may send, then each minute's growth against the minute before (a counter's growth, a
//! histogram's new observations, a gauge's value), and 15-minute windows rolled up across
//! workers.

use std::collections::BTreeMap;

use jiff::Timestamp;
use schedr_db::extension_samples::{ExtensionSample, SampleData};
use schedr_model::extension::{
    ExtensionEvent, ExtensionFigures, MAX_BUCKETS, MAX_LABEL_VALUE, MAX_METRICS, MAX_TEXT,
    MetricKind, OTHER, check_declarations,
};

#[cfg(test)]
mod tests;

/// The most extension versions one heartbeat carries figures of.
pub const MAX_EXTENSIONS: usize = 50;

/// `figures` as a worker may send them: its names valid, its declarations usable, at most
/// [`MAX_METRICS`] metrics of at most its `max_series` series and one `other`, each series of
/// its metric's kind (a counter's growth not negative; a histogram's counts one past its
/// bounds, which rise), labels cut.
///
/// # Errors
///
/// Why they can't be kept.
pub fn checked(mut figures: ExtensionFigures) -> Result<ExtensionFigures, String> {
    schedr_model::task::check_extension_name(&figures.extension)?;
    schedr_model::task::check_extension_version(&figures.version)?;
    if figures.max_series == 0 || figures.max_series > 10_000 {
        return Err("max_series is 1 to 10000".to_owned());
    }
    if let Some(declarations) = &figures.declarations {
        check_declarations(declarations)?;
    }
    if figures.metrics.len() > MAX_METRICS {
        return Err(format!("over {MAX_METRICS} metrics"));
    }
    let most = usize::try_from(figures.max_series).unwrap_or(usize::MAX) + 1;
    for metric in &mut figures.metrics {
        let bad = |why: &str| format!("metric `{}`: {why}", metric.name);
        if metric.series.len() > most {
            return Err(bad("over its series cap"));
        }
        let histogram = metric.kind == MetricKind::Histogram;
        let bounds_rise = metric.buckets.windows(2).all(|w| w[0] < w[1]);
        if metric.buckets.len() > MAX_BUCKETS
            || histogram == metric.buckets.is_empty()
            || !bounds_rise
            || metric.buckets.iter().any(|b| !b.is_finite())
        {
            return Err(bad("buckets that can't be kept"));
        }
        let counts = if histogram {
            metric.buckets.len() + 1
        } else {
            0
        };
        for series in &mut metric.series {
            if series.counts.len() != counts
                || !series.value.is_finite()
                || !series.sum.is_finite()
                || (metric.kind == MetricKind::Counter && series.value < 0.0)
            {
                return Err(bad("a series that can't be kept"));
            }
            for value in series.labels.values_mut() {
                if value.chars().count() > MAX_LABEL_VALUE {
                    *value = value.chars().take(MAX_LABEL_VALUE).collect();
                }
            }
        }
    }
    Ok(figures)
}

/// The newest `most` of the events a worker sent (oldest first), checked; oldest first (agy's
/// review of M5.2i5: the oldest were kept, as nowhere else).
#[must_use]
pub fn newest_events(sent: &[Vec<u8>], most: usize, now: Timestamp) -> Vec<ExtensionEvent> {
    sent[sent.len().saturating_sub(most)..]
        .iter()
        .filter_map(|json| checked_event(json, now))
        .collect()
}

/// An extension event a worker sent (DN19), as kept: its names valid, its text cut, its time
/// no later than `now`; none when it can't be read or names no valid extension.
#[must_use]
pub fn checked_event(json: &[u8], now: Timestamp) -> Option<ExtensionEvent> {
    let mut event: ExtensionEvent = serde_json::from_slice(json).ok()?;
    schedr_model::task::check_extension_name(&event.extension).ok()?;
    if let Some(version) = &event.version {
        schedr_model::task::check_extension_version(version).ok()?;
    }
    event.message = event.message.chars().take(MAX_TEXT).collect();
    event.at_ms = event.at_ms.min(now.as_millisecond());
    Some(event)
}

/// The minute's samples of `figures` (each the growth since the worker's last figures) for
/// `worker_id` at `at`: a counter's growth, a histogram's new observations, a gauge's value;
/// a counter or histogram that didn't grow is left out.
#[must_use]
pub fn minute_samples(
    figures: &ExtensionFigures,
    worker_id: &str,
    at: Timestamp,
) -> Vec<ExtensionSample> {
    let mut samples = Vec::new();
    for metric in &figures.metrics {
        for series in &metric.series {
            let data = match metric.kind {
                MetricKind::Gauge => Some(SampleData::Gauge {
                    avg: series.value,
                    peak: series.value,
                    minutes: 1,
                }),
                MetricKind::Counter => (series.value > 0.0).then_some(SampleData::Counter {
                    growth: series.value,
                }),
                MetricKind::Histogram => {
                    series
                        .counts
                        .iter()
                        .any(|c| *c > 0)
                        .then(|| SampleData::Histogram {
                            counts: series.counts.clone(),
                            sum: series.sum,
                        })
                }
            };
            if let Some(data) = data {
                samples.push(ExtensionSample {
                    name: figures.extension.clone(),
                    version: figures.version.clone(),
                    worker_id: worker_id.to_owned(),
                    metric: metric.name.clone(),
                    labels: series.labels.clone(),
                    step_s: schedr_db::samples::MINUTE,
                    at,
                    data,
                });
            }
        }
    }
    samples
}

type Labels = BTreeMap<String, String>;

/// The named series each extension's metric stored this hour, across workers: a new one past
/// the metric's cap is stored as `other` (Codex's review of M5.2i5: a cap per worker let N
/// workers store N times it). Started again each hour, so series that come and go (a pod's)
/// are not shut out for good; a master taking over loads the hour's from the samples
/// ([`SeriesSeen::unloaded`], Codex's second review). The first series of a metric fixes its
/// label names for the hour: another set of names is dropped (a worker inventing names would
/// pass the cap otherwise).
#[derive(Debug, Default)]
pub struct SeriesSeen {
    hour: i64,
    seen: BTreeMap<(String, String), Seen>,
    /// The metrics whose hour's stored series were read: one whose read failed is read again
    /// with the next figures (agy's review of M5.2i5: it never was, all hour).
    loaded: std::collections::BTreeSet<(String, String)>,
}

#[derive(Debug, Default)]
struct Seen {
    names: Option<std::collections::BTreeSet<String>>,
    named: std::collections::BTreeSet<Labels>,
}

impl SeriesSeen {
    fn start_hour(&mut self, at: Timestamp) {
        let hour = at.as_second().div_euclid(3600);
        if hour != self.hour {
            self.hour = hour;
            self.seen.clear();
            self.loaded.clear();
        }
    }

    /// The start of `at`'s hour, and the metrics of `samples` (extension, metric) not loaded
    /// for it yet: [`SeriesSeen::load`] them first.
    pub fn unloaded(
        &mut self,
        samples: &[ExtensionSample],
        at: Timestamp,
    ) -> (Timestamp, Vec<(String, String)>) {
        self.start_hour(at);
        let start = Timestamp::from_second(self.hour * 3600).unwrap_or(at);
        let mut keys: Vec<(String, String)> = samples
            .iter()
            .map(|s| (s.name.clone(), s.metric.clone()))
            .filter(|key| !self.loaded.contains(key))
            .collect();
        keys.sort();
        keys.dedup();
        (start, keys)
    }

    /// The series of `key` stored this hour already. Any of them fixes the label names, one
    /// without labels or all `other` too, as [`SeriesSeen::fold`] does (agy's review of M5.2i5:
    /// after a takeover, invented names could pin them instead).
    pub fn load(&mut self, key: (String, String), stored: Vec<Labels>) {
        self.loaded.insert(key.clone());
        let seen = self.seen.entry(key).or_default();
        for labels in stored {
            seen.names
                .get_or_insert_with(|| labels.keys().cloned().collect());
            if labels.is_empty() || labels.values().all(|v| v == OTHER) {
                continue;
            }
            seen.named.insert(labels);
        }
    }

    /// `samples` within their metrics' caps: past `max_series` named series of their metric
    /// this hour, their labels all `other`; of other label names than the hour's first, gone.
    /// How many were dropped.
    pub fn fold(
        &mut self,
        samples: &mut Vec<ExtensionSample>,
        max_series: u32,
        at: Timestamp,
    ) -> usize {
        self.start_hour(at);
        let cap = usize::try_from(max_series).unwrap_or(usize::MAX);
        let before = samples.len();
        samples.retain_mut(|sample| {
            let seen = self
                .seen
                .entry((sample.name.clone(), sample.metric.clone()))
                .or_default();
            let names: std::collections::BTreeSet<String> = sample.labels.keys().cloned().collect();
            if seen.names.get_or_insert_with(|| names.clone()) != &names {
                return false;
            }
            let is_other = sample.labels.values().all(|v| v == OTHER);
            if sample.labels.is_empty() || is_other || seen.named.contains(&sample.labels) {
                return true;
            }
            if seen.named.len() < cap {
                seen.named.insert(sample.labels.clone());
            } else {
                for value in sample.labels.values_mut() {
                    OTHER.clone_into(value);
                }
            }
            true
        });
        before - samples.len()
    }
}

/// A window's minute samples as one rollup per extension, version, metric and series, across
/// workers: counters' growth and histograms' observations summed; a gauge's average and peak
/// of its minutes' sums over workers.
#[must_use]
pub fn rolled_up(minutes: &[ExtensionSample], at: Timestamp) -> Vec<ExtensionSample> {
    type Key = (String, String, String, String);
    let mut by_series: BTreeMap<Key, (BTreeMap<String, String>, Vec<&ExtensionSample>)> =
        BTreeMap::new();
    for sample in minutes {
        let key = (
            sample.name.clone(),
            sample.version.clone(),
            sample.metric.clone(),
            serde_json::to_string(&sample.labels).unwrap_or_default(),
        );
        by_series
            .entry(key)
            .or_insert_with(|| (sample.labels.clone(), Vec::new()))
            .1
            .push(sample);
    }
    by_series
        .into_iter()
        .filter_map(|((name, version, metric, _), (labels, samples))| {
            let data = roll(&samples)?;
            Some(ExtensionSample {
                name,
                version,
                worker_id: String::new(),
                metric,
                labels,
                step_s: schedr_db::samples::ROLLUP,
                at,
                data,
            })
        })
        .collect()
}

/// One series' samples, of one kind, as one.
fn roll(samples: &[&ExtensionSample]) -> Option<SampleData> {
    match &samples.first()?.data {
        SampleData::Counter { .. } => Some(SampleData::Counter {
            growth: samples
                .iter()
                .filter_map(|s| match s.data {
                    SampleData::Counter { growth } => Some(growth),
                    _ => None,
                })
                .sum(),
        }),
        SampleData::Gauge { .. } => {
            // Each minute's average and peak summed over workers; the window's peak the highest
            // of the minutes' peaks (Codex's second review: read from the averages, a minute's
            // kept peak was lost).
            let mut by_minute: BTreeMap<Timestamp, (f64, f64)> = BTreeMap::new();
            for sample in samples {
                if let SampleData::Gauge { avg, peak, .. } = sample.data {
                    let minute = by_minute.entry(sample.at).or_default();
                    minute.0 += avg;
                    minute.1 += peak;
                }
            }
            let minutes = u32::try_from(by_minute.len()).unwrap_or(u32::MAX);
            #[expect(clippy::cast_precision_loss, reason = "at most 15 minutes")]
            let avg = by_minute.values().map(|m| m.0).sum::<f64>() / by_minute.len().max(1) as f64;
            let peak = by_minute.values().map(|m| m.1).fold(f64::MIN, f64::max);
            Some(SampleData::Gauge { avg, peak, minutes })
        }
        SampleData::Histogram { counts, .. } => {
            let mut total = vec![0_u64; counts.len()];
            let mut sum = 0.0;
            for sample in samples {
                if let SampleData::Histogram { counts, sum: s } = &sample.data
                    && counts.len() == total.len()
                {
                    for (t, c) in total.iter_mut().zip(counts) {
                        *t = t.saturating_add(*c);
                    }
                    sum += s;
                }
            }
            Some(SampleData::Histogram { counts: total, sum })
        }
    }
}
