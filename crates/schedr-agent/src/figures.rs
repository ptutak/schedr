//! What a worker's plugins report of their declared metrics (decision 513): kept per
//! extension and version, as a counter's total, a gauge's value or a histogram's buckets;
//! served on the worker's Prometheus `/metrics` as `schedr_extension_<extension>_<metric>` and
//! sent to the master in its heartbeat about once a minute.
//!
//! Series are bounded: at most `max_series` per metric. A new label combination past it is
//! folded into one series whose labels are all `other`, with one warning; a label value outside
//! its declared values counts as `other` too.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::MetricType;
use schedr_model::extension::{
    Declarations, ExtensionEvent, ExtensionFigures, MAX_LABEL_VALUE, MAX_PENDING_EVENTS,
    MetricDeclaration, MetricFigures, MetricKind, MetricPoint, OTHER, SeriesFigures, VERSION_LABEL,
};

#[cfg(test)]
mod tests;

type Labels = BTreeMap<String, String>;

/// The most undeclared metric names an extension version is warned of, each once.
pub const MAX_WARNED: usize = 100;

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Number(f64),
    Histogram { counts: Vec<u64>, sum: f64 },
}

#[derive(Debug)]
struct Metric {
    declaration: MetricDeclaration,
    /// Totals since the plugin first reported them, for `/metrics`.
    series: BTreeMap<Labels, Value>,
    /// What counters and histograms grew by since the last figures taken: sent once.
    pending: BTreeMap<Labels, Value>,
    /// Whether the series cap was reached (warned once).
    folded: bool,
}

#[derive(Debug)]
struct Extension {
    declarations: Declarations,
    max_series: u32,
    metrics: BTreeMap<String, Metric>,
    /// Undeclared metrics warned of, once each.
    warned: BTreeSet<String>,
    /// Whether the master has had its declarations on this stream.
    declarations_sent: bool,
}

/// The worker's extension figures, shared by its attempts, its heartbeat and its `/metrics`.
#[derive(Debug, Clone, Default)]
pub struct Figures {
    all: Arc<Mutex<BTreeMap<(String, String), Extension>>>,
    /// What happened to its extensions since the last heartbeat (DN19), oldest first.
    events: Arc<Mutex<Pending>>,
}

/// The events held for the next heartbeat, at most `most` (the worker's
/// `EXTENSION_EVENTS_PENDING`, [`MAX_PENDING_EVENTS`] by default).
#[derive(Debug)]
struct Pending {
    list: VecDeque<ExtensionEvent>,
    most: usize,
}

impl Default for Pending {
    fn default() -> Self {
        Self {
            list: VecDeque::new(),
            most: MAX_PENDING_EVENTS,
        }
    }
}

/// Why a point isn't kept.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotKept {
    /// Its metric isn't declared.
    #[error("metric `{0}` isn't declared")]
    Undeclared(String),
    /// Its labels aren't the declared ones.
    #[error("metric `{metric}`: its labels are {declared:?}")]
    Labels {
        /// The metric.
        metric: String,
        /// Its declared labels.
        declared: Vec<String>,
    },
    /// Its value isn't a finite number, or a counter's is negative.
    #[error("metric `{0}`: a value that can't be kept")]
    Value(String),
}

fn new_value(declaration: &MetricDeclaration) -> Value {
    match declaration.kind {
        MetricKind::Counter | MetricKind::Gauge => Value::Number(0.0),
        MetricKind::Histogram => Value::Histogram {
            counts: vec![0; declaration.buckets.len() + 1],
            sum: 0.0,
        },
    }
}

impl Figures {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<(String, String), Extension>> {
        self.all.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keeps `event` for the next heartbeat; past the most held, the oldest goes.
    pub fn event(&self, event: ExtensionEvent) {
        let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
        while events.list.len() >= events.most.max(1) {
            events.list.pop_front();
        }
        events.list.push_back(event);
    }

    /// Holds at most `most` events for the next heartbeat (the worker's
    /// `EXTENSION_EVENTS_PENDING`).
    pub fn hold_at_most(&self, most: usize) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .most = most;
    }

    /// The events to send, oldest first; none kept after.
    #[must_use]
    pub fn take_events(&self) -> Vec<ExtensionEvent> {
        let mut events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
        events.list.drain(..).collect()
    }

    /// Takes `extension` `version`'s declarations: its series are kept while they stay the
    /// same, started again when they change.
    pub fn declare(
        &self,
        extension: &str,
        version: &str,
        declarations: Declarations,
        max_series: u32,
    ) {
        let mut all = self.lock();
        let key = (extension.to_owned(), version.to_owned());
        if all
            .get(&key)
            .is_some_and(|e| e.declarations == declarations && e.max_series == max_series)
        {
            return;
        }
        let metrics = declarations
            .metrics
            .iter()
            .map(|d| {
                (
                    d.name.clone(),
                    Metric {
                        declaration: d.clone(),
                        series: BTreeMap::new(),
                        pending: BTreeMap::new(),
                        folded: false,
                    },
                )
            })
            .collect();
        all.insert(
            key,
            Extension {
                declarations,
                max_series: max_series.max(1),
                metrics,
                warned: BTreeSet::new(),
                declarations_sent: false,
            },
        );
    }

    /// Keeps `point` of `extension` `version`: a counter adds it, a gauge is set to it, a
    /// histogram observes it.
    ///
    /// # Errors
    ///
    /// [`NotKept`] for a point its declarations don't allow; an undeclared metric's is warned
    /// of once, by the caller.
    pub fn observe(
        &self,
        extension: &str,
        version: &str,
        point: &MetricPoint,
    ) -> Result<(), NotKept> {
        let mut all = self.lock();
        let Some(ext) = all.get_mut(&(extension.to_owned(), version.to_owned())) else {
            return Err(NotKept::Undeclared(point.name.clone()));
        };
        let max_series = usize::try_from(ext.max_series).unwrap_or(usize::MAX);
        let Some(metric) = ext.metrics.get_mut(&point.name) else {
            return Err(NotKept::Undeclared(point.name.clone()));
        };
        let declared: Vec<String> = metric
            .declaration
            .labels
            .iter()
            .map(|l| l.name.clone())
            .collect();
        let given: Vec<&String> = point.labels.keys().collect();
        let mut sorted = declared.clone();
        sorted.sort();
        if given.iter().copied().ne(sorted.iter()) {
            return Err(NotKept::Labels {
                metric: point.name.clone(),
                declared,
            });
        }
        let value = point.value;
        let bad =
            !value.is_finite() || (metric.declaration.kind == MetricKind::Counter && value < 0.0);
        if bad {
            return Err(NotKept::Value(point.name.clone()));
        }
        let mut labels: Labels = metric
            .declaration
            .labels
            .iter()
            .map(|label| {
                let given = &point.labels[&label.name];
                let allowed = label.values.as_ref().is_none_or(|v| v.contains(given));
                let shown = if allowed {
                    given.chars().take(MAX_LABEL_VALUE).collect()
                } else {
                    OTHER.to_owned()
                };
                (label.name.clone(), shown)
            })
            .collect();
        if !metric.series.contains_key(&labels) {
            let others = metric
                .series
                .keys()
                .filter(|l| !l.values().all(|v| v == OTHER) || l.is_empty())
                .count();
            if others >= max_series && !labels.is_empty() {
                if !metric.folded {
                    metric.folded = true;
                    tracing::warn!(
                        extension,
                        version,
                        metric = point.name,
                        "a metric past its {max_series} series: new ones are folded into `other`"
                    );
                }
                labels = labels
                    .into_keys()
                    .map(|name| (name, OTHER.to_owned()))
                    .collect();
            }
        }
        let declaration = &metric.declaration;
        let total = metric
            .series
            .entry(labels.clone())
            .or_insert_with(|| new_value(declaration));
        add(total, declaration, value);
        // A gauge's figures are its value now: nothing waits for them.
        if declaration.kind != MetricKind::Gauge {
            let grown = metric
                .pending
                .entry(labels)
                .or_insert_with(|| new_value(declaration));
            add(grown, declaration, value);
        }
        Ok(())
    }

    /// Whether `extension` `version` has declarations kept.
    #[must_use]
    pub fn is_declared(&self, extension: &str, version: &str) -> bool {
        self.lock()
            .contains_key(&(extension.to_owned(), version.to_owned()))
    }

    /// Whether an undeclared `metric` of `extension` is warned of now: once each.
    #[must_use]
    pub fn warn_once(&self, extension: &str, version: &str, metric: &str) -> bool {
        self.lock()
            .get_mut(&(extension.to_owned(), version.to_owned()))
            .is_none_or(|ext| {
                // Bounded (Codex's review of M5.2i5): past MAX_WARNED names, no more warnings,
                // their points dropped all the same.
                ext.warned.len() < MAX_WARNED && ext.warned.insert(metric.to_owned())
            })
    }

    /// `extension` `version`'s figures not sent after all (past the heartbeat's room): its
    /// declarations go with the next ones (agy's review of M5.2i5: sent with figures dropped,
    /// they never reached the master, which then left out its samples until a new stream).
    pub fn unsent(&self, extension: &str, version: &str) {
        if let Some(ext) = self
            .lock()
            .get_mut(&(extension.to_owned(), version.to_owned()))
        {
            ext.declarations_sent = false;
        }
    }

    /// A new stream to the master: the declarations are sent again with the next figures.
    pub fn connected(&self) {
        for ext in self.lock().values_mut() {
            ext.declarations_sent = false;
        }
    }

    /// The figures to send: each extension version's growth since the last ones taken (the
    /// counters' and histograms', then started again) and its gauges' values, its declarations
    /// with them the first time on a stream (or after they changed).
    #[must_use]
    pub fn take_figures(&self) -> Vec<ExtensionFigures> {
        let mut all = self.lock();
        all.iter_mut()
            .map(|((extension, version), ext)| {
                let declarations = (!ext.declarations_sent).then(|| ext.declarations.clone());
                ext.declarations_sent = true;
                ExtensionFigures {
                    extension: extension.clone(),
                    version: version.clone(),
                    max_series: ext.max_series,
                    declarations,
                    metrics: ext
                        .metrics
                        .iter_mut()
                        .map(|(name, metric)| {
                            let series = if metric.declaration.kind == MetricKind::Gauge {
                                metric
                                    .series
                                    .iter()
                                    .map(|(labels, value)| series_figures(labels, value))
                                    .collect()
                            } else {
                                std::mem::take(&mut metric.pending)
                                    .iter()
                                    .map(|(labels, value)| series_figures(labels, value))
                                    .collect()
                            };
                            MetricFigures {
                                name: name.clone(),
                                kind: metric.declaration.kind,
                                buckets: metric.declaration.buckets.clone(),
                                series,
                            }
                        })
                        .collect(),
                }
            })
            .collect()
    }
}

/// `value` taken into `into`: a counter adds it, a gauge is set to it, a histogram observes it.
fn add(into: &mut Value, declaration: &MetricDeclaration, value: f64) {
    match into {
        Value::Number(n) => {
            if declaration.kind == MetricKind::Counter {
                *n += value;
            } else {
                *n = value;
            }
        }
        Value::Histogram { counts, sum } => {
            let bucket = declaration
                .buckets
                .iter()
                .position(|bound| value <= *bound)
                .unwrap_or(declaration.buckets.len());
            if let Some(count) = counts.get_mut(bucket) {
                *count = count.saturating_add(1);
            }
            *sum += value;
        }
    }
}

fn series_figures(labels: &Labels, value: &Value) -> SeriesFigures {
    match value {
        Value::Number(n) => SeriesFigures {
            labels: labels.clone(),
            value: *n,
            counts: Vec::new(),
            sum: 0.0,
        },
        Value::Histogram { counts, sum } => SeriesFigures {
            labels: labels.clone(),
            value: 0.0,
            counts: counts.clone(),
            sum: *sum,
        },
    }
}

/// A name's characters Prometheus allows: an extension's `-` as `_`.
fn prometheus_name(extension: &str, metric: &str) -> String {
    format!("extension_{}_{metric}", extension.replace('-', "_"))
}

impl Collector for Figures {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let all = self.lock();
        // One family per extension and metric, its versions as a label.
        let mut families: BTreeMap<String, Vec<(&str, &Metric)>> = BTreeMap::new();
        for ((extension, version), ext) in all.iter() {
            for (name, metric) in &ext.metrics {
                families
                    .entry(prometheus_name(extension, name))
                    .or_default()
                    .push((version.as_str(), metric));
            }
        }
        for (name, versions) in families {
            let Some((_, first)) = versions.first() else {
                continue;
            };
            let declaration = &first.declaration;
            let kind = match declaration.kind {
                MetricKind::Counter => MetricType::Counter,
                MetricKind::Gauge => MetricType::Gauge,
                MetricKind::Histogram => MetricType::Histogram,
            };
            let mut family = encoder.encode_descriptor(&name, &declaration.help, None, kind)?;
            for (version, metric) in &versions {
                if metric.declaration.kind != declaration.kind {
                    continue;
                }
                for (labels, value) in &metric.series {
                    let mut set: Vec<(String, String)> =
                        labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    set.push((VERSION_LABEL.to_owned(), (*version).to_owned()));
                    let series = family.encode_family(&set)?;
                    match value {
                        Value::Number(n) => {
                            if declaration.kind == MetricKind::Counter {
                                prometheus_client::metrics::counter::ConstCounter::new(*n)
                                    .encode(series)?;
                            } else {
                                prometheus_client::metrics::gauge::ConstGauge::new(*n)
                                    .encode(series)?;
                            }
                        }
                        Value::Histogram { counts, sum } => {
                            let mut cumulative = 0_u64;
                            let mut buckets: Vec<(f64, u64)> = Vec::new();
                            for (i, count) in counts.iter().enumerate() {
                                cumulative = cumulative.saturating_add(*count);
                                let bound = metric
                                    .declaration
                                    .buckets
                                    .get(i)
                                    .copied()
                                    .unwrap_or(f64::INFINITY);
                                buckets.push((bound, cumulative));
                            }
                            let mut series = series;
                            series.encode_histogram::<prometheus_client::encoding::NoLabelSet>(
                                *sum, cumulative, &buckets, None,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
