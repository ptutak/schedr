//! What each metric is, for the code that registers it and for the metrics reference in the
//! documentation (see PROJECT.md, Observability): one list per role, so the name and help text
//! are written once, and a test per role checks the type and labels against a scrape.

use std::fmt;

#[cfg(test)]
mod tests;

/// A metric's type, as a scrape shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Only goes up; shown with `_total`.
    Counter,
    /// Goes up and down.
    Gauge,
    /// Counts observations into buckets; shown with `_bucket`, `_sum` and `_count`.
    Histogram,
    /// A constant labelled 1; shown with `_info`.
    Info,
}

impl fmt::Display for MetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
            Self::Info => "info",
        })
    }
}

/// One metric: the name it is registered under, its type, labels and help text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricDoc {
    /// The name as registered (without the registry's prefix or the type's suffix).
    pub name: &'static str,
    /// Its type.
    pub kind: MetricKind,
    /// Its label names.
    pub labels: &'static [&'static str],
    /// What it measures.
    pub help: &'static str,
}

impl MetricDoc {
    /// The name in a scrape's samples under `prefix` (e.g. `schedr`): with `_total` for a
    /// counter and `_info` for an info metric.
    #[must_use]
    pub fn exposed(&self, prefix: Option<&str>) -> String {
        let suffix = match self.kind {
            MetricKind::Counter => "_total",
            MetricKind::Info => "_info",
            MetricKind::Gauge | MetricKind::Histogram => "",
        };
        match prefix {
            Some(prefix) => format!("{prefix}_{}{suffix}", self.name),
            None => format!("{}{suffix}", self.name),
        }
    }
}

/// What doesn't match between `docs` and a `scrape` (the text format) of a registry under
/// `prefix` that holds them, every label family having a member: a metric missing from the
/// scrape or with another type, help text or label names, and a metric the scrape has but
/// `docs` doesn't. Empty when they match.
#[must_use]
pub fn scrape_mismatches(scrape: &str, prefix: Option<&str>, docs: &[MetricDoc]) -> Vec<String> {
    let base = |doc: &MetricDoc| match prefix {
        Some(prefix) => format!("{prefix}_{}", doc.name),
        None => doc.name.to_owned(),
    };
    let mut wrong = Vec::new();
    let typed: Vec<(&str, &str)> = scrape
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE ")?.split_once(' '))
        .collect();
    for (name, _) in &typed {
        if !docs.iter().any(|doc| base(doc) == *name) {
            wrong.push(format!("`{name}` is not documented"));
        }
    }
    for doc in docs {
        let name = base(doc);
        let kind = doc.kind.to_string();
        if !typed.contains(&(name.as_str(), kind.as_str())) {
            wrong.push(format!("`{name}` isn't in the scrape as a {kind}"));
            continue;
        }
        if !scrape.contains(&format!("# HELP {name} {}.\n", doc.help)) {
            wrong.push(format!("`{name}` has other help text"));
        }
        for labels in samples(scrape, &name) {
            if labels != doc.labels {
                wrong.push(format!("`{name}` has the labels {labels:?}"));
            }
        }
    }
    wrong
}

/// The label names of each sample of the metric `name` (`le` left out).
fn samples<'a>(scrape: &'a str, name: &str) -> Vec<Vec<&'a str>> {
    let suffixes = ["", "_total", "_info", "_bucket", "_sum", "_count"];
    scrape
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| {
            let sample = line.split(['{', ' ']).next().unwrap_or_default();
            suffixes.iter().any(|s| sample == format!("{name}{s}"))
        })
        .map(|line| {
            let Some((_, rest)) = line.split_once('{') else {
                return Vec::new();
            };
            rest.split("\",")
                .filter_map(|pair| pair.split_once('=').map(|(key, _)| key))
                .filter(|key| *key != "le")
                .collect()
        })
        .collect()
}
