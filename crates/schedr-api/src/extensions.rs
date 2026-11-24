//! The extensions workers ran (decision 513): each one's versions and workers, its declared
//! metrics and page, and its metrics over a range, summed over its workers and versions.
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use std::collections::{BTreeMap, BTreeSet};

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use jiff::Timestamp;
use schedr_config::masked::mask_text;
use schedr_db::extension_samples::{ExtensionSample, ExtensionVersion, SampleData};
use schedr_db::samples::MINUTE;
use schedr_model::extension::{Declarations, MetricKind, OTHER, page_panels};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::routes::ApiResult;
use crate::series::range_of;
use crate::state::ApiState;

pub mod panels;
#[cfg(test)]
mod tests;

pub use panels::ExtensionPanelView;

/// An extension, in the list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionSummaryView {
    /// Its name.
    pub name: String,
    /// The versions workers ran, the highest first ([`version_order`]).
    pub versions: Vec<String>,
    /// The workers that ran it.
    pub workers: Vec<String>,
    /// When a worker last sent its figures.
    pub seen_at: Timestamp,
}

/// The extensions workers ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionList {
    /// By name.
    pub extensions: Vec<ExtensionSummaryView>,
}

/// One version of an extension on one worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionWorkerView {
    /// Its version.
    pub version: String,
    /// The worker.
    pub worker_id: String,
    /// When it last sent its figures.
    pub seen_at: Timestamp,
}

/// What happened to an extension on a worker (DN19).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionEventView {
    /// When.
    pub at: Timestamp,
    /// The worker.
    pub worker_id: String,
    /// The extension's version; empty when not known.
    pub version: String,
    /// `discovered`, `skipped`, `refused` or `dropped`.
    pub kind: String,
    /// What about it.
    pub message: String,
}

/// One of an extension's attempts and the latest phase it reported (DN19).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionAttemptView {
    /// When it reported it.
    pub at: Timestamp,
    /// Its DAG.
    pub dag_id: String,
    /// Its run.
    pub run_id: String,
    /// Its task.
    pub task_id: String,
    /// Its map index; none for a task that isn't mapped.
    pub map_index: Option<u32>,
    /// Its try.
    pub try_number: u32,
    /// How it ended; none while it runs.
    pub ended: Option<String>,
    /// Its latest phase.
    pub phase: String,
    /// More about it.
    pub message: Option<String>,
    /// How far along, 0 to 1.
    pub progress: Option<f64>,
}

/// The most events and attempts an extension's view lists.
pub const LISTED: u32 = 50;

/// An extension: where it runs, its metrics and its page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionView {
    /// Its name.
    pub name: String,
    /// Each version on each worker.
    pub workers: Vec<ExtensionWorkerView>,
    /// The version shown, its metrics and page read from its declarations: the one asked for,
    /// else the highest ([`version_order`], DN40).
    pub version: String,
    /// Its metrics' declarations (name, kind, help, unit, labels, buckets).
    #[schema(value_type = Vec<Object>)]
    pub metrics: Vec<serde_json::Value>,
    /// Its page's panels, each in its row and cell: the declared ones, then a `full` row for
    /// each metric none shows.
    #[schema(value_type = Vec<Object>)]
    pub panels: Vec<serde_json::Value>,
    /// The most series each metric keeps.
    pub max_series: u32,
    /// What happened to it on its workers, newest first, at most [`LISTED`] (DN19).
    #[serde(default)]
    pub events: Vec<ExtensionEventView>,
    /// Its attempts with a phase, newest first, at most [`LISTED`]: each one's latest.
    #[serde(default)]
    pub attempts: Vec<ExtensionAttemptView>,
}

/// One bucket of a series.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionPointView {
    /// The bucket's start.
    pub at: Timestamp,
    /// A counter's growth in it, or a gauge's average; 0 for a histogram.
    pub value: f64,
    /// A gauge's highest; none for the other kinds.
    pub peak: Option<f64>,
    /// A histogram's new observations per bucket, the last past its last bound.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub counts: Vec<u64>,
    /// A histogram's observations' sum.
    pub sum: f64,
}

/// One series of a metric over the range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionSeriesView {
    /// Its labels' values (all `other` for the series past the cap).
    pub labels: BTreeMap<String, String>,
    /// Its buckets with samples, oldest first.
    pub points: Vec<ExtensionPointView>,
}

/// One metric over the range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionMetricView {
    /// Its name.
    pub name: String,
    /// `counter`, `gauge` or `histogram`.
    pub kind: String,
    /// A histogram's buckets' upper bounds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<f64>,
    /// Its series, at most the extension's `max_series` and one `other`.
    pub series: Vec<ExtensionSeriesView>,
    /// Whether series past the cap were folded into `other`.
    pub folded: bool,
}

/// An extension's metrics over a range, summed over its workers and versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExtensionMetrics {
    /// The range: `1h`, `24h`, `7d` or `30d`.
    pub range: String,
    /// A bucket's width, in seconds.
    pub bucket_s: u32,
    /// The range's start.
    pub from: Timestamp,
    /// Its end.
    pub to: Timestamp,
    /// Each declared metric, its series summed.
    pub metrics: Vec<ExtensionMetricView>,
    /// Each page panel drawing a metric, its lines worked out as its functions say (decision
    /// 516).
    #[serde(default)]
    pub panels: Vec<ExtensionPanelView>,
}

/// Whose events and attempts.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ExtensionQuery {
    /// One worker's events and attempts only (DN21), its newest [`LISTED`]; all workers' when
    /// absent. Its workers are all listed either way.
    pub worker: Option<String>,
    /// This version (DN40): its declarations, its events; the highest when absent.
    pub version: Option<String>,
}

/// Which range, and whose figures.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ExtensionMetricsQuery {
    /// `1h`, `24h` (the default), `7d` or `30d`.
    pub range: Option<String>,
    /// One worker's figures only (DN21), for `1h` or `24h`: the master keeps each worker's a
    /// minute each for a day, and sums them over the workers past it. All workers' when absent.
    pub worker: Option<String>,
    /// This version's figures only (DN40), read as it declares them; the highest when absent.
    pub version: Option<String>,
}

/// Versions in order, as people read them (DN40): their dot-separated parts compared one by
/// one, a part of ASCII digits as a number (`1.10` after `1.9`, `1.010` as `1.10`), before one
/// that isn't, which compares as text; a version with more parts after its prefix; equal so far,
/// the text itself, so the order is total (agy's review of DN40: `1.0` and `1.00` were equal and
/// a mixed part made it intransitive, which a sort may panic on).
#[must_use]
pub fn version_order(a: &str, b: &str) -> std::cmp::Ordering {
    /// A part's place: numbers first, by value (leading zeros dropped, then by length and digits:
    /// no overflow), then text.
    fn key(part: &str) -> (u8, usize, &str) {
        if !part.is_empty() && part.bytes().all(|c| c.is_ascii_digit()) {
            let digits = part.trim_start_matches('0');
            (0, digits.len(), digits)
        } else {
            (1, 0, part)
        }
    }
    fn parts(version: &str) -> Vec<(u8, usize, &str)> {
        version.split('.').map(key).collect()
    }
    parts(a).cmp(&parts(b)).then_with(|| a.cmp(b))
}

/// The extensions workers ran, with their versions and workers.
#[utoipa::path(get, path = "/api/v1/extensions", tag = "workers",
    responses((status = 200, body = ExtensionList)))]
pub async fn list_extensions(State(state): State<ApiState>) -> ApiResult<ExtensionList> {
    let versions = state.db.extension_versions().await?;
    // Known only by their events too: a plugin skipped or refused, never run (Codex's second
    // review of M5.2i5: its events were stored, its page not found).
    let only_events: Vec<ExtensionSummaryView> = state
        .db
        .extensions_with_events()
        .await?
        .into_iter()
        .filter(|(name, _, _)| !versions.iter().any(|v| &v.name == name))
        .map(|(name, workers, seen_at)| ExtensionSummaryView {
            name,
            versions: Vec::new(),
            workers,
            seen_at,
        })
        .collect();
    let mut by_name: BTreeMap<String, Vec<ExtensionVersion>> = BTreeMap::new();
    for version in versions {
        by_name
            .entry(version.name.clone())
            .or_default()
            .push(version);
    }
    Ok(Json(ExtensionList {
        extensions: by_name
            .into_iter()
            .filter_map(|(name, versions)| {
                let seen_at = versions.iter().map(|v| v.seen_at).max()?;
                Some(ExtensionSummaryView {
                    name,
                    versions: {
                        let mut listed: Vec<String> = versions
                            .iter()
                            .map(|v| v.version.clone())
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect();
                        listed.sort_by(|a, b| version_order(b, a));
                        listed
                    },
                    workers: versions
                        .iter()
                        .map(|v| v.worker_id.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                    seen_at,
                })
            })
            .chain(only_events)
            // By name, those known only by their events among them (agy's review of M5.2i5:
            // they came last, and the tabs with them).
            .map(|summary| (summary.name.clone(), summary))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect(),
    }))
}

/// `name`'s versions on workers (only `version` when given), the highest first and its newest
/// heard of first ([`version_order`], DN40); not found without any.
async fn versions_of(
    state: &ApiState,
    name: &str,
    version: Option<&str>,
) -> Result<Vec<ExtensionVersion>, ApiError> {
    let mut versions: Vec<ExtensionVersion> = state
        .db
        .extension_versions()
        .await?
        .into_iter()
        .filter(|v| v.name == name)
        .filter(|v| version.is_none_or(|asked| v.version == asked))
        .collect();
    if let Some(asked) = version
        && versions.is_empty()
    {
        return Err(ApiError::NotFound(format!(
            "no worker has run version `{asked}` of extension `{name}`"
        )));
    }
    // One version shown, never several mixed (agy's review of DN40): without one asked for, the
    // highest.
    if version.is_none()
        && let Some(highest) = versions
            .iter()
            .map(|v| v.version.clone())
            .max_by(|a, b| version_order(a, b))
    {
        versions.retain(|v| v.version == highest);
    }
    let known_by_events = || async {
        Ok::<_, ApiError>(
            !state
                .db
                .extension_events(name, (None, None), 1)
                .await?
                .is_empty(),
        )
    };
    if versions.is_empty() && !known_by_events().await? {
        return Err(ApiError::NotFound(format!(
            "no worker has run extension `{name}`"
        )));
    }
    versions.sort_by(|a, b| version_order(&b.version, &a.version).then(b.seen_at.cmp(&a.seen_at)));
    Ok(versions)
}

fn to_values<T: Serialize>(items: &[T]) -> Vec<serde_json::Value> {
    items
        .iter()
        .filter_map(|item| serde_json::to_value(item).ok())
        .collect()
}

/// An extension: its versions on its workers, and its metrics and page as its newest version
/// declares them.
#[utoipa::path(get, path = "/api/v1/extensions/{name}", tag = "workers",
    params(("name" = String, Path, description = "The extension"), ExtensionQuery),
    responses((status = 200, body = ExtensionView), (status = 400, body = crate::views::ErrorBody),
        (status = 404, body = crate::views::ErrorBody)))]
pub async fn get_extension(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    query: Result<Query<ExtensionQuery>, QueryRejection>,
) -> ApiResult<ExtensionView> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let worker = query.worker.filter(|w| !w.is_empty());
    let version = query.version.filter(|v| !v.is_empty());
    let versions = versions_of(&state, &name, version.as_deref()).await?;
    // Known only by its events: its workers are theirs (agy's review of M5.2i5: none listed).
    let event_workers: Vec<ExtensionWorkerView> = if versions.is_empty() {
        state
            .db
            .extensions_with_events()
            .await?
            .into_iter()
            .filter(|(of, _, _)| of == &name)
            .flat_map(|(_, workers, seen_at)| {
                workers
                    .into_iter()
                    .map(move |worker_id| ExtensionWorkerView {
                        version: String::new(),
                        worker_id,
                        seen_at,
                    })
            })
            .collect()
    } else {
        Vec::new()
    };
    let shown = versions.first().map(|v| v.version.clone());
    let events = state
        .db
        .extension_events(&name, (worker.as_deref(), shown.as_deref()), LISTED)
        .await?
        .into_iter()
        .map(|e| ExtensionEventView {
            at: e.at,
            worker_id: e.worker_id,
            version: e.version,
            kind: e.kind.name().to_owned(),
            message: mask_text(&e.message),
        })
        .collect();
    let attempts = state
        .db
        .extension_attempts(&name, (worker.as_deref(), shown.as_deref()), LISTED)
        .await?
        .into_iter()
        .map(|a| ExtensionAttemptView {
            at: a.status.at,
            dag_id: a.dag_id,
            run_id: a.run_id,
            task_id: a.task_id,
            map_index: u32::try_from(a.map_index).ok(),
            try_number: u32::try_from(a.try_number).unwrap_or(0),
            ended: a.ended,
            phase: mask_text(&a.status.phase),
            message: a.status.message.as_deref().map(mask_text),
            progress: a.status.progress,
        })
        .collect();
    // Known only by its events: no version, nothing declared.
    let (version, metrics, panels, max_series) = match versions.first() {
        Some(newest) => (
            newest.version.clone(),
            to_values(&newest.declarations.metrics),
            to_values(&page_panels(&newest.declarations)),
            newest.max_series,
        ),
        None => (
            String::new(),
            Vec::new(),
            Vec::new(),
            schedr_model::extension::DEFAULT_MAX_SERIES,
        ),
    };
    Ok(Json(ExtensionView {
        name: name.clone(),
        workers: versions
            .iter()
            .map(|v| ExtensionWorkerView {
                version: v.version.clone(),
                worker_id: v.worker_id.clone(),
                seen_at: v.seen_at,
            })
            .chain(event_workers)
            .collect(),
        version,
        metrics,
        panels,
        max_series,
        events,
        attempts,
    }))
}

/// An extension's metrics over a range, in buckets, summed over its workers (or of one worker,
/// `worker`, up to a day: DN21), of one version (`version`, else summed over those declaring a
/// metric as the highest does: DN40); at most its `max_series` series a metric, the smallest
/// past it folded into `other`.
#[utoipa::path(get, path = "/api/v1/extensions/{name}/metrics", tag = "workers",
    params(("name" = String, Path, description = "The extension"), ExtensionMetricsQuery),
    responses((status = 200, body = ExtensionMetrics), (status = 400, body = crate::views::ErrorBody),
        (status = 404, body = crate::views::ErrorBody)))]
pub async fn extension_metrics(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    query: Result<Query<ExtensionMetricsQuery>, QueryRejection>,
) -> ApiResult<ExtensionMetrics> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let range = range_of(query.range.as_deref())?;
    let worker = query.worker.filter(|w| !w.is_empty());
    if worker.is_some() && range.source() != MINUTE {
        return Err(ApiError::BadRequest(format!(
            "one worker's figures are kept a day: `range` is 1h or 24h with `worker`, not `{}`",
            range.name()
        )));
    }
    let version = query.version.filter(|v| !v.is_empty());
    let versions = versions_of(&state, &name, version.as_deref()).await?;
    let to = (state.clock)();
    let (from, cut, to) = range.bounds(&state.db, to).await?;
    let mut samples = Vec::new();
    if range.source() != MINUTE {
        samples.extend(
            state
                .db
                .extension_samples(Some(&name), range.source(), from, cut)
                .await?,
        );
    }
    samples.extend(
        state
            .db
            .extension_samples(Some(&name), MINUTE, cut, to)
            .await?,
    );
    if let Some(worker) = &worker {
        samples.retain(|s| &s.worker_id == worker);
    }
    // The version shown alone: asked for, else the highest (`versions_of` keeps only it).
    if let Some(shown) = versions.first() {
        samples.retain(|s| s.version == shown.version);
    }
    let samples = of_compatible_versions(samples, &versions);
    // Known only by its events: no metrics.
    let (metrics, panels) = versions.first().map_or_else(Default::default, |newest| {
        (
            metrics_of(
                &newest.declarations,
                newest.max_series,
                &samples,
                range.bucket_s(),
            ),
            panels_of(
                &newest.declarations,
                newest.max_series,
                &samples,
                range.bucket_s(),
            ),
        )
    });
    Ok(Json(ExtensionMetrics {
        range: range.name().to_owned(),
        bucket_s: range.bucket_s(),
        from,
        to,
        metrics,
        panels,
    }))
}

/// `samples` of the versions that declare their metric as the newest of `versions` (the
/// first) does, of the same kind and buckets (Codex's review of M5.2i5: another's read as the
/// newest's are wrong numbers).
#[must_use]
pub fn of_compatible_versions(
    samples: Vec<ExtensionSample>,
    versions: &[ExtensionVersion],
) -> Vec<ExtensionSample> {
    let Some(newest) = versions.first() else {
        return Vec::new();
    };
    let declared = |v: &ExtensionVersion, metric: &str| {
        v.declarations
            .metrics
            .iter()
            .find(|m| m.name == metric)
            .map(|m| (m.kind, m.buckets.clone()))
    };
    // A minute's sample by its worker's declaration of its version; a rollup (across workers)
    // only when each worker's declaration of its version agrees (Codex's second review: two
    // workers' version `1` declared differently were mixed).
    samples
        .into_iter()
        .filter(|s| {
            let as_newest = declared(newest, &s.metric);
            let mut of_it = versions
                .iter()
                .filter(|v| v.version == s.version)
                .filter(|v| s.worker_id.is_empty() || v.worker_id == s.worker_id)
                .peekable();
            as_newest.is_some()
                && of_it.peek().is_some()
                && of_it.all(|v| declared(v, &s.metric) == as_newest)
        })
        .collect()
}

/// `samples` as `declarations`' metrics in buckets of `bucket_s` seconds, at most
/// `max_series` series each and an `other`.
#[must_use]
pub fn metrics_of(
    declarations: &Declarations,
    max_series: u32,
    samples: &[ExtensionSample],
    bucket_s: u32,
) -> Vec<ExtensionMetricView> {
    declarations
        .metrics
        .iter()
        .map(|declared| {
            let mine: Vec<&ExtensionSample> = samples
                .iter()
                .filter(|s| s.metric == declared.name)
                .collect();
            let (series, folded) = capped(&mine, max_series);
            ExtensionMetricView {
                name: declared.name.clone(),
                kind: match declared.kind {
                    MetricKind::Counter => "counter",
                    MetricKind::Gauge => "gauge",
                    MetricKind::Histogram => "histogram",
                }
                .to_owned(),
                buckets: declared.buckets.clone(),
                series: series
                    .into_iter()
                    .map(|(labels, samples)| ExtensionSeriesView {
                        // Masked by pattern too, as a task's texts are (agy's review of M5.2i5:
                        // the worker masks only the secrets the attempt read).
                        labels: labels
                            .into_iter()
                            .map(|(name, value)| (name, mask_text(&value)))
                            .collect(),
                        points: points(&samples, bucket_s),
                    })
                    .collect(),
                folded,
            }
        })
        .collect()
}

/// Each of `declarations`' page panels that draws a metric, its lines from `samples`
/// (decision 516).
#[must_use]
pub fn panels_of(
    declarations: &Declarations,
    max_series: u32,
    samples: &[ExtensionSample],
    bucket_s: u32,
) -> Vec<ExtensionPanelView> {
    // Split by metric once (agy's review of M5.2i10: each panel scanned every sample).
    let mut by_metric: BTreeMap<&str, Vec<&ExtensionSample>> = BTreeMap::new();
    for sample in samples {
        by_metric
            .entry(sample.metric.as_str())
            .or_default()
            .push(sample);
    }
    page_panels(declarations)
        .iter()
        .enumerate()
        .filter_map(|(index, panel)| {
            let declared = declarations
                .metrics
                .iter()
                .find(|m| Some(&m.name) == panel.metric.as_ref())?;
            let mine = by_metric
                .get(declared.name.as_str())
                .map_or(&[][..], Vec::as_slice);
            Some(panels::panel_view(
                u32::try_from(index).ok()?,
                panel,
                declared,
                mine,
                max_series,
                bucket_s,
            ))
        })
        .collect()
}

type Labels = BTreeMap<String, String>;

/// A sample's weight, to keep the largest series: a counter's growth, a gauge's peak, a
/// histogram's observations.
fn weight(sample: &ExtensionSample) -> f64 {
    match &sample.data {
        SampleData::Counter { growth, .. } => *growth,
        SampleData::Gauge { peak, .. } => peak.abs(),
        #[expect(clippy::cast_precision_loss, reason = "a weight, not a figure")]
        SampleData::Histogram { counts, .. } => counts.iter().sum::<u64>() as f64,
    }
}

/// `samples` by series, the largest `max_series` kept and the rest under all-`other` labels;
/// whether any were folded.
fn capped(
    samples: &[&ExtensionSample],
    max_series: u32,
) -> (BTreeMap<Labels, Vec<ExtensionSample>>, bool) {
    let mut by_labels: BTreeMap<Labels, Vec<ExtensionSample>> = BTreeMap::new();
    for sample in samples {
        by_labels
            .entry(sample.labels.clone())
            .or_default()
            .push((*sample).clone());
    }
    let is_other = |labels: &Labels| !labels.is_empty() && labels.values().all(|v| v == OTHER);
    let cap = usize::try_from(max_series).unwrap_or(usize::MAX);
    let named = by_labels.keys().filter(|l| !is_other(l)).count();
    if named <= cap {
        // Folded by a worker or the master already: an `other` says so (Codex's review).
        let folded = by_labels.keys().any(is_other);
        return (by_labels, folded);
    }
    let mut ranked: Vec<(Labels, f64)> = by_labels
        .iter()
        .filter(|(labels, _)| !is_other(labels))
        .map(|(labels, samples)| (labels.clone(), samples.iter().map(weight).sum()))
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut kept = BTreeMap::new();
    let mut other: Vec<ExtensionSample> = Vec::new();
    let mut other_labels: Option<Labels> = None;
    for (index, (labels, _)) in ranked.into_iter().enumerate() {
        let series = by_labels.remove(&labels).unwrap_or_default();
        if index < cap {
            kept.insert(labels, series);
        } else {
            other_labels.get_or_insert_with(|| {
                labels
                    .keys()
                    .map(|k| (k.clone(), OTHER.to_owned()))
                    .collect()
            });
            other.extend(series);
        }
    }
    for (labels, series) in by_labels {
        // Already `other`.
        other_labels.get_or_insert(labels);
        other.extend(series);
    }
    if let Some(labels) = other_labels {
        kept.insert(labels, other);
    }
    (kept, true)
}

/// One series' samples as buckets of `bucket_s` seconds, summed over workers and versions: a
/// counter's growth and a histogram's observations added; a gauge's minutes summed over workers,
/// then averaged over the bucket, with their peak.
fn points(samples: &[ExtensionSample], bucket_s: u32) -> Vec<ExtensionPointView> {
    let step = i64::from(bucket_s.max(1));
    let bucket_of = |at: Timestamp| {
        Timestamp::from_second(at.as_second().div_euclid(step) * step).unwrap_or(at)
    };
    // Gauges: each sample's time first (a minute, or a rollup's window), summed over workers.
    let mut gauges: BTreeMap<Timestamp, BTreeMap<Timestamp, (f64, f64, u32)>> = BTreeMap::new();
    let mut others: BTreeMap<Timestamp, ExtensionPointView> = BTreeMap::new();
    for sample in samples {
        let bucket = bucket_of(sample.at);
        match &sample.data {
            SampleData::Gauge {
                avg, peak, minutes, ..
            } => {
                let at = gauges
                    .entry(bucket)
                    .or_default()
                    .entry(sample.at)
                    .or_default();
                at.0 += avg;
                at.1 += peak;
                at.2 = at.2.max(*minutes);
            }
            SampleData::Counter { growth, .. } => {
                others.entry(bucket).or_insert_with(|| empty(bucket)).value += growth;
            }
            SampleData::Histogram { counts, sum } => {
                let point = others.entry(bucket).or_insert_with(|| empty(bucket));
                if point.counts.len() < counts.len() {
                    point.counts.resize(counts.len(), 0);
                }
                for (total, count) in point.counts.iter_mut().zip(counts) {
                    *total = total.saturating_add(*count);
                }
                point.sum += sum;
            }
        }
    }
    for (bucket, times) in gauges {
        let minutes: u32 = times.values().map(|(_, _, m)| (*m).max(1)).sum();
        let weighted: f64 = times
            .values()
            .map(|(avg, _, m)| avg * f64::from((*m).max(1)))
            .sum();
        let peak = times
            .values()
            .map(|(_, peak, _)| *peak)
            .fold(f64::MIN, f64::max);
        others.insert(
            bucket,
            ExtensionPointView {
                at: bucket,
                value: weighted / f64::from(minutes.max(1)),
                peak: Some(peak),
                counts: Vec::new(),
                sum: 0.0,
            },
        );
    }
    others.into_values().collect()
}

fn empty(at: Timestamp) -> ExtensionPointView {
    ExtensionPointView {
        at,
        value: 0.0,
        peak: None,
        counts: Vec::new(),
        sum: 0.0,
    }
}
