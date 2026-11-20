//! The Extensions pages (decision 513): every deployment's extensions, and one extension's page
//! of the panels its plugin declared, in numbered rows of one, two or three cells, drawn in
//! schedr's chart kinds and its 20 colours. It reads `GET /api/v1/extensions`,
//! `/extensions/{name}` and `/extensions/{name}/metrics?range=`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use askama::Template;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use jiff::tz::TimeZone;
use schedr_api::extensions::{
    ExtensionMetricView, ExtensionMetrics, ExtensionPointView, ExtensionSeriesView, ExtensionView,
};
use schedr_api::series::Range;
use schedr_client::{Transport, path_segment};
use schedr_model::extension::{Cell, ChartKind, MetricUnit, Panel, row_cells};

use crate::Ui;
use crate::chart::{self, Bar, Bars, Line, Spec, Unit};
use crate::page::Page;
use crate::pages::render;

#[cfg(test)]
mod tests;

/// One extension in the list.
#[derive(Debug, Clone)]
pub struct ListedExtension {
    /// Its name.
    pub name: String,
    /// Its page.
    pub href: String,
    /// Its versions, joined.
    pub versions: String,
    /// Its workers, joined.
    pub workers: String,
    /// When a worker last sent its figures.
    pub seen: String,
}

/// A deployment's extensions.
#[derive(Debug, Clone)]
pub struct DeploymentExtensions {
    /// The deployment.
    pub name: String,
    /// Why they can't be shown.
    pub error: Option<String>,
    /// Its extensions.
    pub extensions: Vec<ListedExtension>,
}

#[derive(Template)]
#[template(path = "extensions.html")]
struct ListPage {
    page: Page,
    deployments: Vec<DeploymentExtensions>,
}

/// `GET /extensions`: every deployment's extensions.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    // One deployment: its first extension's tab (DN21), where the others are tabs beside it.
    if let [only] = ui.deployments.as_slice()
        && let Ok(list) = only.client.extensions().await
        && let Some(first) = list.extensions.first()
    {
        let href = format!(
            "{}/d/{}/extensions/{}",
            page.base,
            path_segment(&only.name),
            path_segment(&first.name)
        );
        return axum::response::Redirect::to(&href).into_response();
    }
    let mut deployments = Vec::new();
    for deployment in &ui.deployments {
        let zone = page.zone.clone().unwrap_or(TimeZone::UTC);
        let listed = match deployment.client.extensions().await {
            Ok(list) => DeploymentExtensions {
                name: deployment.name.clone(),
                error: None,
                extensions: list
                    .extensions
                    .into_iter()
                    .map(|e| ListedExtension {
                        href: format!(
                            "{}/d/{}/extensions/{}",
                            page.base,
                            path_segment(&deployment.name),
                            path_segment(&e.name)
                        ),
                        name: e.name,
                        versions: e.versions.join(", "),
                        workers: e.workers.join(", "),
                        seen: e
                            .seen_at
                            .to_zoned(zone.clone())
                            .strftime("%Y-%m-%d %H:%M")
                            .to_string(),
                    })
                    .collect(),
            },
            Err(error) => DeploymentExtensions {
                name: deployment.name.clone(),
                error: Some(error.to_string()),
                extensions: Vec::new(),
            },
        };
        deployments.push(listed);
    }
    render(&ListPage { page, deployments }, StatusCode::OK)
}

/// One cell of a row.
#[derive(Debug, Clone)]
pub struct CellView {
    /// Its title.
    pub title: String,
    /// Its chart, SVG or HTML.
    pub html: String,
    /// Its legend: colour slot and label.
    pub legend: Vec<(usize, String)>,
    /// `full`, `left`, `center` or `right`, as a CSS class.
    pub place: &'static str,
}

/// A row of the page: its cells, and how many it is made of (1, 2 or 3).
#[derive(Debug, Clone)]
pub struct RowView {
    /// How many cells the row has.
    pub cells_in_row: u8,
    /// Its cells, left to right.
    pub cells: Vec<CellView>,
}

/// A tab: its label, its address, and whether it is the one shown.
type Tab = (String, String, bool);

#[derive(Template)]
#[template(path = "extension.html")]
struct ExtensionPage {
    page: Page,
    deployment: String,
    name: String,
    error: Option<String>,
    /// The deployment's extensions, this one open (DN21).
    tabs: Vec<Tab>,
    /// "All workers", then each worker heard from in the last day.
    worker_tabs: Vec<Tab>,
    /// The worker shown; empty for all of them.
    worker: String,
    ranges: Vec<Tab>,
    rows: Vec<RowView>,
    folded: Vec<String>,
    workers: Vec<(String, String, String)>,
    /// Its version heard of last; empty when known only by its events.
    version: String,
    /// With no panels to list them in: its events and attempts (Codex's second review of
    /// M5.2i5: a plugin never run showed none).
    events: String,
}

/// Which range, and whose figures.
#[derive(Debug, Default, serde::Deserialize)]
pub struct ExtensionQuery {
    /// `1h`, `24h` (the default), `7d` or `30d`.
    pub range: Option<String>,
    /// One worker's figures (DN21): 1h or 24h only, its own kept a day.
    pub worker: Option<String>,
}

/// How long a worker's own figures are kept, and so who has a tab: those heard from in it.
const WORKER_KEPT: jiff::SignedDuration = jiff::SignedDuration::from_hours(24);

/// `GET /d/<deployment>/extensions/<name>?range=&worker=`: its panels over a range, of all its
/// workers or one (DN21), in a tab beside the deployment's other extensions.
pub async fn page<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path((deployment, name)): Path<(String, String)>,
    headers: HeaderMap,
    query: Result<Query<ExtensionQuery>, QueryRejection>,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let Ok(Query(query)) = query else {
        return (StatusCode::BAD_REQUEST, "the page's address doesn't read").into_response();
    };
    let Some(asked) = Range::parse(query.range.as_deref().unwrap_or("24h")) else {
        return (
            StatusCode::BAD_REQUEST,
            "`range` is one of 1h, 24h, 7d and 30d",
        )
            .into_response();
    };
    let worker = query.worker.filter(|w| !w.is_empty());
    let range = shown_range(asked, worker.is_some());
    let Some(found) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return (
            StatusCode::NOT_FOUND,
            format!("no deployment `{deployment}`"),
        )
            .into_response();
    };
    let href = format!(
        "{}/d/{}/extensions/{}",
        page.base,
        path_segment(&deployment),
        path_segment(&name)
    );
    let ranges = range_tabs(&href, range, worker.as_deref());
    let zone = page.zone.clone().unwrap_or(TimeZone::UTC);
    let read = tokio::join!(
        found.client.extensions(),
        found.client.extension(&name, worker.as_deref()),
        found
            .client
            .extension_metrics(&name, range.name(), worker.as_deref())
    );
    let tabs = read.0.as_ref().map_or_else(
        |_| vec![(name.clone(), href.clone(), true)],
        |list| {
            let base = format!("{}/d/{}/extensions", page.base, path_segment(&deployment));
            extension_tabs(list, &base, &name, asked)
        },
    );
    let (extension, metrics) = match (read.1, read.2) {
        (Ok(extension), Ok(metrics)) => (extension, metrics),
        (Err(error), _) | (_, Err(error)) => {
            let shown = ExtensionPage {
                page,
                deployment,
                name,
                error: Some(error.to_string()),
                tabs,
                worker_tabs: Vec::new(),
                worker: worker.unwrap_or_default(),
                ranges,
                rows: Vec::new(),
                folded: Vec::new(),
                workers: Vec::new(),
                version: String::new(),
                events: String::new(),
            };
            return render(&shown, StatusCode::OK);
        }
    };
    let worker_tabs = worker_tabs(&extension, &href, asked, worker.as_deref());
    let runs_href = format!("{}/d/{}/runs", page.base, path_segment(&deployment));
    let rows = rows(&extension, &metrics, &zone, &runs_href);
    let events =
        if rows.is_empty() && !(extension.events.is_empty() && extension.attempts.is_empty()) {
            events(&extension, &runs_href, &zone)
        } else {
            String::new()
        };
    let shown = ExtensionPage {
        page,
        deployment,
        name,
        error: None,
        tabs,
        worker_tabs,
        worker: worker.unwrap_or_default(),
        ranges,
        rows,
        folded: metrics
            .metrics
            .iter()
            .filter(|m| m.folded)
            .map(|m| m.name.clone())
            .collect(),
        workers: workers_table(&extension, &zone),
        version: extension.version.clone(),
        events,
    };
    render(&shown, StatusCode::OK)
}

/// Each version on each worker: the worker, the version, when it was last heard of.
fn workers_table(extension: &ExtensionView, zone: &TimeZone) -> Vec<(String, String, String)> {
    extension
        .workers
        .iter()
        .map(|w| {
            (
                w.worker_id.clone(),
                w.version.clone(),
                w.seen_at
                    .to_zoned(zone.clone())
                    .strftime("%Y-%m-%d %H:%M")
                    .to_string(),
            )
        })
        .collect()
}

/// The range shown for `asked`: a worker's own figures go back a day, so a longer one shows its
/// last day.
const fn shown_range(asked: Range, of_a_worker: bool) -> Range {
    match asked {
        Range::Week | Range::Month if of_a_worker => Range::Day,
        _ => asked,
    }
}

/// The range links at `href`, of `worker` when one is shown (a day at most: DN21); `range` open.
fn range_tabs(href: &str, range: Range, worker: Option<&str>) -> Vec<Tab> {
    let of_worker = worker.map_or_else(String::new, |w| format!("&worker={}", path_segment(w)));
    Range::ALL
        .iter()
        .filter(|r| worker.is_none() || matches!(r, Range::Hour | Range::Day))
        .map(|r| {
            (
                r.name().to_owned(),
                format!("{href}?range={}{of_worker}", r.name()),
                *r == range,
            )
        })
        .collect()
}

/// A tab for each extension of `list`, under `base`, with the range `asked`; `name` open.
fn extension_tabs(
    list: &schedr_api::extensions::ExtensionList,
    base: &str,
    name: &str,
    asked: Range,
) -> Vec<Tab> {
    list.extensions
        .iter()
        .map(|e| {
            (
                e.name.clone(),
                format!("{base}/{}?range={}", path_segment(&e.name), asked.name()),
                e.name == name,
            )
        })
        .collect()
}

/// The sub-tabs (DN21): "All workers", then each worker heard from in the last day, sorted, at
/// `href` with the range `asked` (a worker's shows its last day of a longer one); `worker` open.
#[must_use]
pub fn worker_tabs(
    extension: &ExtensionView,
    href: &str,
    asked: Range,
    worker: Option<&str>,
) -> Vec<Tab> {
    let since = jiff::Timestamp::now() - WORKER_KEPT;
    let recent: std::collections::BTreeSet<&str> = extension
        .workers
        .iter()
        .filter(|w| w.seen_at >= since)
        .map(|w| w.worker_id.as_str())
        .chain(worker)
        .collect();
    let mut tabs = vec![(
        "All workers".to_owned(),
        format!("{href}?range={}", asked.name()),
        worker.is_none(),
    )];
    tabs.extend(recent.into_iter().map(|w| {
        (
            w.to_owned(),
            format!("{href}?range={}&worker={}", asked.name(), path_segment(w)),
            worker == Some(w),
        )
    }));
    tabs
}

/// A chart's drawing width in a row of `cells_in_row`: about its cell's shape at its fixed
/// height, so it fills the cell whole and its text keeps one size across the page (the user,
/// 2026-11-21: "charts shouldn't have scroll, they should fit in their block as they are").
const fn cell_width(cells_in_row: u8) -> f64 {
    match cells_in_row {
        1 => 1260.0,
        2 => 620.0,
        _ => 400.0,
    }
}

/// The page's rows: its panels by row, each drawn in its cell.
#[must_use]
pub fn rows(
    extension: &ExtensionView,
    metrics: &ExtensionMetrics,
    zone: &TimeZone,
    runs_href: &str,
) -> Vec<RowView> {
    let panels: Vec<Panel> = extension
        .panels
        .iter()
        .filter_map(|p| serde_json::from_value(p.clone()).ok())
        .collect();
    let units: BTreeMap<String, MetricUnit> = extension
        .metrics
        .iter()
        .filter_map(|m| {
            Some((
                m.get("name")?.as_str()?.to_owned(),
                serde_json::from_value(m.get("unit").cloned().unwrap_or_default())
                    .unwrap_or_default(),
            ))
        })
        .collect();
    let mut by_row: BTreeMap<u32, Vec<&Panel>> = BTreeMap::new();
    for panel in &panels {
        by_row.entry(panel.row).or_default().push(panel);
    }
    by_row
        .into_values()
        .map(|mut panels| {
            panels.sort_by_key(|p| p.cell);
            let cells: Vec<Cell> = panels.iter().map(|p| p.cell).collect();
            let cells_in_row = row_cells(&cells).unwrap_or(1);
            let width = cell_width(cells_in_row);
            RowView {
                cells_in_row,
                cells: panels
                    .into_iter()
                    .map(|panel| {
                        let metric = panel
                            .metric
                            .as_deref()
                            .and_then(|m| metrics.metrics.iter().find(|v| v.name == m));
                        let unit = panel
                            .metric
                            .as_deref()
                            .and_then(|m| units.get(m))
                            .copied()
                            .unwrap_or_default();
                        let (html, legend) = drawn(
                            panel,
                            metric,
                            unit,
                            metrics,
                            (width, zone, runs_href),
                            extension,
                        );
                        CellView {
                            title: panel.title.clone(),
                            html,
                            legend,
                            place: match panel.cell {
                                Cell::Full => "full",
                                Cell::Left => "left",
                                Cell::Center => "center",
                                Cell::Right => "right",
                            },
                        }
                    })
                    .collect(),
            }
        })
        .collect()
}

/// A series' name: its labels' values, or the metric's when it has none.
fn series_label(series: &ExtensionSeriesView, metric: &str) -> String {
    if series.labels.is_empty() {
        metric.to_owned()
    } else {
        series
            .labels
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .join(" · ")
    }
}

/// A series' colour slot: the panel's for a label value it names, its own colour for the
/// first, else schedr's in order.
fn slot(panel: &Panel, series: &ExtensionSeriesView, index: usize) -> usize {
    if let Some(color) = series.labels.values().find_map(|v| panel.colors.get(v)) {
        return color.slot();
    }
    match (index, panel.color) {
        (0, Some(color)) => color.slot(),
        _ => index % chart::SERIES + 1,
    }
}

/// The chart's unit and the factor its values are scaled by: seconds drawn as milliseconds.
fn chart_unit(unit: MetricUnit) -> (Unit, f64) {
    match unit {
        MetricUnit::Seconds => (Unit::Millis, 1000.0),
        MetricUnit::Bytes => (Unit::Bytes, 1.0),
        MetricUnit::Count | MetricUnit::Ratio => (Unit::Number, 1.0),
    }
}

fn secs(at: jiff::Timestamp) -> i64 {
    at.as_second()
}

/// The `q` quantile of a histogram's `counts` over `bounds` (the last bucket past the last
/// bound): linear within its bucket; none without observations.
#[must_use]
pub fn quantile(counts: &[u64], bounds: &[f64], q: f64) -> Option<f64> {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }
    #[expect(clippy::cast_precision_loss, reason = "a rank among observations")]
    let rank = q.clamp(0.0, 1.0) * total as f64;
    let mut seen = 0_u64;
    for (i, count) in counts.iter().enumerate() {
        let before = seen;
        seen += count;
        #[expect(clippy::cast_precision_loss, reason = "a rank among observations")]
        if (seen as f64) >= rank && *count > 0 {
            let low = match i.checked_sub(1) {
                None => 0.0,
                // More counts than bounds: none read past them (Codex's review).
                Some(below) => *bounds.get(below)?,
            };
            let Some(&high) = bounds.get(i) else {
                // Past the last bound: its bound is all we know.
                return bounds.last().copied();
            };
            #[expect(clippy::cast_precision_loss, reason = "a rank among observations")]
            let within = (rank - before as f64) / *count as f64;
            return Some(low + (high - low) * within.clamp(0.0, 1.0));
        }
    }
    bounds.last().copied()
}

/// What every chart kind of a panel draws from.
struct Drawing<'a> {
    panel: &'a Panel,
    metric: &'a ExtensionMetricView,
    metrics: &'a ExtensionMetrics,
    width: f64,
    zone: &'a TimeZone,
    unit: Unit,
    scale: f64,
}

/// A chart and its legend.
type Drawn = (String, Vec<(usize, String)>);

impl Drawing<'_> {
    fn spec(&self, lines: Vec<Line>, stacked: Vec<Line>, unit: Unit) -> Spec {
        Spec {
            title: self.panel.title.clone(),
            unit,
            width: self.width,
            from: secs(self.metrics.from),
            to: secs(self.metrics.to),
            step: i64::from(self.metrics.bucket_s),
            lines,
            stacked,
            bars: false,
            band: None,
            limits: Vec::new(),
            zone: self.zone.clone(),
        }
    }

    fn lines(&self, value: &dyn Fn(&ExtensionPointView) -> Option<f64>) -> Vec<Line> {
        self.metric
            .series
            .iter()
            .take(chart::SERIES)
            .enumerate()
            .map(|(i, s)| Line {
                label: series_label(s, &self.metric.name),
                slot: slot(self.panel, s, i),
                points: s
                    .points
                    .iter()
                    .filter_map(|p| Some((secs(p.at), value(p)?)))
                    .collect(),
            })
            .collect()
    }

    fn charted(&self, lines: Vec<Line>, stacked: bool, unit: Unit) -> Drawn {
        let legend = lines.iter().map(|l| (l.slot, l.label.clone())).collect();
        let spec = if stacked {
            self.spec(Vec::new(), lines, unit)
        } else {
            self.spec(lines, Vec::new(), unit)
        };
        (chart::draw(&spec), legend)
    }

    /// A bar per bucket, the series stacked: what each bucket counted (a counter's growth, a
    /// histogram's observations) or a gauge's value; an empty bucket left empty (DN24).
    fn bars(&self) -> Drawn {
        // A histogram's bars count observations; a counter's and a gauge's are in its unit (agy's
        // review of M5.2i5: a bytes counter drew raw numbers).
        let counts = self.metric.kind == "histogram";
        let scale = if counts { 1.0 } else { self.scale };
        let lines = self.lines(&|p| {
            #[expect(clippy::cast_precision_loss, reason = "a chart's value")]
            let value = if p.counts.is_empty() {
                p.value
            } else {
                p.counts.iter().sum::<u64>() as f64
            };
            Some(value * scale)
        });
        let unit = if counts { Unit::Number } else { self.unit };
        let legend = lines.iter().map(|l| (l.slot, l.label.clone())).collect();
        let mut spec = self.spec(Vec::new(), lines, unit);
        spec.bars = true;
        (chart::draw(&spec), legend)
    }

    /// A counter's growth, or a histogram's observations, per second.
    fn rate(&self) -> Drawn {
        let seconds = f64::from(self.metrics.bucket_s.max(1));
        let lines = self.lines(&|p| {
            #[expect(clippy::cast_precision_loss, reason = "a chart's value")]
            let grown = if p.counts.is_empty() {
                p.value
            } else {
                p.counts.iter().sum::<u64>() as f64
            };
            Some(grown / seconds)
        });
        self.charted(lines, false, Unit::Number)
    }

    /// Each quantile over every series together, per bucket.
    fn quantiles(&self) -> Drawn {
        let quantiles = if self.panel.quantiles.is_empty() {
            vec![0.5, 0.95, 0.99]
        } else {
            self.panel.quantiles.clone()
        };
        let mut by_at: BTreeMap<i64, Vec<u64>> = BTreeMap::new();
        for point in self.metric.series.iter().flat_map(|s| &s.points) {
            let counts = by_at.entry(secs(point.at)).or_default();
            if counts.len() < point.counts.len() {
                counts.resize(point.counts.len(), 0);
            }
            for (total, c) in counts.iter_mut().zip(&point.counts) {
                *total += c;
            }
        }
        let lines = quantiles
            .iter()
            .take(chart::SERIES)
            .enumerate()
            .map(|(i, q)| Line {
                label: format!("p{}", chart::number(q * 100.0)),
                slot: match (i, self.panel.color) {
                    (0, Some(color)) => color.slot(),
                    _ => i % chart::SERIES + 1,
                },
                points: by_at
                    .iter()
                    .filter_map(|(at, counts)| {
                        Some((
                            *at,
                            quantile(counts, &self.metric.buckets, *q)? * self.scale,
                        ))
                    })
                    .collect(),
            })
            .collect();
        self.charted(lines, false, self.unit)
    }

    /// The range's observations per bucket, every series together.
    fn histogram(&self) -> Drawn {
        let buckets = &self.metric.buckets;
        let mut totals = vec![0_u64; buckets.len() + 1];
        for point in self.metric.series.iter().flat_map(|s| &s.points) {
            for (total, c) in totals.iter_mut().zip(&point.counts) {
                *total += c;
            }
        }
        let shown = |bound: f64| self.unit.show(bound * self.scale);
        let last = format!("> {}", shown(buckets.last().copied().unwrap_or(0.0)));
        let class = self
            .panel
            .color
            .map_or("s1", |c| SLOT_CLASSES[c.slot() - 1]);
        let bars = totals
            .iter()
            .enumerate()
            .map(|(i, count)| {
                let label = buckets
                    .get(i)
                    .map_or_else(|| last.clone(), |b| format!("≤ {}", shown(*b)));
                #[expect(clippy::cast_precision_loss, reason = "a chart's value")]
                let value = *count as f64;
                Bar {
                    value: Some(value),
                    class,
                    marked: false,
                    href: String::new(),
                    title: format!("{label}: {count}"),
                }
            })
            .collect();
        let first = buckets
            .first()
            .map_or_else(String::new, |b| format!("≤ {}", shown(*b)));
        let svg = chart::draw_bars(&Bars {
            title: self.panel.title.clone(),
            unit: Unit::Count,
            width: self.width,
            bars,
            limit: None,
            ends: (first, last),
        });
        (svg, Vec::new())
    }

    /// A gauge's latest, or a counter's growth over the range, every series together.
    fn number(&self) -> Drawn {
        let gauge = self.metric.kind == "gauge";
        let total: f64 = self
            .metric
            .series
            .iter()
            .map(|s| {
                if gauge {
                    s.points.last().map_or(0.0, |p| p.value)
                } else {
                    s.points.iter().map(|p| p.value).sum()
                }
            })
            .sum();
        let shown = html_escape(&self.unit.show(total * self.scale));
        (format!("<p class=\"big-number\">{shown}</p>"), Vec::new())
    }

    /// Each series' latest value.
    fn table(&self) -> Drawn {
        let mut html = String::from("<table class=\"list\"><tbody>");
        for series in &self.metric.series {
            let value = series.points.last().map_or(0.0, |p| p.value);
            let _ = write!(
                html,
                "<tr><td>{}</td><td class=\"mono\">{}</td></tr>",
                html_escape(&series_label(series, &self.metric.name)),
                html_escape(&self.unit.show(value * self.scale))
            );
        }
        html.push_str("</tbody></table>");
        (html, Vec::new())
    }
}

/// A panel's chart and legend.
fn drawn(
    panel: &Panel,
    metric: Option<&ExtensionMetricView>,
    unit: MetricUnit,
    metrics: &ExtensionMetrics,
    (width, zone, runs_href): (f64, &TimeZone, &str),
    extension: &ExtensionView,
) -> Drawn {
    if panel.chart == ChartKind::Events {
        return (events(extension, runs_href, zone), Vec::new());
    }
    let Some(metric) = metric else {
        return (
            "<p class=\"empty\">No samples yet.</p>".to_owned(),
            Vec::new(),
        );
    };
    if metric.series.iter().all(|s| s.points.is_empty()) {
        return (
            "<p class=\"empty\">No samples in this range yet.</p>".to_owned(),
            Vec::new(),
        );
    }
    let (unit, scale) = chart_unit(unit);
    let drawing = Drawing {
        panel,
        metric,
        metrics,
        width,
        zone,
        unit,
        scale,
    };
    match panel.chart {
        ChartKind::Line => drawing.charted(drawing.lines(&|p| Some(p.value * scale)), false, unit),
        ChartKind::Stacked => {
            drawing.charted(drawing.lines(&|p| Some(p.value * scale)), true, unit)
        }
        ChartKind::Rate => drawing.rate(),
        ChartKind::Bars => drawing.bars(),
        ChartKind::Quantiles => drawing.quantiles(),
        ChartKind::Histogram => drawing.histogram(),
        ChartKind::Number => drawing.number(),
        ChartKind::Table => drawing.table(),
        ChartKind::Events => (events(extension, runs_href, zone), Vec::new()),
    }
}

/// The slots' CSS classes, for bars.
const SLOT_CLASSES: [&str; 20] = [
    "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14", "s15",
    "s16", "s17", "s18", "s19", "s20",
];

/// An events panel (DN19): what happened to the extension on its workers and its attempts'
/// latest phases, newest first, each attempt linking to its task's panel (`runs_href` the
/// deployment's runs).
fn events(extension: &ExtensionView, runs_href: &str, zone: &TimeZone) -> String {
    enum Row<'a> {
        Event(&'a schedr_api::extensions::ExtensionEventView),
        Attempt(&'a schedr_api::extensions::ExtensionAttemptView),
    }
    let mut rows: Vec<(jiff::Timestamp, Row<'_>)> = extension
        .events
        .iter()
        .map(|e| (e.at, Row::Event(e)))
        .chain(extension.attempts.iter().map(|a| (a.at, Row::Attempt(a))))
        .collect();
    if rows.is_empty() {
        return "<p class=\"empty\">Nothing has happened to it yet.</p>".to_owned();
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.0));
    let mut html = String::from(
        "<table class=\"list ext-events\"><thead><tr><th>When</th><th>What</th><th>Where</th>\
         <th>About</th></tr></thead><tbody>",
    );
    for (at, row) in rows
        .iter()
        .take(usize::try_from(schedr_api::extensions::LISTED).unwrap_or(50))
    {
        let when = at
            .to_zoned(zone.clone())
            .strftime("%Y-%m-%d %H:%M:%S")
            .to_string();
        match row {
            Row::Event(event) => {
                let place = if event.version.is_empty() {
                    event.worker_id.clone()
                } else {
                    format!("{} · {}", event.worker_id, event.version)
                };
                let _ = write!(
                    html,
                    "<tr><td class=\"time\">{when}</td><td><span class=\"badge ev-{kind}\">{kind}</span></td>\
                     <td class=\"mono\">{}</td><td>{}</td></tr>",
                    html_escape(&place),
                    html_escape(&event.message),
                    kind = html_escape(&event.kind),
                );
            }
            Row::Attempt(attempt) => {
                let mut href = format!(
                    "{runs_href}/{}?task={}&tab=attempts&try={}",
                    schedr_client::path_segment(&attempt.run_id),
                    schedr_client::path_segment(&attempt.task_id),
                    attempt.try_number
                );
                if let Some(index) = attempt.map_index {
                    let _ = write!(href, "&map_index={index}");
                }
                let mut about = attempt.message.clone().unwrap_or_default();
                if let Some(progress) = attempt.progress {
                    let percent = (progress * 100.0).round();
                    about = if about.is_empty() {
                        format!("{percent} %")
                    } else {
                        format!("{about} ({percent} %)")
                    };
                }
                if let Some(ended) = &attempt.ended {
                    about = if about.is_empty() {
                        format!("ended {ended}")
                    } else {
                        format!("{about}; ended {ended}")
                    };
                }
                let _ = write!(
                    html,
                    "<tr><td class=\"time\">{when}</td><td><span class=\"badge phase\">{}</span></td>\
                     <td><a href=\"{}\">{} · {} · try {}</a></td><td>{}</td></tr>",
                    html_escape(&attempt.phase),
                    html_escape(&href),
                    html_escape(&attempt.dag_id),
                    html_escape(&attempt.task_id),
                    attempt.try_number,
                    html_escape(&about),
                );
            }
        }
    }
    html.push_str("</tbody></table>");
    // Limited, even scrolling (the user, 2026-11-21: "events, even with a scroll have to have
    // limit"): the newest only, and said so.
    let shown = usize::try_from(schedr_api::extensions::LISTED).unwrap_or(50);
    if rows.len() > shown {
        let _ = write!(
            html,
            "<p class=\"dim\">The newest {shown} shown; older ones are left out.</p>"
        );
    }
    html
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
