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
use schedr_api::extensions::panels::{per_second, point_of};
use schedr_api::extensions::{
    ExtensionList, ExtensionMetricView, ExtensionMetrics, ExtensionPointView, ExtensionSeriesView,
    ExtensionSummaryView, ExtensionView, version_order,
};
use schedr_api::series::Range;
use schedr_client::{Transport, path_segment};
use schedr_model::extension::{
    Cell, ChartKind, Combine, MetricKind, MetricUnit, Panel, Point, row_cells,
};

use crate::Ui;
use crate::chart::{self, Bar, Bars, Line, Spec, Unit};
use crate::page::Page;
use crate::pages::render;

#[cfg(test)]
mod tests;

/// The extensions page with none to show: why (each deployment that didn't answer).
#[derive(Template)]
#[template(path = "extensions.html")]
struct NonePage {
    page: Page,
    unavailable: Vec<String>,
}

/// Every deployment's extensions, asked at once, in deployment order: each one's list, or why
/// it can't be read.
async fn every_deployments<T: Transport + Send + Sync + 'static>(
    ui: &Arc<Ui<T>>,
) -> Vec<(String, Result<ExtensionList, String>)> {
    let mut asked = tokio::task::JoinSet::new();
    for index in 0..ui.deployments.len() {
        let ui = Arc::clone(ui);
        asked.spawn(schedr_client::for_same_user(async move {
            let answer = ui.deployments[index].client.extensions().await;
            (index, answer.map_err(|e| e.to_string()))
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    answers
        .into_iter()
        .map(|(index, answer)| (ui.deployments[index].name.clone(), answer))
        .collect()
}

/// Every extension's name any deployment ran, sorted, each once.
fn every_name(lists: &[(String, Result<ExtensionList, String>)]) -> Vec<String> {
    lists
        .iter()
        .filter_map(|(_, list)| list.as_ref().ok())
        .flat_map(|list| list.extensions.iter().map(|e| e.name.clone()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// `GET /extensions`: the first extension's page (DN40: the page is its tabs), or that none
/// has run.
pub async fn list<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let lists = every_deployments(&ui).await;
    if let Some(first) = every_name(&lists).first() {
        let href = format!("{}/extensions/{}", page.base, path_segment(first));
        return axum::response::Redirect::to(&href).into_response();
    }
    let unavailable = lists
        .into_iter()
        .filter_map(|(name, list)| list.err().map(|why| format!("{name}: {why}")))
        .collect();
    render(&NonePage { page, unavailable }, StatusCode::OK)
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
    name: String,
    /// The deployment shown.
    deployment: String,
    error: Option<String>,
    /// Every extension any deployment ran, this one open (DN40).
    tabs: Vec<Tab>,
    /// Its versions, the highest first, the one shown chosen (DN40: always one).
    versions: Vec<Tab>,
    /// The deployments running that version; none shown with only one.
    deployment_tabs: Vec<Tab>,
    /// "All workers", then each worker of that version heard from in the last day.
    worker_tabs: Vec<Tab>,
    /// The worker shown; empty for all of them.
    worker: String,
    ranges: Vec<Tab>,
    rows: Vec<RowView>,
    folded: Vec<String>,
    workers: Vec<(String, String, String)>,
    /// The version shown; empty when known only by its events.
    version: String,
    /// Where it runs: each deployment's versions, and its page (DN40).
    runs_here: Vec<(String, String)>,
    /// The deployments that didn't answer, with why.
    unavailable: Vec<String>,
    /// With no panels to list them in: its events and attempts (Codex's second review of
    /// M5.2i5: a plugin never run showed none).
    events: String,
}

/// Which version, deployment, worker and range (DN40).
#[derive(Debug, Default, serde::Deserialize)]
pub struct ExtensionQuery {
    /// `1h`, `24h` (the default), `7d` or `30d`.
    pub range: Option<String>,
    /// One worker's figures (DN21): 1h or 24h only, its own kept a day.
    pub worker: Option<String>,
    /// The version shown; the highest when absent or unknown.
    pub version: Option<String>,
    /// The deployment shown, among those running the version; the first of them when absent.
    pub deployment: Option<String>,
}

/// How long a worker's own figures are kept, and so who has a tab: those heard from in it.
const WORKER_KEPT: jiff::SignedDuration = jiff::SignedDuration::from_hours(24);

/// Where a page is: what its links keep and change (DN40).
#[derive(Debug, Clone, Copy)]
struct Here<'a> {
    base: &'a str,
    name: &'a str,
    version: &'a str,
    deployment: &'a str,
    worker: Option<&'a str>,
    range: Range,
}

impl Here<'_> {
    /// This page's address.
    fn href(&self) -> String {
        let mut url = format!(
            "{}/extensions/{}?range={}",
            self.base,
            path_segment(self.name),
            self.range.name()
        );
        for (key, value) in [("version", self.version), ("deployment", self.deployment)] {
            if !value.is_empty() {
                let _ = write!(url, "&{key}={}", path_segment(value));
            }
        }
        if let Some(worker) = self.worker {
            let _ = write!(url, "&worker={}", path_segment(worker));
        }
        url
    }
}

/// `GET /extensions/<name>?version=&deployment=&worker=&range=`: one version of an extension
/// on one deployment, its panels over a range, of all its workers or one (DN21, DN40); its tabs
/// every extension any deployment ran.
pub async fn page<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    Path(name): Path<String>,
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
    let lists = every_deployments(&ui).await;
    let names = every_name(&lists);
    let known = names.contains(&name);
    let chosen = Chosen::of(&lists, &name, &query);
    let worker = query.worker.filter(|w| !w.is_empty());
    let range = shown_range(asked, worker.is_some());
    let base = page.base.clone();
    let here = Here {
        base: &base,
        name: &name,
        version: &chosen.version,
        deployment: &chosen.deployment,
        worker: worker.as_deref(),
        range,
    };
    let (tabs, versions, deployment_tabs) = choices(&names, &chosen, here, asked);
    let ranges = range_tabs(here);
    let unavailable = lists
        .iter()
        .filter_map(|(d, list)| list.as_ref().err().map(|why| format!("{d}: {why}")))
        .collect();
    let mut shown = ExtensionPage {
        page,
        name: name.clone(),
        deployment: chosen.deployment.clone(),
        error: None,
        tabs,
        versions,
        deployment_tabs,
        worker_tabs: Vec::new(),
        worker: worker.clone().unwrap_or_default(),
        ranges,
        rows: Vec::new(),
        folded: Vec::new(),
        workers: Vec::new(),
        version: chosen.version.clone(),
        runs_here: runs_here(&chosen, here, asked),
        unavailable,
        events: String::new(),
    };
    // Not one any answering deployment ran: the page, its tabs and banners, said so (agy's
    // review: a bare 404 hid a deployment that didn't answer).
    if !known {
        shown.error = Some(format!(
            "no deployment that answered has run extension `{name}`"
        ));
        return render(&shown, StatusCode::NOT_FOUND);
    }
    let Some(found) = ui.deployments.iter().find(|d| d.name == chosen.deployment) else {
        return render(&shown, StatusCode::OK);
    };
    let version = (!chosen.version.is_empty()).then_some(chosen.version.as_str());
    let read = tokio::join!(
        found.client.extension(&name, worker.as_deref(), version),
        found
            .client
            .extension_metrics(&name, range.name(), worker.as_deref(), version)
    );
    let (extension, metrics) = match read {
        (Ok(extension), Ok(metrics)) => (extension, metrics),
        (Err(error), _) | (_, Err(error)) => {
            shown.error = Some(error.to_string());
            return render(&shown, StatusCode::OK);
        }
    };
    let zone = shown.page.zone.clone().unwrap_or(TimeZone::UTC);
    let runs_href = format!("{base}/d/{}/runs", path_segment(&chosen.deployment));
    shown.worker_tabs = worker_tabs(&extension, here, asked);
    shown.rows = rows(&extension, &metrics, &zone, &runs_href);
    if shown.rows.is_empty() && !(extension.events.is_empty() && extension.attempts.is_empty()) {
        shown.events = events(&extension, &runs_href, &zone);
    }
    shown.folded = metrics
        .metrics
        .iter()
        .filter(|m| m.folded)
        .map(|m| m.name.clone())
        .collect();
    shown.workers = workers_table(&extension, &zone);
    render(&shown, StatusCode::OK)
}

/// The page's extension tabs (every name, each at the range `asked`), its versions, and its
/// deployments' tabs (none with one), each keeping what else `here` shows (DN40).
fn choices(
    names: &[String],
    chosen: &Chosen,
    here: Here<'_>,
    asked: Range,
) -> (Vec<Tab>, Vec<Tab>, Vec<Tab>) {
    let tabs = names
        .iter()
        .map(|n| {
            let href = format!(
                "{}/extensions/{}?range={}",
                here.base,
                path_segment(n),
                asked.name()
            );
            (n.clone(), href, n == here.name)
        })
        .collect();
    // A link keeps the range, and the deployment when it runs that version too (agy's review).
    let runs = |d: &str, v: &str| {
        chosen
            .deployments_versions
            .iter()
            .any(|(name, versions)| name == d && versions.iter().any(|x| x == v))
    };
    let versions = chosen
        .versions
        .iter()
        .map(|v| {
            let deployment = if runs(here.deployment, v) {
                here.deployment
            } else {
                ""
            };
            let href = Here {
                version: v,
                deployment,
                worker: None,
                range: asked,
                ..here
            }
            .href();
            (v.clone(), href, v == &chosen.version)
        })
        .collect();
    let deployments = if chosen.running.len() > 1 {
        chosen
            .running
            .iter()
            .map(|d| {
                let href = Here {
                    deployment: d,
                    worker: None,
                    range: asked,
                    ..here
                }
                .href();
                (d.clone(), href, d == &chosen.deployment)
            })
            .collect()
    } else {
        Vec::new()
    };
    (tabs, versions, deployments)
}

/// "Where it runs": each deployment's versions, a link to its page (its own highest version, or
/// its events for one that knows it only by them).
fn runs_here(chosen: &Chosen, here: Here<'_>, asked: Range) -> Vec<(String, String)> {
    chosen
        .runs_here
        .iter()
        .map(|(said, d)| {
            let href = Here {
                version: "",
                deployment: d,
                worker: None,
                range: asked,
                ..here
            }
            .href();
            (said.clone(), href)
        })
        .collect()
}

/// The version and deployment a page shows of an extension (DN40), and what its tabs offer.
#[derive(Debug, Default, PartialEq, Eq)]
struct Chosen {
    /// Its versions any deployment ran, the highest first.
    versions: Vec<String>,
    /// The version shown: the one asked for if a deployment ran it, else the highest; empty
    /// when it is known only by its events.
    version: String,
    /// The deployments running that version, in deployment order.
    running: Vec<String>,
    /// The deployment shown: the one asked for among them, else the first.
    deployment: String,
    /// Each deployment that ran it: its versions said, and the deployment (a link to it).
    runs_here: Vec<(String, String)>,
    /// Each deployment that ran it and its versions, the highest first.
    deployments_versions: Vec<(String, Vec<String>)>,
}

impl Chosen {
    fn of(
        lists: &[(String, Result<ExtensionList, String>)],
        name: &str,
        query: &ExtensionQuery,
    ) -> Self {
        let of_name: Vec<(&String, &ExtensionSummaryView)> = lists
            .iter()
            .filter_map(|(d, list)| {
                let list = list.as_ref().ok()?;
                Some((d, list.extensions.iter().find(|e| e.name == name)?))
            })
            .collect();
        let mut versions: Vec<String> = of_name
            .iter()
            .flat_map(|(_, e)| e.versions.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        versions.sort_by(|a, b| version_order(b, a));
        let of_deployment = |d: &str| of_name.iter().find(|(name, _)| *name == d).map(|(_, e)| e);
        // A deployment asked for without a version: its own highest, none for one that knows it
        // only by its events (agy's review of DN40: such a one was out of reach).
        let asked = query.deployment.as_deref().and_then(of_deployment);
        let version = match (&query.version, asked) {
            (Some(v), _) if versions.contains(v) => v.clone(),
            (None, Some(e)) => e.versions.first().cloned().unwrap_or_default(),
            _ => versions.first().cloned().unwrap_or_default(),
        };
        let running: Vec<String> = of_name
            .iter()
            .filter(|(_, e)| {
                if version.is_empty() {
                    e.versions.is_empty()
                } else {
                    e.versions.contains(&version)
                }
            })
            .map(|(d, _)| (*d).clone())
            .collect();
        let deployment = query
            .deployment
            .clone()
            .filter(|d| running.contains(d))
            .or_else(|| running.first().cloned())
            .unwrap_or_default();
        let runs_here = of_name
            .iter()
            .map(|(d, e)| {
                let said = if e.versions.is_empty() {
                    format!("{d}: only its events")
                } else {
                    format!("{d}: {}", e.versions.join(", "))
                };
                (said, (*d).clone())
            })
            .collect();
        let deployments_versions = of_name
            .iter()
            .map(|(d, e)| ((*d).clone(), e.versions.clone()))
            .collect();
        Self {
            versions,
            version,
            running,
            deployment,
            runs_here,
            deployments_versions,
        }
    }
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

/// The range links of `here`, of its worker when one is shown (a day at most: DN21); its range
/// open.
fn range_tabs(here: Here<'_>) -> Vec<Tab> {
    Range::ALL
        .iter()
        .filter(|r| here.worker.is_none() || matches!(r, Range::Hour | Range::Day))
        .map(|r| {
            let href = Here { range: *r, ..here }.href();
            (r.name().to_owned(), href, *r == here.range)
        })
        .collect()
}

/// The sub-tabs (DN21): "All workers", then each worker of the version shown heard from in the
/// last day, sorted, each at the range `asked` (a worker's shows its last day of a longer one);
/// the worker of `here` open.
#[must_use]
fn worker_tabs(extension: &ExtensionView, here: Here<'_>, asked: Range) -> Vec<Tab> {
    let since = jiff::Timestamp::now() - WORKER_KEPT;
    let recent: std::collections::BTreeSet<&str> = extension
        .workers
        .iter()
        .filter(|w| w.seen_at >= since)
        .filter(|w| here.version.is_empty() || w.version == here.version)
        .map(|w| w.worker_id.as_str())
        .chain(here.worker)
        .collect();
    let mut tabs = vec![(
        "All workers".to_owned(),
        Here {
            worker: None,
            range: asked,
            ..here
        }
        .href(),
        here.worker.is_none(),
    )];
    tabs.extend(recent.into_iter().map(|w| {
        let href = Here {
            worker: Some(w),
            range: asked,
            ..here
        }
        .href();
        (w.to_owned(), href, here.worker == Some(w))
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
    // Numbered before one that can't be read is left out, as the API numbers them (agy's
    // review of M5.2i10: the next took its lines).
    let panels: Vec<(usize, Panel)> = extension
        .panels
        .iter()
        .enumerate()
        .filter_map(|(index, p)| Some((index, serde_json::from_value(p.clone()).ok()?)))
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
    let mut by_row: BTreeMap<u32, Vec<(usize, &Panel)>> = BTreeMap::new();
    for (index, panel) in &panels {
        by_row.entry(panel.row).or_default().push((*index, panel));
    }
    by_row
        .into_values()
        .map(|mut panels| {
            panels.sort_by_key(|(_, p)| p.cell);
            let cells: Vec<Cell> = panels.iter().map(|(_, p)| p.cell).collect();
            let cells_in_row = row_cells(&cells).unwrap_or(1);
            let width = cell_width(cells_in_row);
            RowView {
                cells_in_row,
                cells: panels
                    .into_iter()
                    .map(|(index, panel)| {
                        let metric = panel
                            .metric
                            .as_deref()
                            .and_then(|m| metrics.metrics.iter().find(|v| v.name == m));
                        // Its own lines, as its functions say (decision 516).
                        let own = metric.and_then(|metric| panel_lines(metrics, index, metric));
                        let worked_out = own.is_some();
                        let metric = own.as_ref().or(metric);
                        let unit = panel
                            .metric
                            .as_deref()
                            .and_then(|m| units.get(m))
                            .copied()
                            .unwrap_or_default();
                        let (html, legend) = drawn(
                            (panel, worked_out),
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

/// The panel at `index`'s lines as the API worked them out, in its metric's view.
fn panel_lines(
    metrics: &ExtensionMetrics,
    index: usize,
    metric: &ExtensionMetricView,
) -> Option<ExtensionMetricView> {
    let panel = metrics
        .panels
        .iter()
        .find(|p| usize::try_from(p.index).ok() == Some(index) && p.metric == metric.name)?;
    Some(ExtensionMetricView {
        series: panel.series.clone(),
        folded: panel.folded,
        ..metric.clone()
    })
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
    /// Whether its points are the panel's own (decision 516), else its metric's summed.
    worked_out: bool,
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
        // Past the chart's lines, a folded `other` (the API's last) still drawn (agy's review of
        // M5.2i10: a `limit` of 8 hid it).
        let series = &self.metric.series;
        let mut shown: Vec<&ExtensionSeriesView> = series.iter().take(chart::SERIES).collect();
        if series.len() > chart::SERIES
            && let Some(other) = series.last().filter(|s| {
                !s.labels.is_empty()
                    && s.labels
                        .values()
                        .all(|v| v == schedr_model::extension::OTHER)
            })
        {
            shown[chart::SERIES - 1] = other;
        }
        shown
            .into_iter()
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
        let (scale, unit) = (self.scale, self.unit);
        let lines = self.lines(&|p| Some(self.counted(p) * scale));
        let legend = lines.iter().map(|l| (l.slot, l.label.clone())).collect();
        let mut spec = self.spec(Vec::new(), lines, unit);
        spec.bars = true;
        (chart::draw(&spec), legend)
    }

    /// A counter's growth, or a histogram's observations, per second.
    fn rate(&self) -> Drawn {
        let seconds = if self.worked_out {
            // Per second already (decision 516), unless the panel asked for its increase.
            1.0
        } else {
            f64::from(self.metrics.bucket_s.max(1))
        };
        let lines = self.lines(&|p| Some(self.counted(p) / seconds));
        self.charted(lines, false, Unit::Number)
    }

    /// A line's figure over the whole range: its increase or count; per second, over the range's
    /// seconds; a histogram's mean, its observations' (agy's review of M5.2i10: a rate and a
    /// mean were added up over the buckets).
    fn over_range(&self, points: &[ExtensionPointView]) -> f64 {
        let kind = if self.metric.kind == "histogram" {
            MetricKind::Histogram
        } else {
            MetricKind::Counter
        };
        if !self.worked_out {
            return points.iter().map(|p| self.counted(p)).sum();
        }
        if point_of(self.panel, kind) == Point::Mean {
            #[expect(clippy::cast_precision_loss, reason = "a chart's value")]
            let count = points.iter().flat_map(|p| &p.counts).sum::<u64>() as f64;
            let sum: f64 = points.iter().map(|p| p.sum).sum();
            return if count > 0.0 { sum / count } else { 0.0 };
        }
        let total: f64 = points.iter().map(|p| p.value).sum();
        if per_second(self.panel, kind) {
            #[expect(clippy::cast_precision_loss, reason = "a range's seconds")]
            let range = (self.metrics.to.as_second() - self.metrics.from.as_second()).max(1) as f64;
            total * f64::from(self.metrics.bucket_s.max(1)) / range
        } else {
            total
        }
    }

    /// A point's value: the panel's own; summed, a histogram's count of observations.
    fn counted(&self, p: &ExtensionPointView) -> f64 {
        if self.worked_out || p.counts.is_empty() {
            p.value
        } else {
            #[expect(clippy::cast_precision_loss, reason = "a chart's value")]
            let count = p.counts.iter().sum::<u64>() as f64;
            count
        }
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
        let values: Vec<f64> = self
            .metric
            .series
            .iter()
            .map(|s| {
                if gauge {
                    s.points.last().map_or(0.0, |p| p.value)
                } else {
                    self.over_range(&s.points)
                }
            })
            .collect();
        // The lines combined as the panel's points were (decision 516).
        #[expect(clippy::cast_precision_loss, reason = "a few lines")]
        let total = match self.panel.combine.unwrap_or(Combine::Sum) {
            Combine::Sum => values.iter().sum(),
            Combine::Max => values.iter().copied().fold(f64::MIN, f64::max),
            Combine::Min => values.iter().copied().fold(f64::MAX, f64::min),
            Combine::Mean => values.iter().sum::<f64>() / values.len().max(1) as f64,
        };
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
    (panel, worked_out): (&Panel, bool),
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
    // A histogram's count is a number of observations, its mean in its unit (agy's review of
    // M5.2i10: 4 observations showed as 4 s).
    let counting =
        metric.kind == "histogram" && point_of(panel, MetricKind::Histogram) != Point::Mean;
    let (unit, scale) = if counting {
        (Unit::Number, 1.0)
    } else {
        chart_unit(unit)
    };
    let drawing = Drawing {
        panel,
        worked_out,
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
