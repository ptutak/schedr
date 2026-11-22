//! What an extension's plugin reports beyond its result (decision 513): its declared metrics
//! and the panels of its page, the points it reports, its status and links, and why the worker
//! stops it. Declared in its `--describe` or its `[extensions.<name>]` config, config winning;
//! everything else (styles, colours, ranges) is schedr's.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The most metrics one extension declares.
pub const MAX_METRICS: usize = 50;
/// The most panels one extension's page has.
pub const MAX_PANELS: usize = 150;
/// The most labels one metric has.
pub const MAX_LABELS: usize = 8;
/// The most buckets one histogram has.
pub const MAX_BUCKETS: usize = 40;
/// The series one metric keeps by default before new ones are folded into `other`
/// (`[extensions.<name>] max_series`).
pub const DEFAULT_MAX_SERIES: u32 = 100;
/// The label value a series past the cap, or a value not among a label's declared ones, is
/// counted under.
pub const OTHER: &str = "other";
/// The longest status phase, message, link label and URL, in characters.
pub const MAX_TEXT: usize = 512;
/// The most links one attempt keeps; a later one with a new label is refused.
pub const MAX_LINKS: usize = 10;

/// A metric's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    /// Only grows; a point adds its value (at least 0).
    Counter,
    /// Goes up and down; a point sets it.
    Gauge,
    /// A distribution; a point is one observation, counted in its declared buckets.
    Histogram,
}

/// What a metric measures, for its axis and its numbers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetricUnit {
    /// A plain number.
    #[default]
    Count,
    /// Seconds.
    Seconds,
    /// Bytes.
    Bytes,
    /// A fraction, 0 to 1 (shown as a percentage).
    Ratio,
}

/// A label a metric's points carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LabelDeclaration {
    /// Its name.
    pub name: String,
    /// The values it may take; any other counts as [`OTHER`]. `None`: any, up to the series cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// One metric a plugin reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetricDeclaration {
    /// Its name: lowercase letters, digits and `_`, starting with a letter.
    pub name: String,
    /// Its kind.
    pub kind: MetricKind,
    /// What it measures, one sentence.
    pub help: String,
    /// Its unit.
    #[serde(default)]
    pub unit: MetricUnit,
    /// Its labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<LabelDeclaration>,
    /// A histogram's buckets' upper bounds, increasing (a last `+Inf` is implied).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<f64>,
}

/// How a panel draws its metric: schedr's chart kinds, in schedr's style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChartKind {
    /// A gauge's value over time, a line per series.
    Line,
    /// A counter's (or a histogram's count's) growth per second over time.
    Rate,
    /// As `line` or `rate`, the series stacked.
    Stacked,
    /// A histogram's distribution over the range, its buckets as bars.
    Histogram,
    /// A histogram's quantiles over time, a line each.
    Quantiles,
    /// One number: a gauge's latest, a counter's growth over the range.
    Number,
    /// A table of the latest value by label.
    Table,
    /// A bar per bucket, the series stacked in it: a counter's growth or a histogram's count of
    /// observations in it, a gauge's value; an empty bucket left empty (the user, 2026-11-21:
    /// "if the user wants to see \"breaks\" he chooses a different type of chart", DN24).
    Bars,
    /// No metric: the extension's own events (plugins discovered, skipped or refused per
    /// worker) and its attempts' latest phases.
    Events,
}

impl ChartKind {
    /// Whether it can draw a metric of `kind`.
    #[must_use]
    pub const fn draws(self, kind: MetricKind) -> bool {
        match self {
            Self::Histogram | Self::Quantiles => matches!(kind, MetricKind::Histogram),
            Self::Line => matches!(kind, MetricKind::Gauge),
            Self::Rate | Self::Stacked | Self::Number | Self::Table | Self::Bars => true,
            Self::Events => false,
        }
    }
}

/// The cell of a row a panel takes: a row is one cell (`full`), two (`left`, `right`) or three
/// (`left`, `center`, `right`); its cells say which.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Cell {
    /// The whole row.
    Full,
    /// The left one.
    Left,
    /// The middle one of three.
    Center,
    /// The right one.
    Right,
}

/// The colours a panel or a series may name (decision 513): schedr's own, each with a light
/// and a dark variant made for its mode; their slot is the stylesheet's `--series-<slot>`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Color {
    /// Slot 1.
    Blue,
    /// Slot 2.
    Orange,
    /// Slot 3.
    Green,
    /// Slot 4.
    Amber,
    /// Slot 5.
    Pink,
    /// Slot 6.
    Forest,
    /// Slot 7.
    Violet,
    /// Slot 8.
    Red,
    /// Slot 9.
    Teal,
    /// Slot 10.
    Cyan,
    /// Slot 11.
    Indigo,
    /// Slot 12.
    Magenta,
    /// Slot 13.
    Brown,
    /// Slot 14.
    Olive,
    /// Slot 15.
    Slate,
    /// Slot 16.
    Gold,
    /// Slot 17.
    Coral,
    /// Slot 18.
    Sky,
    /// Slot 19.
    Plum,
    /// Slot 20.
    Mint,
}

impl Color {
    /// Every colour, by slot.
    pub const ALL: [Self; 20] = [
        Self::Blue,
        Self::Orange,
        Self::Green,
        Self::Amber,
        Self::Pink,
        Self::Forest,
        Self::Violet,
        Self::Red,
        Self::Teal,
        Self::Cyan,
        Self::Indigo,
        Self::Magenta,
        Self::Brown,
        Self::Olive,
        Self::Slate,
        Self::Gold,
        Self::Coral,
        Self::Sky,
        Self::Plum,
        Self::Mint,
    ];

    /// Its slot, 1 to 20: the stylesheet's `--series-<slot>`.
    #[must_use]
    pub fn slot(self) -> usize {
        Self::ALL.iter().position(|c| *c == self).unwrap_or(0) + 1
    }
}

/// The most rows an extension's page has.
pub const MAX_ROWS: u32 = 50;

/// One panel of an extension's page: a chart in one cell of one row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Panel {
    /// Its title.
    pub title: String,
    /// The metric it draws; none for an `events` panel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<String>,
    /// How.
    pub chart: ChartKind,
    /// Its row, from 1 at the top; rows have a fixed height.
    pub row: u32,
    /// Its cell in the row.
    pub cell: Cell,
    /// A `quantiles` chart's quantiles, each between 0 and 1 (default 0.5, 0.95, 0.99).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub quantiles: Vec<f64>,
    /// Its colour (its first series'); none: schedr's, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<Color>,
    /// Its series' colours by a label's value (`{"Running": "green", "Failed": "red"}`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub colors: BTreeMap<String, Color>,
    /// How one point is worked out from its bucket (decision 516); none: its kind's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point: Option<Point>,
    /// How values are combined: the workers' on the "All workers" tab, and the series
    /// `group_by` leaves together (decision 516); none: `sum`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combine: Option<Combine>,
    /// The labels that get a line each (decision 516); none: all, `[]`: one line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_by: Option<Vec<String>>,
    /// How many lines are shown, the largest by their peak over the range; the rest go into
    /// `other` (decision 516). None: all, up to the metric's series cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// How a panel's point is worked out from its bucket (decision 516).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Point {
    /// A counter's growth in the bucket (a counter's default).
    Increase,
    /// A counter's growth per second.
    Rate,
    /// A gauge's last value in the bucket.
    Last,
    /// A gauge's average (its default, DN26), or a histogram's observations' average.
    Mean,
    /// A gauge's highest value in the bucket.
    Max,
    /// A gauge's lowest value in the bucket.
    Min,
    /// A histogram's observations in the bucket (a histogram's default).
    Count,
    /// A histogram's quantiles (a `quantiles` chart's).
    Quantiles,
}

impl Point {
    /// Whether a metric of `kind` has it.
    #[must_use]
    pub const fn fits(self, kind: MetricKind) -> bool {
        match kind {
            MetricKind::Counter => matches!(self, Self::Increase | Self::Rate),
            MetricKind::Gauge => matches!(self, Self::Last | Self::Mean | Self::Max | Self::Min),
            MetricKind::Histogram => matches!(self, Self::Count | Self::Mean | Self::Quantiles),
        }
    }

    /// A `kind`'s default.
    #[must_use]
    pub const fn default_for(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Counter => Self::Increase,
            MetricKind::Gauge => Self::Mean,
            MetricKind::Histogram => Self::Count,
        }
    }
}

/// How a panel's values are combined (decision 516).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Combine {
    /// Added up (the default).
    Sum,
    /// The highest: a value every worker reads from one place (a cluster's pending pods).
    Max,
    /// Their average.
    Mean,
    /// The lowest.
    Min,
}

/// How many cells a row whose panels take `cells` has: 1, 2 or 3.
///
/// # Errors
///
/// Why the cells don't make one row: `full` beside another, or a cell twice.
pub fn row_cells(cells: &[Cell]) -> Result<u8, String> {
    let mut seen = std::collections::BTreeSet::new();
    for cell in cells {
        if !seen.insert(*cell) {
            return Err(format!("cell `{cell:?}` twice"));
        }
    }
    if seen.contains(&Cell::Full) {
        return if seen.len() == 1 {
            Ok(1)
        } else {
            Err("a `full` cell is alone in its row".to_owned())
        };
    }
    Ok(if seen.contains(&Cell::Center) { 3 } else { 2 })
}

/// What a plugin declares for its metrics and its page.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Declarations {
    /// Its metrics.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<MetricDeclaration>,
    /// Its page's panels; a metric none shows gets a `full` row below them ([`page_panels`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub panels: Vec<Panel>,
}

/// One point a plugin reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetricPoint {
    /// The declared metric.
    pub name: String,
    /// Its labels' values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// A counter's increase, a gauge's value, a histogram's observation.
    pub value: f64,
}

/// Why the worker stops a plugin (sent in `stop` with SIGTERM; a memory kill has no grace and
/// no message).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StopKind {
    /// A user cancelled the task.
    Cancel,
    /// The task's timeout passed.
    Timeout,
    /// The worker drains: the task is retried elsewhere.
    Drain,
    /// The worker lost the master: the task may be run (or cleaned up) elsewhere.
    MasterLost,
}

fn check_name(what: &str, name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    let ok = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.len() <= 64;
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{what} `{name}`: lowercase letters, digits and `_`, starting with a letter, at most 64"
        ))
    }
}

fn check_text(what: &str, text: &str) -> Result<(), String> {
    if text.trim().is_empty() || text.chars().count() > MAX_TEXT {
        return Err(format!("{what}: 1 to {MAX_TEXT} characters"));
    }
    Ok(())
}

fn check_metric(metric: &MetricDeclaration) -> Result<(), String> {
    check_name("metric", &metric.name)?;
    check_text(&format!("metric `{}`'s help", metric.name), &metric.help)?;
    if metric.labels.len() > MAX_LABELS {
        return Err(format!(
            "metric `{}`: at most {MAX_LABELS} labels",
            metric.name
        ));
    }
    let mut labels = BTreeSet::new();
    for label in &metric.labels {
        check_name(&format!("metric `{}`'s label", metric.name), &label.name)?;
        if label.name == VERSION_LABEL {
            return Err(format!(
                "metric `{}`: label `{VERSION_LABEL}` is the worker's",
                metric.name
            ));
        }
        if !labels.insert(label.name.as_str()) {
            return Err(format!(
                "metric `{}`: label `{}` twice",
                metric.name, label.name
            ));
        }
        if let Some(values) = &label.values
            && (values.is_empty() || values.iter().any(|v| check_text("a value", v).is_err()))
        {
            return Err(format!(
                "metric `{}`'s label `{}`: its values are 1 to {MAX_TEXT} characters, at least one",
                metric.name, label.name
            ));
        }
    }
    match metric.kind {
        MetricKind::Histogram => {
            let increasing = metric.buckets.windows(2).all(|w| w[0] < w[1]);
            if metric.buckets.is_empty()
                || metric.buckets.len() > MAX_BUCKETS
                || !increasing
                || metric.buckets.iter().any(|b| !b.is_finite())
            {
                return Err(format!(
                    "histogram `{}`: 1 to {MAX_BUCKETS} finite buckets, increasing",
                    metric.name
                ));
            }
        }
        MetricKind::Counter | MetricKind::Gauge if !metric.buckets.is_empty() => {
            return Err(format!(
                "metric `{}`: only a histogram has buckets",
                metric.name
            ));
        }
        MetricKind::Counter | MetricKind::Gauge => {}
    }
    Ok(())
}

/// Whether `declarations` can be used: names, limits, and each panel drawing a declared metric
/// with a chart that can draw it.
///
/// # Errors
///
/// What is wrong, naming the metric or the panel.
pub fn check_declarations(declarations: &Declarations) -> Result<(), String> {
    if declarations.metrics.len() > MAX_METRICS || declarations.panels.len() > MAX_PANELS {
        return Err(format!(
            "at most {MAX_METRICS} metrics and {MAX_PANELS} panels"
        ));
    }
    let mut kinds = BTreeMap::new();
    for metric in &declarations.metrics {
        check_metric(metric)?;
        if kinds.insert(metric.name.as_str(), metric.kind).is_some() {
            return Err(format!("metric `{}` declared twice", metric.name));
        }
    }
    for panel in &declarations.panels {
        check_functions(panel, &declarations.metrics)?;
    }
    for panel in &declarations.panels {
        check_text("a panel's title", &panel.title)?;
        if panel.row == 0 || panel.row > MAX_ROWS {
            return Err(format!(
                "panel `{}`: its row is 1 to {MAX_ROWS}, from the top",
                panel.title
            ));
        }
        match (&panel.metric, panel.chart) {
            (None, ChartKind::Events) => {}
            (Some(_), ChartKind::Events) => {
                return Err(format!(
                    "panel `{}`: an events panel has no metric",
                    panel.title
                ));
            }
            (None, _) => {
                return Err(format!("panel `{}`: which metric it draws", panel.title));
            }
            (Some(metric), chart) => {
                let Some(&kind) = kinds.get(metric.as_str()) else {
                    return Err(format!(
                        "panel `{}`: metric `{metric}` isn't declared",
                        panel.title
                    ));
                };
                if !chart.draws(kind) {
                    return Err(format!(
                        "panel `{}`: a {chart:?} chart can't draw {kind:?} `{metric}`",
                        panel.title
                    ));
                }
            }
        }
        if !panel.quantiles.is_empty()
            && (panel.chart != ChartKind::Quantiles
                || panel.quantiles.iter().any(|q| !(*q > 0.0 && *q < 1.0)))
        {
            return Err(format!(
                "panel `{}`: quantiles only on a quantiles chart, each between 0 and 1",
                panel.title
            ));
        }
    }
    let mut rows: BTreeMap<u32, Vec<Cell>> = BTreeMap::new();
    for panel in &declarations.panels {
        rows.entry(panel.row).or_default().push(panel.cell);
    }
    for (row, cells) in &rows {
        row_cells(cells).map_err(|why| format!("row {row}: {why}"))?;
    }
    Ok(())
}

/// A panel's functions (decision 516) checked against its metric: a `point` or `combine` its
/// kind doesn't have, a `group_by` label it doesn't declare, a `limit` of 0 refused.
fn check_functions(panel: &Panel, metrics: &[MetricDeclaration]) -> Result<(), String> {
    let named = panel.point.is_some()
        || panel.combine.is_some()
        || panel.group_by.is_some()
        || panel.limit.is_some();
    let refused = |what: String| Err(format!("panel `{}`: {what}", panel.title));
    if !named {
        return Ok(());
    }
    // agy's review of M5.2i10: an events panel draws no metric, a quantile only on its chart.
    if panel.chart == ChartKind::Events {
        return refused(
            "an events panel draws no metric: no `point`, `combine`, `group_by` or `limit`"
                .to_owned(),
        );
    }
    if (panel.point == Some(Point::Quantiles)) != (panel.chart == ChartKind::Quantiles)
        && panel.point.is_some()
    {
        return refused("`point: quantiles` is a `quantiles` chart's, and only its".to_owned());
    }
    let Some(metric) = panel
        .metric
        .as_deref()
        .and_then(|name| metrics.iter().find(|m| m.name == name))
    else {
        // A missing metric is said by the panel's own check.
        return Ok(());
    };
    if let Some(point) = panel.point
        && !point.fits(metric.kind)
    {
        return refused(format!(
            "a {:?} `{}` has no point `{point:?}`",
            metric.kind, metric.name
        ));
    }
    if metric.kind == MetricKind::Histogram && panel.combine.is_some_and(|c| c != Combine::Sum) {
        return refused("a histogram's observations are only added up (`combine: sum`)".to_owned());
    }
    for label in panel.group_by.iter().flatten() {
        if !metric.labels.iter().any(|l| &l.name == label) {
            return refused(format!(
                "`group_by` names `{label}`, not a label of `{}`",
                metric.name
            ));
        }
    }
    if panel.limit == Some(0) {
        return refused("its `limit` is at least 1".to_owned());
    }
    Ok(())
}

/// An extension's page: its declared panels, then each metric no panel shows in a `full` row
/// of its own, one under another below them (as the Metrics page lays out its charts), with the
/// chart its kind suggests.
#[must_use]
pub fn page_panels(declarations: &Declarations) -> Vec<Panel> {
    let shown: BTreeSet<&str> = declarations
        .panels
        .iter()
        .filter_map(|p| p.metric.as_deref())
        .collect();
    let mut panels = declarations.panels.clone();
    let mut row = panels.iter().map(|p| p.row).max().unwrap_or(0);
    for metric in &declarations.metrics {
        if shown.contains(metric.name.as_str()) {
            continue;
        }
        row = row.saturating_add(1);
        panels.push(Panel {
            title: metric.help.clone(),
            metric: Some(metric.name.clone()),
            chart: match metric.kind {
                MetricKind::Counter => ChartKind::Rate,
                MetricKind::Gauge => ChartKind::Line,
                MetricKind::Histogram => ChartKind::Quantiles,
            },
            row,
            cell: Cell::Full,
            quantiles: Vec::new(),
            color: None,
            colors: BTreeMap::new(),
            point: None,
            combine: None,
            group_by: None,
            limit: None,
        });
    }
    panels
}

/// Whether a link's URL may be shown: `http` or `https` only, so a plugin can't send script.
#[must_use]
pub fn link_url_allowed(url: &str) -> bool {
    let lower = url.trim_start().to_ascii_lowercase();
    (lower.starts_with("https://") || lower.starts_with("http://"))
        && url.chars().count() <= MAX_TEXT
}

#[cfg(test)]
mod tests;

/// The name no label may take: the worker's `/metrics` labels each series with its extension's
/// version under it.
pub const VERSION_LABEL: &str = "extension_version";

/// The longest label value kept; a longer one is cut.
pub const MAX_LABEL_VALUE: usize = 128;

/// One series' figures as a worker holds them, since its plugin first reported it: a counter's
/// total or a gauge's value in `value`; a histogram's count per bucket (its last `+Inf`) and its
/// observations' sum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesFigures {
    /// Its labels' values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// A counter's growth since the worker's last figures, a gauge's value; 0 for a histogram.
    #[serde(default)]
    pub value: f64,
    /// A histogram's new observations per bucket since the worker's last figures, not
    /// cumulative, the last past its last bound.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub counts: Vec<u64>,
    /// A histogram's new observations' sum.
    #[serde(default)]
    pub sum: f64,
}

/// One metric's series.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricFigures {
    /// The metric.
    pub name: String,
    /// Its kind, as declared: the master reads its series by it, with no declarations kept.
    pub kind: MetricKind,
    /// A histogram's bucket bounds, as declared; none for the other kinds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<f64>,
    /// Its series; at most the extension's `max_series` and one `other`.
    pub series: Vec<SeriesFigures>,
}

/// What happened to an extension on a worker (decision 514, DN19): shown in its events panels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// Its plugin found on the worker's `PATH`.
    Discovered,
    /// A plugin of it not used: configured, or one found earlier on `PATH`, or its
    /// `--describe` gave nothing the worker can use.
    Skipped,
    /// Its plugin's `hello` refused: a protocol the worker doesn't speak.
    Refused,
    /// Something it declared or sent dropped: declarations that can't be used, a metric point
    /// they don't allow.
    Dropped,
}

impl EventKind {
    /// Its name, as stored and shown.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Discovered => "discovered",
            Self::Skipped => "skipped",
            Self::Refused => "refused",
            Self::Dropped => "dropped",
        }
    }

    /// The kind `name` names.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "discovered" => Self::Discovered,
            "skipped" => Self::Skipped,
            "refused" => Self::Refused,
            "dropped" => Self::Dropped,
            _ => return None,
        })
    }
}

/// One thing that happened to an extension on a worker, sent to the master in a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionEvent {
    /// When, in milliseconds since the Unix epoch.
    pub at_ms: i64,
    /// The extension.
    pub extension: String,
    /// Its version, when known (a plugin skipped before saying it has none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// What happened.
    pub kind: EventKind,
    /// What about it, at most [`MAX_TEXT`] characters.
    pub message: String,
}

impl ExtensionEvent {
    /// An event now.
    #[must_use]
    pub fn now(extension: &str, version: Option<&str>, kind: EventKind, message: &str) -> Self {
        Self {
            at_ms: jiff::Timestamp::now().as_millisecond(),
            extension: extension.to_owned(),
            version: version.map(ToOwned::to_owned),
            kind,
            message: message.chars().take(MAX_TEXT).collect(),
        }
    }
}

/// The most events a worker holds for its next heartbeat: past them, the oldest go.
pub const MAX_PENDING_EVENTS: usize = 100;

/// The most events the master keeps per extension: past them, the oldest go.
pub const KEPT_EVENTS: usize = 200;

/// What one extension version's metrics did since the worker's last figures, sent to the
/// master in its heartbeat about once a minute (decision 513): counters' growth, histograms'
/// new observations, gauges' values. Sent once: the master adds them to the minute's samples,
/// keeping nothing of a worker's earlier figures (Codex's review of M5.2i5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionFigures {
    /// The extension.
    pub extension: String,
    /// Its version.
    pub version: String,
    /// The most series each metric keeps.
    pub max_series: u32,
    /// What its metrics and page are; sent with a stream's first figures and when they change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declarations: Option<Declarations>,
    /// Its metrics' series.
    pub metrics: Vec<MetricFigures>,
}
