//! The Metrics page (M5.1j; decisions 465, 466): per deployment, over 1 h, 24 h, 7 d or 30 d,
//! memory and CPU charts of its processes (the 8 highest memory peaks of those running as
//! lines, the rest as one band, those that ended as one line, container limits dashed) over a
//! sortable table of those running; a row opens a panel
//! with that process's memory by part and its largest tasks; below, its attempts ended and
//! their start latency (decision 494). It reads each deployment's
//! `GET /api/v1/metrics/samples` and `GET /api/v1/metrics/tasks` and refreshes once a minute,
//! as the samples come.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use askama::Template;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use jiff::tz::TimeZone;
use schedr_api::series::Range;
use schedr_api::views::{ProcessSamplesView, SampleSeriesList, TaskFlowList};
use schedr_client::{ClientError, Transport, path_segment};

use crate::Ui;
use crate::chart::{self, Line, Spec, Unit};
use crate::deployments::short;
use crate::page::Page;
use crate::pages::render;

mod flow;

use flow::FlowCharts;

/// What the page shows.
#[derive(Debug, Default, serde::Deserialize)]
pub struct MetricsQuery {
    /// `1h`, `24h` (the default), `7d` or `30d`.
    pub range: Option<String>,
    /// One deployment's only.
    pub deployment: Option<String>,
    /// The table's order: `name`, `now`, `peak` (the default), `container`, `use`.
    pub sort: Option<String>,
    /// The process whose panel is open (with `deployment`).
    pub process: Option<String>,
}

/// The table's columns one can sort by: the key and its heading.
const SORTS: [(&str, &str); 5] = [
    ("name", "Process"),
    ("now", "Now"),
    ("peak", "Peak"),
    ("container", "Container"),
    ("use", "Use"),
];

/// A link of the page's selectors: its text, where it goes, whether it is the one shown.
#[derive(Debug, Clone)]
pub struct Choice {
    /// Its text.
    pub label: String,
    /// Where it goes.
    pub href: String,
    /// Whether it is the current one.
    pub current: bool,
}

/// An entry of a chart's legend.
#[derive(Debug, Clone)]
pub struct LegendItem {
    /// Its colour slot (1 to 8); 0: the band.
    pub slot: usize,
    /// What it is.
    pub label: String,
}

/// A process's row in the table.
#[derive(Debug, Clone)]
pub struct ProcessRow {
    /// Its name: its roles and their nodes.
    pub label: String,
    /// What it runs: `master, worker`.
    pub kind: String,
    /// Its whole memory now (the latest point): itself and, for a worker, its parts.
    pub now: String,
    /// That by part, for its title.
    pub split: String,
    /// The process itself now (resident).
    pub itself: String,
    /// A worker's running tasks now; empty for another process.
    pub tasks: String,
    /// The highest over the range.
    pub peak: String,
    /// Its container's memory now; empty outside a container.
    pub container: String,
    /// Its container's limit; empty when none.
    pub limit: String,
    /// The container's memory as a share of its limit, in percent; none without both.
    pub share: Option<u64>,
    /// Its cores now.
    pub cpu: String,
    /// Opens its panel.
    pub href: String,
    /// Its line's colour slot; none: in the band.
    pub slot: Option<usize>,
    /// Whether its panel is open.
    pub open: bool,
    sort_now: u64,
    sort_peak: u64,
    sort_container: u64,
}

/// The open process's panel.
#[derive(Debug, Clone)]
pub struct ProcessPanel {
    /// Its title: `worker w1 · 5.1 GiB`.
    pub title: String,
    /// Its memory by part over the range.
    pub chart: String,
    /// The legend of that chart.
    pub legend: Vec<LegendItem>,
    /// What it is now, part by part.
    pub facts: Vec<(String, String)>,
    /// Its largest tasks now: the task, its memory, its run's page.
    pub tasks: Vec<(String, String, String)>,
    /// Closes it.
    pub close: String,
}

/// One deployment's part of the page.
#[derive(Debug, Clone)]
pub struct Section {
    /// The deployment.
    pub name: String,
    /// Why it couldn't be asked (unreachable); none when it answered.
    pub error: Option<String>,
    /// Its memory chart.
    pub memory: String,
    /// Its memory by kind, stacked: masters and webservers, workers, and the workers' fork
    /// servers, wait step hosts and tasks.
    pub kinds: String,
    /// That chart's legend, each kind with its memory now.
    pub kinds_legend: Vec<LegendItem>,
    /// Its CPU chart.
    pub cpu: String,
    /// Both charts' legend.
    pub legend: Vec<LegendItem>,
    /// Each running process, in the table's order.
    pub rows: Vec<ProcessRow>,
    /// Whether it had no process sampled in the range.
    pub empty: bool,
    /// The processes that ended in the range (drawn as one line), as a note under the table;
    /// empty when none did.
    pub ended: String,
    /// The table's headings: sort links.
    pub sorts: Vec<Choice>,
    /// The open panel, if one of its processes'.
    pub panel: Option<ProcessPanel>,
    /// Its attempts' charts, or why they couldn't be read.
    pub flow: Result<FlowCharts, String>,
}

#[derive(Template)]
#[template(path = "metrics.html")]
struct MetricsPage {
    page: Page,
    ranges: Vec<Choice>,
    deployments: Vec<Choice>,
    sections: Vec<Section>,
    zone: String,
}

/// Where the page with these choices is.
fn href(
    base: &str,
    range: Range,
    deployment: Option<&str>,
    sort: &str,
    process: Option<&str>,
) -> String {
    let mut href = format!("{base}/metrics?range={}", range.name());
    if let Some(deployment) = deployment {
        let _ = write!(href, "&deployment={}", path_segment(deployment));
    }
    if sort != "peak" {
        let _ = write!(href, "&sort={}", path_segment(sort));
    }
    if let Some(process) = process {
        let _ = write!(href, "&process={}#process-panel", path_segment(process));
    }
    href
}

/// A process's name from what it runs: `master …0d3a9c1f · worker w1`.
#[must_use]
pub fn label(process: &ProcessSamplesView) -> String {
    if process.roles.is_empty() {
        return format!("process {}", short(&process.process_id));
    }
    let order = |role: &str| match role {
        "master" => 0,
        "worker" => 1,
        "webserver" => 2,
        _ => 3,
    };
    let mut roles: Vec<_> = process.roles.iter().collect();
    roles.sort_by_key(|(role, _)| order(role));
    roles
        .into_iter()
        .map(|(role, node)| match role.as_str() {
            "master" | "webserver" => format!("{role} …{}", short(node)),
            _ => format!("{role} {node}"),
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

/// The time zone the axes show.
fn zone_of(page: &Page) -> TimeZone {
    page.zone.clone().unwrap_or(TimeZone::UTC)
}

/// The colour slot of the one line of the processes that ended: the last, the running ones
/// taking the others.
const ENDED_SLOT: usize = chart::SERIES;

/// How many of `processes` have ended.
fn ended_count(processes: &[ProcessSamplesView]) -> usize {
    processes.iter().filter(|p| p.ended).count()
}

/// `n processes`, `1 process`.
fn processes_word(n: usize) -> String {
    format!("{n} process{}", if n == 1 { "" } else { "es" })
}

/// The running processes drawn as lines (the [`chart::SERIES`] highest memory peaks, one
/// fewer when some ended: their line takes [`ENDED_SLOT`]), each with its colour slot, given
/// in name order so a colour stays with its process while the set holds. A process that ended
/// (a restart's old one: the user, 2026-11-16) is never one: they are one line together, so
/// restarts don't crowd the running ones out.
fn drawn(processes: &[ProcessSamplesView]) -> BTreeMap<String, usize> {
    let mut by_peak: Vec<(&ProcessSamplesView, u64)> = processes
        .iter()
        .filter(|p| !p.ended)
        .map(|p| (p, p.points.iter().map(|x| x.memory_peak).max().unwrap_or(0)))
        .collect();
    by_peak.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| label(a.0).cmp(&label(b.0))));
    let room = chart::SERIES - usize::from(ended_count(processes) > 0);
    let mut top: Vec<&ProcessSamplesView> =
        by_peak.into_iter().take(room).map(|(p, _)| p).collect();
    top.sort_by_key(|p| label(p));
    top.into_iter()
        .enumerate()
        .map(|(i, p)| (p.process_id.clone(), i + 1))
        .collect()
}

/// Seconds since the epoch.
fn secs(at: jiff::Timestamp) -> i64 {
    at.as_second()
}

/// A memory or CPU chart of `list`'s processes: the drawn ones as lines, the other running ones
/// as a band, and those that ended as one line, their values added up (each bucket's: they
/// seldom overlap, one replacing another).
fn process_chart(
    title: String,
    list: &SampleSeriesList,
    drawn: &BTreeMap<String, usize>,
    unit: Unit,
    zone: &TimeZone,
) -> String {
    let value = |p: &schedr_api::views::SamplePointView| match unit {
        #[expect(clippy::cast_precision_loss, reason = "a chart's coordinates")]
        Unit::Bytes => Some(p.memory_avg as f64),
        Unit::Cores => p.cpu_avg,
        // Not a process's: the attempt charts draw their own.
        Unit::Count | Unit::Millis => None,
    };
    let mut lines = Vec::new();
    let mut limits = Vec::new();
    let mut rest: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
    let mut others = 0;
    let mut ended: BTreeMap<i64, f64> = BTreeMap::new();
    for process in &list.processes {
        let points: Vec<(i64, f64)> = process
            .points
            .iter()
            .filter_map(|p| value(p).map(|v| (secs(p.at), v)))
            .collect();
        if let Some(&slot) = drawn.get(&process.process_id) {
            if unit == Unit::Bytes
                && let Some(limit) = process.container_limit
            {
                #[expect(clippy::cast_precision_loss, reason = "a chart's coordinates")]
                limits.push((
                    format!("{}'s container limit", label(process)),
                    slot,
                    limit as f64,
                ));
            }
            lines.push(Line {
                label: label(process),
                slot,
                points,
            });
        } else if process.ended {
            for (at, v) in points {
                *ended.entry(at).or_default() += v;
            }
        } else {
            others += 1;
            for (at, v) in points {
                let entry = rest.entry(at).or_insert((v, v));
                entry.0 = entry.0.min(v);
                entry.1 = entry.1.max(v);
            }
        }
    }
    let ended_n = ended_count(&list.processes);
    if ended_n > 0 {
        lines.push(Line {
            label: format!("{} that ended", processes_word(ended_n)),
            slot: ENDED_SLOT,
            points: ended.into_iter().collect(),
        });
    }
    lines.sort_by_key(|l| l.slot);
    let band = (others > 0).then(|| {
        (
            format!("{} other running", processes_word(others)),
            rest.into_iter()
                .map(|(at, (lo, hi))| (at, lo, hi))
                .collect(),
        )
    });
    chart::draw(&Spec {
        title,
        unit,
        width: chart::WIDTH,
        from: secs(list.from),
        to: secs(list.to),
        step: i64::from(list.step_s),
        lines,
        band,
        stacked: Vec::new(),
        limits,
        zone: zone.clone(),
        bars: false,
    })
}

/// The kinds of memory a deployment's processes hold, bottom first, and what of a point each
/// is (decided by the process's roles and its parts).
const KINDS: [&str; 5] = [
    "masters and webservers",
    "workers",
    "fork servers",
    "wait step hosts",
    "tasks",
];

/// `point`'s memory of each of [`KINDS`] for a process that runs a worker (`worker`) or not.
fn by_kind(point: &schedr_api::views::SamplePointView, worker: bool) -> [u64; 5] {
    let parts = point.parts.unwrap_or(schedr_api::views::PartsPointView {
        fork_servers_bytes: 0,
        watcher_hosts_bytes: 0,
        tasks_bytes: 0,
    });
    let (others, workers) = if worker {
        (0, point.resident_avg)
    } else {
        (point.resident_avg, 0)
    };
    [
        others,
        workers,
        parts.fork_servers_bytes,
        parts.watcher_hosts_bytes,
        parts.tasks_bytes,
    ]
}

/// A deployment's memory by kind, stacked (the user, 2026-11-14: how much the masters and
/// workers hold, how much the tasks), and its legend with each kind's memory now.
fn kinds_chart(name: &str, list: &SampleSeriesList, zone: &TimeZone) -> (String, Vec<LegendItem>) {
    // Each process's kinds weighted by the minutes it was sampled in a bucket, over the minutes
    // the deployment was sampled there (the API's coverage; else the most any process has): a
    // process replaced within a bucket counts for the time it ran, not twice (Codex's reviews
    // of M5.1j).
    let coverage: BTreeMap<i64, u32> = list
        .coverage
        .iter()
        .map(|c| (secs(c.at), c.minutes))
        .collect();
    let mut weighted: BTreeMap<i64, ([u128; 5], u32)> = BTreeMap::new();
    for process in &list.processes {
        let worker = process.roles.contains_key("worker");
        for point in &process.points {
            let entry = weighted.entry(secs(point.at)).or_default();
            for (sum, add) in entry.0.iter_mut().zip(by_kind(point, worker)) {
                *sum += u128::from(add) * u128::from(point.minutes);
            }
            entry.1 = entry.1.max(point.minutes);
        }
    }
    let sums: BTreeMap<i64, [u64; 5]> = weighted
        .into_iter()
        .map(|(at, (kinds, most))| {
            let minutes = u128::from(coverage.get(&at).copied().unwrap_or(most).max(1));
            let kinds =
                kinds.map(|sum| u64::try_from((sum + minutes / 2) / minutes).unwrap_or(u64::MAX));
            (at, kinds)
        })
        .collect();
    let now = sums.values().next_back().copied().unwrap_or_default();
    #[expect(clippy::cast_precision_loss, reason = "a chart's coordinates")]
    let f = |n: u64| n as f64;
    let mut legend = Vec::new();
    let mut stacked = Vec::new();
    for (i, kind) in KINDS.iter().enumerate() {
        // A kind no process has had over the range is left out.
        if sums.values().all(|kinds| kinds[i] == 0) {
            continue;
        }
        legend.push(LegendItem {
            slot: i + 1,
            label: format!("{kind} {}", chart::bytes(f(now[i]))),
        });
        stacked.push(Line {
            label: (*kind).to_owned(),
            slot: i + 1,
            points: sums.iter().map(|(at, kinds)| (*at, f(kinds[i]))).collect(),
        });
    }
    let svg = chart::draw(&Spec {
        title: format!("Memory of {name} by kind"),
        unit: Unit::Bytes,
        width: chart::WIDTH,
        from: secs(list.from),
        to: secs(list.to),
        step: i64::from(list.step_s),
        lines: Vec::new(),
        band: None,
        stacked,
        limits: Vec::new(),
        zone: zone.clone(),
        bars: false,
    });
    (svg, legend)
}

/// The table's rows of `list`, sorted by `sort` (biggest first; names A to Z).
fn rows(
    list: &SampleSeriesList,
    drawn: &BTreeMap<String, usize>,
    open: Option<&str>,
    choose: &dyn Fn(Option<&str>) -> String,
    sort: &str,
) -> Vec<ProcessRow> {
    let mut rows: Vec<ProcessRow> = list
        .processes
        .iter()
        .filter(|process| !process.ended)
        .map(|process| {
            let latest = process.points.last();
            let now = latest.map_or(0, |p| p.memory_avg);
            let peak = process
                .points
                .iter()
                .map(|p| p.memory_peak)
                .max()
                .unwrap_or(0);
            let parts = latest.and_then(|p| p.parts);
            let container = latest.and_then(|p| p.container_avg);
            let limit = process.container_limit;
            #[expect(clippy::cast_precision_loss, reason = "a share in percent")]
            let show = |n: u64| chart::bytes(n as f64);
            let share = container
                .zip(limit)
                .filter(|(_, limit)| *limit > 0)
                .map(|(used, limit)| used.saturating_mul(100) / limit);
            ProcessRow {
                label: label(process),
                kind: process.roles.keys().cloned().collect::<Vec<_>>().join(", "),
                now: show(now),
                split: latest.map_or_else(String::new, |p| {
                    let mut split = format!("itself {}", show(p.resident_avg));
                    if let Some(parts) = p.parts {
                        let _ = write!(
                            split,
                            " · fork servers {} · wait step hosts {} · tasks {}",
                            show(parts.fork_servers_bytes),
                            show(parts.watcher_hosts_bytes),
                            show(parts.tasks_bytes)
                        );
                    }
                    split
                }),
                itself: show(latest.map_or(0, |p| p.resident_avg)),
                tasks: parts.map(|p| show(p.tasks_bytes)).unwrap_or_default(),
                peak: show(peak),
                container: container.map(show).unwrap_or_default(),
                limit: limit.map(show).unwrap_or_default(),
                share,
                cpu: latest
                    .and_then(|p| p.cpu_avg)
                    .map(|c| format!("{c:.2}"))
                    .unwrap_or_default(),
                href: choose(Some(&process.process_id)),
                slot: drawn.get(&process.process_id).copied(),
                open: open == Some(process.process_id.as_str()),
                sort_now: now,
                sort_peak: peak,
                sort_container: container.unwrap_or(0),
            }
        })
        .collect();
    match sort {
        "name" => rows.sort_by(|a, b| a.label.cmp(&b.label)),
        "now" => rows.sort_by_key(|r| Reverse(r.sort_now)),
        "container" => rows.sort_by_key(|r| Reverse(r.sort_container)),
        "use" => rows.sort_by_key(|r| Reverse(r.share)),
        _ => rows.sort_by_key(|r| Reverse(r.sort_peak)),
    }
    rows
}

/// A process's memory by part over the range, stacked (itself, and a worker's fork servers,
/// wait step hosts and tasks), its container's memory as a line and its limit dashed; and
/// the chart's legend.
fn parts_chart(
    process: &ProcessSamplesView,
    list: &SampleSeriesList,
    zone: &TimeZone,
) -> (String, Vec<LegendItem>) {
    #[expect(clippy::cast_precision_loss, reason = "a chart's coordinates")]
    let f = |n: u64| n as f64;
    let at = |p: &schedr_api::views::SamplePointView| secs(p.at);
    let worker = process.roles.contains_key("worker");
    // The kinds after "masters and webservers" and "workers": the process itself first.
    let names = [
        "the process itself",
        "fork servers",
        "wait step hosts",
        "tasks",
    ];
    let mut stacked = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let points: Vec<_> = process
            .points
            .iter()
            .map(|p| {
                let kinds = by_kind(p, worker);
                let value = if i == 0 {
                    kinds[0] + kinds[1]
                } else {
                    kinds[i + 1]
                };
                (at(p), f(value))
            })
            .collect();
        if i == 0 || points.iter().any(|p| p.1 > 0.0) {
            stacked.push(Line {
                label: (*name).to_owned(),
                slot: i + 2,
                points,
            });
        }
    }
    let container: Vec<_> = process
        .points
        .iter()
        .filter_map(|p| p.container_avg.map(|c| (at(p), f(c))))
        .collect();
    let mut lines = Vec::new();
    if !container.is_empty() {
        lines.push(Line {
            label: "its container".to_owned(),
            slot: 1,
            points: container,
        });
    }
    let limits = process
        .container_limit
        .map(|limit| ("container limit".to_owned(), 1, f(limit)))
        .into_iter()
        .collect();
    let legend = stacked
        .iter()
        .chain(&lines)
        .map(|l| LegendItem {
            slot: l.slot,
            label: l.label.clone(),
        })
        .collect();
    let chart = chart::draw(&Spec {
        title: format!("Memory of {} by part", label(process)),
        unit: Unit::Bytes,
        width: chart::NARROW,
        from: secs(list.from),
        to: secs(list.to),
        step: i64::from(list.step_s),
        lines,
        band: None,
        stacked,
        limits,
        zone: zone.clone(),
        bars: false,
    });
    (chart, legend)
}

/// The open process's panel: its memory by part over the range, and now.
fn panel(
    process: &ProcessSamplesView,
    list: &SampleSeriesList,
    zone: &TimeZone,
    (runs, close): (String, String),
) -> ProcessPanel {
    #[expect(clippy::cast_precision_loss, reason = "a byte count shown")]
    let f = |n: u64| n as f64;
    let (chart, legend) = parts_chart(process, list, zone);
    let name = label(process);
    let latest = process.points.last();
    let show = |n: u64| chart::bytes(f(n));
    let mut facts = vec![("Process".to_owned(), process.process_id.clone())];
    if let Some(p) = latest {
        facts.push(("Resident now".to_owned(), show(p.resident_avg)));
        if let Some(c) = p.container_avg {
            let limit = process
                .container_limit
                .map_or_else(String::new, |l| format!(" of {}", show(l)));
            facts.push(("Container now".to_owned(), format!("{}{limit}", show(c))));
        }
    }
    let mut tasks = Vec::new();
    if let Some(parts) = &process.parts {
        facts.push(("Fork servers".to_owned(), show(parts.fork_servers_bytes)));
        facts.push((
            "Wait step hosts".to_owned(),
            show(parts.watcher_hosts_bytes),
        ));
        facts.push(("Running tasks".to_owned(), show(parts.tasks_bytes)));
        tasks = parts
            .largest_tasks
            .iter()
            .map(|t| {
                let task = t.map_index.map_or_else(
                    || format!("{}.{}", t.dag_id, t.task_id),
                    |i| format!("{}.{}[{i}]", t.dag_id, t.task_id),
                );
                (
                    task,
                    show(t.bytes),
                    format!("{runs}{}", path_segment(&t.run_id)),
                )
            })
            .collect();
    }
    ProcessPanel {
        title: latest.map_or_else(
            || name.clone(),
            |p| format!("{name} · {}", show(p.memory_avg)),
        ),
        chart,
        legend,
        facts,
        tasks,
        close,
    }
}

/// Both process charts' legend (the drawn processes, the band of the other running ones, the
/// line of those that ended) and the note of those that ended under the table.
fn legend_of(
    list: &SampleSeriesList,
    drawn: &BTreeMap<String, usize>,
) -> (Vec<LegendItem>, String) {
    let mut legend: Vec<LegendItem> = list
        .processes
        .iter()
        .filter_map(|p| {
            drawn.get(&p.process_id).map(|&slot| LegendItem {
                slot,
                label: label(p),
            })
        })
        .collect();
    legend.sort_by_key(|item| item.slot);
    let ended_n = ended_count(&list.processes);
    let others = list.processes.len().saturating_sub(drawn.len() + ended_n);
    if others > 0 {
        legend.push(LegendItem {
            slot: 0,
            label: format!(
                "{} other running, lowest to highest",
                processes_word(others)
            ),
        });
    }
    if ended_n > 0 {
        legend.push(LegendItem {
            slot: ENDED_SLOT,
            label: format!("{} that ended, added up", processes_word(ended_n)),
        });
    }
    let ended = if ended_n > 0 {
        format!(
            "{} ended in this range (stopped, or restarted as a new process): one line in the \
             charts, not in the table.",
            processes_word(ended_n)
        )
    } else {
        String::new()
    };
    (legend, ended)
}

/// One deployment's section from its answer.
fn section(
    page: &Page,
    name: &str,
    (answer, flow): (
        Result<SampleSeriesList, ClientError>,
        Result<TaskFlowList, ClientError>,
    ),
    query: &MetricsQuery,
    range: Range,
    sort: &str,
) -> Section {
    let zone = zone_of(page);
    let flow = flow
        .map(|list| flow::charts(name, &list, &zone))
        .map_err(|error| error.to_string());
    let list = match answer {
        Ok(list) => list,
        Err(error) => {
            return Section {
                name: name.to_owned(),
                error: Some(error.to_string()),
                memory: String::new(),
                kinds: String::new(),
                kinds_legend: Vec::new(),
                cpu: String::new(),
                legend: Vec::new(),
                rows: Vec::new(),
                empty: false,
                ended: String::new(),
                sorts: Vec::new(),
                panel: None,
                flow,
            };
        }
    };
    let drawn = drawn(&list.processes);
    let filter = query.deployment.as_deref();
    let choose = |process: Option<&str>| {
        let deployment = (filter.is_some() || process.is_some()).then_some(filter.unwrap_or(name));
        href(&page.base, range, deployment, sort, process)
    };
    let open = (query.deployment.as_deref() == Some(name))
        .then_some(query.process.as_deref())
        .flatten();
    let (legend, ended) = legend_of(&list, &drawn);
    let sorts = SORTS
        .iter()
        .map(|(key, heading)| Choice {
            label: (*heading).to_owned(),
            href: href(&page.base, range, filter, key, None),
            current: *key == sort,
        })
        .collect();
    let panel = open.and_then(|id| {
        // An ended process has no panel: what it says is "now" (Codex's review of D499: one
        // restarted while its panel was open).
        list.processes
            .iter()
            .find(|p| p.process_id == id && !p.ended)
            .map(|p| {
                let runs = format!("{}/d/{}/runs/", page.base, path_segment(name));
                panel(
                    p,
                    &list,
                    &zone,
                    (runs, href(&page.base, range, filter, sort, None)),
                )
            })
    });
    let (kinds, kinds_legend) = kinds_chart(name, &list, &zone);
    Section {
        name: name.to_owned(),
        error: None,
        kinds,
        kinds_legend,
        memory: process_chart(
            format!("Memory of {name}'s processes"),
            &list,
            &drawn,
            Unit::Bytes,
            &zone,
        ),
        cpu: process_chart(
            format!("CPU of {name}'s processes"),
            &list,
            &drawn,
            Unit::Cores,
            &zone,
        ),
        rows: rows(&list, &drawn, open, &choose, sort),
        empty: list.processes.is_empty(),
        ended,
        legend,
        sorts,
        panel,
        flow,
    }
}

/// `GET /metrics`: each deployment's memory and CPU over a range (or one deployment's), its
/// processes in a table, one's panel open.
pub async fn page<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
    query: Result<Query<MetricsQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return (StatusCode::BAD_REQUEST, "the page's address doesn't read").into_response();
    };
    let mut page = Page::new(&headers, ui.docs_dir.is_some());
    let Some(range) = Range::parse(query.range.as_deref().unwrap_or("24h")) else {
        return (
            StatusCode::BAD_REQUEST,
            "`range` is one of 1h, 24h, 7d and 30d",
        )
            .into_response();
    };
    let sort = query
        .sort
        .as_deref()
        .filter(|s| SORTS.iter().any(|(key, _)| key == s))
        .unwrap_or("peak");
    let filter = query.deployment.as_deref();
    let shown: Vec<usize> = (0..ui.deployments.len())
        .filter(|&i| filter.is_none_or(|name| ui.deployments[i].name == name))
        .collect();
    // Read again once a minute, as the samples come (decision 466).
    href(&page.base, range, filter, sort, query.process.as_deref())
        .split('#')
        .next()
        .unwrap_or_default()
        .clone_into(&mut page.refresh);
    let mut asked = tokio::task::JoinSet::new();
    for &index in &shown {
        let ui = Arc::clone(&ui);
        asked.spawn(schedr_client::for_same_user(async move {
            let client = &ui.deployments[index].client;
            let answers = tokio::join!(
                client.samples(range.name(), None),
                client.task_flow(range.name())
            );
            (index, answers)
        }));
    }
    let mut answers = asked.join_all().await;
    answers.sort_by_key(|(index, _)| *index);
    let sections = answers
        .into_iter()
        .map(|(index, answer)| {
            section(
                &page,
                &ui.deployments[index].name,
                answer,
                &query,
                range,
                sort,
            )
        })
        .collect();
    let ranges = Range::ALL
        .into_iter()
        .map(|r| Choice {
            label: r.name().replace('h', " h").replace('d', " d"),
            href: href(&page.base, r, filter, sort, query.process.as_deref()),
            current: r == range,
        })
        .collect();
    let mut deployments = vec![Choice {
        label: "All".to_owned(),
        href: href(&page.base, range, None, sort, None),
        current: filter.is_none(),
    }];
    if ui.deployments.len() > 1 {
        deployments.extend(ui.deployments.iter().map(|d| Choice {
            label: d.name.clone(),
            href: href(&page.base, range, Some(&d.name), sort, None),
            current: filter == Some(d.name.as_str()),
        }));
    } else {
        deployments.clear();
    }
    let zone = zone_of(&page).iana_name().unwrap_or("UTC").to_owned();
    render(
        &MetricsPage {
            page,
            ranges,
            deployments,
            sections,
            zone,
        },
        StatusCode::OK,
    )
}

#[derive(Template)]
#[template(path = "metrics_chart.html")]
struct ChartFragment {
    chart: String,
    legend: Vec<LegendItem>,
    error: Option<String>,
}

/// `GET /metrics/chart?deployment=…&process=…`: one process's memory by part over 24 h, for
/// its node's panel on the Deployments page (loaded when the panel opens).
pub async fn node_chart<T: Transport + Send + Sync + 'static>(
    State(ui): State<Arc<Ui<T>>>,
    headers: HeaderMap,
    query: Result<Query<MetricsQuery>, QueryRejection>,
) -> Response {
    let page = Page::new(&headers, ui.docs_dir.is_some());
    let Ok(Query(query)) = query else {
        return (StatusCode::BAD_REQUEST, "the address doesn't read").into_response();
    };
    let (Some(deployment), Some(process)) = (query.deployment, query.process) else {
        return (StatusCode::BAD_REQUEST, "a deployment and a process").into_response();
    };
    let Some(found) = ui.deployments.iter().find(|d| d.name == deployment) else {
        return (StatusCode::NOT_FOUND, "no such deployment").into_response();
    };
    let answer =
        schedr_client::for_same_user(found.client.samples(Range::Day.name(), Some(&process))).await;
    let fragment = match answer {
        Ok(list) => match list.processes.iter().find(|p| p.process_id == process) {
            Some(samples) => {
                let (chart, legend) = parts_chart(samples, &list, &zone_of(&page));
                ChartFragment {
                    chart,
                    legend,
                    error: None,
                }
            }
            None => ChartFragment {
                chart: String::new(),
                legend: Vec::new(),
                error: Some("No samples in the last 24 h.".to_owned()),
            },
        },
        Err(error) => ChartFragment {
            chart: String::new(),
            legend: Vec::new(),
            error: Some(error.to_string()),
        },
    };
    render(&fragment, StatusCode::OK)
}
