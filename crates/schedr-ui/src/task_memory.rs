//! A task's peak memory over its attempts (M5.1k, decision 467): the task panel's Memory tab,
//! a bar a attempt, and the DAG page's table of each task's p95 peak against its limit.

use jiff::Timestamp;
use jiff::tz::TimeZone;
use schedr_api::views::{AttemptMemoryList, AttemptMemoryView, DagMemoryList};

use crate::chart::{self, Bar, Bars, Unit};
use schedr_client::path_segment;

/// The share of its limit from which a task's p95 is marked.
const NEAR: f64 = 0.9;

/// The Memory tab's chart: its SVG (empty: no attempt ended yet) and its limit's label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryChart {
    /// The chart; empty without an ended attempt.
    pub svg: String,
    /// The attempts it shows.
    pub attempts: usize,
    /// Its limit, as people read it; empty: the worker's share.
    pub limit: String,
    /// Whether an attempt was killed for its memory (the legend names the cross).
    pub killed: bool,
}

/// How an attempt's bar is coloured: by how it ended.
fn class_of(status: &str) -> &'static str {
    match status {
        "success" => "ok",
        "failed" | "timed_out" | "oom_killed" => "failed",
        _ => "other",
    }
}

/// `at` as the chart's labels show it.
fn when(at: Timestamp, zone: &TimeZone) -> String {
    at.to_zoned(zone.clone())
        .strftime("%b %d %H:%M")
        .to_string()
}

/// The Memory tab of task `task_id`: its last attempts' peaks as bars, each linking to its
/// run's panel of the task (`run_href(run_id)`), its limit dashed.
#[must_use]
pub fn memory_chart(
    list: &AttemptMemoryList,
    zone: &TimeZone,
    run_href: impl Fn(&str) -> String,
) -> MemoryChart {
    let limit_bytes = list.limit_bytes;
    #[expect(clippy::cast_precision_loss, reason = "bytes on a chart")]
    let bars: Vec<Bar> = list
        .attempts
        .iter()
        .map(|attempt| Bar {
            value: attempt.peak_memory_bytes.map(|b| b as f64),
            class: class_of(&attempt.status),
            marked: attempt.status == "oom_killed",
            href: attempt_href(attempt, &list.task_id, &run_href),
            title: bar_title(attempt, zone),
        })
        .collect();
    let killed = bars.iter().any(|b| b.marked);
    let svg = match (list.attempts.first(), list.attempts.last()) {
        (Some(oldest), Some(newest)) => {
            #[expect(clippy::cast_precision_loss, reason = "bytes on a chart")]
            let limit = limit_bytes.map(|l| ("its memory limit".to_owned(), l as f64));
            chart::draw_bars(&Bars {
                title: format!("Peak memory of {}'s last attempts", list.task_id),
                unit: Unit::Bytes,
                width: chart::NARROW,
                bars,
                limit,
                ends: (when(oldest.ended_at, zone), when(newest.ended_at, zone)),
            })
        }
        _ => String::new(),
    };
    MemoryChart {
        svg,
        attempts: list.attempts.len(),
        #[expect(clippy::cast_precision_loss, reason = "bytes as people read them")]
        limit: limit_bytes
            .map(|l| chart::bytes(l as f64))
            .unwrap_or_default(),
        killed,
    }
}

/// Where an attempt's bar goes: its run's panel of the task, on its Attempts tab.
fn attempt_href(
    attempt: &AttemptMemoryView,
    task_id: &str,
    run_href: impl Fn(&str) -> String,
) -> String {
    let instance = attempt
        .map_index
        .map_or_else(String::new, |i| format!("&map_index={i}"));
    format!(
        "{}?task={}&tab=attempts{instance}",
        run_href(&attempt.run_id),
        path_segment(task_id)
    )
}

/// An attempt's hover text: when it ended, its try (and instance), how, and its peak.
fn bar_title(attempt: &AttemptMemoryView, zone: &TimeZone) -> String {
    let instance = attempt
        .map_index
        .map_or_else(String::new, |i| format!(" of instance {i}"));
    #[expect(clippy::cast_precision_loss, reason = "bytes as people read them")]
    let peak = attempt
        .peak_memory_bytes
        .map_or_else(|| "no peak read".to_owned(), |b| chart::bytes(b as f64));
    format!(
        "{}: try {}{instance}, {}, {peak}",
        when(attempt.ended_at, zone),
        attempt.try_number,
        attempt.status.replace('_', " ")
    )
}

/// A task's row of the DAG page's Memory table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRow {
    /// The task.
    pub task_id: String,
    /// The attempts counted.
    pub attempts: u32,
    /// Their p95 peak.
    pub p95: String,
    /// Their highest.
    pub max: String,
    /// Its limit; `worker's share` without one.
    pub limit: String,
    /// The p95 as a share of the limit (`90 %`); empty without one.
    pub share: String,
    /// The share is at [`NEAR`] or more.
    pub near: bool,
    /// How many were killed for their memory.
    pub oom_killed: u32,
}

/// The DAG page's Memory table: each task's p95 peak against its limit, closest first.
#[must_use]
pub fn memory_rows(list: &DagMemoryList) -> Vec<MemoryRow> {
    #[expect(clippy::cast_precision_loss, reason = "bytes as people read them")]
    let show = |b: u64| chart::bytes(b as f64);
    list.tasks
        .iter()
        .map(|task| MemoryRow {
            task_id: task.task_id.clone(),
            attempts: task.attempts,
            p95: show(task.p95_bytes),
            max: show(task.max_bytes),
            limit: task
                .limit_bytes
                .map_or_else(|| "worker's share".to_owned(), show),
            share: task
                .of_limit
                .map_or_else(String::new, |share| format!("{:.0} %", share * 100.0)),
            near: task.of_limit.is_some_and(|share| share >= NEAR),
            oom_killed: task.oom_killed,
        })
        .collect()
}
