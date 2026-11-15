//! `GET /api/v1/metrics/tasks`: the Metrics page's throughput and start latency (decision
//! 494), from the attempts table. A range's final buckets are kept between requests, as the
//! samples' are (decision 492): each request reads only the buckets since the last final one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use jiff::{SignedDuration, Timestamp};
use schedr_db::task_flow::{EndedCount, StartLatency};
use serde::Deserialize;
use utoipa::IntoParams;

use crate::error::ApiError;
use crate::routes::ApiResult;
use crate::series::{Range, cache::final_to};
use crate::state::ApiState;
use crate::views::{TaskFlowList, TaskFlowView};

#[cfg(test)]
mod tests;

/// Which range.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FlowQuery {
    /// `1h`, `24h` (the default), `7d` or `30d`.
    pub range: Option<String>,
}

/// A range's kept buckets: those final when last read, of a range starting at `kept_from`.
#[derive(Debug, Default)]
pub struct FlowEntry {
    kept_from: Option<Timestamp>,
    final_to: Option<Timestamp>,
    ended: Vec<EndedCount>,
    latency: Vec<StartLatency>,
}

impl FlowEntry {
    /// Where a request for `[from, to)` reads from: after what is kept when that covers its
    /// start, else its start (the entry cleared: a clock gone back, or nothing kept).
    fn read_from(&mut self, from: Timestamp, to: Timestamp) -> Timestamp {
        match (self.kept_from, self.final_to) {
            (Some(kept_from), Some(final_to)) if kept_from <= from && final_to <= to => {
                final_to.max(from)
            }
            _ => {
                *self = Self::default();
                from
            }
        }
    }
}

/// Each range's kept buckets, one lock a range (a request reading it holds it, the next
/// waits).
#[derive(Debug, Default)]
pub struct FlowCache {
    entries: Mutex<HashMap<Range, Arc<tokio::sync::Mutex<FlowEntry>>>>,
}

impl FlowCache {
    /// `range`'s entry, locked.
    pub async fn entry(&self, range: Range) -> tokio::sync::OwnedMutexGuard<FlowEntry> {
        let slot = Arc::clone(
            self.entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(range)
                .or_default(),
        );
        slot.lock_owned().await
    }
}

/// How an attempt that ended counts: succeeded, failed or stopped.
fn counts(status: &str) -> (u64, u64, u64) {
    match status {
        "success" => (1, 0, 0),
        "failed" | "timed_out" | "oom_killed" => (0, 1, 0),
        _ => (0, 0, 1),
    }
}

/// `kept`'s buckets from `from` on, then `read`'s.
fn merged<T: Clone>(kept: &[T], read: Vec<T>, from: Timestamp, at: fn(&T) -> Timestamp) -> Vec<T> {
    kept.iter()
        .filter(|row| at(row) >= from)
        .cloned()
        .chain(read)
        .collect()
}

/// The attempts of a range in buckets: how many ended, by how they ended, and the start
/// latency of those started, as p50 and p95 (decision 494).
///
/// # Errors
///
/// [`ApiError::BadRequest`] for a range not known, [`ApiError`] if the database fails.
#[utoipa::path(get, path = "/api/v1/metrics/tasks", tag = "workers", params(FlowQuery),
    responses((status = 200, body = TaskFlowList), (status = 400, body = crate::views::ErrorBody)))]
pub async fn task_flow(
    State(state): State<ApiState>,
    query: Result<Query<FlowQuery>, QueryRejection>,
) -> ApiResult<TaskFlowList> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let range = crate::series::range_of(query.range.as_deref())?;
    let to = (state.clock)();
    let from = range.from(to);
    let bucket_s = range.bucket_s();
    let mut entry = state.flow.entry(range).await;
    let read_from = entry.read_from(from, to);
    let ended = state.db.attempts_ended((read_from, to), bucket_s).await?;
    let latency = state.db.start_latency((read_from, to), bucket_s).await?;
    let ended = merged(&entry.ended, ended, from, |row| row.at);
    let latency = merged(&entry.latency, latency, from, |row| row.at);
    let final_to = final_to(to, bucket_s, state.series.limits().final_after);
    *entry = FlowEntry {
        kept_from: Some(from),
        final_to: Some(final_to),
        ended: ended
            .iter()
            .filter(|row| row.at < final_to)
            .cloned()
            .collect(),
        latency: latency
            .iter()
            .filter(|row| row.at < final_to)
            .copied()
            .collect(),
    };
    drop(entry);
    let mut views: BTreeMap<Timestamp, TaskFlowView> = BTreeMap::new();
    let mut at = from;
    while at <= to {
        views.insert(
            at,
            TaskFlowView {
                at,
                succeeded: 0,
                failed: 0,
                stopped: 0,
                started: 0,
                start_p50_ms: None,
                start_p95_ms: None,
            },
        );
        at = at
            .checked_add(SignedDuration::from_secs(i64::from(bucket_s)))
            .unwrap_or(Timestamp::MAX);
    }
    for row in &ended {
        if let Some(bucket) = views.get_mut(&row.at) {
            let (succeeded, failed, stopped) = counts(&row.status);
            bucket.succeeded += succeeded * row.count;
            bucket.failed += failed * row.count;
            bucket.stopped += stopped * row.count;
        }
    }
    for row in &latency {
        if let Some(bucket) = views.get_mut(&row.at) {
            bucket.started = row.started;
            bucket.start_p50_ms = Some(row.p50_ms);
            bucket.start_p95_ms = Some(row.p95_ms);
        }
    }
    Ok(Json(TaskFlowList {
        range: range.name().to_owned(),
        step_s: bucket_s,
        from,
        to,
        buckets: views.into_values().collect(),
    }))
}
