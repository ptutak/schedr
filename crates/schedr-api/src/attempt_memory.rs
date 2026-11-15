//! A task's peak memory over its attempts (M5.1k, decision 467): the task panel's chart of its
//! last attempts, and the DAG page's p95 peak of each task against its limit.
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use axum::Json;
use axum::extract::{Path, State};
use jiff::{SignedDuration, Timestamp};
use schedr_db::dags::StoredDag;
use schedr_db::task_memory::{AttemptPeak, TaskPeaks};

use crate::error::ApiError;
use crate::routes::{ApiResult, known_dag, parse_dag_id};
use crate::state::ApiState;
use crate::views::{AttemptMemoryList, AttemptMemoryView, DagMemoryList, TaskPeakView};

#[cfg(test)]
mod tests;

/// The attempts the task panel charts.
pub const CHARTED_ATTEMPTS: u32 = 30;

/// How far back the DAG page's peaks reach.
const PEAKS_OVER: SignedDuration = SignedDuration::from_hours(30 * 24);

/// A task's last ended attempts' peak memory, all its runs and instances (decision 467).
#[utoipa::path(get, path = "/api/v1/dags/{dag_id}/tasks/{task_id}/memory", tag = "dags",
    params(("dag_id" = String, Path, description = "The DAG's ID"),
        ("task_id" = String, Path, description = "The task's ID")),
    responses((status = 200, body = AttemptMemoryList), (status = 404, body = crate::views::ErrorBody)))]
pub async fn task_memory(
    State(state): State<ApiState>,
    Path((dag_id, task_id)): Path<(String, String)>,
) -> ApiResult<AttemptMemoryList> {
    let dag_id = parse_dag_id(&dag_id)?;
    let dag = known_dag(&state, &dag_id).await?;
    let attempts = state
        .db
        .task_attempt_peaks(dag_id.as_str(), &task_id, CHARTED_ATTEMPTS)
        .await?;
    let defined = limits(&dag)
        .into_iter()
        .find(|(id, _)| *id == task_id)
        .map(|(_, limit)| limit);
    if defined.is_none() && attempts.is_empty() {
        return Err(ApiError::NotFound(format!(
            "no task `{task_id}` in DAG `{dag_id}`"
        )));
    }
    Ok(Json(AttemptMemoryList {
        task_id,
        limit_bytes: defined.flatten(),
        attempts: attempts.into_iter().map(view).collect(),
    }))
}

/// Each task's p95 peak memory over its attempts of the last 30 days, against its limit:
/// closest to it first (decision 467).
#[utoipa::path(get, path = "/api/v1/dags/{dag_id}/memory", tag = "dags",
    params(("dag_id" = String, Path, description = "The DAG's ID")),
    responses((status = 200, body = DagMemoryList), (status = 404, body = crate::views::ErrorBody)))]
pub async fn dag_memory(
    State(state): State<ApiState>,
    Path(dag_id): Path<String>,
) -> ApiResult<DagMemoryList> {
    let dag_id = parse_dag_id(&dag_id)?;
    let dag = known_dag(&state, &dag_id).await?;
    let since = (state.clock)()
        .checked_sub(PEAKS_OVER)
        .unwrap_or(Timestamp::UNIX_EPOCH);
    let peaks = state.db.dag_task_peaks(dag_id.as_str(), since).await?;
    Ok(Json(DagMemoryList {
        since,
        tasks: ranked(peaks, &limits(&dag)),
    }))
}

/// Each task of the DAG's current version with its memory budget.
fn limits(dag: &StoredDag) -> Vec<(String, Option<u64>)> {
    dag.current
        .iter()
        .flat_map(|current| &current.dag.tasks)
        .map(|task| (task.id.to_string(), task.settings.memory_bytes))
        .collect()
}

fn view(peak: AttemptPeak) -> AttemptMemoryView {
    AttemptMemoryView {
        attempt_id: peak.attempt_id,
        run_id: peak.run_id,
        map_index: peak.map_index,
        try_number: peak.try_number,
        status: peak.status,
        peak_memory_bytes: peak.peak_memory_bytes,
        ended_at: peak.ended_at,
    }
}

/// The tasks' peaks, those with a limit closest to it first, then the rest by p95.
fn ranked(peaks: Vec<TaskPeaks>, limits: &[(String, Option<u64>)]) -> Vec<TaskPeakView> {
    let mut tasks: Vec<TaskPeakView> = peaks
        .into_iter()
        .filter_map(|mut task| {
            let p95 = p95(&mut task.peaks)?;
            let limit_bytes = limits
                .iter()
                .find(|(id, _)| *id == task.task_id)
                .and_then(|(_, limit)| *limit);
            #[expect(
                clippy::cast_precision_loss,
                reason = "a share of a limit: two decimals are shown"
            )]
            let of_limit = limit_bytes
                .filter(|limit| *limit > 0)
                .map(|limit| p95 as f64 / limit as f64);
            Some(TaskPeakView {
                attempts: u32::try_from(task.peaks.len()).unwrap_or(u32::MAX),
                max_bytes: task.peaks.last().copied().unwrap_or(p95),
                p95_bytes: p95,
                oom_killed: task.oom_killed,
                limit_bytes,
                of_limit,
                task_id: task.task_id,
            })
        })
        .collect();
    tasks.sort_by(|a, b| {
        match (a.of_limit, b.of_limit) {
            (Some(x), Some(y)) => y.total_cmp(&x),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => b.p95_bytes.cmp(&a.p95_bytes),
        }
        .then_with(|| a.task_id.cmp(&b.task_id))
    });
    tasks
}

/// The 95th percentile by nearest rank (the smallest value at least 95% of them are at or
/// below), sorting `peaks`; none of none.
fn p95(peaks: &mut [u64]) -> Option<u64> {
    peaks.sort_unstable();
    let rank = (peaks.len() * 95).div_ceil(100);
    peaks.get(rank.checked_sub(1)?).copied()
}
