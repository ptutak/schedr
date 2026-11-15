//! The samples of each process over a range, in buckets (M5.1j; decisions 464, 465): the
//! Metrics page's charts and the Deployments page's sparkline.
#![expect(
    clippy::missing_errors_doc,
    reason = "handler docs become OpenAPI descriptions; errors are listed as responses"
)]

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use jiff::{SignedDuration, Timestamp};
use schedr_db::samples::{Bucket, MINUTE, ROLLUP};

use crate::error::ApiError;
use crate::routes::ApiResult;
use crate::state::ApiState;
use crate::views::{
    CoverageView, MemoryPointView, MemoryTotals, PartsPointView, ProcessSamplesView,
    SamplePointView, SampleSeriesList, TaskMemoryView, WorkerPartsView,
};

pub mod cache;
#[cfg(test)]
mod tests;

/// How recent a worker's last parts must be to be shown as now.
const PARTS_WITHIN: SignedDuration = SignedDuration::from_mins(5);

/// A range the charts show.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Range {
    /// The last hour: each minute.
    Hour,
    /// The last 24 hours: 6 minutes a point.
    Day,
    /// The last 7 days: 45 minutes a point, from the rollups.
    Week,
    /// The last 30 days: 3 hours a point, from the rollups.
    Month,
}

impl Range {
    /// Every range, shortest first.
    pub const ALL: [Self; 4] = [Self::Hour, Self::Day, Self::Week, Self::Month];

    /// The range a name names (`1h`, `24h`, `7d`, `30d`).
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|range| range.name() == name)
    }

    /// Its name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hour => "1h",
            Self::Day => "24h",
            Self::Week => "7d",
            Self::Month => "30d",
        }
    }

    /// Its length, in seconds.
    #[must_use]
    pub const fn span_s(self) -> i64 {
        match self {
            Self::Hour => 3600,
            Self::Day => 86_400,
            Self::Week => 7 * 86_400,
            Self::Month => 30 * 86_400,
        }
    }

    /// The samples it reads: minutes or rollups.
    #[must_use]
    pub const fn source(self) -> u32 {
        match self {
            Self::Hour | Self::Day => MINUTE,
            Self::Week | Self::Month => ROLLUP,
        }
    }

    /// A point's width, in seconds: 60, 240, 224 and 240 points.
    #[must_use]
    pub const fn bucket_s(self) -> u32 {
        match self {
            Self::Hour => 60,
            Self::Day => 360,
            Self::Week => 2700,
            Self::Month => 10_800,
        }
    }

    /// Its start, when it ends at `to`: as many whole buckets before `to`'s as make its
    /// length, so it has at most `span / bucket` points, `to`'s bucket the last.
    #[must_use]
    pub fn from(self, to: Timestamp) -> Timestamp {
        floor(to, self.bucket_s())
            .checked_sub(SignedDuration::from_secs(
                self.span_s() - i64::from(self.bucket_s()),
            ))
            .unwrap_or(Timestamp::UNIX_EPOCH)
    }

    /// Where its rollups end and its minutes begin, when it ends at `to` and the newest
    /// rollup's window ends at `rolled` (none: no rollup): the rollups before it, the minutes
    /// not rolled up yet after it (Codex's reviews of M5.1j: a new process showed only once
    /// rolled up, and a window the leader hadn't rolled went missing); its start for minutes
    /// only.
    #[must_use]
    pub fn cut(self, to: Timestamp, rolled: Option<Timestamp>) -> Timestamp {
        let from = self.from(to);
        if self.source() == MINUTE {
            return from;
        }
        rolled.map_or(from, |rolled| rolled.clamp(from, to))
    }

    /// Its start, cut and end when it ends at `to`, reading where the rollups end from `db`.
    async fn bounds(
        self,
        db: &schedr_db::Database,
        to: Timestamp,
    ) -> Result<(Timestamp, Timestamp, Timestamp), ApiError> {
        let rolled = if self.source() == MINUTE {
            None
        } else {
            db.rolled_up_to().await?
        };
        Ok((self.from(to), self.cut(to, rolled), to))
    }
}

/// Which samples.
#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SeriesQuery {
    /// `1h`, `24h` (the default), `7d` or `30d`.
    pub range: Option<String>,
    /// One process's only (its ID).
    pub process: Option<String>,
}

/// The range a query names.
fn range_of(name: Option<&str>) -> Result<Range, ApiError> {
    name.map_or(Ok(Range::Day), |name| {
        Range::parse(name).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "`range` is one of 1h, 24h, 7d and 30d, not `{name}`"
            ))
        })
    })
}

/// Each process's memory and CPU over a range, in buckets: averages and peaks, its container's
/// limit, and a worker's parts (decisions 464, 465).
#[utoipa::path(get, path = "/api/v1/metrics/samples", tag = "workers", params(SeriesQuery),
    responses((status = 200, body = SampleSeriesList), (status = 400, body = crate::views::ErrorBody)))]
pub async fn list_samples(
    State(state): State<ApiState>,
    query: Result<Query<SeriesQuery>, QueryRejection>,
) -> ApiResult<SampleSeriesList> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let range = range_of(query.range.as_deref())?;
    let to = (state.clock)();
    let bounds = range.bounds(&state.db, to).await?;
    let (from, cut, _) = bounds;
    let process = query.process.as_deref();
    // At most four statements, filtered in the database: where the rollups end, the buckets
    // with the deployment's coverage, each process's latest roles and limit, and the last few
    // minutes (a worker's parts now); the buckets and latest samples since the last final
    // bucket only, the rest kept (`cache`).
    let key = cache::Key {
        totals: false,
        range,
        process: query.process.clone(),
    };
    let mut entry = state.series.entry(key, to).await;
    let read_from = entry.read_from(from, to);
    let read = state
        .db
        .sample_buckets((read_from, cut, to), range.bucket_s(), process)
        .await?;
    let buckets = cache::merged(&entry.buckets, read, from);
    let final_to = cache::final_to(to, range.bucket_s());
    let read_latest = state.db.latest_of(read_from, to, process).await?;
    let mut latest_of = cache::merged_latest(&entry.latest, read_latest, from);
    entry.latest.clone_from(&latest_of);
    cache::keep_buckets(&mut entry, &buckets, final_to);
    drop(entry);
    // A deployment's total in a bucket divides by these (memory by kind), read with the
    // buckets in one statement.
    let coverage: Vec<CoverageView> = buckets
        .iter()
        .map(|b| (b.at, b.covered))
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .map(|(at, minutes)| CoverageView { at, minutes })
        .collect();
    let since = to.checked_sub(PARTS_WITHIN).unwrap_or(to);
    let latest = state.db.samples_of(MINUTE, since, to, process).await?;
    let mut processes: Vec<ProcessSamplesView> = Vec::new();
    for bucket in buckets {
        if processes
            .last()
            .is_none_or(|p| p.process_id != bucket.process_id)
        {
            processes.push(ProcessSamplesView {
                container_limit: latest_of
                    .get(&bucket.process_id)
                    .and_then(|l| l.container_limit),
                roles: latest_of
                    .remove(&bucket.process_id)
                    .map(|l| l.roles)
                    .unwrap_or_default(),
                process_id: bucket.process_id.clone(),
                points: Vec::new(),
                parts: None,
            });
        }
        if let Some(process) = processes.last_mut() {
            process.points.push(point(&bucket));
        }
    }
    for process in &mut processes {
        let last = latest
            .iter()
            .rev()
            .find(|s| s.process_id == process.process_id);
        process.parts = last.and_then(|s| {
            s.parts.clone().map(|parts| WorkerPartsView {
                at: s.at,
                fork_servers_bytes: parts.fork_servers_bytes,
                watcher_hosts_bytes: parts.watcher_hosts_bytes,
                tasks_bytes: parts.tasks_bytes,
                largest_tasks: parts
                    .largest_tasks
                    .into_iter()
                    .map(TaskMemoryView::from)
                    .collect(),
            })
        });
    }
    Ok(Json(SampleSeriesList {
        coverage,
        range: range.name().to_owned(),
        step_s: range.bucket_s(),
        from,
        to,
        processes,
    }))
}

/// Every process's memory together over a range, in buckets (decision 462's sparkline).
#[utoipa::path(get, path = "/api/v1/metrics/memory", tag = "workers", params(SeriesQuery),
    responses((status = 200, body = MemoryTotals), (status = 400, body = crate::views::ErrorBody)))]
pub async fn memory_totals(
    State(state): State<ApiState>,
    query: Result<Query<SeriesQuery>, QueryRejection>,
) -> ApiResult<MemoryTotals> {
    let Query(query) = query.map_err(|e| ApiError::BadRequest(e.body_text()))?;
    let range = range_of(query.range.as_deref())?;
    let to = (state.clock)();
    let from = range.from(to);
    let (_, cut, _) = range.bounds(&state.db, to).await?;
    let key = cache::Key {
        totals: true,
        range,
        process: None,
    };
    let mut entry = state.series.entry(key, to).await;
    let read_from = entry.read_from(from, to);
    let read = state
        .db
        .memory_totals((read_from, cut, to), range.bucket_s())
        .await?;
    let totals = cache::merged_totals(
        &mut entry,
        read,
        from,
        cache::final_to(to, range.bucket_s()),
    );
    drop(entry);
    Ok(Json(MemoryTotals {
        step_s: range.bucket_s(),
        from,
        to,
        points: totals
            .into_iter()
            .map(|(at, resident)| MemoryPointView { at, resident })
            .collect(),
    }))
}

/// `at` down to a multiple of `step_s` seconds.
fn floor(at: Timestamp, step_s: u32) -> Timestamp {
    let step = i64::from(step_s.max(1));
    Timestamp::from_second(at.as_second().div_euclid(step) * step).unwrap_or(at)
}

/// A bucket as the API shows it.
fn point(bucket: &Bucket) -> SamplePointView {
    SamplePointView {
        at: bucket.at,
        resident_avg: bucket.resident_avg,
        resident_peak: bucket.resident_peak,
        memory_avg: bucket.memory_avg,
        memory_peak: bucket.memory_peak,
        minutes: bucket.minutes,
        cpu_avg: bucket.cpu_avg,
        cpu_peak: bucket.cpu_peak,
        container_avg: bucket.container_avg,
        container_peak: bucket.container_peak,
        container_limit: bucket.container_limit,
        parts: bucket.parts.map(|(forks, hosts, tasks)| PartsPointView {
            fork_servers_bytes: forks,
            watcher_hosts_bytes: hosts,
            tasks_bytes: tasks,
        }),
    }
}
