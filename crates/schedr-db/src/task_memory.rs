//! A task's peak memory over its attempts (decision 467): the task panel's chart of its last
//! attempts and the DAG page's p95 peaks, read from the attempts table.

use jiff::Timestamp;

use crate::database::{Database, DbError};
use crate::query::{db_time, on_pool, parse_time, sql};

#[cfg(test)]
mod tests;

/// The runs, and the attempts of one task, its peaks are taken from at most, the newest (the
/// p95 of each).
pub const PEAKS_PER_TASK: u32 = 1000;

/// An ended attempt's peak memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptPeak {
    /// The attempt.
    pub attempt_id: String,
    /// Its run.
    pub run_id: String,
    /// Its instance of a mapped task; none for a task that isn't mapped.
    pub map_index: Option<u32>,
    /// Its try.
    pub try_number: u32,
    /// How it ended (`success`, `oom_killed`, ...).
    pub status: String,
    /// Its peak memory, in bytes; none when it wasn't read (an attempt lost with its worker).
    pub peak_memory_bytes: Option<u64>,
    /// When it ended.
    pub ended_at: Timestamp,
}

/// Each task's ended attempts' peaks, for its p95.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPeaks {
    /// The task.
    pub task_id: String,
    /// The peaks read, at most [`PEAKS_PER_TASK`], in no order.
    pub peaks: Vec<u64>,
    /// Of those attempts, how many were killed for their memory.
    pub oom_killed: u32,
}

impl Database {
    /// The last `count` ended attempts of `task_id` of DAG `dag_id`, all its instances, the
    /// oldest first.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails or a row doesn't read.
    pub async fn task_attempt_peaks(
        &self,
        dag_id: &str,
        task_id: &str,
        count: u32,
    ) -> Result<Vec<AttemptPeak>, DbError> {
        // The DAG's runs newest first by `runs_by_dag`, each run's instance of the task by its
        // unique key, its attempts by theirs: the newest runs are read, not the whole history.
        const LAST: &str = "SELECT a.id{text}, r.id{text}, CAST(i.map_index AS BIGINT), \
            CAST(a.try_number AS BIGINT), a.status, a.peak_memory_bytes, {ts<}a.ended_at{>ts} \
            FROM runs r JOIN task_instances i ON i.run_id = r.id AND i.task_id = $2 \
            JOIN attempts a ON a.task_instance_id = i.id \
            WHERE r.dag_id = $1 AND a.ended_at IS NOT NULL AND a.status IS NOT NULL \
            ORDER BY r.planned_for DESC, r.id DESC, i.map_index DESC, a.try_number DESC \
            LIMIT $3";
        type Row = (String, String, i64, i64, String, Option<i64>, String);
        let rows: Vec<Row> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, LAST))
                .bind(dag_id)
                .bind(task_id)
                .bind(i64::from(count))
                .fetch_all(pool)
                .await?
        });
        let mut peaks = rows
            .into_iter()
            .map(
                |(attempt_id, run_id, map_index, try_number, status, peak, ended)| {
                    Ok(AttemptPeak {
                        attempt_id,
                        run_id,
                        map_index: u32::try_from(map_index).ok(),
                        try_number: u32::try_from(try_number).unwrap_or(0),
                        status,
                        peak_memory_bytes: peak.and_then(|p| u64::try_from(p).ok()),
                        ended_at: parse_time("attempts.ended_at", &ended)?,
                    })
                },
            )
            .collect::<Result<Vec<_>, DbError>>()?;
        peaks.reverse();
        Ok(peaks)
    }

    /// Each task of DAG `dag_id` with attempts ended since `since` in its newest
    /// [`PEAKS_PER_TASK`] runs planned since then: the peaks of its newest [`PEAKS_PER_TASK`]
    /// that have one, by task.
    ///
    /// # Errors
    ///
    /// [`DbError`] if the query fails.
    pub async fn dag_task_peaks(
        &self,
        dag_id: &str,
        since: Timestamp,
    ) -> Result<Vec<TaskPeaks>, DbError> {
        // The DAG's newest runs first (`runs_by_dag`), so that a DAG running every minute
        // reads a thousand runs, not 30 days of them (216,000 attempts of 5 tasks: 850 ms).
        const PEAKS: &str = "SELECT task_id, peak, oom FROM (\
            SELECT i.task_id AS task_id, a.peak_memory_bytes AS peak, \
            CASE WHEN a.status = 'oom_killed' THEN 1 ELSE 0 END AS oom, \
            ROW_NUMBER() OVER (PARTITION BY i.task_id ORDER BY a.ended_at DESC, a.id DESC) AS n \
            FROM (SELECT id FROM runs WHERE dag_id = $1 AND planned_for >= $2{ts} \
                  ORDER BY planned_for DESC LIMIT $3) r \
            JOIN task_instances i ON i.run_id = r.id \
            JOIN attempts a ON a.task_instance_id = i.id \
            WHERE a.ended_at >= $2{ts} AND a.peak_memory_bytes IS NOT NULL\
            ) newest WHERE n <= $3 ORDER BY task_id";
        let rows: Vec<(String, i64, i32)> = on_pool!(self, |pool, backend| {
            sqlx::query_as(sql(backend, PEAKS))
                .bind(dag_id)
                .bind(db_time(since))
                .bind(i64::from(PEAKS_PER_TASK))
                .fetch_all(pool)
                .await?
        });
        let mut tasks: Vec<TaskPeaks> = Vec::new();
        for (task_id, peak, oom) in rows {
            if tasks.last().is_none_or(|t| t.task_id != task_id) {
                tasks.push(TaskPeaks {
                    task_id,
                    peaks: Vec::new(),
                    oom_killed: 0,
                });
            }
            if let Some(task) = tasks.last_mut() {
                task.peaks.push(u64::try_from(peak).unwrap_or(0));
                task.oom_killed += u32::from(oom != 0);
            }
        }
        Ok(tasks)
    }
}
