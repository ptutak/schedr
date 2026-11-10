//! The master's retention job (decisions 430-435): every `SCHEDR_HIDDEN__MASTER__RETENTION_INTERVAL`
//! (1 h) while it leads, it deletes what `[retention]` no longer keeps, a batch of
//! `SCHEDR_HIDDEN__MASTER__RETENTION_BATCH` runs per transaction, until nothing is left, so it
//! never holds long locks nor holds the scheduler up (it runs beside the master's loop).

use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use schedr_config::config::{Age, RetentionSection};
use schedr_db::{Database, DbError};

use crate::metrics::MasterMetrics;

#[cfg(test)]
mod tests;

/// How long each kind is kept: `None` forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSettings {
    /// Finished runs.
    pub runs: Option<Duration>,
    /// Finished runs' outputs (only when shorter than `runs`).
    pub outputs: Option<Duration>,
    /// Events triggers found and events posted through the API.
    pub events: Option<Duration>,
    /// Decided commands.
    pub commands: Option<Duration>,
    /// How often a pass runs.
    pub interval: Duration,
    /// The most runs (events, commands) one transaction deletes.
    pub batch: u32,
}

impl Default for RetentionSettings {
    /// Everything kept: a master built for a test deletes nothing.
    fn default() -> Self {
        Self {
            runs: None,
            outputs: None,
            events: None,
            commands: None,
            interval: Duration::from_hours(1),
            batch: 100,
        }
    }
}

const fn kept(age: Age) -> Option<Duration> {
    match age {
        Age::For(duration) => Some(duration),
        Age::Forever => None,
    }
}

impl RetentionSettings {
    /// From `[retention]` and the hidden interval and batch.
    #[must_use]
    pub fn from_config(section: &RetentionSection, interval: Duration, batch: usize) -> Self {
        let runs = kept(section.runs);
        // Outputs kept as long as their runs, or longer: they go with the runs.
        let outputs = kept(section.outputs()).filter(|o| runs.is_none_or(|r| *o < r));
        Self {
            runs,
            outputs,
            events: kept(section.events()),
            commands: kept(section.commands),
            interval,
            batch: u32::try_from(batch).unwrap_or(u32::MAX).max(1),
        }
    }
}

/// What one pass deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Runs.
    pub runs: u64,
    /// Runs whose outputs went.
    pub outputs: u64,
    /// Events.
    pub events: u64,
    /// Commands.
    pub commands: u64,
    /// Removed DAGs gone with their last run.
    pub dags: u64,
    /// DAG versions no run, trigger or DAG uses any more.
    pub versions: u64,
}

impl Pass {
    /// Whether it deleted anything.
    #[must_use]
    pub const fn any(&self) -> bool {
        self.runs + self.outputs + self.events + self.commands + self.dags + self.versions > 0
    }
}

/// `now` less `age`: what ended before is old.
fn cut(now: Timestamp, age: Duration) -> Timestamp {
    SignedDuration::try_from(age)
        .ok()
        .and_then(|age| now.checked_sub(age).ok())
        .unwrap_or(Timestamp::MIN)
}

/// Deletes batch after batch through `delete`, while `leading`, until one deletes nothing (a
/// batch of runs is one DAG's, so one not full doesn't mean none is left).
async fn batches<F, Fut>(leading: &impl Fn() -> bool, mut delete: F) -> Result<u64, DbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u64, DbError>>,
{
    let mut total = 0;
    while leading() {
        let deleted = delete().await?;
        total += deleted;
        if deleted == 0 {
            break;
        }
    }
    Ok(total)
}

/// One pass at `now`: outputs, runs, removed DAGs, unused DAG versions, events, commands, each
/// counted in `metrics`. Stops when this master no longer leads.
///
/// # Errors
///
/// [`DbError`] if a batch fails: what the earlier ones deleted stays deleted.
pub async fn pass(
    db: &Database,
    settings: &RetentionSettings,
    now: Timestamp,
    leading: impl Fn() -> bool,
    metrics: &MasterMetrics,
) -> Result<Pass, DbError> {
    let batch = settings.batch;
    let mut pass = Pass::default();
    if let Some(age) = settings.outputs {
        let before = cut(now, age);
        pass.outputs = batches(&leading, || db.delete_old_outputs(before, batch, now)).await?;
        metrics.retention_deleted("outputs", pass.outputs);
    }
    if let Some(age) = settings.runs {
        let before = cut(now, age);
        pass.runs = batches(&leading, || db.delete_old_runs(before, batch)).await?;
        metrics.retention_deleted("runs", pass.runs);
        if leading() {
            pass.dags = db.delete_removed_dags(before).await?;
            metrics.retention_deleted("dags", pass.dags);
        }
        pass.versions = batches(&leading, || db.delete_unused_versions()).await?;
        metrics.retention_deleted("versions", pass.versions);
    }
    if let Some(age) = settings.events {
        let before = cut(now, age);
        pass.events = batches(&leading, || db.delete_old_events(before, batch)).await?;
        metrics.retention_deleted("events", pass.events);
    }
    if let Some(age) = settings.commands {
        let before = cut(now, age);
        pass.commands = batches(&leading, || db.delete_old_commands(before, batch)).await?;
        metrics.retention_deleted("commands", pass.commands);
    }
    Ok(pass)
}
